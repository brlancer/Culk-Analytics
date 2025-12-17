# Culk Analytics Architecture

## ELT Pattern Overview

Culk Analytics follows the **ELT (Extract-Load-Transform)** architectural pattern, where raw data is extracted from source systems, loaded into the data warehouse, and then transformed using SQL.

```
┌─────────────────────────────────────────────────────────────┐
│                        EXTRACT LAYER                        │
│  Python scripts fetch raw data from APIs (REST, GraphQL)    │
│                                                             │
│  ┌──────────┐  ┌──────┐  ┌──────────┐  ┌──────┐             │
│  │ Shopify  │  │Faire │  │ ShipHero │  │ Loop │  ...        │
│  └────┬─────┘  └───┬──┘  └────┬─────┘  └───┬──┘             │
│       │            │          │            │                │
└───────┼────────────┼──────────┼────────────┼─-──────────────┘
        │            │          │            │
        ▼            ▼          ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│                         LOAD LAYER                          │
│     dlt library handles schema detection & loading          │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  dlt Pipeline                                       │    │
│  │  - Auto schema inference                            │    │
│  │  - Normalization (flatten nested JSON)              │    │
│  │  - Incremental state management                     │    │
│  │  - Data type mapping                                │    │
│  └─────────────────────────────────────────────────────┘    │
│                          │                                  │
└──────────────────────────┼──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   POSTGRESQL DATABASE                       │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  public schema (raw data from dlt)                   │   │
│  │  - Auto-generated tables                             │   │
│  │  - One table per API endpoint/resource               │   │
│  │  - Minimal transformations                           │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                  │
│                          ▼                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  staging schema (intermediate transformations)       │   │
│  │  - Data cleaning (nulls, types, deduplication)       │   │
│  │  - Joining raw tables                                │   │
│  │  - Calculated fields (margins, ratios)               │   │
│  │  - Standardization (date formats, currencies)        │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                  │
│                          ▼                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  analytics schema (final business logic)             │   │
│  │  - Business metrics & KPIs                           │   │
│  │  - Aggregations (daily, weekly, monthly)             │   │
│  │  - Dimensional models (star schema)                  │   │
│  │  - Reporting tables for BI tools                     │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Why ELT vs. ETL?

### Traditional ETL (Extract-Transform-Load)
- Transform data **before** loading into warehouse
- Transformation logic in Python/application code
- Slower to iterate on business logic
- Complex dependency management

### Modern ELT (Extract-Load-Transform)
- Load raw data **first**, transform in SQL **later**
- Transformation logic close to the data (PostgreSQL)
- Faster iteration on business logic (SQL is easier to modify)
- Leverage database optimizations (indexes, parallel processing)
- Data lineage and audit trail (raw → staging → analytics)

## Three-Layer Architecture

### Layer 1: Extract (Python in `ingestion/`)

**Purpose**: Fetch raw data from source APIs

**Responsibilities**:
- API authentication
- Pagination handling
- Rate limit management
- Error handling and retries
- Minimal data cleaning (only what's necessary for API interaction)

**REST API Sources** (majority):
- Use **dlt REST API client** for automatic pagination and schema handling
- Examples: Shopify, Faire, Loop Returns, Airtable, Google Ads

**GraphQL Sources** (custom extraction):
- Custom queries with `aiohttp` or `requests`
- Parse nested responses (edges/nodes structures)
- Feed extracted data into dlt pipeline
- Examples: ShipHero, Meta Ads

**Output**: Raw JSON/dict structures ready for dlt loading

### Layer 2: Load (dlt Library)

**Purpose**: Automated schema detection, normalization, and loading

**dlt Handles**:
- **Schema Inference**: Automatically detects data types from JSON
- **Normalization**: Flattens nested JSON structures into relational tables
- **Incremental Loading**: Tracks state to avoid reprocessing data
- **Data Type Mapping**: Maps JSON types to PostgreSQL types
- **Error Recovery**: Handles transient failures and retries

**Target**: PostgreSQL `public` schema

**Table Naming**: dlt creates tables based on resource names (e.g., `orders`, `products`)

**Schema Evolution**: dlt handles schema changes automatically (new fields → new columns)

### Layer 3: Transform (SQL in PostgreSQL)

**Purpose**: Business logic, data cleaning, and analytical models

#### Staging Schema
Intermediate transformations:
- **Data Cleaning**: Remove duplicates, handle nulls, fix data types
- **Standardization**: Consistent date formats, currency conversions
- **Joining**: Combine raw tables (orders + products + customers)
- **Calculated Fields**: Profit margins, customer lifetime value

#### Analytics Schema
Final business logic:
- **Dimensional Models**: Fact and dimension tables (star schema)
- **Metrics & KPIs**: Revenue, ROAS, inventory turnover, return rate
- **Aggregations**: Daily/weekly/monthly summaries
- **Reporting Tables**: Pre-aggregated for BI tools (Metabase, Looker, Tableau)

**Benefits of SQL Transformations**:
- Version controlled (SQL files in git)
- Testable (unit tests on transformations)
- Performant (database optimizations)
- Transparent (easy to audit and debug)

## Data Flow Example: Shopify Orders

```
1. EXTRACT (ingestion/shopify.py)
   └─> Fetch orders from Shopify REST API
       └─> Filter by updated_at_min for incremental
           └─> Return raw JSON response

2. LOAD (dlt pipeline)
   └─> dlt receives raw JSON
       └─> Infers schema (order_id, customer_id, total_price, etc.)
           └─> Creates/updates table: public.orders
               └─> Inserts new rows (merge mode)

3. TRANSFORM (SQL scripts - future phase)
   └─> Staging layer
       └─> staging.clean_orders
           └─> Remove duplicates, parse timestamps
               └─> Join with staging.clean_products
                   └─> Calculate line item margins
   └─> Analytics layer
       └─> analytics.daily_revenue
           └─> GROUP BY date, channel
               └─> Used by BI dashboards
```

## Handling GraphQL Sources

GraphQL APIs (ShipHero, Meta Ads) require **custom extraction** because dlt's REST API client is designed for REST patterns.

**Strategy**:
1. **Extract**: Write custom Python code to query GraphQL endpoint
2. **Flatten**: Parse nested response (edges/nodes) into flat dictionaries
3. **Load**: Pass flattened data to dlt pipeline as a Python generator
4. **Benefit**: Still leverage dlt for schema management and incremental loading

**Example Pattern**:
```python
@dlt.source
def shiphero_source():
    # Custom GraphQL query
    data = fetch_from_graphql()
    # Flatten nested structure
    flat_data = flatten_graphql_response(data)
    # Yield to dlt
    yield flat_data

# dlt handles the rest
pipeline = dlt.pipeline(destination="postgres")
pipeline.run(shiphero_source())
```

## Incremental Loading Strategy

**State Management**: dlt tracks the "high water mark" (e.g., last `updated_at`) to avoid reprocessing

**Strategies by Source**:
- **Timestamp-based**: Filter by `updated_at > last_run` (Shopify, Faire, Loop)
- **Date segments**: Pull data for specific date ranges (Meta Ads, Google Ads)
- **Full refresh**: Re-extract entire dataset (Airtable product master - small dataset)

**dlt Write Modes**:
- `merge`: Upsert based on primary key (for incremental)
- `replace`: Drop and recreate table (for full refresh)
- `append`: Add new rows without deduplication

## Data Independence

**Critical Principle**: Each data source is extracted and loaded **independently**.

- No assumptions about join keys during extraction
- No cross-source logic in Python extraction scripts
- Data integration happens **later** in the Transform layer (SQL)
- Benefits:
  - Easier to debug (isolate issues to one source)
  - Parallel extraction (run sources simultaneously)
  - Schema changes in one source don't break others

## Scalability Considerations

### Current Setup (Local PostgreSQL)
- Suitable for small to medium data volumes (<1M rows/day)
- Local development and testing

### Future Growth
- **Cloud Warehouse**: Migrate to Snowflake, BigQuery, or Redshift
- **Orchestration**: Add Airflow, Prefect, or Dagster for scheduling
- **dbt Integration**: Use dbt for SQL transformation layer
- **Data Quality**: Add Great Expectations or Soda for validation
- **Monitoring**: Add observability (data freshness, row counts, anomalies)

## Technology Choices

### Why dlt?
- **Open Source**: No vendor lock-in
- **Python-Native**: Easy to integrate with data science workflows
- **Automatic Schema Evolution**: Handles API changes gracefully
- **Verified Sources**: Pre-built connectors for common APIs
- **State Management**: Built-in incremental loading

### Why PostgreSQL?
- **Mature**: Battle-tested for OLAP workloads
- **SQL Support**: Full SQL feature set (CTEs, window functions, JSON)
- **Cost**: Free and open source
- **Migration Path**: Easy to migrate to cloud warehouses later

### Why Python?
- **API Integration**: Rich ecosystem for API clients
- **dlt Native**: dlt is a Python library
- **Data Science**: Easy integration with pandas, numpy, scikit-learn

## Security & Best Practices

1. **Secrets Management**: 
   - Store API keys in `.dlt/secrets.toml` (gitignored)
   - Never commit credentials
   - Use environment variables in production

2. **Schema Isolation**:
   - `public`: Raw dlt data (don't query directly in production)
   - `staging`: Intermediate work (not for end users)
   - `analytics`: Production-ready tables (for BI tools)

3. **Data Retention**:
   - Keep raw data in `public` for audit trail
   - Implement retention policies for old data (e.g., archive after 2 years)

4. **Access Control**:
   - Dedicated database user for pipeline (`culk_analyst`)
   - Read-only users for BI tools
   - Restrict access to `public` schema in production

## Future Scalability Considerations

### Current Setup (Local PostgreSQL)
- Suitable for small to medium data volumes (<1M rows/day)
- Local development and testing
- Single-machine processing

### Future Growth Options
- **Cloud Warehouse**: Migrate to Snowflake, BigQuery, or Redshift for better scale and performance
- **Orchestration**: Add Airflow, Prefect, or Dagster for scheduling and dependency management
- **dbt Integration**: Use dbt for SQL transformation layer with version control and testing
- **Data Quality**: Add Great Expectations or Soda for automated validation
- **Monitoring**: Add observability tools for data freshness, row counts, and anomaly detection
- **Performance**: Implement partitioning, indexing strategies, and materialized views for large datasets
