# Culk Analytics

**A Centralized Data Warehouse for E-commerce Operations**

## Project Overview
This is an ELT (Extract-Load-Transform) pipeline designed to consolidate data from a multi-channel apparel brand into a single source of truth.

### The Business Problem
Data for Culk is currently fragmented across distinct operational silos:
* **Sales:** Shopify (DTC) & Faire (Wholesale)
* **Logistics:** ShipHero (Inventory & Fulfillment)
* **Returns:** Loop Returns
* **Marketing:** Meta & Google Ads
* **Master Data:** Airtable (Product attributes & COGS)

### The Solution
By ingesting these sources into PostgreSQL, we move beyond siloed CSV exports to enable complex SQL transformations. This allows for advanced analysis on critical metrics (e.g. SKU Level Contribution Margin).

## Tech Stack

- **Python 3.10+**: Core orchestration and extraction logic
- **dlt (data load tool)**: Automated schema detection, normalization, and loading
- **PostgreSQL**: Target data warehouse (local development)
- **ELT Architecture**: Raw extraction → Load to database → SQL-based transformations

## ELT Architecture Overview

This project follows the **ELT pattern**:

1. **Extract**: Python scripts in `ingestion/` fetch raw data from various APIs (REST, GraphQL)
2. **Load**: `dlt` library handles schema inference, normalization, and loading to PostgreSQL `public` schema
3. **Transform**: (Future) SQL views/materialized tables in `staging` and `analytics` schemas for business logic

graph LR
    subgraph "Sources (Extract)"
        S[Shopify]
        F[Faire]
        SH[ShipHero]
        L[Loop]
        M[Marketing APIs]
    end

    subgraph "Ingestion (dlt)"
        Py[Python/dlt Scripts]
    end

    subgraph "Warehouse (Postgres)"
        Raw[(Public Schema<br/>Raw Data)]
        Stg[(Staging Schema<br/>Cleaned)]
        Anl[(Analytics Schema<br/>Metrics)]
    end

    S --> Py
    F --> Py
    SH --> Py
    L --> Py
    M --> Py

    Py --> Raw
    Raw --> Stg
    Stg --> Anl

### Why ELT?
- Load raw data first, transform later in SQL (closer to the data)
- dlt automates schema evolution and handles API pagination/state
- Separation of concerns: extraction logic vs. transformation logic

## Data Sources

- Shopify (commerce hub for B2B + DTC)
- Faire (wholesale orders)
- ShipHero (3PL inventory & shipments)
- Loop Returns (return tracking)
- Meta/Facebook Ads (ad spend)
- Google Ads (ad spend)
- Airtable (product master data)

See `docs/DATA_SOURCES.md` for detailed API information.

## Setup Instructions

### 1. Database Setup (PostgreSQL)

Ensure PostgreSQL is installed and running locally:

```bash
# macOS (via Homebrew)
brew install postgresql@15
brew services start postgresql@15

# Verify connection
psql postgres
```

Initialize the database:

```bash
cd database
./init_db.sh
```

This creates:
- Database: `culk_db`
- Schemas: `public` (dlt loads), `staging`, `analytics`

See `database/README.md` for more details.

### 2. Python Environment

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### 3. Configure Credentials

Copy the example configuration files:

```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
cp .dlt/config.toml.example .dlt/config.toml
```

Edit `.dlt/secrets.toml` with your actual API keys and database credentials.

**Important**: `.dlt/secrets.toml` is gitignored and should never be committed.

### 4. Run the Pipeline

```bash
python run_pipeline.py
```

## Project Structure

```
culk-analytics/
├── ingestion/          # Data source extraction scripts (one per source)
├── database/           # PostgreSQL initialization scripts
├── .dlt/               # dlt configuration and secrets
├── logs/               # Runtime logs
├── docs/               # Project documentation
├── tests/              # Data quality tests
├── requirements.txt    # Python dependencies
├── run_pipeline.py     # Main orchestration script
└── README.md           # This file
```

## Development Workflow

1. Implement extraction logic for each source in `ingestion/`
2. Test individual source loads: `python ingestion/shopify.py`
3. Build SQL transformations in `staging` and `analytics` schemas
4. Orchestrate full pipeline runs via `run_pipeline.py`

## Implementation Status

### Phase 1: Infrastructure ✅ COMPLETE
- ✅ Project structure created
- ✅ Database initialized (PostgreSQL with 3-layer schema)
- ✅ Configuration templates ready (.dlt/secrets.toml.example, config.toml.example)
- ✅ Python dependencies defined (requirements.txt)
- ✅ Testing framework configured (pytest with shared fixtures)

### Phase 2: Data Extraction ⏳ IN PROGRESS
**Completed Sources:**
- ✅ **Shopify** (B2B + DTC commerce hub) - Custom GraphQL with cost monitoring, 4 resources (orders, products, customers, inventory)
- ✅ **Faire** (Wholesale orders) - dlt REST API client with dual auth headers, 2 resources with auto-nested normalization
- ✅ **ShipHero** (3PL fulfillment) - Custom GraphQL with complexity monitoring, 2 resources (products, orders)

**Remaining Sources:**
- 🗓️ Loop Returns (return tracking)
- 🗓️ Meta/Facebook Ads (ad spend & performance)
- 🗓️ Google Ads (ad spend & performance)
- 🗓️ Airtable (product master data)

### Phase 3: SQL Transformations 🗓️ TODO
- Build SQL views/tables in `staging` schema
- Join data across sources (e.g., Shopify orders + ShipHero shipments)
- Create calculated fields (margins, ROAS, inventory turnover)
- Build dimensional models in `analytics` schema
- Create business metrics and aggregations

### Phase 4: Orchestration & Monitoring 🗓️ TODO
- Add scheduling (Airflow/Prefect/Dagster)
- Implement data quality checks (Great Expectations or Soda)
- Set up alerting for pipeline failures
- Add monitoring for data freshness and row counts

### Phase 5: Production Readiness 🗓️ TODO
- Migrate to cloud warehouse (Snowflake/BigQuery/Redshift) - optional
- Implement secrets management (AWS Secrets Manager/HashiCorp Vault)
- Add read-only users for BI tools
- Configure connection pooling and performance tuning
- Implement data retention policies

## Resources

- [dlt Documentation](https://dlthub.com/docs)
- [dlt REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- Project Architecture: `docs/ARCHITECTURE.md`
