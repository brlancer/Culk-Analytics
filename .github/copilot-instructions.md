# Culk Analytics - AI Agent Instructions

## Project Overview
Apparel analytics ELT data pipeline consolidating B2B/DTC commerce, 3PL fulfillment, returns, advertising, and product master data into PostgreSQL. **ELT architecture**: Extract (Python) → Load (dlt library) → Transform (SQL, future phase).

## Tech Stack & Key Libraries
- **Python 3.10+** with **dlt[postgres]** (data load tool): Auto schema inference, normalization, incremental state
- **PostgreSQL**: Local data warehouse with 3-layer schema: `public` (raw dlt loads), `staging`, `analytics`
- **aiohttp/requests**: Async API extraction (GraphQL/REST)
- **pytest**: Data quality tests with shared fixtures in `tests/conftest.py`

## Critical Workflows

### Database Initialization
```bash
cd database && ./init_db.sh  # Creates culk_db, schemas, user
```
**Key point**: Schema structure is fixed—dlt auto-creates tables in `public`. Never manually add table DDL to `database/` scripts.

### Running Pipelines
```bash
python ingestion/shopify.py          # Test individual source
python run_pipeline.py                # Full orchestration (future)
pytest tests/test_shopify.py          # Data quality tests
```

### Credentials Setup
- Copy `.dlt/secrets.toml.example` → `.dlt/secrets.toml` (gitignored)
- Copy `.dlt/config.toml.example` → `.dlt/config.toml`
- **Never commit** `.dlt/secrets.toml` or hardcode API keys

## Architecture Patterns

### dlt Resource Pattern (Standard Extraction)
All ingestion scripts follow this structure:
```python
import dlt
import aiohttp

@dlt.resource(write_disposition="merge", primary_key="id")
async def resource_name(updated_at_min: str = "2024-01-01T00:00:00Z"):
    """Fetch data with pagination, yield flattened dicts."""
    cursor = None
    has_next_page = True
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            data = await fetch_api(cursor, session)
            flattened = flatten_response(data)
            for item in flattened:
                yield item
            # Update pagination cursor

@dlt.source
def source_name():
    return [resource_name(), other_resource()]

def load_to_postgres():
    pipeline = dlt.pipeline(
        pipeline_name="source_name",
        destination="postgres",
        dataset_name="source_name_raw"
    )
    pipeline.run(source_name())
```

### GraphQL vs REST Extraction
- **GraphQL** (Shopify, ShipHero): Custom queries with `aiohttp`, flatten `edges/nodes` structures, monitor complexity/cost
- **REST** (Faire, Loop, Airtable, Ads): Use dlt REST API client for auto-pagination (future implementation) OR custom `requests`/`aiohttp`

### Data Source Specifics
- **Shopify**: GraphQL with cost-based rate limits (1000 points, restore 50/sec). Check `extensions.cost` in responses. Main source of truth for orders.
- **ShipHero**: GraphQL with 50k complexity/hour limit—**aggressive pagination hits limits fast**. OAuth tokens expire; implement refresh flow.
- **Faire**: REST API, 1000 req/hour. Wholesale orders that sync into Shopify.

## Project Conventions

### File Organization
- `ingestion/{source}.py`: One file per data source. Each has `extract()`, `load_to_postgres()`, and `if __name__ == "__main__"` for testing
- `database/`: Schema structure only—**do not add table DDL here**. Future SQL transforms go in separate `transforms/` directory
- `docs/`: `ARCHITECTURE.md` (diagrams), `DATA_SOURCES.md` (API specs), `README.md` (setup)

### Testing Pattern
Tests in `tests/` validate loaded data in PostgreSQL:
- Table existence, data presence, schema constraints
- Business rules (valid enum values, positive prices, non-null IDs)
- Use `conftest.py` fixtures: `db_connection`, `db_cursor` with auto-rollback
- Target schema: `{source}_raw.{resource}_resource` (e.g., `shopify_raw.shopify_orders_resource`)

### Incremental Loading
- Use `updated_at_min` filters in API queries for incremental extraction
- dlt manages state automatically via `.dlt/.sources` (gitignored)
- Primary keys in `@dlt.resource()` enable merge mode (upsert)

## Critical Developer Notes

1. **Schema Evolution**: dlt handles this automatically—new API fields become new columns. Don't fight it.
2. **Rate Limit Monitoring**: Always log and check rate limit headers. ShipHero complexity is especially tight.
3. **Flatten Before Load**: GraphQL nested structures (edges/nodes) must be flattened to dicts before yielding to dlt.
4. **Test After Changes**: Run source-specific tests (`pytest tests/test_shopify.py`) after modifying ingestion logic.
5. **SQL Transforms**: NOT in `database/`—create separate `transforms/` directory when building staging/analytics SQL.

## Common Pitfalls
- ❌ Adding table DDL to `database/02_create_schemas.sql`—dlt creates tables automatically
- ❌ Forgetting `write_disposition="merge"` and `primary_key` in `@dlt.resource()`—causes duplicates
- ❌ Not flattening GraphQL responses—dlt can't handle deeply nested structures well
- ❌ Ignoring rate limit response headers—leads to 429 errors and pipeline failures
- ❌ Hardcoding credentials instead of using dlt secrets (`dlt.secrets["sources.shopify.access_token"]`)

## Key Files to Reference
- [ingestion/shopify.py](ingestion/shopify.py): Complete GraphQL extraction pattern with flattening, pagination, cost monitoring
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): ELT flow diagrams and layer responsibilities
- [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md): API specs, rate limits, auth methods for all 7 sources
- [tests/test_shopify.py](tests/test_shopify.py): Data quality test patterns
