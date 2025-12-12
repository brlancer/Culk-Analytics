# Culk Analytics

An ELT (Extract-Load-Transform) data pipeline for apparel analytics, consolidating data from multiple sales channels, fulfillment, returns, advertising, and product master sources.

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

### Why ELT?
- Load raw data first, transform later in SQL (closer to the data)
- dlt automates schema evolution and handles API pagination/state
- Separation of concerns: extraction logic vs. transformation logic

## Data Sources

- Shopify (DTC sales)
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
├── requirements.txt    # Python dependencies
├── run_pipeline.py     # Main orchestration script
└── README.md           # This file
```

## Development Workflow

1. Implement extraction logic for each source in `ingestion/`
2. Test individual source loads: `python ingestion/shopify.py`
3. Build SQL transformations in `staging` and `analytics` schemas
4. Orchestrate full pipeline runs via `run_pipeline.py`

## Next Steps (Phase 2+)

- [ ] Implement API extraction logic for each source
- [ ] Add incremental loading strategies (state management)
- [ ] Build SQL transformation layer (staging → analytics)
- [ ] Add data quality checks and testing
- [ ] Implement scheduling (cron/Airflow/Prefect)
- [ ] Add monitoring and alerting

## Resources

- [dlt Documentation](https://dlthub.com/docs)
- [dlt REST API Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api)
- Project Architecture: `docs/ARCHITECTURE.md`
