# Culk Analytics - Quick Start Guide

## 📁 Project Structure

```
culk-analytics/
├── 📂 .dlt/                    # dlt configuration
│   ├── config.toml.example     # Non-secret settings template
│   └── secrets.toml.example    # API credentials template
├── 📂 database/                # PostgreSQL setup
│   ├── init_db.sh             # Initialization script (executable)
│   ├── 01_create_database.sql # Creates culk_db database
│   ├── 02_create_schemas.sql  # Creates public, staging, analytics schemas
│   ├── 03_create_user.sql     # Optional user setup
│   └── README.md              # Database setup instructions
├── 📂 docs/                    # Documentation
│   ├── ARCHITECTURE.md        # ELT architecture overview
│   └── DATA_SOURCES.md        # API details and rate limits
├── 📂 ingestion/               # Data source extraction scripts
│   ├── shopify.py             # ✅ Shopify commerce hub (B2B + DTC) (GraphQL)
│   ├── faire.py               # ✅ Faire wholesale (REST)
│   ├── shiphero.py            # ✅ ShipHero 3PL (GraphQL)
│   ├── loop_returns.py        # 🗓️ Loop Returns (REST)
│   ├── meta_ads.py            # 🗓️ Meta/Facebook Ads (Graph API)
│   ├── google_ads.py          # 🗓️ Google Ads (REST)
│   └── airtable.py            # 🗓️ Airtable product master (REST)
├── 📂 logs/                    # Runtime logs (empty, git tracked)
│   └── .gitkeep
├── .gitignore                  # Excludes secrets, logs, cache
├── README.md                   # Main project documentation
├── requirements.txt            # Python dependencies
└── run_pipeline.py             # Main orchestration script
```

## 🚀 Quick Setup (5 minutes)

### 1. Database Setup
```bash
cd database
./init_db.sh
```

### 2. Python Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configuration
```bash
cp .dlt/secrets.toml.example .dlt/secrets.toml
cp .dlt/config.toml.example .dlt/config.toml
# Edit .dlt/secrets.toml with your API keys
```

### 4. Run Pipeline
```bash
python run_pipeline.py
```

## 📊 Data Sources (7 Total)

| Source | Type | File | Status |
|--------|------|------|--------|
| Shopify (Commerce Hub: B2B + DTC) | GraphQL | `ingestion/shopify.py` | ✅ Complete |
| Faire (Wholesale) | REST | `ingestion/faire.py` | ✅ Complete |
| ShipHero (3PL) | GraphQL | `ingestion/shiphero.py` | ✅ Complete |
| Loop Returns | REST | `ingestion/loop_returns.py` | 🗓️ TODO |
| Meta Ads | Graph API | `ingestion/meta_ads.py` | 🗓️ TODO |
| Google Ads | REST | `ingestion/google_ads.py` | 🗓️ TODO |
| Airtable (Product) | REST | `ingestion/airtable.py` | 🗓️ TODO |

## 🗄️ Database Schemas

- **`public`**: Raw data loaded by dlt (auto-generated tables)
- **`staging`**: Intermediate transformations (future)
- **`analytics`**: Final business metrics (future)

## 📝 What's Implemented

✅ Complete project structure  
✅ Database initialization scripts  
✅ Configuration templates  
✅ Python extraction files with dlt pipelines
✅ **Shopify extraction** - Custom GraphQL with 4 resources (orders, products, customers, inventory)
✅ **Faire extraction** - dlt REST API client with auto-nested normalization
✅ **ShipHero extraction** - Custom GraphQL with complexity monitoring
✅ Testing framework with pytest (Shopify, Faire tests passing)
✅ Comprehensive documentation  
✅ .gitignore for secrets protection  

## 🔧 What's Next

🗓️ Complete remaining data sources (Loop, Meta Ads, Google Ads, Airtable)
🗓️ Build SQL transformation layer (staging → analytics)
🗓️ Add ShipHero tests (test_shiphero.py)
🗓️ Add data quality checks for all sources
🗓️ Set up orchestration/scheduling (Airflow/Prefect)
🗓️ Add monitoring and alerting  

## 📚 Key Documentation

- [README.md](../README.md) - Full project overview
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - ELT architecture deep dive
- [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md) - API details and rate limits
- [database/README.md](database/README.md) - Database setup guide

## 🔐 Security Notes

- ⚠️ **Never commit** `.dlt/secrets.toml`
- ⚠️ All credentials use placeholder values
- ⚠️ Production: Use environment variables or secret managers

## 💡 Tips

- Test sources individually: `python ingestion/shopify.py`
- Check logs in `logs/` directory after each run
- Start with one source, then expand
- Monitor API rate limits (see DATA_SOURCES.md)

---

**Ready to start building!** All infrastructure is in place for Phase 1. 🎉
