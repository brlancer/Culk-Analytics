# Database Setup

This directory contains initialization scripts for the Culk Analytics PostgreSQL database.

## Prerequisites

- PostgreSQL 13+ installed and running
- Access to a PostgreSQL superuser account (usually `postgres`)

## Quick Start

### macOS / Linux

```bash
# Make the init script executable (first time only)
chmod +x init_db.sh

# Run the initialization
./init_db.sh
```

The script will prompt for the PostgreSQL password if needed.

### Windows

Run each SQL file manually using `psql`:

```cmd
psql -U postgres -d postgres -f 01_create_database.sql
psql -U postgres -d culk_db -f 02_create_schemas.sql
psql -U postgres -d culk_db -f 03_create_user.sql
```

## What Gets Created

### Database: `culk_db`
- Main data warehouse database
- UTF-8 encoding for international character support

### Schemas
1. **public** (default): Raw data loaded by dlt
   - dlt automatically creates tables here
   - Schema evolution handled by dlt
   
2. **staging**: Intermediate transformations
   - Data cleaning and normalization
   - Joining raw tables
   - Calculated fields
   
3. **analytics**: Final analytical models
   - Business metrics and KPIs
   - Aggregated views
   - Reporting tables

### User (optional)
- `culk_analyst`: Dedicated pipeline user
- Commented out by default in `03_create_user.sql`
- Uncomment and set a strong password for production

## Connection Details

### Connection String Format

```
postgresql://[user]:[password]@[host]:[port]/culk_db
```

### Local Development (default)
```
postgresql://postgres:your_password@localhost:5432/culk_db
```

### With Custom User
```
postgresql://culk_analyst:your_password@localhost:5432/culk_db
```

## Manual Verification

Connect to the database and verify setup:

```bash
psql -U postgres -d culk_db
```

Then run:

```sql
-- List all schemas
\dn

-- List tables (will be empty initially)
\dt public.*
\dt staging.*
\dt analytics.*

-- Check current database
SELECT current_database();
```

## Troubleshooting

### "database already exists" error
The database was already created. You can either:
- Drop it first: `DROP DATABASE culk_db;` (⚠️ destroys all data)
- Skip `01_create_database.sql` and run only `02_create_schemas.sql`

### Connection refused
- Verify PostgreSQL is running: `pg_isready`
- Check PostgreSQL port: default is 5432
- Verify `pg_hba.conf` allows local connections

### Permission denied
- Ensure you're running as a PostgreSQL superuser (usually `postgres`)
- For production, create a dedicated user with appropriate permissions

## Next Steps

After database initialization:
1. Update `.dlt/secrets.toml` with the connection string
2. Run `python run_pipeline.py` to start loading data
3. dlt will automatically create tables in the `public` schema
