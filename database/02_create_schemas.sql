-- Create schemas for the ELT pipeline
-- Connect to culk_db before running this script

-- ============================================================================
-- IMPORTANT: This file creates the schema STRUCTURE only (containers)
-- 
-- You do NOT need to update this file as you add data sources!
-- 
-- How tables get created:
-- 1. Extract/Load Phase: dlt automatically creates tables in 'public' schema
--    - No manual SQL table creation needed
--    - dlt infers schema from API data structure
--    - dlt handles schema evolution automatically
-- 
-- 2. Transform Phase (future): You'll create NEW SQL files for views/tables
--    in 'staging' and 'analytics' schemas
--    - Those files live in a separate transforms/ directory
--    - They reference the dlt-created tables in 'public'
-- ============================================================================

-- Schema: public
-- Used by dlt for automatic loading of raw data
-- dlt will auto-create tables here with inferred schemas
COMMENT ON SCHEMA public IS 'Raw data loaded by dlt - auto-generated tables';

-- Schema: staging
-- For intermediate transformations and data cleaning
CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS 'Staging layer for SQL transformations - cleaned and joined data';

-- Schema: analytics
-- For final analytical models and aggregations
CREATE SCHEMA IF NOT EXISTS analytics;
COMMENT ON SCHEMA analytics IS 'Analytics layer - final business logic and metrics';

-- Display created schemas
\dn
