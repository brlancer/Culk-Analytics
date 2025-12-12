-- Optional: Create a dedicated user for the analytics pipeline
-- Uncomment and customize as needed for production environments

/*
-- Create user
CREATE USER culk_analyst WITH PASSWORD 'CHANGE_ME_IN_PRODUCTION';

-- Grant database access
GRANT CONNECT ON DATABASE culk_db TO culk_analyst;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO culk_analyst;
GRANT USAGE ON SCHEMA staging TO culk_analyst;
GRANT USAGE ON SCHEMA analytics TO culk_analyst;

-- Grant table permissions (for existing and future tables)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO culk_analyst;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO culk_analyst;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analytics TO culk_analyst;

-- Grant default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO culk_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO culk_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO culk_analyst;

-- Grant sequence privileges (for auto-incrementing IDs)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO culk_analyst;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO culk_analyst;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA analytics TO culk_analyst;

-- Display user info
\du culk_analyst
*/

-- User creation is commented out by default
-- For local development, using the postgres superuser is typically sufficient
SELECT 'User creation skipped - using default postgres user' AS notice;
