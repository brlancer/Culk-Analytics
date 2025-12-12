-- Create the main database for Culk Analytics
-- Run this as the postgres superuser

-- Drop if exists (careful in production!)
-- DROP DATABASE IF EXISTS culk_db;

CREATE DATABASE culk_db
    WITH
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TEMPLATE = template0;

COMMENT ON DATABASE culk_db IS 'Culk Analytics data warehouse';
