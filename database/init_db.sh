#!/bin/bash

# Database initialization script for Culk Analytics
# Runs all SQL files in sequence to set up the PostgreSQL database

set -e  # Exit on error

echo "Initializing Culk Analytics database..."

# Default PostgreSQL connection (modify if needed)
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"

echo "Connecting to PostgreSQL at $DB_HOST:$DB_PORT as user $DB_USER"

# Run each SQL file in order
echo "Creating database..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -f 01_create_database.sql

echo "Creating schemas..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d culk_db -f 02_create_schemas.sql

echo "Setting up user permissions (if enabled)..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d culk_db -f 03_create_user.sql

echo "Database initialization complete!"
echo ""
echo "Connection string:"
echo "postgresql://$DB_USER@$DB_HOST:$DB_PORT/culk_db"
