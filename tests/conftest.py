"""
Pytest configuration and shared fixtures for data quality tests.
"""
import pytest
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="session")
def db_connection():
    """
    Create a database connection that persists for the entire test session.
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DATABASE", "culk_db"),
        user=os.getenv("POSTGRES_USER", "brianlance"),
        password=os.getenv("POSTGRES_PASSWORD", "")
    )
    yield conn
    conn.close()


@pytest.fixture
def db_cursor(db_connection):
    """
    Create a cursor for executing queries. Rolls back after each test.
    """
    cursor = db_connection.cursor()
    yield cursor
    db_connection.rollback()
    cursor.close()
