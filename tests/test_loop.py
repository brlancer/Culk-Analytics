"""
Loop Returns Data Quality Tests

Validates loaded data in loop_returns_raw schema.
Focus: Core return fields and label postage rates.
"""

import pytest


def test_returns_table_exists(db_cursor):
    """Verify returns table was created by dlt."""
    db_cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'loop_returns_raw' 
            AND table_name = 'returns'
        );
    """)
    assert db_cursor.fetchone()[0], "returns table does not exist"


def test_returns_has_data(db_cursor):
    """Verify returns table has records."""
    db_cursor.execute("SELECT COUNT(*) FROM loop_returns_raw.returns;")
    count = db_cursor.fetchone()[0]
    assert count > 0, f"returns table is empty"


def test_returns_core_columns(db_cursor):
    """Verify required columns exist in returns table."""
    required_columns = [
        'id',
        'state',
        'created_at',
        'updated_at',
        'order_id',
        'outcome',
        'carrier',
        'tracking_number',
        'label_rate',
        'label_status'
    ]
    
    db_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'loop_returns_raw' 
        AND table_name = 'returns';
    """)
    
    existing_columns = {row[0] for row in db_cursor.fetchall()}
    
    for col in required_columns:
        assert col in existing_columns, f"Missing column: {col}"


def test_returns_valid_states(db_cursor):
    """Verify return states match Loop's documented values."""
    valid_states = ['open', 'closed', 'cancelled', 'expired', 'review']
    
    db_cursor.execute("""
        SELECT DISTINCT state 
        FROM loop_returns_raw.returns 
        WHERE state IS NOT NULL;
    """)
    
    states = {row[0] for row in db_cursor.fetchall()}
    invalid_states = states - set(valid_states)
    
    assert len(invalid_states) == 0, f"Invalid states found: {invalid_states}"


def test_returns_valid_outcomes(db_cursor):
    """Verify return outcomes match Loop's documented values."""
    valid_outcomes = ['exchange', 'upsell', 'refund', 'credit', 'exchange+refund', 'exchange+credit', 'credit+refund']
    
    db_cursor.execute("""
        SELECT DISTINCT outcome 
        FROM loop_returns_raw.returns 
        WHERE outcome IS NOT NULL;
    """)
    
    outcomes = {row[0] for row in db_cursor.fetchall()}
    invalid_outcomes = outcomes - set(valid_outcomes)
    
    assert len(invalid_outcomes) == 0, f"Invalid outcomes found: {invalid_outcomes}"


def test_returns_non_null_ids(db_cursor):
    """Verify all returns have non-null IDs."""
    db_cursor.execute("""
        SELECT COUNT(*) 
        FROM loop_returns_raw.returns 
        WHERE id IS NULL;
    """)
    
    null_count = db_cursor.fetchone()[0]
    assert null_count == 0, f"Found {null_count} returns with null ID"


def test_returns_label_rates_positive(db_cursor):
    """Verify label rates are positive when present (key metric for postage costs)."""
    db_cursor.execute("""
        SELECT COUNT(*) 
        FROM loop_returns_raw.returns 
        WHERE label_rate IS NOT NULL 
        AND label_rate <= 0;
    """)
    
    invalid_count = db_cursor.fetchone()[0]
    assert invalid_count == 0, f"Found {invalid_count} returns with non-positive label_rate"


def test_returns_labels_child_table_exists(db_cursor):
    """Verify labels child table was created."""
    db_cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'loop_returns_raw' 
            AND table_name = 'returns__labels'
        );
    """)
    assert db_cursor.fetchone()[0], "returns__labels child table does not exist"


def test_returns_labels_foreign_keys(db_cursor):
    """Verify labels have valid foreign keys to parent returns."""
    db_cursor.execute("""
        SELECT COUNT(*) 
        FROM loop_returns_raw.returns__labels l
        LEFT JOIN loop_returns_raw.returns r ON l._dlt_parent_id = r._dlt_id
        WHERE r._dlt_id IS NULL;
    """)
    
    orphan_count = db_cursor.fetchone()[0]
    assert orphan_count == 0, f"Found {orphan_count} orphaned labels without parent return"


def test_returns_line_items_child_table_exists(db_cursor):
    """Verify line_items child table was created."""
    db_cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'loop_returns_raw' 
            AND table_name = 'returns__line_items'
        );
    """)
    assert db_cursor.fetchone()[0], "returns__line_items child table does not exist"


def test_returns_exchanges_child_table_exists(db_cursor):
    """Verify exchanges child table was created."""
    db_cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'loop_returns_raw' 
            AND table_name = 'returns__exchanges'
        );
    """)
    assert db_cursor.fetchone()[0], "returns__exchanges child table does not exist"


def test_returns_pii_customer_excluded(db_cursor):
    """Verify customer email (PII) is excluded from database."""
    db_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'loop_returns_raw' 
        AND table_name = 'returns'
        AND column_name = 'customer';
    """)
    
    result = db_cursor.fetchone()
    assert result is None, "PII field 'customer' should not exist in database"


def test_returns_pii_status_page_url_excluded(db_cursor):
    """Verify status_page_url (unique customer tracking URL) is excluded from database."""
    db_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'loop_returns_raw' 
        AND table_name = 'returns'
        AND column_name = 'status_page_url';
    """)
    
    result = db_cursor.fetchone()
    assert result is None, "PII field 'status_page_url' should not exist in database"


def test_returns_line_items_pii_excluded(db_cursor):
    """Verify no PII fields exist in line_items child table."""
    pii_fields = ['customer', 'status_page_url', 'address', 'phone', 'email']
    
    db_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'loop_returns_raw' 
        AND table_name = 'returns__line_items';
    """)
    
    existing_columns = {row[0] for row in db_cursor.fetchall()}
    found_pii = [field for field in pii_fields if field in existing_columns]
    
    assert len(found_pii) == 0, f"PII fields found in line_items: {found_pii}"


def test_returns_labels_pii_excluded(db_cursor):
    """Verify no PII fields exist in labels child table."""
    pii_fields = ['customer', 'address', 'phone', 'email', 'qr_code_url']
    
    db_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'loop_returns_raw' 
        AND table_name = 'returns__labels';
    """)
    
    existing_columns = {row[0] for row in db_cursor.fetchall()}
    found_pii = [field for field in pii_fields if field in existing_columns]
    
    assert len(found_pii) == 0, f"PII fields found in labels: {found_pii}"