"""
Data quality tests for Shopify extraction pipeline.
Tests validate schema, data integrity, and business rules.
"""
import pytest
from datetime import datetime, timedelta


class TestShopifyOrders:
    """Tests for orders table data quality."""
    
    def test_orders_table_exists(self, db_cursor):
        """Verify orders table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shopify_raw' 
                AND table_name = 'shopify_orders_resource'
            );
        """)
        assert db_cursor.fetchone()[0], "shopify_orders_resource table does not exist"
    
    def test_orders_has_data(self, db_cursor):
        """Verify orders table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM shopify_raw.shopify_orders_resource;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "shopify_orders_resource table is empty"
    
    def test_orders_no_null_ids(self, db_cursor):
        """Verify no orders have null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shopify_raw.shopify_orders_resource 
            WHERE id IS NULL;
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} orders with null IDs"
    
    def test_orders_valid_financial_status(self, db_cursor):
        """Verify financial status values are valid."""
        db_cursor.execute("""
            SELECT DISTINCT financial_status 
            FROM shopify_raw.shopify_orders_resource 
            WHERE financial_status IS NOT NULL;
        """)
        statuses = {row[0] for row in db_cursor.fetchall()}
        valid_statuses = {
            'PENDING', 'AUTHORIZED', 'PARTIALLY_PAID', 'PAID',
            'PARTIALLY_REFUNDED', 'REFUNDED', 'VOIDED'
        }
        invalid = statuses - valid_statuses
        assert not invalid, f"Found invalid financial statuses: {invalid}"
    
    def test_orders_positive_prices(self, db_cursor):
        """Verify order prices are non-negative."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shopify_raw.shopify_orders_resource 
            WHERE CAST(total_price AS NUMERIC) < 0;
        """)
        negative_count = db_cursor.fetchone()[0]
        assert negative_count == 0, f"Found {negative_count} orders with negative prices"
    
    def test_line_items_table_exists(self, db_cursor):
        """Verify line items table was created."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shopify_raw' 
                AND table_name = 'shopify_orders_resource__line_items'
            );
        """)
        assert db_cursor.fetchone()[0], "line items table does not exist"


class TestShopifyProducts:
    """Tests for products and variants data quality."""
    
    def test_products_table_exists(self, db_cursor):
        """Verify products table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shopify_raw' 
                AND table_name = 'shopify_products_resource'
            );
        """)
        assert db_cursor.fetchone()[0], "shopify_products_resource table does not exist"
    
    def test_products_has_data(self, db_cursor):
        """Verify products table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM shopify_raw.shopify_products_resource;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "shopify_products_resource table is empty"
    
    def test_products_have_titles(self, db_cursor):
        """Verify all products have titles."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shopify_raw.shopify_products_resource 
            WHERE title IS NULL OR TRIM(title) = '';
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} products without titles"
    
    def test_variants_table_exists(self, db_cursor):
        """Verify variants table was created."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shopify_raw' 
                AND table_name = 'shopify_products_resource__variants'
            );
        """)
        assert db_cursor.fetchone()[0], "variants table does not exist"
    
    def test_variants_positive_prices(self, db_cursor):
        """Verify variant prices are non-negative."""
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM shopify_raw.shopify_products_resource__variants
            WHERE CAST(price AS NUMERIC) < 0;
        """)
        negative_count = db_cursor.fetchone()[0]
        assert negative_count == 0, f"Found {negative_count} variants with negative prices"


class TestShopifyCustomers:
    """Tests for customers table data quality and privacy."""
    
    def test_customers_table_exists(self, db_cursor):
        """Verify customers table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shopify_raw' 
                AND table_name = 'shopify_customers_resource'
            );
        """)
        assert db_cursor.fetchone()[0], "shopify_customers_resource table does not exist"
    
    def test_customers_has_data(self, db_cursor):
        """Verify customers table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM shopify_raw.shopify_customers_resource;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "shopify_customers_resource table is empty"
    
    def test_no_pii_columns(self, db_cursor):
        """Verify no PII columns exist (privacy check)."""
        db_cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'shopify_raw' 
              AND table_name = 'shopify_customers_resource'
              AND column_name IN ('email', 'phone', 'first_name', 'last_name', 
                                  'default_address', 'addresses')
        """)
        pii_columns = db_cursor.fetchall()
        assert len(pii_columns) == 0, f"Found PII columns: {[col[0] for col in pii_columns]}"


class TestShopifyInventory:
    """Tests for inventory levels data quality."""
    
    def test_inventory_table_exists(self, db_cursor):
        """Verify inventory table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shopify_raw' 
                AND table_name = 'shopify_inventory_resource'
            );
        """)
        assert db_cursor.fetchone()[0], "shopify_inventory_resource table does not exist"
    
    def test_inventory_has_data(self, db_cursor):
        """Verify inventory table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM shopify_raw.shopify_inventory_resource;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "shopify_inventory_resource table is empty"
    
    def test_inventory_reasonable_range(self, db_cursor):
        """Verify available quantities are within reasonable range."""
        # Allow negative for backorders, but check for extreme outliers
        db_cursor.execute("""
            SELECT COUNT(*) FROM shopify_raw.shopify_inventory_resource 
            WHERE available < -10000 OR available > 1000000;
        """)
        extreme_count = db_cursor.fetchone()[0]
        assert extreme_count == 0, f"Found {extreme_count} inventory records with extreme quantities"

    def test_inventory_negative_count(self, db_cursor):
        """Document negative inventory (backorders are expected)."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shopify_raw.shopify_inventory_resource 
            WHERE available < 0;
        """)
        negative_count = db_cursor.fetchone()[0]
        # This is informational - negative inventory means backorders
        print(f"\nINFO: {negative_count} inventory records are negative (backorders/oversold)")


class TestDataFreshness:
    """Tests for data recency and pipeline health."""
    
    def test_orders_recent_data(self, db_cursor):
        """Verify orders data is recent (within 7 days)."""
        db_cursor.execute("""
            SELECT MAX(to_timestamp(_dlt_load_id::double precision)) as last_load
            FROM shopify_raw.shopify_orders_resource;
        """)
        last_load = db_cursor.fetchone()[0]
        if last_load:
            days_old = (datetime.now(last_load.tzinfo) - last_load).days
            assert days_old <= 7, f"Orders data is {days_old} days old (max 7 allowed)"
    
    def test_products_recent_data(self, db_cursor):
        """Verify products data is recent (within 7 days)."""
        db_cursor.execute("""
            SELECT MAX(to_timestamp(_dlt_load_id::double precision)) as last_load
            FROM shopify_raw.shopify_products_resource;
        """)
        last_load = db_cursor.fetchone()[0]
        if last_load:
            days_old = (datetime.now(last_load.tzinfo) - last_load).days
            assert days_old <= 7, f"Products data is {days_old} days old (max 7 allowed)"
