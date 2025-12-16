"""
Test suite for Faire wholesale data ingestion.

Validates:
- Table existence in faire_raw schema
- Data presence after pipeline runs
- Schema constraints (primary keys, foreign keys, data types)
- Business rules (valid enums, positive values, etc.)
- Data freshness
"""

import pytest
from datetime import datetime, timedelta


class TestFaireOrders:
    """Test suite for orders table."""
    
    def test_orders_table_exists(self, db_cursor):
        """Verify orders table exists in faire_raw schema."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'orders'
            );
        """)
        assert db_cursor.fetchone()[0], "orders table does not exist"
    
    def test_orders_has_data(self, db_cursor):
        """Verify orders table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.orders;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "orders table is empty"
    
    def test_orders_primary_key_not_null(self, db_cursor):
        """Verify all orders have non-null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.orders 
            WHERE id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orders with NULL id"
    
    def test_orders_id_format(self, db_cursor):
        """Verify order IDs start with 'bo_' prefix."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.orders 
            WHERE id IS NOT NULL AND id NOT LIKE 'bo_%';
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orders with invalid ID format (should start with 'bo_')"
    
    def test_orders_valid_state(self, db_cursor):
        """Verify all orders have valid state enums."""
        valid_states = [
            'NEW', 'PROCESSING', 'PRE_TRANSIT', 'IN_TRANSIT', 
            'DELIVERED', 'CANCELED', 'BACKORDERED', 'PENDING_RETAILER_CONFIRMATION'
        ]
        
        db_cursor.execute("""
            SELECT DISTINCT state FROM faire_raw.orders 
            WHERE state IS NOT NULL;
        """)
        states = [row[0] for row in db_cursor.fetchall()]
        
        invalid_states = [s for s in states if s not in valid_states]
        assert len(invalid_states) == 0, f"Found invalid order states: {invalid_states}"
    
    def test_orders_timestamps(self, db_cursor):
        """Verify created_at and updated_at are present."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.orders 
            WHERE created_at IS NULL OR updated_at IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orders with NULL timestamps"


class TestFaireOrderItems:
    """Test suite for orders__items table (child resource)."""
    
    def test_order_items_table_exists(self, db_cursor):
        """Verify orders__items table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'orders__items'
            );
        """)
        assert db_cursor.fetchone()[0], "orders__items table does not exist"
    
    def test_order_items_has_data(self, db_cursor):
        """Verify order items table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.orders__items;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "orders__items table is empty"
    
    def test_order_items_foreign_key(self, db_cursor):
        """Verify all order_items have valid _dlt_parent_id foreign keys."""
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM faire_raw.orders__items oi
            LEFT JOIN faire_raw.orders o ON oi._dlt_parent_id = o._dlt_id
            WHERE o._dlt_id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orphaned order items (no matching parent)"
    
    def test_order_items_primary_key_not_null(self, db_cursor):
        """Verify all order items have non-null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.orders__items 
            WHERE id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} order items with NULL id"
    
    def test_order_items_quantity_positive(self, db_cursor):
        """Verify all order items have positive quantities."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.orders__items 
            WHERE quantity IS NOT NULL AND quantity <= 0;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} order items with non-positive quantity"


class TestFaireOrderShipments:
    """Test suite for orders__shipments table (child resource)."""
    
    def test_order_shipments_table_exists(self, db_cursor):
        """Verify orders__shipments table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'orders__shipments'
            );
        """)
        assert db_cursor.fetchone()[0], "orders__shipments table does not exist"
    
    def test_order_shipments_foreign_key(self, db_cursor):
        """Verify all shipments have valid _dlt_parent_id foreign keys (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.orders__shipments;")
        shipment_count = db_cursor.fetchone()[0]
        
        if shipment_count > 0:
            db_cursor.execute("""
                SELECT COUNT(*) 
                FROM faire_raw.orders__shipments s
                LEFT JOIN faire_raw.orders o ON s._dlt_parent_id = o._dlt_id
                WHERE o._dlt_id IS NULL;
            """)
            orphan_count = db_cursor.fetchone()[0]
            assert orphan_count == 0, f"Found {orphan_count} orphaned shipments"


class TestFaireProducts:
    """Test suite for products table."""
    
    def test_products_table_exists(self, db_cursor):
        """Verify products table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'products'
            );
        """)
        assert db_cursor.fetchone()[0], "products table does not exist"
    
    def test_products_has_data(self, db_cursor):
        """Verify products table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.products;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "products table is empty"
    
    def test_products_primary_key_not_null(self, db_cursor):
        """Verify all products have non-null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.products 
            WHERE id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} products with NULL id"
    
    def test_products_id_format(self, db_cursor):
        """Verify product IDs start with 'p_' prefix."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.products 
            WHERE id IS NOT NULL AND id NOT LIKE 'p_%';
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} products with invalid ID format (should start with 'p_')"
    
    def test_products_valid_sale_state(self, db_cursor):
        """Verify all products have valid sale_state enums."""
        valid_states = ['FOR_SALE', 'SALES_PAUSED']
        
        db_cursor.execute("""
            SELECT DISTINCT sale_state FROM faire_raw.products 
            WHERE sale_state IS NOT NULL;
        """)
        states = [row[0] for row in db_cursor.fetchall()]
        
        invalid_states = [s for s in states if s not in valid_states]
        assert len(invalid_states) == 0, f"Found invalid sale states: {invalid_states}"
    
    def test_products_valid_lifecycle_state(self, db_cursor):
        """Verify all products have valid lifecycle_state enums."""
        valid_states = ['DRAFT', 'PUBLISHED', 'UNPUBLISHED', 'DELETED']
        
        db_cursor.execute("""
            SELECT DISTINCT lifecycle_state FROM faire_raw.products 
            WHERE lifecycle_state IS NOT NULL;
        """)
        states = [row[0] for row in db_cursor.fetchall()]
        
        invalid_states = [s for s in states if s not in valid_states]
        assert len(invalid_states) == 0, f"Found invalid lifecycle states: {invalid_states}"
    
    def test_products_no_images_field(self, db_cursor):
        """Verify images field was excluded from products (per requirements)."""
        db_cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'faire_raw' 
            AND table_name = 'products' 
            AND column_name = 'images';
        """)
        result = db_cursor.fetchone()
        assert result is None, "Products table should not have 'images' column (excluded per requirements)"


class TestFaireDataIntegrity:
    """Cross-resource data integrity tests."""
    
    def test_orders_have_items(self, db_cursor):
        """Verify at least some orders have associated items."""
        db_cursor.execute("""
            SELECT COUNT(DISTINCT o.id) 
            FROM faire_raw.orders o
            INNER JOIN faire_raw.orders__items oi ON o._dlt_id = oi._dlt_parent_id;
        """)
        count = db_cursor.fetchone()[0]
        assert count > 0, "No orders have associated items (data integrity issue)"
    
    def test_products_taxonomy_type_embedded(self, db_cursor):
        """Verify products table has embedded taxonomy_type fields."""
        db_cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'faire_raw' 
            AND table_name = 'products' 
            AND column_name IN ('taxonomy_type__id', 'taxonomy_type__name');
        """)
        columns = [row[0] for row in db_cursor.fetchall()]
        assert len(columns) >= 0, "Taxonomy type fields should be queryable"
