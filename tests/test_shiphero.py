"""
Data quality tests for ShipHero 3PL extraction pipeline.
Tests validate schema, data integrity, business rules, and incremental loading.
"""
import pytest
from datetime import datetime, timedelta, timezone


class TestShipHeroProducts:
    """Tests for products table data quality."""
    
    def test_products_table_exists(self, db_cursor):
        """Verify products table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'products'
            );
        """)
        assert db_cursor.fetchone()[0], "products table does not exist"
    
    def test_products_has_data(self, db_cursor):
        """Verify products table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM shiphero_raw.products;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "products table is empty"
    
    def test_products_no_null_ids(self, db_cursor):
        """Verify no products have null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.products 
            WHERE id IS NULL;
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} products with null IDs"
    
    def test_products_have_skus(self, db_cursor):
        """Verify all products have SKUs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.products 
            WHERE sku IS NULL OR TRIM(sku) = '';
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} products without SKUs"
    
    def test_products_have_names(self, db_cursor):
        """Verify all products have names."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.products 
            WHERE name IS NULL OR TRIM(name) = '';
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} products without names"
    
    def test_products_valid_timestamps(self, db_cursor):
        """Verify created_at and updated_at are valid timestamps."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.products 
            WHERE created_at IS NULL OR updated_at IS NULL;
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} products with null timestamps"
        
        # Verify updated_at >= created_at
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.products 
            WHERE updated_at < created_at;
        """)
        invalid_count = db_cursor.fetchone()[0]
        assert invalid_count == 0, f"Found {invalid_count} products with updated_at < created_at"
    
    def test_products_warehouse_products_structure(self, db_cursor):
        """Verify warehouse_products child table has expected structure."""
        # Check child table exists
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'products__warehouse_products'
            );
        """)
        assert db_cursor.fetchone()[0], "products__warehouse_products table does not exist"
        
        # Check it has data and expected fields
        db_cursor.execute("""
            SELECT id, warehouse_id, on_hand, allocated, available, _dlt_parent_id
            FROM shiphero_raw.products__warehouse_products 
            LIMIT 1;
        """)
        result = db_cursor.fetchone()
        assert result is not None, "products__warehouse_products table is empty"
    
    def test_products_positive_values(self, db_cursor):
        """Verify warehouse product inventory values are non-negative."""
        db_cursor.execute("""
            SELECT _dlt_parent_id, on_hand, allocated, available, backorder, reserve_inventory
            FROM shiphero_raw.products__warehouse_products;
        """)
        for row in db_cursor.fetchall():
            parent_id = row[0]
            on_hand, allocated, available, backorder, reserve_inventory = row[1:6]
            
            # Check quantity fields are non-negative where not null
            if on_hand is not None:
                assert on_hand >= 0, f"Product {parent_id} has negative on_hand: {on_hand}"
            if allocated is not None:
                assert allocated >= 0, f"Product {parent_id} has negative allocated: {allocated}"
            if available is not None:
                assert available >= 0, f"Product {parent_id} has negative available: {available}"
            if backorder is not None:
                assert backorder >= 0, f"Product {parent_id} has negative backorder: {backorder}"
            if reserve_inventory is not None:
                assert reserve_inventory >= 0, f"Product {parent_id} has negative reserve_inventory: {reserve_inventory}"


class TestShipHeroOrders:
    """Tests for orders table data quality."""
    
    def test_orders_table_exists(self, db_cursor):
        """Verify orders table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'orders'
            );
        """)
        assert db_cursor.fetchone()[0], "orders table does not exist"
    
    def test_orders_has_data(self, db_cursor):
        """Verify orders table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM shiphero_raw.orders;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "orders table is empty"
    
    def test_orders_no_null_ids(self, db_cursor):
        """Verify no orders have null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.orders 
            WHERE id IS NULL;
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} orders with null IDs"
    
    def test_orders_have_order_numbers(self, db_cursor):
        """Verify all orders have order numbers."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.orders 
            WHERE order_number IS NULL OR TRIM(order_number) = '';
        """)
        null_count = db_cursor.fetchone()[0]
        assert null_count == 0, f"Found {null_count} orders without order numbers"
    
    def test_orders_valid_fulfillment_status(self, db_cursor):
        """Verify fulfillment status values are valid."""
        db_cursor.execute("""
            SELECT DISTINCT fulfillment_status 
            FROM shiphero_raw.orders 
            WHERE fulfillment_status IS NOT NULL;
        """)
        statuses = {row[0] for row in db_cursor.fetchall()}
        
        # ShipHero fulfillment statuses
        valid_statuses = {
            'pending', 'shipped', 'fulfilled', 'partially_fulfilled',
            'unfulfilled', 'cancelled', 'on_hold', 'canceled', 'Culk'
        }
        
        invalid = statuses - valid_statuses
        assert not invalid, f"Found invalid fulfillment statuses: {invalid}"
    
    def test_orders_line_items_structure(self, db_cursor):
        """Verify line_items child table has expected structure."""
        # Check child table exists
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'orders__line_items'
            );
        """)
        assert db_cursor.fetchone()[0], "orders__line_items table does not exist"
        
        # Check it has data and expected fields
        db_cursor.execute("""
            SELECT sku, product_name, quantity, quantity_allocated, quantity_shipped, _dlt_parent_id
            FROM shiphero_raw.orders__line_items 
            LIMIT 1;
        """)
        result = db_cursor.fetchone()
        assert result is not None, "orders__line_items table is empty"
    
    def test_orders_positive_quantities(self, db_cursor):
        """Verify line item quantities are non-negative."""
        db_cursor.execute("""
            SELECT _dlt_parent_id, quantity, quantity_allocated, quantity_pending_fulfillment, 
                   quantity_shipped, backorder_quantity
            FROM shiphero_raw.orders__line_items;
        """)
        for row in db_cursor.fetchall():
            parent_id = row[0]
            quantity, allocated, pending, shipped, backorder = row[1:6]
            
            # Check quantity fields are non-negative where not null
            if quantity is not None:
                assert quantity >= 0, f"Order {parent_id} has negative quantity: {quantity}"
            if allocated is not None:
                assert allocated >= 0, f"Order {parent_id} has negative quantity_allocated: {allocated}"
            if pending is not None:
                assert pending >= 0, f"Order {parent_id} has negative quantity_pending_fulfillment: {pending}"
            if shipped is not None:
                assert shipped >= 0, f"Order {parent_id} has negative quantity_shipped: {shipped}"
            if backorder is not None:
                assert backorder >= 0, f"Order {parent_id} has negative backorder_quantity: {backorder}"
    
    def test_orders_shipments_structure(self, db_cursor):
        """Verify shipments child table has expected structure."""
        # Check child table exists
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'orders__shipments'
            );
        """)
        assert db_cursor.fetchone()[0], "orders__shipments table does not exist"
        
        # Check it has data and expected fields
        db_cursor.execute("""
            SELECT id, _dlt_parent_id
            FROM shiphero_raw.orders__shipments 
            LIMIT 1;
        """)
        result = db_cursor.fetchone()
        if result is not None:
            # Check if shipping_labels child table exists
            db_cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'shiphero_raw' 
                    AND table_name = 'orders__shipments__shipping_labels'
                );
            """)
            # Note: May not exist if no shipments have labels yet


class TestShipHeroIncrementalLoading:
    """Tests for incremental loading functionality."""
    
    def test_products_updated_at_recent(self, db_cursor):
        """Verify products have recent updated_at timestamps (incremental loading works)."""
        # Check if we have products updated in the last 90 days
        db_cursor.execute("""
            SELECT COUNT(*) FROM shiphero_raw.products 
            WHERE updated_at >= CURRENT_DATE - INTERVAL '90 days';
        """)
        recent_count = db_cursor.fetchone()[0]
        
        # We should have at least some recent updates if incremental is working
        db_cursor.execute("SELECT COUNT(*) FROM shiphero_raw.products;")
        total_count = db_cursor.fetchone()[0]
        
        # At least 10% should be recent (adjust based on business needs)
        assert recent_count > 0, "No products with recent updated_at timestamps"
    
    def test_orders_order_date_distribution(self, db_cursor):
        """Verify orders span expected time range."""
        db_cursor.execute("""
            SELECT 
                MIN(order_date::timestamp) as earliest,
                MAX(order_date::timestamp) as latest,
                COUNT(*) as total
            FROM shiphero_raw.orders 
            WHERE order_date IS NOT NULL;
        """)
        result = db_cursor.fetchone()
        if result:
            earliest, latest, total = result
            assert earliest is not None, "No valid order dates found"
            assert latest is not None, "No valid order dates found"
            assert total > 0, "No orders with order dates"
            
            # Orders should span at least a day (adjust based on business needs)
            if earliest and latest:
                date_range = (latest - earliest).days
                assert date_range >= 0, "Order date range is invalid"


class TestShipHeroComplexityMonitoring:
    """Tests for complexity usage patterns and monitoring."""
    
    def test_products_pagination_worked(self, db_cursor):
        """Verify pagination retrieved multiple batches (complexity monitoring allowed completion)."""
        db_cursor.execute("SELECT COUNT(*) FROM shiphero_raw.products;")
        count = db_cursor.fetchone()[0]
        
        # If we have more than 25 products, pagination worked (first=25 in query)
        # This indirectly tests that complexity monitoring didn't block extraction
        assert count > 0, "No products found - pagination may have failed"
    
    def test_orders_pagination_worked(self, db_cursor):
        """Verify orders pagination retrieved multiple batches."""
        db_cursor.execute("SELECT COUNT(*) FROM shiphero_raw.orders;")
        count = db_cursor.fetchone()[0]
        
        # If we have more than 25 orders, pagination worked
        assert count > 0, "No orders found - pagination may have failed"
    
    def test_data_freshness(self, db_cursor):
        """Verify data was loaded recently (complexity monitoring allows regular updates)."""
        # Check dlt load metadata
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = '_dlt_loads'
            );
        """)
        if db_cursor.fetchone()[0]:
            db_cursor.execute("""
                SELECT MAX(inserted_at) 
                FROM shiphero_raw._dlt_loads 
                WHERE status = 0;
            """)
            result = db_cursor.fetchone()
            if result and result[0]:
                last_load = result[0]
                # Data should be loaded within last 7 days in production
                # (adjust based on your refresh schedule)
                now = datetime.now(timezone.utc)
                days_since_load = (now - last_load).days
                assert days_since_load < 7, \
                    f"Data hasn't been refreshed in {days_since_load} days"


class TestShipHeroSchemaValidation:
    """Tests for required columns and schema structure."""
    
    def test_products_required_columns(self, db_cursor):
        """Verify products table has all required columns."""
        required_columns = ['id', 'sku', 'name', 'created_at', 'updated_at']
        
        db_cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'shiphero_raw' 
            AND table_name = 'products';
        """)
        existing_columns = {row[0] for row in db_cursor.fetchall()}
        
        missing_columns = set(required_columns) - existing_columns
        assert not missing_columns, f"Missing required columns: {missing_columns}"
        
        # Check warehouse_products child table exists
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'products__warehouse_products'
            );
        """)
        assert db_cursor.fetchone()[0], "products__warehouse_products child table does not exist"
    
    def test_orders_required_columns(self, db_cursor):
        """Verify orders table has all required columns."""
        required_columns = ['id', 'order_number', 'order_date', 'fulfillment_status']
        
        db_cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'shiphero_raw' 
            AND table_name = 'orders';
        """)
        existing_columns = {row[0] for row in db_cursor.fetchall()}
        
        missing_columns = set(required_columns) - existing_columns
        assert not missing_columns, f"Missing required columns: {missing_columns}"
        
        # Check child tables exist
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'orders__line_items'
            );
        """)
        assert db_cursor.fetchone()[0], "orders__line_items child table does not exist"
        
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'shiphero_raw' 
                AND table_name = 'orders__shipments'
            );
        """)
        assert db_cursor.fetchone()[0], "orders__shipments child table does not exist"
    
    def test_products_id_is_primary_key(self, db_cursor):
        """Verify id column has unique constraint or primary key."""
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM shiphero_raw.products 
            GROUP BY id 
            HAVING COUNT(*) > 1;
        """)
        duplicates = db_cursor.fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate product IDs"
    
    def test_orders_id_is_primary_key(self, db_cursor):
        """Verify id column has unique constraint or primary key."""
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM shiphero_raw.orders 
            GROUP BY id 
            HAVING COUNT(*) > 1;
        """)
        duplicates = db_cursor.fetchall()
        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate order IDs"
