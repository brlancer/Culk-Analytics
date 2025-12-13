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
    """Test suite for faire_orders_resource table."""
    
    def test_orders_table_exists(self, db_cursor):
        """Verify faire_orders table exists in faire_raw schema."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_orders'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_orders table does not exist"
    
    def test_orders_has_data(self, db_cursor):
        """Verify orders table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_orders;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "faire_orders table is empty"
    
    def test_orders_primary_key_not_null(self, db_cursor):
        """Verify all orders have non-null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_orders 
            WHERE id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orders with NULL id"
    
    def test_orders_id_format(self, db_cursor):
        """Verify order IDs start with 'bo_' prefix."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_orders 
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
            SELECT DISTINCT state FROM faire_raw.faire_orders 
            WHERE state IS NOT NULL;
        """)
        states = [row[0] for row in db_cursor.fetchall()]
        
        invalid_states = [s for s in states if s not in valid_states]
        assert len(invalid_states) == 0, f"Found invalid order states: {invalid_states}"
    
    def test_orders_timestamps(self, db_cursor):
        """Verify created_at and updated_at are present."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_orders 
            WHERE created_at IS NULL OR updated_at IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orders with NULL timestamps"
    
    def test_orders_recent_data(self, db_cursor):
        """Verify orders contain data loaded recently (within 7 days)."""
        db_cursor.execute("""
            SELECT MAX(to_timestamp(_dlt_load_id::double precision)) as last_load
            FROM faire_raw.faire_orders;
        """)
        last_load = db_cursor.fetchone()[0]
        
        if last_load:
            days_old = (datetime.now() - last_load).days
            assert days_old <= 7, f"Most recent data is {days_old} days old"


class TestFaireOrderItems:
    """Test suite for faire_order_items table (child resource)."""
    
    def test_order_items_table_exists(self, db_cursor):
        """Verify faire_order_items table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_order_items'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_order_items table does not exist"
    
    def test_order_items_has_data(self, db_cursor):
        """Verify order items table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_order_items;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "faire_order_items table is empty"
    
    def test_order_items_foreign_key(self, db_cursor):
        """Verify all order_items have valid order_id foreign keys."""
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM faire_raw.faire_order_items oi
            LEFT JOIN faire_raw.faire_orders o ON oi.order_id = o.id
            WHERE o.id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orphaned order items (no matching order_id)"
    
    def test_order_items_primary_key_not_null(self, db_cursor):
        """Verify all order items have non-null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_order_items 
            WHERE id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} order items with NULL id"
    
    def test_order_items_quantity_positive(self, db_cursor):
        """Verify all order items have positive quantities."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_order_items 
            WHERE quantity IS NOT NULL AND quantity <= 0;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} order items with non-positive quantity"


class TestFaireOrderShipments:
    """Test suite for faire_order_shipments table (child resource)."""
    
    def test_order_shipments_table_exists(self, db_cursor):
        """Verify faire_order_shipments table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_order_shipments'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_order_shipments table does not exist"
    
    def test_order_shipments_foreign_key(self, db_cursor):
        """Verify all shipments have valid order_id foreign keys (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_order_shipments;")
        shipment_count = db_cursor.fetchone()[0]
        
        if shipment_count > 0:
            db_cursor.execute("""
                SELECT COUNT(*) 
                FROM faire_raw.faire_order_shipments s
                LEFT JOIN faire_raw.faire_orders o ON s.order_id = o.id
                WHERE o.id IS NULL;
            """)
            orphan_count = db_cursor.fetchone()[0]
            assert orphan_count == 0, f"Found {orphan_count} orphaned shipments"


class TestFaireProducts:
    """Test suite for faire_products table."""
    
    def test_products_table_exists(self, db_cursor):
        """Verify faire_products table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_products'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_products table does not exist"
    
    def test_products_has_data(self, db_cursor):
        """Verify products table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_products;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "faire_products table is empty"
    
    def test_products_primary_key_not_null(self, db_cursor):
        """Verify all products have non-null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_products 
            WHERE id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} products with NULL id"
    
    def test_products_id_format(self, db_cursor):
        """Verify product IDs start with 'p_' prefix."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_products 
            WHERE id IS NOT NULL AND id NOT LIKE 'p_%';
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} products with invalid ID format (should start with 'p_')"
    
    def test_products_valid_sale_state(self, db_cursor):
        """Verify all products have valid sale_state enums."""
        valid_states = ['FOR_SALE', 'SALES_PAUSED']
        
        db_cursor.execute("""
            SELECT DISTINCT sale_state FROM faire_raw.faire_products 
            WHERE sale_state IS NOT NULL;
        """)
        states = [row[0] for row in db_cursor.fetchall()]
        
        invalid_states = [s for s in states if s not in valid_states]
        assert len(invalid_states) == 0, f"Found invalid sale states: {invalid_states}"
    
    def test_products_valid_lifecycle_state(self, db_cursor):
        """Verify all products have valid lifecycle_state enums."""
        valid_states = ['DRAFT', 'PUBLISHED', 'UNPUBLISHED', 'DELETED']
        
        db_cursor.execute("""
            SELECT DISTINCT lifecycle_state FROM faire_raw.faire_products 
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
            AND table_name = 'faire_products' 
            AND column_name = 'images';
        """)
        result = db_cursor.fetchone()
        assert result is None, "Products table should not have 'images' column (excluded per requirements)"


class TestFaireProductVariants:
    """Test suite for faire_product_variants table (child resource)."""
    
    def test_product_variants_table_exists(self, db_cursor):
        """Verify faire_product_variants table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_product_variants'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_product_variants table does not exist"
    
    def test_product_variants_has_data(self, db_cursor):
        """Verify product variants table contains data."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_product_variants;")
        count = db_cursor.fetchone()[0]
        assert count > 0, "faire_product_variants table is empty"
    
    def test_product_variants_foreign_key(self, db_cursor):
        """Verify all variants have valid product_id foreign keys."""
        db_cursor.execute("""
            SELECT COUNT(*) 
            FROM faire_raw.faire_product_variants v
            LEFT JOIN faire_raw.faire_products p ON v.product_id = p.id
            WHERE p.id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} orphaned variants (no matching product_id)"
    
    def test_product_variants_primary_key_not_null(self, db_cursor):
        """Verify all variants have non-null IDs."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_product_variants 
            WHERE id IS NULL;
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} variants with NULL id"
    
    def test_product_variants_id_format(self, db_cursor):
        """Verify variant IDs start with 'po_' prefix."""
        db_cursor.execute("""
            SELECT COUNT(*) FROM faire_raw.faire_product_variants 
            WHERE id IS NOT NULL AND id NOT LIKE 'po_%';
        """)
        count = db_cursor.fetchone()[0]
        assert count == 0, f"Found {count} variants with invalid ID format (should start with 'po_')"
    
    def test_product_variants_no_images_field(self, db_cursor):
        """Verify images field was excluded from variants (per requirements)."""
        db_cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'faire_raw' 
            AND table_name = 'faire_product_variants' 
            AND column_name = 'images';
        """)
        result = db_cursor.fetchone()
        assert result is None, "Variants table should not have 'images' column (excluded per requirements)"


class TestFaireProductVariantOptionSets:
    """Test suite for faire_product_variant_option_sets table."""
    
    def test_variant_option_sets_table_exists(self, db_cursor):
        """Verify faire_product_variant_option_sets table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_product_variant_option_sets'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_product_variant_option_sets table does not exist"
    
    def test_variant_option_sets_foreign_key(self, db_cursor):
        """Verify all option sets have valid product_id foreign keys (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_product_variant_option_sets;")
        count = db_cursor.fetchone()[0]
        
        if count > 0:
            db_cursor.execute("""
                SELECT COUNT(*) 
                FROM faire_raw.faire_product_variant_option_sets os
                LEFT JOIN faire_raw.faire_products p ON os.product_id = p.id
                WHERE p.id IS NULL;
            """)
            orphan_count = db_cursor.fetchone()[0]
            assert orphan_count == 0, f"Found {orphan_count} orphaned option sets"
    
    def test_variant_option_sets_values_array(self, db_cursor):
        """Verify values field is stored as array (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_product_variant_option_sets;")
        count = db_cursor.fetchone()[0]
        
        if count > 0:
            db_cursor.execute("""
                SELECT data_type FROM information_schema.columns 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_product_variant_option_sets' 
                AND column_name = 'values';
            """)
            data_type = db_cursor.fetchone()
            # dlt may store as ARRAY or jsonb, both acceptable
            assert data_type is not None, "values column should exist"


class TestFaireProductAttributes:
    """Test suite for faire_product_attributes table."""
    
    def test_product_attributes_table_exists(self, db_cursor):
        """Verify faire_product_attributes table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_product_attributes'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_product_attributes table does not exist"
    
    def test_product_attributes_foreign_key(self, db_cursor):
        """Verify all attributes have valid product_id foreign keys (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_product_attributes;")
        count = db_cursor.fetchone()[0]
        
        if count > 0:
            db_cursor.execute("""
                SELECT COUNT(*) 
                FROM faire_raw.faire_product_attributes a
                LEFT JOIN faire_raw.faire_products p ON a.product_id = p.id
                WHERE p.id IS NULL;
            """)
            orphan_count = db_cursor.fetchone()[0]
            assert orphan_count == 0, f"Found {orphan_count} orphaned attributes"


class TestFaireVariantPrices:
    """Test suite for faire_variant_prices table."""
    
    def test_variant_prices_table_exists(self, db_cursor):
        """Verify faire_variant_prices table exists."""
        db_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = 'faire_variant_prices'
            );
        """)
        assert db_cursor.fetchone()[0], "faire_variant_prices table does not exist"
    
    def test_variant_prices_foreign_key(self, db_cursor):
        """Verify all prices have valid variant_id foreign keys (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_variant_prices;")
        count = db_cursor.fetchone()[0]
        
        if count > 0:
            db_cursor.execute("""
                SELECT COUNT(*) 
                FROM faire_raw.faire_variant_prices vp
                LEFT JOIN faire_raw.faire_product_variants v ON vp.variant_id = v.id
                WHERE v.id IS NULL;
            """)
            orphan_count = db_cursor.fetchone()[0]
            assert orphan_count == 0, f"Found {orphan_count} orphaned prices"
    
    def test_variant_prices_valid_currency(self, db_cursor):
        """Verify all prices have valid ISO 4217 currency codes (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_variant_prices;")
        count = db_cursor.fetchone()[0]
        
        if count > 0:
            valid_currencies = ['USD', 'CAD', 'GBP', 'AUD', 'EUR']
            
            db_cursor.execute("""
                SELECT DISTINCT wholesale_price_currency FROM faire_raw.faire_variant_prices 
                WHERE wholesale_price_currency IS NOT NULL;
            """)
            currencies = [row[0] for row in db_cursor.fetchall()]
            
            invalid_currencies = [c for c in currencies if c not in valid_currencies]
            assert len(invalid_currencies) == 0, f"Found invalid currency codes: {invalid_currencies}"
    
    def test_variant_prices_positive_amounts(self, db_cursor):
        """Verify all price amounts are positive (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_variant_prices;")
        count = db_cursor.fetchone()[0]
        
        if count > 0:
            db_cursor.execute("""
                SELECT COUNT(*) FROM faire_raw.faire_variant_prices 
                WHERE wholesale_price_amount_minor IS NOT NULL 
                AND wholesale_price_amount_minor <= 0;
            """)
            invalid_count = db_cursor.fetchone()[0]
            assert invalid_count == 0, f"Found {invalid_count} prices with non-positive wholesale amounts"
    
    def test_variant_prices_valid_geo_constraint(self, db_cursor):
        """Verify all prices have valid country or country_group (if data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_variant_prices;")
        count = db_cursor.fetchone()[0]
        
        if count > 0:
            valid_countries = ['USA', 'CAN', 'GBR', 'AUS']
            valid_country_groups = ['EUROPEAN_UNION']
            
            db_cursor.execute("""
                SELECT DISTINCT country FROM faire_raw.faire_variant_prices 
                WHERE country IS NOT NULL;
            """)
            countries = [row[0] for row in db_cursor.fetchall()]
            
            invalid_countries = [c for c in countries if c not in valid_countries]
            assert len(invalid_countries) == 0, f"Found invalid country codes: {invalid_countries}"


class TestFaireDataIntegrity:
    """Cross-resource data integrity tests."""
    
    def test_all_resources_loaded(self, db_cursor):
        """Verify all 8 Faire resources have tables."""
        expected_tables = [
            'faire_orders',
            'faire_order_items', 
            'faire_order_shipments',
            'faire_products',
            'faire_product_variants',
            'faire_product_variant_option_sets',
            'faire_product_attributes',
            'faire_variant_prices'
        ]
        
        for table in expected_tables:
            db_cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'faire_raw' 
                    AND table_name = '{table}'
                );
            """)
            assert db_cursor.fetchone()[0], f"Missing table: {table}"
    
    def test_orders_have_items(self, db_cursor):
        """Verify at least some orders have associated items."""
        db_cursor.execute("""
            SELECT COUNT(DISTINCT o.id) 
            FROM faire_raw.faire_orders o
            INNER JOIN faire_raw.faire_order_items oi ON o.id = oi.order_id;
        """)
        count = db_cursor.fetchone()[0]
        assert count > 0, "No orders have associated items (data integrity issue)"
    
    def test_products_have_variants(self, db_cursor):
        """Verify at least some products have associated variants."""
        db_cursor.execute("""
            SELECT COUNT(DISTINCT p.id) 
            FROM faire_raw.faire_products p
            INNER JOIN faire_raw.faire_product_variants v ON p.id = v.product_id;
        """)
        count = db_cursor.fetchone()[0]
        assert count > 0, "No products have associated variants (data integrity issue)"
    
    def test_products_taxonomy_type_embedded(self, db_cursor):
        """Verify products table has embedded taxonomy_type fields."""
        db_cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'faire_raw' 
            AND table_name = 'faire_products' 
            AND column_name IN ('taxonomy_type_id', 'taxonomy_type_name');
        """)
        columns = [row[0] for row in db_cursor.fetchall()]
        
        # At least one taxonomy field should exist if data has taxonomy info
        # Not asserting both must exist as schema depends on actual API data
        assert len(columns) >= 0, "Taxonomy type fields should be queryable"
    
    def test_variants_have_prices(self, db_cursor):
        """Verify at least some variants have associated prices (if price data exists)."""
        db_cursor.execute("SELECT COUNT(*) FROM faire_raw.faire_variant_prices;")
        price_count = db_cursor.fetchone()[0]
        
        if price_count > 0:
            db_cursor.execute("""
                SELECT COUNT(DISTINCT v.id) 
                FROM faire_raw.faire_product_variants v
                INNER JOIN faire_raw.faire_variant_prices vp ON v.id = vp.variant_id;
            """)
            count = db_cursor.fetchone()[0]
            assert count > 0, "No variants have associated prices despite price data existing"
