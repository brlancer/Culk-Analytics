# Data Quality Tests

Automated pytest-based tests to validate data quality after each pipeline extraction.

## Setup

Install test dependencies:
```bash
pip install pytest psycopg2-binary
```

## Running Tests

Run all tests:
```bash
pytest tests/ -v
```

Run specific test file:
```bash
pytest tests/test_shopify.py -v
pytest tests/test_shiphero.py -v
```

Run specific test class:
```bash
pytest tests/test_shopify.py::TestShopifyOrders -v
```

Run specific test:
```bash
pytest tests/test_shopify.py::TestShopifyOrders::test_orders_has_data -v
```

## Test Categories

### TestShopifyOrders
- Table existence and data presence
- ID integrity (no nulls)
- Valid financial/fulfillment status values
- Price validation (non-negative)

### TestShopifyProducts
- Table existence and data presence
- Product titles (no nulls/empty)
- Variant price validation

### TestShopifyCustomers
- Table existence and data presence

### TestShopifyInventory
- Table existence and data presence
- Non-negative quantities

### TestShipHeroProducts
- Table existence and data presence
- ID integrity (no nulls)
- SKU and name validation (no nulls/empty)
- Timestamp validation (created_at, updated_at)
- Warehouse_products JSONB structure
- Non-negative values (on_hand, allocated, available)

### TestShipHeroOrders
- Table existence and data presence
- ID integrity and order numbers
- Valid fulfillment_status enum values
- Line_items JSONB structure
- Non-negative quantities (quantity, quantity_allocated, quantity_shipped)
- Shipments JSONB structure with shipping_labels

### TestShipHeroIncrementalLoading
- Recent updated_at timestamps (incremental loading works)
- Order date distribution (time range validation)

### TestShipHeroComplexityMonitoring
- Pagination success (>25 items = multiple pages)
- Data freshness (loaded within 7 days)

### TestShipHeroSchemaValidation
- Required columns present (products and orders)
- Primary key uniqueness (no duplicate IDs)

### TestDataFreshness
- Orders data recency (< 7 days)
- Products data recency (< 7 days)

## Configuration

Tests read database credentials from environment variables or .env file:
- `POSTGRES_HOST` (default: localhost)
- `POSTGRES_PORT` (default: 5432)
- `POSTGRES_DATABASE` (default: culk_db)
- `POSTGRES_USER` (default: brianlance)
- `POSTGRES_PASSWORD`

## Adding New Tests

1. Create new test class in appropriate test file
2. Use `db_cursor` fixture for database queries
3. Follow naming convention: `test_<what_is_being_tested>`
4. Include descriptive docstrings
5. Add assertions with clear error messages

## Integration with Pipeline

Optional: Run tests automatically after extraction:
```python
import subprocess

# Run ShipHero tests after extraction
result = subprocess.run(['pytest', 'tests/test_shiphero.py', '-v'], 
                       capture_output=True)
if result.returncode != 0:
    print("ShipHero data quality tests failed!")

# Run Shopify tests after extraction
result = subprocess.run(['pytest', 'tests/test_shopify.py', '-v'], 
                       capture_output=True)
if result.returncode != 0:
    print("Shopify data quality tests failed!")
```
