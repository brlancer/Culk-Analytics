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
result = subprocess.run(['pytest', 'tests/test_shopify.py', '-v'], 
                       capture_output=True)
if result.returncode != 0:
    print("Data quality tests failed!")
```
