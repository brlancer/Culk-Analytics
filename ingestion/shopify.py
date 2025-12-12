"""
Shopify DTC Sales Extraction

Data Source: Shopify DTC sales via REST Admin API
API Type: REST API
Key Endpoints:
  - /admin/api/2024-01/orders.json
  - /admin/api/2024-01/products.json
  - /admin/api/2024-01/customers.json

Extraction Method: dlt REST API client with automatic pagination
Incremental Strategy: Use updated_at_min parameter for incremental loads

Rate Limits: 
  - 2 requests/second (Basic/Shopify plans)
  - 4 requests/second (Advanced/Plus plans)
  
Authentication: Admin API access token (X-Shopify-Access-Token header)
"""

import dlt


def extract():
    """
    Extract data from Shopify Admin API.
    
    TODO: Implement extraction logic
    - Configure dlt REST API source with Shopify endpoints
    - Set up pagination handling (link headers)
    - Implement incremental loading via updated_at_min
    - Extract: orders, products, customers, inventory_levels
    
    Returns:
        dict: Extracted data ready for loading
    """
    pass


def load_to_postgres():
    """
    Load extracted Shopify data to PostgreSQL using dlt.
    
    TODO: Implement loading logic
    - Initialize dlt pipeline with destination='postgres'
    - Configure write disposition (merge for incremental, replace for full refresh)
    - Set primary keys for deduplication
    - Run pipeline and handle state management
    
    Returns:
        dlt.Pipeline: Completed pipeline with load info
    """
    pass


if __name__ == "__main__":
    # For testing individual source extraction
    print("Extracting Shopify data...")
    data = extract()
    print("Loading to PostgreSQL...")
    load_to_postgres()
    print("Shopify pipeline completed")
