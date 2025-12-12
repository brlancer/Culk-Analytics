"""
Faire Wholesale Orders Extraction

Data Source: Faire wholesale orders via REST API v2
API Type: REST API
Key Endpoints:
  - /api/v2/orders
  - /api/v2/products

Extraction Method: dlt REST API client with automatic pagination
Incremental Strategy: Filter by updated_at or created_at timestamps

Rate Limits:
  - 1000 requests per hour per access token
  
Authentication: Bearer token (Authorization: Bearer fav2_...)
"""

import dlt


def extract():
    """
    Extract data from Faire REST API.
    
    TODO: Implement extraction logic
    - Configure dlt REST API source with Faire endpoints
    - Set up pagination handling (page-based or cursor)
    - Implement incremental loading via timestamp filters
    - Extract: orders, products, inventory
    
    Returns:
        dict: Extracted data ready for loading
    """
    pass


def load_to_postgres():
    """
    Load extracted Faire data to PostgreSQL using dlt.
    
    TODO: Implement loading logic
    - Initialize dlt pipeline with destination='postgres'
    - Configure write disposition for incremental updates
    - Set primary keys (order_id, product_id)
    - Run pipeline and handle state management
    
    Returns:
        dlt.Pipeline: Completed pipeline with load info
    """
    pass


if __name__ == "__main__":
    # For testing individual source extraction
    print("Extracting Faire data...")
    data = extract()
    print("Loading to PostgreSQL...")
    load_to_postgres()
    print("Faire pipeline completed")
