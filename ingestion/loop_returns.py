"""
Loop Returns Data Extraction

Data Source: Loop Returns data via REST API
API Type: REST API
Key Endpoints:
  - /api/v1/returns/list
  - /api/v1/returns/{id}

Extraction Method: dlt REST API client with automatic pagination
Incremental Strategy: Filter by updated_at or created_at timestamps

Rate Limits:
  - Standard rate limits (check API documentation)
  
Authentication: API key (X-Loop-Api-Key header or sk_prod_ prefix)
"""

import dlt


def extract():
    """
    Extract data from Loop Returns API.
    
    TODO: Implement extraction logic
    - Configure dlt REST API source with Loop endpoints
    - Set up pagination handling
    - Implement incremental loading via timestamp filters
    - Extract: returns, return items, refund status
    
    Returns:
        dict: Extracted data ready for loading
    """
    pass


def load_to_postgres():
    """
    Load extracted Loop Returns data to PostgreSQL using dlt.
    
    TODO: Implement loading logic
    - Initialize dlt pipeline with destination='postgres'
    - Configure write disposition for incremental updates
    - Set primary keys (return_id)
    - Run pipeline and handle state management
    
    Returns:
        dlt.Pipeline: Completed pipeline with load info
    """
    pass


if __name__ == "__main__":
    # For testing individual source extraction
    print("Extracting Loop Returns data...")
    data = extract()
    print("Loading to PostgreSQL...")
    load_to_postgres()
    print("Loop Returns pipeline completed")
