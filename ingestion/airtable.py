"""
Airtable Product Master Data Extraction

Data Source: Product master data (COGS, categories) via Airtable REST API
API Type: REST API
Key Endpoints:
  - /v0/{base_id}/{table_name} to list records
  
Extraction Method: dlt REST API client with automatic pagination
Incremental Strategy: Full refresh (small dataset) or filter by Last Modified

Rate Limits:
  - 5 requests per second per base
  
Authentication: Personal access token or API key (Authorization: Bearer ...)
"""

import dlt


def extract():
    """
    Extract product master data from Airtable.
    
    TODO: Implement extraction logic
    - Configure dlt REST API source with Airtable endpoint
    - Set up pagination handling (offset-based)
    - Extract all fields: SKU, product name, COGS, category, attributes
    - Handle Airtable field types (attachments, links, etc.)
    - Consider full refresh vs. incremental based on data volume
    
    Returns:
        dict: Extracted product master data ready for loading
    """
    pass


def load_to_postgres():
    """
    Load extracted Airtable data to PostgreSQL using dlt.
    
    TODO: Implement loading logic
    - Initialize dlt pipeline with destination='postgres'
    - Configure write disposition (replace for full refresh, merge for incremental)
    - Set primary keys (Airtable record ID or SKU)
    - Run pipeline and handle schema changes
    
    Returns:
        dlt.Pipeline: Completed pipeline with load info
    """
    pass


if __name__ == "__main__":
    # For testing individual source extraction
    print("Extracting Airtable product master data...")
    data = extract()
    print("Loading to PostgreSQL...")
    load_to_postgres()
    print("Airtable pipeline completed")
