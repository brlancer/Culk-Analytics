"""
Faire Wholesale Orders Extraction

Data Source: Faire wholesale orders via REST API v2
API Type: REST API
Key Endpoints:
  - /external-api/v2/orders
  - /external-api/v2/products

Rate Limits: 1000 requests per hour per access token
Authentication: Dual custom headers (X-FAIRE-APP-CREDENTIALS + X-FAIRE-OAUTH-ACCESS-TOKEN)

Incremental Strategy:
  - Queries MAX(updated_at) from PostgreSQL for each resource
  - Uses updated_at_min filter on Faire API
  - Known Issue: Faire occasionally bulk-updates updated_at timestamps across
    hundreds of orders (likely cache refresh/backend migration). This causes
    incremental loads to fetch more records than expected, but merge on primary_key
    deduplicates them.

Usage:
    python ingestion/faire.py  # Incremental load
    python -c "from ingestion.faire import load_to_postgres; load_to_postgres(use_incremental=False)"  # Full reload
"""

import base64
from typing import Iterator, Dict, Any, Optional
from datetime import datetime, timezone

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.common.pipeline import LoadInfo


# ============================================================================
# AUTHENTICATION
# ============================================================================

def get_faire_auth_headers():
    """Build Faire authentication headers from dlt secrets."""
    config = dlt.secrets.get("sources.faire.oauth", {})
    application_id = config.get("application_id")
    application_secret = config.get("application_secret")
    access_token = dlt.secrets.get("sources.faire.access_token")
    
    if not all([application_id, application_secret, access_token]):
        raise ValueError("Missing Faire credentials in .dlt/secrets.toml")
    
    credentials = f"{application_id}:{application_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    return {
        "X-FAIRE-APP-CREDENTIALS": encoded_credentials,
        "X-FAIRE-OAUTH-ACCESS-TOKEN": access_token,
    }


# ============================================================================
# INCREMENTAL STATE MANAGEMENT
# ============================================================================

def get_last_updated_timestamp(resource_name: str) -> str:
    """
    Query PostgreSQL for the most recent updated_at timestamp from the resource.
    
    Args:
        resource_name: Name of the resource (orders or products)
    
    Returns:
        ISO 8601 timestamp string or default "2000-01-01T00:00:00.000Z"
    """
    try:
        import psycopg2
        import os
        from dotenv import load_dotenv
        
        load_dotenv()
        
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DATABASE", "culk_db"),
            user=os.getenv("POSTGRES_USER", "brianlance"),
            password=os.getenv("POSTGRES_PASSWORD", "")
        )
        
        cursor = conn.cursor()
        
        # Check if table exists first
        check_table = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'faire_raw' 
                AND table_name = %s
            );
        """
        cursor.execute(check_table, (resource_name,))
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            cursor.close()
            conn.close()
            return "2000-01-01T00:00:00.000Z"
        
        # Query for max updated_at from the resource table
        query = f"""
            SELECT MAX(updated_at) 
            FROM faire_raw.{resource_name}
            WHERE updated_at IS NOT NULL;
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result and result[0]:
            timestamp = result[0]
            
            # Handle string timestamps
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            # Ensure UTC timezone
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            # Format as ISO 8601 with milliseconds
            iso_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            return iso_timestamp
        
        return "2000-01-01T00:00:00.000Z"

    except Exception as e:
        default_cutoff = "2000-01-01T00:00:00.000Z"
        print(f"Warning: Could not query last updated timestamp for {resource_name}: {e}")
        print(f"Falling back to default cutoff date {default_cutoff}")
        return default_cutoff


# ============================================================================
# RESOURCE DEFINITIONS
# ============================================================================

RESOURCES = [
    {
        "name": "orders",
        "endpoint": "orders",
        "data_key": "orders",
        "write_disposition": "merge",
        "primary_key": "id",
    },
    {
        "name": "products",
        "endpoint": "products",
        "data_key": "products",
        "write_disposition": "merge",
        "primary_key": "id",
    }
]


# ============================================================================
# DLT SOURCE
# ============================================================================

@dlt.source
def faire_source(
    orders_updated_at_min: Optional[str] = None,
    products_updated_at_min: Optional[str] = None,
    use_incremental: bool = True
):
    """
    Faire wholesale data source with database-driven incremental loading.
    
    Args:
        orders_updated_at_min: Override timestamp for orders incremental filter
        products_updated_at_min: Override timestamp for products incremental filter
        use_incremental: If True, query database for last updated timestamps
    
    dlt automatically:
    - Normalizes nested arrays (items, shipments, variants) into child tables
    - Infers schemas from JSON structure
    - Creates foreign key relationships via _dlt_parent_id
    """
    client = RESTClient(
        base_url="https://www.faire.com/external-api/v2",
        headers=get_faire_auth_headers()
    )
    
    resources = []
    
    for resource_config in RESOURCES:
        resource_name = resource_config["name"]
        
        # Determine updated_at_min for this resource
        if use_incremental:
            if resource_name == "orders" and orders_updated_at_min:
                updated_at_min = orders_updated_at_min
            elif resource_name == "products" and products_updated_at_min:
                updated_at_min = products_updated_at_min
            else:
                # Query database for last updated timestamp
                updated_at_min = get_last_updated_timestamp(resource_name)
                print(f"Incremental load: Fetching {resource_name} updated after {updated_at_min}")
        else:
            # Full historical load
            updated_at_min = "2000-01-01T00:00:00.000Z"
            print(f"Full load: Fetching all {resource_name} since {updated_at_min}")
        
        @dlt.resource(
            name=resource_config["name"],
            write_disposition=resource_config["write_disposition"],
            primary_key=resource_config["primary_key"]
        )
        def fetch_resource(
            endpoint=resource_config["endpoint"],
            data_key=resource_config["data_key"],
            filter_timestamp=updated_at_min
        ) -> Iterator[Dict[str, Any]]:
            cursor: Optional[str] = None
            total_fetched = 0
            
            while True:
                # First page: use updated_at_min filter
                # Subsequent pages: use only cursor (Faire returns 400 if both present)
                params = {"limit": 50}
                if cursor:
                    params["cursor"] = cursor
                else:
                    params["updated_at_min"] = filter_timestamp
                
                response = client.get(endpoint, params=params)
                data = response.json()
                items = data.get(data_key, [])
                
                for item in items:
                    yield item
                    total_fetched += 1
                
                cursor = data.get("cursor")
                if not cursor:
                    break
            
            print(f"  ✓ Fetched {total_fetched} {endpoint} records")
        
        resources.append(fetch_resource)
    
    return resources


# ============================================================================
# PIPELINE
# ============================================================================

def load_to_postgres(
    use_incremental: bool = True,
    orders_updated_at_min: Optional[str] = None,
    products_updated_at_min: Optional[str] = None
) -> LoadInfo:
    """
    Load Faire data to PostgreSQL with incremental loading.
    
    Args:
        use_incremental: If True, query database for last updated timestamps
        orders_updated_at_min: Manual override for orders timestamp
        products_updated_at_min: Manual override for products timestamp
    
    Returns:
        dlt LoadInfo object
    
    dlt auto-creates normalized tables:
    - orders (parent)
    - orders__items (child)
    - orders__shipments (child)
    - products (parent)
    - products__variants (child)
    - etc.
    """
    pipeline = dlt.pipeline(
        pipeline_name="faire",
        destination="postgres",
        dataset_name="faire_raw"
    )
    
    print("="*60)
    print("Starting Faire extraction...")
    print("="*60)
    
    load_info = pipeline.run(
        faire_source(
            orders_updated_at_min=orders_updated_at_min,
            products_updated_at_min=products_updated_at_min,
            use_incremental=use_incremental
        )
    )
    
    print(f"\n{'='*60}")
    print("Load Summary:")
    print(f"{'='*60}")
    print(f"Load ID: {load_info.loads_ids[0] if load_info.loads_ids else 'N/A'}")
    print(f"{'='*60}\n")
    
    return load_info


if __name__ == "__main__":
    # Default: incremental load
    load_to_postgres()
    
    # For full historical reload:
    # load_to_postgres(use_incremental=False)
    
    # For manual timestamp override:
    # load_to_postgres(
    #     orders_updated_at_min="2024-12-01T00:00:00.000Z",
    #     products_updated_at_min="2024-12-01T00:00:00.000Z"
    # )
