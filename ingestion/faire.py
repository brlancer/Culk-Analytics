"""
Faire Wholesale Orders Extraction

Data Source: Faire wholesale orders via REST API v2
API Type: REST API
Key Endpoints:
  - /external-api/v2/orders
  - /external-api/v2/products

Rate Limits: 1000 requests per hour per access token
Authentication: Dual custom headers (X-FAIRE-APP-CREDENTIALS + X-FAIRE-OAUTH-ACCESS-TOKEN)
"""

import base64
import time
from typing import Optional

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source


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
# DLT SOURCE
# ============================================================================

@dlt.source
def faire_source():
    """
    Faire wholesale data source.
    
    dlt automatically:
    - Normalizes nested arrays (items, shipments, variants) into child tables
    - Infers schemas from JSON structure
    - Creates foreign key relationships via _dlt_parent_id
    """
    headers = get_faire_auth_headers()
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.faire.com/external-api/v2",
            "headers": headers,
        },
        "resource_defaults": {
            "endpoint": {
                "params": {"limit": 50},
                "paginator": {
                    "type": "cursor",
                    "cursor_path": "cursor",
                    "cursor_param": "cursor",
                },
            }
        },
        "resources": [
            {
                "name": "orders",
                "endpoint": {
                    "path": "orders",
                    "data_selector": "orders",
                },
                "write_disposition": "merge",
                "primary_key": "id",
            },
            {
                "name": "products",
                "endpoint": {
                    "path": "products",
                    "data_selector": "products",
                },
                "write_disposition": "replace",
                "primary_key": "id",
            }
        ]
    }
    
    return rest_api_source(config)


# ============================================================================
# PIPELINE
# ============================================================================

def load_to_postgres():
    """
    Load Faire data to PostgreSQL.
    
    dlt will auto-create normalized tables:
    - orders (parent)
    - orders__items (child)
    - orders__shipments (child)
    - products (parent)
    - products__variants (child)
    - products__variants__prices (grandchild)
    - etc.
    """
    pipeline = dlt.pipeline(
        pipeline_name="faire",
        destination="postgres",
        dataset_name="faire_raw"
    )
    
    print("Starting Faire extraction...")
    load_info = pipeline.run(faire_source())
    
    print(f"\nLoad ID: {load_info.loads_ids[0] if load_info.loads_ids else 'N/A'}")
    return pipeline


if __name__ == "__main__":
    load_to_postgres()
