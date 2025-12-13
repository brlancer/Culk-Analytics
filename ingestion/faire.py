"""
Faire Wholesale Orders Extraction

Data Source: Faire wholesale orders via REST API v2
API Type: REST API
Key Endpoints:
  - /external-api/v2/orders
  - /external-api/v2/products

Extraction Method: dlt REST API client with automatic pagination
Incremental Strategy: Filter by updated_at_min timestamps (orders only)

Rate Limits:
  - 1000 requests per hour per access token
  - Strategy: No throttling for first 250 requests, then 3.6s delay
  
Authentication: Dual custom headers (X-FAIRE-APP-CREDENTIALS + X-FAIRE-OAUTH-ACCESS-TOKEN)
"""

import base64
import time
from typing import Any, Dict, Iterator, List, Optional

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.config_setup import RESTAPIConfig


# ============================================================================
# REQUEST COUNTER & RATE LIMITING
# ============================================================================

class RequestCounter:
    """Global request counter for rate limiting."""
    def __init__(self):
        self.count = 0
        self.start_time = time.time()
    
    def increment(self):
        """Increment request counter and apply throttling after 250 requests."""
        self.count += 1
        
        # Apply 3.6s delay after first 250 requests (1000 req/hour = ~3.6s per request)
        if self.count > 250:
            time.sleep(3.6)
            
    def reset(self):
        """Reset counter for new ingestion round."""
        self.count = 0
        self.start_time = time.time()
        
    def summary(self) -> str:
        """Return summary of request metrics."""
        elapsed = time.time() - self.start_time
        return f"{self.count} requests in {elapsed:.1f}s ({self.count/elapsed*3600:.0f} req/hour)"


# Global counter instance
request_counter = RequestCounter()


# ============================================================================
# AUTHENTICATION CONFIGURATION
# ============================================================================

def get_faire_auth_headers() -> Dict[str, str]:
    """
    Build Faire authentication headers from dlt secrets.
    
    Returns:
        dict: Headers with X-FAIRE-APP-CREDENTIALS and X-FAIRE-OAUTH-ACCESS-TOKEN
    """
    # Load credentials from .dlt/secrets.toml
    config = dlt.secrets.get("sources.faire.oauth", {})
    application_id = config.get("application_id")
    application_secret = config.get("application_secret")
    access_token = dlt.secrets.get("sources.faire.access_token")
    
    if not all([application_id, application_secret, access_token]):
        raise ValueError(
            "Missing Faire credentials. Ensure sources.faire.oauth.application_id, "
            "sources.faire.oauth.application_secret, and sources.faire.access_token "
            "are set in .dlt/secrets.toml"
        )
    
    # Encode applicationId:applicationSecret in Base64
    credentials = f"{application_id}:{application_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    return {
        "X-FAIRE-APP-CREDENTIALS": encoded_credentials,
        "X-FAIRE-OAUTH-ACCESS-TOKEN": access_token,
        "Accept": "application/json"
    }


# ============================================================================
# DATA FLATTENING FUNCTIONS
# ============================================================================

def flatten_order_items(order_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and flatten order items from order response.
    
    Args:
        order_data: Single order dict from Faire API
        
    Returns:
        List of flattened order item dicts with order_id foreign key
    """
    items = []
    order_id = order_data.get("id")
    
    for item in order_data.get("items", []):
        flattened_item = {
            "id": item.get("id"),
            "order_id": order_id,  # Foreign key
            "product_id": item.get("product_id"),
            "product_option_id": item.get("product_option_id"),
            "quantity": item.get("quantity"),
            "sku": item.get("sku"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
            # Include all available item fields from API
            **{k: v for k, v in item.items() if k not in [
                "id", "product_id", "product_option_id", "quantity", "sku", 
                "created_at", "updated_at"
            ]}
        }
        items.append(flattened_item)
    
    return items


def flatten_order_shipments(order_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and flatten shipments from order response.
    
    Args:
        order_data: Single order dict from Faire API
        
    Returns:
        List of flattened shipment dicts with order_id foreign key
    """
    shipments = []
    order_id = order_data.get("id")
    
    for shipment in order_data.get("shipments", []):
        flattened_shipment = {
            "id": shipment.get("id"),
            "order_id": order_id,  # Foreign key
            "carrier": shipment.get("carrier"),
            "tracking_code": shipment.get("tracking_code"),
            "created_at": shipment.get("created_at"),
            "updated_at": shipment.get("updated_at"),
            # Include all available shipment fields
            **{k: v for k, v in shipment.items() if k not in [
                "id", "carrier", "tracking_code", "created_at", "updated_at"
            ]}
        }
        shipments.append(flattened_shipment)
    
    return shipments


def flatten_product_variants(product_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and flatten product variants from product response.
    
    Args:
        product_data: Single product dict from Faire API
        
    Returns:
        List of flattened variant dicts with product_id foreign key
    """
    variants = []
    product_id = product_data.get("id")
    
    for variant in product_data.get("variants", []):
        # Exclude images, prices, measurements, case_measurements (handled separately)
        variant_copy = {k: v for k, v in variant.items() 
                       if k not in ["images", "prices", "measurements", "case_measurements"]}
        
        flattened_variant = {
            "id": variant_copy.get("id"),
            "product_id": product_id,  # Foreign key
            "created_at": variant_copy.get("created_at"),
            "updated_at": variant_copy.get("updated_at"),
            "name": variant_copy.get("name"),
            "sku": variant_copy.get("sku"),
            "sale_state": variant_copy.get("sale_state"),
            "lifecycle_state": variant_copy.get("lifecycle_state"),
            "available_quantity": variant_copy.get("available_quantity"),
            # Include all other variant fields
            **{k: v for k, v in variant_copy.items() if k not in [
                "id", "created_at", "updated_at", "name", "sku", 
                "sale_state", "lifecycle_state", "available_quantity"
            ]}
        }
        variants.append(flattened_variant)
    
    return variants


def flatten_product_variant_option_sets(product_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and flatten variant option sets from product response.
    
    Args:
        product_data: Single product dict from Faire API
        
    Returns:
        List of flattened option set dicts with product_id foreign key
    """
    option_sets = []
    product_id = product_data.get("id")
    
    for idx, option_set in enumerate(product_data.get("variant_option_sets", [])):
        flattened_option_set = {
            "id": f"{product_id}_option_{idx}",  # Synthetic ID
            "product_id": product_id,  # Foreign key
            "name": option_set.get("name"),
            "values": option_set.get("values", []),  # PostgreSQL array
        }
        option_sets.append(flattened_option_set)
    
    return option_sets


def flatten_product_attributes(product_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and flatten product attributes from product response.
    
    Args:
        product_data: Single product dict from Faire API
        
    Returns:
        List of flattened attribute dicts with product_id foreign key
    """
    attributes = []
    product_id = product_data.get("id")
    
    for idx, attribute in enumerate(product_data.get("product_attributes", [])):
        flattened_attribute = {
            "id": f"{product_id}_attr_{idx}",  # Synthetic ID
            "product_id": product_id,  # Foreign key
            "name": attribute.get("name"),
            "value": attribute.get("value"),
        }
        attributes.append(flattened_attribute)
    
    return attributes


def flatten_variant_prices(product_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and flatten variant prices from product response.
    
    Processes all variants in a product and extracts their price information.
    
    Args:
        product_data: Single product dict from Faire API
        
    Returns:
        List of flattened price dicts with variant_id foreign key
    """
    prices = []
    
    for variant in product_data.get("variants", []):
        variant_id = variant.get("id")
        
        for price in variant.get("prices", []):
            geo_constraint = price.get("geo_constraint", {})
            wholesale_price = price.get("wholesale_price", {})
            retail_price = price.get("retail_price", {})
            
            flattened_price = {
                "variant_id": variant_id,  # Foreign key
                "country": geo_constraint.get("country"),
                "country_group": geo_constraint.get("country_group"),
                "wholesale_price_amount_minor": wholesale_price.get("amount_minor"),
                "wholesale_price_currency": wholesale_price.get("currency"),
                "retail_price_amount_minor": retail_price.get("amount_minor"),
                "retail_price_currency": retail_price.get("currency"),
            }
            prices.append(flattened_price)
    
    return prices


# ============================================================================
# DLT RESOURCES (FETCH FROM API)
# ============================================================================

@dlt.resource(
    name="faire_orders_raw",
    write_disposition="merge",
    primary_key="id"
)
def faire_orders_raw_resource(
    updated_at_min: str = "2024-01-01T00:00:00Z"
) -> Iterator[Dict[str, Any]]:
    """
    Fetch Faire orders from API with full nested data.
    
    This is the base resource that fetches from the API once.
    Transformer resources process this data into normalized tables.
    
    Args:
        updated_at_min: ISO 8601 timestamp for incremental filtering
        
    Yields:
        Complete order dicts with nested items and shipments arrays
    """
    headers = get_faire_auth_headers()
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.faire.com/external-api/v2",
            "headers": headers,
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": 100,
                    "updated_at_min": updated_at_min,
                },
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
                }
            }
        ]
    }
    
    source = rest_api_source(config)
    
    # Fetch orders once with full nested data
    for order in source.with_resources("orders"):
        request_counter.increment()
        yield order  # Keep full order with nested arrays


@dlt.resource(
    name="faire_products_raw",
    write_disposition="replace",
    primary_key="id"
)
def faire_products_raw_resource() -> Iterator[Dict[str, Any]]:
    """
    Fetch Faire products from API with full nested data.
    
    This is the base resource that fetches from the API once.
    Transformer resources process this data into normalized tables.
    
    Images are excluded at API level via fields parameter to reduce bandwidth.
    
    Yields:
        Complete product dicts with nested variants array (no images)
    """
    headers = get_faire_auth_headers()
    
    # Specify fields to include (exclude images at product and variant level)
    # Faire API supports field filtering via 'fields' parameter
    fields_to_include = [
        "id","created_at","updated_at","name","description","short_description",
        "sale_state","lifecycle_state","variants","unit_multiplier","per_style_minimum_order_quantity",
        "allow_sales_when_out_of_stock","taxonomy_type","preorderable","preorder_details",
        "product_attirbutes","made_in_country"
    ]
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.faire.com/external-api/v2",
            "headers": headers,
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": 100,
                    "fields": ",".join(fields_to_include),  # Exclude images at API level
                },
                "paginator": {
                    "type": "cursor",
                    "cursor_path": "cursor",
                    "cursor_param": "cursor",
                },
            }
        },
        "resources": [
            {
                "name": "products",
                "endpoint": {
                    "path": "products",
                    "data_selector": "products",
                }
            }
        ]
    }
    
    source = rest_api_source(config)
    
    # Fetch products once with full nested data (images excluded at API level)
    for product in source.with_resources("products"):
        request_counter.increment()
        yield product


# ============================================================================
# DLT TRANSFORMERS (PROCESS FETCHED DATA)
# ============================================================================

@dlt.transformer(
    name="faire_orders",
    write_disposition="merge",
    primary_key="id"
)
def faire_orders_transformer(
    order: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw order data into parent order records (no nested arrays).
    
    This transformer depends on faire_orders_raw_resource and processes
    the same data without making additional API calls.
    
    Args:
        order: Complete order dict from faire_orders_raw_resource
        
    Yields:
        Flattened order dict without items/shipments arrays
    """
    order_copy = order.copy()
    order_copy.pop("items", None)
    order_copy.pop("shipments", None)
    yield order_copy


@dlt.transformer(
    name="faire_order_items",
    write_disposition="merge",
    primary_key="id"
)
def faire_order_items_transformer(
    order: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw order data into order items records.
    
    Extracts items array from order and yields flattened records
    with order_id foreign key.
    
    Args:
        order: Complete order dict from faire_orders_raw_resource
        
    Yields:
        Flattened order item dicts with order_id FK
    """
    items = flatten_order_items(order)
    for item in items:
        yield item


@dlt.transformer(
    name="faire_order_shipments",
    write_disposition="merge",
    primary_key="id"
)
def faire_order_shipments_transformer(
    order: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw order data into shipment records.
    
    Extracts shipments array from order and yields flattened records
    with order_id foreign key.
    
    Args:
        order: Complete order dict from faire_orders_raw_resource
        
    Yields:
        Flattened shipment dicts with order_id FK
    """
    shipments = flatten_order_shipments(order)
    for shipment in shipments:
        yield shipment


@dlt.transformer(
    name="faire_products",
    write_disposition="replace",
    primary_key="id"
)
def faire_products_transformer(
    product: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw product data into parent product records (no nested arrays).
    
    Embeds taxonomy_type fields directly in product record.
    
    This transformer depends on faire_products_raw_resource and processes
    the same data without making additional API calls.
    
    Args:
        product: Complete product dict from faire_products_raw_resource
        
    Yields:
        Flattened product dict without images/variants (images excluded per requirements)
    """
    product_copy = product.copy()
    
    # Remove nested arrays/objects (handled by other transformers)
    product_copy.pop("images", None)
    product_copy.pop("variants", None)
    product_copy.pop("variant_option_sets", None)
    product_copy.pop("product_attributes", None)
    
    # Embed taxonomy_type fields (if present)
    taxonomy_type = product_copy.pop("taxonomy_type", None)
    if taxonomy_type:
        product_copy["taxonomy_type_id"] = taxonomy_type.get("id")
        product_copy["taxonomy_type_name"] = taxonomy_type.get("name")
    
    yield product_copy


@dlt.transformer(
    name="faire_product_variants",
    write_disposition="replace",
    primary_key="id"
)
def faire_product_variants_transformer(
    product: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw product data into variant records.
    
    Extracts variants array from product and yields flattened records
    with product_id foreign key. Excludes variant images.
    
    Args:
        product: Complete product dict from faire_products_raw_resource
        
    Yields:
        Flattened variant dicts with product_id FK (no images)
    """
    variants = flatten_product_variants(product)
    for variant in variants:
        yield variant


@dlt.transformer(
    name="faire_product_variant_option_sets",
    write_disposition="replace",
    primary_key="id"
)
def faire_product_variant_option_sets_transformer(
    product: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw product data into variant option sets records.
    
    Extracts variant_option_sets array from product and yields flattened records
    with product_id foreign key. Values stored as PostgreSQL array.
    
    Args:
        product: Complete product dict from faire_products_raw_resource
        
    Yields:
        Flattened option set dicts with product_id FK
    """
    option_sets = flatten_product_variant_option_sets(product)
    for option_set in option_sets:
        yield option_set


@dlt.transformer(
    name="faire_product_attributes",
    write_disposition="replace",
    primary_key="id"
)
def faire_product_attributes_transformer(
    product: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw product data into product attributes records.
    
    Extracts product_attributes array from product and yields flattened records
    with product_id foreign key.
    
    Args:
        product: Complete product dict from faire_products_raw_resource
        
    Yields:
        Flattened attribute dicts with product_id FK
    """
    attributes = flatten_product_attributes(product)
    for attribute in attributes:
        yield attribute


@dlt.transformer(
    name="faire_variant_prices",
    write_disposition="replace",
    primary_key=["variant_id", "country", "country_group"]
)
def faire_variant_prices_transformer(
    product: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    """
    Transform raw product data into variant prices records.
    
    Extracts prices array from all variants in product and yields flattened records
    with variant_id foreign key. Uses composite key for geo-specific pricing.
    
    Args:
        product: Complete product dict from faire_products_raw_resource
        
    Yields:
        Flattened price dicts with variant_id FK and geo constraints
    """
    prices = flatten_variant_prices(product)
    for price in prices:
        yield price


# ============================================================================
# DLT SOURCE & PIPELINE
# ============================================================================

@dlt.source
def faire_source(
    updated_at_min: Optional[str] = None
) -> List[Any]:
    """
    Faire wholesale data source combining all resources.
    
    Uses transformer pattern: fetches API data once per endpoint,
    then fans out to multiple normalized tables.
    
    Args:
        updated_at_min: ISO 8601 timestamp for incremental order loading.
                       If None, uses 2024-01-01T00:00:00Z
    
    Returns:
        List of dlt resources with transformers chained
    """
    if updated_at_min is None:
        updated_at_min = "2024-01-01T00:00:00Z"
    
    # Base resources (fetch from API)
    orders_raw = faire_orders_raw_resource(updated_at_min=updated_at_min)
    products_raw = faire_products_raw_resource()
    
    # Transformers (process fetched data without additional API calls)
    return [
        orders_raw,
        faire_orders_transformer(orders_raw),
        faire_order_items_transformer(orders_raw),
        faire_order_shipments_transformer(orders_raw),
        products_raw,
        faire_products_transformer(products_raw),
        faire_product_variants_transformer(products_raw),
        faire_product_variant_option_sets_transformer(products_raw),
        faire_product_attributes_transformer(products_raw),
        faire_variant_prices_transformer(products_raw),
    ]


def load_to_postgres(updated_at_min: Optional[str] = None):
    """
    Load extracted Faire data to PostgreSQL using dlt.
    
    Creates tables in faire_raw schema:
    - faire_orders (parent)
    - faire_order_items (child, FK: order_id)
    - faire_order_shipments (child, FK: order_id)
    - faire_products (parent, taxonomy_type embedded)
    - faire_product_variants (child, FK: product_id)
    - faire_product_variant_option_sets (child, FK: product_id)
    - faire_product_attributes (child, FK: product_id)
    - faire_variant_prices (child, FK: variant_id, composite key)
    
    Args:
        updated_at_min: ISO 8601 timestamp for incremental order loading
        
    Returns:
        dlt.Pipeline: Completed pipeline with load info
    """
    # Reset request counter
    request_counter.reset()
    
    # Initialize pipeline
    pipeline = dlt.pipeline(
        pipeline_name="faire",
        destination="postgres",
        dataset_name="faire_raw"
    )
    
    # Run pipeline
    print(f"Starting Faire extraction (orders since {updated_at_min or '2024-01-01'})...")
    load_info = pipeline.run(faire_source(updated_at_min=updated_at_min))
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Faire Pipeline Completed")
    print(f"{'='*60}")
    print(f"Requests: {request_counter.summary()}")
    print(f"Load ID: {load_info.loads_ids[0] if load_info.loads_ids else 'N/A'}")
    print(f"{'='*60}\n")
    
    return pipeline


if __name__ == "__main__":
    # For testing individual source extraction
    print("Extracting Faire data...")
    load_to_postgres()
    print("Faire pipeline completed")
