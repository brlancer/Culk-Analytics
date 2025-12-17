"""
ShipHero 3PL Inventory & Shipments Extraction

Data Source: ShipHero 3PL inventory/shipments via GraphQL API
API Type: GraphQL
Key Queries:
  - products { ... } for inventory snapshots
  - shipments { ... } for fulfillment data
  - inventory_changes { ... } for inventory movements

Extraction Method: Custom GraphQL queries with aiohttp, load via dlt pipeline
Incremental Strategy: Filter by updated_at or created_at in GraphQL variables

Rate Limits:
  - 50,000 complexity points per hour
  - Each query has a complexity score (check response headers)
  
Authentication: OAuth access token (Authorization: Bearer ...)
Note: Tokens expire - implement refresh token flow

Important: Monitor complexity usage carefully to avoid rate limit issues
"""

import dlt
import aiohttp
import asyncio
from typing import AsyncGenerator, Optional, Dict, Any, List
from datetime import datetime


# GraphQL Queries
PRODUCTS_QUERY = """
query getProducts($cursor: String, $updatedFrom: ISODateTime) {
  products(updated_from: $updatedFrom) {
    complexity
    data(first: 25, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        cursor
        node {
          id
          legacy_id
          sku
          name
          barcode
          created_at
          updated_at
          warehouse_products {
            id
            warehouse_id
            warehouse_identifier
            on_hand
            allocated
            available
            backorder
            reserve_inventory
            price
            value
            value_currency
          }
        }
      }
    }
  }
}
"""

ORDERS_QUERY = """
query getOrders($cursor: String, $warehouseId: String!, $updatedFrom: ISODateTime) {
  orders(warehouse_id: $warehouseId, updated_from: $updatedFrom) {
    complexity
    request_id
    data(first: 25, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        cursor
        node {
          id
          order_number
          order_date
          fulfillment_status
          line_items(first: 100) {
            edges {
              node {
                sku
                product_name
                quantity
                quantity_allocated
                quantity_pending_fulfillment
                backorder_quantity
                quantity_shipped
              }
            }
          }
          shipments {
            id
            total_packages
            shipping_labels {
              created_date
              cost
              refunded
              status
              tracking_number
              tracking_status
              carrier
              shipping_name
              shipping_method
            }
          }
        }
      }
    }
  }
}
"""


async def fetch_shiphero_graphql(
    query: str,
    variables: Dict[str, Any],
    session: aiohttp.ClientSession
) -> Dict[str, Any]:
    """
    Execute a GraphQL query against ShipHero API with complexity monitoring.
    
    Args:
        query: GraphQL query string
        variables: Query variables (cursor, filters)
        session: aiohttp session
    
    Returns:
        GraphQL response data with complexity information
    
    Raises:
        Exception: If GraphQL errors or HTTP errors occur
    """
    access_token = dlt.secrets["sources.shiphero.access_token"]
    
    url = "https://public-api.shiphero.com/graphql"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    async with session.post(
        url,
        json={"query": query, "variables": variables},
        headers=headers
    ) as response:
        response.raise_for_status()
        data = await response.json()
        
        # Check for GraphQL errors
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        
        # Extract complexity information from response data
        result_data = data.get("data", {})
        
        # Get the first key in data (products, shipments, etc.)
        if result_data:
            first_key = next(iter(result_data.keys()))
            resource_data = result_data[first_key]
            
            if "complexity" in resource_data:
                complexity = resource_data["complexity"]
                print(f"Query complexity: {complexity}")
                
                # Parse throttle status from response headers if available
                complexity_available = response.headers.get("X-Complexity-Available")
                complexity_max = response.headers.get("X-Complexity-Max")
                
                if complexity_available and complexity_max:
                    available = int(complexity_available)
                    maximum = int(complexity_max)
                    print(
                        f"Complexity status: {available}/{maximum} available "
                        f"({(available/maximum)*100:.1f}%)"
                    )
                    
                    # Warn if approaching limit
                    if available < 10000:
                        print(f"⚠️  WARNING: Low complexity available ({available} points)")
                    
                    # Calculate adaptive delay
                    if available < maximum * 0.5:  # Below 50% capacity
                        restore_rate = 13.89  # ~50k points per hour
                        delay = max(0.5, complexity / restore_rate)
                        print(f"Adaptive delay: {delay:.2f}s to manage complexity")
                        await asyncio.sleep(delay)
        
        return result_data


def flatten_products(products_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten GraphQL products response (edges/nodes) to simple dict structure.
    
    Args:
        products_data: Raw GraphQL response
    
    Returns:
        List of flattened product dicts
    """
    flattened = []
    
    for edge in products_data["data"]["edges"]:
        product = edge["node"]
        
        # Store warehouse_products as JSON for now (can normalize to child table later)
        flattened_product = {
            "id": product["id"],
            "legacy_id": product.get("legacy_id"),
            "sku": product["sku"],
            "name": product["name"],
            "price": product.get("price"),
            "value": product.get("value"),
            "barcode": product.get("barcode"),
            "created_at": product["created_at"],
            "updated_at": product["updated_at"],
            "warehouse_products": product.get("warehouse_products", [])  # Keep as nested JSON
        }
        
        flattened.append(flattened_product)
    
    return flattened


def flatten_orders(orders_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten GraphQL orders response.
    
    Args:
        orders_data: Raw GraphQL response
    
    Returns:
        List of flattened order dicts
    """
    flattened = []
    
    for edge in orders_data["data"]["edges"]:
        order = edge["node"]
        
        # Flatten line items into JSON array
        line_items = [
            {
                "sku": li["node"]["sku"],
                "product_name": li["node"]["product_name"],
                "quantity": li["node"]["quantity"],
                "quantity_allocated": li["node"]["quantity_allocated"],
                "quantity_pending_fulfillment": li["node"]["quantity_pending_fulfillment"],
                "backorder_quantity": li["node"]["backorder_quantity"],
                "quantity_shipped": li["node"]["quantity_shipped"]
            }
            for li in order["line_items"]["edges"]
        ] if order.get("line_items") else []
        
        # Flatten shipments array (keep as nested JSON with shipping labels)
        shipments = [
            {
                "id": shipment["id"],
                "total_packages": shipment.get("total_packages"),
                "shipping_labels": [
                    {
                        "created_date": label.get("created_date"),
                        "cost": label.get("cost"),
                        "refunded": label.get("refunded"),
                        "status": label.get("status"),
                        "tracking_number": label.get("tracking_number"),
                        "tracking_status": label.get("tracking_status"),
                        "carrier": label.get("carrier"),
                        "shipping_name": label.get("shipping_name"),
                        "shipping_method": label.get("shipping_method")
                    }
                    for label in shipment.get("shipping_labels", [])
                ]
            }
            for shipment in order.get("shipments", [])
        ]
        
        flattened_order = {
            "id": order["id"],
            "order_number": order.get("order_number"),
            "order_date": order.get("order_date"),
            "fulfillment_status": order.get("fulfillment_status"),
            "line_items": line_items,  # Keep as nested JSON
            "shipments": shipments  # Keep as nested JSON
        }
        
        flattened.append(flattened_order)
    
    return flattened


@dlt.resource(write_disposition="merge", primary_key="id")
async def products(
    updated_from: str = "2024-01-01T00:00:00Z"
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Extract products from ShipHero GraphQL API with incremental loading.
    
    Args:
        updated_from: Filter products updated after this timestamp
    
    Yields:
        Flattened product dicts
    """
    cursor = None
    has_next_page = True
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            variables = {
                "cursor": cursor,
                "updatedFrom": updated_from
            }
            
            print(f"Fetching products (cursor: {cursor})...")
            data = await fetch_shiphero_graphql(PRODUCTS_QUERY, variables, session)
            products_data = data["products"]
            
            # Flatten and yield
            flattened_products = flatten_products(products_data)
            print(f"Retrieved {len(flattened_products)} products")
            
            for product in flattened_products:
                yield product
            
            # Pagination
            page_info = products_data["data"]["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            cursor = page_info.get("endCursor")
            
            # Base delay between requests
            if has_next_page:
                await asyncio.sleep(0.5)


@dlt.resource(write_disposition="merge", primary_key="id")
async def orders(
    warehouse_id: str,
    updated_from: str = "2024-01-01T00:00:00Z"
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Extract orders from ShipHero GraphQL API with incremental loading.
    
    Args:
        warehouse_id: ShipHero warehouse ID (required)
        updated_from: Filter orders updated after this timestamp
    
    Yields:
        Flattened order dicts
    """
    cursor = None
    has_next_page = True
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            variables = {
                "cursor": cursor,
                "warehouseId": warehouse_id,
                "updatedFrom": updated_from
            }
            
            print(f"Fetching orders (cursor: {cursor})...")
            data = await fetch_shiphero_graphql(ORDERS_QUERY, variables, session)
            orders_data = data["orders"]
            
            # Flatten and yield
            flattened_orders = flatten_orders(orders_data)
            print(f"Retrieved {len(flattened_orders)} orders")
            
            for order in flattened_orders:
                yield order
            
            # Pagination
            page_info = orders_data["data"]["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            cursor = page_info.get("endCursor")
            
            # Base delay between requests
            if has_next_page:
                await asyncio.sleep(0.5)


@dlt.source
def shiphero_source(
    warehouse_id: Optional[str] = None,
    updated_from: Optional[str] = None
) -> List[Any]:
    """
    Main dlt source for ShipHero GraphQL data.
    
    Args:
        warehouse_id: ShipHero warehouse ID (required for orders)
        updated_from: Override default incremental start date for products and orders
    
    Returns:
        List of dlt resources
    """
    if updated_from is None:
        updated_from = "2024-01-01T00:00:00Z"
    
    # Get warehouse_id from secrets if not provided
    if warehouse_id is None:
        warehouse_id = dlt.secrets.get("sources.shiphero.warehouse_id")
    
    if not warehouse_id:
        raise ValueError("warehouse_id must be provided or set in secrets.toml")
    
    return [
        products(updated_from),
        orders(warehouse_id, updated_from)
    ]


def load_to_postgres():
    """
    Load ShipHero data to PostgreSQL using dlt.
    """
    print("Initializing ShipHero pipeline...")
    
    pipeline = dlt.pipeline(
        pipeline_name="shiphero",
        destination="postgres",
        dataset_name="shiphero_raw"
    )
    
    print("Running extraction and load...")
    
    # Run the source
    load_info = pipeline.run(shiphero_source())
    
    print(f"\n{'='*60}")
    print("Load Summary:")
    print(f"{'='*60}")
    print(load_info)
    print(f"{'='*60}\n")
    
    return load_info


if __name__ == "__main__":
    # For testing individual source extraction
    print("\n" + "="*60)
    print("ShipHero GraphQL Extraction Pipeline")
    print("="*60 + "\n")
    
    try:
        load_info = load_to_postgres()
        print("\n✅ ShipHero pipeline completed successfully!")
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        raise
