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
import logging
import json
import sys
from typing import AsyncGenerator, Optional, Dict, Any, List
from datetime import datetime
from pathlib import Path

# Import token refresh utility
try:
    from ingestion.utils.shiphero_token_refresh import refresh_token_if_needed
except ImportError:
    # Fallback if running from different directory
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from ingestion.utils.shiphero_token_refresh import refresh_token_if_needed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Error log directory
ERROR_LOG_DIR = Path(__file__).parent.parent / "logs" / "shiphero_errors"
ERROR_LOG_DIR.mkdir(parents=True, exist_ok=True)


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


class ShipHeroAPIError(Exception):
    """Custom exception for ShipHero API errors."""
    pass


def save_error_context(error_type: str, query: str, variables: Dict[str, Any], 
                       response_data: Any, error_msg: str) -> None:
    """Save error context to log file for debugging."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_file = ERROR_LOG_DIR / f"{error_type}_{timestamp}.json"
    
    error_context = {
        "timestamp": timestamp,
        "error_type": error_type,
        "error_message": error_msg,
        "query": query,
        "variables": variables,
        "response": str(response_data)
    }
    
    with open(error_file, 'w') as f:
        json.dump(error_context, f, indent=2)
    
    logger.error(f"Error context saved to: {error_file}")


async def fetch_shiphero_graphql(
    query: str,
    variables: Dict[str, Any],
    session: aiohttp.ClientSession,
    max_retries: int = 3,
    base_delay: float = 1.0
) -> Dict[str, Any]:
    """
    Execute a GraphQL query against ShipHero API with throttle handling and retry logic.
    
    Args:
        query: GraphQL query string
        variables: Query variables (cursor, filters)
        session: aiohttp session
        max_retries: Maximum number of retry attempts for transient failures
        base_delay: Base delay in seconds for exponential backoff
    
    Returns:
        GraphQL response data
    
    Raises:
        ShipHeroAPIError: If GraphQL errors or HTTP errors occur after retries
    """
    # Get access token from secrets
    access_token = dlt.secrets.get("sources.shiphero.access_token")
    if not access_token:
        raise ShipHeroAPIError(
            "ShipHero access token not found. "
            "Set sources.shiphero.access_token in .dlt/secrets.toml"
        )
    
    url = "https://public-api.shiphero.com/graphql"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Retry loop with exponential backoff
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            async with session.post(
                url,
                json={"query": query, "variables": variables},
                headers=headers
            ) as response:
                # Handle rate limiting (429)
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited (429). Waiting {retry_after}s before retry...")
                    await asyncio.sleep(retry_after)
                    continue
                
                # Handle unauthorized (401) - token expired
                if response.status == 401:
                    raise ShipHeroAPIError(
                        "ShipHero token expired (401). Please refresh manually:\n"
                        "1. Get new token from ShipHero OAuth\n"
                        "2. Update sources.shiphero.access_token in .dlt/secrets.toml\n"
                        "3. Update sources.shiphero.token_expires_at in .dlt/config.toml"
                    )
                
                # Handle other HTTP errors
                if response.status >= 400:
                    error_text = await response.text()
                    logger.error(f"HTTP {response.status}: {error_text}")
                    
                    # Save error context
                    save_error_context(
                        f"http_{response.status}",
                        query,
                        variables,
                        error_text,
                        f"HTTP error {response.status}"
                    )
                    
                    # Retry on server errors (5xx)
                    if response.status >= 500:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            logger.info(f"Server error. Retrying in {delay}s... (attempt {attempt + 1}/{max_retries})")
                            await asyncio.sleep(delay)
                            continue
                    
                    raise ShipHeroAPIError(f"HTTP {response.status}: {error_text}")
                
                # Parse response
                data = await response.json()
                
                # Check for GraphQL errors
                if "errors" in data:
                    # Check for throttling error (code 30)
                    for error in data["errors"]:
                        if error.get("code") == 30:
                            time_remaining_str = error.get("time_remaining", "60 seconds")
                            wait_time = int(time_remaining_str.split()[0])
                            logger.warning(f"ShipHero throttling (code 30). Waiting {wait_time}s before retry...")
                            await asyncio.sleep(wait_time)
                            # Break inner loop to retry request
                            break
                    else:
                        # No throttling error found - log and raise
                        error_msg = json.dumps(data['errors'], indent=2)
                        logger.error(f"GraphQL errors: {error_msg}")
                        
                        # Save error context
                        save_error_context(
                            "graphql_error",
                            query,
                            variables,
                            data,
                            error_msg
                        )
                        
                        raise ShipHeroAPIError(f"GraphQL errors: {error_msg}")
                    
                    # Continue outer loop to retry after throttle wait
                    continue
                
                # Success - break retry loop
                break
        
        except aiohttp.ClientError as e:
            last_exception = e
            logger.warning(f"Network error (attempt {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"Retrying in {delay}s...")
                await asyncio.sleep(delay)
            else:
                raise ShipHeroAPIError(f"Network error after {max_retries} attempts: {e}") from e
        
        except ShipHeroAPIError:
            # Don't retry - propagate immediately
            raise
        
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error (attempt {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"Retrying in {delay}s...")
                await asyncio.sleep(delay)
            else:
                raise ShipHeroAPIError(f"Unexpected error after {max_retries} attempts: {e}") from e
    
    if last_exception:
        raise ShipHeroAPIError(f"Failed after {max_retries} attempts") from last_exception
    
    # Simple complexity logging
    result_data = data.get("data", {})
    if result_data:
        first_key = next(iter(result_data.keys()))
        resource_data = result_data.get(first_key, {})
        
        # Log complexity if available
        if "complexity" in resource_data:
            logger.info(f"Query complexity: {resource_data['complexity']}")
    
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
    
    Raises:
        ShipHeroAPIError: If extraction fails after retries
    """
    cursor = None
    has_next_page = True
    page_count = 0
    total_products = 0
    
    logger.info(f"Starting products extraction (updated_from: {updated_from})")
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            page_count += 1
            variables = {
                "cursor": cursor,
                "updatedFrom": updated_from
            }
            
            try:
                logger.info(f"Fetching products page {page_count} (cursor: {cursor})...")
                data = await fetch_shiphero_graphql(PRODUCTS_QUERY, variables, session)
                products_data = data["products"]
                
                # Flatten and yield
                flattened_products = flatten_products(products_data)
                logger.info(f"Retrieved {len(flattened_products)} products")
                total_products += len(flattened_products)
                
                for product in flattened_products:
                    yield product
                
                # Pagination
                page_info = products_data["data"]["pageInfo"]
                has_next_page = page_info["hasNextPage"]
                cursor = page_info.get("endCursor")
                
                # Fixed delay between requests to respect rate limits
                if has_next_page:
                    await asyncio.sleep(0.5)
            
            except ShipHeroAPIError as e:
                logger.error(f"API error at page {page_count}: {e}")
                raise
    
    logger.info(f"Products extraction complete: {total_products} products across {page_count} pages")


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
    
    Raises:
        ShipHeroAPIError: If extraction fails after retries
    """
    cursor = None
    has_next_page = True
    page_count = 0
    total_orders = 0
    
    logger.info(f"Starting orders extraction (warehouse_id: {warehouse_id}, updated_from: {updated_from})")
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            page_count += 1
            variables = {
                "cursor": cursor,
                "warehouseId": warehouse_id,
                "updatedFrom": updated_from
            }
            
            try:
                logger.info(f"Fetching orders page {page_count} (cursor: {cursor})...")
                data = await fetch_shiphero_graphql(ORDERS_QUERY, variables, session)
                orders_data = data["orders"]
                
                # Flatten and yield
                flattened_orders = flatten_orders(orders_data)
                logger.info(f"Retrieved {len(flattened_orders)} orders")
                total_orders += len(flattened_orders)
                
                for order in flattened_orders:
                    yield order
                
                # Pagination
                page_info = orders_data["data"]["pageInfo"]
                has_next_page = page_info["hasNextPage"]
                cursor = page_info.get("endCursor")
                
                # Fixed delay between requests to respect rate limits
                if has_next_page:
                    await asyncio.sleep(0.5)
            
            except ShipHeroAPIError as e:
                logger.error(f"API error at page {page_count}: {e}")
                raise
    
    logger.info(f"Orders extraction complete: {total_orders} orders across {page_count} pages")


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
    Load ShipHero data to PostgreSQL using dlt with comprehensive error handling.
    
    Returns:
        LoadInfo object with pipeline execution details
    
    Raises:
        Exception: If pipeline execution fails
    """
    # Check and refresh token if needed before starting pipeline
    logger.info("Checking ShipHero token status...")
    if not refresh_token_if_needed():
        raise ShipHeroAPIError(
            "ShipHero token is expired and automatic refresh failed. "
            "Please refresh token manually."
        )
    
    logger.info("Initializing ShipHero pipeline...")
    
    try:
        pipeline = dlt.pipeline(
            pipeline_name="shiphero",
            destination="postgres",
            dataset_name="shiphero_raw"
        )
        
        logger.info("Running extraction and load...")
        
        # Run the source
        load_info = pipeline.run(shiphero_source())
        
        logger.info("="*60)
        logger.info("Load Summary:")
        logger.info("="*60)
        logger.info(str(load_info))
        logger.info("="*60)
        
        # Check for errors in load_info
        if hasattr(load_info, 'has_failed_jobs') and load_info.has_failed_jobs:
            logger.error("Pipeline completed with failed jobs")
            for job in load_info.failed_jobs:
                logger.error(f"Failed job: {job}")
        
        return load_info
    
    except ShipHeroAPIError as e:
        logger.error(f"Pipeline failed due to API error: {e}")
        raise
    
    except Exception as e:
        logger.error(f"Pipeline failed with unexpected error: {e}")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    # For testing individual source extraction
    logger.info("\n" + "="*60)
    logger.info("ShipHero GraphQL Extraction Pipeline")
    logger.info("="*60 + "\n")
    
    try:
        load_info = load_to_postgres()
        logger.info("\n✅ ShipHero pipeline completed successfully!")
        logger.info(f"Logs saved to: {ERROR_LOG_DIR}")
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Pipeline interrupted by user")
        raise
    except ShipHeroAPIError as e:
        logger.error(f"\n❌ Pipeline failed - API error: {e}")
        logger.info(f"Check error logs in: {ERROR_LOG_DIR}")
        raise
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed with unexpected error: {e}")
        logger.exception("Full traceback:")
        raise
