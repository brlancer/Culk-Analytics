"""
Shopify Extraction

Data Source: Shopify commerce hub (B2B + DTC) via GraphQL Admin API
API Type: GraphQL
Endpoint: /admin/api/2025-10/graphql.json

Extraction Method: Custom async GraphQL queries with aiohttp, load via dlt
Incremental Strategy: Use query filters (updated_at:>'timestamp')
Rate Limits: Cost-based (1000 points available, restore at 50/sec)

Operational Context:
    - Faire B2B orders sync into Shopify; Shopify then feeds ShipHero for fulfillment
    - Shopify serves as operational source of truth for all sales; redundancy with Faire expected
    - GraphQL API is recommended by Shopify (REST is legacy as of Oct 2024)

Privacy: Customer PII excluded at query level (no post-processing needed)

Authentication: Admin API access token (X-Shopify-Access-Token header)
"""

import dlt
import aiohttp
import asyncio
from typing import AsyncGenerator, Optional, Dict, Any, List


# GraphQL Queries
ORDERS_QUERY = """
query getOrders($cursor: String, $query: String) {
  orders(
    first: 250
    after: $cursor
    query: $query
    sortKey: UPDATED_AT
  ) {
    edges {
      node {
        id
        legacyResourceId
        name
        createdAt
        updatedAt
        processedAt
        displayFinancialStatus
        displayFulfillmentStatus
        totalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        subtotalPriceSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalTaxSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        totalDiscountsSet {
          shopMoney {
            amount
            currencyCode
          }
        }
        customer {
          id
        }
        shippingAddress {
          city
          provinceCode
          zip
          countryCodeV2
        }
        tags
        sourceIdentifier
        lineItems(first: 250) {
          edges {
            node {
              id
              sku
              name
              title
              quantity
              requiresShipping
              taxable
              originalUnitPriceSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              discountedTotalSet {
                shopMoney {
                  amount
                  currencyCode
                }
              }
              variant {
                id
                legacyResourceId
                product {
                  id
                }
              }
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

PRODUCTS_QUERY = """
query getProducts($cursor: String, $query: String) {
  products(
    first: 250
    after: $cursor
    query: $query
    sortKey: UPDATED_AT
  ) {
    edges {
      node {
        id
        legacyResourceId
        title
        description
        vendor
        productType
        createdAt
        updatedAt
        publishedAt
        status
        tags
        variants(first: 250) {
          edges {
            node {
              id
              legacyResourceId
              sku
              barcode
              title
              price
              compareAtPrice
              inventoryItem {
                id
                legacyResourceId
                measurement {
                  weight {
                    value
                    unit
                  }
                }
              }
              position
              createdAt
              updatedAt
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

CUSTOMERS_QUERY = """
query getCustomers($cursor: String, $query: String) {
  customers(
    first: 250
    after: $cursor
    query: $query
    sortKey: UPDATED_AT
  ) {
    edges {
      node {
        id
        legacyResourceId
        createdAt
        updatedAt
        numberOfOrders
        amountSpent {
          amount
          currencyCode
        }
        state
        tags
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

INVENTORY_QUERY = """
query getInventoryLevels($cursor: String) {
  inventoryItems(
    first: 250
    after: $cursor
  ) {
    edges {
      node {
        id
        legacyResourceId
        inventoryLevels(first: 250) {
          edges {
            node {
              id
              quantities(names: "available") {
                id
                name
                quantity
              }
              location {
                id
                legacyResourceId
                name
              }
              updatedAt
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


async def fetch_shopify_graphql(
    query: str,
    variables: Dict[str, Any],
    session: aiohttp.ClientSession
) -> Dict[str, Any]:
    """
    Execute a GraphQL query against Shopify Admin API.
    
    Args:
        query: GraphQL query string
        variables: Query variables (cursor, filters)
        session: aiohttp session
    
    Returns:
        GraphQL response data
    
    Raises:
        Exception: If GraphQL errors or HTTP errors occur
    """
    shop_url = dlt.secrets["sources.shopify.shop_url"]
    access_token = dlt.secrets["sources.shopify.access_token"]
    
    url = f"https://{shop_url}/admin/api/2025-10/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
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
        
        # Log cost information
        if "extensions" in data and "cost" in data["extensions"]:
            cost_info = data["extensions"]["cost"]
            throttle = cost_info["throttleStatus"]
            print(
                f"Query cost: {cost_info['actualQueryCost']} "
                f"(available: {throttle['currentlyAvailable']}/{throttle['maximumAvailable']})"
            )
            
            # Warn if approaching limit
            if throttle['currentlyAvailable'] < 200:
                print(f"WARNING: Approaching rate limit. Restore rate: {throttle['restoreRate']}/sec")
        
        return data["data"]


def flatten_orders(orders_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten GraphQL orders response (edges/nodes) to simple dict structure.
    
    Args:
        orders_data: Raw GraphQL response
    
    Returns:
        List of flattened order dicts
    """
    flattened = []
    
    for edge in orders_data["edges"]:
        order = edge["node"]
        
        # Flatten price sets
        flattened_order = {
            "id": order["id"],
            "legacy_resource_id": order.get("legacyResourceId"),
            "name": order["name"],
            "created_at": order["createdAt"],
            "updated_at": order["updatedAt"],
            "processed_at": order.get("processedAt"),
            "financial_status": order.get("displayFinancialStatus"),
            "fulfillment_status": order.get("displayFulfillmentStatus"),
            "total_price": order["totalPriceSet"]["shopMoney"]["amount"],
            "currency": order["totalPriceSet"]["shopMoney"]["currencyCode"],
            "subtotal_price": order["subtotalPriceSet"]["shopMoney"]["amount"],
            "total_tax": order["totalTaxSet"]["shopMoney"]["amount"],
            "total_discounts": order["totalDiscountsSet"]["shopMoney"]["amount"],
            "customer_id": order["customer"]["id"] if order.get("customer") else None,
            "shipping_address": order.get("shippingAddress"),  # Already anonymized in query
            "tags": order.get("tags", []),
            "source_identifier": order.get("sourceIdentifier"),
            "line_items": [
                flatten_line_item(li_edge["node"])
                for li_edge in order["lineItems"]["edges"]
            ]
        }
        
        flattened.append(flattened_order)
    
    return flattened


def flatten_line_item(line_item: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a single line item."""
    return {
        "id": line_item["id"],
        "sku": line_item.get("sku"),
        "name": line_item["name"],
        "title": line_item.get("title"),
        "quantity": line_item["quantity"],
        "requires_shipping": line_item.get("requiresShipping"),
        "taxable": line_item.get("taxable"),
        "price": line_item["originalUnitPriceSet"]["shopMoney"]["amount"],
        "currency": line_item["originalUnitPriceSet"]["shopMoney"]["currencyCode"],
        "discounted_total": line_item["discountedTotalSet"]["shopMoney"]["amount"],
        "variant_id": line_item["variant"]["id"] if line_item.get("variant") else None,
        "variant_legacy_id": line_item["variant"].get("legacyResourceId") if line_item.get("variant") else None,
        "product_id": line_item["variant"]["product"]["id"] if line_item.get("variant") and line_item["variant"].get("product") else None
    }


def flatten_products(products_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten GraphQL products response."""
    flattened = []
    
    for edge in products_data["edges"]:
        product = edge["node"]
        
        flattened_product = {
            "id": product["id"],
            "legacy_resource_id": product.get("legacyResourceId"),
            "title": product["title"],
            "description": product.get("description"),
            "vendor": product.get("vendor"),
            "product_type": product.get("productType"),
            "created_at": product["createdAt"],
            "updated_at": product["updatedAt"],
            "published_at": product.get("publishedAt"),
            "status": product["status"],
            "tags": product.get("tags", []),
            "variants": [
                flatten_variant(v_edge["node"])
                for v_edge in product["variants"]["edges"]
            ]
        }
        
        flattened.append(flattened_product)
    
    return flattened


def flatten_variant(variant: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a product variant."""
    return {
        "id": variant["id"],
        "legacy_resource_id": variant.get("legacyResourceId"),
        "sku": variant.get("sku"),
        "barcode": variant.get("barcode"),
        "title": variant.get("title"),
        "price": variant["price"],
        "compare_at_price": variant.get("compareAtPrice"),
        "inventory_item_id": variant["inventoryItem"]["id"] if variant.get("inventoryItem") else None,
        "inventory_item_legacy_id": variant["inventoryItem"].get("legacyResourceId") if variant.get("inventoryItem") else None,
        "weight": variant.get("inventoryItem", {}).get("measurement", {}).get("weight", {}).get("value"),
        "weight_unit": variant.get("inventoryItem", {}).get("measurement", {}).get("weight", {}).get("unit"),
        "position": variant.get("position"),
        "created_at": variant["createdAt"],
        "updated_at": variant["updatedAt"]
    }


def flatten_customers(customers_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten GraphQL customers response."""
    flattened = []
    
    for edge in customers_data["edges"]:
        customer = edge["node"]
        
        flattened_customer = {
            "id": customer["id"],
            "legacy_resource_id": customer.get("legacyResourceId"),
            "created_at": customer["createdAt"],
            "updated_at": customer["updatedAt"],
            "number_of_orders": customer.get("numberOfOrders"),
            "amount_spent": customer["amountSpent"]["amount"] if customer.get("amountSpent") else None,
            "amount_spent_currency": customer["amountSpent"]["currencyCode"] if customer.get("amountSpent") else None,
            "state": customer.get("state"),
            "tags": customer.get("tags", [])
        }
        
        flattened.append(flattened_customer)
    
    return flattened


def flatten_inventory(inventory_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten GraphQL inventory levels response."""
    flattened = []
    
    for edge in inventory_data["edges"]:
        inventory_item = edge["node"]
        
        # Each inventory item can have multiple locations
        for level_edge in inventory_item["inventoryLevels"]["edges"]:
            level = level_edge["node"]
            
            # Extract available quantity (quantities is an array, we filter for 'available' name)
            available_qty = None
            if level.get("quantities"):
                for qty in level["quantities"]:
                    if qty.get("name") == "available":
                        available_qty = qty.get("quantity")
                        break
            
            flattened_level = {
                "id": level["id"],
                "inventory_item_id": inventory_item["id"],
                "inventory_item_legacy_id": inventory_item.get("legacyResourceId"),
                "location_id": level["location"]["id"],
                "location_legacy_id": level["location"].get("legacyResourceId"),
                "location_name": level["location"].get("name"),
                "available": available_qty,
                "updated_at": level.get("updatedAt")
            }
            
            flattened.append(flattened_level)
    
    return flattened


@dlt.resource(write_disposition="merge", primary_key="id")
async def orders(
    updated_at_min: str = "2024-01-01T00:00:00Z"
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Extract orders from Shopify GraphQL API with incremental loading.
    
    Args:
        updated_at_min: Filter orders updated after this timestamp
    
    Yields:
        Flattened order dicts
    """
    cursor = None
    has_next_page = True
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            variables = {
                "cursor": cursor,
                "query": f"updated_at:>'{updated_at_min}'"
            }
            
            print(f"Fetching orders (cursor: {cursor})...")
            data = await fetch_shopify_graphql(ORDERS_QUERY, variables, session)
            orders = data["orders"]
            
            # Flatten and yield
            flattened_orders = flatten_orders(orders)
            print(f"Retrieved {len(flattened_orders)} orders")
            
            for order in flattened_orders:
                yield order
            
            # Pagination
            page_info = orders["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            cursor = page_info.get("endCursor")
            
            # Small delay to respect rate limits
            if has_next_page:
                await asyncio.sleep(0.2)


@dlt.resource(write_disposition="merge", primary_key="id")
async def products(
    updated_at_min: str = "2024-01-01T00:00:00Z"
) -> AsyncGenerator[Dict[str, Any], None]:
    """Extract products from Shopify GraphQL API."""
    cursor = None
    has_next_page = True
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            variables = {
                "cursor": cursor,
                "query": f"updated_at:>'{updated_at_min}'"
            }
            
            print(f"Fetching products (cursor: {cursor})...")
            data = await fetch_shopify_graphql(PRODUCTS_QUERY, variables, session)
            products = data["products"]
            
            flattened_products = flatten_products(products)
            print(f"Retrieved {len(flattened_products)} products")
            
            for product in flattened_products:
                yield product
            
            page_info = products["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            cursor = page_info.get("endCursor")
            
            if has_next_page:
                await asyncio.sleep(0.2)


@dlt.resource(write_disposition="merge", primary_key="id")
async def customers(
    updated_at_min: str = "2024-01-01T00:00:00Z"
) -> AsyncGenerator[Dict[str, Any], None]:
    """Extract customers (anonymized) from Shopify GraphQL API."""
    cursor = None
    has_next_page = True
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            variables = {
                "cursor": cursor,
                "query": f"updated_at:>'{updated_at_min}'"
            }
            
            print(f"Fetching customers (cursor: {cursor})...")
            data = await fetch_shopify_graphql(CUSTOMERS_QUERY, variables, session)
            customers = data["customers"]
            
            flattened_customers = flatten_customers(customers)
            print(f"Retrieved {len(flattened_customers)} customers")
            
            for customer in flattened_customers:
                yield customer
            
            page_info = customers["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            cursor = page_info.get("endCursor")
            
            if has_next_page:
                await asyncio.sleep(0.2)


@dlt.resource(write_disposition="merge", primary_key=["inventory_item_id", "location_id"])
async def inventory() -> AsyncGenerator[Dict[str, Any], None]:
    """Extract current inventory levels from Shopify GraphQL API."""
    cursor = None
    has_next_page = True
    
    async with aiohttp.ClientSession() as session:
        while has_next_page:
            variables = {"cursor": cursor}
            
            print(f"Fetching inventory levels (cursor: {cursor})...")
            data = await fetch_shopify_graphql(INVENTORY_QUERY, variables, session)
            inventory_items = data["inventoryItems"]
            
            flattened_inventory = flatten_inventory(inventory_items)
            print(f"Retrieved {len(flattened_inventory)} inventory level records")
            
            for inventory_level in flattened_inventory:
                yield inventory_level
            
            page_info = inventory_items["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            cursor = page_info.get("endCursor")
            
            if has_next_page:
                await asyncio.sleep(0.2)


@dlt.source
def shopify_source(
    updated_at_min: Optional[str] = None
) -> List[Any]:
    """
    Main dlt source for Shopify GraphQL data.
    
    Args:
        updated_at_min: Override default incremental start date
    
    Returns:
        List of dlt resources
    """
    if updated_at_min is None:
        # Default to historical cutoff
        updated_at_min = "2024-01-01T00:00:00Z"
    
    return [
        orders(updated_at_min),
        products(updated_at_min),
        customers(updated_at_min),
        inventory()
    ]


def load_to_postgres():
    """
    Load Shopify data to PostgreSQL using dlt.
    """
    print("Initializing Shopify pipeline...")
    
    pipeline = dlt.pipeline(
        pipeline_name="shopify",
        destination="postgres",
        dataset_name="shopify_raw"
    )
    
    print("Running extraction and load...")
    
    # Run the source
    load_info = pipeline.run(shopify_source())
    
    print(f"\n{'='*60}")
    print("Load Summary:")
    print(f"{'='*60}")
    print(load_info)
    print(f"{'='*60}\n")
    
    return load_info


if __name__ == "__main__":
    # For testing individual source extraction
    print("\n" + "="*60)
    print("Shopify GraphQL Extraction Pipeline")
    print("="*60 + "\n")
    
    try:
        load_info = load_to_postgres()
        print("\n✅ Shopify pipeline completed successfully!")
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        raise
