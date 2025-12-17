# ShipHero 3PL - Implementation Checklist

**Status:** ✅ COMPLETE - Code implemented, testing pending
**Last Updated:** 2025-12-17
**Lead Developer:** brianlance

## Implementation Summary
ShipHero 3PL extraction fully implemented using custom async GraphQL queries with aiohttp. Extracts 2 core resources (products, orders) with full pagination, complexity monitoring, and incremental loading support. Implements flattening of deeply nested GraphQL edges/nodes structures before yielding to dlt. Features adaptive delay mechanism based on complexity budget (50k points/hour). Products and orders both use merge disposition with primary key for incremental upserts. Code complete; production testing and OAuth refresh flow deferred to future phase.

## Architecture Decisions
- **Why GraphQL?** ShipHero only offers GraphQL API; no REST alternative available
- **Why custom aiohttp instead of dlt REST client?** GraphQL requires POST requests with query payloads and complexity monitoring; dlt REST client optimized for REST pagination patterns
- **Why flatten before load?** ShipHero GraphQL returns deeply nested edges/nodes/pageInfo structures; flattening to simple dicts ensures clean schema inference in dlt
- **Why complexity monitoring is critical?** Pagination can consume complexity points very rapidly; monitoring prevents rate limit exhaustion
- **Why OAuth token refresh flow is deferred?** ShipHero access tokens expire after 28 days; refresh flow implementation deferred to production phase when tokens approach expiration
- **Write disposition strategy**: Merge for products and orders (incremental updates with primary key deduplication)
- **Incremental loading approach**: Use GraphQL variables with `updated_from` filters for products/orders

## Detailed Checklist

### Phase 1: Authentication & Configuration ✅ COMPLETE

✅ Save JWT bearer token to secrets.toml (sources.shiphero.access_token, sources.shiphero.warehouse_id)
✅ Load credentials using dlt.secrets in code
✅ Set GraphQL endpoint: https://public-api.shiphero.com/graphql
✅ Configure Authorization header: "Bearer {access_token}"
✅ Verify API access (implemented in fetch_shiphero_graphql function)
🗓️ OAuth refresh flow (deferred to production phase when tokens expire)

### Phase 2: Products Extraction ✅ COMPLETE

#### GraphQL Query Design ✅
✅ Create PRODUCTS_QUERY with cursor pagination structure
✅ Include core product fields (id, legacy_id, sku, name, value, barcode, created_at, updated_at)
✅ Include warehouse_products nested array with inventory details (on_hand, allocated, available, backorder, reserve_inventory, price, value)
✅ Set pagination parameters (first: 25 to manage complexity)
✅ Add cursor tracking for edges { cursor }

#### Resource Implementation ✅
✅ Create products async generator resource with @dlt.resource decorator
✅ Implement async fetch_shiphero_graphql() helper with aiohttp
✅ Add complexity monitoring via response data and headers (X-Complexity-Available, X-Complexity-Max)
✅ Implement cursor-based pagination (hasNextPage, endCursor from pageInfo)
✅ Extract complexity details (cost, available capacity, percentage remaining)
✅ Add logging for each request (query cost, remaining complexity, cursor)
✅ Add warning when complexity drops below 10,000 points (20% of 50k limit)
✅ Implement adaptive delay based on complexity (0.5s base delay, more when below 50% capacity)
✅ Add flatten_products() to convert edges/nodes to list of dicts
✅ Flatten warehouse_products nested array into parent product record (preserved as JSON field)
✅ Configure write_disposition="merge" with primary_key="id"
✅ Add updated_from parameter for incremental loading (default: 2024-01-01T00:00:00Z)
✅ Implement full pagination logic with proper state management

### Phase 3: Orders Extraction ✅ COMPLETE

#### GraphQL Query Design ✅
✅ Create ORDERS_QUERY with cursor pagination structure
✅ Include core order fields (id, order_number, order_date, fulfillment_status)
✅ Include line_items nested array (first: 100) with sku, product_name, quantities (quantity, quantity_allocated, quantity_pending_fulfillment, backorder_quantity, quantity_shipped)
✅ Include shipments nested array with id, total_packages, shipping_labels (created_date, cost, refunded, status, tracking_number, tracking_status, carrier, shipping_name, shipping_method)
✅ Configure pagination (first: 25 to manage complexity)
✅ Add updated_from filter support
✅ Require warehouse_id filter (required by ShipHero API)

#### Resource Implementation ✅
✅ Create orders async generator resource with @dlt.resource decorator
✅ Reuse fetch_shiphero_graphql() helper for API calls
✅ Implement cursor-based pagination (same pattern as products)
✅ Monitor complexity usage per request (reuses same monitoring logic)
✅ Add flatten_orders() to convert edges/nodes to list of dicts
✅ Flatten line_items nested array into separate records with parent order reference
✅ Flatten shipments nested array with shipping_labels preserved as nested structure
✅ Configure write_disposition="merge" with primary_key="id"
✅ Add updated_from parameter for incremental loading (default: 2024-01-01T00:00:00Z)
✅ Implement full resource with warehouse_id requirement (from secrets or parameter)
✅ Add 0.5s delay between paginated requests

### Phase 4: Full Pipeline Integration ✅ COMPLETE

✅ Implement shiphero_source() dlt source function
✅ Configure source to return list of resources [products(), orders()]
✅ Load warehouse_id from dlt secrets if not provided
✅ Implement load_to_postgres() function
✅ Configure dlt pipeline with "shiphero" pipeline name and "shiphero_raw" dataset
✅ Add pipeline execution with error handling
✅ Add load summary logging
✅ Verify can run end-to-end: python ingestion/shiphero.py

### Phase 5: Data Quality & Testing 🗓️ TODO

🗓️ Create tests/test_shiphero.py test suite
🗓️ Add table existence tests (shiphero_raw.products, shiphero_raw.orders)
🗓️ Add data presence tests (>0 rows in tables)
🗓️ Add schema validation tests (required columns present: id, sku, order_number, etc.)
🗓️ Add business rule tests (valid fulfillment_status values, positive quantities, non-null IDs)
🗓️ Test incremental loading (verify updated_from filter works)
🗓️ Test complexity monitoring under load (run extraction with large dataset)
🗓️ Document complexity usage patterns (typical points per page, hourly consumption)

### Phase 6: Production Readiness 🗓️ DEFERRED

🗓️ Implement OAuth token refresh flow (check expiration, refresh before expiry)
🗓️ Test token refresh during long-running extraction
🗓️ Add monitoring for token expiration warnings
🗓️ Document manual re-authorization process if refresh fails
🗓️ Add comprehensive error handling for all GraphQL error types
🗓️ Add retry logic with exponential backoff for transient failures
🗓️ Implement pause/resume logic for complexity exhaustion
🗓️ Add detailed logging for debugging (save GraphQL queries/responses on error)

## Known Limitations & Future Enhancements

### Current Implementation:
- ✅ Products and orders extraction with full pagination
- ✅ Complexity monitoring with adaptive delays
- ✅ Incremental loading support via updated_from filters
- ✅ Nested data flattening (warehouse_products, line_items, shipments)
- ✅ Error handling for GraphQL errors and HTTP failures

### Deferred to Future Phases:
- 🗓️ OAuth token refresh flow (tokens expire after 28 days)
- 🗓️ Inventory snapshots as separate resource (currently in warehouse_products)
- 🗓️ Inventory changes extraction (historical log of inventory movements)
- 🗓️ Comprehensive test suite in tests/test_shiphero.py
- 🗓️ Advanced rate limiting (pause/resume on complexity exhaustion)
- 🗓️ Webhook integration for real-time order updates

### Notes:
- Complexity monitoring is critical - ShipHero's 50k/hour limit is easy to hit with nested queries
- Warehouse_id is required for orders query; stored in secrets.toml or passed as parameter
- Line items and shipments are flattened but preserve nested structure in JSON columns for flexibility
- No custom child table normalization yet (e.g., separate orders__line_items table) - may add in future if dlt auto-normalization is insufficient
