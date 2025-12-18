# ShipHero 3PL - Implementation Checklist

**Status:** ✅ COMPLETE - Production-ready with throttling and error handling
**Last Updated:** 2025-01-17
**Lead Developer:** brianlance

## Implementation Summary
ShipHero 3PL extraction fully implemented using custom async GraphQL queries with aiohttp. Extracts 2 core resources (products, orders) with cursor-based pagination and incremental loading support. Flattens deeply nested GraphQL edges/nodes structures before yielding to dlt. Features basic token refresh flow with expiration checking, retry logic with exponential backoff for transient failures, automatic throttling handling for ShipHero error code 30, and error context logging to logs/shiphero_errors/. Products and orders use merge disposition with primary key for upserts. Comprehensive test suite validates schema, data quality, and business rules. Production-ready.

## Architecture Decisions
- **Why GraphQL?** ShipHero only offers GraphQL API; no REST alternative available
- **Why custom aiohttp instead of dlt REST client?** GraphQL requires POST requests with query payloads; dlt REST client optimized for REST pagination patterns
- **Why flatten before load?** ShipHero GraphQL returns deeply nested edges/nodes/pageInfo structures; flattening to simple dicts ensures clean schema inference in dlt
- **Why token refresh?** ShipHero access tokens expire after 28 days; basic refresh flow ensures continued access
- **Write disposition strategy**: Merge for products and orders (incremental updates with primary key deduplication)
- **Incremental loading approach**: Use GraphQL variables with `updated_from` filters
- **Error handling strategy**: Retry logic with exponential backoff (max 3 retries), **automatic throttling detection and wait (error code 30)**, error context logging to logs/shiphero_errors/
- **Rate limit strategy**: Fixed 0.5s delay between paginated requests; **automatic throttling handling parses time_remaining from error code 30 and waits before retry**; manual intervention if HTTP 429 rate limit hit (triggers wait based on Retry-After header)

## Detailed Checklist

### Phase 1: Authentication & Configuration ✅ COMPLETE

✅ Save JWT bearer token to secrets.toml (sources.shiphero.access_token, sources.shiphero.warehouse_id)
✅ Load credentials using dlt.secrets in code
✅ Set GraphQL endpoint: https://public-api.shiphero.com/graphql
✅ Configure Authorization header: "Bearer {access_token}"
✅ Verify API access (implemented in fetch_shiphero_graphql function)
✅ Basic OAuth refresh flow (ingestion/utils/shiphero_token_refresh.py)

### Phase 2: Products Extraction ✅ COMPLETE

#### GraphQL Query Design ✅
✅ Create PRODUCTS_QUERY with cursor pagination structure
✅ Include core product fields (id, legacy_id, sku, name, barcode, created_at, updated_at)
✅ Include warehouse_products nested array with inventory details (on_hand, allocated, available, backorder, reserve_inventory, price, value)
✅ Set pagination parameters (first: 25)
✅ Add cursor tracking for edges { cursor }

#### Resource Implementation ✅
✅ Create products async generator resource with @dlt.resource decorator
✅ Implement async fetch_shiphero_graphql() helper with aiohttp
✅ Add basic complexity logging from GraphQL response
✅ Implement cursor-based pagination (hasNextPage, endCursor from pageInfo)
✅ Add logging for each request (page number, product count)
✅ Add fixed 0.5s delay between paginated requests
✅ Add flatten_products() to convert edges/nodes to list of dicts
✅ Keep warehouse_products as nested JSON array (dlt auto-normalizes to child table)
✅ Configure write_disposition="merge" with primary_key="id"
✅ Add updated_from parameter for incremental loading (default: 2024-01-01T00:00:00Z)
✅ Implement full pagination logic with proper state management

### Phase 3: Orders Extraction ✅ COMPLETE

#### GraphQL Query Design ✅
✅ Create ORDERS_QUERY with cursor pagination structure
✅ Include core order fields (id, order_number, order_date, fulfillment_status)
✅ Include line_items nested array (first: 100) with sku, product_name, quantities
✅ Include shipments nested array with shipping_labels
✅ Configure pagination (first: 25)
✅ Add updated_from filter support
✅ Require warehouse_id filter (required by ShipHero API)

#### Resource Implementation ✅
✅ Create orders async generator resource with @dlt.resource decorator
✅ Reuse fetch_shiphero_graphql() helper for API calls
✅ Implement cursor-based pagination (same pattern as products)
✅ Add flatten_orders() to convert edges/nodes to list of dicts
✅ Keep line_items and shipments as nested JSON arrays (dlt auto-normalizes to child tables)
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

### Phase 5: Data Quality & Testing ✅ COMPLETE

✅ Create tests/test_shiphero.py test suite
✅ Add table existence tests (shiphero_raw.products, shiphero_raw.orders)
✅ Add data presence tests (>0 rows in tables)
✅ Add schema validation tests (required columns present: id, sku, order_number, etc.)
✅ Add business rule tests (valid fulfillment_status values, positive quantities, non-null IDs)
✅ Add child table structure tests (products__warehouse_products, orders__line_items, orders__shipments)
✅ Add data freshness tests (verify recent loads in _dlt_loads table)

### Phase 6: Production Readiness ✅ COMPLETE

✅ Implement basic OAuth token refresh flow (check expiration, refresh before expiry)
✅ Create ingestion/utils/shiphero_token_refresh.py with token management utilities
✅ Add automatic token refresh check in load_to_postgres()
✅ Document manual re-authorization process in shiphero_token_refresh.py
✅ Add retry logic with exponential backoff for transient failures (max 3 retries in fetch_shiphero_graphql)
✅ **Add automatic throttling detection for ShipHero error code 30 (parses time_remaining field and waits before retry)**
✅ Handle rate limiting (429) with Retry-After header wait
✅ Handle token expiration (401) with clear error message
✅ Handle server errors (5xx) with retries
✅ Add error context logging to logs/shiphero_errors/ (save GraphQL queries/responses on error)
✅ Add custom ShipHeroAPIError exception for clear error messaging

## Known Limitations & Future Enhancements

### Current Implementation:
- ✅ Products and orders extraction with full pagination
- ✅ Basic complexity logging (no proactive monitoring or adaptive delays)
- ✅ Incremental loading support via updated_from filters
- ✅ Nested data kept as JSON arrays (dlt auto-normalizes to child tables: products__warehouse_products, orders__line_items, orders__shipments)
- ✅ Basic error handling (retry logic, error context logging)
- ✅ **Automatic throttling handling for ShipHero error code 30 (waits time_remaining before retry)**
- ✅ Basic OAuth token refresh flow with expiration checking
- ✅ Fixed 0.5s delay between paginated requests
- ✅ Comprehensive test suite in tests/test_shiphero.py

### Deferred to Future Phases (Only If Needed):
- 🗓️ Advanced complexity monitoring with adaptive delays (not currently needed - fixed delay works fine)
- 🗓️ Proactive rate limit management (manual intervention acceptable for now)
- 🗓️ Inventory snapshots as separate resource (currently in warehouse_products child table)
- 🗓️ Inventory changes extraction (historical log of inventory movements)
- 🗓️ Webhook integration for real-time order updates
- 🗓️ Performance optimization for very large datasets

### Notes:
- **Rate Limits**: ShipHero has 50k complexity/hour limit. Fixed 0.5s delay between requests is sufficient. **If throttled (error code 30), pipeline automatically parses time_remaining and waits before retry.** If HTTP 429 rate limit hit, pipeline waits based on Retry-After header. Manual intervention is acceptable for edge cases.
- **OAuth Tokens**: Expire after 28 days. Basic refresh implemented in ingestion/utils/shiphero_token_refresh.py. Manual re-authorization required if refresh token expires (acceptable - happens rarely).
- **Token Expiry**: Stored in .dlt/config.toml (sources.shiphero.token_expires_at)
- **Warehouse ID**: Required for orders query; stored in secrets.toml or passed as parameter
- **Nested Data**: Line items, shipments, and warehouse_products kept as JSON arrays - dlt automatically normalizes to child tables (products__warehouse_products, orders__line_items, orders__shipments, orders__shipments__shipping_labels)
- **Error Context**: GraphQL queries/responses saved to logs/shiphero_errors/ on error for debugging
- **Retry Logic**: Max 3 retries with exponential backoff for transient failures (network errors, 5xx responses)
- **Complexity Monitoring**: Simple logging only - no adaptive delays, pause/resume, or tiered management. **Throttling (error code 30) is handled automatically by parsing time_remaining and waiting before retry.** If complexity issues arise, add manual intervention strategy.
