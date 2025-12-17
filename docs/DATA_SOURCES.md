# Data Sources

Comprehensive overview of all data sources integrated into the Culk Analytics platform.

## Source Summary

| Source Name | API Type | Rate Limits | Key Endpoints | Extraction Method | Notes |
|------------|----------|-------------|---------------|-------------------|-------|
| **Shopify** | GraphQL | Cost-based: 1000 points available, 50/sec restore | `/admin/api/2025-10/graphql.json` | Custom async GraphQL with aiohttp | ✅ **IMPLEMENTED** - Commerce hub for B2B + DTC. Custom GraphQL queries with cost monitoring. 4 resources: orders, products, customers, inventory. Incremental via `updated_at:>` filter. Flattened edges/nodes before dlt load. Privacy-by-design: PII excluded at query level. |
| **Faire** | REST API | 1000 req/hour | `/external-api/v2/orders`<br>`/external-api/v2/products` | dlt REST API client | ✅ **IMPLEMENTED** - Wholesale orders. Dual custom headers (X-FAIRE-APP-CREDENTIALS + X-FAIRE-OAUTH-ACCESS-TOKEN with base64-encoded credentials). Cursor pagination (50/page). Orders use merge disposition, products use replace. Auto-nested normalization creates 5+ tables. |
| **ShipHero** | GraphQL | 50k complexity/hour | `products { ... }`<br>`orders { ... }` | Custom async GraphQL with aiohttp | ✅ **IMPLEMENTED** - 3PL fulfillment & inventory. Complexity monitoring with adaptive delays. 2 resources: products, orders (warehouse_id required). Incremental via `updated_from` filter. Flattened edges/nodes with warehouse_products and line_items preserved as JSON. OAuth refresh flow deferred to production. |
| **Loop Returns** | REST API | Standard limits | `/api/v1/returns/list`<br>`/api/v1/returns/{id}` | dlt REST API client | 🗓️ **TODO** - Returns tracking. Incremental via timestamp filters. |
| **Meta Ads** | REST (Graph API) | App/account-dependent | `/{ad_account_id}/insights` | Custom extraction (requests) → dlt | 🗓️ **TODO** - Ad spend & performance. GraphQL-like structure. Campaign-level aggregation. Monitor rate limit headers. |
| **Google Ads** | REST API | 15k ops/day | Campaign performance reports<br>Ad group performance | dlt REST API client or verified source | 🗓️ **TODO** - Ad spend & performance. Requires developer token. OAuth 2.0 authentication. Date segments for incremental. |
| **Airtable** | REST API | 5 req/sec per base | `/v0/{base_id}/{table_name}` | dlt REST API client | 🗓️ **TODO** - Product master data (COGS, categories, attributes). Offset-based pagination. Consider full refresh. |

## Data Freshness Strategy

| Source | Update Frequency | Incremental Strategy | Primary Keys | Status |
|--------|------------------|----------------------|--------------|--------|
| Shopify | Hourly (recommended) | GraphQL query filter `updated_at:>'{timestamp}'` | id (GID format) | ✅ Implemented |
| Faire | Daily | `updated_at_min` filter (queried from DB) | id | ✅ Implemented |
| ShipHero | Hourly | GraphQL `updated_from` variable | id | ✅ Implemented |
| Loop Returns | Daily | `updated_at` filter | return_id | 🗓️ TODO |
| Meta Ads | Daily | `date_preset` or `time_range` | date + campaign_id | 🗓️ TODO |
| Google Ads | Daily | `segments.date` filter | date + campaign_id | 🗓️ TODO |
| Airtable | Weekly | Full refresh (small dataset) | record_id or sku | 🗓️ TODO |

## Authentication Methods

### API Keys / Access Tokens
- **Shopify** ✅: X-Shopify-Access-Token header with admin API access token (`shpat_...`)
- **Faire** ✅: Dual custom headers - X-FAIRE-APP-CREDENTIALS (base64-encoded app credentials) + X-FAIRE-OAUTH-ACCESS-TOKEN (OAuth token)
- **Loop Returns** 🗓️: API key (`sk_prod_...`)
- **Airtable** 🗓️: Personal access token or API key

### OAuth 2.0
- **ShipHero** ✅: OAuth access token (Bearer) - refresh flow deferred to production
- **Meta Ads** 🗓️: Long-lived access token from Graph API
- **Google Ads** 🗓️: OAuth with client credentials + refresh token + developer token

## API-Specific Considerations

### Shopify (GraphQL) ✅ IMPLEMENTED
- **API Type**: GraphQL Admin API 2025-10 (NOT REST - REST is legacy as of Oct 2024)
- **Rate Limiting**: Cost-based system with 1000 points available, restore at 50 points/sec
- **Authentication**: X-Shopify-Access-Token header with admin API access token
- **Pagination**: Cursor-based with pageInfo (hasNextPage, endCursor)
- **Response Structure**: Nested edges/nodes format - requires flattening before dlt load
- **Cost Monitoring**: Check extensions.cost.requestedQueryCost and extensions.cost.throttleStatus in responses
- **Incremental Loading**: Use query filters like `updated_at:>'2024-01-01T00:00:00Z'`
- **Operational Context**: Faire B2B orders sync into Shopify; Shopify then feeds ShipHero for fulfillment
- **Privacy**: Customer PII excluded at query level (no email, phone, name, addresses) - privacy-by-design
- **Resources Implemented**: orders (with line items), products (with variants), customers (anonymized), inventory (by location)

### Faire (REST) ✅ IMPLEMENTED
- **API Type**: REST API v2
- **Rate Limiting**: 1000 requests per hour per access token
- **Authentication**: Dual custom headers (X-FAIRE-APP-CREDENTIALS + X-FAIRE-OAUTH-ACCESS-TOKEN)
  - X-FAIRE-APP-CREDENTIALS: Base64-encoded "{application_id}:{application_secret}"
  - X-FAIRE-OAUTH-ACCESS-TOKEN: OAuth access token from authorization flow
- **Pagination**: Cursor-based with 50 items per page (limit=50, cursor param)
- **Incremental Loading**: Filter by `updated_at_min` on orders endpoint
- **Auto-Normalization**: dlt REST API client auto-creates nested tables (orders→orders__items, orders→orders__shipments, products→products__variants)
- **Known Issue**: Faire occasionally bulk-updates `updated_at` timestamps across hundreds of orders (likely backend refresh); merge on primary key deduplicates
- **Write Dispositions**: Merge for orders (incremental), replace for products (full refresh)
- **Resources Implemented**: orders, products (with automatic child table creation)

### ShipHero (GraphQL) ✅ IMPLEMENTED
- **API Type**: GraphQL (only option - no REST alternative)
- **Rate Limiting**: 50,000 complexity points per hour (aggressive pagination can exhaust quickly)
- **Complexity Limits**: Each query has complexity score; monitor response headers and data
  - Headers: X-Complexity-Available, X-Complexity-Max
  - Response data: {resource}.complexity field
- **Authentication**: OAuth Bearer token (Authorization: Bearer {access_token})
- **Pagination**: Cursor-based with pageInfo (first: 25 for products/orders to manage complexity)
- **Complexity Monitoring**: 
  - Warn when available < 10,000 (20% of limit)
  - Adaptive delays: 0.5s base, increase when below 50% capacity
  - Formula: delay = max(0.5, complexity / restore_rate) when capacity low
- **Incremental Loading**: Use `updated_from` variable in GraphQL queries
- **Response Structure**: Deeply nested edges/nodes with warehouse_products and line_items arrays
- **Flattening**: Convert edges/nodes to simple dicts; preserve nested arrays as JSON fields
- **Warehouse Requirement**: Orders query requires warehouse_id (from secrets or parameter)
- **OAuth Refresh**: Tokens expire after 28 days - refresh flow deferred to production phase
- **Resources Implemented**: products (with warehouse_products), orders (with line_items and shipments)

### Meta Ads (Graph API) 🗓️ NOT IMPLEMENTED
- **Pagination**: Cursor-based (`paging.cursors.after`)
- **Date Ranges**: Use `time_range` or `date_preset` (last_7d, last_30d, etc.)
- **Metrics**: Spend, impressions, clicks, CTR, CPC, conversions
- **Aggregation**: Campaign-level recommended to avoid data explosion

### Google Ads
- **Developer Token**: Required from Google Ads MCC account
- **Query Language**: Google Ads Query Language (GAQL) - SQL-like syntax
- **Reports**: Use `campaigns`, `ad_groups`, `keywords` resources with `metrics` fields
- **Date Segmentation**: `segments.date` for daily breakdowns

### Shopify
- **API Versioning**: Use dated versions (2024-01) - versions deprecated after 12 months
- **Pagination**: Link headers (`rel="next"`)
- **Bulk Operations**: Consider Bulk Query API for large historical loads

## Data Volume Estimates

| Source | Records per Day | Growth Rate |
|--------|----------------|-------------|
| Shopify Orders | 10-100 | Variable (seasonal) |
| Faire Orders | 0-4 | Steady |
| ShipHero Shipments | 10-100 | Matches order volume |
| Loop Returns | 1-10 | ~5-10% of orders |
| Meta Ads | 5-10 campaigns | Daily aggregates |
| Google Ads | 3-5 campaigns | Daily aggregates |
| Airtable Products | 500-2000 (total) | Low churn |

## Error Handling & Retry Strategy

All extraction scripts should implement:
- **Exponential backoff** for rate limit errors (429)
- **Retry logic** for transient failures (500, 502, 503, 504)
- **Logging** of API errors with context (endpoint, request ID)
- **State management** via dlt for resumable incremental loads

## Next Steps

1. Implement extraction logic for each source (Phase 2)
2. Test individual source loads with small date ranges
3. Monitor rate limits and adjust polling frequency
4. Build SQL transformations to join data (Phase 3)
