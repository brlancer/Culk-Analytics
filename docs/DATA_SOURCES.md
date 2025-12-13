# Data Sources

Comprehensive overview of all data sources integrated into the Culk Analytics platform.

## Source Summary

| Source Name | API Type | Rate Limits | Key Endpoints | Extraction Method | Notes |
|------------|----------|-------------|---------------|-------------------|-------|
| **Shopify** | REST API | 2-4 req/sec (plan-dependent) | `/admin/api/2025-10/orders.json`<br>`/admin/api/2025-10/products.json`<br>`/admin/api/2025-10/customers.json` | dlt REST API client | Commerce hub for B2B + DTC; Faire orders sync into Shopify, then to ShipHero. Incremental via `updated_at_min`. Link header pagination. |
| **Faire** | REST API | 1000 req/hour | `/api/v2/orders`<br>`/api/v2/products` | dlt REST API client | Wholesale orders. Page-based or cursor pagination. Filter by timestamps. |
| **ShipHero** | GraphQL | 50k complexity/hour | `products { ... }`<br>`shipments { ... }`<br>`inventory_changes { ... }` | Custom extraction (aiohttp) → dlt | 3PL fulfillment & inventory. **Critical**: Monitor complexity usage. OAuth tokens expire - implement refresh. |
| **Loop Returns** | REST API | Standard limits | `/api/v1/returns/list`<br>`/api/v1/returns/{id}` | dlt REST API client | Returns tracking. Incremental via timestamp filters. |
| **Meta Ads** | REST (Graph API) | App/account-dependent | `/{ad_account_id}/insights` | Custom extraction (requests) → dlt | Ad spend & performance. GraphQL-like structure. Campaign-level aggregation. Monitor rate limit headers. |
| **Google Ads** | REST API | 15k ops/day | Campaign performance reports<br>Ad group performance | dlt REST API client or verified source | Ad spend & performance. Requires developer token. OAuth 2.0 authentication. Date segments for incremental. |
| **Airtable** | REST API | 5 req/sec per base | `/v0/{base_id}/{table_name}` | dlt REST API client | Product master data (COGS, categories, attributes). Offset-based pagination. Consider full refresh. |

## Data Freshness Strategy

| Source | Update Frequency | Incremental Strategy | Primary Keys |
|--------|------------------|----------------------|--------------|
| Shopify | Hourly | `updated_at_min` filter | `order_id`, `product_id`, `customer_id` |
| Faire | Daily | `updated_at` filter | `order_id`, `product_id` |
| ShipHero | Hourly | GraphQL `updated_from` variable | `product_id`, `shipment_id` |
| Loop Returns | Daily | `updated_at` filter | `return_id` |
| Meta Ads | Daily | `date_preset` or `time_range` | `date` + `campaign_id` |
| Google Ads | Daily | `segments.date` filter | `date` + `campaign_id` |
| Airtable | Weekly | Full refresh (small dataset) | `record_id` or `sku` |

## Authentication Methods

### API Keys / Access Tokens
- **Shopify**: Admin API access token (`shpat_...`)
- **Faire**: Bearer token (`fav2_...`)
- **Loop Returns**: API key (`sk_prod_...`)
- **Airtable**: Personal access token or API key

### OAuth 2.0
- **ShipHero**: OAuth access token + refresh token (tokens expire)
- **Meta Ads**: Long-lived access token from Graph API
- **Google Ads**: OAuth with client credentials + refresh token + developer token

## API-Specific Considerations

### ShipHero (GraphQL)
- **Complexity Limits**: Each query has a complexity score. Monitor response headers.
- **Rate Limit**: 50,000 complexity points per hour - aggressive pagination can hit limits quickly
- **Authentication**: OAuth tokens expire - implement refresh flow
- **Response Structure**: Nested edges/nodes format - requires flattening before dlt load

### Meta Ads (Graph API)
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
