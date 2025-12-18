"""
Loop Returns Data Extraction

Endpoint: GET https://api.loopreturns.com/api/v1/warehouse/return/list
Auth: X-Authorization: <api_key>
Pagination: nextPageUrl / previousPageUrl (enable with paginate=true)
Initial load: from=2024-01-01
Incremental: filter=updated_at
API Restriction: Max 120 days per request (chunked to 100 days)
PII Sanitization: Removes customer email, addresses, phone numbers, personal URLs
"""

from typing import Iterator, Dict, Any, Optional
from datetime import datetime, timedelta
import requests
import dlt
from dlt.common.pipeline import LoadInfo


def get_loop_auth_headers() -> Dict[str, str]:
    api_key = dlt.secrets.get("sources.loop_returns.api_key")
    if not api_key:
        raise ValueError("Missing Loop Returns API key in .dlt/secrets.toml at sources.loop_returns.api_key")
    return {"X-Authorization": api_key}


def sanitize_return(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove PII fields from return record before loading to database.
    
    Removes:
    - customer (email address)
    - return_method.address (name, address, phone, coordinates)
    - status_page_url (unique customer tracking URL)
    - return_method.qr_code_url (unique QR code)
    """
    # Remove top-level PII
    item.pop('customer', None)
    item.pop('status_page_url', None)
    
    # Remove nested address data from return_method
    if 'return_method' in item and isinstance(item['return_method'], dict):
        item['return_method'].pop('address', None)
        item['return_method'].pop('qr_code_url', None)
    
    return item


def chunk_date_range(start_date: str, end_date: str, chunk_days: int = 100) -> list[tuple[str, str]]:
    """
    Split date range into chunks to respect Loop's 120-day API limit.
    
    Args:
        start_date: YYYY-MM-DD format
        end_date: YYYY-MM-DD format  
        chunk_days: Days per chunk (default 100, well under 120 limit)
    
    Returns:
        List of (from_date, to_date) tuples
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    chunks = []
    current = start
    
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        chunks.append((
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        ))
        current = chunk_end
    
    return chunks


@dlt.source
def loop_returns_source(from_date: str = "2024-01-01", to_date: Optional[str] = None):
    """Loop Returns source with 100-day chunking and PII sanitization."""

    @dlt.resource(name="returns", write_disposition="merge", primary_key="id")
    def fetch_returns() -> Iterator[Dict[str, Any]]:
        base_url = "https://api.loopreturns.com/api/v1/warehouse/return/list"
        headers = get_loop_auth_headers()
        
        # Default to today if no end date provided
        end_date = to_date or datetime.now().strftime("%Y-%m-%d")
        
        # Split into 100-day chunks
        date_chunks = chunk_date_range(from_date, end_date, chunk_days=100)
        print(f"Fetching returns in {len(date_chunks)} chunks ({from_date} to {end_date})")
        
        total = 0
        
        for chunk_from, chunk_to in date_chunks:
            params = {
                "paginate": "true",
                "pageSize": 100,
                "from": chunk_from,
                "to": chunk_to,
                "filter": "updated_at",
            }
            
            url: Optional[str] = base_url
            chunk_count = 0
            
            while url:
                resp = requests.get(url, headers=headers, params=params if url == base_url else None, timeout=30)
                resp.raise_for_status()
                body = resp.json()
                
                if isinstance(body, dict):
                    items = body.get("returns", [])
                    next_url = body.get("nextPageUrl")
                else:
                    items = body if isinstance(body, list) else []
                    next_url = None
                
                for item in items:
                    # Sanitize PII before yielding
                    sanitized = sanitize_return(item)
                    yield sanitized
                    chunk_count += 1
                    total += 1
                
                if not next_url or len(items) == 0:
                    break
                
                url = next_url
                params = None
            
            print(f"  Chunk {chunk_from} to {chunk_to}: {chunk_count} returns")
        
        print(f"Total: {total} returns (PII sanitized)")

    return [fetch_returns]


def load_to_postgres(from_date: str = "2024-01-01", to_date: Optional[str] = None) -> LoadInfo:
    """Load Loop Returns data to PostgreSQL with PII sanitization."""
    pipeline = dlt.pipeline(
        pipeline_name="loop_returns",
        destination="postgres",
        dataset_name="loop_returns_raw",
    )
    load_info = pipeline.run(loop_returns_source(from_date=from_date, to_date=to_date))
    print(f"Load complete: {load_info.loads_ids[0] if load_info.loads_ids else 'N/A'}")
    return load_info


if __name__ == "__main__":
    load_to_postgres()
