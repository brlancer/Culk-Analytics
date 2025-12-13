"""
Test Faire API Access
Quick script to verify if the existing access token works
"""

import requests
import dlt
import base64

# Load the access token from secrets.toml
config = dlt.secrets.get("sources.faire.oauth", {})
application_id = config.get("application_id")
application_secret = config.get("application_secret")
access_token = dlt.secrets.get("sources.faire.access_token")

if not access_token:
    print("✗ No access token found in secrets.toml")
    print("Make sure you have: [sources.faire] access_token = \"...\"")
    exit(1)

if not application_id or not application_secret:
    print("✗ Missing OAuth credentials in secrets.toml")
    print("Make sure you have: [sources.faire.oauth] application_id and application_secret")
    exit(1)

print("Testing Faire API with existing token...")
print(f"Token: {access_token[:20]}...")

# Encode applicationId:applicationSecret in Base64
credentials = f"{application_id}:{application_secret}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()

# Try a simple API call - get products
headers = {
    "X-FAIRE-APP-CREDENTIALS": encoded_credentials,
    "X-FAIRE-OAUTH-ACCESS-TOKEN": access_token
}

# Also try API v2 endpoint
print("\n2. Testing API v2 endpoint...")
response_v2 = requests.get(
    "https://www.faire.com/external-api/v2/orders",
    headers=headers,
    params={"limit": 10}
)

print(f"Status: {response_v2.status_code}")
print(f"Response body: {response_v2.text}")
print(f"Response headers: {dict(response_v2.headers)}")

if response_v2.status_code == 200:
    print("✓ API v2 also works!")
    data = response_v2.json()
    print(f"Orders found: {len(data.get('orders', []))}")
elif response_v2.status_code == 401:
    print("✗ API v2 - Authentication failed")
    print("The token may be invalid or expired")
elif response_v2.status_code == 400:
    print("✗ API v2 - Bad request")
    try:
        error_data = response_v2.json()
        print(f"Error message: {error_data}")
    except:
        print(f"Error response: {response_v2.text}")
else:
    print(f"✗ Unexpected response: {response_v2.status_code}")
    print(f"Response: {response_v2.text}")

print("\n" + "="*60)
if response_v2.status_code == 200:
    print("✓ Your existing token works! You can skip the OAuth flow.")
else:
    print("✗ Token doesn't work. Run: python scripts/oauth/faire_oauth.py")
print("="*60)