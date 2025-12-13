"""
Manually exchange Faire authorization code for access token
"""

import requests
import dlt

# Load credentials from secrets.toml
config = dlt.secrets.get("sources.faire.oauth", {})
APPLICATION_ID = config.get("application_id")
APPLICATION_SECRET = config.get("application_secret")
REDIRECT_URI = "http://localhost:8080/callback"

# The authorization code from your recent OAuth attempt
AUTHORIZATION_CODE = "oac_acg8rxz0qgsxx4c4i22zodt4vc6mq1wmvamwtakl8u2i7gsnchc6regaah69t7ornzqr2k85a3qbbqw7az8k2ewwhwr1bxsc"

print("Exchanging authorization code for access token...")
print(f"Code: {AUTHORIZATION_CODE[:30]}...")

token_url = "https://www.faire.com/api/external-api-authentication/oauth/token"

token_data = {
    "applicationId": APPLICATION_ID,
    "applicationSecret": APPLICATION_SECRET,
    "redirectUrl": REDIRECT_URI,
    "scope": "READ_PRODUCTS,READ_ORDERS,READ_RETAILER,READ_INVENTORIES,READ_SHIPMENTS",
    "grantType": "AUTHORIZATION_CODE",
    "authorizationCode": AUTHORIZATION_CODE,
}

print(f"\nPOST {token_url}")
print(f"Data: {dict(token_data, applicationSecret='***', authorizationCode=AUTHORIZATION_CODE[:30]+'...')}\n")

response = requests.post(token_url, json=token_data)

print(f"Status: {response.status_code}")
print(f"Headers: {dict(response.headers)}\n")

if response.status_code == 200:
    tokens = response.json()
    print("✓ Success! Tokens obtained:\n")
    print("=" * 60)
    print("Add these to .dlt/secrets.toml:")
    print("=" * 60)
    print(f'\n[sources.faire]')
    print(f'access_token = "{tokens["access_token"]}"')
    if "refresh_token" in tokens:
        print(f'refresh_token = "{tokens["refresh_token"]}"')
    if "expires_in" in tokens:
        print(f'\n# Token expires in: {tokens["expires_in"]} seconds')
    print("=" * 60)
else:
    print(f"✗ Failed to exchange code")
    print(f"Response: {response.text}")
    
    if response.status_code == 400:
        print("\nNote: Authorization codes are single-use and expire quickly.")
        print("You may need to run the full OAuth flow again.")