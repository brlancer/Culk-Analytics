"""
Faire OAuth 2.0 Flow
Run this script once to obtain access and refresh tokens for Faire API.
The tokens will be displayed for you to add to .dlt/secrets.toml
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import webbrowser
import secrets
import requests
import dlt
import json
import os

# Load credentials from secrets.toml
config = dlt.secrets.get("sources.faire.oauth", {})
APPLICATION_ID = config.get("application_id")
APPLICATION_SECRET = config.get("application_secret")
REDIRECT_URI = "http://localhost:8080/callback"

# File to store the authorization code temporarily
AUTH_CODE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tmp", "faire_auth_code.json")

authorization_code = None

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global authorization_code
        
        # Parse the full URL to see what we're receiving
        print(f"\n[DEBUG] Callback received: {self.path}")
        
        query = urlparse(self.path).query
        params = parse_qs(query)
        
        # Check for error parameter from Faire
        if 'error' in params:
            print(f"[DEBUG] Error from Faire: {params['error'][0]}")
            if 'error_description' in params:
                print(f"[DEBUG] Error description: {params['error_description'][0]}")
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(f"""
                <html>
                <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                    <h1 style="color: red;">Error from Faire</h1>
                    <p>{params.get('error_description', ['Unknown error'])[0]}</p>
                </body>
                </html>
            """.encode('utf-8'))
            return
        
        if 'authorization_code' in params:
            authorization_code = params['authorization_code'][0]
            print(f"[DEBUG] Authorization code: {authorization_code[:20]}...")
            
            # Save the authorization code to file
            with open(AUTH_CODE_FILE, 'w') as f:
                json.dump({
                    'authorization_code': authorization_code,
                    'timestamp': str(secrets.token_urlsafe(16))
                }, f)
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write("""
                <html>
                <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                    <h1 style="color: green;">Authorization Successful!</h1>
                    <p>You can close this window and return to your terminal.</p>
                </body>
                </html>
            """.encode('utf-8'))
        else:
            print(f"[DEBUG] No authorization_code in params: {params}")
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write("""
                <html>
                <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                    <h1 style="color: red;">Error</h1>
                    <p>No authorization code received</p>
                </body>
                </html>
            """.encode('utf-8'))
    
    def log_message(self, format, *args):
        pass  # Suppress logging

def main():
    print("=" * 60)
    print("Faire OAuth 2.0 Setup")
    print("=" * 60)
    
    # Validate credentials
    if not APPLICATION_ID or not APPLICATION_SECRET:
        print("\n✗ Missing OAuth credentials!")
        print("\nPlease add to .dlt/secrets.toml:\n")
        print("[sources.faire.oauth]")
        print('application_id = "apa_cyh8e9uvcz"')
        print('application_secret = "YOUR_SECRET_HERE"')
        print("\nThen run this script again.")
        return
    
    # Ensure tmp directory exists
    tmp_dir = os.path.dirname(AUTH_CODE_FILE)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
        print(f"[DEBUG] Created directory: {tmp_dir}")
    
    # Check if we have a saved authorization code
    global authorization_code
    print(f"[DEBUG] Looking for saved code at: {AUTH_CODE_FILE}")
    print(f"[DEBUG] File exists: {os.path.exists(AUTH_CODE_FILE)}")
    
    if os.path.exists(AUTH_CODE_FILE):
        try:
            with open(AUTH_CODE_FILE, 'r') as f:
                saved_data = json.load(f)
                authorization_code = saved_data.get('authorization_code')
            
            print("\n✓ Found saved authorization code!")
            print(f"Code: {authorization_code[:30]}...")
            
            user_input = input("\nUse this code? (y/n, or 'clear' to delete): ").strip().lower()
            
            if user_input == 'clear':
                os.remove(AUTH_CODE_FILE)
                authorization_code = None
                print("Cleared saved code.")
            elif user_input == 'y':
                print("Using saved authorization code...\n")
                # Skip to token exchange
            else:
                authorization_code = None
        except Exception as e:
            print(f"[DEBUG] Error reading saved code: {e}")
            authorization_code = None
    
    # Only do OAuth flow if we don't have a code
    if not authorization_code:
        print("\nBefore continuing:")
        print("1. Go to your Faire Developer Dashboard")
        print("2. Edit your app settings")
        print(f"3. Add this redirect URI: {REDIRECT_URI}")
        print("\nPress Enter when ready...")
        input()
        
        # Step 1: Build authorization URL and open in browser
        state = secrets.token_urlsafe(16)
        
        auth_url = (
            f"https://faire.com/oauth2/authorize?"
            f"applicationId={APPLICATION_ID}&"
            f"scope=READ_PRODUCTS&scope=READ_ORDERS&scope=READ_RETAILER&scope=READ_INVENTORIES&scope=READ_SHIPMENTS&"
            f"state={state}&"
            f"redirectUrl={REDIRECT_URI}"
        )
        
        print("\n" + "=" * 60)
        print("Step 1: Authorize Application")
        print("=" * 60)
        print(f"\nOpening browser for authorization...")
        print(f"If browser doesn't open, visit:\n{auth_url}\n")
        
        webbrowser.open(auth_url)
        
        # Step 2: Start local server to capture the callback
        print(f"Waiting for callback on {REDIRECT_URI}...")
        print("(The browser will redirect here after you click Accept)\n")
        
        server = HTTPServer(('localhost', 8080), CallbackHandler)
        server.handle_request()  # Handle one request then stop
        
        if not authorization_code:
            print("\n✗ No authorization code received. Please try again.")
            return
        
        print(f"✓ Authorization code received and saved!")
    
    # Step 3: Exchange code for access token
    print("\n" + "=" * 60)
    print("Step 2: Exchange Code for Tokens")
    print("=" * 60)
    
    # Correct Faire token endpoint
    token_url = "https://www.faire.com/api/external-api-oauth2/token"
    print(f"[DEBUG] Token endpoint: {token_url}")
    
    # Build form data with repeated scope parameters
    token_data = {
        "application_token": APPLICATION_ID,
        "application_secret": APPLICATION_SECRET,
        "redirect_url": REDIRECT_URI,
        "scope": ["READ_PRODUCTS", "READ_ORDERS", "READ_RETAILER", "READ_INVENTORIES", "READ_SHIPMENTS"],
        "grant_type": "AUTHORIZATION_CODE",
        "authorization_code": authorization_code,
    }
    
    print(f"[DEBUG] Request data: {json.dumps(dict(token_data, applicationSecret='***'), indent=2)}")

    print(f"[DEBUG] Request data: {dict(token_data, applicationSecret='***')}")
    
    # Send as JSON with explicit Content-Type
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(token_url, json=token_data, headers=headers)
    
    print(f"[DEBUG] Response status: {response.status_code}")
    print(f"[DEBUG] Response body: {response.text}")
    
    if response.status_code == 200:
        tokens = response.json()
        print("\n✓ Tokens obtained successfully!\n")
        
        # Delete the saved auth code since it's now used
        if os.path.exists(AUTH_CODE_FILE):
            os.remove(AUTH_CODE_FILE)
            print("[DEBUG] Deleted used authorization code")
        
        print("=" * 60)
        print("Update .dlt/secrets.toml with these tokens:")
        print("=" * 60)
        print(f'\n[sources.faire]')
        print(f'access_token = "{tokens["access_token"]}"')
        if "refresh_token" in tokens:
            print(f'refresh_token = "{tokens["refresh_token"]}"')
        print(f'\n# Token expires in: {tokens.get("expires_in", "unknown")} seconds')
        print("=" * 60)
    else:
        print(f"\n✗ Token exchange failed!")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code == 400:
            print("\n⚠️  The authorization code may have expired or already been used.")
            print("Run the script again and choose 'clear' to get a fresh code.")

if __name__ == "__main__":
    main()