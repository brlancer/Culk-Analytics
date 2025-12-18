"""Simple ShipHero token refresh utility."""

import requests
import dlt
from datetime import datetime, timedelta

SHIPHERO_REFRESH_ENDPOINT = "https://public-api.shiphero.com/auth/refresh"


def is_token_expired() -> bool:
    """Check if the ShipHero access token has expired."""
    expiration_str = dlt.config.get("sources.shiphero.token_expires_at")
    if not expiration_str:
        return True
    
    expiration_time = datetime.fromisoformat(expiration_str)
    return datetime.now() >= expiration_time


def refresh_shiphero_token() -> tuple[str | None, datetime | None]:
    """Refresh the ShipHero API token using the refresh token."""
    refresh_token = dlt.secrets.get("sources.shiphero.refresh_token")
    if not refresh_token:
        print("No refresh_token found in .dlt/secrets.toml")
        return None, None
    
    headers = {"Content-Type": "application/json"}
    data = {"refresh_token": refresh_token}
    
    response = requests.post(SHIPHERO_REFRESH_ENDPOINT, json=data, headers=headers)
    
    if response.status_code == 200:
        response_data = response.json()
        new_token = response_data.get("access_token")
        expires_in = response_data.get("expires_in")
        
        if new_token and expires_in:
            expiration_time = datetime.now() + timedelta(seconds=expires_in)
            print("ShipHero API token refreshed successfully.")
            update_token_in_secrets(new_token, expiration_time)
            return new_token, expiration_time
    
    print("Failed to refresh ShipHero API token.")
    return None, None


def update_token_in_secrets(new_token: str, expiration_time: datetime) -> None:
    """Update .dlt/secrets.toml and .dlt/config.toml with new token."""
    import toml
    from pathlib import Path
    
    project_root = Path(__file__).parent.parent.parent
    secrets_path = project_root / ".dlt" / "secrets.toml"
    config_path = project_root / ".dlt" / "config.toml"
    
    # Update secrets.toml
    secrets = toml.load(secrets_path)
    secrets.setdefault("sources", {}).setdefault("shiphero", {})["access_token"] = new_token
    with open(secrets_path, 'w') as f:
        toml.dump(secrets, f)
    
    # Update config.toml
    config = toml.load(config_path) if config_path.exists() else {}
    config.setdefault("sources", {}).setdefault("shiphero", {})["token_expires_at"] = expiration_time.isoformat()
    with open(config_path, 'w') as f:
        toml.dump(config, f)


def refresh_token_if_needed() -> bool:
    """Check if token is expired and refresh if needed."""
    if not is_token_expired():
        return True
    
    new_token, expiration = refresh_shiphero_token()
    return new_token is not None
