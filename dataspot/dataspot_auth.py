import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from common import requests_post


class DataspotAuth:
    """Handles authentication for Dataspot API using Azure AD."""
    
    def __init__(self):
        load_dotenv('.dataspot.env')
        self.token_url = os.getenv("DATASPOT_AUTHENTICATION_TOKEN_URL")
        self.client_id = os.getenv("DATASPOT_CLIENT_ID")
        self.username = os.getenv("DATASPOT_EDITOR_USERNAME")
        self.password = os.getenv("DATASPOT_EDITOR_PASSWORD")
        self.base_url = os.getenv("DATASPOT_API_BASE_URL")
        self.token = None
        self.token_expires_at = None

    def get_base_url(self):
        return self.base_url

    def get_bearer_token(self):
        """Get a valid token, either from cache or by requesting a new one."""
        if self._is_token_valid():
            return self.token
        
        return self._request_new_bearer_token()

    def _is_token_valid(self):
        """Check if the current token is still valid."""
        if not self.token or not self.token_expires_at:
            return False
        # Add 5 minutes buffer before expiration
        return datetime.now() < self.token_expires_at - timedelta(minutes=5)

    def _request_new_bearer_token(self):
        """Request a new bearer token."""
        data = {
            'client_id': self.client_id,
            'grant_type': 'password',
            'scope': 'openid',
            'username': self.username,
            'password': self.password
        }

        try:
            response = requests_post(self.token_url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.token = token_data['id_token']
            # Calculate token expiration time
            expires_in = int(token_data.get('expires_in', 3600))
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            
            return self.token

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to obtain authentication token: {str(e)}")

    def get_headers(self):
        """Get all required authentication headers."""
        token = self.get_bearer_token()
        
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

if __name__=="__main__":
    auth = DataspotAuth()
    token = auth.get_bearer_token()
    print("Received token:")
    print(token)
