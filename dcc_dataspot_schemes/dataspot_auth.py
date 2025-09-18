import os
import requests
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
from common import requests_get


class DataspotAuth:
    """Handles M2M authentication for Dataspot API using Azure AD."""

    def __init__(self):
        load_dotenv()

        # M2M authentication against Entra ID
        exposed_client_id = os.getenv("DATASPOT_EXPOSED_CLIENT_ID")
        self.scope = f'api://{exposed_client_id}/.default'
        self.tenant_id = os.getenv("DATASPOT_TENANT_ID")
        self.client_id = os.getenv("DATASPOT_CLIENT_ID")
        self.client_secret = os.getenv("DATASPOT_CLIENT_SECRET")
        self.token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        # Dataspot API access
        self.dataspot_access_key = os.getenv("DATASPOT_SERVICE_USER_ACCESS_KEY")

        # Token caching
        self.token = None
        self.token_expires_at = None

        self._validate_access_key()

    def get_bearer_access_token(self):
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
        """Request a new bearer token using M2M authentication."""
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials',
            'scope': self.scope
        }

        try:
            response_bearer = requests.post(self.token_url, data=data)
            response_bearer.raise_for_status()

            token_data = response_bearer.json()
            self.token = token_data['access_token']
            # Calculate token expiration time
            expires_in = int(token_data.get('expires_in', 3600))
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

            return self.token

        except requests.exceptions.RequestException as e:
            error_msg = str(e).lower()
            if hasattr(e, 'response') and e.response.status_code == 401:
                logging.error("\n" + "!" * 80)
                logging.error("AUTHENTICATION FAILED: Your DATASPOT_CLIENT_SECRET has likely expired!")
                logging.error("Please create a new client secret in the Azure portal:")
                logging.error("Entra ID > App registrations > Your app > Certificates & secrets > New client secret")
                logging.error("Then update the DATASPOT_CLIENT_SECRET environment variable with the new value.")
                logging.error("!" * 80 + "\n")
                raise Exception("DATASPOT_CLIENT_SECRET validation failed - the secret may have expired")

            # For other errors, just raise with the original message
            raise Exception(f"Failed to obtain M2M access token from Entra ID: {str(e)}")

    def _validate_access_key(self):
        """Validates the access key by making a test request to dataspot API.

        Raises:
            Exception: If the access key is invalid or expired.
        """
        # Make a simple test request - /tenants/Mandant should always work
        test_url = f"https://datenkatalog.bs.ch/rest/prod/tenants/Mandant"
        r = requests.get(url=test_url, headers=self.get_headers())

        if r.status_code == 500:
            logging.error("\n" + "!" * 80)
            logging.error("AUTHENTICATION FAILED: Your DATASPOT_SERVICE_USER_ACCESS_KEY may have expired!")
            logging.error(
                "Please create a new access key for the service user in dataspot and update the environment variable.")
            logging.error("!" * 80 + "\n")
            raise Exception("DATASPOT_SERVICE_USER_ACCESS_KEY validation failed - the key may have expired")

        if r.status_code != 200:
            raise Exception(f"Dataspot API validation request failed with status code {r.status_code}")

        return True

    def get_headers(self):
        """Get all required authentication headers for dataspot API."""
        if not self.dataspot_access_key:
            raise Exception("DATASPOT_SERVICE_USER_ACCESS_KEY is not set")

        bearer_access_token = self.get_bearer_access_token()
        return {
            'Authorization': f'Bearer {bearer_access_token}',
            'dataspot-access-key': self.dataspot_access_key,
            'Content-Type': 'application/json'
        }


if __name__ == "__main__":
    auth = DataspotAuth()
    token = auth.get_bearer_access_token()
    print("Obtained authentication token")

    print("Testing sample request to dataspot...")
    headers = auth.get_headers()
    response = requests_get(url="https://datenkatalog.bs.ch/rest/prod/schemes/Systeme", headers=headers)
    response.raise_for_status()

    if response.status_code == 200:
        print("✅ Authentication successful")
    else:
        print("❌ Request failed")