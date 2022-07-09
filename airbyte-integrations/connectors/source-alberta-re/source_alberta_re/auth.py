from fake_useragent import UserAgent
import pendulum
import requests

from typing import Any, List, Mapping, MutableMapping, Tuple
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator


class EmailAuthenticator(Oauth2Authenticator):
    def __init__(
        self,
        token_refresh_endpoint: str,
        email: str,
        token_expiry_date: pendulum.DateTime = None,
        access_token_name: str = "token",
        expires_in_seconds: int = 604_800
    ):
        self.token_refresh_endpoint = token_refresh_endpoint
        self.email = email
        self.access_token_name = access_token_name
        self.expires_in_seconds = expires_in_seconds

        self._token_expiry_date = token_expiry_date or pendulum.now().subtract(days=1)
        self._access_token = None
        
    def get_headers(self) -> Mapping[str, Any]:
        
        headers: MutableMapping[str, Any] = {
            "Accept": "application/json",
            "User-Agent": UserAgent(verify_ssl=False).random
        }
        return headers
    
    def get_refresh_request_body(self) -> Mapping[str, Any]:
        
        payload: MutableMapping[str, Any] = {
            "lead":{"email": self.email}
        }
        return payload
    
    def refresh_access_token(self) -> Tuple[str, int]:
        """
        returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        try:
            response = requests.request(method="POST", url=self.token_refresh_endpoint, headers=self.get_headers(), json=self.get_refresh_request_body())
            response.raise_for_status()
            response_json = response.json().get("data")
            return response_json[self.access_token_name], self.expires_in_seconds
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e
    