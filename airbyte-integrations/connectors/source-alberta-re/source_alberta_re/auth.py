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
        
    def get_user_agent(self) -> str:
        try:
            user_agent = UserAgent(verify_ssl=False).random
        # Workaround for error occurred during loading data. 
        # Trying to use cache server https://fake-useragent.herokuapp.com/browsers/0.1.11
        except:
            user_agent = "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36"
        return user_agent
    
    def get_headers(self) -> Mapping[str, Any]:
        
        headers: MutableMapping[str, Any] = {
            "Accept": "application/json",
            "User-Agent": self.get_user_agent()
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
    