
import math
from functools import reduce
from abc import ABC, abstractmethod
from itertools import chain
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import pendulum
import requests

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import IncrementalMixin

from urllib.parse import (
    urljoin,
    urlparse,
    parse_qs
)


# Basic full refresh stream
class AlbertaReStream(HttpStream, ABC):
    url_base = "https://albertare.com/api/"
    primary_key = "id"
    http_method = "GET"
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.email = config.get("email")
        self.status = config.get("status")
        self.max_days_listed = config.get("max_days_listed")
    
    @staticmethod
    def get_deep(dictionary, keys, default=None):
        """
        Custom method.
        Returns value of nested dictionary key.
        :: Input example: data = {"meta": "pagination": {"links": {"this_key": 1, "other_key": 2}}}
        :: Output example: get_deep(data, "meta.pagination.links.this_key") -> 1
        """
        return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        if bool(decoded_response.get("data", []) and self.get_deep(decoded_response, "meta.pagination.links.next")):
            
            current_page = self.get_deep(decoded_response, "meta.pagination.current_page")
            total_pages = self.get_deep(decoded_response, "meta.pagination.total_pages")
            AirbyteLogger().log("INFO", f"Getting page {current_page} out of {total_pages}")

            next_page = self.get_deep(decoded_response, "meta.pagination.links.next")
            _page = urlparse(next_page).query
            return {"page": parse_qs(_page)["page"][0]}

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        
        # Default pagination is 10, max is 100
        params = {"limit": 100}
        
        if self.status is not None:
            params["status"] = self.status
        
        if self.max_days_listed is not None:
            params["maxdayslisted"] = self.max_days_listed
        
        # Handle pagination by inserting the next page's token in the request parameters
        if next_page_token:
            params.update(next_page_token)

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("data", [])  # Records are in a container array "data"
        
        

class Properties(AlbertaReStream):
    primary_key = "id"
    
    def path(self, **kwargs) -> str:
        return "properties"
    

class TestStreamConnection(AlbertaReStream):
    """
    Test the connectivity and permissions to read the data from the stream.
    """
    def path(self, **kwargs) -> str:
        return "properties"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """For test reading pagination is not required"""
        return None