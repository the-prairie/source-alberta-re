
import math
import time
from functools import reduce, partial
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future, ProcessPoolExecutor
from itertools import chain
from typing import Any, Iterable, Mapping, MutableMapping, Optional, List

import pendulum
import requests

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException
from airbyte_cdk.sources.streams.http.rate_limiting import TRANSIENT_EXCEPTIONS
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import IncrementalMixin
from requests_futures.sessions import PICKLE_ERROR, FuturesSession


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

class SourceAlbertaReFuturesSession(FuturesSession):
    """
    Check the docs at https://github.com/ross/requests-futures.
    Used to async execute a set of requests.
    """

    def send_future(self, request: requests.PreparedRequest, **kwargs) -> Future:
        """
        Use instead of default `Session.send()` method.
        `Session.send()` should not be overridden as it used by `requests-futures` lib.
        """

        if self.session:
            func = self.session.send
        else:
            # avoid calling super to not break pickled method
            func = partial(requests.Session.send, self)

        if isinstance(self.executor, ProcessPoolExecutor):
            # verify function can be pickled
            try:
                dumps(func)
            except (TypeError, PickleError):
                raise RuntimeError(PICKLE_ERROR)

        return self.executor.submit(func, request, **kwargs)        
        

class Properties(AlbertaReStream):
    primary_key = "id"
    
    response_list_name: str = None
    future_requests: deque = None


    def __init__(self, authenticator, **kwargs):
        super().__init__(**kwargs)

        self._session = SourceAlbertaReFuturesSession()
        self._session.auth = authenticator
        self.future_requests = deque()

    def path(self, **kwargs) -> str:
        return "properties"

    def next_page_token(self, *args, **kwargs):
        return None

    def get_api_total_pages(self, stream_slice: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None):
        """
        Count total pages before generating the future requests
        to then correctly generate the pagination parameters.
        """

        count_url = urljoin(self.url_base, f"{self.path()}")
        params = self.request_params(stream_state=stream_state, stream_slice=stream_slice)
        headers = self.authenticator.get_auth_header()
        response = self._session.request("get", url=count_url, params=params, headers=headers).result()
        total_pages = self.get_deep(response.json(),  "meta.pagination.total_pages")
        

        return total_pages

    def generate_future_requests(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ):
        total_pages = self.get_api_total_pages(stream_slice=stream_slice, stream_state=stream_state)

        
        for page_number in range(1, total_pages + 1):
            params = self.request_params(stream_state=stream_state, stream_slice=stream_slice)
            params["page"] = page_number
            request_headers = self.request_headers(stream_state=stream_state, stream_slice=stream_slice)

            request = self._create_prepared_request(
                path=self.path(stream_state=stream_state, stream_slice=stream_slice),
                headers=dict(request_headers, **self.authenticator.get_auth_header()),
                params=params,
                json=self.request_body_json(stream_state=stream_state, stream_slice=stream_slice),
                data=self.request_body_data(stream_state=stream_state, stream_slice=stream_slice),
            )

            request_kwargs = self.request_kwargs(stream_state=stream_state, stream_slice=stream_slice)
            self.future_requests.append(
                {"future": self._send_request(request, request_kwargs), "request": request, "request_kwargs": request_kwargs, "retries": 0}
            )

    def _send(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> Future:
        response: Future = self._session.send_future(request, **request_kwargs)
        return response

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> Future:
        return self._send(request, request_kwargs)



    def _retry(
        self,
        request: requests.PreparedRequest,
        retries: int,
        original_exception: Exception = None,
        response: requests.Response = None,
        **request_kwargs,
    ):
        if retries == self.max_retries:
            if original_exception:
                raise original_exception
            raise DefaultBackoffException(request=request, response=response)
        if response:
            backoff_time = self.backoff_time(response)
            time.sleep(max(0, int(backoff_time - response.elapsed.total_seconds())))
        self.future_requests.append(
            {
                "future": self._send_request(request, request_kwargs),
                "request": request,
                "request_kwargs": request_kwargs,
                "retries": retries + 1,
            }
        )

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        self.generate_future_requests(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)

        while len(self.future_requests) > 0:
            item = self.future_requests.popleft()
            request, retries, future, kwargs = item["request"], item["retries"], item["future"], item["request_kwargs"]

            try:
                response = future.result()
            except TRANSIENT_EXCEPTIONS as exc:
                self._retry(request=request, retries=retries, original_exception=exc, **kwargs)
                continue
            if self.should_retry(response):
                self._retry(request=request, retries=retries, response=response, **kwargs)
                continue
            yield from self.parse_response(response, stream_state=stream_state, stream_slice=stream_slice)
    

class TestStreamConnection(AlbertaReStream):
    """
    Test the connectivity and permissions to read the data from the stream.
    """
    def path(self, **kwargs) -> str:
        return "properties"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """For test reading pagination is not required"""
        return None