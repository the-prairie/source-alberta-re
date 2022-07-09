#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .streams import Properties, TestStreamConnection
from .auth import EmailAuthenticator

TOKEN_URL = "https://albertare.com/api/leads/register/start"


class SourceAlbertaRe(AbstractSource):
    
    # @staticmethod
    # def get_authenticator(config: Mapping) -> EmailAuthenticator:
    #     return EmailAuthenticator(
    #         token_refresh_endpoint=TOKEN_URL,
    #         email=config["email"]
    #     )
    
    
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = EmailAuthenticator(token_refresh_endpoint=TOKEN_URL, email=config["email"])
            stream = TestStreamConnection(authenticator=authenticator, config=config)
            read_check = next(stream.read_records(sync_mode=None))
            if read_check:
                return True, None
            return (
                False,
                f"Could not authenticate with provided email: {config['email']}"
            )
        except requests.exceptions.RequestException as e:
            error_msg = e.response.json().get("error")
            if e.response.status_code == 403:
                return False, f"Permissions error: {error_msg}"
            else:
                return False, f"{error_msg}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        AirbyteLogger().log("INFO", f"Using email: {config['email']}")

        authenticator = EmailAuthenticator(token_refresh_endpoint=TOKEN_URL, email=config["email"])
        args = {"authenticator": authenticator, "config": config}
        return [
            Properties(**args)
        ]
