"""REST client handling, including NetsuiteSuiteQLStream base class."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterable

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator, BaseOffsetPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

from tap_netsuitesuiteql.auth import NetsuiteSuiteQLAuthenticator

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

from datetime import date, datetime
import logging
import json

class NetsuiteSuiteQLStream(RESTStream):
    """NetsuiteSuiteQL stream class."""
    rest_method = "POST"

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$.rows[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.rows[-1:][r]"  # noqa: S105

    query = None

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["url"]

    @cached_property
    def authenticator(self) -> NetsuiteSuiteQLAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return NetsuiteSuiteQLAuthenticator(
            realm=self.config["realm"],
            client_key=self.config["client_key"],
            client_secret=self.config["client_secret"],
            resource_owner_key=self.config["resource_owner_key"],
            resource_owner_secret=self.config["resource_owner_secret"],
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {'Content-Type': 'application/json'}
        return headers

    def get_new_paginator(self) -> BaseAPIPaginatorBaseOffsetPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return BaseOffsetPaginator(start_value=0, page_size=5000)


    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {"script": 1074, "deploy": 1}

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        starting_timestamp = self.get_starting_timestamp(context)
        if starting_timestamp is not None:
            starting_timestamp = starting_timestamp
        elif self.start_date is not None:
            starting_timestamp = self.start_date
        else:
            starting_timestamp = datetime()

        timestamped_query = self.query.replace("__STARTING_TIMESTAMP__", starting_timestamp.isoformat(" "))

        # Next page token is an offset
        offset=0
        if next_page_token:
            offset = next_page_token

        query = f"SELECT * from (SELECT  *, rownum as r FROM ( {timestamped_query} )) WHERE r BETWEEN {offset} and {offset + 4999}"
        logging.info("TIMESTAMP")
        logging.info(next_page_token)
        logging.info(query.replace("\n", " "))
        return {"query": query, "offset": offset}

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.
        
        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
