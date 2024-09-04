"""NetsuiteSuiteQL tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_netsuitesuiteql import streams


class TapNetsuiteSuiteQL(Tap):
    """NetsuiteSuiteQL tap class."""

    name = "tap-netsuitesuiteql"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "realm",
            th.IntegerType,
            required=True,
        ),
        th.Property(
            "client_key",
            th.StringType,
            required=True,
        ),
        th.Property(
            "client_secret",
            th.StringType,
            secret=True,
        ),
        th.Property(
            "resource_owner_key",
            th.StringType,
            description="The url for the API service",
        ),
        th.Property(
            "resource_owner_secret",
            th.StringType,
            secret=True,
        ),
        th.Property(
            "url",
            th.StringType,
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.NetsuiteSuiteQLStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ArrHistoryStream(self),
            streams.TransactionLineStream(self),
            streams.EndusersStream(self),
            streams.GeographicalHierarchyStream(self)
        ]


if __name__ == "__main__":
    TapNetsuiteSuiteQL.cli()
