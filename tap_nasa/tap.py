"""NASA tap class."""

from __future__ import annotations

from typing import override

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_nasa import client


class TapNASA(Tap):
    """Singer tap for NASA."""

    name = "tap-nasa"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="API Key for NASA",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="Earliest datetime to get data from",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[Stream]:
        return [
            client.NASAStream(tap=self),
        ]
