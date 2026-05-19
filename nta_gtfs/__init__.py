"""NTA GTFS async client library."""

from nta_gtfs.exceptions import (
    GtfsRtAuthError,
    GtfsRtFetchError,
    GtfsRtParseError,
    NtaGtfsError,
    StaticGtfsLoadError,
)
from nta_gtfs.gtfs_rt import GtfsRtClient, StopTimeUpdate, TripUpdate
from nta_gtfs.static_gtfs import ScheduledDeparture, StaticGtfsClient

__all__ = [
    "GtfsRtAuthError",
    "GtfsRtClient",
    "GtfsRtFetchError",
    "GtfsRtParseError",
    "NtaGtfsError",
    "ScheduledDeparture",
    "StaticGtfsClient",
    "StaticGtfsLoadError",
    "StopTimeUpdate",
    "TripUpdate",
]
