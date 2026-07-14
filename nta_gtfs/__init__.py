"""NTA GTFS async client library."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("nta-gtfs")
except PackageNotFoundError:
    __version__ = "unknown"

from nta_gtfs.exceptions import (
    GtfsRtAuthError,
    GtfsRtFetchError,
    GtfsRtParseError,
    NtaGtfsError,
    StaticGtfsLoadError,
)
from nta_gtfs.gtfs_picker import Route, StaticGtfsPickerClient, Stop
from nta_gtfs.gtfs_rt import GtfsRtClient, StopTimeUpdate, TripUpdate
from nta_gtfs.static_gtfs import ScheduledDeparture, StaticGtfsClient

__all__ = [
    "GtfsRtAuthError",
    "GtfsRtClient",
    "GtfsRtFetchError",
    "GtfsRtParseError",
    "NtaGtfsError",
    "Route",
    "ScheduledDeparture",
    "StaticGtfsClient",
    "StaticGtfsLoadError",
    "StaticGtfsPickerClient",
    "Stop",
    "StopTimeUpdate",
    "TripUpdate",
    "__version__",
]
