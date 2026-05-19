"""Exception types for the nta_gtfs library."""


class NtaGtfsError(Exception):
    """Base class for all nta_gtfs library errors."""


class GtfsRtAuthError(NtaGtfsError):
    """Raised when the GTFS-RT feed returns HTTP 401."""


class GtfsRtFetchError(NtaGtfsError):
    """Raised on a non-401 HTTP error from the GTFS-RT feed or a network failure."""


class GtfsRtParseError(NtaGtfsError):
    """Raised when the GTFS-RT response cannot be parsed as a valid FeedMessage."""


class StaticGtfsLoadError(NtaGtfsError):
    """Raised when the static GTFS zip cannot be downloaded or parsed."""
