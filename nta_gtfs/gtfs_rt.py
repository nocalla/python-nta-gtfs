"""GTFS-RT client for fetching real-time trip updates from the NTA feed."""

from __future__ import annotations

from dataclasses import dataclass, field

import aiohttp
from google.protobuf.message import DecodeError, Message
from google.transit import gtfs_realtime_pb2

from nta_gtfs.exceptions import GtfsRtAuthError, GtfsRtFetchError, GtfsRtParseError


@dataclass
class StopTimeUpdate:
    """A single stop-level real-time update within a trip update.

    Attributes:
        stop_id: GTFS stop identifier.
        arrival_delay: Arrival delay in seconds; ``None`` if absent in feed.
        departure_delay: Departure delay in seconds; ``None`` if absent in feed.
        arrival_time: Absolute arrival time as a POSIX timestamp; ``None`` if absent.
        departure_time: Absolute departure time as a POSIX timestamp; ``None`` if
            absent.
    """

    stop_id: str
    arrival_delay: int | None
    departure_delay: int | None
    arrival_time: int | None
    departure_time: int | None


@dataclass
class TripUpdate:
    """A real-time update for a single GTFS trip.

    Attributes:
        trip_id: GTFS trip identifier.
        route_id: GTFS route identifier.
        direction_id: GTFS direction as a string (``"0"`` or ``"1"``); ``None`` if
            absent.
        start_date: Service start date string (YYYYMMDD) from the feed; ``None`` if
            absent.
        stop_time_updates: Ordered list of stop-level updates for this trip.
    """

    trip_id: str
    route_id: str
    direction_id: str | None
    start_date: str | None
    stop_time_updates: list[StopTimeUpdate] = field(default_factory=list)


def _optional_int(message: Message | None, field_name: str) -> int | None:
    """Read an optional integer field from a protobuf message.

    Args:
        message: Protobuf message to read from, or ``None`` when the enclosing
            block is absent.
        field_name: Name of the optional scalar field.

    Returns:
        The field value as ``int``, or ``None`` when ``message`` is ``None``
        or the field is not set.
    """
    if message is None or not message.HasField(field_name):
        return None
    return int(getattr(message, field_name))


class GtfsRtClient:
    """Async client for fetching real-time trip updates from a GTFS-RT feed.

    The feed is expected to be a protobuf-encoded GTFS-RT ``FeedMessage``
    (the NTA endpoint's default response format).  Accepts a caller-supplied
    ``aiohttp.ClientSession`` and does not create its own session.  All
    errors are raised as library exceptions; no logging is performed
    internally.
    """

    def __init__(
        self,
        feed_url: str,
        api_key: str,
        session: aiohttp.ClientSession,
    ) -> None:
        """Initialise the client.

        Args:
            feed_url: Full HTTPS URL of the GTFS-RT feed endpoint.  Must
                use the ``https://`` scheme to protect the API key in transit.
            api_key: API key sent as the ``x-api-key`` request header.
            session: Caller-managed ``aiohttp.ClientSession`` used for all requests.

        Raises:
            ValueError: ``feed_url`` does not use the ``https://`` scheme.
        """
        self._feed_url = feed_url
        self._api_key = api_key
        self._session = session
        if not feed_url.startswith("https://"):
            raise ValueError(
                f"feed_url must use HTTPS to protect the API key; got: {feed_url!r}"
            )

    def __repr__(self) -> str:
        """Return a developer-friendly string representation.

        Returns:
            String showing the feed URL; the API key is intentionally omitted.
        """
        return f"GtfsRtClient(feed_url={self._feed_url!r})"

    async def async_fetch_trip_updates(self) -> list[TripUpdate]:
        """Fetch and parse the GTFS-RT trip updates feed.

        Performs an HTTP GET to the configured ``feed_url`` with the
        ``x-api-key`` header, parses the protobuf ``FeedMessage`` body, and
        returns a list of ``TripUpdate`` objects.

        Returns:
            List of ``TripUpdate`` objects parsed from the feed.  Returns an
            empty list when the feed contains no ``entity`` entries.

        Raises:
            GtfsRtAuthError: The feed returns HTTP 401.
            GtfsRtFetchError: The feed returns any other HTTP 4xx or 5xx
                status, or an ``aiohttp.ClientError`` occurs.
            GtfsRtParseError: The response body is not a valid protobuf
                ``FeedMessage``.
        """
        try:
            async with self._session.get(
                self._feed_url, headers={"x-api-key": self._api_key}
            ) as resp:
                if resp.status == 401:
                    raise GtfsRtAuthError("HTTP 401")

                if resp.status >= 400:
                    raise GtfsRtFetchError(f"HTTP {resp.status}")

                raw = await resp.read()

        except (GtfsRtAuthError, GtfsRtFetchError):
            raise
        except aiohttp.ClientError as exc:
            raise GtfsRtFetchError(str(exc)) from exc

        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(raw)
        except DecodeError as exc:
            raise GtfsRtParseError(f"Invalid protobuf FeedMessage: {exc}") from exc

        return self._parse_feed(feed)

    @staticmethod
    def _parse_feed(feed: gtfs_realtime_pb2.FeedMessage) -> list[TripUpdate]:
        """Parse a GTFS-RT ``FeedMessage`` into a list of ``TripUpdate`` objects.

        Iterates over the ``entity`` entries in ``feed``, extracts each
        ``trip_update`` block, and normalises it into typed dataclass
        instances.  Unset optional fields are replaced with ``None`` rather
        than raising.

        Args:
            feed: Decoded protobuf ``FeedMessage``.

        Returns:
            List of ``TripUpdate`` instances parsed from the feed entities.
        """
        updates: list[TripUpdate] = []

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            trip_update = entity.trip_update
            trip = trip_update.trip

            stop_time_updates: list[StopTimeUpdate] = []
            for stu in trip_update.stop_time_update:
                arrival = stu.arrival if stu.HasField("arrival") else None
                departure = stu.departure if stu.HasField("departure") else None
                stop_time_updates.append(
                    StopTimeUpdate(
                        stop_id=stu.stop_id,
                        arrival_delay=_optional_int(arrival, "delay"),
                        departure_delay=_optional_int(departure, "delay"),
                        arrival_time=_optional_int(arrival, "time"),
                        departure_time=_optional_int(departure, "time"),
                    )
                )

            updates.append(
                TripUpdate(
                    trip_id=trip.trip_id,
                    route_id=trip.route_id,
                    direction_id=(
                        str(trip.direction_id)
                        if trip.HasField("direction_id")
                        else None
                    ),
                    start_date=(
                        trip.start_date if trip.HasField("start_date") else None
                    ),
                    stop_time_updates=stop_time_updates,
                )
            )

        return updates
