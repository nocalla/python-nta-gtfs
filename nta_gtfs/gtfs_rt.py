"""GTFS-RT client for fetching real-time trip updates from the NTA feed."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import aiohttp

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


def _int_or_none(value: Any) -> int | None:
    """Convert a value to ``int``, returning ``None`` when conversion fails.

    Args:
        value: Any value that may be cast to ``int``.

    Returns:
        Integer representation of ``value``, or ``None`` if ``value`` is
        ``None`` or cannot be cast.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class GtfsRtClient:
    """Async client for fetching real-time trip updates from a GTFS-RT JSON feed.

    Accepts a caller-supplied ``aiohttp.ClientSession`` and does not create
    its own session.  All errors are raised as library exceptions; no logging
    is performed internally.
    """

    def __init__(
        self,
        feed_url: str,
        api_key: str,
        session: aiohttp.ClientSession,
    ) -> None:
        """Initialise the client.

        Args:
            feed_url: Full HTTPS URL of the GTFS-RT JSON feed endpoint.  Must
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
        ``x-api-key`` header, parses the JSON FeedMessage, and returns a list
        of ``TripUpdate`` objects.

        Returns:
            List of ``TripUpdate`` objects parsed from the feed.  Returns an
            empty list when the feed contains no ``entity`` entries.

        Raises:
            GtfsRtAuthError: The feed returns HTTP 401.
            GtfsRtFetchError: The feed returns any other HTTP 4xx or 5xx
                status, or an ``aiohttp.ClientError`` occurs.
            GtfsRtParseError: The response body is not valid JSON or the
                top-level structure is not a dict.
        """
        try:
            async with self._session.get(
                self._feed_url, headers={"x-api-key": self._api_key}
            ) as resp:
                if resp.status == 401:
                    raise GtfsRtAuthError("HTTP 401")

                if resp.status >= 400:
                    raise GtfsRtFetchError(f"HTTP {resp.status}")

                raw = await resp.text()

        except (GtfsRtAuthError, GtfsRtFetchError):
            raise
        except aiohttp.ClientError as exc:
            raise GtfsRtFetchError(str(exc)) from exc

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GtfsRtParseError(f"Invalid JSON: {exc}") from exc

        if not isinstance(payload, dict):
            raise GtfsRtParseError("Expected a JSON object at the top level")

        return self._parse_feed(payload)

    @staticmethod
    def _parse_feed(payload: dict[str, Any]) -> list[TripUpdate]:
        """Parse a GTFS-RT FeedMessage dict into a list of ``TripUpdate`` objects.

        Iterates over the ``entity`` array in ``payload``, extracts each
        ``trip_update`` block, and normalises it into typed dataclass instances.
        Missing optional keys are replaced with ``None`` rather than raising.

        Args:
            payload: Parsed JSON object representing the GTFS-RT FeedMessage.

        Returns:
            List of ``TripUpdate`` instances parsed from the feed entities.
        """
        updates: list[TripUpdate] = []

        for entity in payload.get("entity", []):
            trip_update = entity.get("trip_update")
            if trip_update is None:
                continue

            trip = trip_update.get("trip", {})
            raw_direction = trip.get("direction_id")

            stop_time_updates: list[StopTimeUpdate] = []
            for stu in trip_update.get("stop_time_update", []):
                arrival = stu.get("arrival") or {}
                departure = stu.get("departure") or {}
                stop_time_updates.append(
                    StopTimeUpdate(
                        stop_id=str(stu.get("stop_id", "")),
                        arrival_delay=_int_or_none(arrival.get("delay")),
                        departure_delay=_int_or_none(departure.get("delay")),
                        arrival_time=_int_or_none(arrival.get("time")),
                        departure_time=_int_or_none(departure.get("time")),
                    )
                )

            updates.append(
                TripUpdate(
                    trip_id=str(trip.get("trip_id", "")),
                    route_id=str(trip.get("route_id", "")),
                    direction_id=(
                        str(raw_direction) if raw_direction is not None else None
                    ),
                    start_date=trip.get("start_date"),
                    stop_time_updates=stop_time_updates,
                )
            )

        return updates
