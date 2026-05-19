"""Unit tests for GtfsRtClient.

All HTTP interactions are mocked via ``unittest.mock`` — no live network calls
are made.  Each test maps to one or more acceptance criteria from
``specs/002-library-split/spec.md`` "New tests required item 1".
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from nta_gtfs.exceptions import GtfsRtAuthError, GtfsRtFetchError, GtfsRtParseError
from nta_gtfs.gtfs_rt import GtfsRtClient, StopTimeUpdate, TripUpdate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FEED_URL = "https://api.example.com/gtfs-rt"
_API_KEY = "test-api-key"


def _make_feed_payload(entities: list[dict[str, Any]]) -> str:
    """Serialise a minimal GTFS-RT FeedMessage as JSON.

    Args:
        entities: List of entity dicts to embed under the ``entity`` key.

    Returns:
        JSON string representing the FeedMessage.
    """
    return json.dumps({"header": {}, "entity": entities})


def _minimal_entity(
    trip_id: str = "TRIP-1",
    route_id: str = "ROUTE-A",
    direction_id: int | None = 0,
    start_date: str | None = "20260518",
    stop_time_updates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a minimal GTFS-RT entity dict.

    Args:
        trip_id: GTFS trip identifier.
        route_id: GTFS route identifier.
        direction_id: Direction integer, or ``None`` to omit the key.
        start_date: Start date string, or ``None`` to omit the key.
        stop_time_updates: List of raw stop-time-update dicts.

    Returns:
        Entity dict suitable for embedding in a FeedMessage ``entity`` array.
    """
    trip: dict[str, Any] = {"trip_id": trip_id, "route_id": route_id}
    if direction_id is not None:
        trip["direction_id"] = direction_id
    if start_date is not None:
        trip["start_date"] = start_date

    return {
        "id": "1",
        "trip_update": {
            "trip": trip,
            "stop_time_update": stop_time_updates or [],
        },
    }


def _make_mock_response(
    status: int,
    body: str,
) -> MagicMock:
    """Return a mock aiohttp response usable as an async context manager.

    The returned mock is configured so that ``async with session.get(...) as
    resp:`` yields an object with the given ``status`` and ``await resp.text()``
    returning ``body``.

    Args:
        status: HTTP status code.
        body: Response body text.

    Returns:
        ``MagicMock`` configured to behave like an ``aiohttp.ClientResponse``
        async context manager.
    """
    resp = MagicMock()
    resp.status = status
    resp.text = AsyncMock(return_value=body)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _make_client_with_mock_response(
    status: int,
    body: str,
) -> tuple[GtfsRtClient, MagicMock]:
    """Construct a ``GtfsRtClient`` backed by a mock session returning ``body``.

    Args:
        status: HTTP status code the mock session will return.
        body: Response body text the mock session will return.

    Returns:
        A ``(client, session_mock)`` tuple.
    """
    session = MagicMock(spec=aiohttp.ClientSession)
    session.get = MagicMock(return_value=_make_mock_response(status, body))
    client = GtfsRtClient(feed_url=_FEED_URL, api_key=_API_KEY, session=session)
    return client, session


def _make_client_raising_client_error(exc: aiohttp.ClientError) -> GtfsRtClient:
    """Construct a ``GtfsRtClient`` whose session raises ``exc`` on GET.

    Args:
        exc: The ``aiohttp.ClientError`` to raise.

    Returns:
        A ``GtfsRtClient`` configured with the raising mock session.
    """
    session = MagicMock(spec=aiohttp.ClientSession)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=exc)
    cm.__aexit__ = AsyncMock(return_value=False)
    session.get = MagicMock(return_value=cm)
    return GtfsRtClient(feed_url=_FEED_URL, api_key=_API_KEY, session=session)


# ---------------------------------------------------------------------------
# Test 1 — Valid feed returns populated TripUpdate list
# AC: Given a valid JSON feed, async_fetch_trip_updates returns TripUpdate
#     objects with all fields correctly populated.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_feed_returns_trip_updates_with_all_fields() -> None:
    """A fully-populated feed entity maps to a TripUpdate with all fields set."""
    # Arrange
    stu_raw: list[dict[str, Any]] = [
        {
            "stop_id": "STOP-1",
            "arrival": {"delay": 30, "time": 1700000000},
            "departure": {"delay": 60, "time": 1700000060},
        }
    ]
    entity = _minimal_entity(
        trip_id="TRIP-1",
        route_id="ROUTE-A",
        direction_id=1,
        start_date="20260518",
        stop_time_updates=stu_raw,
    )
    body = _make_feed_payload([entity])
    client, session = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert — outer TripUpdate
    assert len(result) == 1
    trip = result[0]
    assert isinstance(trip, TripUpdate)
    assert trip.trip_id == "TRIP-1"
    assert trip.route_id == "ROUTE-A"
    assert trip.direction_id == "1"  # stored as str per spec
    assert trip.start_date == "20260518"

    # Assert — nested StopTimeUpdate
    assert len(trip.stop_time_updates) == 1
    stu = trip.stop_time_updates[0]
    assert isinstance(stu, StopTimeUpdate)
    assert stu.stop_id == "STOP-1"
    assert stu.arrival_delay == 30
    assert stu.departure_delay == 60
    assert stu.arrival_time == 1700000000
    assert stu.departure_time == 1700000060


@pytest.mark.asyncio
async def test_valid_feed_sends_x_api_key_header() -> None:
    """The GET request carries the x-api-key header with the configured value."""
    # Arrange
    body = _make_feed_payload([])
    client, session = _make_client_with_mock_response(200, body)

    # Act
    await client.async_fetch_trip_updates()

    # Assert
    session.get.assert_called_once_with(_FEED_URL, headers={"x-api-key": _API_KEY})


# ---------------------------------------------------------------------------
# Test 2 — Missing optional fields → None
# AC: direction_id and start_date absent in feed → TripUpdate fields are None.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_optional_direction_id_and_start_date_are_none() -> None:
    """direction_id and start_date are None when absent from the feed entity."""
    # Arrange — build entity with no direction_id and no start_date in trip
    entity: dict[str, Any] = {
        "id": "1",
        "trip_update": {
            "trip": {"trip_id": "TRIP-2", "route_id": "ROUTE-B"},
            "stop_time_update": [],
        },
    }
    body = _make_feed_payload([entity])
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    assert len(result) == 1
    trip = result[0]
    assert trip.direction_id is None
    assert trip.start_date is None


@pytest.mark.asyncio
async def test_missing_stop_time_update_optional_fields_are_none() -> None:
    """arrival and departure sub-fields absent in a stop_time_update are None."""
    # Arrange — stop_time_update with no arrival or departure blocks
    stu_raw: list[dict[str, Any]] = [{"stop_id": "STOP-99"}]
    entity = _minimal_entity(stop_time_updates=stu_raw)
    body = _make_feed_payload([entity])
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    stu = result[0].stop_time_updates[0]
    assert stu.arrival_delay is None
    assert stu.departure_delay is None
    assert stu.arrival_time is None
    assert stu.departure_time is None


# ---------------------------------------------------------------------------
# Test 3 — Non-castable numeric field → None
# AC: A delay/timestamp that cannot be cast to int is stored as None.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_castable_numeric_field_stored_as_none() -> None:
    """A delay value of 'not-a-number' is stored as None rather than raising."""
    # Arrange
    stu_raw: list[dict[str, Any]] = [
        {
            "stop_id": "STOP-X",
            "arrival": {"delay": "not-a-number", "time": "also-bad"},
            "departure": {"delay": None, "time": "bad-time"},
        }
    ]
    entity = _minimal_entity(stop_time_updates=stu_raw)
    body = _make_feed_payload([entity])
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    stu = result[0].stop_time_updates[0]
    assert stu.arrival_delay is None
    assert stu.arrival_time is None
    assert stu.departure_delay is None
    assert stu.departure_time is None


# ---------------------------------------------------------------------------
# Test 4 — Empty entity array → []
# AC: Feed with an empty entity array returns an empty list without error.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_entity_array_returns_empty_list() -> None:
    """A feed with entity: [] returns [] without raising GtfsRtParseError."""
    # Arrange
    body = _make_feed_payload([])
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    assert result == []


@pytest.mark.asyncio
async def test_entity_with_no_trip_update_block_is_skipped() -> None:
    """An entity dict that lacks a trip_update key is silently skipped."""
    # Arrange
    body = json.dumps({"entity": [{"id": "1"}]})
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    assert result == []


# ---------------------------------------------------------------------------
# Test 5 — HTTP 401 → GtfsRtAuthError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_401_raises_gtfs_rt_auth_error() -> None:
    """A 401 response raises GtfsRtAuthError."""
    # Arrange
    client, _ = _make_client_with_mock_response(401, "Unauthorized")

    # Act / Assert
    with pytest.raises(GtfsRtAuthError):
        await client.async_fetch_trip_updates()


# ---------------------------------------------------------------------------
# Test 6 — HTTP 500 → GtfsRtFetchError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_500_raises_gtfs_rt_fetch_error() -> None:
    """A 500 response raises GtfsRtFetchError."""
    # Arrange
    client, _ = _make_client_with_mock_response(500, "Internal Server Error")

    # Act / Assert
    with pytest.raises(GtfsRtFetchError):
        await client.async_fetch_trip_updates()


@pytest.mark.asyncio
async def test_http_403_raises_gtfs_rt_fetch_error() -> None:
    """A non-401 4xx response (403) raises GtfsRtFetchError, not GtfsRtAuthError."""
    # Arrange
    client, _ = _make_client_with_mock_response(403, "Forbidden")

    # Act / Assert
    with pytest.raises(GtfsRtFetchError):
        await client.async_fetch_trip_updates()


# ---------------------------------------------------------------------------
# Test 7 — aiohttp.ClientError → GtfsRtFetchError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aiohttp_client_error_raises_gtfs_rt_fetch_error() -> None:
    """An aiohttp.ClientError during the request raises GtfsRtFetchError."""
    # Arrange
    client = _make_client_raising_client_error(
        aiohttp.ClientConnectionError("connection refused")
    )

    # Act / Assert
    with pytest.raises(GtfsRtFetchError):
        await client.async_fetch_trip_updates()


@pytest.mark.asyncio
async def test_aiohttp_client_error_chains_original_exception() -> None:
    """GtfsRtFetchError raised from a ClientError chains the original exception."""
    # Arrange
    original = aiohttp.ClientConnectionError("timeout")
    client = _make_client_raising_client_error(original)

    # Act / Assert
    with pytest.raises(GtfsRtFetchError) as exc_info:
        await client.async_fetch_trip_updates()

    assert exc_info.value.__cause__ is original


# ---------------------------------------------------------------------------
# Test 8 — Non-JSON response body → GtfsRtParseError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_json_body_raises_gtfs_rt_parse_error() -> None:
    """A response body that is not valid JSON raises GtfsRtParseError."""
    # Arrange
    client, _ = _make_client_with_mock_response(200, "this is not json }{")

    # Act / Assert
    with pytest.raises(GtfsRtParseError):
        await client.async_fetch_trip_updates()


# ---------------------------------------------------------------------------
# Test 9 — Valid JSON but top-level is not a dict → GtfsRtParseError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_json_array_at_top_level_raises_gtfs_rt_parse_error() -> None:
    """Valid JSON that is a list at the top level raises GtfsRtParseError."""
    # Arrange
    client, _ = _make_client_with_mock_response(200, json.dumps([1, 2, 3]))

    # Act / Assert
    with pytest.raises(GtfsRtParseError):
        await client.async_fetch_trip_updates()


@pytest.mark.asyncio
async def test_json_string_at_top_level_raises_gtfs_rt_parse_error() -> None:
    """Valid JSON that is a bare string at the top level raises GtfsRtParseError."""
    # Arrange
    client, _ = _make_client_with_mock_response(200, json.dumps("just a string"))

    # Act / Assert
    with pytest.raises(GtfsRtParseError):
        await client.async_fetch_trip_updates()


# ---------------------------------------------------------------------------
# Test 10 — Library importable without homeassistant installed
# AC: nta_gtfs is importable in an environment where homeassistant is absent.
# ---------------------------------------------------------------------------


def test_nta_gtfs_importable_without_homeassistant() -> None:
    """nta_gtfs imports successfully; it must not import any homeassistant module."""
    # Arrange / Act — the import at the top of this module already proved it;
    # here we additionally assert that no homeassistant module ended up loaded
    # as a side-effect of the import.
    import sys

    ha_modules = [k for k in sys.modules if k.startswith("homeassistant")]
    assert ha_modules == [], (
        f"nta_gtfs must not import homeassistant; found: {ha_modules}"
    )


def test_gtfs_rt_client_importable_from_nta_gtfs_top_level() -> None:
    """GtfsRtClient is importable directly from the nta_gtfs package."""
    # Arrange / Act
    from nta_gtfs import GtfsRtClient as _GtfsRtClient  # noqa: F401 (import-only check)

    # Assert — if we reach here the import succeeded
    assert _GtfsRtClient is GtfsRtClient


def test_exception_types_importable_from_nta_gtfs_top_level() -> None:
    """All three GtfsRt exception types are importable from nta_gtfs directly."""
    from nta_gtfs import GtfsRtAuthError as _A
    from nta_gtfs import GtfsRtFetchError as _F
    from nta_gtfs import GtfsRtParseError as _P

    assert issubclass(_A, Exception)
    assert issubclass(_F, Exception)
    assert issubclass(_P, Exception)


# ---------------------------------------------------------------------------
# Boundary / additional contract tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_entities_parsed_in_order() -> None:
    """Multiple entities in the feed are returned in the order they appear."""
    # Arrange
    entities = [
        _minimal_entity(trip_id="TRIP-A", route_id="R1"),
        _minimal_entity(trip_id="TRIP-B", route_id="R2"),
        _minimal_entity(trip_id="TRIP-C", route_id="R3"),
    ]
    body = _make_feed_payload(entities)
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    assert [t.trip_id for t in result] == ["TRIP-A", "TRIP-B", "TRIP-C"]


@pytest.mark.asyncio
async def test_direction_id_stored_as_string_not_int() -> None:
    """direction_id of 0 is stored as the string '0', not the integer 0."""
    # Arrange
    entity = _minimal_entity(direction_id=0)
    body = _make_feed_payload([entity])
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    assert result[0].direction_id == "0"
    assert isinstance(result[0].direction_id, str)


@pytest.mark.asyncio
async def test_integer_delay_zero_stored_as_zero_not_none() -> None:
    """A delay value of 0 is stored as 0, not None (zero is a valid delay)."""
    # Arrange
    stu_raw: list[dict[str, Any]] = [
        {"stop_id": "STOP-Z", "arrival": {"delay": 0}, "departure": {"delay": 0}}
    ]
    entity = _minimal_entity(stop_time_updates=stu_raw)
    body = _make_feed_payload([entity])
    client, _ = _make_client_with_mock_response(200, body)

    # Act
    result = await client.async_fetch_trip_updates()

    # Assert
    stu = result[0].stop_time_updates[0]
    assert stu.arrival_delay == 0
    assert stu.departure_delay == 0


# ---------------------------------------------------------------------------
# Test — HTTP URL raises ValueError at construction time (issue #4)
# ---------------------------------------------------------------------------

def test_http_url_raises_value_error() -> None:
    """GtfsRtClient raises ValueError when feed_url uses http:// scheme."""
    session = MagicMock(spec=aiohttp.ClientSession)
    with pytest.raises(ValueError, match="HTTPS"):
        GtfsRtClient(
            feed_url="http://api.example.com/gtfs-rt",
            api_key=_API_KEY,
            session=session,
        )


# ---------------------------------------------------------------------------
# Test — __repr__ does not expose the API key (issue #9)
# ---------------------------------------------------------------------------

def test_repr_omits_api_key() -> None:
    """repr(client) contains the feed URL but not the API key value."""
    session = MagicMock(spec=aiohttp.ClientSession)
    client = GtfsRtClient(feed_url=_FEED_URL, api_key=_API_KEY, session=session)
    result = repr(client)
    assert _API_KEY not in result
    assert _FEED_URL in result
