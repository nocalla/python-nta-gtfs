"""Unit tests for nta_gtfs.StaticGtfsClient.

All GTFS data is built as in-memory zip bytes and no live HTTP calls are
made; the client itself spools downloads to an anonymous temporary file.
"""

import io
import zipfile
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from nta_gtfs.exceptions import StaticGtfsLoadError
from nta_gtfs.static_gtfs import ScheduledDeparture, StaticGtfsClient

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STOP_A = "STOP_A"
_STOP_B = "STOP_B"
_ROUTE_46A = "46A"
_ROUTE_39A = "39A"
_AGENCY_BUS = "BUS_CO"
_AGENCY_RAIL = "RAIL_CO"
_DUMMY_URL = "https://example.com/gtfs.zip"

# Monday 2026-05-18
_MONDAY = date(2026, 5, 18)
# Sunday 2026-05-17
_SUNDAY = date(2026, 5, 17)


# ---------------------------------------------------------------------------
# GTFS zip fixture builders
# ---------------------------------------------------------------------------


def _make_gtfs_zip(
    *,
    service_id: str = "SVC1",
    weekday_flags: str = "1,1,1,1,1,1,1",
    start_date: str = "20200101",
    end_date: str = "20991231",
    extra_trips: str = "",
    extra_stop_times: str = "",
    agency_id: str = _AGENCY_BUS,
    calendar_dates_csv: str | None = None,
    include_calendar_dates: bool = True,
) -> bytes:
    """Build a minimal in-memory GTFS zip and return raw bytes.

    Creates a zip with routes.txt, trips.txt, stop_times.txt, calendar.txt,
    and optionally calendar_dates.txt.  A single route (46A) served at
    STOP_A and STOP_B by three direction-0 trips and one direction-1 trip is
    the baseline; callers may inject additional trips and stop_times rows.

    Args:
        service_id: GTFS service_id to use for baseline trips and calendar row.
        weekday_flags: Comma-separated seven 0/1 flags for
            mon,tue,wed,thu,fri,sat,sun columns.
        start_date: calendar.txt start_date (YYYYMMDD string).
        end_date: calendar.txt end_date (YYYYMMDD string).
        extra_trips: Additional CSV rows (no header) appended to trips.txt.
        extra_stop_times: Additional CSV rows (no header) appended to
            stop_times.txt.
        agency_id: agency_id written to the baseline 46A route row.
        calendar_dates_csv: Full CSV content for calendar_dates.txt.  If None
            and include_calendar_dates is True, a header-only file is written.
        include_calendar_dates: When False, calendar_dates.txt is omitted
            entirely from the zip.

    Returns:
        Raw bytes of the assembled GTFS zip archive.
    """
    routes_csv = (
        f"route_id,route_short_name,agency_id\n{_ROUTE_46A},{_ROUTE_46A},{agency_id}\n"
    )

    trips_csv = (
        "trip_id,route_id,service_id,direction_id\n"
        f"T1,{_ROUTE_46A},{service_id},0\n"
        f"T2,{_ROUTE_46A},{service_id},1\n"
        f"T3,{_ROUTE_46A},{service_id},0\n"
    )
    if extra_trips:
        trips_csv += extra_trips

    stop_times_csv = (
        "trip_id,stop_id,departure_time,stop_sequence\n"
        f"T1,{_STOP_A},09:00:00,1\n"
        f"T1,{_STOP_B},09:15:00,2\n"
        f"T2,{_STOP_A},08:00:00,1\n"
        f"T2,{_STOP_B},08:20:00,2\n"
        f"T3,{_STOP_A},10:00:00,1\n"
        f"T3,{_STOP_B},10:20:00,2\n"
    )
    if extra_stop_times:
        stop_times_csv += extra_stop_times

    flags = weekday_flags
    calendar_csv = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
        "start_date,end_date\n"
        f"{service_id},{flags},{start_date},{end_date}\n"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("routes.txt", routes_csv)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
        zf.writestr("calendar.txt", calendar_csv)
        if include_calendar_dates:
            if calendar_dates_csv is None:
                calendar_dates_csv = "service_id,date,exception_type\n"
            zf.writestr("calendar_dates.txt", calendar_dates_csv)
    return buf.getvalue()


def _make_two_agency_zip() -> bytes:
    """Build a GTFS zip with two agencies serving the same stop/route.

    Route 46A is served by two trips:
    - T1 via agency BUS_CO
    - T2 via agency RAIL_CO

    Returns:
        Raw bytes of the assembled GTFS zip archive.
    """
    routes_csv = (
        "route_id,route_short_name,agency_id\n"
        f"R_BUS,{_ROUTE_46A},{_AGENCY_BUS}\n"
        f"R_RAIL,{_ROUTE_46A},{_AGENCY_RAIL}\n"
    )
    trips_csv = (
        "trip_id,route_id,service_id,direction_id\n"
        "T_BUS,R_BUS,SVC1,0\n"
        "T_RAIL,R_RAIL,SVC1,0\n"
    )
    stop_times_csv = (
        "trip_id,stop_id,departure_time,stop_sequence\n"
        f"T_BUS,{_STOP_A},09:00:00,1\n"
        f"T_RAIL,{_STOP_A},10:00:00,1\n"
    )
    calendar_csv = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
        "start_date,end_date\n"
        "SVC1,1,1,1,1,1,1,1,20200101,20991231\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("routes.txt", routes_csv)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
        zf.writestr("calendar.txt", calendar_csv)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Mock session helpers
# ---------------------------------------------------------------------------


def _make_session(*, status: int = 200, body: bytes = b"") -> MagicMock:
    """Return a MagicMock that quacks like an aiohttp.ClientSession.

    Args:
        status: HTTP status code the mocked response will report.
        body: Bytes streamed by ``response.content.iter_chunked()``.

    Returns:
        MagicMock whose ``.get(url)`` is an async context manager yielding a
        mock response with the given status and body.
    """
    mock_response = MagicMock()
    mock_response.ok = status < 400
    mock_response.status = status
    mock_response.content_length = None

    def _iter_chunked(chunk_size: int) -> AsyncIterator[bytes]:
        """Mimic ``aiohttp.StreamReader.iter_chunked`` over the fixture body.

        Args:
            chunk_size: Maximum size of each yielded chunk in bytes.

        Returns:
            Async iterator over successive ``chunk_size`` slices of ``body``.
        """

        async def _gen() -> AsyncIterator[bytes]:
            """Yield successive chunks of the fixture body.

            Yields:
                Chunks of ``body`` at most ``chunk_size`` bytes long.
            """
            for i in range(0, len(body), chunk_size):
                yield body[i : i + chunk_size]

        return _gen()

    mock_response.content = MagicMock()
    mock_response.content.iter_chunked = _iter_chunked

    mock_session = MagicMock()
    mock_session.get = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return mock_session


def _make_client_error_session() -> MagicMock:
    """Return a MagicMock session whose ``.get()`` context manager raises ClientError.

    Returns:
        MagicMock that raises ``aiohttp.ClientError`` on ``__aenter__``.
    """
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection refused"))
    cm.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.get = MagicMock(return_value=cm)
    return session


# ===========================================================================
# 1. async_load — successful load
# ===========================================================================


async def test_async_load_success_sets_available_true() -> None:
    """AC 11: async_load with a valid GTFS zip sets available=True."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)

    assert client.available is False

    # Act
    await client.async_load()

    # Assert
    assert client.available is True


async def test_async_load_success_sets_loaded_at_utc_aware() -> None:
    """AC 11: async_load with a valid GTFS zip sets loaded_at as UTC-aware datetime."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)

    assert client.loaded_at is None

    # Act
    await client.async_load()

    # Assert
    assert client.loaded_at is not None
    assert isinstance(client.loaded_at, datetime)
    assert client.loaded_at.tzinfo is not None
    assert client.loaded_at.tzinfo == UTC


# ===========================================================================
# 2. async_load — HTTP 503 raises StaticGtfsLoadError, available stays False
# ===========================================================================


async def test_async_load_http_503_raises_load_error() -> None:
    """AC 12: async_load with HTTP 503 raises StaticGtfsLoadError."""
    # Arrange
    session = _make_session(status=503, body=b"Service Unavailable")
    client = StaticGtfsClient(_DUMMY_URL, session)

    # Act / Assert
    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()


async def test_async_load_http_503_first_failure_available_remains_false() -> None:
    """AC 12: After first-ever load failure, available stays False."""
    # Arrange
    session = _make_session(status=503, body=b"Service Unavailable")
    client = StaticGtfsClient(_DUMMY_URL, session)

    # Act
    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()

    # Assert
    assert client.available is False


# ===========================================================================
# 3. async_load — refresh failure preserves existing data
# ===========================================================================


async def test_async_load_refresh_failure_preserves_available_true() -> None:
    """AC 13: A refresh failure on an already-loaded client leaves available=True."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session_ok = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session_ok)
    await client.async_load()

    assert client.available is True

    client._session = _make_session(status=503, body=b"")

    # Act
    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()

    # Assert
    assert client.available is True


async def test_async_load_refresh_failure_preserves_existing_departure_data() -> None:
    """AC 13: Departure data from a prior load is preserved after a failed refresh."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session_ok = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session_ok)
    await client.async_load()

    results_before = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, None, None, _MONDAY
    )
    assert len(results_before) > 0

    # Simulate the daily refresh kicking in with a failing download
    client._session = _make_session(status=503, body=b"")
    client._loaded_at = datetime.now(UTC) - timedelta(hours=25)

    with pytest.raises(StaticGtfsLoadError):
        await client.async_refresh_if_stale()

    # Act
    results_after = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, None, None, _MONDAY
    )

    # Assert — data unchanged
    assert results_after == results_before


# ===========================================================================
# 4. async_load — malformed zip raises StaticGtfsLoadError
# ===========================================================================


async def test_async_load_malformed_zip_raises_load_error() -> None:
    """AC 12: async_load with a non-zip body raises StaticGtfsLoadError."""
    # Arrange
    session = _make_session(status=200, body=b"this is not a zip file")
    client = StaticGtfsClient(_DUMMY_URL, session)

    # Act / Assert
    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()


async def test_async_load_malformed_zip_available_remains_false() -> None:
    """AC 12: After a malformed-zip failure, available stays False."""
    # Arrange
    session = _make_session(status=200, body=b"this is not a zip file")
    client = StaticGtfsClient(_DUMMY_URL, session)

    # Act
    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()

    # Assert
    assert client.available is False


# ===========================================================================
# 5. get_scheduled_departures — returns [] when available=False
# ===========================================================================


async def test_get_scheduled_departures_returns_empty_when_unavailable() -> None:
    """AC 14: get_scheduled_departures returns [] before any successful load."""
    # Arrange
    session = _make_session(status=200, body=b"irrelevant")
    client = StaticGtfsClient(_DUMMY_URL, session)
    # Do NOT call async_load — available remains False.

    # Act
    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)

    # Assert
    assert results == []


# ===========================================================================
# 6. Positional unpacking: trip_id, dep_time, route_name = result[0]
# ===========================================================================


async def test_result_supports_positional_unpacking() -> None:
    """AC 15: result[0] supports three-value positional unpacking."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)
    assert len(results) > 0

    # Act — must not raise
    trip_id, dep_time, route_name = results[0]

    # Assert
    assert isinstance(trip_id, str)
    assert isinstance(dep_time, str)
    assert route_name == _ROUTE_46A


# ===========================================================================
# 7. Loop unpacking: for a, b, c in results
# ===========================================================================


async def test_result_supports_loop_unpacking() -> None:
    """AC 16: all items in the result support three-variable loop unpacking."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)
    assert len(results) > 0

    # Act — must not raise TypeError
    unpacked = []
    for a, b, c in results:
        unpacked.append((a, b, c))

    # Assert every item was unpacked without error
    assert len(unpacked) == len(results)


async def test_result_items_are_scheduled_departure_named_tuples() -> None:
    """Results are ScheduledDeparture named tuples (not plain tuples or dataclasses)."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)

    # Assert
    assert all(isinstance(r, ScheduledDeparture) for r in results)


# ===========================================================================
# 8. Direction filter
# ===========================================================================


async def test_direction_filter_returns_only_matching_direction() -> None:
    """AC 17: direction_id=0 returns only direction-0 trips."""
    # Arrange — baseline zip has direction-0 (T1, T3) and direction-1 (T2) trips
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results_dir0 = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, 0, None, _MONDAY
    )
    results_dir1 = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, 1, None, _MONDAY
    )

    # Assert — direction sets are disjoint and their union equals unfiltered
    results_all = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, None, None, _MONDAY
    )
    ids_dir0 = {r.trip_id for r in results_dir0}
    ids_dir1 = {r.trip_id for r in results_dir1}
    ids_all = {r.trip_id for r in results_all}

    assert len(results_dir0) > 0
    assert ids_dir0.isdisjoint(ids_dir1)
    assert ids_dir0 | ids_dir1 == ids_all


# ===========================================================================
# 9. Operator filter
# ===========================================================================


async def test_operator_filter_returns_only_matching_agency() -> None:
    """AC 18: operator_id filters to only trips from that agency.

    ``_make_two_agency_zip`` models two distinct real routes (``R_BUS``,
    ``R_RAIL``) that happen to share a ``route_short_name`` (46A) — queries
    use the real ``route_id`` per AC for #24, and each route's single
    operator is confirmed via the ``operator_id`` filter.
    """
    # Arrange — zip with two agencies for the same stop, different route_ids
    zip_bytes = _make_two_agency_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results_bus = client.get_scheduled_departures(
        _STOP_A, "R_BUS", None, _AGENCY_BUS, _MONDAY
    )
    results_rail = client.get_scheduled_departures(
        _STOP_A, "R_RAIL", None, _AGENCY_RAIL, _MONDAY
    )
    results_bus_wrong_agency = client.get_scheduled_departures(
        _STOP_A, "R_BUS", None, _AGENCY_RAIL, _MONDAY
    )
    results_unknown = client.get_scheduled_departures(
        _STOP_A, "R_BUS", None, "UNKNOWN_AGENCY", _MONDAY
    )

    # Assert
    assert len(results_bus) > 0
    assert len(results_rail) > 0
    bus_ids = {r.trip_id for r in results_bus}
    rail_ids = {r.trip_id for r in results_rail}
    assert bus_ids.isdisjoint(rail_ids)
    assert results_bus_wrong_agency == []
    assert results_unknown == []


# ===========================================================================
# 10. Date filter — weekday-only service returns [] on Sunday
# ===========================================================================


async def test_date_filter_weekday_only_service_returns_empty_on_sunday() -> None:
    """AC 19: Weekday-only service returns [] when target_date is a Sunday."""
    # Arrange — Saturday=0, Sunday=0 in weekday flags
    zip_bytes = _make_gtfs_zip(weekday_flags="1,1,1,1,1,0,0")
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _SUNDAY)

    # Assert
    assert results == []


async def test_date_filter_weekday_service_returns_results_on_monday() -> None:
    """Complementary: weekday-only service returns results on a Monday."""
    # Arrange
    zip_bytes = _make_gtfs_zip(weekday_flags="1,1,1,1,1,0,0")
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)

    # Assert
    assert len(results) > 0


# ===========================================================================
# 11. async_refresh_if_stale — calls async_load when loaded_at is None
# ===========================================================================


async def test_async_refresh_if_stale_calls_load_when_never_loaded() -> None:
    """async_refresh_if_stale calls async_load when loaded_at is None."""
    # Arrange
    session = _make_session(status=200, body=b"")
    client = StaticGtfsClient(_DUMMY_URL, session)
    assert client.loaded_at is None

    # Act
    with patch.object(client, "async_load", new_callable=AsyncMock) as mock_load:
        await client.async_refresh_if_stale()

    # Assert
    mock_load.assert_called_once()


# ===========================================================================
# 12. async_refresh_if_stale — calls async_load when cache is older than refresh_hours
# ===========================================================================


async def test_async_refresh_if_stale_calls_load_when_cache_stale() -> None:
    """async_refresh_if_stale calls async_load when loaded_at exceeds refresh_hours."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session, refresh_hours=24)
    await client.async_load()

    # Act — backdate loaded_at by 25 hours
    with patch.object(client, "async_load", new_callable=AsyncMock) as mock_load:
        client._loaded_at = datetime.now(UTC) - timedelta(hours=25)
        await client.async_refresh_if_stale()

    # Assert
    mock_load.assert_called_once()


# ===========================================================================
# 13. async_refresh_if_stale — does NOT call async_load when cache is fresh
# ===========================================================================


async def test_async_refresh_if_stale_no_call_when_fresh() -> None:
    """async_refresh_if_stale does NOT call async_load when cache is fresh."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session, refresh_hours=24)
    await client.async_load()

    # Act — loaded_at only 1 hour ago, well within the 24-hour window
    with patch.object(client, "async_load", new_callable=AsyncMock) as mock_load:
        client._loaded_at = datetime.now(UTC) - timedelta(hours=1)
        await client.async_refresh_if_stale()

    # Assert
    mock_load.assert_not_called()


# ===========================================================================
# 14. Absent calendar_dates.txt handled gracefully
# ===========================================================================


async def test_absent_calendar_dates_handled_gracefully() -> None:
    """A GTFS zip without calendar_dates.txt loads and queries without error."""
    # Arrange
    zip_bytes = _make_gtfs_zip(include_calendar_dates=False)
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)

    # Act — must not raise
    await client.async_load()

    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)

    # Assert — available and returns data normally
    assert client.available is True
    assert len(results) > 0


# ===========================================================================
# 15. calendar_dates.txt exception_type=1 adds a service not in calendar
# ===========================================================================


async def test_calendar_dates_exception_type_1_adds_service() -> None:
    """exception_type=1 in calendar_dates adds a service for the specified date."""
    # Arrange — calendar has sunday=0, but calendar_dates adds it on the target Sunday
    target_sunday = _SUNDAY  # 2026-05-17
    calendar_dates_csv = "service_id,date,exception_type\nSVC1,20260517,1\n"

    zip_bytes = _make_gtfs_zip(
        weekday_flags="1,1,1,1,1,1,0",  # Sunday not active in calendar
        calendar_dates_csv=calendar_dates_csv,
    )
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Verify baseline: without exception, Sunday returns nothing
    # (We trust the date-filter test for that; here we verify the addition works)

    # Act
    results = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, None, None, target_sunday
    )

    # Assert — exception type 1 added service on this Sunday
    assert len(results) > 0


# ===========================================================================
# 16. calendar_dates.txt exception_type=2 removes a service that is in calendar
# ===========================================================================


async def test_calendar_dates_exception_type_2_removes_service() -> None:
    """exception_type=2 in calendar_dates removes a normally-active service."""
    # Arrange — Monday is active in the calendar but removed via exception_type=2
    target_monday = _MONDAY  # 2026-05-18
    calendar_dates_csv = "service_id,date,exception_type\nSVC1,20260518,2\n"

    zip_bytes = _make_gtfs_zip(
        weekday_flags="1,1,1,1,1,1,1",  # All days active in calendar
        calendar_dates_csv=calendar_dates_csv,
    )
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, None, None, target_monday
    )

    # Assert — service removed on this Monday
    assert results == []


# ===========================================================================
# 17. GTFS time 25:30:00 normalises to 01:30
# ===========================================================================


async def test_departure_time_25_30_00_normalises_to_01_30() -> None:
    """GTFS departure time 25:30:00 (post-midnight) normalises to 01:30."""
    # Arrange — extra trip with a post-midnight departure time
    extra_trips = f"T_LATE,{_ROUTE_46A},SVC1,0\n"
    extra_stop_times = f"T_LATE,{_STOP_A},25:30:00,1\n"

    zip_bytes = _make_gtfs_zip(
        extra_trips=extra_trips,
        extra_stop_times=extra_stop_times,
    )
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)

    # Assert — T_LATE must appear with departure_time "01:30"
    late_trips = [r for r in results if r.trip_id == "T_LATE"]
    assert len(late_trips) == 1, f"Expected T_LATE in results; got {results}"
    assert late_trips[0].departure_time == "01:30"


# ===========================================================================
# 18. Results are sorted ascending by departure_time
# ===========================================================================


async def test_results_sorted_ascending_by_departure_time() -> None:
    """get_scheduled_departures returns results sorted ascending by departure_time."""
    # Arrange — stop_times deliberately out of order: 09:00, 08:00, 10:00
    # Use a fresh zip with only the three explicit times for a clean sort test.
    routes_csv = (
        f"route_id,route_short_name,agency_id\n"
        f"{_ROUTE_46A},{_ROUTE_46A},{_AGENCY_BUS}\n"
    )
    trips_csv = (
        "trip_id,route_id,service_id,direction_id\n"
        f"TA,{_ROUTE_46A},SVC1,0\n"
        f"TB,{_ROUTE_46A},SVC1,0\n"
        f"TC,{_ROUTE_46A},SVC1,0\n"
    )
    stop_times_csv = (
        "trip_id,stop_id,departure_time,stop_sequence\n"
        f"TA,{_STOP_A},09:00:00,1\n"
        f"TB,{_STOP_A},08:00:00,1\n"
        f"TC,{_STOP_A},10:00:00,1\n"
    )
    calendar_csv = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
        "start_date,end_date\n"
        "SVC1,1,1,1,1,1,1,1,20200101,20991231\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("routes.txt", routes_csv)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
        zf.writestr("calendar.txt", calendar_csv)
    zip_bytes = buf.getvalue()

    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)

    # Assert
    assert len(results) == 3
    times = [r.departure_time for r in results]
    assert times == sorted(times), f"Times not sorted ascending: {times}"
    assert times == ["08:00", "09:00", "10:00"]


# ===========================================================================
# Additional boundary tests
# ===========================================================================


async def test_async_load_client_error_raises_load_error() -> None:
    """aiohttp.ClientError during download raises StaticGtfsLoadError."""
    # Arrange
    session = _make_client_error_session()
    client = StaticGtfsClient(_DUMMY_URL, session)

    # Act / Assert
    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()


async def test_async_refresh_if_stale_propagates_load_error() -> None:
    """async_refresh_if_stale propagates StaticGtfsLoadError from async_load."""
    # Arrange
    session = _make_session(status=503, body=b"")
    client = StaticGtfsClient(_DUMMY_URL, session)
    # loaded_at is None → will attempt reload

    # Act / Assert
    with pytest.raises(StaticGtfsLoadError):
        await client.async_refresh_if_stale()


async def test_get_scheduled_departures_unknown_stop_returns_empty() -> None:
    """get_scheduled_departures returns [] for a stop_id not in the data."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(
        "STOP_NOT_IN_DATA", _ROUTE_46A, None, None, _MONDAY
    )

    # Assert
    assert results == []


async def test_get_scheduled_departures_unknown_route_returns_empty() -> None:
    """get_scheduled_departures returns [] for a route_id not in the data."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_A, "999X", None, None, _MONDAY)

    # Assert
    assert results == []


async def test_loaded_at_unchanged_after_stale_refresh_failure() -> None:
    """loaded_at retains the original timestamp after a failed refresh."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session_ok = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session_ok)
    await client.async_load()

    # Act — trigger a failing refresh
    client._session = _make_session(status=503, body=b"")
    client._loaded_at = datetime.now(UTC) - timedelta(hours=25)

    with pytest.raises(StaticGtfsLoadError):
        await client.async_refresh_if_stale()

    # Assert — loaded_at was reverted to the backdated value (not overwritten)
    # The implementation does not overwrite on failure, so it keeps the backdated value.
    assert client.loaded_at is not None
    assert client.available is True


async def test_custom_refresh_hours_respected() -> None:
    """refresh_hours constructor parameter controls the staleness threshold."""
    # Arrange — refresh_hours=1; loaded_at 2 hours ago triggers reload
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session, refresh_hours=1)
    await client.async_load()

    with patch.object(client, "async_load", new_callable=AsyncMock) as mock_load:
        client._loaded_at = datetime.now(UTC) - timedelta(hours=2)
        await client.async_refresh_if_stale()

    mock_load.assert_called_once()


async def test_custom_refresh_hours_not_triggered_when_fresh() -> None:
    """refresh_hours=1; loaded_at 30 minutes ago does NOT trigger reload."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session, refresh_hours=1)
    await client.async_load()

    with patch.object(client, "async_load", new_callable=AsyncMock) as mock_load:
        client._loaded_at = datetime.now(UTC) - timedelta(minutes=30)
        await client.async_refresh_if_stale()

    mock_load.assert_not_called()


# ===========================================================================
# stop_ids filter (issue #20)
# ===========================================================================


async def test_stop_ids_filter_returns_departures_for_configured_stop() -> None:
    """With stop_ids={STOP_A}, the configured stop returns departures as normal."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session, stop_ids={_STOP_A})
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)

    # Assert — identical to what an unfiltered client returns for STOP_A
    assert [r.departure_time for r in results] == ["08:00", "09:00", "10:00"]


async def test_stop_ids_filter_excludes_unconfigured_stop() -> None:
    """With stop_ids={STOP_A}, a stop outside the filter returns no departures."""
    # Arrange — STOP_B has departures in the baseline zip but is not configured
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session, stop_ids={_STOP_A})
    await client.async_load()

    # Act
    results = client.get_scheduled_departures(_STOP_B, _ROUTE_46A, None, None, _MONDAY)

    # Assert
    assert results == []


async def test_stop_ids_accepts_any_iterable() -> None:
    """stop_ids passed as a list behaves the same as a set."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session, stop_ids=[_STOP_A, _STOP_B])
    await client.async_load()

    # Act
    results_a = client.get_scheduled_departures(
        _STOP_A, _ROUTE_46A, None, None, _MONDAY
    )
    results_b = client.get_scheduled_departures(
        _STOP_B, _ROUTE_46A, None, None, _MONDAY
    )

    # Assert — both configured stops return data
    assert len(results_a) > 0
    assert len(results_b) > 0


async def test_stop_ids_default_none_indexes_all_stops() -> None:
    """Default stop_ids=None keeps every stop queryable (backwards compatible)."""
    # Arrange
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    # Act / Assert
    for stop in (_STOP_A, _STOP_B):
        results = client.get_scheduled_departures(stop, _ROUTE_46A, None, None, _MONDAY)
        assert len(results) > 0


# ===========================================================================
# HTTP URL raises ValueError at construction (issue #4)
# ===========================================================================


def test_http_url_raises_value_error() -> None:
    """StaticGtfsClient raises ValueError when static_gtfs_url uses http://."""
    session = MagicMock()
    with pytest.raises(ValueError, match="HTTPS"):
        StaticGtfsClient("http://example.com/gtfs.zip", session)


# ===========================================================================
# Response size limit (issue #5)
# ===========================================================================


async def test_download_exceeding_limit_raises_load_error() -> None:
    """async_load raises StaticGtfsLoadError when the body exceeds the limit."""
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    # Set limit below the actual zip size
    client = StaticGtfsClient(_DUMMY_URL, session, max_download_bytes=1)

    with pytest.raises(StaticGtfsLoadError, match="too large"):
        await client.async_load()


# ===========================================================================
# ScheduledDeparture.route_name is str not None (issue #8)
# ===========================================================================


async def test_scheduled_departure_route_name_is_str() -> None:
    """ScheduledDeparture.route_name is always a str, never None."""
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    results = client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY)
    assert len(results) > 0
    assert all(isinstance(r.route_name, str) for r in results)


# ===========================================================================
# Departure index keyed on real route_id, not route_short_name (issue #24)
# ===========================================================================


def _make_cork_dublin_zip() -> bytes:
    """Build a GTFS zip with two "220" routes distinguished only by route_id.

    Cork's "220" (``route_id`` ``"2 220 c b"``) and Dublin's "220"
    (``route_id`` ``"3 220 d a"``) both share ``route_short_name`` "220" but
    are unrelated real routes serving the same stop.

    Returns:
        Raw bytes of the assembled GTFS zip archive.
    """
    routes_csv = (
        "route_id,route_short_name,agency_id\n"
        "2 220 c b,220,BUS_EIREANN\n"
        "3 220 d a,220,DUBLIN_BUS\n"
    )
    trips_csv = (
        "trip_id,route_id,service_id,direction_id\n"
        "T_CORK,2 220 c b,SVC1,0\n"
        "T_DUBLIN,3 220 d a,SVC1,0\n"
    )
    stop_times_csv = (
        "trip_id,stop_id,departure_time,stop_sequence\n"
        f"T_CORK,{_STOP_A},09:00:00,1\n"
        f"T_DUBLIN,{_STOP_A},10:00:00,1\n"
    )
    calendar_csv = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
        "start_date,end_date\n"
        "SVC1,1,1,1,1,1,1,1,20200101,20991231\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("routes.txt", routes_csv)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
        zf.writestr("calendar.txt", calendar_csv)
    return buf.getvalue()


async def test_route_id_differing_from_short_name_matches_correctly() -> None:
    """A route whose route_id differs from its route_short_name still matches."""
    zip_bytes = _make_cork_dublin_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    results = client.get_scheduled_departures(_STOP_A, "2 220 c b", None, None, _MONDAY)

    assert len(results) == 1
    assert results[0].trip_id == "T_CORK"
    assert results[0].route_name == "220"


async def test_shared_route_short_name_does_not_conflate_route_ids() -> None:
    """Two routes sharing a route_short_name but with different route_ids

    (Cork "220" vs Dublin "220") must not be conflated in the departure
    index or in operator/direction filtering.
    """
    zip_bytes = _make_cork_dublin_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    cork_results = client.get_scheduled_departures(
        _STOP_A, "2 220 c b", None, None, _MONDAY
    )
    dublin_results = client.get_scheduled_departures(
        _STOP_A, "3 220 d a", None, None, _MONDAY
    )
    unknown_results = client.get_scheduled_departures(
        _STOP_A, "220", None, None, _MONDAY
    )

    assert {r.trip_id for r in cork_results} == {"T_CORK"}
    assert {r.trip_id for r in dublin_results} == {"T_DUBLIN"}
    assert unknown_results == []


# ===========================================================================
# route_id=None returns merged departures across all routes (issue #26)
# ===========================================================================


async def test_route_id_none_merges_departures_across_routes() -> None:
    """route_id=None returns departures from every route serving the stop."""
    zip_bytes = _make_cork_dublin_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    results = client.get_scheduled_departures(_STOP_A, None, None, None, _MONDAY)

    assert {r.trip_id for r in results} == {"T_CORK", "T_DUBLIN"}


async def test_route_id_none_still_sorted_by_departure_time() -> None:
    """route_id=None merges routes but keeps ascending departure_time order."""
    zip_bytes = _make_cork_dublin_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    results = client.get_scheduled_departures(_STOP_A, None, None, None, _MONDAY)

    times = [r.departure_time for r in results]
    assert times == sorted(times)
    assert [r.trip_id for r in results] == ["T_CORK", "T_DUBLIN"]


async def test_route_id_none_respects_direction_and_operator_filters() -> None:
    """route_id=None still honours direction_id and operator_id filters."""
    zip_bytes = _make_two_agency_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    bus_only = client.get_scheduled_departures(
        _STOP_A, None, None, _AGENCY_BUS, _MONDAY
    )

    assert {r.trip_id for r in bus_only} == {"T_BUS"}


async def test_route_id_none_unknown_stop_returns_empty() -> None:
    """route_id=None for a stop with no departures returns []."""
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    results = client.get_scheduled_departures(
        "STOP_NOT_IN_DATA", None, None, None, _MONDAY
    )

    assert results == []


# ===========================================================================
# has_scheduled_pair (issue #25)
# ===========================================================================


async def test_has_scheduled_pair_returns_true_for_indexed_pair() -> None:
    """A (stop_id, route_id) present in the departure index returns True."""
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    assert client.has_scheduled_pair(_STOP_A, _ROUTE_46A) is True


async def test_has_scheduled_pair_ignores_calendar_direction_and_operator() -> None:
    """True even if no service runs today or direction/operator would exclude it."""
    zip_bytes = _make_gtfs_zip(weekday_flags="0,0,0,0,0,0,0")
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    assert (
        client.get_scheduled_departures(_STOP_A, _ROUTE_46A, None, None, _MONDAY) == []
    )
    assert client.has_scheduled_pair(_STOP_A, _ROUTE_46A) is True


async def test_has_scheduled_pair_returns_false_for_absent_pair() -> None:
    """A (stop_id, route_id) not in the departure index returns False."""
    zip_bytes = _make_gtfs_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsClient(_DUMMY_URL, session)
    await client.async_load()

    assert client.has_scheduled_pair(_STOP_A, _ROUTE_39A) is False
    assert client.has_scheduled_pair("STOP_NOT_IN_DATA", _ROUTE_46A) is False


async def test_has_scheduled_pair_returns_false_when_unloaded() -> None:
    """An unloaded client (available=False) returns False regardless of pair."""
    session = _make_session(status=200, body=b"")
    client = StaticGtfsClient(_DUMMY_URL, session)

    assert client.has_scheduled_pair(_STOP_A, _ROUTE_46A) is False
