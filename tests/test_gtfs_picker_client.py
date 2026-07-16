"""Unit tests for nta_gtfs.StaticGtfsPickerClient.

All GTFS data is built as in-memory zip bytes and no live HTTP calls are
made; the client itself spools downloads to an anonymous temporary file.
"""

import io
import zipfile
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from nta_gtfs.exceptions import StaticGtfsLoadError
from nta_gtfs.gtfs_picker import Route, StaticGtfsPickerClient, Stop

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DUMMY_URL = "https://example.com/gtfs.zip"


# ---------------------------------------------------------------------------
# GTFS zip fixture builder
# ---------------------------------------------------------------------------


def _make_picker_zip(*, blank_agency: bool = False) -> bytes:
    """Build a minimal in-memory GTFS zip for picker-client tests.

    Two stops (S1, S2), two routes (R1/46A, R2/39A). R1 has a real
    stop_times.txt link to S1; R2 has a real link to S2 only. Neither route
    is linked to both stops, so routes-for-stop narrowing is meaningful.

    Args:
        blank_agency: When True, R1's agency_id column is left blank.

    Returns:
        Raw bytes of the assembled GTFS zip archive.
    """
    stops_csv = "stop_id,stop_code,stop_name\nS1,S1CODE,Stop One\nS2,S2CODE,Stop Two\n"
    agency_r1 = "" if blank_agency else "BUS_CO"
    routes_csv = (
        f"route_id,route_short_name,agency_id\nR1,46A,{agency_r1}\nR2,39A,BUS_CO\n"
    )
    trips_csv = "trip_id,route_id\nT1,R1\nT2,R2\n"
    stop_times_csv = "trip_id,stop_id\nT1,S1\nT2,S2\n"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("stops.txt", stops_csv)
        zf.writestr("routes.txt", routes_csv)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
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


# ===========================================================================
# Construction
# ===========================================================================


def test_http_url_raises_value_error() -> None:
    """StaticGtfsPickerClient raises ValueError when static_gtfs_url uses http://."""
    session = MagicMock()
    with pytest.raises(ValueError, match="HTTPS"):
        StaticGtfsPickerClient("http://example.com/gtfs.zip", session)


# ===========================================================================
# async_load
# ===========================================================================


async def test_async_load_success_sets_available_true() -> None:
    """async_load with a valid GTFS zip sets available=True."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)

    assert client.available is False
    await client.async_load()
    assert client.available is True


async def test_async_load_http_error_raises_load_error() -> None:
    """async_load with an HTTP error status raises StaticGtfsLoadError."""
    session = _make_session(status=503, body=b"Service Unavailable")
    client = StaticGtfsPickerClient(_DUMMY_URL, session)

    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()

    assert client.available is False


async def test_async_load_malformed_zip_raises_load_error() -> None:
    """async_load with a non-zip body raises StaticGtfsLoadError."""
    session = _make_session(status=200, body=b"this is not a zip file")
    client = StaticGtfsPickerClient(_DUMMY_URL, session)

    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()


async def test_download_exceeding_limit_raises_load_error() -> None:
    """async_load raises StaticGtfsLoadError when the body exceeds the limit."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session, max_download_bytes=1)

    with pytest.raises(StaticGtfsLoadError, match="too large"):
        await client.async_load()


async def test_async_load_client_error_raises_load_error() -> None:
    """aiohttp.ClientError during download raises StaticGtfsLoadError."""
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection refused"))
    cm.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.get = MagicMock(return_value=cm)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)

    with pytest.raises(StaticGtfsLoadError):
        await client.async_load()


# ===========================================================================
# list_stops / list_routes
# ===========================================================================


async def test_list_stops_returns_parsed_rows() -> None:
    """list_stops returns Stop tuples parsed from stops.txt only."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    stops = client.list_stops()

    assert set(stops) == {
        Stop(stop_id="S1", stop_code="S1CODE", stop_name="Stop One"),
        Stop(stop_id="S2", stop_code="S2CODE", stop_name="Stop Two"),
    }


async def test_list_routes_returns_parsed_rows() -> None:
    """list_routes returns Route tuples parsed from routes.txt only."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    routes = client.list_routes()

    assert set(routes) == {
        Route(route_id="R1", route_short_name="46A", agency_id="BUS_CO"),
        Route(route_id="R2", route_short_name="39A", agency_id="BUS_CO"),
    }


async def test_blank_agency_id_becomes_none() -> None:
    """A blank agency_id column parses to None, not an empty string."""
    zip_bytes = _make_picker_zip(blank_agency=True)
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    r1 = next(r for r in client.list_routes() if r.route_id == "R1")
    assert r1.agency_id is None


async def test_list_stops_and_routes_empty_before_load() -> None:
    """list_stops/list_routes return [] before any successful load."""
    session = _make_session(status=200, body=b"irrelevant")
    client = StaticGtfsPickerClient(_DUMMY_URL, session)

    assert client.list_stops() == []
    assert client.list_routes() == []


# ===========================================================================
# async_get_routes_for_stop
# ===========================================================================


async def test_async_get_routes_for_stop_returns_only_linked_routes() -> None:
    """Only routes with a real stop_times.txt link to the stop are returned."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    routes_for_s1 = await client.async_get_routes_for_stop("S1")

    assert {r.route_id for r in routes_for_s1} == {"R1"}


async def test_async_get_routes_for_stop_unknown_stop_returns_empty() -> None:
    """A stop_id with no stop_times.txt rows returns []."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    routes = await client.async_get_routes_for_stop("STOP_NOT_IN_DATA")

    assert routes == []


async def test_async_get_routes_for_stop_before_load_raises() -> None:
    """async_get_routes_for_stop before async_load raises StaticGtfsLoadError."""
    session = _make_session(status=200, body=b"irrelevant")
    client = StaticGtfsPickerClient(_DUMMY_URL, session)

    with pytest.raises(StaticGtfsLoadError):
        await client.async_get_routes_for_stop("S1")


async def test_async_get_routes_for_stop_missing_stop_times_raises_load_error() -> None:
    """A cached archive missing stop_times.txt raises StaticGtfsLoadError.

    async_load only requires stops.txt/routes.txt to succeed, so a malformed
    archive can reach async_get_routes_for_stop with stop_times.txt absent;
    the lookup must still surface a library exception, not a raw KeyError.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("stops.txt", "stop_id,stop_code,stop_name\nS1,S1CODE,Stop One\n")
        zf.writestr(
            "routes.txt", "route_id,route_short_name,agency_id\nR1,46A,BUS_CO\n"
        )
    zip_bytes = buf.getvalue()

    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    with pytest.raises(StaticGtfsLoadError):
        await client.async_get_routes_for_stop("S1")


async def test_async_get_routes_for_stop_does_not_redownload() -> None:
    """Two lookups against the same load only download the archive once."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    await client.async_get_routes_for_stop("S1")
    await client.async_get_routes_for_stop("S2")

    assert session.get.call_count == 1


# ===========================================================================
# async_get_termini
# ===========================================================================


def _make_termini_zip() -> bytes:
    """Build a GTFS zip for terminus-lookup tests.

    Stop S1 is served by route R1 in both directions, with two branches in
    direction 0 (trips T1/T2 ending at different termini, S3 and S4) and one
    trip in direction 1 (T3, ending at S1 itself). Route R2 also calls at S1
    (trip T4, direction 0, ending at S5) to exercise the combined-routes
    (``route_id=None``) case and the single-route filter against it.

    Returns:
        Raw bytes of the assembled GTFS zip archive.
    """
    stops_csv = (
        "stop_id,stop_code,stop_name\n"
        "S1,S1CODE,Stop One\n"
        "S3,S3CODE,Terminus Three\n"
        "S4,S4CODE,Terminus Four\n"
        "S5,S5CODE,Terminus Five\n"
    )
    routes_csv = "route_id,route_short_name,agency_id\nR1,46A,BUS_CO\nR2,39A,BUS_CO\n"
    trips_csv = "trip_id,route_id,direction_id\nT1,R1,0\nT2,R1,0\nT3,R1,1\nT4,R2,0\n"
    stop_times_csv = (
        "trip_id,stop_id,stop_sequence\n"
        "T1,S1,1\n"
        "T1,S3,2\n"
        "T2,S1,1\n"
        "T2,S4,2\n"
        "T3,S4,1\n"
        "T3,S1,2\n"
        "T4,S1,1\n"
        "T4,S5,2\n"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("stops.txt", stops_csv)
        zf.writestr("routes.txt", routes_csv)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
    return buf.getvalue()


async def test_async_get_termini_returns_distinct_branch_termini() -> None:
    """Two branches of the same route/direction return both terminus names."""
    zip_bytes = _make_termini_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    termini = await client.async_get_termini("S1", "R1", 0)

    assert termini == ["Terminus Four", "Terminus Three"]


async def test_async_get_termini_filters_by_direction() -> None:
    """The opposite direction resolves to its own, different terminus."""
    zip_bytes = _make_termini_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    termini = await client.async_get_termini("S1", "R1", 1)

    assert termini == ["Stop One"]


async def test_async_get_termini_none_route_combines_all_routes() -> None:
    """route_id=None merges termini across every route serving the stop."""
    zip_bytes = _make_termini_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    termini = await client.async_get_termini("S1", None, 0)

    assert termini == ["Terminus Five", "Terminus Four", "Terminus Three"]


async def test_async_get_termini_unmatched_route_returns_empty() -> None:
    """A route_id with no matching trips at the stop returns []."""
    zip_bytes = _make_termini_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    termini = await client.async_get_termini("S1", "R2", 1)

    assert termini == []


async def test_async_get_termini_before_load_raises() -> None:
    """async_get_termini before async_load raises StaticGtfsLoadError."""
    session = _make_session(status=200, body=b"irrelevant")
    client = StaticGtfsPickerClient(_DUMMY_URL, session)

    with pytest.raises(StaticGtfsLoadError):
        await client.async_get_termini("S1", "R1", 0)


async def test_async_get_termini_missing_stop_times_raises_load_error() -> None:
    """A cached archive missing stop_times.txt surfaces StaticGtfsLoadError."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr("stops.txt", "stop_id,stop_code,stop_name\nS1,S1CODE,Stop One\n")
        zf.writestr(
            "routes.txt", "route_id,route_short_name,agency_id\nR1,46A,BUS_CO\n"
        )
    zip_bytes = buf.getvalue()

    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    with pytest.raises(StaticGtfsLoadError):
        await client.async_get_termini("S1", "R1", 0)


# ===========================================================================
# async_close
# ===========================================================================


async def test_async_close_releases_archive() -> None:
    """After async_close, async_get_routes_for_stop raises StaticGtfsLoadError."""
    zip_bytes = _make_picker_zip()
    session = _make_session(status=200, body=zip_bytes)
    client = StaticGtfsPickerClient(_DUMMY_URL, session)
    await client.async_load()

    await client.async_close()

    with pytest.raises(StaticGtfsLoadError):
        await client.async_get_routes_for_stop("S1")
