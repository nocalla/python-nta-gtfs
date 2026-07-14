"""Lightweight one-shot static GTFS picker client for config-flow lookups."""

import asyncio
import zipfile
from typing import IO, NamedTuple

import aiohttp

from nta_gtfs._streaming import download_zip_to_tempfile, iter_csv
from nta_gtfs.exceptions import StaticGtfsLoadError


class Stop(NamedTuple):
    """A single stop parsed from ``stops.txt``.

    Attributes:
        stop_id: GTFS stop identifier.
        stop_code: Rider-facing stop code, may be empty.
        stop_name: Human-readable stop name.
    """

    stop_id: str
    stop_code: str
    stop_name: str


class Route(NamedTuple):
    """A single route parsed from ``routes.txt``.

    Attributes:
        route_id: Real GTFS route ID, the same identifier used by
            ``StaticGtfsClient.get_scheduled_departures`` and GTFS-RT
            ``TripDescriptor.route_id``.
        route_short_name: Route short name (e.g. ``"46A"``), for display only.
        agency_id: Agency ID from ``routes.txt``, or ``None`` when blank.
    """

    route_id: str
    route_short_name: str
    agency_id: str | None


class StaticGtfsPickerClient:
    """One-shot client for a "download once, query twice, discard" lifecycle.

    Purpose-built for a config-flow style stop/route picker: downloads the
    static GTFS zip exactly once via ``async_load``, then answers both
    ``list_stops``/``list_routes`` and ``async_get_routes_for_stop`` against
    that single cached archive without re-downloading. Distinct from
    ``StaticGtfsClient``'s long-lived, coordinator-polling lifecycle — this
    class has no staleness tracking or ``stop_ids`` filter. The caller
    supplies an ``aiohttp.ClientSession``; this class never creates its own.
    Call ``async_close`` once finished to release the cached archive.
    """

    def __init__(
        self,
        static_gtfs_url: str,
        session: aiohttp.ClientSession,
        max_download_bytes: int = 200 * 1024 * 1024,
    ) -> None:
        """Initialise the client.

        Args:
            static_gtfs_url: URL of the static GTFS zip to download.  Must use
                the ``https://`` scheme; an ``http://`` URL raises
                ``ValueError``.
            session: Caller-supplied aiohttp client session used for downloads.
            max_download_bytes: Maximum permitted response body size in bytes.
                Defaults to 200 MiB.  ``async_load`` raises
                ``StaticGtfsLoadError`` if the Content-Length header or the
                actual downloaded body exceeds this limit.

        Raises:
            ValueError: If ``static_gtfs_url`` does not start with
                ``https://``.
        """
        self._url = static_gtfs_url
        self._session = session
        self._max_download_bytes = max_download_bytes
        if not static_gtfs_url.startswith("https://"):
            raise ValueError(
                f"static_gtfs_url must use HTTPS; got: {static_gtfs_url!r}"
            )
        self._archive: IO[bytes] | None = None
        self._stops: list[Stop] = []
        self._routes: list[Route] = []
        self._available: bool = False

    @property
    def available(self) -> bool:
        """True when the client has been successfully loaded.

        Returns:
            Boolean availability flag.
        """
        return self._available

    async def async_load(self) -> None:
        """Download the static GTFS zip once and parse stops.txt + routes.txt.

        Streams the zip from ``static_gtfs_url`` in chunks to an anonymous
        temporary file, then parses only ``stops.txt`` and ``routes.txt`` —
        ``trips.txt``, ``stop_times.txt``, and ``calendar*.txt`` are skipped.
        The downloaded archive is kept open and cached on the instance so
        ``async_get_routes_for_stop`` can query it later without
        re-downloading. CPU-intensive parsing is offloaded to a thread via
        ``asyncio.to_thread`` so the event loop is not blocked.

        On success ``available`` is set to ``True``.

        Raises:
            StaticGtfsLoadError: On any download or parse failure.
        """
        tmp = await download_zip_to_tempfile(
            self._url, self._session, self._max_download_bytes
        )
        try:
            stops, routes = await asyncio.to_thread(_parse_stops_and_routes, tmp)
        except Exception as exc:
            await asyncio.to_thread(tmp.close)
            if isinstance(exc, StaticGtfsLoadError):
                raise
            raise StaticGtfsLoadError(f"Static GTFS parse error: {exc}") from exc

        if self._archive is not None:
            await asyncio.to_thread(self._archive.close)
        self._archive = tmp
        self._stops = stops
        self._routes = routes
        self._available = True

    def list_stops(self) -> list[Stop]:
        """Return every stop parsed from ``stops.txt``.

        Returns:
            List of ``Stop`` named tuples. Empty before ``async_load`` has
            succeeded.
        """
        return list(self._stops)

    def list_routes(self) -> list[Route]:
        """Return every route parsed from ``routes.txt``.

        Returns:
            List of ``Route`` named tuples. Empty before ``async_load`` has
            succeeded.
        """
        return list(self._routes)

    async def async_get_routes_for_stop(self, stop_id: str) -> list[Route]:
        """Return routes with a real ``stop_times.txt`` link to ``stop_id``.

        Performs a targeted ``stop_times.txt``→``trips.txt`` join against the
        already-downloaded archive, without loading the full departure index
        that ``StaticGtfsClient`` builds. The join runs in a thread via
        ``asyncio.to_thread`` since it re-scans the archive's CSV files.

        Args:
            stop_id: GTFS stop ID to find linked routes for.

        Returns:
            List of ``Route`` named tuples serving ``stop_id``, in
            ``list_routes`` order. Empty if no ``stop_times.txt`` row links
            to ``stop_id``.

        Raises:
            StaticGtfsLoadError: If called before ``async_load`` has
                succeeded, after ``async_close``, or if the cached archive
                cannot be re-read (e.g. a required file is missing).
        """
        if self._archive is None:
            raise StaticGtfsLoadError(
                "StaticGtfsPickerClient has not been loaded; call async_load() first."
            )
        try:
            route_ids = await asyncio.to_thread(
                _route_ids_for_stop, self._archive, stop_id
            )
        except Exception as exc:
            raise StaticGtfsLoadError(
                f"Static GTFS routes-for-stop lookup error: {exc}"
            ) from exc
        if not route_ids:
            return []
        return [route for route in self._routes if route.route_id in route_ids]

    async def async_close(self) -> None:
        """Close and discard the cached archive.

        Safe to call multiple times or when never loaded.
        """
        if self._archive is not None:
            await asyncio.to_thread(self._archive.close)
            self._archive = None


def _parse_stops_and_routes(fileobj: IO[bytes]) -> tuple[list[Stop], list[Route]]:
    """Extract ``stops.txt`` and ``routes.txt`` from a seekable GTFS zip.

    Args:
        fileobj: Seekable binary file object containing the GTFS zip archive.

    Returns:
        A two-tuple of ``(stops, routes)``.

    Raises:
        StaticGtfsLoadError: If a required file is missing from the zip or a
            CSV cannot be parsed.
    """
    with zipfile.ZipFile(fileobj) as zf:
        stops = [
            Stop(
                stop_id=row.get("stop_id", ""),
                stop_code=row.get("stop_code", ""),
                stop_name=row.get("stop_name", ""),
            )
            for row in iter_csv(zf, "stops.txt")
        ]
        routes = [
            Route(
                route_id=row.get("route_id", ""),
                route_short_name=row.get("route_short_name", ""),
                agency_id=row.get("agency_id") or None,
            )
            for row in iter_csv(zf, "routes.txt")
        ]
    return stops, routes


def _route_ids_for_stop(fileobj: IO[bytes], stop_id: str) -> set[str]:
    """Return the set of real ``route_id``s linked to ``stop_id``.

    Streams ``stop_times.txt`` filtered to ``stop_id`` to collect trip IDs,
    then streams ``trips.txt`` filtered to those trip IDs to collect route
    IDs — mirroring the ``stop_ids``-filter narrowing pattern used by
    ``StaticGtfsClient``'s departure index, but scoped to route discovery.

    Args:
        fileobj: Seekable binary file object containing the GTFS zip archive.
        stop_id: GTFS stop ID to find linked trips for.

    Returns:
        Set of GTFS ``route_id`` strings with a real ``stop_times.txt`` link
        to ``stop_id``.
    """
    with zipfile.ZipFile(fileobj) as zf:
        trip_ids = {
            row.get("trip_id", "")
            for row in iter_csv(zf, "stop_times.txt")
            if row.get("stop_id") == stop_id
        }
        if not trip_ids:
            return set()

        route_ids = {
            row.get("route_id", "")
            for row in iter_csv(zf, "trips.txt")
            if row.get("trip_id") in trip_ids
        }
    return route_ids
