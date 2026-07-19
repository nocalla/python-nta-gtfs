"""Lightweight one-shot static GTFS picker client for config-flow lookups."""

import asyncio
import zipfile
from dataclasses import dataclass
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


@dataclass
class _StopCache:
    """Per-``stop_id`` cache entry of candidate trips and their termini.

    Attributes:
        candidate_trip_ids: Trip IDs with a real ``stop_times.txt`` link to
            this stop.
        terminus_by_trip: Mapping of trip_id to terminus stop_id for
            ``candidate_trip_ids``. ``None`` until the first
            ``async_get_termini`` call for this stop computes it.
    """

    candidate_trip_ids: frozenset[str]
    terminus_by_trip: dict[str, str] | None = None


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
        self._trip_index: dict[str, tuple[str, str]] | None = None
        self._stop_cache: dict[str, _StopCache] = {}

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
        self._trip_index = None
        self._stop_cache = {}
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

    async def _ensure_trip_index(self) -> None:
        """Build ``self._trip_index`` from ``trips.txt`` if not already built.

        No-op once ``self._trip_index`` is populated; reused for the rest of
        the instance's lifetime.
        """
        if self._trip_index is None:
            self._trip_index = await asyncio.to_thread(_build_trip_index, self._archive)

    async def _ensure_stop_cache(self, stop_id: str) -> _StopCache:
        """Return the ``_StopCache`` entry for ``stop_id``, building it if new.

        Args:
            stop_id: GTFS stop ID to fetch or build a cache entry for.

        Returns:
            The (possibly newly-built) ``_StopCache`` entry for ``stop_id``.
        """
        entry = self._stop_cache.get(stop_id)
        if entry is None:
            candidate_trip_ids = await asyncio.to_thread(
                _candidate_trip_ids_for_stop, self._archive, stop_id
            )
            entry = _StopCache(candidate_trip_ids=candidate_trip_ids)
            self._stop_cache[stop_id] = entry
        return entry

    async def async_get_routes_for_stop(self, stop_id: str) -> list[Route]:
        """Return routes with a real ``stop_times.txt`` link to ``stop_id``.

        Builds (or reuses) the trip index and this stop's candidate-trip
        cache, then derives matching routes purely in memory — no
        ``trips.txt`` rescan on repeat calls or repeat stops sharing trips.

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
            await self._ensure_trip_index()
            entry = await self._ensure_stop_cache(stop_id)
        except Exception as exc:
            raise StaticGtfsLoadError(
                f"Static GTFS routes-for-stop lookup error: {exc}"
            ) from exc
        trip_index = self._trip_index or {}
        route_ids = {
            trip_index[trip_id][0]
            for trip_id in entry.candidate_trip_ids
            if trip_id in trip_index
        }
        if not route_ids:
            return []
        return [route for route in self._routes if route.route_id in route_ids]

    async def async_get_termini(
        self, stop_id: str, route_id: str | None, direction_id: int
    ) -> list[str]:
        """Return terminus stop name(s) for trips through a stop/route/direction.

        Finds every trip that calls at ``stop_id`` and matches ``route_id``
        (or every route, when ``route_id`` is ``None``, for the "all routes
        at this stop" combined case) and ``direction_id``, then resolves each
        such trip's own last stop — its terminus — via
        ``max(stop_time.stop_sequence)``. Distinct branches of the same
        route/direction can end at different termini, so this returns every
        distinct terminus name found, not a single value.

        Scoped to trips already narrowed to a single stop (typically tens to
        a few hundred, per the config-flow use case this exists for), so the
        ``stop_times.txt`` pass(es) this performs stay small regardless of
        the archive's overall size; candidates and termini are each computed
        at most once per ``stop_id`` for the instance's lifetime, and reused
        across repeat calls and repeat directions.

        Args:
            stop_id: GTFS stop ID the trip must call at.
            route_id: Real GTFS route ID to filter on; ``None`` matches every
                route serving ``stop_id``.
            direction_id: GTFS direction ID (``0`` or ``1``) to filter on.

        Returns:
            Sorted list of distinct terminus ``stop_name`` values. Empty if
            no trip matches, or if any matching trip's terminus stop is
            missing from ``stops.txt`` or has a blank name.

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
            await self._ensure_trip_index()
            entry = await self._ensure_stop_cache(stop_id)
            if entry.terminus_by_trip is None:
                entry.terminus_by_trip = await asyncio.to_thread(
                    _terminus_by_trip_for_candidates,
                    self._archive,
                    entry.candidate_trip_ids,
                )
        except Exception as exc:
            raise StaticGtfsLoadError(
                f"Static GTFS termini lookup error: {exc}"
            ) from exc

        trip_index = self._trip_index or {}
        direction_str = str(direction_id)
        matching_trip_ids = {
            trip_id
            for trip_id in entry.candidate_trip_ids
            if trip_id in trip_index
            and (route_id is None or trip_index[trip_id][0] == route_id)
            and trip_index[trip_id][1] == direction_str
        }
        if not matching_trip_ids:
            return []
        terminus_by_trip = entry.terminus_by_trip or {}
        terminus_stop_ids = {
            terminus_by_trip[trip_id]
            for trip_id in matching_trip_ids
            if trip_id in terminus_by_trip
        }
        if not terminus_stop_ids:
            return []
        names = {
            stop.stop_name
            for stop in self._stops
            if stop.stop_id in terminus_stop_ids and stop.stop_name
        }
        return sorted(names)

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


def _build_trip_index(fileobj: IO[bytes]) -> dict[str, tuple[str, str]]:
    """Build a ``trip_id -> (route_id, direction_id)`` index from ``trips.txt``.

    Args:
        fileobj: Seekable binary file object containing the GTFS zip archive.

    Returns:
        Mapping of ``trip_id`` to ``(route_id, direction_id)``, both kept as
        raw strings matching ``trips.txt``'s native columns.
    """
    with zipfile.ZipFile(fileobj) as zf:
        return {
            row.get("trip_id", ""): (
                row.get("route_id", ""),
                row.get("direction_id", ""),
            )
            for row in iter_csv(zf, "trips.txt")
        }


def _candidate_trip_ids_for_stop(fileobj: IO[bytes], stop_id: str) -> frozenset[str]:
    """Return candidate ``trip_id``s from ``stop_times.txt`` rows at ``stop_id``.

    Args:
        fileobj: Seekable binary file object containing the GTFS zip archive.
        stop_id: GTFS stop ID to find linked trips for.

    Returns:
        Frozenset of ``trip_id`` strings with a real ``stop_times.txt`` link
        to ``stop_id``.
    """
    with zipfile.ZipFile(fileobj) as zf:
        return frozenset(
            row.get("trip_id", "")
            for row in iter_csv(zf, "stop_times.txt")
            if row.get("stop_id") == stop_id
        )


def _terminus_by_trip_for_candidates(
    fileobj: IO[bytes], candidate_trip_ids: frozenset[str]
) -> dict[str, str]:
    """Resolve each candidate trip's terminus ``stop_id`` via ``max(stop_sequence)``.

    Per the #124 research finding that ``stop_sequence`` reliably identifies a
    single unambiguous last stop. Rows with a non-integer ``stop_sequence``
    are skipped rather than raised on, since a future feed publish is not
    guaranteed to stay as clean as the one surveyed.

    Args:
        fileobj: Seekable binary file object containing the GTFS zip archive.
        candidate_trip_ids: Trip IDs to resolve termini for.

    Returns:
        Mapping of ``trip_id`` to its terminus ``stop_id``, one entry per
        candidate trip with at least one valid ``stop_times.txt`` row.
    """
    max_sequence_by_trip: dict[str, int] = {}
    terminus_by_trip: dict[str, str] = {}
    with zipfile.ZipFile(fileobj) as zf:
        for row in iter_csv(zf, "stop_times.txt"):
            trip_id = row.get("trip_id", "")
            if trip_id not in candidate_trip_ids:
                continue
            try:
                sequence = int(row.get("stop_sequence", ""))
            except ValueError:
                continue
            if sequence >= max_sequence_by_trip.get(trip_id, -1):
                max_sequence_by_trip[trip_id] = sequence
                terminus_by_trip[trip_id] = row.get("stop_id", "")
    return terminus_by_trip
