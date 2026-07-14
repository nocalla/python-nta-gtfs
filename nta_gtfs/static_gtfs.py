"""Static GTFS schedule client for the nta_gtfs library."""

import asyncio
import zipfile
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import IO, NamedTuple

import aiohttp

from nta_gtfs._streaming import download_zip_to_tempfile, iter_csv
from nta_gtfs.exceptions import StaticGtfsLoadError

_WEEKDAY_COLUMNS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


class ScheduledDeparture(NamedTuple):
    """A single scheduled departure from the static GTFS feed.

    Attributes:
        trip_id: GTFS trip identifier.
        departure_time: Scheduled departure time in ``HH:MM`` format.
        route_name: Route short name (e.g. ``"46A"``), for display only.
    """

    trip_id: str
    departure_time: str
    route_name: str


class _TripInfo(NamedTuple):
    """Parse-time join of a trip with its route.

    Attributes:
        route_id: Real GTFS route ID from ``routes.txt``, used as the
            departure index match key.
        route_short_name: Route short name from ``routes.txt``, used only for
            display (``ScheduledDeparture.route_name``).
        direction_id: GTFS direction ID string (``"0"``/``"1"``, may be empty).
        service_id: GTFS service ID the trip runs under.
        agency_id: Agency ID from the route, or ``None`` when blank.
    """

    route_id: str
    route_short_name: str
    direction_id: str
    service_id: str
    agency_id: str | None


class StaticGtfsClient:
    """Async client for downloading and querying static GTFS schedule data.

    Streams a GTFS zip from a URL to a temporary file, parses it row-by-row
    without materialising whole CSV files in memory, and exposes synchronous
    queries against the parsed schedule.  Passing ``stop_ids`` restricts the
    departure index to the given stops, keeping the memory footprint small
    on nationwide feeds.  The caller supplies an ``aiohttp.ClientSession``;
    this class never creates its own.
    """

    def __init__(
        self,
        static_gtfs_url: str,
        session: aiohttp.ClientSession,
        refresh_hours: int = 24,
        max_download_bytes: int = 200 * 1024 * 1024,
        stop_ids: Iterable[str] | None = None,
    ) -> None:
        """Initialise the client.

        Args:
            static_gtfs_url: URL of the static GTFS zip to download.  Must use
                the ``https://`` scheme; an ``http://`` URL raises
                ``ValueError``.
            session: Caller-supplied aiohttp client session used for downloads.
            refresh_hours: Age threshold in hours after which
                ``async_refresh_if_stale`` triggers a reload.
            max_download_bytes: Maximum permitted response body size in bytes.
                Defaults to 200 MiB.  ``async_load`` raises
                ``StaticGtfsLoadError`` if the Content-Length header or the
                actual downloaded body exceeds this limit.
            stop_ids: Optional collection of GTFS stop IDs to index.  When
                given, only ``stop_times.txt`` rows for these stops are kept
                during parsing, which drastically reduces memory on large
                feeds; ``get_scheduled_departures`` returns ``[]`` for any
                other stop.  ``None`` (the default) indexes every stop.

        Raises:
            ValueError: If ``static_gtfs_url`` does not start with
                ``https://``.
        """
        self._url = static_gtfs_url
        self._session = session
        self._refresh_hours = refresh_hours
        self._max_download_bytes = max_download_bytes
        self._stop_ids: frozenset[str] | None = (
            frozenset(stop_ids) if stop_ids is not None else None
        )
        if not static_gtfs_url.startswith("https://"):
            raise ValueError(
                f"static_gtfs_url must use HTTPS; got: {static_gtfs_url!r}"
            )
        self._available: bool = False
        self._loaded_at: datetime | None = None

        self._trip_service_ids: dict[str, str] = {}
        self._calendar: list[dict[str, str]] = []
        self._calendar_dates: list[dict[str, str]] = []
        self._departure_index: dict[
            tuple[str, str], list[tuple[str, str, str, str | None, str]]
        ] = {}

    @property
    def available(self) -> bool:
        """True when the client has been successfully loaded at least once.

        Returns:
            Boolean availability flag.
        """
        return self._available

    @property
    def loaded_at(self) -> datetime | None:
        """UTC datetime of the last successful load, or None if never loaded.

        Returns:
            Timezone-aware UTC datetime, or ``None``.
        """
        return self._loaded_at

    async def async_load(self) -> None:
        """Download and parse the static GTFS zip into in-memory structures.

        Streams the zip from ``static_gtfs_url`` in chunks to an anonymous
        temporary file (removed automatically when the load finishes), then
        parses it to build lookup structures for calendar, calendar_dates,
        and a pre-joined departure index.  The CPU-intensive zip and CSV
        parsing is offloaded to a thread via ``asyncio.to_thread`` so the
        event loop is not blocked; file writes likewise happen in a thread.

        On success ``available`` is set to ``True`` and ``loaded_at`` is set
        to ``datetime.now(UTC)``.

        Failure behaviour:
        - First-ever failure: ``available`` remains ``False``.
        - Refresh failure (client was already available): ``available`` stays
          ``True`` and the previously loaded data is preserved.

        Raises:
            StaticGtfsLoadError: On any download or parse failure.
        """
        tmp = await download_zip_to_tempfile(
            self._url, self._session, self._max_download_bytes
        )
        try:
            try:
                (
                    trip_service_ids,
                    calendar,
                    calendar_dates,
                    departure_index,
                ) = await asyncio.to_thread(_parse_zip, tmp, self._stop_ids)
            except StaticGtfsLoadError:
                raise
            except Exception as exc:
                raise StaticGtfsLoadError(f"Static GTFS parse error: {exc}") from exc
        finally:
            await asyncio.to_thread(tmp.close)

        self._trip_service_ids = trip_service_ids
        self._calendar = calendar
        self._calendar_dates = calendar_dates
        self._departure_index = departure_index
        self._available = True
        self._loaded_at = datetime.now(UTC)

    async def async_refresh_if_stale(self) -> None:
        """Reload static GTFS data if absent or older than ``refresh_hours``.

        Calls ``async_load`` when ``loaded_at`` is ``None`` or when more than
        ``refresh_hours`` hours have elapsed since the last successful load.
        Does nothing when the data is still fresh.

        Raises:
            StaticGtfsLoadError: Propagated from ``async_load`` on failure.
        """
        if self._loaded_at is None or (datetime.now(UTC) - self._loaded_at) > timedelta(
            hours=self._refresh_hours
        ):
            await self.async_load()

    def get_scheduled_departures(
        self,
        stop_id: str,
        route_id: str | None,
        direction_id: int | None,
        operator_id: str | None,
        target_date: date,
    ) -> list[ScheduledDeparture]:
        """Return scheduled departures for a stop on a given date.

        Looks up the pre-built departure index by ``(stop_id, route_id)`` and
        filters by active service IDs, optional direction, and optional
        agency.

        Args:
            stop_id: GTFS stop ID to filter on.
            route_id: Real GTFS route ID (e.g. ``"2 220 c b"``) matched
                against ``routes.route_id`` — the same identifier used by
                GTFS-RT ``TripDescriptor.route_id``.  ``None`` skips route
                filtering, merging departures across every route serving
                ``stop_id`` (for stop-wide monitoring).
            direction_id: GTFS direction filter (``0`` or ``1``); ``None``
                means no direction filter is applied.
            operator_id: GTFS agency ID to filter on; ``None`` means no
                agency filter.
            target_date: The date for which to return scheduled departures.

        Returns:
            List of ``ScheduledDeparture`` named tuples sorted ascending by
            ``departure_time`` in ``HH:MM`` format.  Returns an empty list
            when ``available`` is ``False``.
        """
        if not self._available:
            return []

        active_services = _active_service_ids(
            self._calendar, self._calendar_dates, target_date
        )
        if not active_services:
            return []

        if route_id is None:
            candidates = [
                candidate
                for key, candidates_for_key in self._departure_index.items()
                if key[0] == stop_id
                for candidate in candidates_for_key
            ]
        else:
            candidates = self._departure_index.get((stop_id, route_id), [])
        if not candidates:
            return []

        direction_str: str | None = (
            str(direction_id) if direction_id is not None else None
        )

        results: list[ScheduledDeparture] = []
        for (
            trip_id,
            departure_time_raw,
            dep_direction_id,
            agency_id,
            route_short_name,
        ) in candidates:
            service_id = self._trip_service_ids.get(trip_id)
            if service_id is None or service_id not in active_services:
                continue
            if direction_str is not None and dep_direction_id != direction_str:
                continue
            if operator_id is not None and agency_id != operator_id:
                continue
            time_hhmm = _normalise_time(departure_time_raw)
            results.append(ScheduledDeparture(trip_id, time_hhmm, route_short_name))

        results.sort(key=lambda t: t.departure_time)
        return results


def _parse_zip(
    fileobj: IO[bytes],
    stop_filter: frozenset[str] | None,
) -> tuple[
    dict[str, str],
    list[dict[str, str]],
    list[dict[str, str]],
    dict[tuple[str, str], list[tuple[str, str, str, str | None, str]]],
]:
    """Extract a GTFS zip from a seekable file and build schedule lookups.

    Reads ``routes.txt``, ``trips.txt``, ``stop_times.txt``, ``calendar.txt``,
    and optionally ``calendar_dates.txt`` from the zip, streaming each file
    row-by-row so no CSV is ever fully materialised in memory.  Only trips
    referenced by the departure index are retained in the returned service-ID
    lookup.

    Args:
        fileobj: Seekable binary file object containing the GTFS zip archive.
        stop_filter: When not ``None``, only ``stop_times.txt`` rows whose
            ``stop_id`` is in this set are indexed.

    Returns:
        A four-tuple of
        ``(trip_service_ids, calendar, calendar_dates, departure_index)``.

    Raises:
        StaticGtfsLoadError: If a required file is missing from the zip or a
            CSV cannot be parsed.
    """
    with zipfile.ZipFile(fileobj) as zf:
        names = set(zf.namelist())

        # route_id -> (route_short_name, agency_id or None); parse-time only.
        routes_by_id: dict[str, tuple[str, str | None]] = {}
        for row in iter_csv(zf, "routes.txt"):
            route_id = row.get("route_id")
            if route_id is None:
                continue
            agency_id_raw = row.get("agency_id", "")
            routes_by_id[route_id] = (
                row.get("route_short_name", ""),
                agency_id_raw if agency_id_raw else None,
            )

        # Trips pre-joined against routes so the routes dict can be dropped
        # and stop_times needs a single lookup per row.
        trips_by_id: dict[str, _TripInfo] = {}
        for row in iter_csv(zf, "trips.txt"):
            trip_id = row.get("trip_id")
            if trip_id is None:
                continue
            route_id = row.get("route_id", "")
            route_info = routes_by_id.get(route_id)
            if route_info is None:
                continue
            trips_by_id[trip_id] = _TripInfo(
                route_id=route_id,
                route_short_name=route_info[0],
                direction_id=row.get("direction_id", ""),
                service_id=row.get("service_id", ""),
                agency_id=route_info[1],
            )
        del routes_by_id

        departure_index: dict[
            tuple[str, str], list[tuple[str, str, str, str | None, str]]
        ] = {}

        for st_row in iter_csv(zf, "stop_times.txt"):
            stop_id = st_row.get("stop_id", "")
            if stop_filter is not None and stop_id not in stop_filter:
                continue
            trip_id = st_row.get("trip_id", "")

            trip_info = trips_by_id.get(trip_id)
            if trip_info is None:
                continue

            departure_index.setdefault((stop_id, trip_info.route_id), []).append(
                (
                    trip_id,
                    st_row.get("departure_time", ""),
                    trip_info.direction_id,
                    trip_info.agency_id,
                    trip_info.route_short_name,
                )
            )

        # Retain service IDs only for trips that made it into the index.
        trip_service_ids: dict[str, str] = {
            trip_id: trips_by_id[trip_id].service_id
            for candidates in departure_index.values()
            for trip_id, _time, _direction, _agency, _route_name in candidates
        }
        del trips_by_id

        calendar_rows = list(iter_csv(zf, "calendar.txt"))
        calendar_dates_rows = (
            list(iter_csv(zf, "calendar_dates.txt"))
            if "calendar_dates.txt" in names
            else []
        )

    return (
        trip_service_ids,
        calendar_rows,
        calendar_dates_rows,
        departure_index,
    )


def _active_service_ids(
    calendar: list[dict[str, str]],
    calendar_dates: list[dict[str, str]],
    target_date: date,
) -> set[str]:
    """Return the set of service IDs running on ``target_date``.

    Applies the regular ``calendar`` schedule (weekday flags and date range)
    and ``calendar_dates`` exceptions (type 1 additions, type 2 removals).

    Args:
        calendar: Parsed rows from ``calendar.txt``.
        calendar_dates: Parsed rows from ``calendar_dates.txt`` (may be empty).
        target_date: The date for which to evaluate service validity.

    Returns:
        Set of GTFS ``service_id`` strings active on the given date.
    """
    date_str = target_date.strftime("%Y%m%d")
    weekday_col = _WEEKDAY_COLUMNS[target_date.weekday()]

    active: set[str] = {
        row["service_id"]
        for row in calendar
        if (
            "service_id" in row
            and "start_date" in row
            and "end_date" in row
            and weekday_col in row
            and row[weekday_col] == "1"
            and row["start_date"] <= date_str
            and row["end_date"] >= date_str
        )
    }

    for row in calendar_dates:
        if row.get("date") == date_str:
            sid = row.get("service_id", "")
            if row.get("exception_type") == "1":
                active.add(sid)
            elif row.get("exception_type") == "2":
                active.discard(sid)

    return active


def _normalise_time(raw: str) -> str:
    """Convert a GTFS departure time string to ``HH:MM`` format.

    GTFS times may exceed ``23:59:59`` for trips running after midnight
    (e.g. ``25:30:00``).  Hours are wrapped modulo 24.

    Args:
        raw: Raw GTFS time string, e.g. ``"08:15:00"`` or ``"25:30:00"``.

    Returns:
        Time string in ``HH:MM`` format with hours in ``[0, 23]``.
    """
    parts = raw.strip().split(":")
    try:
        hour = int(parts[0]) % 24
    except (ValueError, IndexError):
        return "00:00"
    minute = parts[1] if len(parts) > 1 else "00"
    return f"{hour:02d}:{minute}"
