"""Static GTFS schedule client for the nta_gtfs library."""

import asyncio
import csv
import io
import zipfile
from datetime import UTC, date, datetime, timedelta
from typing import NamedTuple

import aiohttp

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
        route_name: Route short name; matches the ``route_id`` parameter passed
            to ``get_scheduled_departures``.
    """

    trip_id: str
    departure_time: str
    route_name: str | None


class StaticGtfsClient:
    """Async client for downloading and querying static GTFS schedule data.

    Downloads a GTFS zip from a URL, parses it entirely in memory, and
    exposes synchronous queries against the parsed schedule.  The caller
    supplies an ``aiohttp.ClientSession``; this class never creates its own.
    """

    def __init__(
        self,
        static_gtfs_url: str,
        session: aiohttp.ClientSession,
        refresh_hours: int = 24,
    ) -> None:
        """Initialise the client.

        Args:
            static_gtfs_url: URL of the static GTFS zip to download.
            session: Caller-supplied aiohttp client session used for downloads.
            refresh_hours: Age threshold in hours after which
                ``async_refresh_if_stale`` triggers a reload.
        """
        self._url = static_gtfs_url
        self._session = session
        self._refresh_hours = refresh_hours
        self._available: bool = False
        self._loaded_at: datetime | None = None

        self._routes_by_id: dict[str, dict[str, str]] = {}
        self._trips_by_id: dict[str, dict[str, str]] = {}
        self._calendar: list[dict[str, str]] = []
        self._calendar_dates: list[dict[str, str]] = []
        self._departure_index: dict[
            tuple[str, str], list[tuple[str, str, str, str | None]]
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

        Downloads the zip from ``static_gtfs_url``, extracts it entirely in
        memory (no disk writes), and builds lookup structures for routes,
        trips, calendar, calendar_dates, and a pre-joined departure index.
        The CPU-intensive zip and CSV parsing is offloaded to a thread via
        ``asyncio.to_thread`` so the event loop is not blocked.

        On success ``available`` is set to ``True`` and ``loaded_at`` is set
        to ``datetime.now(UTC)``.

        Failure behaviour:
        - First-ever failure: ``available`` remains ``False``.
        - Refresh failure (client was already available): ``available`` stays
          ``True`` and the previously loaded data is preserved.

        Raises:
            StaticGtfsLoadError: On any download or parse failure.
        """
        try:
            async with self._session.get(self._url) as resp:
                if not resp.ok:
                    raise StaticGtfsLoadError(
                        f"Static GTFS download failed: HTTP {resp.status}"
                        f" from {self._url}"
                    )
                content = await resp.read()
        except aiohttp.ClientError as exc:
            raise StaticGtfsLoadError(
                f"Static GTFS download error for {self._url}: {exc}"
            ) from exc

        try:
            (
                routes_by_id,
                trips_by_id,
                calendar,
                calendar_dates,
                departure_index,
            ) = await asyncio.to_thread(_parse_zip, content)
        except StaticGtfsLoadError:
            raise
        except Exception as exc:
            raise StaticGtfsLoadError(f"Static GTFS parse error: {exc}") from exc

        self._routes_by_id = routes_by_id
        self._trips_by_id = trips_by_id
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
        route_id: str,
        direction_id: int | None,
        operator_id: str | None,
        target_date: date,
    ) -> list[ScheduledDeparture]:
        """Return scheduled departures for a stop/route on a given date.

        Looks up the pre-built departure index by ``(stop_id, route_short_name)``
        and filters by active service IDs, optional direction, and optional
        agency.

        Args:
            stop_id: GTFS stop ID to filter on.
            route_id: Route short name (e.g. ``"46A"``) matched against
                ``routes.route_short_name``.
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

        candidates = self._departure_index.get((stop_id, route_id), [])
        if not candidates:
            return []

        direction_str: str | None = (
            str(direction_id) if direction_id is not None else None
        )

        results: list[ScheduledDeparture] = []
        for trip_id, departure_time_raw, dep_direction_id, agency_id in candidates:
            trip_info = self._trips_by_id.get(trip_id)
            if trip_info is None:
                continue
            if trip_info["service_id"] not in active_services:
                continue
            if direction_str is not None and dep_direction_id != direction_str:
                continue
            if operator_id is not None and agency_id != operator_id:
                continue
            time_hhmm = _normalise_time(departure_time_raw)
            results.append(ScheduledDeparture(trip_id, time_hhmm, route_id))

        results.sort(key=lambda t: t.departure_time)
        return results


def _parse_zip(
    content: bytes,
) -> tuple[
    dict[str, dict[str, str]],
    dict[str, dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    dict[tuple[str, str], list[tuple[str, str, str, str | None]]],
]:
    """Extract a GTFS zip from raw bytes and build schedule lookup structures.

    Reads ``routes.txt``, ``trips.txt``, ``stop_times.txt``, ``calendar.txt``,
    and optionally ``calendar_dates.txt`` from the zip.  Builds and returns
    the five internal data structures used by ``StaticGtfsClient``.

    Args:
        content: Raw bytes of the GTFS zip archive.

    Returns:
        A five-tuple of
        ``(routes_by_id, trips_by_id, calendar, calendar_dates, departure_index)``.

    Raises:
        StaticGtfsLoadError: If a required file is missing from the zip or a
            CSV cannot be parsed.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = set(zf.namelist())

        def _read_csv(filename: str) -> list[dict[str, str]]:
            """Read a CSV file from the open zip into a list of row dicts.

            Args:
                filename: Name of the file inside the zip archive.

            Returns:
                List of row dicts with string values; BOM-stripped headers.
            """
            with zf.open(filename) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8-sig")
                return list(csv.DictReader(text))

        routes_rows = _read_csv("routes.txt")
        trips_rows = _read_csv("trips.txt")
        stop_times_rows = _read_csv("stop_times.txt")
        calendar_rows = _read_csv("calendar.txt")

        calendar_dates_rows = (
            _read_csv("calendar_dates.txt") if "calendar_dates.txt" in names else []
        )

        routes_by_id: dict[str, dict[str, str]] = {
            row["route_id"]: {
                "route_short_name": row.get("route_short_name", ""),
                "agency_id": row.get("agency_id", ""),
            }
            for row in routes_rows
            if "route_id" in row
        }

        trips_by_id: dict[str, dict[str, str]] = {
            row["trip_id"]: {
                "route_id": row.get("route_id", ""),
                "direction_id": row.get("direction_id", ""),
                "service_id": row.get("service_id", ""),
            }
            for row in trips_rows
            if "trip_id" in row
        }

        departure_index: dict[
            tuple[str, str], list[tuple[str, str, str, str | None]]
        ] = {}

        for st_row in stop_times_rows:
            stop_id = st_row.get("stop_id", "")
            trip_id = st_row.get("trip_id", "")
            departure_time_raw = st_row.get("departure_time", "")

            trip_info = trips_by_id.get(trip_id)
            if trip_info is None:
                continue

            route_id = trip_info["route_id"]
            direction_id = trip_info["direction_id"]

            route_info = routes_by_id.get(route_id)
            if route_info is None:
                continue

            route_short_name = route_info["route_short_name"]
            agency_id_raw = route_info["agency_id"]
            agency_id: str | None = agency_id_raw if agency_id_raw else None

            key = (stop_id, route_short_name)
            if key not in departure_index:
                departure_index[key] = []
            departure_index[key].append(
                (trip_id, departure_time_raw, direction_id, agency_id)
            )

    return (
        routes_by_id,
        trips_by_id,
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
