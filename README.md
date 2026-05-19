# python-nta-gtfs

Async Python client for the Irish National Transport Authority (NTA) GTFS feeds.

## What it does

`python-nta-gtfs` provides two async clients for working with NTA transit data. `GtfsRtClient` fetches and parses real-time trip updates from the NTA GTFS-RT JSON feed, returning typed dataclass objects for each trip and its stop-level delay information. `StaticGtfsClient` downloads the static GTFS schedule zip, parses it entirely in memory, and exposes synchronous queries to look up scheduled departures for a given stop, route, date, and direction.

Both clients accept a caller-supplied `aiohttp.ClientSession` and raise library-specific exceptions on all error conditions. The library is under active development.

## Installation

```
pip install python-nta-gtfs
```

## Prerequisites

**NTA API key** — required by the GTFS-RT feed. Register at [developer.nationaltransport.ie](https://developer.nationaltransport.ie/) to obtain one.

**Static GTFS feed URL** — the URL of the NTA static GTFS zip archive. This is also available via the NTA developer portal once you have an account.

## Quickstart

### Real-time trip updates with `GtfsRtClient`

```python
import asyncio
import aiohttp
from nta_gtfs import GtfsRtClient

FEED_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
API_KEY = "your-api-key-here"

async def main():
    async with aiohttp.ClientSession() as session:
        client = GtfsRtClient(
            feed_url=FEED_URL,
            api_key=API_KEY,
            session=session,
        )
        updates = await client.async_fetch_trip_updates()

    for trip in updates:
        print(trip.trip_id, trip.route_id, trip.direction_id)
        for stu in trip.stop_time_updates:
            print(f"  stop {stu.stop_id}: arrival delay {stu.arrival_delay}s")

asyncio.run(main())
```

`async_fetch_trip_updates` returns a list of `TripUpdate` objects. Each `TripUpdate` carries a list of `StopTimeUpdate` objects with per-stop delay and absolute time information. Both are plain dataclasses with no hidden state.

### Static schedule data with `StaticGtfsClient`

```python
import asyncio
from datetime import date
import aiohttp
from nta_gtfs import StaticGtfsClient

STATIC_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_All.zip"

async def main():
    async with aiohttp.ClientSession() as session:
        client = StaticGtfsClient(
            static_gtfs_url=STATIC_URL,
            session=session,
            refresh_hours=24,
        )

        # Load on first use; call async_refresh_if_stale on subsequent runs
        await client.async_load()

        departures = client.get_scheduled_departures(
            stop_id="8250DB001234",
            route_id="46A",
            direction_id=0,
            operator_id=None,
            target_date=date.today(),
        )

    for dep in departures:
        print(dep.departure_time, dep.trip_id)

asyncio.run(main())
```

`async_load` downloads and parses the GTFS zip, offloading the CPU-intensive work to a thread so the event loop is not blocked. Call `async_refresh_if_stale` on subsequent requests — it only re-downloads the data when it is older than `refresh_hours` (default 24). The `available` property is `False` until the first successful load; `get_scheduled_departures` returns an empty list when `available` is `False`.

## Error handling

All exceptions inherit from `NtaGtfsError`.

```python
from nta_gtfs import (
    GtfsRtAuthError,
    GtfsRtFetchError,
    GtfsRtParseError,
    NtaGtfsError,
    StaticGtfsLoadError,
)

try:
    updates = await client.async_fetch_trip_updates()
except GtfsRtAuthError:
    # HTTP 401 — API key is invalid or missing
    ...
except GtfsRtFetchError:
    # Non-401 HTTP error or network failure
    ...
except GtfsRtParseError:
    # Response was not valid GTFS-RT JSON
    ...
except NtaGtfsError:
    # Catch-all for any other library error (e.g. StaticGtfsLoadError)
    ...
```

Exception hierarchy:

```
NtaGtfsError
├── GtfsRtAuthError      — HTTP 401 from the GTFS-RT feed
├── GtfsRtFetchError     — other HTTP or network error from the GTFS-RT feed
├── GtfsRtParseError     — response is not valid GTFS-RT JSON
└── StaticGtfsLoadError  — static GTFS zip download or parse failure
```

## API reference

### `GtfsRtClient(feed_url, api_key, session)`

| Method | Returns | Description |
|---|---|---|
| `async_fetch_trip_updates()` | `list[TripUpdate]` | Fetch and parse the GTFS-RT trip updates feed. |

### `TripUpdate`

Dataclass representing a real-time update for a single trip.

| Attribute | Type | Description |
|---|---|---|
| `trip_id` | `str` | GTFS trip identifier. |
| `route_id` | `str` | GTFS route identifier. |
| `direction_id` | `str \| None` | Direction (`"0"` or `"1"`); `None` if absent in feed. |
| `start_date` | `str \| None` | Service start date (YYYYMMDD); `None` if absent. |
| `stop_time_updates` | `list[StopTimeUpdate]` | Ordered list of per-stop updates. |

### `StopTimeUpdate`

Dataclass representing a single stop-level real-time update.

| Attribute | Type | Description |
|---|---|---|
| `stop_id` | `str` | GTFS stop identifier. |
| `arrival_delay` | `int \| None` | Arrival delay in seconds. |
| `departure_delay` | `int \| None` | Departure delay in seconds. |
| `arrival_time` | `int \| None` | Absolute arrival time as a POSIX timestamp. |
| `departure_time` | `int \| None` | Absolute departure time as a POSIX timestamp. |

### `StaticGtfsClient(static_gtfs_url, session, refresh_hours=24)`

| Method / Property | Returns | Description |
|---|---|---|
| `async_load()` | `None` | Download and parse the GTFS zip. |
| `async_refresh_if_stale()` | `None` | Reload only when data is absent or older than `refresh_hours`. |
| `get_scheduled_departures(stop_id, route_id, direction_id, operator_id, target_date)` | `list[ScheduledDeparture]` | Return departures sorted by time for the given stop, route, and date. |
| `available` | `bool` | `True` after at least one successful load. |
| `loaded_at` | `datetime \| None` | UTC datetime of the last successful load. |

### `ScheduledDeparture`

Named tuple representing a single scheduled departure.

| Attribute | Type | Description |
|---|---|---|
| `trip_id` | `str` | GTFS trip identifier. |
| `departure_time` | `str` | Scheduled departure time in `HH:MM` format. |
| `route_name` | `str \| None` | Route short name. |

## Requirements

- Python 3.12 or later
- [aiohttp](https://docs.aiohttp.org/) 3.9 or later
