# python-nta-gtfs

Async Python client for the Irish National Transport Authority (NTA) GTFS feeds.

## What it does

`python-nta-gtfs` provides two async clients for working with NTA transit data. `GtfsRtClient` fetches and parses real-time trip updates from the NTA GTFS-RT protobuf feed, returning typed dataclass objects for each trip and its stop-level delay information. `StaticGtfsClient` downloads the static GTFS schedule zip, parses it entirely in memory, and exposes synchronous queries to look up scheduled departures for a given stop, route, date, and direction.

Both clients accept a caller-supplied `aiohttp.ClientSession` and raise library-specific exceptions on all error conditions. The library is under active development.

## Installation

```
pip install python-nta-gtfs
```

## Prerequisites

**NTA API key** — required by the GTFS-RT feed. Register at [developer.nationaltransport.ie](https://developer.nationaltransport.ie/) to obtain one.

**Static GTFS feed URL** — the URL of the NTA static GTFS zip archive. This is also available via the NTA developer portal once you have an account.

## Usage

Both clients take a caller-supplied `aiohttp.ClientSession` — the library never creates its own. The essentials:

```python
client = GtfsRtClient(feed_url=FEED_URL, api_key=API_KEY, session=session)
updates = await client.async_fetch_trip_updates()  # -> list[TripUpdate]

client = StaticGtfsClient(static_gtfs_url=STATIC_URL, session=session)
await client.async_load()  # first use; async_refresh_if_stale() on later runs
departures = client.get_scheduled_departures(
    stop_id="8250DB001234", route_id="46A", direction_id=0,
    operator_id=None, target_date=date.today(),
)  # -> list[ScheduledDeparture]
```

`async_fetch_trip_updates` returns typed dataclasses: each `TripUpdate` carries a list of `StopTimeUpdate` objects with per-stop delay and absolute time information. `async_load` downloads and parses the GTFS zip, offloading the CPU-intensive work to a thread so the event loop is not blocked; `async_refresh_if_stale` only re-downloads when the data is older than `refresh_hours` (default 24).

For complete, runnable programs see the [`examples/`](examples/) directory.

## Running the examples

The scripts in `examples/` query the live NTA API. To run them locally:

```bash
git clone https://github.com/nocalla/python-nta-gtfs.git
cd python-nta-gtfs
uv sync

# API key from https://developer.nationaltransport.ie/
export NTA_API_KEY=your-key

# Real-time trip updates (prints a summary of the first few trips)
uv run python examples/fetch_trip_updates.py

# Scheduled departures from the static GTFS feed
# (downloads a large zip on each run — expect a wait)
uv run python examples/scheduled_departures.py 8250DB001234 46A --direction 0
uv run python examples/scheduled_departures.py --help
```

Environment variable overrides: `NTA_FEED_URL` (GTFS-RT feed URL) and `NTA_STATIC_GTFS_URL` (static zip URL). The static example needs no API key — the zip is publicly downloadable.

## Error handling

All exceptions inherit from `NtaGtfsError`, so a single `except NtaGtfsError:` catches every library error; catch the specific subclasses when you need to distinguish auth failures (`GtfsRtAuthError`, HTTP 401) from other fetch or parse problems.

Exception hierarchy:

```
NtaGtfsError
├── GtfsRtAuthError      — HTTP 401 from the GTFS-RT feed
├── GtfsRtFetchError     — other HTTP or network error from the GTFS-RT feed
├── GtfsRtParseError     — response is not a valid GTFS-RT protobuf FeedMessage
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

## Data licence, attribution and fair usage

The GTFS data served by these feeds is provided by the
[National Transport Authority (NTA)](https://www.nationaltransport.ie/) under the
[Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/)
licence, subject to the
[NTA GTFS fair usage policy](https://developer.nationaltransport.ie/usagepolicy).
This library's MIT licence covers the code only, not the data.

If you use this library in a public-facing application, presentation, or
publication, the policy requires you to:

- credit the NTA as the data provider,
- link to the GTFS data source or the [NTA website](https://www.nationaltransport.ie/), and
- state that the GTFS data is provided "as is" and that the NTA is not
  responsible for any errors or inaccuracies in it.

The policy also limits each API token to **one GTFS-RT request every 60
seconds**. This library does not throttle requests itself — the polling
cadence is yours to control — so make sure your application calls
`async_fetch_trip_updates` no more than once per 60 seconds per token. The
static GTFS zip changes infrequently; the default `refresh_hours=24` is well
within fair usage, and you should avoid re-downloading it more often than
needed.

## Requirements

- Python 3.12 or later
- [aiohttp](https://docs.aiohttp.org/) 3.9 or later
- [gtfs-realtime-bindings](https://pypi.org/project/gtfs-realtime-bindings/) 1.0 or later
