# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.0] - 2026-07-14

### Added

- `StaticGtfsClient.has_scheduled_pair(stop_id, route_id) -> bool`, for
  distinguishing a stop/route pair that will never appear in the static
  schedule from one that is correctly configured but has no departures due
  right now. Checks presence of the `(stop_id, route_id)` key in the
  departure index only — no calendar, direction, or operator filtering (#25)

## [0.2.1] - 2026-07-14

### Fixed

- `StaticGtfsClient.get_scheduled_departures`'s `route_id` parameter now
  matches against the real GTFS `route_id` instead of `route_short_name`.
  Previously, a caller passing the same value used to filter GTFS-RT
  `TripDescriptor.route_id` got silently wrong results whenever a route's
  `route_id` differed from its `route_short_name` — routes sharing a short
  name across different real `route_id`s (e.g. Cork "220" vs Dublin "220")
  could be conflated. `ScheduledDeparture.route_name` still returns the
  short name for display (#24)

## [0.2.0] - 2026-07-12

### Added

- `StaticGtfsClient` accepts an optional `stop_ids` collection; when given, only
  `stop_times.txt` rows for those stops are indexed, cutting peak memory roughly
  17x on a large feed (#20)

### Changed

- `StaticGtfsClient.async_load` now streams the zip download to an anonymous
  temporary file in 1 MiB chunks and parses each CSV row-by-row instead of
  holding the archive and all parsed rows in memory; peak RSS on a large feed
  drops ~2.3x even without a stop filter (#20)
- After parsing, only trips referenced by the departure index are retained

## [0.1.0] - 2026-07-11

### Added

- Initial release
- `GtfsRtClient` for fetching real-time GTFS-RT trip updates from NTA feeds
- `StaticGtfsClient` for downloading and querying static GTFS schedule data
- Exception hierarchy: `NtaGtfsError`, `GtfsRtAuthError`, `GtfsRtFetchError`, `GtfsRtParseError`, `StaticGtfsLoadError`
