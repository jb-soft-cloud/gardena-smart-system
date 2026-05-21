# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- Webhook HMAC verification now logs at INFO level on the first observation of each scheme (plain SHA-256 vs HMAC-SHA256), then drops back to DEBUG for subsequent matches. This lets operators confirm which scheme Husqvarna actually emits without flooding the log. Preparation for switching the handler from soft-warn to hard-reject (`401`) once a single scheme is observed in stable operation.

## [3.0.2] - 2026-05-20

### Fixed

- **Webhook events silently dropped** — `_process_message` rejected Husqvarna's documented `WEBHOOK` envelope (`{data: {type: "WEBHOOK", attributes: {events: [...]}}}`) because the outer type was not in the allow-list. Events are now unpacked and dispatched one-by-one. Unwrapped single-document payloads remain supported.

### Added

- `gardena_smart_system.webhook_diagnostics` service — logs current webhook state (registered, callback URL, validUntil epoch, events received, last-event-at, hmac secret presence, renewal task alive).
- `gardena_smart_system.webhook_renew` service — forces a re-POST to `/v2/webhook` (refreshes validUntil and hmacSecret) without reloading the config entry.
- Internal counters `events_received` and `last_event_at` on the webhook client, surfaced via diagnostics.

## [3.0.1] - 2026-05-13

### Fixed

- Fix ImportError for `UnitOfIlluminance` on older HA versions (#356)

## [3.0.0] - 2026-05-13

### Breaking Changes

- **WebSocket Status entity migrated from `sensor` to `binary_sensor`** — The old `sensor.gardena_websocket_status` entity is removed. A new `binary_sensor.gardena_websocket_connected` entity with device class `connectivity` replaces it. Automations using the old entity must be updated.

### Added

- English translations for all mower error codes from the Gardena API v2
- German translations for all mower error codes and updated config/services translations (#347)

### Fixed

- Mower error code sensor showing "Unknown" when there is no error (#347)
- Light intensity sensor using invalid unit "lux" instead of "lx" for illuminance device class (#351)
- WebSocket disconnection no longer logs a warning on transient network interruptions (#352)

## [2.1.0] - 2026-05-11

### Added

- Support removal of orphaned devices via `async_remove_config_entry_device` (#346)

## [2.0.9] - 2026-05-11

### Fixed

- Mower error code sensor showing "Unknown" when there is no error (#347)
- Complete list of recognized mower error codes to match Gardena API v2

## [2.0.8] - 2026-05-11

### Removed

- "Measure Now" button and `sensor_measure` service — the `SENSOR_CONTROL` command type does not exist in the Gardena API v2 (#345)

## [2.0.7] - 2026-05-09

### Added

- Operating hours and RF link quality as dedicated sensor entities (#344)

## [2.0.6] - 2026-05-08

### Fixed

- Resolve HA device_id to Gardena UUID in services and fix API command format (#342, #343)

## [2.0.5] - 2026-05-07

### Fixed

- Retry on HTTP 504 gateway timeout from Gardena API (#309)
- Make WebSocket status sensor unique_id unique per config entry
- Handle HTTP 429 rate limiting with retry and clear error message

### Added

- Watering remaining time sensor for valves (#167)
- `state_class` to sensors for long-term statistics support

## [2.0.4] - 2026-05-05

### Fixed

- Use MOISTURE device class for soil humidity sensor (#339)

## [2.0.3] - 2026-05-04

### Fixed

- Add missing mower activity states

## [2.0.2] - 2026-05-01

### Fixed

- Initial stable release of v2 rewrite

## [2.0.1] - 2026-04-28

### Fixed

- Beta fixes and stabilization
