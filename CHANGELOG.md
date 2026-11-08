## [Unreleased]

### Added
- Nexo gear control page with collapsible General Information, Send command and Inspect Init JSON sections, without a sidebar entry.
- Connection details, init resource statistics, command sending and saved command names on the control page.
- Latest init JSON snapshots with receipt times, search, copy and download, plus native diagnostics with common secrets redacted.
- Host and port fields in setup and reconfiguration; existing entries without a port continue using 8766 without reconfiguration.
- Standalone dimmers, LED groups, dimmer groups and output groups, adapted from PR #24 by @sebcie.
- Local Nexo weather observations without external forecast requests.
- Per-entry external command buttons, automatic discovery and a command-name options form.
- Regression tests for commands, entity IDs, callbacks, discovery and connection cleanup.

### Changed
- Use Karta MM as the default entry name and rename existing Name of the device placeholders while preserving custom names.
- Run Pylint CI with Python 3.14 and installed runtime dependencies.

### Fixed
- Keep open and close enabled for blinds whose module reports no position (`blind_target` 255) instead of showing them as fully open (#4, #28).
- Version frontend assets by content so updated control pages use a new JavaScript URL.
- Align cover, lock and climate action signatures with Home Assistant and resolve integration Pylint diagnostics.
- Preserve existing light and switch entity IDs while adding new resource types.
- Stop background connection workers on successful unload and failed startup; retry when initial resources time out.
- Run synchronous and asynchronous resource callbacks without awaiting non-awaitable results.
- Replaced the thread-based `websocket-client`/`rel` connection with an asynchronous `websockets` client (keepalive via protocol pings, reconnect loop, non-blocking sends); fixes broken pipe errors.
- Improved initialization and resource load handling to avoid stale or broken connection states during startup.
- Kept the receive loop alive when a single bridge message cannot be handled.
- Report failed resource commands to Home Assistant instead of dropping them silently.
- Refresh resource states from repeated initial data after a reconnect.
- Hardened WebSocket error handling and reconnect timing for more reliable operation in Home Assistant.

## [1.2.0] - 19.03.2024

### Added
- Websocket reconnection improvments
- Updated websocket-client version to 1.7.0
- Analog sensors temperature
- Blinds / Covers


## [1.1.0] - 26.11.2023

### Added
- Websocket reconnection
- Updated websocket-client version to 1.6.4
- Analog sensor integration
- Switch integration
- Fix for entities received after unsupported partition resource
- typos code quality ...

## [1.0.1] - 07.09.2023

### Added

- Binary Sensor integration


## [1.0.0] - 06.09.2023

### Added

- Lights integration.
