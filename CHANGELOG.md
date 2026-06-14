<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes to OrionStream are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-15

### Added

Initial release. Server-Sent Events for ASP.NET Core.

- `ISseHub` / `SseHub`: topic-based broadcast with a bounded per-subscriber buffer (drop-oldest),
  so a publish never blocks on a slow client.
- `SseFormatter`: spec-correct `text/event-stream` rendering (id/event/retry fields, multi-line
  data split, heartbeat comment).
- `ServerSentEvent`: the data payload plus optional id, event name, and retry.
- `WriteStreamAsync` response extension: drains a subscription to the client with heartbeats,
  returning when the client disconnects.
- `StreamOptions`: subscriber capacity and heartbeat interval; validated on registration.
- `StreamDiagnostics`: `Moongazing.OrionStream` meter with published, dropped, and subscribers
  instruments.
- `AddOrionStream()` DI extension.

### Tests

16 tests across the formatter, the hub (fan-out, topic isolation, unsubscribe, drop-oldest,
double-dispose), the response writer, and registration.

[0.1.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.1.0
