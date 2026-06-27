<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes to OrionStream are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-06-27

### Added

Core ergonomics and observability. All additive; the existing hub, resume, replay, and wire path are
unchanged.

- `MapServerSentEvents` endpoint helper (`Moongazing.OrionStream.AspNetCore`): a minimal-API
  extension over `IEndpointRouteBuilder` that wires an SSE endpoint to the hub in one line. It reads
  the `Last-Event-ID` request header, subscribes (resuming when the header is present), and streams
  the subscription to the response with the correct SSE headers, heartbeats, and cancellation on
  client disconnect. Overloads take a fixed topic or a per-request topic selector (for example off a
  route value); an empty selected topic is a `400`.
- Typed publish: `ISseHub.Publish<T>(topic, payload, ...)` serializes a payload to the `data:` field
  with `System.Text.Json` (web defaults, or a supplied `JsonSerializerOptions`), with optional
  `event:`, `id:`, and `retry:`. The raw string publish is unchanged and remains the primitive.
- Async-enumerable sugar: `StreamSubscription.ReadAllAsync()` and `ReadAllAsync<T>()` expose a
  subscription as an `IAsyncEnumerable<T>` for `await foreach`, and `ISseHub.PublishAllAsync<T>` drains
  an `IAsyncEnumerable<T>` into a topic. Each completes on cancellation.
- `StreamOptions.SerializerOptions`: the default `JsonSerializerOptions` (web defaults) for the typed
  publish helpers.

### Observability

- The `orionstream.published` and `orionstream.dropped` counters now carry an `orionstream.topic` tag
  (`StreamDiagnostics.TopicTagName`) so they can be sliced per topic. Drops are recorded once per
  publish with the topic, rather than one untagged add per evicting subscriber.
- `StreamDiagnostics` exposes an `ActivitySource` named `Moongazing.OrionStream` carrying an
  `OrionStream.Publish` (producer) span and an `OrionStream.Subscribe` (consumer) span, each tagged
  with the topic; the publish span also tags the delivered subscriber count. Mirrors the existing
  meter for distributed-tracing consumers.

### Tests

20 new tests: the endpoint helper serves an SSE stream with the right headers and resumes from
`Last-Event-ID`; typed publish round-trips through the serializer and honors a supplied options
instance; the async-enumerable helpers yield in order and complete on cancellation; and the per-topic
metric tags and publish/subscribe activities are asserted via a `MeterListener` and an
`ActivityListener`.

## [0.2.1] - 2026-06-20

### Performance

Reduced per-event allocation on the SSE serialization hot path, with byte-identical wire output.

- `SseFormatter.Format` no longer runs unconditional `Replace`/`Split` passes over the payload, id,
  and event name. It pre-sizes the `StringBuilder`, splits `data:` lines by scanning spans in place,
  and strips newlines from the `id:`/`event:` fields only when one is present. Measured allocation
  drop: about 41% for a single-line data-only event, 26% for a full-metadata event, and 44% for a
  multi-line payload.
- `SseResponseExtensions.WriteStreamAsync` encodes each event into a pooled `ArrayPool<byte>` buffer
  instead of allocating a fresh `byte[]` per write.

## [0.2.0] - 2026-06-19

### Added

SSE resume via `Last-Event-ID`.

- `SseHub` now stamps every published event with a topic-monotonic `id:` (the SSE id field) when
  the producer did not supply one, mutating the event in place so the same instance is still
  broadcast. A producer-supplied id is never overwritten.
- A bounded per-topic in-memory replay buffer keeps the newest events so a reconnecting client can
  resume without gaps.
- `ISseHub.Subscribe(string topic, string? lastEventId)`: resumes after a client-supplied
  `Last-Event-ID`, replaying only the events after it that the buffer still holds. An unknown,
  unparsable, or evicted id falls back to a from-now stream (replaying whatever remains).
- `StreamOptions.ReplayBufferCapacity` (default 256, 0 disables replay): bounds the per-topic
  replay buffer; validated on registration.

### Tests

17 new tests covering incrementing ids, producer-id precedence on the wire, known/latest/unknown/
evicted resume, the from-now fallback, replay buffer capacity bounds, disabled replay, the
subscriber-buffer interaction, and `ReplayBufferCapacity` validation.

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

[0.3.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.3.0
[0.2.1]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.2.1
[0.2.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.2.0
[0.1.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.1.0
