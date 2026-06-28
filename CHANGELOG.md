<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes to OrionStream are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-06-28

### Added

The durable backplane replay store that plugs into the 0.5.0 seam, shipped as a new opt-in package
`OrionStream.Redis` (PackageId `OrionStream.Redis`). All additive: the core hub is unchanged, the
in-memory ring stays the default, and nothing moves to Redis unless the new registration is called.

- `OrionStream.Redis`: a Redis-backed `IReplayStore` (`RedisReplayStore`) and its
  `RedisReplayStoreFactory`, over `StackExchange.Redis`. With it registered, a client can resume by
  `Last-Event-ID` after a load balancer reconnects it to a *different* hub instance, and the backlog
  survives a process restart, because the backlog lives in Redis rather than in the publishing process.
- Scope is exactly the resume backlog. It does not turn the hub into a cross-instance publish bus: an
  event published on instance A is still delivered only to A's live subscribers; Redis is read on resume
  to rebuild what a reconnecting client missed, not to fan out live events between instances.
- Redis structure: one capped list per topic, keyed `{KeyPrefix}{topic}` (default prefix
  `orionstream:replay:`), holding JSON-encoded entries oldest first. Each append is an `RPUSH` then an
  `LTRIM` that keeps the newest `capacity` entries, matching the `ReplayBufferCapacity`
  drop-oldest-beyond-capacity bound. Entries are ordered by the hub's gap-free per-topic sequence;
  resume matches the returning `Last-Event-ID` against the exact wire id each entry emitted, resolves a
  duplicate id to the oldest matching entry, and replays the ascending suffix after it: the same
  ordering and duplicate-WireId contract the in-memory store documents.
- Registration: `AddOrionStreamRedisReplayStore(connectionString, configure?)` (registers a shared
  `IConnectionMultiplexer` and swaps the factory) or `AddOrionStreamRedisReplayStore(configure?)` over
  an already-registered multiplexer. The Redis factory replaces the in-memory default definitively, so
  call order relative to `AddOrionStream` does not matter. `RedisReplayStoreOptions` tunes the key
  prefix, database index, and an optional sliding backlog TTL.

### Tests

14 new tests against a REAL Redis via Testcontainers (`Testcontainers.Redis`): cross-instance resume
(an append on one store instance, and a publish on one hub instance, replayed in sequence order by a
second instance pointed at the same Redis, including over a separate multiplexer); the bounded
drop-oldest capacity; the duplicate-WireId resolution to the oldest match; resume-by-`Last-Event-ID`
returning the right slice; the unknown-id from-now fallback; producer-supplied ids round-tripping
through cross-instance resume; and backlog survival across a new store instance modelling a restart.
The Redis test project is single-TFM (net10.0) and CI-validated under the build matrix.

## [0.5.0] - 2026-06-28

### Added

Resume groundwork: a stated event-id allocation contract and a pluggable replay store seam. All
additive; with nothing configured the hub, resume, replay, and the in-memory ring behave exactly as
before.

- Event-id allocation contract: the hub's id allocation is now a documented contract on `ISseHub`
  (XML docs and [FEATURES.md](docs/FEATURES.md)), not an implementation detail, so a replay store can
  rely on what an id means. The contract states the per-topic, strictly-increasing, gap-free hub
  sequence (starting at 1); that monotonicity scope is per topic and sequences are not comparable
  across topics; that a producer-supplied `Id` always wins on the wire while the sequence is still
  assigned underneath; the ascending-sequence ordering guarantee; and what a consumer may and may not
  assume when producer ids and hub sequences mix on one topic (each round-trips through resume by exact
  wire-id match, but wire ids are not globally ordered, numeric, or comparable across the two kinds).
- Pluggable replay store seam: the per-topic backlog is abstracted behind `IReplayStore` (with
  `ReplayEntry` and an `IReplayStoreFactory`), so the in-memory ring is one implementation and a caller
  can swap in an external store without the hub knowing where the backlog lives. `InMemoryReplayStore`
  (handed out by `InMemoryReplayStoreFactory`) stays the default and the only implementation with no
  dependencies; `AddOrionStream` registers it via `TryAdd`, so registering a custom factory first wins.
  The hub reads resume backlog through the seam, serializing every store call under the same per-topic
  lock it assigns sequences under, so a store sees a gap-free sequence and the in-memory store needs no
  internal locking. Behavior is identical when the default store is used; a topic with replay disabled
  gets no store at all.

### Deferred

- Durable / backplane replay store (Redis- or Postgres-backed, surviving a process restart and a
  different instance behind a load balancer) is still planned and explicitly out of this release. It
  will ship as a separate opt-in package behind the `IReplayStore` seam; the core stays in-process
  fan-out with no mandatory dependency.

### Tests

16 new tests: the event-id contract guarantees (per-topic gap-free sequence starting at 1; sequence
assigned even when a producer id is present; per-topic independence; producer id wins on the wire;
delivery order equals publish order; mixed producer-id and hub-sequence events each round-trip through
resume; a reused producer wire id resumes from the oldest match; the buffer retains in ascending
sequence order); and the replay store seam (the default hub uses the in-memory factory; resume through
the default replays identically; no store is created for a replay-disabled topic; a custom store is
appended to on publish; resume reads the backlog from a custom store and from-now falls back on an
empty result; a custom store's `HasBacklog` keeps an idle topic alive).

## [0.4.0] - 2026-06-27

### Added

Delivery and back-pressure. All additive; with nothing configured the hub, resume, replay, and the
drop-oldest default behave exactly as before.

- Configurable full-buffer policy: `StreamOptions.FullBufferPolicy` chooses what happens when a
  subscriber buffer is full at publish time. `DropOldest` (default, unchanged behavior) evicts the
  oldest event; `DropNewest` keeps the buffered events and discards the incoming one; `Wait` waits
  for room up to `StreamOptions.MaxPublishWait` before giving up on that subscriber. `Wait` is the
  only policy that can apply back-pressure to the publisher, so it requires an explicit
  `MaxPublishWait` cap (validated on registration) and is documented as such; `DropOldest` and
  `DropNewest` keep the never-blocks guarantee.
- Slow-consumer disconnect: an opt-in `StreamOptions.SlowConsumerPolicy` disconnects a subscriber
  whose buffer stays full on `MaxConsecutiveFullPublishes` publishes in a row, so a wedged client is
  shed (its channel completed, like a client disconnect) instead of fed a permanently lossy stream.
  A single publish that finds room resets the run, so a subscriber that briefly saturates and catches
  up is never disconnected. The threshold is counted in publishes, not wall-clock time, so it is
  independent of publish rate. Disabled by default.
- Per-topic capacity overrides: `StreamOptions.ConfigureTopic(topic, ...)` raises (or lowers) the
  subscriber buffer and/or replay buffer for one topic without changing the global default for every
  other topic.
- Per-subscriber filtering: `ISseHub.Subscribe(topic, lastEventId, filter)` takes an optional
  predicate evaluated once per publish for that subscriber, before the event enters the subscriber's
  buffer, so a chatty topic does not fill a client's buffer with events it would discard. The filter
  also applies to replayed events on resume. A null filter delivers every event, matching the
  existing overloads.

### Tests

26 new tests: drop-newest keeps the oldest and drops the newest under saturation (versus drop-oldest);
the bounded-wait policy applies back-pressure and proceeds the instant room appears, and gives up
after its cap when no room appears (both gated on observable state, not timing); a subscriber
saturated past the threshold is disconnected while one that catches up is not; per-topic subscriber
and replay overrides give a topic a larger buffer than the global default; a per-subscriber filter
delivers only matching events, the filtered-out events never enter the buffer or count as delivered,
and the filter applies to replayed backlog; plus validation of the new options.

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

[0.6.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.6.0
[0.5.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.5.0
[0.4.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.4.0
[0.3.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.3.0
[0.2.1]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.2.1
[0.2.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.2.0
[0.1.0]: https://github.com/tunahanaliozturk/OrionStream/releases/tag/v0.1.0
