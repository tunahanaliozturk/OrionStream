# OrionStream Roadmap

OrionStream is at **0.6.0**: an in-process Server-Sent Events hub for ASP.NET Core, with topic
fan-out, `Last-Event-ID` resume from a bounded per-topic replay buffer behind a pluggable
`IReplayStore` seam, a documented event-id allocation contract, an allocation-light wire writer, a
one-line endpoint mapping helper, typed and async-enumerable publish/consume sugar, per-topic metric
tags plus a publish/subscribe `ActivitySource`, and a configurable delivery and back-pressure surface
(full-buffer policy, slow-consumer disconnect, per-topic capacity overrides, and per-subscriber
filtering).

This is a list of ideas under consideration, not a schedule and not a set of promises. Items here
may ship, change shape, or be dropped. The goal is to be honest about what the library does today
(see [FEATURES.md](FEATURES.md)) and transparent about what is being thought about. Version
milestones below are intent, not commitments, and the dates are targets that will move if the work
does.

OrionStream is pre-1.0. The first priority is to keep the existing surface small, correct, and
spec-faithful. New ideas are weighed against that: anything that complicates the core hub or the
formatter has to earn its place.

---

## Guiding principles

- **Stay small.** The value of OrionStream is that the whole thing fits in your head: a hub, an
  event, a formatter, a writer. Features that blur that line need a strong reason.
- **HTTP stays at the edge.** The hub and formatter must remain testable without HTTP. Anything new
  should respect that seam.
- **Spec-faithful first.** Correctness against the HTML SSE specification beats convenience.
- **No mandatory dependencies.** The core should keep working with only the ASP.NET Core shared
  framework. Integrations, if any, belong in separate opt-in packages.

---

## Recently shipped

These have landed and are reflected in [FEATURES.md](FEATURES.md) and the
[changelog](../CHANGELOG.md).

- **`Last-Event-ID` resume (0.2.0).** Every published event carries a wire `id:`, either the
  producer-supplied `ServerSentEvent.Id` or a hub-assigned topic-monotonic sequence. The
  `ISseHub.Subscribe(string topic, string? lastEventId)` overload turns a returning `Last-Event-ID`
  into a resume that replays the retained backlog. The resume is gap-free as long as that backlog fits
  the subscriber's buffer; a backlog larger than the subscriber capacity can have its oldest replayed
  events evicted under the channel's `DropOldest` policy before the client reads them.
- **Bounded per-topic replay buffer (0.2.0).** `StreamOptions.ReplayBufferCapacity` (default 256, `0`
  disables) retains the newest events per topic so a reconnecting client resumes after its last seen
  id. Resume is all-or-nothing: an unknown or evicted id falls back to a from-now stream with no
  partial backlog. The buffer is in-process and bounded, so per-topic memory is capped.
- **Allocation-light wire path (0.2.1).** `SseFormatter.Format` pre-sizes its buffer and splits
  `data:` lines by scanning spans in place instead of running unconditional `Replace`/`Split` passes,
  and `WriteStreamAsync` encodes into a pooled `ArrayPool<byte>` buffer rather than a fresh `byte[]`
  per write. Wire output is byte-identical to before; measured per-event allocation dropped by
  roughly a quarter to just under a half depending on event shape.
- **Endpoint mapping helper (0.3.0).** `MapServerSentEvents` over `IEndpointRouteBuilder` wires an
  SSE endpoint to the hub in one line: it reads `Last-Event-ID`, subscribes (resuming when present),
  and streams the subscription with the correct headers, heartbeats, and disconnect cancellation.
  Overloads take a fixed topic or a per-request topic selector.
- **Typed publish (0.3.0).** `ISseHub.Publish<T>` serializes a payload to the `data:` field with
  `System.Text.Json` (web defaults or a supplied `JsonSerializerOptions`), alongside the unchanged
  raw string publish.
- **Async-enumerable sugar (0.3.0).** `StreamSubscription.ReadAllAsync()` / `ReadAllAsync<T>()` expose
  a subscription as `IAsyncEnumerable<T>` for `await foreach`, and `ISseHub.PublishAllAsync<T>` drains
  an async stream into a topic.
- **Per-topic metric tags and a tracing `ActivitySource` (0.3.0).** `orionstream.published` and
  `orionstream.dropped` now carry an `orionstream.topic` tag, and `StreamDiagnostics` exposes an
  `ActivitySource` named `Moongazing.OrionStream` with `OrionStream.Publish` and
  `OrionStream.Subscribe` spans tagged with the topic.
- **Configurable full-buffer policy (0.4.0).** `StreamOptions.FullBufferPolicy` chooses what happens
  when a subscriber buffer is full: `DropOldest` (default, unchanged), `DropNewest`, or a bounded
  `Wait` capped by `StreamOptions.MaxPublishWait`. `Wait` is the one policy that can apply
  back-pressure to the publisher, so it requires the explicit cap; the drop policies keep the
  never-blocks guarantee.
- **Slow-consumer disconnect (0.4.0).** The opt-in `StreamOptions.SlowConsumerPolicy` disconnects a
  subscriber whose buffer stays full on `MaxConsecutiveFullPublishes` publishes in a row, shedding a
  wedged client instead of feeding it a permanently lossy stream. A publish that finds room resets the
  run; disabled by default.
- **Per-topic capacity overrides (0.4.0).** `StreamOptions.ConfigureTopic` raises or lowers the
  subscriber and/or replay buffer for one topic without changing the global default for the rest.
- **Per-subscriber filtering (0.4.0).** `ISseHub.Subscribe(topic, lastEventId, filter)` takes an
  optional predicate evaluated before the event enters the subscriber's buffer, so a chatty topic does
  not fill a client's buffer with events it would discard. The filter also applies to replayed backlog
  on resume.
- **Event-id allocation contract (0.5.0).** The hub's id allocation is now a stated contract on
  `ISseHub` (XML docs and [FEATURES.md](FEATURES.md)), not an implementation detail: a per-topic,
  strictly-increasing, gap-free hub sequence starting at 1; monotonicity scoped per topic; a
  producer-supplied id always winning on the wire while the sequence is still assigned underneath; an
  ascending-sequence ordering guarantee; and what a consumer may and may not assume when producer ids
  and hub sequences mix on one topic. This is the prerequisite for any durable or cross-instance resume
  store, since those have to agree on what an id means.
- **Pluggable replay store seam (0.5.0).** The per-topic replay buffer is abstracted behind
  `IReplayStore` (with `ReplayEntry` and an `IReplayStoreFactory`), so the in-memory ring is one
  implementation and a caller can swap in an external store without the hub knowing where the backlog
  lives. `InMemoryReplayStore` stays the default and the only one with no dependencies; resume reads
  through the seam, and behavior is identical with the default store.
- **Durable Redis backplane replay store (0.6.0).** The opt-in `OrionStream.Redis` package plugs a
  Redis-backed `IReplayStore` into that seam, over `StackExchange.Redis`. With it registered, a client
  can resume by `Last-Event-ID` after a load balancer reconnects it to a *different* hub instance, and
  the backlog survives a process restart, because the backlog lives in Redis instead of an in-process
  ring. It is scoped strictly to the resume backlog: an event published on instance A is still
  delivered only to A's live subscribers (it is not a cross-instance publish bus). One capped Redis list
  per topic holds entries ordered by the hub's gap-free sequence, bounded to `ReplayBufferCapacity` by
  drop-oldest, honoring the same ordering and duplicate-WireId contract the in-memory store documents.
  The core stays in-process fan-out with no mandatory dependency; the package is additive and the
  in-memory ring remains the default.

---

## Ideas under consideration

These are possibilities, ordered loosely by how aligned they are with the library's purpose. None is
committed, and the version tags are targets.

### Resume and multi-instance (Redis backplane shipped, ~Q1 2027)

The event-id allocation contract and the pluggable `IReplayStore` seam shipped in 0.5.0, and the
durable Redis backplane store that plugs into that seam shipped in 0.6.0 as the `OrionStream.Redis`
package (see *Recently shipped*). A client can now resume by `Last-Event-ID` across instances and
across a restart when that package is registered.

- **A Postgres-backed durable replay store (opt-in package, possible).** A relational alternative to
  the Redis store behind the same `IReplayStore` seam, for deployments that already run Postgres and
  would rather not add Redis. Same scope as the Redis store: the resume backlog only, not a
  cross-instance publish bus. Not committed; weighed against whether the Redis package already covers
  the durable-resume need for most users.
- **Cross-instance live fan-out (open question, not committed).** Whether to deliver an event published
  on instance A to instance B's live subscribers at all, now that a shared store abstraction exists.
  This is a deliberate non-goal today (see below); the store backplane is scoped to resume backlog
  only. Listed here only to record the question, not as planned work.

### Hardening (targeting 0.5.0, ~Q1 2027)

- **Configurable heartbeat and keep-alive.** The heartbeat interval is already configurable; the
  remaining work is letting a caller customize the keep-alive further (for example a different
  heartbeat comment payload, or an initial `retry:` emitted once at stream open to set the client's
  reconnect delay without a per-event field).
- **Configurable response headers.** Letting callers add or override headers (for example CORS, or a
  custom `retry` default) on the SSE response without hand-writing the writer.
- **Backpressure-aware write timeouts.** A guard so a wedged client connection cannot hold a writer
  loop open indefinitely beyond cancellation.

### Observability (targeting 0.5.0, ~Q1 2027)

- **Resume and replay metrics.** Counters for resume attempts split by outcome (exact resume versus
  from-now fallback) and for replayed events, so operators can see how often clients reconnect with a
  usable `Last-Event-ID` and size `ReplayBufferCapacity` from data rather than guesswork.

---

## Explicit non-goals

- **Becoming a message broker.** OrionStream is in-process fan-out for SSE. For durable, cross-process
  messaging use a real broker. A backplane replay store, if built, is scoped to `Last-Event-ID`
  resume backlog, not to live cross-instance publishing.
- **Guaranteed delivery.** The DropOldest model is intentional. Clients that must not miss events
  should use `Id` plus the replay buffer (or a future durable store) rather than expecting the
  per-subscriber buffer to grow without bound.
- **A client library.** The whole point is that a browser `EventSource` needs no client. OrionStream
  will not ship a bespoke JavaScript client.
- **Transport beyond SSE.** WebSockets and long-polling are out of scope; that is what SignalR is for.

---

## Shaping this list

If one of these ideas matters to you, or you have a use case the current surface does not cover,
open an issue describing the real scenario. Concrete use cases are what move an idea from this list
into actual work.
