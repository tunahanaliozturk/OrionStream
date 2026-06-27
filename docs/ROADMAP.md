# OrionStream Roadmap

OrionStream is at **0.3.0**: an in-process Server-Sent Events hub for ASP.NET Core, with topic
fan-out, `Last-Event-ID` resume from a bounded per-topic replay buffer, an allocation-light wire
writer, a one-line endpoint mapping helper, typed and async-enumerable publish/consume sugar, and
per-topic metric tags plus a publish/subscribe `ActivitySource`.

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

---

## Ideas under consideration

These are possibilities, ordered loosely by how aligned they are with the library's purpose. None is
committed, and the version tags are targets.

### Delivery and back-pressure (targeting 0.4.0, ~Q4 2026)

- **A configurable full-buffer policy.** Today the per-subscriber buffer is always `DropOldest`. An
  option to choose drop-newest or a bounded wait could suit callers who would rather slow a producer
  than lose events. It conflicts with the never-blocks guarantee, so any wait policy would need an
  explicit cap and clear documentation that it can apply back-pressure to the publisher.
- **Slow-consumer policy beyond drop counting.** Today a subscriber that cannot keep up silently
  drops its oldest events and increments `orionstream.dropped`. An opt-in policy to disconnect a
  subscriber that stays saturated past a threshold would let callers shed a wedged client instead of
  feeding it a permanently lossy stream.
- **Per-topic capacity overrides.** Allowing a busy topic to carry a larger subscriber buffer or a
  larger replay buffer than the global default, without raising it for every topic.
- **Per-subscriber filtering.** An optional predicate supplied at `Subscribe` time so a subscriber
  receives only the events on a topic that match it, evaluated before the event enters the
  subscriber's buffer. This keeps a chatty topic from filling a buffer with events a given client
  will discard, at the cost of running the predicate once per subscriber per publish.

### Resume and multi-instance (targeting 0.5.0, ~Q1 2027)

- **A documented event-id allocation contract.** The hub already assigns a topic-monotonic sequence
  when the producer sets no `Id`, and a producer id always wins on the wire. Promoting that from an
  implementation detail to a stated contract (monotonicity scope, ordering guarantees, what a
  consumer may assume when mixing producer ids and hub sequences on one topic) is a prerequisite for
  any durable or cross-instance resume store, since those have to agree on what an id means.
- **A pluggable replay store.** An abstraction over the per-topic replay buffer so the in-memory ring
  is one implementation behind an interface, letting a caller swap in an external store without the
  hub knowing where the backlog lives. The in-memory store stays the default and the only one with
  no dependencies.
- **A durable / backplane replay store (opt-in package).** A Redis- or Postgres-backed replay store
  behind that abstraction, so a client can resume by `Last-Event-ID` after reconnecting to a
  *different* instance behind a load balancer, and so a backlog survives a process restart. This
  would ship as a separate opt-in package, not in the core: the core hub stays in-process fan-out
  with no mandatory dependency. It is scoped specifically to the resume backlog. It does not turn the
  hub into a publish bus across instances; an event published on instance A is still delivered only
  to instance A's live subscribers (see non-goals). Whether cross-instance live fan-out is worth
  building at all is an open question to settle after the store abstraction exists.

### Hardening (targeting 0.4.0, ~Q4 2026)

- **Configurable heartbeat and keep-alive.** The heartbeat interval is already configurable; the
  remaining work is letting a caller customize the keep-alive further (for example a different
  heartbeat comment payload, or an initial `retry:` emitted once at stream open to set the client's
  reconnect delay without a per-event field).
- **Configurable response headers.** Letting callers add or override headers (for example CORS, or a
  custom `retry` default) on the SSE response without hand-writing the writer.
- **Backpressure-aware write timeouts.** A guard so a wedged client connection cannot hold a writer
  loop open indefinitely beyond cancellation.

### Observability (targeting 0.4.0, ~Q4 2026)

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
