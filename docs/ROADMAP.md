# OrionStream Roadmap

This is a list of ideas under consideration, not a schedule and not a set of promises. There are no
dates. Items here may ship, change shape, or be dropped. The goal is to be honest about what the
library does today (see [FEATURES.md](FEATURES.md)) and transparent about what is being thought
about.

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

## Ideas under consideration

These are possibilities, ordered loosely from "most aligned with the library's purpose" to "least
certain." None is committed.

### Ergonomics

- **An endpoint mapping helper.** A `MapServerSentEvents("/events/{topic}")` style extension that
  wraps the subscribe-then-write pattern, so the common case is one line instead of a handler body.
- **A typed publish overload.** A helper that serializes a payload to `Data` with a supplied
  `JsonSerializerOptions`, to remove the manual `JsonSerializer.Serialize` call at publish sites.
- **Async enumeration sugar.** A small extension over `StreamSubscription.Reader` for consumers that
  prefer `await foreach` over manual `TryRead` loops outside the HTTP writer.

### Delivery and back-pressure

- **A configurable full-buffer policy.** Today the buffer is always `DropOldest`. An option to choose
  drop-newest or a bounded wait could suit callers who would rather slow a producer than lose events,
  though it conflicts with the never-blocks guarantee and would need care.
- **Per-topic capacity overrides.** Allowing a busy topic to carry a larger buffer than the global
  default without raising it for every topic.
- **Optional replay buffer.** A small per-topic ring of recent events so a reconnecting client can be
  caught up from `Last-Event-ID` without a separate server-side store. This is a real feature with
  real memory cost, so it would be strictly opt-in.

### Observability

- **Tags on the metrics.** Emitting the topic (and possibly a drop reason) as a metric tag so
  published/dropped counts can be sliced per topic, balanced against cardinality risk.
- **An `ActivitySource` for tracing.** A span around publish/subscribe for distributed-tracing
  consumers, mirroring the existing meter.

### Hardening

- **Configurable response headers.** Letting callers add or override headers (for example CORS or a
  custom `retry` default) on the SSE response without hand-writing the writer.
- **Backpressure-aware write timeouts.** A guard so a wedged client connection cannot hold a writer
  loop open indefinitely beyond cancellation.

---

## Explicit non-goals

- **Becoming a message broker.** OrionStream is in-process fan-out for SSE. For durable, cross-process
  messaging use a real broker.
- **Guaranteed delivery.** The DropOldest model is intentional. Clients that must not miss events
  should use `Id` plus a replay source rather than expecting the hub to buffer indefinitely.
- **A client library.** The whole point is that a browser `EventSource` needs no client. OrionStream
  will not ship a bespoke JavaScript client.
- **Transport beyond SSE.** WebSockets and long-polling are out of scope; that is what SignalR is for.

---

## Shaping this list

If one of these ideas matters to you, or you have a use case the current surface does not cover,
open an issue describing the real scenario. Concrete use cases are what move an idea from this list
into actual work.
</content>
