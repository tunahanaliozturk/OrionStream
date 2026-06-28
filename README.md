<p align="center">
  <img src="docs/logo.png" alt="OrionStream" width="150" />
</p>

# OrionStream

[![CI/CD](https://github.com/tunahanaliozturk/OrionStream/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/tunahanaliozturk/OrionStream/actions/workflows/ci-cd.yml)
[![NuGet](https://img.shields.io/nuget/v/OrionStream.svg)](https://www.nuget.org/packages/OrionStream/)

Server-Sent Events for ASP.NET Core. Publish an event to a topic and every connected client
subscribed to that topic receives it, over a plain `text/event-stream` response that any browser
`EventSource` can read with no extra client library.

Part of the **Orion** family. Usable entirely on its own.

---

## Why

SSE is the simplest way to push server events to a browser: one long-lived HTTP response, a tiny
wire format, automatic client reconnect. The fiddly parts are the wire format (multi-line data, the
field order, heartbeats so proxies do not close an idle stream) and fan-out without letting a slow
client stall the others. OrionStream handles both: a topic hub with a bounded buffer per subscriber
and a spec-correct writer.

The library has three independent pieces. The hub (`ISseHub`) and the formatter (`SseFormatter`)
have no dependency on HTTP, so both are unit-tested directly. The writer extension
(`WriteStreamAsync`) is the only piece that touches `HttpResponse`, and it adapts a subscription to
the response body.

---

## Features

- **Topic-based broadcast hub.** Publish once to a topic; every current subscriber receives the
  event. Topics are created on first subscribe and removed when their last subscriber leaves, so an
  idle topic costs nothing.
- **Bounded per-subscriber buffering.** Each subscriber gets its own bounded channel. By default a
  publish never blocks: a slow subscriber drops its own oldest event to admit the newest, and a slow
  client degrades only its own stream.
- **Configurable delivery and back-pressure.** `StreamOptions.FullBufferPolicy` chooses drop-oldest
  (default), drop-newest, or a bounded `Wait` that applies back-pressure to the publisher up to
  `MaxPublishWait`. An opt-in `SlowConsumerPolicy` disconnects a wedged subscriber that stays
  saturated past a threshold. `ConfigureTopic` overrides the subscriber and replay capacities for one
  busy topic, and `Subscribe(topic, lastEventId, filter)` delivers only events matching a predicate
  evaluated before they enter the buffer.
- **Spec-correct wire format.** `SseFormatter` renders the `text/event-stream` fields in canonical
  order (`id`, `event`, `retry`, `data`), splits multi-line payloads across multiple `data:` lines,
  and strips stray newlines from single-line fields.
- **Heartbeats.** The writer sends an SSE comment line on an idle stream so proxies and load
  balancers keep the connection open.
- **Last-Event-ID resume.** Every published event carries a wire `id:`, either the producer-supplied
  `ServerSentEvent.Id` or a hub-assigned topic-monotonic sequence. A bounded per-topic replay buffer
  (`StreamOptions.ReplayBufferCapacity`, default 256, set to `0` to disable) retains the most recent
  events, so a client that reconnects with `Subscribe(topic, lastEventId)` resumes after its
  `Last-Event-ID` with no gap. An unknown or evicted id falls back to a from-now stream.
- **Built-in metrics.** A `System.Diagnostics.Metrics` meter named `Moongazing.OrionStream` exposes
  published and dropped counters and a current-subscribers gauge.
- **Multi-targeted.** `net8.0`, `net9.0`, `net10.0`, nullable enabled, warnings as errors.

See [docs/FEATURES.md](docs/FEATURES.md) for the full surface and [docs/ROADMAP.md](docs/ROADMAP.md)
for where this is going.

---

## Install

```bash
dotnet add package OrionStream
```

The package id is `OrionStream`; the root namespace is `Moongazing.OrionStream`. It carries a
`FrameworkReference` to `Microsoft.AspNetCore.App`, so add it to a project that targets the ASP.NET
Core shared framework.

---

## Quick start

Register the hub, its options, and diagnostics (all singletons):

```csharp
builder.Services.AddOrionStream(o =>
{
    o.SubscriberCapacity = 256;
    o.HeartbeatInterval = TimeSpan.FromSeconds(15);
});
```

Map an endpoint that streams a topic to the client. The `MapServerSentEvents` helper wires the
subscribe-then-write pattern (including reading `Last-Event-ID` to resume) in one line:

```csharp
app.MapServerSentEvents("/events/orders", "orders");
// or derive the topic per request from a route value:
app.MapServerSentEvents("/events/{topic}", ctx => (string?)ctx.Request.RouteValues["topic"]);
```

Publish from anywhere that has the hub. The typed overload serializes the payload for you:

```csharp
hub.Publish("orders", order, eventName: "order.created", id: order.Id.ToString());

// or build the event yourself with the raw string publish:
hub.Publish("orders", new ServerSentEvent
{
    Id = order.Id.ToString(),
    EventName = "order.created",
    Data = JsonSerializer.Serialize(order),
});
```

The browser side needs no client library:

```js
const es = new EventSource("/events/orders");
es.addEventListener("order.created", e => console.log(JSON.parse(e.data)));
```

---

## Usage

### Topics and the hub

`ISseHub` is the entire producer/consumer surface:

```csharp
public interface ISseHub
{
    StreamSubscription Subscribe(string topic);
    StreamSubscription Subscribe(string topic, string? lastEventId);
    int Publish(string topic, ServerSentEvent evt);
    int SubscriberCount(string topic);
}
```

- `Subscribe(topic)` returns a `StreamSubscription`. Read events off `subscription.Reader` (a
  `ChannelReader<ServerSentEvent>`) and dispose the subscription to unsubscribe. Disposal is
  idempotent.
- `Publish(topic, evt)` delivers to every current subscriber of the topic and returns the number of
  subscribers it reached. Publishing to a topic with no subscribers returns `0` and does nothing
  else.
- `SubscriberCount(topic)` is the current count for a topic, handy for diagnostics or for skipping
  serialization when nobody is listening.

Topic matching is ordinal (case-sensitive). A topic is tracked lazily on first subscribe and removed
once its last subscriber disposes, so the topic map does not grow with idle topics.

### Back-pressure and DropOldest

Each subscriber gets a bounded channel sized to `SubscriberCapacity`, created with
`BoundedChannelFullMode.DropOldest`. The consequence is that `Publish` is non-blocking by
construction: a slow reader can never stall the producer or the other subscribers. When a
subscriber's buffer is already full at publish time, its oldest buffered event is evicted to make
room for the newest, and that eviction is counted in telemetry (`orionstream.dropped`).

```csharp
// With SubscriberCapacity = 2 and a reader that has not drained:
hub.Publish("orders", new ServerSentEvent { Data = "1" });
hub.Publish("orders", new ServerSentEvent { Data = "2" });
hub.Publish("orders", new ServerSentEvent { Data = "3" }); // evicts "1"; reader now sees "2", "3"
```

This is a deliberate trade-off: OrionStream favors keeping every stream live and current over
guaranteeing delivery of every event to a client that cannot keep up. Pick `SubscriberCapacity`
large enough to ride out normal bursts; for a client that drops the connection and must recover the
events it missed while away, use the built-in `Last-Event-ID` resume below.

### Tuning delivery and back-pressure

The drop-oldest default keeps the never-blocks guarantee, but the policy is configurable when a
caller wants different trade-offs:

```csharp
builder.Services.AddOrionStream(o =>
{
    // Slow a producer instead of losing events, capped so a wedged reader cannot stall it forever.
    o.FullBufferPolicy = FullBufferPolicy.Wait;
    o.MaxPublishWait = TimeSpan.FromMilliseconds(200);

    // Shed a subscriber that stays saturated rather than feeding it a permanently lossy stream.
    o.SlowConsumerPolicy = new SlowConsumerPolicy { MaxConsecutiveFullPublishes = 128 };

    // Give one busy topic a larger buffer without raising the global default.
    o.ConfigureTopic("ticks", t => { t.SubscriberCapacity = 4096; t.ReplayBufferCapacity = 1024; });
});

// Deliver only the events this subscriber cares about; the filter runs before the buffer, so the
// rest never take a slot.
using var sub = hub.Subscribe("ticks", lastEventId: null, filter: e => e.Data.StartsWith("EURUSD"));
```

`FullBufferPolicy.DropNewest` keeps the buffered events and discards the incoming one instead of the
oldest; `FullBufferPolicy.Wait` is the only policy that applies back-pressure to `Publish`, which is
why it requires `MaxPublishWait`. See [docs/FEATURES.md](docs/FEATURES.md) section 6 for the details.

### Last-Event-ID resume

A browser `EventSource` remembers the `id:` of the last event it received and sends it back as the
`Last-Event-ID` request header when it reconnects. The `Subscribe(topic, lastEventId)` overload turns
that header into a gap-free resume.

For this to work every event needs a wire id. The hub assigns one automatically: when a producer does
not set `ServerSentEvent.Id`, the hub stamps a topic-monotonic sequence as the `id:` on the wire. A
producer-supplied `Id` always takes precedence and round-trips through resume unchanged, so you can
resume against your own ids (an order id, a database row version) or let the hub number events for
you.

The hub retains the newest `StreamOptions.ReplayBufferCapacity` events per topic (default 256). On
reconnect it matches the client's `Last-Event-ID` against the wire id of each retained event:

- A match replays only the events published after that id, then live events flow. The client misses
  nothing, provided `StreamOptions.SubscriberCapacity` covers the replay burst: replayed events share
  the subscriber's bounded `DropOldest` channel, so if the backlog to replay exceeds
  `SubscriberCapacity` the oldest replayed entries are dropped. When gap-free resume matters, size
  `SubscriberCapacity` at least as large as `ReplayBufferCapacity` (plus live headroom).
- An unknown or evicted id (older than the buffer still holds, or one the buffer never saw) falls
  back to a from-now stream with no replay. Resume is all-or-nothing: a client either resumes exactly
  or starts clean, never on a partial backlog.

Read the header from the request and pass it straight to `Subscribe`:

```csharp
app.MapGet("/events/orders", async (HttpContext ctx, ISseHub hub, StreamOptions options) =>
{
    var lastEventId = ctx.Request.Headers["Last-Event-ID"].FirstOrDefault();
    using var subscription = hub.Subscribe("orders", lastEventId);
    await ctx.Response.WriteStreamAsync(subscription, options.HeartbeatInterval, ctx.RequestAborted);
});
```

Set `ReplayBufferCapacity` to `0` to disable replay entirely; every subscribe then starts from now.
Sizing the buffer is the usual trade-off: it bounds how long a client can be disconnected and still
resume without a gap, against the memory held per active topic.

How the wire id is chosen is a stated contract on `ISseHub`, not an implementation detail: the hub
sequence is per topic, strictly increasing by one with no gaps starting at 1; a producer id always
wins on the wire while the sequence is still assigned underneath; delivery and retention are in
ascending-sequence (publish) order; and when producer ids and hub sequences mix on one topic each
round-trips through resume by exact wire-id match, but wire ids are not globally ordered, numeric, or
comparable across the two kinds. See [docs/FEATURES.md](docs/FEATURES.md) for the full contract.

The per-topic backlog lives behind the `IReplayStore` seam, so the in-memory ring is one
implementation and you can swap in an external store without the hub knowing where the backlog lives:
register your own `IReplayStoreFactory` before `AddOrionStream`. `InMemoryReplayStore` is the default
and the only one with no dependencies. A durable, cross-instance store (resume after reconnecting to a
different instance, backlog surviving a restart) is still planned and will ship as a separate opt-in
package behind this same seam.

### The formatter

`SseFormatter` is a pure, allocation-light renderer you can use independently of the hub or the HTTP
writer, for example in tests:

```csharp
var wire = SseFormatter.Format(new ServerSentEvent
{
    Id = "42",
    EventName = "tick",
    RetryMilliseconds = 3000,
    Data = "payload",
});
// "id: 42\nevent: tick\nretry: 3000\ndata: payload\n\n"
```

It follows the HTML SSE spec: fields render in canonical order, only the fields you set are emitted,
multi-line `Data` becomes multiple `data:` lines (`\r\n`, `\r`, and `\n` are all normalized), and
stray newlines in `Id`/`EventName` are stripped so they cannot break the framing. A heartbeat is the
constant `SseFormatter.Heartbeat` (`": heartbeat\n\n"`), a comment line that carries no event.

### Subscriber lifecycle and the writer

`WriteStreamAsync` is the bridge from a subscription to an `HttpResponse`. It sets the SSE response
headers (`Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`),
then loops: it drains the subscription's reader to the response body, and whenever the stream is idle
for `heartbeatInterval` it writes a heartbeat comment so intermediaries keep the connection open. It
returns when the client disconnects (the cancellation token trips, typically `ctx.RequestAborted`)
or when the subscription completes. A client disconnect is the expected exit and is handled silently.

The canonical pattern is to scope the subscription to the request with `using`, so that when
`WriteStreamAsync` returns the subscription is disposed and the subscriber is removed from the hub:

```csharp
app.MapGet("/events/{topic}", async (string topic, HttpContext ctx, ISseHub hub, StreamOptions options) =>
{
    using var subscription = hub.Subscribe(topic);
    await ctx.Response.WriteStreamAsync(subscription, options.HeartbeatInterval, ctx.RequestAborted);
});
```

---

## Configuration

`AddOrionStream` takes an optional `Action<StreamOptions>`. Options are validated eagerly at
registration time, so an invalid value throws from `AddOrionStream`, not later at first use.

```csharp
public sealed class StreamOptions
{
    public int SubscriberCapacity { get; set; } = 256;
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);
    public int ReplayBufferCapacity { get; set; } = 256;
}
```

| Option | Default | Meaning |
| --- | --- | --- |
| `SubscriberCapacity` | `256` | Bounded buffer size per subscriber. Must be at least `1`. When a subscriber falls this far behind, its oldest buffered event is dropped to admit the newest. |
| `HeartbeatInterval` | `15s` | How long a stream may be idle before the writer sends a heartbeat comment. Must be positive. |
| `ReplayBufferCapacity` | `256` | How many of the most recent events per topic are retained for `Last-Event-ID` resume. Must be zero or greater; `0` disables replay so every subscribe starts from now. |

`AddOrionStream` registers four singletons via `TryAdd`, so you can override any of them before or
after the call: `StreamOptions`, `StreamDiagnostics`, `IReplayStoreFactory` (default
`InMemoryReplayStoreFactory`), and `ISseHub` (implemented by `SseHub`). Registering a custom
`IReplayStoreFactory` first is how you swap the resume backlog store without touching the hub.

---

## Telemetry

`StreamDiagnostics` owns a `System.Diagnostics.Metrics.Meter` named `Moongazing.OrionStream`
(also exposed as `StreamDiagnostics.MeterName`). Subscribe to it from OpenTelemetry or any
`MeterListener`:

| Instrument | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `orionstream.published` | Counter | `{event}` | Events published to the hub, counted once per publish (not per subscriber). Tagged with `orionstream.topic`. |
| `orionstream.dropped` | Counter | `{event}` | Events dropped because a subscriber buffer was full at publish time. Tagged with `orionstream.topic`. |
| `orionstream.subscribers` | Observable gauge | `{subscriber}` | Currently connected subscribers across all topics. |

The `orionstream.topic` tag (`StreamDiagnostics.TopicTagName`) slices the published and dropped
counters per topic. `StreamDiagnostics` also exposes an `ActivitySource` named `Moongazing.OrionStream`
with an `OrionStream.Publish` span and an `OrionStream.Subscribe` span, each tagged with the topic.

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter(StreamDiagnostics.MeterName))
    .WithTracing(t => t.AddSource(StreamDiagnostics.MeterName));
```

A steadily climbing `orionstream.dropped` is the signal that subscribers cannot keep up: raise
`SubscriberCapacity`, reduce publish volume, or shrink per-event payloads.

---

## Testing

The hub and the formatter are plain in-memory types with no HTTP dependency, so they test directly.
The writer is verified against a real `DefaultHttpContext` with a capturing response body.

```csharp
using var diagnostics = new StreamDiagnostics();
var hub = new SseHub(new StreamOptions { SubscriberCapacity = 2 }, diagnostics);
using var subscription = hub.Subscribe("orders");

var delivered = hub.Publish("orders", new ServerSentEvent { Data = "hello" });

Assert.Equal(1, delivered);
Assert.True(subscription.Reader.TryRead(out var evt));
Assert.Equal("hello", evt!.Data);
```

`SseFormatter.Format` is a pure function over a `ServerSentEvent`, which makes wire-format assertions
exact and trivial:

```csharp
Assert.Equal("data: hello\n\n", SseFormatter.Format(new ServerSentEvent { Data = "hello" }));
```

---

## Benchmarks

In-memory micro-benchmarks for the formatter and the hub (publish fan-out, throughput with and
without the DropOldest path, and subscribe/dispose churn) live in
`benchmarks/Moongazing.OrionStream.Benchmarks` and reference the library directly. See
[benchmarks.md](benchmarks.md) for what each one measures. No result numbers are committed because
they are machine-specific; run the suite locally:

```bash
dotnet run -c Release --project benchmarks/Moongazing.OrionStream.Benchmarks -- --filter "*"
```

---

## Design

- Multi-targets `net8.0`, `net9.0`, `net10.0`.
- `TreatWarningsAsErrors`, latest analyzers, nullable enabled, XML docs generated.
- The hub and the SSE formatter are independent of HTTP, so both are unit-tested directly; the
  writer extension adapts them to an `HttpResponse`.

---

## Versioning

OrionStream is at **0.2.0**. While it is pre-1.0 the public API may still change between minor
versions; once it reaches 1.0 it will follow [SemVer 2.0.0](https://semver.org/). See
[CHANGELOG.md](CHANGELOG.md) for the per-release history.

---

## Contributing

Issues and pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md) before opening one.

## License

MIT. See [LICENSE](LICENSE).

## Author

**Tunahan Ali Ozturk** - [GitHub](https://github.com/tunahanaliozturk)
</content>
</invoke>
