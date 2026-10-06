<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/logo.png">
    <img src="docs/icon.png" alt="OrionStream logo" width="150">
  </picture>
</p>

# OrionStream

[![CI/CD](https://github.com/tunahanaliozturk/OrionStream/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/tunahanaliozturk/OrionStream/actions/workflows/ci-cd.yml)
[![NuGet](https://img.shields.io/nuget/v/OrionStream.svg)](https://www.nuget.org/packages/OrionStream/)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
![.NET 8.0 | 9.0 | 10.0](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-purple.svg)

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

![OrionStream overview: your code publishes to the SseHub, which fans each event out to a bounded buffer per subscriber; the SSE endpoint (MapServerSentEvents / WriteStreamAsync) drains the buffer to the browser EventSource, which reconnects with Last-Event-ID; the hub retains a replay backlog per topic in the in-memory store or, with OrionStream.Redis, in Redis](docs/diagrams/overview.png)

---

## Packages

| Package | What it is |
| --- | --- |
| [`OrionStream`](https://www.nuget.org/packages/OrionStream/) | The hub, the formatter, the HTTP writer, the endpoint helper, the in-memory replay store and telemetry. |
| [`OrionStream.Redis`](https://www.nuget.org/packages/OrionStream.Redis/) | An opt-in Redis replay store for `Last-Event-ID` resume across instances and restarts. |

---

## Features

- **Topic-based broadcast hub.** Publish once to a topic; every current subscriber receives the
  event. A topic is created on first subscribe (or, with replay enabled, on first publish) and
  removed when its last subscriber leaves and it holds no replay backlog.
- **Bounded per-subscriber buffering.** Each subscriber gets its own bounded channel. By default a
  slow subscriber drops its own oldest event to admit the newest, so the publisher does not wait for
  it and a slow client degrades only its own stream.
- **Configurable delivery and back-pressure.** `StreamOptions.FullBufferPolicy` chooses drop-oldest
  (default), drop-newest, or a bounded `Wait` that applies back-pressure to the publisher for up to
  `MaxPublishWait` per full subscriber. An opt-in `SlowConsumerPolicy` disconnects a wedged
  subscriber that stays saturated past a threshold. `ConfigureTopic` overrides the subscriber and
  replay capacities for one busy topic, and `Subscribe(topic, lastEventId, filter)` delivers only
  events matching a predicate evaluated before they enter the buffer.
- **Spec-correct wire format.** `SseFormatter` renders the `text/event-stream` fields in canonical
  order (`id`, `event`, `retry`, `data`), splits multi-line payloads across multiple `data:` lines,
  and strips stray newlines from single-line fields.
- **Heartbeats.** The writer sends an SSE comment line on an idle stream so proxies and load
  balancers keep the connection open.
- **Last-Event-ID resume.** Every published event carries a wire `id:`, either the producer-supplied
  `ServerSentEvent.Id` or a hub-assigned topic-monotonic sequence. A bounded per-topic replay buffer
  (`StreamOptions.ReplayBufferCapacity`, default 256, set to `0` to disable) retains the most recent
  events, so a client that reconnects with `Subscribe(topic, lastEventId)` resumes after its
  `Last-Event-ID`. An unknown or evicted id falls back to a from-now stream.
- **Built-in telemetry.** A `System.Diagnostics.Metrics` meter and an `ActivitySource`, both named
  `Moongazing.OrionStream`, expose published and dropped counters, a current-subscribers gauge, and
  publish/subscribe spans.
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
Core shared framework (`Microsoft.NET.Sdk.Web`).

For a durable, cross-instance replay backlog, also add the Redis-backed store:

```bash
dotnet add package OrionStream.Redis
```

---

## Quick start

A complete `Program.cs` in an ASP.NET Core project (`Microsoft.NET.Sdk.Web`, implicit usings on).
`AddOrionStream` registers the hub, its options and diagnostics as singletons;
`MapServerSentEvents` maps a GET endpoint that subscribes to a topic (resuming from the
`Last-Event-ID` request header when present) and streams it; the typed `Publish<T>` serializes the
payload to the `data:` field.

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Streaming;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOrionStream(o =>
{
    o.SubscriberCapacity = 256;
    o.HeartbeatInterval = TimeSpan.FromSeconds(15);
});

var app = builder.Build();

// Stream one fixed topic...
app.MapServerSentEvents("/events/orders", "orders");
// ...or derive the topic per request from a route value.
app.MapServerSentEvents("/events/{topic}", ctx => (string?)ctx.Request.RouteValues["topic"]);

// Publish from anywhere that has the hub.
app.MapPost("/orders", (Order order, ISseHub hub) =>
{
    hub.Publish("orders", order, eventName: "order.created", id: order.Id.ToString());
    return Results.Accepted();
});

app.Run();

public sealed record Order(Guid Id, decimal Total);
```

`Publish<T>` serializes with reflection-based `System.Text.Json`, so it is annotated
`[RequiresUnreferencedCode]` / `[RequiresDynamicCode]`. Under trimming or NativeAOT, serialize
yourself and publish a raw `ServerSentEvent`:

```csharp
using Moongazing.OrionStream.Streaming;

public static class OrderEvents
{
    public static int PublishCreated(ISseHub hub, Guid orderId, string json) =>
        hub.Publish("orders", new ServerSentEvent
        {
            Id = orderId.ToString(),
            EventName = "order.created",
            Data = json, // for example JsonSerializer.Serialize(order, AppJsonContext.Default.Order)
        });
}
```

The browser side needs no client library:

```js
const es = new EventSource("/events/orders");
es.addEventListener("order.created", e => console.log(JSON.parse(e.data)));
```

---

## Usage

The snippets below are top-level `Program.cs` code in a `Microsoft.NET.Sdk.Web` project with
implicit usings on.

### Topics and the hub

`ISseHub` is the entire producer/consumer surface:

```csharp
public interface ISseHub
{
    StreamSubscription Subscribe(string topic);
    StreamSubscription Subscribe(string topic, string? lastEventId);
    StreamSubscription Subscribe(string topic, string? lastEventId, Func<ServerSentEvent, bool>? filter);
    int Publish(string topic, ServerSentEvent evt);
    int SubscriberCount(string topic);
}
```

- `Subscribe(topic)` returns a `StreamSubscription`. Read events off `subscription.Reader` (a
  `ChannelReader<ServerSentEvent>`) and dispose the subscription to unsubscribe. Disposal is
  idempotent.
- `Publish(topic, evt)` delivers to every current subscriber of the topic and returns the number of
  subscribers whose buffer accepted it (a subscriber whose filter rejects the event, or whose full
  buffer refuses it under `DropNewest` or a timed-out `Wait`, is not counted). Publishing to a topic
  with no subscribers returns `0`; with replay enabled (the default) the event is still sequenced and
  retained in the topic's replay buffer, so a client that reconnects later can resume from it.
- `SubscriberCount(topic)` is the current count for a topic, handy for diagnostics or for skipping
  serialization when nobody is listening.

Topic matching is ordinal (case-sensitive). A topic is tracked lazily and removed once its last
subscriber disposes, unless it still holds replay backlog that a reconnecting client could resume
from. With replay enabled, every topic that has received a publish keeps up to
`ReplayBufferCapacity` events in memory until the process ends, so prefer a bounded set of topic
names (or `ReplayBufferCapacity = 0` for per-user or per-request topics).

### Back-pressure and DropOldest

![OrionStream publish flow: Publish records orion.stream.published, assigns the topic sequence and appends to the replay store under the topic lock, then for each subscriber applies the filter and, when the buffer is full, the FullBufferPolicy (DropOldest evicts the oldest, DropNewest discards the incoming event, Wait blocks up to MaxPublishWait then drops), optionally disconnects a slow consumer, records orion.stream.dropped once and returns the delivered count](docs/diagrams/publish-flow.png)

Each subscriber gets a bounded channel sized to `SubscriberCapacity`. Under the default
`FullBufferPolicy.DropOldest`, `Publish` never waits for a reader: a slow reader cannot stall the
producer or the other subscribers. When a subscriber's buffer is already full at publish time, its
oldest buffered event is evicted to make room for the newest, and that eviction is counted in
telemetry (`orion.stream.dropped`).

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using var diagnostics = new StreamDiagnostics();
var hub = new SseHub(new StreamOptions { SubscriberCapacity = 2 }, diagnostics);
using var subscription = hub.Subscribe("orders");

hub.Publish("orders", new ServerSentEvent { Data = "1" });
hub.Publish("orders", new ServerSentEvent { Data = "2" });
hub.Publish("orders", new ServerSentEvent { Data = "3" }); // evicts "1"; the reader now sees "2", "3"
```

This is a deliberate trade-off: OrionStream favors keeping every stream live and current over
guaranteeing delivery of every event to a client that cannot keep up. Pick `SubscriberCapacity`
large enough to ride out normal bursts; for a client that drops the connection and must recover the
events it missed while away, use the built-in `Last-Event-ID` resume below.

### Tuning delivery and back-pressure

The drop-oldest default keeps the never-waits behaviour, but the policy is configurable when a
caller wants different trade-offs:

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Streaming;

var builder = WebApplication.CreateBuilder(args);

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

var app = builder.Build();

// Deliver only the events this subscriber cares about; the filter runs before the buffer, so the
// rest never take a slot.
app.MapGet("/events/ticks/eurusd", async (HttpContext ctx, ISseHub hub, StreamOptions options) =>
{
    var lastEventId = ctx.Request.Headers["Last-Event-ID"].FirstOrDefault();
    using var subscription = hub.Subscribe("ticks", lastEventId, filter: e => e.EventName == "EURUSD");
    await ctx.Response.WriteStreamAsync(subscription, options.HeartbeatInterval, ctx.RequestAborted);
});

app.Run();
```

`FullBufferPolicy.DropNewest` keeps the buffered events and discards the incoming one instead of the
oldest. `FullBufferPolicy.Wait` is the only policy that applies back-pressure to `Publish`, which is
why it requires `MaxPublishWait`. The wait happens inside `Publish`, under the topic's lock, once
per full subscriber in turn, so one publish can block for up to `MaxPublishWait` times the number of
saturated subscribers, and other publishes and subscribes on that topic wait behind it. A subscribe
waiting there holds the hub-wide registration lock, so it can in turn delay subscribes, unsubscribes
and replay-enabled publishes on other topics. See
[docs/FEATURES.md](docs/FEATURES.md) section 6 for the details.

### Last-Event-ID resume

A browser `EventSource` remembers the `id:` of the last event it received and sends it back as the
`Last-Event-ID` request header when it reconnects. The `Subscribe(topic, lastEventId)` overload turns
that header into a resume.

For this to work every event needs a wire id. The hub assigns one automatically: when a producer does
not set `ServerSentEvent.Id`, the hub stamps a topic-monotonic sequence as the `id:` on the wire. A
producer-supplied `Id` always takes precedence and round-trips through resume unchanged, so you can
resume against your own ids (an order id, a database row version) or let the hub number events for
you.

The hub retains the newest `StreamOptions.ReplayBufferCapacity` events per topic (default 256). On
reconnect it matches the client's `Last-Event-ID` against the wire id of each retained event:

- A match replays only the events published after that id, then live events flow. The client misses
  nothing provided `SubscriberCapacity` covers the replay burst: replayed events go through the
  subscriber's bounded buffer under its `FullBufferPolicy`, so if the backlog to replay exceeds
  `SubscriberCapacity`, `DropOldest` loses the oldest replayed entries and `DropNewest` or `Wait`
  refuse the newest ones (resume never waits). These replay-time losses are not counted in
  `orion.stream.dropped`. When gap-free resume matters, size `SubscriberCapacity` at least as large
  as `ReplayBufferCapacity` (plus live headroom).
- An unknown or evicted id (older than the buffer still holds, or one the buffer never saw) falls
  back to a from-now stream with no replay. The lookup is all-or-nothing: a client either resumes
  after an exact match or starts clean, never from a partial backlog.

`MapServerSentEvents` reads the header for you. By hand, read it from the request and pass it to
`Subscribe`:

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Streaming;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOrionStream();
var app = builder.Build();

app.MapGet("/events/orders", async (HttpContext ctx, ISseHub hub, StreamOptions options) =>
{
    var lastEventId = ctx.Request.Headers["Last-Event-ID"].FirstOrDefault();
    using var subscription = hub.Subscribe("orders", lastEventId);
    await ctx.Response.WriteStreamAsync(subscription, options.HeartbeatInterval, ctx.RequestAborted);
});

app.Run();
```

Set `ReplayBufferCapacity` to `0` to disable replay entirely; every subscribe then starts from now.
Sizing the buffer is the usual trade-off: it bounds how long a client can be disconnected and still
resume without a gap, against the memory held per active topic.

How the wire id is chosen is a stated contract on `ISseHub`, not an implementation detail: the hub
sequence is per topic, strictly increasing by one with no gaps starting at 1; a producer id always
wins on the wire while the sequence is still assigned underneath; delivery and retention are in
ascending-sequence (publish) order; and when producer ids and hub sequences mix on one topic each
round-trips through resume by exact wire-id match, but wire ids are not globally ordered, numeric, or
comparable across the two kinds. The sequence lives in the hub instance's memory: it restarts at 1
when the process restarts, and two instances number the same topic independently. See
[docs/FEATURES.md](docs/FEATURES.md) for the full contract.

The per-topic backlog lives behind the `IReplayStore` seam, so the in-memory ring is one
implementation and you can swap in an external store without the hub knowing where the backlog lives:
register your own `IReplayStoreFactory` before `AddOrionStream`. `InMemoryReplayStore` is the default
and the only one with no dependencies. For a durable, cross-instance store (resume after reconnecting
to a different instance, backlog surviving a restart), install the **`OrionStream.Redis`** package and
call `AddOrionStreamRedisReplayStore(...)`, which plugs a StackExchange.Redis-backed store in behind
this same seam:

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Redis;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOrionStream();
builder.Services.AddOrionStreamRedisReplayStore("localhost:6379", o => o.BacklogTimeToLive = TimeSpan.FromHours(1));
var app = builder.Build();
app.MapServerSentEvents("/events/orders", "orders");
app.Run();
```

Redis holds only the resume backlog: an event published on instance A still reaches only A's live
subscribers. Because hub sequences restart per process and per instance, set a unique
`ServerSentEvent.Id` on every event when more than one instance publishes to a topic or when the
backlog must survive a restart; otherwise two retained events can carry the same hub-assigned wire
id. With the Redis store, every `Publish` on a replay-enabled topic makes a synchronous Redis call,
and a Redis error is thrown from `Publish` before any subscriber receives the event.

### The formatter

`SseFormatter` is a pure, allocation-light renderer you can use independently of the hub or the HTTP
writer, for example in tests:

```csharp
using Moongazing.OrionStream.Streaming;

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

![OrionStream stream lifecycle: a GET request reaches MapServerSentEvents (empty topic answers 400), Subscribe resumes from Last-Event-ID or starts from now, WriteStreamAsync sets the SSE headers and loops writing events and heartbeats; client disconnect cancels the loop, a slow-consumer disconnect completes the subscription, and in both cases the using block disposes the subscription, which unsubscribes from the hub; the browser reconnects with Last-Event-ID](docs/diagrams/stream-lifecycle.png)

`WriteStreamAsync` is the bridge from a subscription to an `HttpResponse`. It sets the SSE response
headers (`Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`),
flushes, then loops: it drains the subscription's reader to the response body (flushing after each
event), and whenever no event arrives for `heartbeatInterval` it writes a heartbeat comment so
intermediaries keep the connection open. It returns when the client disconnects (the cancellation
token trips, typically `ctx.RequestAborted`) or when the subscription completes (for example when the
slow-consumer policy disconnects it). A client disconnect is the expected exit and is handled
silently.

The canonical pattern is to scope the subscription to the request with `using`, so that when
`WriteStreamAsync` returns the subscription is disposed and the subscriber is removed from the hub:

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Streaming;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOrionStream();
var app = builder.Build();

app.MapGet("/events/{topic}", async (string topic, HttpContext ctx, ISseHub hub, StreamOptions options) =>
{
    using var subscription = hub.Subscribe(topic);
    await ctx.Response.WriteStreamAsync(subscription, options.HeartbeatInterval, ctx.RequestAborted);
});

app.Run();
```

---

## Configuration

`AddOrionStream` takes an optional `Action<StreamOptions>`. Options are validated eagerly at
registration time, so an invalid value throws from `AddOrionStream`, not later at first use.

```csharp
public sealed class StreamOptions
{
    public int SubscriberCapacity { get; set; } = 256;
    public FullBufferPolicy FullBufferPolicy { get; set; } = FullBufferPolicy.DropOldest;
    public TimeSpan? MaxPublishWait { get; set; }
    public SlowConsumerPolicy? SlowConsumerPolicy { get; set; }
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);
    public int ReplayBufferCapacity { get; set; } = 256;
    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);
    public StreamOptions ConfigureTopic(string topic, Action<TopicCapacityOverride> configure);
}
```

| Option | Default | Meaning |
| --- | --- | --- |
| `SubscriberCapacity` | `256` | Bounded buffer size per subscriber. Must be at least `1`. When a subscriber falls this far behind, `FullBufferPolicy` decides what happens. |
| `FullBufferPolicy` | `DropOldest` | `DropOldest` evicts the oldest buffered event, `DropNewest` discards the incoming one, `Wait` blocks `Publish` for room up to `MaxPublishWait`. |
| `MaxPublishWait` | `null` | Required and positive when `FullBufferPolicy` is `Wait`; the wait cap per full subscriber. Ignored by the drop policies. |
| `SlowConsumerPolicy` | `null` | When set, a subscriber found full on `MaxConsecutiveFullPublishes` (default `64`, at least `1`) publishes in a row is disconnected. |
| `HeartbeatInterval` | `15s` | How long a stream may be idle before the writer sends a heartbeat comment. Must be positive. |
| `ReplayBufferCapacity` | `256` | How many of the most recent events per topic are retained for `Last-Event-ID` resume. Must be zero or greater; `0` disables replay so every subscribe starts from now. |
| `SerializerOptions` | web defaults | The `JsonSerializerOptions` the typed `Publish<T>` and `PublishAllAsync<T>` use when no per-call options are passed. |
| `ConfigureTopic(topic, ...)` | none | Overrides `SubscriberCapacity` (at least `1`) and/or `ReplayBufferCapacity` (zero or greater) for one topic. |

`AddOrionStream` registers four singletons via `TryAdd`: `StreamOptions`, `StreamDiagnostics`,
`IReplayStoreFactory` (default `InMemoryReplayStoreFactory`), and `ISseHub` (implemented by
`SseHub`). A registration of your own made before the call wins. Registering a custom
`IReplayStoreFactory` first is how you swap the resume backlog store without touching the hub.

---

## Telemetry

`StreamDiagnostics` owns a `System.Diagnostics.Metrics.Meter` and an `ActivitySource`, both named
`Moongazing.OrionStream` (also exposed as `StreamDiagnostics.MeterName`). Subscribe to them from
OpenTelemetry or any `MeterListener`:

| Instrument | Kind | Unit | Recorded by |
| --- | --- | --- | --- |
| `orion.stream.published` | Counter | `{event}` | `Publish`, once per call (not per subscriber), including calls that reach no subscriber. Tagged with `orion.stream.topic`. |
| `orion.stream.dropped` | Counter | `{event}` | `Publish`, once per call with the number of subscribers whose buffer was full: `DropOldest` evictions, `DropNewest` discards and timed-out `Wait`s. Entries refused while replaying a backlog on `Subscribe` are not counted. Tagged with `orion.stream.topic`. |
| `orion.stream.subscribers` | Observable gauge | `{subscriber}` | Currently connected subscribers across all topics: `Subscribe` adds one; disposing a subscription or a slow-consumer disconnect removes one. |

The `orion.stream.topic` tag (`StreamDiagnostics.TopicTagName`) slices the published and dropped
counters per topic. The `ActivitySource` emits an `OrionStream.Publish` span (tagged with the topic
and the delivered count as `orionstream.delivered`) and an `OrionStream.Subscribe` span tagged with
the topic.

With the `OpenTelemetry.Extensions.Hosting` package (not a dependency of OrionStream):

```csharp
using Moongazing.OrionStream.Diagnostics;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter(StreamDiagnostics.MeterName))
    .WithTracing(t => t.AddSource(StreamDiagnostics.MeterName));
```

A steadily climbing `orion.stream.dropped` is the signal that subscribers cannot keep up: raise
`SubscriberCapacity`, reduce publish volume, or shrink per-event payloads.

---

## Testing

The hub and the formatter are plain in-memory types with no HTTP dependency, so they test directly.
The writer is verified against a real `DefaultHttpContext` with a capturing response body. With
xUnit:

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;
using Xunit;

public class OrdersStreamTests
{
    [Fact]
    public void Publish_reaches_the_subscriber()
    {
        using var diagnostics = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 2 }, diagnostics);
        using var subscription = hub.Subscribe("orders");

        var delivered = hub.Publish("orders", new ServerSentEvent { Data = "hello" });

        Assert.Equal(1, delivered);
        Assert.True(subscription.Reader.TryRead(out var evt));
        Assert.Equal("hello", evt!.Data);
    }

    [Fact]
    public void Format_renders_a_data_only_event() =>
        Assert.Equal("data: hello\n\n", SseFormatter.Format(new ServerSentEvent { Data = "hello" }));
}
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
- `TreatWarningsAsErrors`, `latest-recommended` analyzers, nullable enabled, XML docs generated.
- The hub and the SSE formatter are independent of HTTP, so both are unit-tested directly; the
  writer extension adapts them to an `HttpResponse`.

---

## Versioning

OrionStream is at **0.7.0**. While it is pre-1.0 the public API may still change between minor
versions; once it reaches 1.0 it will follow [SemVer 2.0.0](https://semver.org/). See
[CHANGELOG.md](CHANGELOG.md) for the per-release history.

---

## Contributing

Issues and pull requests are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md) before opening one. Report security issues privately as
described in [SECURITY.md](SECURITY.md).

## More from the Orion family

Focused .NET libraries built to one quality bar. Each is usable on its own; several share the small [`Orion.Abstractions`](https://github.com/tunahanaliozturk/Orion.Abstractions) contracts spine, but there is no deep dependency web — pick only what you need:

- [OrionGuard](https://github.com/tunahanaliozturk/OrionGuard) — validation, guard clauses, DDD primitives, domain events
- [Orion.Abstractions](https://github.com/tunahanaliozturk/Orion.Abstractions) — the shared contracts spine: telemetry, options, result, clock
- [OrionAudit](https://github.com/tunahanaliozturk/OrionAudit) — automatic EF Core change-audit trail
- [OrionBeacon](https://github.com/tunahanaliozturk/OrionBeacon) — leader election with fencing tokens
- [OrionClock](https://github.com/tunahanaliozturk/OrionClock) — testable time, TTLs, and deadlines
- [OrionGrant](https://github.com/tunahanaliozturk/OrionGrant) — permission / authorization checks
- [OrionKey](https://github.com/tunahanaliozturk/OrionKey) — source-generated strongly-typed IDs
- [OrionLedger](https://github.com/tunahanaliozturk/OrionLedger) — API-key issuance, verification, and rotation
- [OrionLens](https://github.com/tunahanaliozturk/OrionLens) — ambient correlation-context propagation
- [OrionLock](https://github.com/tunahanaliozturk/OrionLock) — distributed locks with fencing tokens
- [OrionOnce](https://github.com/tunahanaliozturk/OrionOnce) — idempotency keys for exactly-once request handling
- [OrionPatch](https://github.com/tunahanaliozturk/OrionPatch) — transactional outbox for EF Core
- [OrionRelay](https://github.com/tunahanaliozturk/OrionRelay) — outbound webhook delivery (HMAC, retries, backoff)
- [OrionResult](https://github.com/tunahanaliozturk/OrionResult) — Result/Option types and a shared error vocabulary
- [OrionSaga](https://github.com/tunahanaliozturk/OrionSaga) — sagas / process managers for long-running workflows
- [OrionShade](https://github.com/tunahanaliozturk/OrionShade) — sensitive-data redaction for logs and telemetry
- [OrionVault](https://github.com/tunahanaliozturk/OrionVault) — field-level encryption for EF Core

See it all working together in [OrionShowcase](https://github.com/tunahanaliozturk/OrionShowcase), a production-shaped banking sample.

## License

MIT. See [LICENSE](LICENSE).

## Author

**Tunahan Ali Ozturk** - [GitHub](https://github.com/tunahanaliozturk)
