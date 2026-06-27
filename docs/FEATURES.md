# OrionStream Features

A reference for everything in the current public surface (0.4.0). Every item here maps to a type or
member you can call today. For ideas not yet built, see [ROADMAP.md](ROADMAP.md).

---

## Table of contents

1. [The broadcast hub](#1-the-broadcast-hub)
2. [Subscriptions](#2-subscriptions)
3. [The event model](#3-the-event-model)
4. [The wire-format renderer](#4-the-wire-format-renderer)
5. [The HTTP writer](#5-the-http-writer)
6. [Delivery and back-pressure](#6-delivery-and-back-pressure)
7. [Heartbeats and reconnect](#7-heartbeats-and-reconnect)
8. [Options and validation](#8-options-and-validation)
9. [Telemetry](#9-telemetry)
10. [Dependency injection](#10-dependency-injection)
11. [Target frameworks](#11-target-frameworks)

---

## 1. The broadcast hub

`ISseHub` (implemented by `SseHub`) is the producer/consumer surface. It is a topic-based fan-out:
publish once to a topic and every current subscriber of that topic receives the event.

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

- `Subscribe(topic)` registers a subscriber and returns a `StreamSubscription`.
- `Subscribe(topic, lastEventId)` registers a subscriber that resumes after a client-supplied
  `Last-Event-ID`, replaying the retained backlog after that id (see resume below).
- `Subscribe(topic, lastEventId, filter)` adds an optional predicate evaluated before the event
  enters the subscriber's buffer, so the subscriber receives only matching events (see
  [section 6](#6-delivery-and-back-pressure)).
- `Publish(topic, evt)` delivers to every current subscriber and returns how many it reached.
  Publishing to a topic with no subscribers returns `0`.
- `SubscriberCount(topic)` returns the current subscriber count for a topic.

The extension `Publish<T>(topic, payload, ...)` (in `SseHubTypedExtensions`) serializes a payload to
the `data:` field with `System.Text.Json` so a publish site does not call `JsonSerializer.Serialize`
by hand. It accepts an optional `JsonSerializerOptions` (web defaults otherwise) and optional
`eventName`, `id`, and `retryMilliseconds`.

Topics are matched ordinally (case-sensitive). A topic is created lazily on first subscribe and
removed once its last subscriber leaves, so idle topics do not accumulate. `Subscribe` and `Publish`
throw on a null or empty `topic`; `Publish` throws on a null event.

The hub is thread-safe: topics and their subscribers are held in concurrent dictionaries, and each
subscriber's channel allows a single reader with multiple concurrent writers.

---

## 2. Subscriptions

`Subscribe` returns a `StreamSubscription`:

```csharp
public sealed class StreamSubscription : IDisposable
{
    public string Topic { get; }
    public ChannelReader<ServerSentEvent> Reader { get; }
    public void Dispose();
}
```

- `Reader` is the `ChannelReader<ServerSentEvent>` you drain to receive events, typically with
  `await foreach` or `TryRead`.
- `Topic` is the topic this subscription is attached to.
- `Dispose()` unsubscribes from the hub and releases the buffer. It is idempotent: disposing twice is
  safe and unsubscribes exactly once. The HTTP writer disposes implicitly via the `using` pattern
  when the client disconnects.

---

## 3. The event model

`ServerSentEvent` is an immutable record of the fields the SSE protocol defines. Only `Data` is
required; everything else is optional.

```csharp
public sealed class ServerSentEvent
{
    public required string Data { get; init; }
    public string? EventName { get; init; }
    public string? Id { get; init; }
    public int? RetryMilliseconds { get; init; }
}
```

| Member | SSE field | Meaning |
| --- | --- | --- |
| `Data` | `data:` | The payload. Multi-line content is emitted as multiple `data:` lines. |
| `EventName` | `event:` | The event name a browser `EventSource` dispatches on; null means the default `message` event. |
| `Id` | `id:` | The event id. When set, the browser returns it as `Last-Event-ID` on reconnect. |
| `RetryMilliseconds` | `retry:` | Reconnection delay hint in milliseconds; null leaves the client default. |

A heartbeat is not modelled as an event; it is a comment line produced by the writer.

---

## 4. The wire-format renderer

`SseFormatter` renders a `ServerSentEvent` to the `text/event-stream` format. It is static and pure,
so it can be used independently of the hub or HTTP.

```csharp
public static class SseFormatter
{
    public const string Heartbeat = ": heartbeat\n\n";
    public static string Format(ServerSentEvent evt);
}
```

Guarantees:

- Fields render in canonical order: `id`, `event`, `retry`, `data`.
- Only the fields you set are emitted.
- Multi-line `Data` becomes multiple `data:` lines; `\r\n`, `\r`, and `\n` are all normalized so a
  multi-line payload stays a single event.
- Newlines inside `Id` and `EventName` are stripped so they cannot break event framing.
- The output always ends with the blank-line terminator.

---

## 5. The HTTP writer

`SseResponseExtensions.WriteStreamAsync` adapts a subscription to an ASP.NET Core `HttpResponse`.

```csharp
public static Task WriteStreamAsync(
    this HttpResponse response,
    StreamSubscription subscription,
    TimeSpan heartbeatInterval,
    CancellationToken cancellationToken);
```

It sets the SSE response headers (`Content-Type: text/event-stream`, `Cache-Control: no-cache`,
`X-Accel-Buffering: no`), flushes, then loops draining the subscription's reader to the response body
and flushing after each write. When the stream is idle for `heartbeatInterval` it writes a heartbeat
comment. The call returns when `cancellationToken` trips (client disconnect, typically
`HttpContext.RequestAborted`) or the subscription completes. Cancellation is the expected exit and is
swallowed.

### The endpoint mapping helper

`SseEndpointRouteBuilderExtensions.MapServerSentEvents` wraps the subscribe-then-write pattern into a
single minimal-API mapping, so the common case is one line instead of a handler body.

```csharp
public static IEndpointConventionBuilder MapServerSentEvents(
    this IEndpointRouteBuilder endpoints, string pattern, string topic);

public static IEndpointConventionBuilder MapServerSentEvents(
    this IEndpointRouteBuilder endpoints, string pattern, Func<HttpContext, string?> topicSelector);
```

The mapped GET endpoint reads the `Last-Event-ID` request header, calls `Subscribe(topic, lastEventId)`
(so it resumes when the header is present), and streams the subscription via `WriteStreamAsync` with
the hub's configured `HeartbeatInterval` and `HttpContext.RequestAborted`. The first overload binds a
fixed topic; the second derives the topic per request, for example off a route value. A null or empty
selected topic responds `400 Bad Request`.

```csharp
app.MapServerSentEvents("/events/orders", "orders");
app.MapServerSentEvents("/events/{topic}", ctx => (string?)ctx.Request.RouteValues["topic"]);
```

### Async-enumerable sugar

For consumers outside the HTTP writer, `StreamSubscriptionAsyncEnumerableExtensions` exposes a
subscription as an async stream and drains an async stream into a topic.

```csharp
IAsyncEnumerable<ServerSentEvent> ReadAllAsync(this StreamSubscription s, CancellationToken ct = default);
IAsyncEnumerable<T?> ReadAllAsync<T>(this StreamSubscription s, JsonSerializerOptions? o = null, CancellationToken ct = default);
Task<long> PublishAllAsync<T>(this ISseHub hub, string topic, IAsyncEnumerable<T> source, ...);
```

`ReadAllAsync` is a thin view over `StreamSubscription.Reader`; the typed overload deserializes each
event's `data:` from JSON. `PublishAllAsync` publishes one event per source item. Each completes when
the source completes or cancellation trips.

---

## 6. Delivery and back-pressure

Every subscriber gets its own bounded channel of capacity `SubscriberCapacity` (or the per-topic
override, below). What happens when that buffer is full at publish time is set by
`StreamOptions.FullBufferPolicy`:

| Policy | Full-buffer behavior | Blocks publisher? |
| --- | --- | --- |
| `DropOldest` (default) | Evict the oldest buffered event to admit the newest. | No |
| `DropNewest` | Keep the buffered events; discard the incoming one. | No |
| `Wait` | Wait for room up to `MaxPublishWait`, then give up on that subscriber. | Yes, up to the cap |

- A slow subscriber under either drop policy affects only its own stream, never the producer or other
  subscribers, and the loss is recorded as `orionstream.dropped`.
- `Wait` is the only policy that applies back-pressure to the publisher. It requires an explicit
  `MaxPublishWait` cap (validated at registration) so a wedged reader cannot stall `Publish` forever:
  the call returns the instant room appears, or after the cap drops the event for that subscriber and
  proceeds. Choose it only when slowing the producer is preferable to losing events.

This trades guaranteed per-client delivery for keeping every stream live and current. Use
`ServerSentEvent.Id` with a server-side replay source if a client must recover missed events.

**Slow-consumer disconnect.** With `StreamOptions.SlowConsumerPolicy` set, a subscriber whose buffer
is full on `MaxConsecutiveFullPublishes` publishes in a row is disconnected (its channel completed,
like a client disconnect) rather than fed a permanently lossy stream. A single publish that finds room
resets the run, so a subscriber that briefly saturates and catches up is never disconnected. The
threshold is counted in publishes, not wall-clock time. Disabled by default.

**Per-topic capacity overrides.** `StreamOptions.ConfigureTopic(topic, o => ...)` raises or lowers the
`SubscriberCapacity` and/or `ReplayBufferCapacity` for one topic without changing the global default
for every other topic, so a busy topic can carry a larger buffer on its own.

**Per-subscriber filtering.** `ISseHub.Subscribe(topic, lastEventId, filter)` takes an optional
predicate. It runs once per publish for that subscriber, *before* the event enters the buffer, so a
chatty topic does not fill a client's buffer with events it would discard, and a filtered-out event
never counts as delivered or dropped for that subscriber. The filter also applies to replayed backlog
on resume. A null filter delivers every event. Keep the predicate cheap; it runs inside the publish
path and must not throw.

---

## 7. Heartbeats and reconnect

- **Heartbeats.** On an idle stream the writer emits `SseFormatter.Heartbeat`, an SSE comment that
  carries no event but keeps proxies and load balancers from closing the connection.
- **Reconnect.** Browser `EventSource` reconnects automatically. Set `ServerSentEvent.Id` and the
  browser sends it back as the `Last-Event-ID` request header on reconnect, which the server can read
  to resume from where the client left off. `RetryMilliseconds` lets the server suggest the
  reconnection delay.

---

## 8. Options and validation

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

- `SubscriberCapacity` must be at least `1`.
- `FullBufferPolicy` defaults to `DropOldest`; see [section 6](#6-delivery-and-back-pressure).
- `MaxPublishWait` is required and must be positive when `FullBufferPolicy` is `Wait`; it is the cap
  on how long a slow subscriber can stall a publish.
- `SlowConsumerPolicy.MaxConsecutiveFullPublishes` must be at least `1`. Null disables the policy.
- `HeartbeatInterval` must be positive.
- `ReplayBufferCapacity` must be zero or greater; `0` disables `Last-Event-ID` replay.
- `ConfigureTopic` overrides per topic: each override's `SubscriberCapacity` (if set) must be at least
  `1` and `ReplayBufferCapacity` (if set) zero or greater.
- `SerializerOptions` is the default serializer for the typed publish helpers (web defaults).

Options are validated eagerly when you call `AddOrionStream`, so a bad value throws at registration
rather than at first use.

---

## 9. Telemetry

`StreamDiagnostics` owns a `System.Diagnostics.Metrics.Meter` and an
`System.Diagnostics.ActivitySource`, both named `Moongazing.OrionStream` (`StreamDiagnostics.MeterName`).

| Instrument | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `orionstream.published` | Counter | `{event}` | Events published, counted once per publish. Tagged with `orionstream.topic`. |
| `orionstream.dropped` | Counter | `{event}` | Events dropped on a full subscriber buffer. Tagged with `orionstream.topic`. |
| `orionstream.subscribers` | Observable gauge | `{subscriber}` | Currently connected subscribers across all topics. |

The `orionstream.topic` tag (`StreamDiagnostics.TopicTagName`) lets the published and dropped counters
be sliced per topic. The `ActivitySource` carries an `OrionStream.Publish` (producer) span and an
`OrionStream.Subscribe` (consumer) span, each tagged with the topic; the publish span also tags the
delivered subscriber count.

`StreamDiagnostics` is `IDisposable`; disposing it disposes the meter and the activity source. Wire it
into OpenTelemetry with `AddMeter(StreamDiagnostics.MeterName)` and
`AddSource(StreamDiagnostics.MeterName)`.

---

## 10. Dependency injection

```csharp
public static IServiceCollection AddOrionStream(
    this IServiceCollection services,
    Action<StreamOptions>? configure = null);
```

`AddOrionStream` registers three singletons via `TryAdd`, so each can be overridden:

- `StreamOptions` (the configured instance)
- `StreamDiagnostics`
- `ISseHub` -> `SseHub`

Because registration uses `TryAdd`, registering your own `ISseHub`, `StreamOptions`, or
`StreamDiagnostics` before calling `AddOrionStream` wins.

---

## 11. Target frameworks

OrionStream multi-targets `net8.0`, `net9.0`, and `net10.0`. Nullable reference types are enabled,
analyzers run at `latest-recommended`, warnings are treated as errors, and an XML documentation file
is generated. The package carries a `FrameworkReference` to `Microsoft.AspNetCore.App`.
</content>
