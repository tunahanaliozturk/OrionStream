# OrionStream Features

A reference for everything in the current public surface (0.1.0). Every item here maps to a type or
member you can call today. For ideas not yet built, see [ROADMAP.md](ROADMAP.md).

---

## Table of contents

1. [The broadcast hub](#1-the-broadcast-hub)
2. [Subscriptions](#2-subscriptions)
3. [The event model](#3-the-event-model)
4. [The wire-format renderer](#4-the-wire-format-renderer)
5. [The HTTP writer](#5-the-http-writer)
6. [Back-pressure: DropOldest](#6-back-pressure-dropoldest)
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
    int Publish(string topic, ServerSentEvent evt);
    int SubscriberCount(string topic);
}
```

- `Subscribe(topic)` registers a subscriber and returns a `StreamSubscription`.
- `Publish(topic, evt)` delivers to every current subscriber and returns how many it reached.
  Publishing to a topic with no subscribers returns `0`.
- `SubscriberCount(topic)` returns the current subscriber count for a topic.

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

---

## 6. Back-pressure: DropOldest

Every subscriber gets its own bounded channel created with `BoundedChannelFullMode.DropOldest` and
capacity `SubscriberCapacity`. The effects:

- `Publish` never blocks on a slow subscriber.
- A slow subscriber affects only its own stream, never the producer or other subscribers.
- When a subscriber's buffer is full at publish time, its oldest buffered event is evicted to admit
  the newest, and the eviction is recorded as `orionstream.dropped`.

This trades guaranteed per-client delivery for keeping every stream live and current. Use
`ServerSentEvent.Id` with a server-side replay source if a client must recover missed events.

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
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);
}
```

- `SubscriberCapacity` must be at least `1`.
- `HeartbeatInterval` must be positive.

Options are validated eagerly when you call `AddOrionStream`, so a bad value throws at registration
rather than at first use.

---

## 9. Telemetry

`StreamDiagnostics` owns a `System.Diagnostics.Metrics.Meter` named `Moongazing.OrionStream`
(`StreamDiagnostics.MeterName`).

| Instrument | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `orionstream.published` | Counter | `{event}` | Events published, counted once per publish. |
| `orionstream.dropped` | Counter | `{event}` | Events dropped on a full subscriber buffer. |
| `orionstream.subscribers` | Observable gauge | `{subscriber}` | Currently connected subscribers across all topics. |

`StreamDiagnostics` is `IDisposable`; disposing it disposes the meter. Wire it into OpenTelemetry
with `AddMeter(StreamDiagnostics.MeterName)`.

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
