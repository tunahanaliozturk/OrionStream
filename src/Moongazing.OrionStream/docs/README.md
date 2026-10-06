# OrionStream

Server-Sent Events for ASP.NET Core: publish an event to a topic and every connected client subscribed to it receives it over a plain `text/event-stream` response that a browser `EventSource` reads with no client library. Bounded buffer per subscriber, `Last-Event-ID` resume, heartbeats and telemetry included.

![OrionStream publish flow: Publish records orion.stream.published, assigns the topic sequence and appends to the replay store under the topic lock, then for each subscriber applies the filter and, when the buffer is full, the FullBufferPolicy (DropOldest, DropNewest, or Wait up to MaxPublishWait), optionally disconnects a slow consumer, records orion.stream.dropped once and returns the delivered count](https://raw.githubusercontent.com/tunahanaliozturk/OrionStream/main/docs/diagrams/publish-flow.png)

## Install

    dotnet add package OrionStream

The package carries a `FrameworkReference` to `Microsoft.AspNetCore.App`, so use it in an ASP.NET Core project (`Microsoft.NET.Sdk.Web`). It targets `net8.0`, `net9.0` and `net10.0`.

## Quick start

A complete `Program.cs` (implicit usings on):

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Streaming;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOrionStream(o => o.HeartbeatInterval = TimeSpan.FromSeconds(15));

var app = builder.Build();

// GET endpoint: subscribes to "orders" (resuming from the Last-Event-ID header when present)
// and streams it with the SSE headers, heartbeats and disconnect handling.
app.MapServerSentEvents("/events/orders", "orders");

app.MapPost("/orders", (Order order, ISseHub hub) =>
{
    // Serializes the payload to the data: field with System.Text.Json.
    hub.Publish("orders", order, eventName: "order.created", id: order.Id.ToString());
    return Results.Accepted();
});

app.Run();

public sealed record Order(Guid Id, decimal Total);
```

In the browser:

```js
const es = new EventSource("/events/orders");
es.addEventListener("order.created", e => console.log(JSON.parse(e.data)));
```

The typed `Publish<T>` extension used above serializes with reflection, so it is annotated `[RequiresUnreferencedCode]` / `[RequiresDynamicCode]`. Under trimming or NativeAOT, serialize yourself and call the raw `hub.Publish(topic, new ServerSentEvent { Id = ..., EventName = ..., Data = json })`, which is trim- and AOT-safe.

## Options

| `StreamOptions` | Default |
|-----------------|---------|
| `SubscriberCapacity` (bounded buffer per subscriber) | 256 |
| `FullBufferPolicy` (`DropOldest`, `DropNewest`, `Wait`) | `DropOldest` |
| `MaxPublishWait` (required for `Wait`; cap per full subscriber) | none |
| `SlowConsumerPolicy` (disconnect after N consecutive full publishes) | none |
| `HeartbeatInterval` | 15 s |
| `ReplayBufferCapacity` (events kept per topic for resume; 0 disables) | 256 |
| `SerializerOptions` (typed publish helpers) | web defaults |

`ConfigureTopic(topic, t => ...)` overrides the subscriber and replay capacities for one topic. Options are validated when `AddOrionStream` runs.

## Delivery semantics

- Each subscriber has its own bounded channel. Under `DropOldest` (default) a full buffer evicts its oldest event, so `Publish` never waits for a slow reader and a slow client degrades only its own stream. `DropNewest` discards the incoming event instead.
- `Wait` blocks `Publish` for room, up to `MaxPublishWait` for each full subscriber in turn, then drops the event for that subscriber. It is the only policy that slows the publisher.
- `Publish` returns the number of subscribers whose buffer accepted the event. A filter passed to `Subscribe(topic, lastEventId, filter)` runs before the buffer; rejected events never take a slot.
- Every event gets a wire `id:`: the producer's `ServerSentEvent.Id`, or a per-topic hub sequence (1, 2, 3, ...). On reconnect, a `Last-Event-ID` that matches a retained event replays the events after it; an unknown or evicted id starts from now. Replayed events go through the subscriber's buffer, so size `SubscriberCapacity` at least as large as `ReplayBufferCapacity` when gap-free resume matters.
- The hub sequence lives in process memory. Resume across instances or restarts needs a shared store (`OrionStream.Redis`) and unique producer ids.

## Telemetry

`Meter` and `ActivitySource` named `Moongazing.OrionStream` (`StreamDiagnostics.MeterName`):

- `orion.stream.published` - recorded by `Publish` once per call, tagged `orion.stream.topic`.
- `orion.stream.dropped` - recorded by `Publish` once per call with the number of full subscribers it dropped for (evictions, discards, `Wait` timeouts), tagged `orion.stream.topic`. Replay-time losses on `Subscribe` are not counted.
- `orion.stream.subscribers` - gauge of currently connected subscribers.
- Spans `OrionStream.Publish` and `OrionStream.Subscribe`.

## Related packages

- `OrionStream.Redis` - Redis replay store for `Last-Event-ID` resume across instances and restarts.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionStream
- Changelog: https://github.com/tunahanaliozturk/OrionStream/blob/main/CHANGELOG.md
- License: MIT
