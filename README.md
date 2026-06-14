# OrionStream

[![CI/CD](https://github.com/tunahanaliozturk/OrionStream/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/tunahanaliozturk/OrionStream/actions/workflows/ci-cd.yml)
[![NuGet](https://img.shields.io/nuget/v/OrionStream.svg)](https://www.nuget.org/packages/OrionStream/)

Server-Sent Events for ASP.NET Core. Publish an event to a topic and every connected client
subscribed to that topic receives it, over a plain `text/event-stream` response that any browser
`EventSource` can read with no extra client library.

Part of the **Orion** family. Usable entirely on its own.

## Why

SSE is the simplest way to push server events to a browser: one long-lived HTTP response, a tiny
wire format, automatic client reconnect. The fiddly parts are the wire format (multi-line data,
the field order, heartbeats so proxies do not close an idle stream) and fan-out without letting a
slow client stall the others. OrionStream handles both: a hub with a bounded buffer per subscriber
and a spec-correct writer.

## Install

```
dotnet add package OrionStream
```

## Quick start

```csharp
builder.Services.AddOrionStream(o =>
{
    o.SubscriberCapacity = 256;
    o.HeartbeatInterval = TimeSpan.FromSeconds(15);
});
```

Endpoint that streams a topic to the client:

```csharp
app.MapGet("/events/orders", async (HttpContext ctx, ISseHub hub, StreamOptions options) =>
{
    using var subscription = hub.Subscribe("orders");
    await ctx.Response.WriteStreamAsync(subscription, options.HeartbeatInterval, ctx.RequestAborted);
});
```

Publish from anywhere:

```csharp
hub.Publish("orders", new ServerSentEvent
{
    Id = order.Id.ToString(),
    EventName = "order.created",
    Data = JsonSerializer.Serialize(order),
});
```

The browser side is just:

```js
const es = new EventSource("/events/orders");
es.addEventListener("order.created", e => console.log(JSON.parse(e.data)));
```

## Back-pressure

Each subscriber has its own bounded buffer (`SubscriberCapacity`). A publish never blocks: if a
subscriber has fallen behind and its buffer is full, the oldest buffered event is dropped to admit
the newest, and the drop is counted in telemetry. A slow client degrades only its own stream.

## Heartbeats and reconnect

When a stream is idle for `HeartbeatInterval`, the writer sends an SSE comment line so proxies keep
the connection open. Set `ServerSentEvent.Id` and the browser will send it back as `Last-Event-ID`
on reconnect, which you can read to resume.

## Telemetry

Subscribe to the `Moongazing.OrionStream` meter: `orionstream.published`, `orionstream.dropped`,
and an `orionstream.subscribers` gauge of currently connected subscribers.

## Design

- Multi-targets `net8.0`, `net9.0`, `net10.0`.
- `TreatWarningsAsErrors`, latest analyzers, nullable enabled.
- The hub and the SSE formatter are independent of HTTP, so both are unit-tested directly; the
  writer extension adapts them to an `HttpResponse`.

## License

MIT.
