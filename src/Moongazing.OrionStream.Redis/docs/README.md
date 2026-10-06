# OrionStream.Redis

A Redis-backed replay store for OrionStream, the Server-Sent Events hub for ASP.NET Core: the `Last-Event-ID` resume backlog lives in Redis instead of process memory, so a client can resume after a load balancer reconnects it to a different instance, and the backlog survives a restart.

![OrionStream overview: your code publishes to the SseHub, which fans each event out to a bounded buffer per subscriber and the SSE endpoint streams it to the browser; the hub keeps the replay backlog per topic in the in-memory store or, with OrionStream.Redis, in Redis](https://raw.githubusercontent.com/tunahanaliozturk/OrionStream/main/docs/diagrams/overview.png)

## Install

    dotnet add package OrionStream.Redis

It plugs into the core `OrionStream` package (referenced for you) behind its `IReplayStore` seam, over StackExchange.Redis.

## Quick start

A complete `Program.cs` in an ASP.NET Core project (implicit usings on):

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOrionStream(o => o.ReplayBufferCapacity = 256);
builder.Services.AddOrionStreamRedisReplayStore("localhost:6379");

var app = builder.Build();
app.MapServerSentEvents("/events/orders", "orders");
app.Run();
```

The two registration calls can run in either order: the Redis factory replaces the in-memory default rather than using `TryAdd`. The connection-string overload registers an `IConnectionMultiplexer` that connects when it is first resolved; calling it again replaces that multiplexer.

To reuse a multiplexer you register yourself, use the overload without a connection string:

```csharp
using Moongazing.OrionStream;
using Moongazing.OrionStream.Redis;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect("localhost:6379"));
builder.Services.AddOrionStream();
builder.Services.AddOrionStreamRedisReplayStore(o =>
{
    o.KeyPrefix = "myapp:orionstream:replay:";
    o.BacklogTimeToLive = TimeSpan.FromHours(1);
});
```

## Options

| `RedisReplayStoreOptions` | Default | Purpose |
|---------------------------|---------|---------|
| `KeyPrefix` | `orionstream:replay:` | Per-topic list key is `{KeyPrefix}{topic}`. Give independent hubs sharing one Redis distinct prefixes. |
| `Database` | `-1` | Redis logical database, or `-1` for the multiplexer default. |
| `BacklogTimeToLive` | none | Sliding expiry refreshed on every append; none keeps the backlog with no expiry. |

The backlog length per topic is the hub's `ReplayBufferCapacity` (or its per-topic override).

## How it works

- One Redis list per topic holds the newest `ReplayBufferCapacity` entries, oldest first.
- Each append is one Lua script (`EVAL`): `INCR` a per-topic counter (`{key}:seq`), `RPUSH` the entry prefixed with that counter, `LTRIM` to capacity and refresh the optional TTL. The script runs as one unit on the Redis server, so a reader never sees an over-capacity or half-trimmed list.
- Resume reads the list, matches `Last-Event-ID` against the exact wire id each entry emitted (the oldest entry wins when an id repeats) and returns the entries after it in list order.

## Limits

- Scope is the resume backlog only. An event published on instance A still reaches only A's live subscribers; this is not a cross-instance publish bus.
- Hub-assigned wire ids (1, 2, 3, ...) are numbered per process: they restart at 1 after a restart, and two instances number the same topic independently. When more than one instance publishes to a topic, or the backlog must survive a restart, set a unique `ServerSentEvent.Id` on every event; otherwise two retained entries can share a wire id and resume can start from the wrong one.
- The hub sorts a replayed backlog by its own per-instance sequence before delivering it, so when several instances publish to one topic the replay order follows each instance's numbering, not the Redis list order.
- Every `Publish` on a replay-enabled topic makes a synchronous Redis call under the topic lock, and `Subscribe` with a `Last-Event-ID` reads the list. A Redis error is thrown from `Publish` (before any subscriber receives the event) or from `Subscribe`.

## Related packages

- `OrionStream` - the core hub, formatter, writer and endpoint helper.

## Links

- Documentation and full README: https://github.com/tunahanaliozturk/OrionStream
- Changelog: https://github.com/tunahanaliozturk/OrionStream/blob/main/CHANGELOG.md
- License: MIT
