# OrionStream.Redis

A durable, cross-instance replay store for [OrionStream](https://github.com/tunahanaliozturk/OrionStream),
the in-process Server-Sent Events hub for ASP.NET Core.

OrionStream's core resume backlog is an in-process ring: it is fast and dependency-free, but it lives
in one process and does not survive a restart. This opt-in package plugs a Redis-backed
`IReplayStore` into the same seam, so a client can resume by `Last-Event-ID` after a load balancer
reconnects it to a **different** hub instance, and the backlog survives a process restart.

## Scope

This package stores and serves only the **resume backlog**. It does not turn the hub into a
cross-instance publish bus: an event published on instance A is still delivered only to A's live
subscribers. Redis is read on resume to rebuild what a reconnecting client missed, not to fan out live
events between instances. For durable cross-process messaging, use a real broker.

## Install

```bash
dotnet add package OrionStream.Redis
```

## Use

Register OrionStream as usual, then swap the replay backlog onto Redis:

```csharp
builder.Services.AddOrionStream(o => o.ReplayBufferCapacity = 256);
builder.Services.AddOrionStreamRedisReplayStore("localhost:6379");
```

The two calls can run in either order. The Redis factory replaces the default in-memory one
definitively, so nothing else changes: the hub keeps owning event-id allocation and live delivery, and
only the backlog moves to Redis.

To reuse an `IConnectionMultiplexer` you already register, use the no-connection-string overload:

```csharp
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect("localhost:6379"));
builder.Services.AddOrionStream();
builder.Services.AddOrionStreamRedisReplayStore(o =>
{
    o.KeyPrefix = "myapp:orionstream:replay:";
    o.BacklogTimeToLive = TimeSpan.FromHours(1);
});
```

## How it works

- One Redis list per topic, keyed `{KeyPrefix}{topic}`, holds backlog entries oldest first. Each
  element carries a Redis-wide order prefix and the JSON-encoded entry.
- Each append runs a single Lua script (one `EVAL`) that increments a per-topic Redis-wide counter
  (`{key}:seq`), `RPUSH`es the new element, `LTRIM`s to the newest `capacity` entries, and refreshes
  the optional TTL, all atomically. The cap matches the `ReplayBufferCapacity`
  drop-oldest-beyond-capacity bound of the in-memory store, and no concurrent reader can observe an
  over-capacity or half-trimmed backlog.
- Entries are ordered by the **Redis-wide** counter, not the hub's per-instance sequence: two instances
  publishing to the same topic each start their own sequence at 1, so a single total order over the
  shared backlog must come from Redis. Resume matches the returning `Last-Event-ID` against the exact
  wire id each entry emitted, resolves a duplicate id to the oldest matching entry, and replays the
  ascending suffix after it in Redis-wide order: the same duplicate contract the in-memory store
  documents, made correct across instances.
- An optional sliding TTL lets a topic that has gone quiet be reclaimed by Redis on its own while an
  active topic never expires.

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `KeyPrefix` | `orionstream:replay:` | Namespaces per-topic backlog keys. Give independent hubs sharing one Redis distinct prefixes. |
| `Database` | `-1` | Redis logical database index, or `-1` for the multiplexer default. |
| `BacklogTimeToLive` | `null` | Optional sliding expiry refreshed on every append; null retains the backlog with no expiry. |

## License

MIT.
