namespace Moongazing.OrionStream.Redis;

using Moongazing.OrionStream.Streaming;

using StackExchange.Redis;

/// <summary>
/// A durable, cross-instance <see cref="IReplayStore"/> backed by a capped Redis list, one per topic.
/// It is the opt-in backplane behind the same seam the in-memory ring sits behind: a client can resume
/// by <c>Last-Event-ID</c> after a load balancer reconnects it to a <em>different</em> hub instance,
/// and the backlog survives a process restart, because the backlog lives in Redis rather than in the
/// publishing process.
/// </summary>
/// <remarks>
/// <para>
/// Scope: this stores and serves only the resume backlog. It does NOT make the hub a cross-instance
/// publish bus. An event published on instance A is still delivered only to A's live subscribers; the
/// Redis store is read on resume to rebuild what a reconnecting client missed, not to fan out live
/// events between instances.
/// </para>
/// <para>
/// Structure: one Redis list per topic, keyed <c>{KeyPrefix}{topic}</c>, holding JSON
/// <see cref="ReplayEntry"/> payloads ordered oldest-first. <see cref="Append"/> does an <c>RPUSH</c>
/// followed by an <c>LTRIM</c> that keeps the newest <c>capacity</c> elements, which is the
/// DropOldest-beyond-capacity bound the in-memory ring implements and the
/// <see cref="StreamOptions.ReplayBufferCapacity"/> contract requires. The hub appends in ascending
/// <see cref="ReplayEntry.Sequence"/> order under its per-topic gate, so list order is sequence order;
/// <see cref="GetReplay"/> reads the whole list with <c>LRANGE</c> and applies the documented
/// first-match-wins, oldest-duplicate resolution and ascending-suffix replay against it. The seam's
/// per-topic single-thread guarantee holds only within one process; the Redis operations are each
/// server-atomic, and the contract tolerates concurrent appenders because every entry carries the
/// hub's own gap-free sequence as its ordering key.
/// </para>
/// </remarks>
public sealed class RedisReplayStore : IReplayStore
{
    private readonly IDatabase database;
    private readonly RedisKey key;
    private readonly int capacity;
    private readonly TimeSpan? ttl;

    /// <summary>Create a Redis-backed store for one topic.</summary>
    /// <param name="database">The Redis database the backlog list lives in.</param>
    /// <param name="key">The fully-qualified Redis key for this topic's backlog list.</param>
    /// <param name="capacity">The retention bound: the newest <paramref name="capacity"/> entries are kept. Must be greater than zero.</param>
    /// <param name="ttl">An optional sliding expiry refreshed on every append, or null for no expiry.</param>
    public RedisReplayStore(IDatabase database, RedisKey key, int capacity, TimeSpan? ttl = null)
    {
        ArgumentNullException.ThrowIfNull(database);

        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "Replay store capacity must be at least 1; the hub does not create a store for a topic with replay disabled.");
        }

        if (ttl is { } t && t <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), ttl,
                "Backlog TTL, when set, must be greater than zero.");
        }

        this.database = database;
        this.key = key;
        this.capacity = capacity;
        this.ttl = ttl;
    }

    /// <inheritdoc />
    public bool HasBacklog => database.ListLength(key) > 0;

    /// <inheritdoc />
    public void Append(ReplayEntry entry)
    {
        var payload = ReplayEntryCodec.Serialize(entry);

        // RPUSH appends to the tail (newest last); LTRIM to the last `capacity` elements evicts the
        // oldest beyond the bound, reproducing the in-memory ring's DropOldest semantics. Both run on
        // the Redis server; issued back to back on the one multiplexer they preserve append-then-trim
        // order for this key. The optional TTL is refreshed here so a still-active topic never expires
        // while a quiet one can be reclaimed by Redis on its own.
        database.ListRightPush(key, payload);
        database.ListTrim(key, -capacity, -1);

        if (ttl is { } expiry)
        {
            database.KeyExpire(key, expiry);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<ReplayEntry> GetReplay(string lastEventId)
    {
        // Read the whole bounded backlog, oldest-first. The list is capped at `capacity`, so this is a
        // bounded read, not an unbounded scan.
        var stored = database.ListRange(key);
        if (stored.Length == 0)
        {
            return [];
        }

        var entries = new ReplayEntry[stored.Length];
        for (var i = 0; i < stored.Length; i++)
        {
            entries[i] = ReplayEntryCodec.Deserialize(stored[i]!);
        }

        // Locate the resume point by matching Last-Event-ID against the EXACT wire id each retained
        // entry emitted. The list is oldest-first, so the FIRST match is the oldest entry carrying the
        // id: that implements the seam's duplicate-WireId contract (a reused producer id resolves to the
        // oldest, replaying the most events and never skipping one). Hub sequences never collide, so a
        // duplicate only arises for a reused producer id.
        long? resumeAfter = null;
        foreach (var entry in entries)
        {
            if (string.Equals(entry.WireId, lastEventId, StringComparison.Ordinal))
            {
                resumeAfter = entry.Sequence;
                break;
            }
        }

        if (resumeAfter is not { } afterSequence)
        {
            // Unknown / evicted id: empty result is the from-now fallback, never a partial backlog.
            return [];
        }

        // Replay every retained entry published AFTER the matched one, in ascending sequence order
        // (which is list order here, since the hub appends in sequence order).
        var replay = new List<ReplayEntry>();
        foreach (var entry in entries)
        {
            if (entry.Sequence > afterSequence)
            {
                replay.Add(entry);
            }
        }

        return replay;
    }
}
