namespace Moongazing.OrionStream.Redis;

using System.Globalization;

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
/// Ordering across instances: the hub's <see cref="ReplayEntry.Sequence"/> is allocated
/// <em>per process</em>, so two instances publishing to the same topic both start their sequence at 1
/// and the shared Redis backlog would carry colliding, non-comparable sequences if it ordered by them.
/// This store therefore orders the shared backlog by a <em>Redis-wide</em> monotonic score assigned
/// atomically at append from a per-topic <c>INCR</c> counter (<c>{key}:seq</c>). That score is the
/// single total order over every append regardless of which instance made it; resume computes the
/// replay suffix against it, not against the per-instance hub sequence. The client-facing resume id
/// stays the entry's <see cref="ReplayEntry.WireId"/> (the producer id or the hub sequence rendered on
/// the wire): <c>Last-Event-ID</c> is matched against the wire id, and the suffix after the matched
/// entry is taken in Redis-wide score order.
/// </para>
/// <para>
/// Structure: one Redis list per topic, keyed <c>{KeyPrefix}{topic}</c>, holding elements of the form
/// <c>{redisScore}|{json}</c> ordered oldest-first, where <c>json</c> is the serialized
/// <see cref="ReplayEntry"/> and <c>redisScore</c> is the Redis-wide order. <see cref="Append"/> runs a
/// single Lua script (one <c>EVAL</c>) that increments the per-topic counter, <c>RPUSH</c>es the new
/// element, <c>LTRIM</c>s to the newest <c>capacity</c> elements (the DropOldest-beyond-capacity bound
/// the in-memory ring implements and the <see cref="StreamOptions.ReplayBufferCapacity"/> contract
/// requires) and refreshes the optional TTL, all atomically, so no concurrent reader observes a torn
/// or over-capacity backlog. <see cref="GetReplay"/> reads the whole list with <c>LRANGE</c> and applies
/// the documented first-match-wins, oldest-duplicate resolution and ascending-suffix replay keyed on the
/// Redis-wide score.
/// </para>
/// </remarks>
public sealed class RedisReplayStore : IReplayStore
{
    // Separator between the Redis-wide score and the serialized entry in each list element. '|' cannot
    // appear in the decimal score and is irrelevant to the opaque JSON suffix, so a single split at the
    // first occurrence is unambiguous.
    private const char ScoreSeparator = '|';

    // One atomic append. KEYS[1] is the per-topic backlog list, KEYS[2] the per-topic Redis-wide
    // counter. ARGV[1] is the serialized entry JSON, ARGV[2] the capacity, ARGV[3] the TTL in
    // milliseconds (0 = no TTL). The whole script runs server-side under Redis's single-threaded
    // execution, so the INCR, RPUSH, LTRIM and EXPIRE are one indivisible unit: no reader can see an
    // over-capacity list, a half-trimmed list, or a score gap. Returns the assigned Redis-wide score.
    private const string AppendScript = """
        local score = redis.call('INCR', KEYS[2])
        redis.call('RPUSH', KEYS[1], score .. '|' .. ARGV[1])
        redis.call('LTRIM', KEYS[1], -tonumber(ARGV[2]), -1)
        local ttl = tonumber(ARGV[3])
        if ttl > 0 then
          redis.call('PEXPIRE', KEYS[1], ttl)
          redis.call('PEXPIRE', KEYS[2], ttl)
        end
        return score
        """;

    private readonly IDatabase database;
    private readonly RedisKey key;
    private readonly RedisKey sequenceKey;
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
        // The per-topic Redis-wide counter lives next to the backlog list. Keying it off the same string
        // keeps both in the same Redis hash slot (cluster-safe for the single EVAL touching both keys).
        sequenceKey = (RedisKey)(key.ToString() + ":seq");
        this.capacity = capacity;
        this.ttl = ttl;
    }

    /// <inheritdoc />
    public bool HasBacklog => database.ListLength(key) > 0;

    /// <inheritdoc />
    public void Append(ReplayEntry entry)
    {
        var payload = ReplayEntryCodec.Serialize(entry);
        var ttlMilliseconds = ttl is { } expiry
            ? (long)expiry.TotalMilliseconds
            : 0L;

        // One EVAL does INCR + RPUSH + LTRIM + (optional) EXPIRE atomically. Assigning the Redis-wide
        // score and capping the list in the same server-side unit is what keeps the backlog totally
        // ordered across instances AND torn-read free under concurrent appenders.
        database.ScriptEvaluate(
            AppendScript,
            [key, sequenceKey],
            [payload, capacity, ttlMilliseconds]);
    }

    /// <inheritdoc />
    public IReadOnlyList<ReplayEntry> GetReplay(string lastEventId)
    {
        // Read the whole bounded backlog, oldest-first (Redis-wide score order). The list is capped at
        // `capacity`, so this is a bounded read, not an unbounded scan.
        var stored = database.ListRange(key);
        if (stored.Length == 0)
        {
            return [];
        }

        var scores = new long[stored.Length];
        var entries = new ReplayEntry[stored.Length];
        for (var i = 0; i < stored.Length; i++)
        {
            (scores[i], entries[i]) = Decode(stored[i]!);
        }

        // Locate the resume point by matching Last-Event-ID against the EXACT wire id each retained entry
        // emitted. The list is oldest-first in Redis-wide score order, so the FIRST match is the oldest
        // entry carrying the id: that implements the seam's duplicate-WireId contract (a reused producer
        // id resolves to the oldest, replaying the most events and never skipping one). The resume point
        // is keyed on the REDIS-WIDE score, not the per-instance hub sequence, so the suffix is correctly
        // ordered no matter which instance published each event.
        long? resumeAfter = null;
        for (var i = 0; i < entries.Length; i++)
        {
            if (string.Equals(entries[i].WireId, lastEventId, StringComparison.Ordinal))
            {
                resumeAfter = scores[i];
                break;
            }
        }

        if (resumeAfter is not { } afterScore)
        {
            // Unknown / evicted id: empty result is the from-now fallback, never a partial backlog.
            return [];
        }

        // Replay every retained entry whose Redis-wide score is greater than the matched one, in list
        // order (which IS ascending Redis-wide score order).
        var replay = new List<ReplayEntry>();
        for (var i = 0; i < entries.Length; i++)
        {
            if (scores[i] > afterScore)
            {
                replay.Add(entries[i]);
            }
        }

        return replay;
    }

    // Split a stored "{redisScore}|{json}" element back into its Redis-wide score and the entry. Splits
    // at the FIRST separator only: the score is a plain decimal with no separator, and the JSON suffix is
    // passed through untouched even if it contains the separator character.
    private static (long Score, ReplayEntry Entry) Decode(string element)
    {
        var split = element.IndexOf(ScoreSeparator, StringComparison.Ordinal);
        if (split <= 0)
        {
            throw new FormatException("A stored OrionStream replay element was missing its Redis-wide order prefix.");
        }

        var scoreText = element.AsSpan(0, split);
        if (!long.TryParse(scoreText, NumberStyles.None, CultureInfo.InvariantCulture, out var score))
        {
            throw new FormatException("A stored OrionStream replay element had an unparsable Redis-wide order prefix.");
        }

        var entry = ReplayEntryCodec.Deserialize(element[(split + 1)..]);
        return (score, entry);
    }
}
