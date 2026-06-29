namespace Moongazing.OrionStream.Redis;

/// <summary>
/// Tuning for the Redis-backed replay store. Defaults are chosen so the store behaves like the
/// in-memory ring with no extra configuration; override only to share one Redis across more than one
/// logically separate hub or to move the per-topic backlog to a non-default database.
/// </summary>
public sealed class RedisReplayStoreOptions
{
    /// <summary>
    /// Prefix for every per-topic backlog key. The full key for a topic is
    /// <c>{KeyPrefix}{topic}</c>. Sharing one Redis between two independent hubs that use the same
    /// topic names requires giving each a distinct prefix so their backlogs do not collide. Defaults
    /// to <c>orionstream:replay:</c>.
    /// </summary>
    public string KeyPrefix { get; set; } = "orionstream:replay:";

    /// <summary>
    /// The Redis logical database index the backlog lives in, or <c>-1</c> for the multiplexer's
    /// default database. Defaults to <c>-1</c>.
    /// </summary>
    public int Database { get; set; } = -1;

    /// <summary>
    /// Optional sliding time-to-live applied to each per-topic backlog key on every append, or null
    /// to leave the backlog without an expiry. A TTL lets a backlog for a topic that has gone quiet be
    /// reclaimed by Redis on its own rather than lingering forever; each append refreshes it, so an
    /// active topic never expires. Null by default, matching the in-memory store, which retains the
    /// backlog for the process lifetime.
    /// </summary>
    public TimeSpan? BacklogTimeToLive { get; set; }

    /// <summary>Validate the options, throwing when a value cannot produce a working store.</summary>
    /// <exception cref="ArgumentException">A required value is missing or out of range.</exception>
    public void Validate()
    {
        if (string.IsNullOrEmpty(KeyPrefix))
        {
            throw new ArgumentException(
                "RedisReplayStoreOptions.KeyPrefix must be a non-empty string so per-topic backlog keys are namespaced.",
                nameof(KeyPrefix));
        }

        if (Database < -1)
        {
            throw new ArgumentException(
                "RedisReplayStoreOptions.Database must be -1 for the multiplexer's default database or a non-negative logical database index.",
                nameof(Database));
        }

        if (BacklogTimeToLive is { } ttl && ttl <= TimeSpan.Zero)
        {
            throw new ArgumentException(
                "RedisReplayStoreOptions.BacklogTimeToLive, when set, must be greater than zero; use null to disable expiry.",
                nameof(BacklogTimeToLive));
        }
    }
}
