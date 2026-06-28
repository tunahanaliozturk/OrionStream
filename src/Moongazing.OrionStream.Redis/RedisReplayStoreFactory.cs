namespace Moongazing.OrionStream.Redis;

using Moongazing.OrionStream.Streaming;

using StackExchange.Redis;

/// <summary>
/// An <see cref="IReplayStoreFactory"/> that hands out a <see cref="RedisReplayStore"/> per topic over
/// a shared <see cref="IConnectionMultiplexer"/>. Registering this in place of the default
/// <see cref="InMemoryReplayStoreFactory"/> moves every topic's replay backlog into Redis without the
/// hub knowing where the backlog lives, so resume works across instances and survives a restart.
/// </summary>
public sealed class RedisReplayStoreFactory : IReplayStoreFactory
{
    private readonly IConnectionMultiplexer connection;
    private readonly RedisReplayStoreOptions options;

    /// <summary>Create the factory.</summary>
    /// <param name="connection">The shared Redis connection the per-topic stores use.</param>
    /// <param name="options">Key prefix, database, and TTL tuning. Validated on construction.</param>
    public RedisReplayStoreFactory(IConnectionMultiplexer connection, RedisReplayStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        this.connection = connection;
        this.options = options;
    }

    /// <inheritdoc />
    public IReplayStore Create(string topic, int capacity)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);

        var database = connection.GetDatabase(options.Database);
        var key = (RedisKey)(options.KeyPrefix + topic);
        return new RedisReplayStore(database, key, capacity, options.BacklogTimeToLive);
    }
}
