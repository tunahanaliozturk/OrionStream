namespace Moongazing.OrionStream.Redis;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using Moongazing.OrionStream.Streaming;

using StackExchange.Redis;

/// <summary>
/// Registration helpers that swap OrionStream's replay backlog onto Redis. The core hub stays an
/// in-process fan-out with no mandatory dependency; this opt-in package only replaces the
/// <see cref="IReplayStoreFactory"/> so the per-topic resume backlog lives in Redis instead of an
/// in-process ring, enabling cross-instance resume and survival across a restart.
/// </summary>
public static class OrionStreamRedisServiceCollectionExtensions
{
    /// <summary>
    /// Use Redis as the OrionStream replay backlog, connecting with <paramref name="connectionString"/>.
    /// Registers a shared <see cref="IConnectionMultiplexer"/> (only if one is not already registered)
    /// and replaces the replay store factory with the Redis one. Call this alongside
    /// <c>AddOrionStream</c>; order does not matter, because the Redis factory is registered
    /// definitively rather than with <c>TryAdd</c>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">A StackExchange.Redis connection string, for example <c>localhost:6379</c>.</param>
    /// <param name="configure">Optional key-prefix, database, and TTL tuning.</param>
    /// <returns>The same collection for chaining.</returns>
    public static IServiceCollection AddOrionStreamRedisReplayStore(
        this IServiceCollection services,
        string connectionString,
        Action<RedisReplayStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.TryAddSingleton<IConnectionMultiplexer>(
            _ => ConnectionMultiplexer.Connect(connectionString));

        return AddOrionStreamRedisReplayStore(services, configure);
    }

    /// <summary>
    /// Use Redis as the OrionStream replay backlog over an already-registered
    /// <see cref="IConnectionMultiplexer"/>. Replaces the replay store factory with the Redis one;
    /// the multiplexer must be registered separately (by this package's connection-string overload or
    /// by the caller).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional key-prefix, database, and TTL tuning.</param>
    /// <returns>The same collection for chaining.</returns>
    public static IServiceCollection AddOrionStreamRedisReplayStore(
        this IServiceCollection services,
        Action<RedisReplayStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new RedisReplayStoreOptions();
        configure?.Invoke(options);
        options.Validate();

        // Replace any previously-registered factory (including the in-memory default that AddOrionStream
        // wires with TryAdd) so the swap is deterministic regardless of the order the two registration
        // calls run in. AddOrionStream itself uses TryAdd, so it will never overwrite this.
        services.RemoveAll<IReplayStoreFactory>();
        services.AddSingleton<IReplayStoreFactory>(
            sp => new RedisReplayStoreFactory(sp.GetRequiredService<IConnectionMultiplexer>(), options));

        return services;
    }
}
