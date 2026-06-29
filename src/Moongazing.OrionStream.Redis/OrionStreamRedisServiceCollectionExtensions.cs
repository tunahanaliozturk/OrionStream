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
    /// Registers a shared <see cref="IConnectionMultiplexer"/> built from the connection string and
    /// replaces the replay store factory with the Redis one. Call this alongside <c>AddOrionStream</c>;
    /// order does not matter, because the Redis factory is registered definitively rather than with
    /// <c>TryAdd</c>.
    /// </summary>
    /// <remarks>
    /// Calling this more than once is last-write-wins on BOTH the connection and the options: a later
    /// call's <paramref name="connectionString"/> replaces the multiplexer the earlier call registered,
    /// rather than being silently ignored. This matters because a misconfigured first call (wrong host,
    /// stale credentials) would otherwise be impossible to override by a corrected second call. A
    /// multiplexer the CALLER registered itself is left untouched: this overload only replaces the one
    /// IT owns (one built from a connection string by this package).
    /// </remarks>
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

        // Replace any multiplexer THIS package previously registered from a connection string so a later
        // call's connection string wins. A multiplexer the caller registered themselves (not tagged as
        // package-owned) is left in place: the no-connection-string overload is the seam for reusing it.
        RemovePackageOwnedMultiplexer(services);
        var multiplexerFactory = new PackageOwnedConnectionMultiplexerFactory(connectionString);
        services.AddSingleton<IConnectionMultiplexer>(multiplexerFactory.Create);

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

    // Remove only the IConnectionMultiplexer registration THIS package added from a connection string,
    // identified by its package-owned factory marker. A caller-registered multiplexer carries no marker
    // and is left in place.
    private static void RemovePackageOwnedMultiplexer(IServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType == typeof(IConnectionMultiplexer) &&
                descriptor.ImplementationFactory?.Target is PackageOwnedConnectionMultiplexerFactory)
            {
                services.RemoveAt(i);
            }
        }
    }

    // A connection-string-backed multiplexer factory whose TYPE marks the registration as owned by this
    // package, so a later AddOrionStreamRedisReplayStore(connectionString) call can find and replace it
    // (last connection string wins) without disturbing a multiplexer the caller registered themselves.
    // Connecting lazily inside the factory keeps the connection out of registration time.
    private sealed class PackageOwnedConnectionMultiplexerFactory
    {
        private readonly string connectionString;

        public PackageOwnedConnectionMultiplexerFactory(string connectionString) =>
            this.connectionString = connectionString;

        // The DI factory. Returns the concrete multiplexer; delegate return-type covariance lets the
        // method group bind to the registered Func&lt;IServiceProvider, IConnectionMultiplexer&gt;, and
        // the resulting delegate's Target is this instance, which is how a later call detects and
        // replaces the package-owned registration.
        public ConnectionMultiplexer Create(IServiceProvider _) =>
            ConnectionMultiplexer.Connect(connectionString);
    }
}
