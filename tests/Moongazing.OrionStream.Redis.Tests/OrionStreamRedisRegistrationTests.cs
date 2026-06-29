namespace Moongazing.OrionStream.Redis.Tests;

using Microsoft.Extensions.DependencyInjection;

using Moongazing.OrionStream.Streaming;

using StackExchange.Redis;

/// <summary>
/// Registration semantics for <see cref="OrionStreamRedisServiceCollectionExtensions"/>. These assert on
/// the <see cref="ServiceCollection"/> shape only and never resolve the multiplexer, so they make no
/// network call and do not need the Redis container.
/// </summary>
public sealed class OrionStreamRedisRegistrationTests
{
    [Fact]
    public void Repeated_registration_replaces_the_package_owned_multiplexer_rather_than_keeping_the_first()
    {
        var services = new ServiceCollection();

        services.AddOrionStreamRedisReplayStore("first-host:6379");
        services.AddOrionStreamRedisReplayStore("second-host:6379");

        // Exactly one multiplexer registration survives: the second call REPLACED the first rather than
        // leaving a stale (and ignored) earlier descriptor behind. Before the fix this kept the first
        // host's multiplexer because it was registered with TryAdd.
        var multiplexers = services.Where(d => d.ServiceType == typeof(IConnectionMultiplexer)).ToList();
        Assert.Single(multiplexers);

        // And exactly one factory: the second call's RemoveAll + AddSingleton leaves a single Redis
        // factory, not a duplicate.
        Assert.Single(services, d => d.ServiceType == typeof(IReplayStoreFactory));
    }

    [Fact]
    public void A_caller_registered_multiplexer_is_left_untouched_by_the_connection_string_overload()
    {
        var services = new ServiceCollection();

        // The caller owns the multiplexer (a singleton instance they registered). The connection-string
        // overload must NOT clobber it - only the no-arg overload is meant to reuse a caller multiplexer,
        // but the replace logic must scope itself to package-owned registrations regardless.
        var callerMux = new FakeMultiplexer();
        services.AddSingleton<IConnectionMultiplexer>(callerMux);

        services.AddOrionStreamRedisReplayStore("some-host:6379");

        // The caller's instance is still registered (a package-owned factory was ADDED alongside, but the
        // caller's instance descriptor was not removed).
        Assert.Contains(services, d =>
            d.ServiceType == typeof(IConnectionMultiplexer) &&
            ReferenceEquals(d.ImplementationInstance, callerMux));
    }

    [Fact]
    public void No_connection_string_overload_does_not_register_a_multiplexer()
    {
        var services = new ServiceCollection();

        services.AddOrionStreamRedisReplayStore(configure: null);

        // The no-arg overload only swaps the factory; the caller is responsible for the multiplexer.
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IConnectionMultiplexer));
        Assert.Single(services, d => d.ServiceType == typeof(IReplayStoreFactory));
    }

    [Fact]
    public void Invalid_options_are_rejected_at_registration_time()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() =>
            services.AddOrionStreamRedisReplayStore("host:6379", o => o.Database = -2));
    }

    // A no-op IConnectionMultiplexer stand-in: registration tests only inspect the descriptor, never call
    // any member, so throwing everywhere is acceptable and keeps the fake tiny.
    private sealed class FakeMultiplexer : IConnectionMultiplexer
    {
        public string ClientName => throw new NotSupportedException();
        public string Configuration => throw new NotSupportedException();
        public int TimeoutMilliseconds => throw new NotSupportedException();
        public long OperationCount => throw new NotSupportedException();
        public bool PreserveAsyncOrder { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public bool IsConnected => throw new NotSupportedException();
        public bool IsConnecting => throw new NotSupportedException();
        public bool IncludeDetailInExceptions { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public int StormLogThreshold { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public event EventHandler<RedisErrorEventArgs> ErrorMessage { add { } remove { } }
        public event EventHandler<ConnectionFailedEventArgs> ConnectionFailed { add { } remove { } }
        public event EventHandler<InternalErrorEventArgs> InternalError { add { } remove { } }
        public event EventHandler<ConnectionFailedEventArgs> ConnectionRestored { add { } remove { } }
        public event EventHandler<EndPointEventArgs> ConfigurationChanged { add { } remove { } }
        public event EventHandler<EndPointEventArgs> ConfigurationChangedBroadcast { add { } remove { } }
        public event EventHandler<StackExchange.Redis.Maintenance.ServerMaintenanceEvent> ServerMaintenanceEvent { add { } remove { } }
        public event EventHandler<HashSlotMovedEventArgs> HashSlotMoved { add { } remove { } }

        public void Close(bool allowCommandsToComplete = true) => throw new NotSupportedException();
        public Task CloseAsync(bool allowCommandsToComplete = true) => throw new NotSupportedException();
        public bool Configure(System.IO.TextWriter? log = null) => throw new NotSupportedException();
        public Task<bool> ConfigureAsync(System.IO.TextWriter? log = null) => throw new NotSupportedException();
        public void Dispose() { }

        // IConnectionMultiplexer redeclares ToString() as non-nullable, so the inherited object.ToString()
        // (string?) does not satisfy it. The descriptor tests never call it; return a constant non-null
        // string to match the interface's nullability.
        public override string ToString() => nameof(FakeMultiplexer);
        public ValueTask DisposeAsync() => throw new NotSupportedException();
        public void ExportConfiguration(System.IO.Stream destination, ExportOptions options = (ExportOptions)(-1)) => throw new NotSupportedException();
        public ServerCounters GetCounters() => throw new NotSupportedException();
        public IDatabase GetDatabase(int db = -1, object? asyncState = null) => throw new NotSupportedException();
        public System.Net.EndPoint[] GetEndPoints(bool configuredOnly = false) => throw new NotSupportedException();
        public int GetHashSlot(RedisKey key) => throw new NotSupportedException();
        public IServer GetServer(string host, int port, object? asyncState = null) => throw new NotSupportedException();
        public IServer GetServer(string hostAndPort, object? asyncState = null) => throw new NotSupportedException();
        public IServer GetServer(System.Net.IPAddress host, int port) => throw new NotSupportedException();
        public IServer GetServer(System.Net.EndPoint endpoint, object? asyncState = null) => throw new NotSupportedException();
        public IServer[] GetServers() => throw new NotSupportedException();
        public string GetStatus() => throw new NotSupportedException();
        public void GetStatus(System.IO.TextWriter log) => throw new NotSupportedException();
        public string? GetStormLog() => throw new NotSupportedException();
        public ISubscriber GetSubscriber(object? asyncState = null) => throw new NotSupportedException();
        public int HashSlot(RedisKey key) => throw new NotSupportedException();
        public long PublishReconfigure(CommandFlags flags = CommandFlags.None) => throw new NotSupportedException();
        public Task<long> PublishReconfigureAsync(CommandFlags flags = CommandFlags.None) => throw new NotSupportedException();
        public void RegisterProfiler(Func<StackExchange.Redis.Profiling.ProfilingSession?> profilingSessionProvider) => throw new NotSupportedException();
        public void ResetStormLog() => throw new NotSupportedException();
        public void Wait(Task task) => throw new NotSupportedException();
        public T Wait<T>(Task<T> task) => throw new NotSupportedException();
        public void WaitAll(params Task[] tasks) => throw new NotSupportedException();
        public void AddLibraryNameSuffix(string suffix) => throw new NotSupportedException();
    }
}
