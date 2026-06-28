namespace Moongazing.OrionStream.Redis.Tests;

using System.Diagnostics;

using StackExchange.Redis;

using Testcontainers.Redis;

/// <summary>
/// One real Redis container (Testcontainers) shared by every test in the suite. A single shared
/// container is faster and far less exposed to transient Docker-daemon blips than a fresh container per
/// test; tests stay isolated from each other because each uses a unique topic name, not container
/// isolation.
/// </summary>
/// <remarks>
/// CI resilience: the first <c>ConnectionMultiplexer.ConnectAsync</c> after the container starts can
/// blip (slow first handshake) and the Docker daemon can transiently fault the start under load, either
/// of which would otherwise fail the whole class. The start and the connect+PING warm-up are retried
/// with exponential backoff under a generous budget; see <see cref="RedisContainerStartup"/>.
/// </remarks>
public sealed class RedisContainerFixture : IAsyncLifetime
{
    private readonly RedisContainer container = new RedisBuilder().Build();

    /// <summary>The connection multiplexer to the running Redis, valid for the fixture's lifetime.</summary>
    public IConnectionMultiplexer Mux { get; private set; } = default!;

    /// <summary>The container connection string, for opening a SECOND independent multiplexer that points at the same Redis.</summary>
    public string ConnectionString { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        Mux = await RedisContainerStartup.StartAndConnectAsync(container).ConfigureAwait(false);
        ConnectionString = container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        await Mux.DisposeAsync().ConfigureAwait(false);
        await container.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Starts a Redis container and opens a verified <see cref="IConnectionMultiplexer"/>, retrying both the
/// container start and the connect+PING warm-up with exponential backoff under an overall budget. This
/// is the single biggest lever against a flaky CI suite: a slow first connection on a loaded runner is
/// retried rather than failing an entire Redis test class.
/// </summary>
internal static class RedisContainerStartup
{
    public static async Task<IConnectionMultiplexer> StartAndConnectAsync(RedisContainer container)
    {
        ArgumentNullException.ThrowIfNull(container);

        var budget = TimeSpan.FromMinutes(2);
        var sw = Stopwatch.StartNew();
        var delay = TimeSpan.FromSeconds(1);
        var maxDelay = TimeSpan.FromSeconds(10);
        var attempt = 0;
        Exception? last = null;

        while (sw.Elapsed < budget)
        {
            attempt++;
            using var cts = new CancellationTokenSource(budget - sw.Elapsed);
            ConnectionMultiplexer? mux = null;
            try
            {
                await container.StartAsync(cts.Token).ConfigureAwait(false);
                mux = await ConnectionMultiplexer.ConnectAsync(container.GetConnectionString()).ConfigureAwait(false);
                await mux.GetDatabase().PingAsync().ConfigureAwait(false);
                return mux;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not StackOverflowException)
            {
                last = ex;
                if (mux is not null)
                {
                    await mux.DisposeAsync().ConfigureAwait(false);
                }

                var remaining = budget - sw.Elapsed;
                if (remaining <= TimeSpan.Zero)
                {
                    break;
                }

                var wait = delay < remaining ? delay : remaining;
                await Task.Delay(wait).ConfigureAwait(false);
                delay = delay + delay < maxDelay ? delay + delay : maxDelay;
            }
        }

        throw new InvalidOperationException(
            $"Redis container '{container.Name}' did not become connectable within {budget.TotalSeconds:N0}s after {attempt} attempt(s).",
            last);
    }
}
