namespace Moongazing.OrionStream.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Measures the subscribe/unsubscribe churn path (<see cref="SseHub.Subscribe"/> followed by
/// <see cref="StreamSubscription.Dispose"/>): bounded-channel allocation, the per-topic concurrent
/// dictionary insert and removal, and the lazy topic teardown when the last subscriber leaves. This
/// is the connect/disconnect cost paid once per client lifetime.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class SseHubSubscriptionBenchmarks
{
    private const string Topic = "orders";

    private StreamDiagnostics diagnostics = null!;
    private SseHub hub = null!;

    /// <summary>Number of subscribe/dispose cycles performed per invocation.</summary>
    [Params(100, 1000)]
    public int Churn { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        diagnostics = new StreamDiagnostics();
        hub = new SseHub(new StreamOptions { SubscriberCapacity = 256 }, diagnostics);
    }

    [GlobalCleanup]
    public void Cleanup() => diagnostics.Dispose();

    /// <summary>
    /// Subscribe then immediately dispose, so each cycle creates and tears down a topic with a
    /// single subscriber (worst case for the lazy topic add/remove bookkeeping).
    /// </summary>
    [Benchmark]
    public void SubscribeDisposeSingle()
    {
        for (var i = 0; i < Churn; i++)
        {
            var subscription = hub.Subscribe(Topic);
            subscription.Dispose();
        }
    }

    /// <summary>
    /// Open all subscriptions first, then dispose them all, so the topic stays alive across the
    /// batch and only the final disposal triggers topic teardown.
    /// </summary>
    [Benchmark]
    public void SubscribeAllThenDisposeAll()
    {
        var subscriptions = new StreamSubscription[Churn];
        for (var i = 0; i < Churn; i++)
        {
            subscriptions[i] = hub.Subscribe(Topic);
        }

        for (var i = 0; i < Churn; i++)
        {
            subscriptions[i].Dispose();
        }
    }
}
