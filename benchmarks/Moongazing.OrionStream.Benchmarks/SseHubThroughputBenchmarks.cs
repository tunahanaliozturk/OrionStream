namespace Moongazing.OrionStream.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Measures end-to-end single-subscriber throughput: publishing a stream of events into the hub and
/// draining them through the subscription reader, exercising the bounded-channel write/read path.
/// Two variants contrast a reader that keeps up (capacity holds the burst) with one that falls
/// behind (capacity smaller than the burst, so the <see cref="System.Threading.Channels.BoundedChannelFullMode.DropOldest"/>
/// back-pressure path runs on every excess publish).
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class SseHubThroughputBenchmarks
{
    private const string Topic = "orders";

    private StreamDiagnostics diagnostics = null!;
    private ServerSentEvent evt = null!;

    /// <summary>Number of events published per benchmark invocation.</summary>
    [Params(1_000, 10_000, 100_000)]
    public int Messages { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        diagnostics = new StreamDiagnostics();
        evt = new ServerSentEvent
        {
            Id = "1",
            EventName = "order.created",
            Data = "{\"id\":1,\"status\":\"created\"}",
        };
    }

    [GlobalCleanup]
    public void Cleanup() => diagnostics.Dispose();

    /// <summary>
    /// The reader keeps up: the buffer is large enough to hold the whole burst, so every event is
    /// delivered and drained with no drops.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int PublishAndDrain_ReaderKeepsUp()
    {
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = Messages }, diagnostics);
        using var subscription = hub.Subscribe(Topic);
        var reader = subscription.Reader;

        for (var i = 0; i < Messages; i++)
        {
            hub.Publish(Topic, evt);
        }

        var drained = 0;
        while (reader.TryRead(out _))
        {
            drained++;
        }

        return drained;
    }

    /// <summary>
    /// The reader falls behind: a small buffer forces the DropOldest back-pressure path on every
    /// publish past capacity, the case a slow client triggers in production.
    /// </summary>
    [Benchmark]
    public int PublishWithBackpressure()
    {
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 256 }, diagnostics);
        using var subscription = hub.Subscribe(Topic);

        var delivered = 0;
        for (var i = 0; i < Messages; i++)
        {
            delivered += hub.Publish(Topic, evt);
        }

        return delivered;
    }
}
