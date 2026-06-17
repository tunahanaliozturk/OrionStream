namespace Moongazing.OrionStream.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Measures the broadcast fan-out hot path (<see cref="SseHub.Publish"/>): for a single publish,
/// the cost of iterating every subscriber of a topic and writing the event into each subscriber's
/// bounded channel. Parameterized by subscriber count to show how fan-out scales.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class SseHubPublishBenchmarks
{
    private const string Topic = "orders";

    private StreamDiagnostics diagnostics = null!;
    private SseHub hub = null!;
    private StreamSubscription[] subscriptions = null!;
    private ServerSentEvent evt = null!;

    /// <summary>Number of subscribers the published event fans out to.</summary>
    [Params(1, 10, 100, 1000)]
    public int Subscribers { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        diagnostics = new StreamDiagnostics();
        // Large capacity so the steady-state Publish path never hits the drop branch here; the
        // back-pressure drop path is exercised separately in the throughput benchmarks.
        hub = new SseHub(new StreamOptions { SubscriberCapacity = 4096 }, diagnostics);

        subscriptions = new StreamSubscription[Subscribers];
        for (var i = 0; i < Subscribers; i++)
        {
            subscriptions[i] = hub.Subscribe(Topic);
        }

        evt = new ServerSentEvent
        {
            Id = "1",
            EventName = "order.created",
            Data = "{\"id\":1,\"status\":\"created\"}",
        };
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }

        diagnostics.Dispose();
    }

    [Benchmark]
    public int PublishToTopic() => hub.Publish(Topic, evt);
}
