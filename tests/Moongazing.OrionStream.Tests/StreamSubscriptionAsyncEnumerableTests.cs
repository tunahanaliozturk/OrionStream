namespace Moongazing.OrionStream.Tests;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class StreamSubscriptionAsyncEnumerableTests
{
    private sealed record Tick(int N);

    private static readonly string[] ExpectedAbc = { "a", "b", "c" };
    private static readonly int[] ExpectedOneTwo = { 1, 2 };
    private static readonly int[] ExpectedOneTwoThree = { 1, 2, 3 };

    [Fact]
    public async Task Read_all_async_yields_published_events_in_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "a" });
        hub.Publish("orders", new ServerSentEvent { Data = "b" });
        hub.Publish("orders", new ServerSentEvent { Data = "c" });

        var seen = new List<string>();
        await foreach (var evt in sub.ReadAllAsync())
        {
            seen.Add(evt.Data);
            if (seen.Count == 3)
            {
                break; // all expected events drained; stop the otherwise-open stream
            }
        }

        Assert.Equal(ExpectedAbc, seen);
    }

    [Fact]
    public async Task Typed_read_all_async_deserializes_each_payload_in_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("ticks");

        hub.Publish("ticks", new Tick(1));
        hub.Publish("ticks", new Tick(2));

        var seen = new List<int>();
        await foreach (var tick in sub.ReadAllAsync<Tick>())
        {
            seen.Add(tick!.N);
            if (seen.Count == 2)
            {
                break;
            }
        }

        Assert.Equal(ExpectedOneTwo, seen);
    }

    [Fact]
    public async Task Read_all_async_completes_when_cancellation_is_requested()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        using var cts = new CancellationTokenSource();

        var consumed = new List<string>();
        var consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var evt in sub.ReadAllAsync(cts.Token))
                {
                    consumed.Add(evt.Data);
                }
            }
            catch (OperationCanceledException)
            {
                // Cancelling an in-flight WaitToReadAsync surfaces as cancellation; that is the
                // expected completion path, not a failure.
            }
        });

        hub.Publish("orders", new ServerSentEvent { Data = "a" });

        // Wait deterministically for the first event to be consumed, then cancel.
        for (var i = 0; i < 200 && consumed.Count == 0; i++)
        {
            await Task.Delay(10);
        }

        await cts.CancelAsync();
        await consumer; // completes rather than hanging

        Assert.Contains("a", consumed);
    }

    [Fact]
    public async Task Publish_all_async_drains_an_async_stream_into_a_topic_in_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("ticks");

        static async IAsyncEnumerable<Tick> Source()
        {
            for (var n = 1; n <= 3; n++)
            {
                await Task.Yield();
                yield return new Tick(n);
            }
        }

        var published = await hub.PublishAllAsync("ticks", Source());

        Assert.Equal(3, published);

        var seen = new List<int>();
        await foreach (var tick in sub.ReadAllAsync<Tick>())
        {
            seen.Add(tick!.N);
            if (seen.Count == 3)
            {
                break;
            }
        }

        Assert.Equal(ExpectedOneTwoThree, seen);
    }
}
