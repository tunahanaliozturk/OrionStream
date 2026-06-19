namespace Moongazing.OrionStream.Demo;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Demonstrates an async producer and consumer running concurrently against the hub, the way a real
/// SSE endpoint would: one task publishes a bounded number of events while another awaits them off
/// the subscription reader. Everything is bounded (a fixed event count and an overall timeout) so
/// the demo always terminates.
/// </summary>
internal static class ConcurrentStreamDemo
{
    private const int EventCount = 5;

    public static async Task RunAsync()
    {
        DemoConsole.Header("6. Concurrent async publish / drain (bounded, terminates)");

        using var diagnostics = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 64 }, diagnostics);

        // Overall safety timeout so a logic error can never hang the demo.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var token = cts.Token;

        using var subscription = hub.Subscribe("live");
        DemoConsole.Step($"Producer will publish {EventCount} events; consumer awaits them concurrently.");

        var consumer = Task.Run(async () =>
        {
            var seen = 0;
            // Read exactly EventCount events, awaiting asynchronously as they arrive.
            while (seen < EventCount && await subscription.Reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (seen < EventCount && subscription.Reader.TryRead(out var evt))
                {
                    seen++;
                    DemoConsole.Detail($"consumer received: {evt!.Data}");
                }
            }

            return seen;
        }, token);

        var producer = Task.Run(async () =>
        {
            for (var i = 1; i <= EventCount; i++)
            {
                hub.Publish("live", new ServerSentEvent
                {
                    Id = i.ToString(),
                    EventName = "heartbeat.tick",
                    Data = $"tick {i}/{EventCount}",
                });

                // Small delay so producer and consumer genuinely interleave.
                await Task.Delay(20, token).ConfigureAwait(false);
            }
        }, token);

        await producer.ConfigureAwait(false);
        var received = await consumer.ConfigureAwait(false);

        DemoConsole.Step($"Done. Producer published {EventCount}, consumer drained {received}. App terminates cleanly.");
    }
}
