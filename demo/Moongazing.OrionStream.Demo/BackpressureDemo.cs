namespace Moongazing.OrionStream.Demo;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Demonstrates the DropOldest back-pressure policy with a deliberately tiny buffer
/// (<c>SubscriberCapacity = 2</c>). A subscriber that has not drained will lose its oldest buffered
/// event when a newer one arrives, so the producer is never blocked by a slow reader.
/// </summary>
internal static class BackpressureDemo
{
    public static void Run()
    {
        DemoConsole.Header("3. Back-pressure: DropOldest with a small buffer");

        using var diagnostics = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 2 }, diagnostics);

        using var subscription = hub.Subscribe("metrics");
        DemoConsole.Step("SubscriberCapacity = 2. Publish 1, 2, 3 WITHOUT draining between publishes.");

        foreach (var n in new[] { "1", "2", "3" })
        {
            hub.Publish("metrics", new ServerSentEvent { Data = n });
            DemoConsole.Detail($"published '{n}' (buffered now: {subscription.Reader.Count})");
        }

        DemoConsole.Step("Buffer held only the two newest; the oldest ('1') was evicted:");
        var received = new List<string>();
        while (subscription.Reader.TryRead(out var evt))
        {
            received.Add(evt!.Data);
        }

        DemoConsole.Detail($"reader saw: [{string.Join(", ", received)}]  (expected: [2, 3])");
    }
}
