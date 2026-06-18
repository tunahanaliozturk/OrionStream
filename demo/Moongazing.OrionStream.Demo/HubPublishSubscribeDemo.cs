namespace Moongazing.OrionStream.Demo;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Demonstrates <see cref="SseHub"/> publish/subscribe entirely in memory: subscribe to a topic,
/// publish a few events, then drain the subscriber's channel and print what arrived. Also shows
/// that publishing to a topic with no subscribers is a no-op that returns zero.
/// </summary>
internal static class HubPublishSubscribeDemo
{
    public static void Run()
    {
        DemoConsole.Header("2. SseHub: in-memory publish / subscribe");

        using var diagnostics = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 16 }, diagnostics);

        DemoConsole.Step("Publish before anyone subscribes -> delivered to 0 subscribers:");
        var beforeAnyone = hub.Publish("orders", new ServerSentEvent { Data = "ignored" });
        DemoConsole.Detail($"Publish returned {beforeAnyone}; SubscriberCount('orders') = {hub.SubscriberCount("orders")}");

        DemoConsole.Step("Subscribe to 'orders', then publish three events:");
        using var subscription = hub.Subscribe("orders");
        DemoConsole.Detail($"SubscriberCount('orders') = {hub.SubscriberCount("orders")}");

        for (var i = 1; i <= 3; i++)
        {
            var delivered = hub.Publish("orders", new ServerSentEvent
            {
                Id = i.ToString(),
                EventName = "order.created",
                Data = $"order #{i}",
            });
            DemoConsole.Detail($"Published order #{i} -> delivered to {delivered} subscriber(s)");
        }

        DemoConsole.Step("Drain the subscriber's reader (the same order they were published):");
        while (subscription.Reader.TryRead(out var evt))
        {
            DemoConsole.Detail($"received id={evt!.Id} event={evt.EventName} data='{evt.Data}'");
        }
    }
}
