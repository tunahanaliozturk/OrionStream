namespace Moongazing.OrionStream.Demo;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Demonstrates subscriber lifecycle: a topic is tracked on first subscribe and dropped once its
/// last subscriber disposes, disposal is idempotent, and a publish only reaches live subscribers.
/// </summary>
internal static class SubscriberLifecycleDemo
{
    public static void Run()
    {
        DemoConsole.Header("4. Subscriber lifecycle: subscribe / dispose");

        using var diagnostics = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 16 }, diagnostics);

        DemoConsole.Step("Two subscribers join 'news':");
        var first = hub.Subscribe("news");
        var second = hub.Subscribe("news");
        DemoConsole.Detail($"SubscriberCount('news') = {hub.SubscriberCount("news")}");

        var deliveredToBoth = hub.Publish("news", new ServerSentEvent { Data = "breaking" });
        DemoConsole.Detail($"publish reached {deliveredToBoth} subscriber(s)");

        DemoConsole.Step("Dispose the first subscriber, then publish again:");
        first.Dispose();
        DemoConsole.Detail($"SubscriberCount('news') = {hub.SubscriberCount("news")}");
        var deliveredToOne = hub.Publish("news", new ServerSentEvent { Data = "update" });
        DemoConsole.Detail($"publish reached {deliveredToOne} subscriber(s)");

        DemoConsole.Step("Disposal is idempotent (disposing again does not double-count):");
        first.Dispose();
        DemoConsole.Detail($"SubscriberCount('news') = {hub.SubscriberCount("news")}");

        DemoConsole.Step("Dispose the last subscriber -> topic is removed and count returns to 0:");
        second.Dispose();
        DemoConsole.Detail($"SubscriberCount('news') = {hub.SubscriberCount("news")}");
    }
}
