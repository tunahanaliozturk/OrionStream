namespace Moongazing.OrionStream.Demo;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Demonstrates Last-Event-ID resume entirely in memory. A first subscriber reads a few events and
/// remembers the wire id of the last one it saw; it then drops off. After more events are published,
/// a second subscriber resumes with that id and receives only the events published after it, with no
/// gap and no duplicate. A resume against an unknown or evicted id falls back to a from-now stream.
/// Producer-supplied ids are used here so the wire id is observable from the consumer side; when a
/// producer leaves <see cref="ServerSentEvent.Id"/> null the hub stamps a topic-monotonic sequence
/// instead and resume works the same way against that sequence.
/// </summary>
internal static class ResumeDemo
{
    public static void Run()
    {
        DemoConsole.Header("5. Last-Event-ID resume: reconnect without a gap");

        using var diagnostics = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions { SubscriberCapacity = 16, ReplayBufferCapacity = 8 },
            diagnostics);

        DemoConsole.Step("A client subscribes to 'orders' and reads the first two events:");
        string? lastSeenId;
        using (var firstConnection = hub.Subscribe("orders"))
        {
            Publish(hub, 1);
            Publish(hub, 2);

            lastSeenId = DrainAndReportLastId(firstConnection, "first connection");
            DemoConsole.Detail($"client remembers Last-Event-ID = {lastSeenId}");
        }

        DemoConsole.Step("The client is gone. Three more events are published to the topic:");
        Publish(hub, 3);
        Publish(hub, 4);
        Publish(hub, 5);
        DemoConsole.Detail("published 3, 4, 5 while no one was connected (retained in the replay buffer)");

        DemoConsole.Step($"The client reconnects with Last-Event-ID = {lastSeenId} -> replays only 3, 4, 5:");
        using (var resumed = hub.Subscribe("orders", lastSeenId))
        {
            DrainAndReportLastId(resumed, "resumed connection");
        }

        DemoConsole.Step("A reconnect with an unknown id falls back to a from-now stream (no replay):");
        using (var fromNow = hub.Subscribe("orders", "does-not-exist"))
        {
            DemoConsole.Detail($"buffered immediately after resume: {fromNow.Reader.Count} (expected 0)");
            Publish(hub, 6);
            DrainAndReportLastId(fromNow, "from-now connection");
        }
    }

    private static void Publish(SseHub hub, int n) =>
        hub.Publish("orders", new ServerSentEvent
        {
            Id = n.ToString(),
            EventName = "order.created",
            Data = $"order #{n}",
        });

    private static string? DrainAndReportLastId(StreamSubscription subscription, string label)
    {
        var received = new List<string>();
        string? lastId = null;
        while (subscription.Reader.TryRead(out var evt))
        {
            received.Add(evt!.Data);
            lastId = evt.Id;
        }

        DemoConsole.Detail($"{label} received: [{string.Join(", ", received)}]");
        return lastId;
    }
}
