namespace Moongazing.OrionStream.Tests;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

/// <summary>
/// Topic lifecycle and concurrency: idle topics with no replay backlog are reclaimed, topics that
/// still hold backlog survive a zero-subscriber gap, and a subscribe racing an unsubscribe is never
/// orphaned onto a removed topic.
/// </summary>
public sealed class SseHubLifecycleTests
{
    private static SseHub NewHub(StreamDiagnostics diagnostics, int replayCapacity) =>
        new(new StreamOptions { SubscriberCapacity = 256, ReplayBufferCapacity = replayCapacity }, diagnostics);

    private static ServerSentEvent Event(string data) => new() { Data = data };

    [Fact]
    public void An_idle_topic_with_no_backlog_is_reclaimed_when_its_last_subscriber_leaves()
    {
        using var diag = new StreamDiagnostics();
        // Replay enabled, but this topic never publishes, so its replay buffer stays empty and
        // carries no resume value: the topic must not leak after the last subscriber disconnects.
        var hub = NewHub(diag, replayCapacity: 256);

        var sub = hub.Subscribe("ephemeral");
        Assert.Equal(1, hub.TopicCount);

        sub.Dispose();

        Assert.Equal(0, hub.TopicCount);
        Assert.Equal(0, hub.SubscriberCount("ephemeral"));
    }

    [Fact]
    public void Many_distinct_short_lived_topics_with_no_backlog_do_not_leak()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 256);

        for (var i = 0; i < 1000; i++)
        {
            var sub = hub.Subscribe("user-" + i.ToString(CultureInfo.InvariantCulture));
            sub.Dispose();
        }

        Assert.Equal(0, hub.TopicCount);
    }

    [Fact]
    public void A_topic_holding_replay_backlog_survives_a_zero_subscriber_gap()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 256);

        var sub = hub.Subscribe("orders");
        hub.Publish("orders", Event("a")); // id 1, retained for replay
        sub.Dispose();

        // The topic is kept because its buffer holds an event a reconnecting client could resume
        // from. Resuming from id 1 finds the topic alive and replays nothing newer.
        Assert.Equal(1, hub.TopicCount);

        using var resumed = hub.Subscribe("orders", lastEventId: "1");
        Assert.False(resumed.Reader.TryRead(out _));

        hub.Publish("orders", Event("b")); // id 2, live
        Assert.True(resumed.Reader.TryRead(out var evt));
        Assert.Equal("b", evt!.Data);
    }

    [Fact]
    public void With_replay_disabled_an_idle_topic_is_reclaimed()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 0);

        var sub = hub.Subscribe("orders");
        Assert.Equal(1, hub.TopicCount);

        sub.Dispose();
        Assert.Equal(0, hub.TopicCount);
    }

    [Fact]
    public async Task A_subscribe_racing_an_unsubscribe_is_never_orphaned()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 256);

        // Hammer subscribe/unsubscribe on the same topic from many threads. If a topic removal could
        // race a registration, a late subscriber would attach to a detached Topic instance: a
        // publish would report it as delivered against a topic the hub no longer exposes, so the
        // SubscriberCount the hub reports would drift below the channels that actually received the
        // event. We assert the hub stays internally consistent and the subscriber gauge returns to 0.
        const int Workers = 16;
        const int Iterations = 500;

        var tasks = new List<Task>(Workers);
        for (var w = 0; w < Workers; w++)
        {
            tasks.Add(Task.Run(() =>
            {
                for (var i = 0; i < Iterations; i++)
                {
                    var sub = hub.Subscribe("contended");
                    // A publish interleaving with another thread's unsubscribe must never throw and
                    // must only ever count subscribers the hub still tracks.
                    var delivered = hub.Publish("contended", Event("x"));
                    Assert.True(delivered >= 0);
                    Assert.True(delivered <= Workers);
                    sub.Dispose();
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Every subscription was disposed, so no topic should retain a live subscriber, and the
        // global subscriber gauge must be back to zero (no orphaned, uncounted subscribers).
        Assert.Equal(0, hub.SubscriberCount("contended"));

        using var verify = hub.Subscribe("contended");
        Assert.Equal(1, hub.SubscriberCount("contended"));
    }

    [Fact]
    public async Task A_concurrent_subscribe_during_the_last_unsubscribe_still_receives_live_events()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 0); // no backlog, so removal-on-empty is in play

        // Repeatedly drive the last-subscriber-leaves removal concurrently with a fresh subscribe,
        // then confirm the fresh subscriber actually gets a subsequently published event. A subscribe
        // that attached to a removed topic would silently miss the publish.
        for (var round = 0; round < 200; round++)
        {
            var leaving = hub.Subscribe("topic");

            StreamSubscription? joining = null;
            var join = Task.Run(() => joining = hub.Subscribe("topic"));
            leaving.Dispose();
            await join;

            using (joining!)
            {
                var delivered = hub.Publish("topic", Event("live"));
                Assert.Equal(hub.SubscriberCount("topic"), delivered);
                Assert.True(joining!.Reader.TryRead(out var evt));
                Assert.Equal("live", evt!.Data);
            }
        }
    }
}
