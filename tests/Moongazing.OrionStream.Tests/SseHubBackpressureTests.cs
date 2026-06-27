namespace Moongazing.OrionStream.Tests;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

/// <summary>
/// v0.4.0 delivery and back-pressure: full-buffer policy (drop-newest / bounded wait), slow-consumer
/// disconnect, per-topic capacity overrides, and per-subscriber filtering. Every test is deterministic
/// and gated on observable state rather than wall-clock timing.
/// </summary>
public sealed class SseHubBackpressureTests
{
    private static ServerSentEvent Event(string data) => new() { Data = data };

    private static List<string> DrainData(StreamSubscription sub)
    {
        var data = new List<string>();
        while (sub.Reader.TryRead(out var evt))
        {
            data.Add(evt!.Data);
        }
        return data;
    }

    private static string N(int i) => i.ToString(CultureInfo.InvariantCulture);

    // --- Full-buffer policy: DropNewest vs DropOldest -----------------------------------------

    [Fact]
    public void DropOldest_keeps_the_newest_events_under_saturation()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions { SubscriberCapacity = 3, FullBufferPolicy = FullBufferPolicy.DropOldest, ReplayBufferCapacity = 0 },
            diag);
        using var sub = hub.Subscribe("orders");

        for (var i = 1; i <= 6; i++)
        {
            hub.Publish("orders", Event(N(i)));
        }

        // Oldest evicted: only the newest three survive.
        Assert.Equal("4,5,6", string.Join(",", DrainData(sub)));
    }

    [Fact]
    public void DropNewest_keeps_the_oldest_events_and_discards_the_newest_under_saturation()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions { SubscriberCapacity = 3, FullBufferPolicy = FullBufferPolicy.DropNewest, ReplayBufferCapacity = 0 },
            diag);
        using var sub = hub.Subscribe("orders");

        for (var i = 1; i <= 6; i++)
        {
            hub.Publish("orders", Event(N(i)));
        }

        // The buffer fills with the first three; every later event is the newest and is dropped.
        Assert.Equal("1,2,3", string.Join(",", DrainData(sub)));
    }

    [Fact]
    public void DropNewest_reports_zero_delivered_for_a_dropped_newest_event()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions { SubscriberCapacity = 1, FullBufferPolicy = FullBufferPolicy.DropNewest, ReplayBufferCapacity = 0 },
            diag);
        using var sub = hub.Subscribe("orders");

        Assert.Equal(1, hub.Publish("orders", Event("first")));  // buffered
        Assert.Equal(0, hub.Publish("orders", Event("second"))); // dropped, not delivered
    }

    // --- Full-buffer policy: bounded Wait ------------------------------------------------------

    [Fact]
    public void Wait_policy_requires_a_cap()
    {
        using var diag = new StreamDiagnostics();
        Assert.Throws<ArgumentException>(() =>
            new SseHub(new StreamOptions { FullBufferPolicy = FullBufferPolicy.Wait }, diag));
    }

    [Fact]
    public async Task Wait_policy_applies_back_pressure_then_proceeds_when_room_appears()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions
            {
                SubscriberCapacity = 1,
                FullBufferPolicy = FullBufferPolicy.Wait,
                // Generous cap: the test does NOT rely on it elapsing. Room appears via the reader
                // draining on a gate, so the publish returns the instant the slot frees, well inside
                // the cap. The cap is only a safety bound that this test never reaches.
                MaxPublishWait = TimeSpan.FromSeconds(30),
                ReplayBufferCapacity = 0,
            },
            diag);
        using var sub = hub.Subscribe("orders");

        // Saturate the single-slot buffer.
        Assert.Equal(1, hub.Publish("orders", Event("first")));

        // This publish must block: the buffer is full and the policy waits for room.
        var publishStarted = new ManualResetEventSlim(false);
        var publishTask = Task.Run(() =>
        {
            publishStarted.Set();
            return hub.Publish("orders", Event("second"));
        });

        Assert.True(publishStarted.Wait(TimeSpan.FromSeconds(5)));
        // The publish is genuinely blocked on back-pressure: it does not complete while the buffer
        // stays full. A short, bounded negative check (the task must NOT have completed yet).
        Assert.False(publishTask.IsCompleted);

        // Free exactly one slot. This is the gate that releases the back-pressured publisher.
        Assert.True(sub.Reader.TryRead(out var first));
        Assert.Equal("first", first!.Data);

        // The publisher now proceeds and delivers the held-back event.
        var delivered = await publishTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, delivered);

        var second = await sub.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("second", second.Data);
    }

    [Fact]
    public async Task Wait_policy_gives_up_after_its_cap_when_no_room_appears()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions
            {
                SubscriberCapacity = 1,
                FullBufferPolicy = FullBufferPolicy.Wait,
                // Small but non-trivial cap: the reader never drains, so the wait must elapse and the
                // publish must proceed (dropping for this subscriber) rather than hang forever.
                MaxPublishWait = TimeSpan.FromMilliseconds(100),
                ReplayBufferCapacity = 0,
            },
            diag);
        using var sub = hub.Subscribe("orders");

        Assert.Equal(1, hub.Publish("orders", Event("first"))); // saturate; never read

        // No reader drains, so the cap elapses and the publish gives up on the subscriber: it
        // completes (does not hang) and reports zero delivered.
        var delivered = await Task.Run(() => hub.Publish("orders", Event("second")))
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(0, delivered);
        // The buffer still holds only the first event; the second was dropped after the cap.
        Assert.True(sub.Reader.TryRead(out var only));
        Assert.Equal("first", only!.Data);
        Assert.False(sub.Reader.TryRead(out _));
    }

    // --- Slow-consumer disconnect --------------------------------------------------------------

    [Fact]
    public void A_subscriber_saturated_past_the_threshold_is_disconnected()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions
            {
                SubscriberCapacity = 1,
                FullBufferPolicy = FullBufferPolicy.DropOldest,
                SlowConsumerPolicy = new SlowConsumerPolicy { MaxConsecutiveFullPublishes = 3 },
                ReplayBufferCapacity = 0,
            },
            diag);
        var sub = hub.Subscribe("orders");

        // First publish fills the single slot but does NOT find it full yet, so the run is still 0.
        hub.Publish("orders", Event("1"));
        Assert.Equal(1, hub.SubscriberCount("orders"));

        // Each of these finds the buffer already full. After three consecutive full publishes the
        // subscriber crosses the threshold and is shed.
        hub.Publish("orders", Event("2")); // full #1
        Assert.Equal(1, hub.SubscriberCount("orders"));
        hub.Publish("orders", Event("3")); // full #2
        Assert.Equal(1, hub.SubscriberCount("orders"));
        hub.Publish("orders", Event("4")); // full #3 -> disconnect

        Assert.Equal(0, hub.SubscriberCount("orders"));

        // The channel was completed on disconnect. Completion is observable once the buffered backlog
        // is drained (a bounded channel completes its reader only after the last buffered item is
        // read), so drain first, then assert the reader has completed and a later publish reaches it
        // not at all.
        while (sub.Reader.TryRead(out _))
        {
        }
        Assert.True(sub.Reader.Completion.IsCompleted);
        Assert.Equal(0, hub.Publish("orders", Event("5")));

        sub.Dispose();
    }

    [Fact]
    public void A_subscriber_that_catches_up_before_the_threshold_is_not_disconnected()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(
            new StreamOptions
            {
                SubscriberCapacity = 1,
                FullBufferPolicy = FullBufferPolicy.DropOldest,
                SlowConsumerPolicy = new SlowConsumerPolicy { MaxConsecutiveFullPublishes = 3 },
                ReplayBufferCapacity = 0,
            },
            diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", Event("1")); // fills slot
        hub.Publish("orders", Event("2")); // full #1
        hub.Publish("orders", Event("3")); // full #2

        // Reader catches up: the next publish finds room, which resets the consecutive-full run.
        Assert.True(sub.Reader.TryRead(out _));
        hub.Publish("orders", Event("4")); // found room -> run resets to 0

        // A fresh run of full publishes must start over; two more do not reach the threshold of 3.
        hub.Publish("orders", Event("5")); // full #1
        hub.Publish("orders", Event("6")); // full #2

        Assert.Equal(1, hub.SubscriberCount("orders"));
        Assert.False(sub.Reader.Completion.IsCompleted);
    }

    // --- Per-topic capacity overrides ----------------------------------------------------------

    [Fact]
    public void A_per_topic_subscriber_capacity_override_gives_that_topic_a_larger_buffer()
    {
        using var diag = new StreamDiagnostics();
        var options = new StreamOptions { SubscriberCapacity = 2, ReplayBufferCapacity = 0 };
        options.ConfigureTopic("busy", o => o.SubscriberCapacity = 5);
        var hub = new SseHub(options, diag);

        using var busy = hub.Subscribe("busy");
        using var normal = hub.Subscribe("normal");

        for (var i = 1; i <= 5; i++)
        {
            hub.Publish("busy", Event(N(i)));
            hub.Publish("normal", Event(N(i)));
        }

        // The overridden topic buffers all five; the default topic keeps only its two newest.
        Assert.Equal("1,2,3,4,5", string.Join(",", DrainData(busy)));
        Assert.Equal("4,5", string.Join(",", DrainData(normal)));
    }

    [Fact]
    public void A_per_topic_replay_capacity_override_retains_more_backlog_for_that_topic()
    {
        using var diag = new StreamDiagnostics();
        var options = new StreamOptions { SubscriberCapacity = 256, ReplayBufferCapacity = 2 };
        options.ConfigureTopic("busy", o => o.ReplayBufferCapacity = 5);
        var hub = new SseHub(options, diag);

        // No live subscriber: events accumulate only in each topic's replay buffer.
        for (var i = 1; i <= 5; i++)
        {
            hub.Publish("busy", Event(N(i)));
            hub.Publish("normal", Event(N(i)));
        }

        // Resume from the very first id. The overridden topic still holds events 2..5; the default
        // topic retained only its newest two, so its id "1" was evicted and it falls back to from-now.
        using var busyResume = hub.Subscribe("busy", "1");
        using var normalResume = hub.Subscribe("normal", "1");

        Assert.Equal("2,3,4,5", string.Join(",", DrainData(busyResume)));
        Assert.Empty(DrainData(normalResume)); // id 1 evicted from the size-2 default buffer
    }

    // --- Per-subscriber filtering --------------------------------------------------------------

    [Fact]
    public void A_per_subscriber_filter_delivers_only_matching_events()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 256, ReplayBufferCapacity = 0 }, diag);

        using var evens = hub.Subscribe("nums", lastEventId: null,
            filter: e => int.Parse(e.Data, CultureInfo.InvariantCulture) % 2 == 0);
        using var all = hub.Subscribe("nums");

        for (var i = 1; i <= 6; i++)
        {
            hub.Publish("nums", Event(N(i)));
        }

        Assert.Equal("2,4,6", string.Join(",", DrainData(evens)));
        Assert.Equal("1,2,3,4,5,6", string.Join(",", DrainData(all)));
    }

    [Fact]
    public void Filtered_out_events_never_enter_the_subscriber_buffer()
    {
        using var diag = new StreamDiagnostics();
        // Capacity 2. If filtered-out events entered the buffer, the two matching events would be
        // pushed out by the six non-matching ones under DropOldest. Because the filter runs BEFORE the
        // buffer, the non-matching events never consume a slot and both matches survive.
        var hub = new SseHub(
            new StreamOptions { SubscriberCapacity = 2, FullBufferPolicy = FullBufferPolicy.DropOldest, ReplayBufferCapacity = 0 },
            diag);

        using var matching = hub.Subscribe("nums", lastEventId: null,
            filter: e => e.Data is "keep-a" or "keep-b");

        hub.Publish("nums", Event("keep-a"));
        for (var i = 0; i < 6; i++)
        {
            hub.Publish("nums", Event("noise"));
        }
        hub.Publish("nums", Event("keep-b"));

        // Both matching events are intact and in order; none of the six noise events ever took a slot.
        Assert.Equal("keep-a,keep-b", string.Join(",", DrainData(matching)));
    }

    [Fact]
    public void A_filtered_subscriber_does_not_count_filtered_out_events_as_delivered()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 256, ReplayBufferCapacity = 0 }, diag);

        using var only5 = hub.Subscribe("nums", lastEventId: null, filter: e => e.Data == "5");

        Assert.Equal(0, hub.Publish("nums", Event("4"))); // rejected by the only subscriber's filter
        Assert.Equal(1, hub.Publish("nums", Event("5"))); // accepted
    }

    [Fact]
    public void A_filter_applies_to_replayed_events_on_resume()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 256, ReplayBufferCapacity = 10 }, diag);

        for (var i = 1; i <= 6; i++)
        {
            hub.Publish("nums", Event(N(i)));
        }

        // Resume from id 1 with a filter: only the even events after id 1 are replayed.
        using var resume = hub.Subscribe("nums", "1",
            filter: e => int.Parse(e.Data, CultureInfo.InvariantCulture) % 2 == 0);

        Assert.Equal("2,4,6", string.Join(",", DrainData(resume)));
    }
}
