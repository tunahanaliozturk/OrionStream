namespace Moongazing.OrionStream.Tests;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseHubResumeTests
{
    private static SseHub NewHub(StreamDiagnostics diagnostics, int capacity = 256, int replayCapacity = 256) =>
        new(new StreamOptions { SubscriberCapacity = capacity, ReplayBufferCapacity = replayCapacity }, diagnostics);

    private static ServerSentEvent Event(string data) => new() { Data = data };

    private static List<ServerSentEvent> Drain(StreamSubscription sub)
    {
        var events = new List<ServerSentEvent>();
        while (sub.Reader.TryRead(out var evt))
        {
            events.Add(evt!);
        }
        return events;
    }

    // Comma-join the projected values so the expected/actual comparison is a single string and does
    // not carry an inline array argument (which CA1861 would flag under warnings-as-errors).
    private static string Ids(IEnumerable<ServerSentEvent> events) =>
        string.Join(",", events.Select(e => e.EffectiveId));

    private static string Datas(IEnumerable<ServerSentEvent> events) =>
        string.Join(",", events.Select(e => e.Data));

    [Fact]
    public void Published_events_are_stamped_with_incrementing_ids()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", Event("a"));
        hub.Publish("orders", Event("b"));
        hub.Publish("orders", Event("c"));

        Assert.Equal("1,2,3", Ids(Drain(sub)));
    }

    [Fact]
    public void Ids_are_monotonic_per_topic_independently()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var orders = hub.Subscribe("orders");
        using var invoices = hub.Subscribe("invoices");

        hub.Publish("orders", Event("o1"));
        hub.Publish("invoices", Event("i1"));
        hub.Publish("orders", Event("o2"));

        Assert.Equal("1,2", Ids(Drain(orders)));
        Assert.Equal("1", Ids(Drain(invoices)));
    }

    [Fact]
    public void A_producer_supplied_id_is_not_overwritten()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "x", Id = "custom" });

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Equal("custom", evt!.Id);
        // The producer id wins on the wire even though the hub also assigned a sequence id.
        Assert.Equal("custom", evt.EffectiveId);
        Assert.Equal(1, evt.SequenceId);
    }

    [Fact]
    public void Resuming_from_a_known_id_replays_only_subsequent_events()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        // Publish a backlog with no live subscriber; it lands in the replay buffer.
        hub.Publish("orders", Event("a")); // id 1
        hub.Publish("orders", Event("b")); // id 2
        hub.Publish("orders", Event("c")); // id 3

        using var resumed = hub.Subscribe("orders", lastEventId: "1");

        var replayed = Drain(resumed);
        Assert.Equal("b,c", Datas(replayed));
        Assert.Equal("2,3", Ids(replayed));
    }

    [Fact]
    public void Resuming_from_the_latest_id_replays_nothing()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        hub.Publish("orders", Event("a"));
        hub.Publish("orders", Event("b")); // id 2 is the latest

        using var resumed = hub.Subscribe("orders", lastEventId: "2");

        Assert.Empty(Drain(resumed));
    }

    [Fact]
    public void Resuming_then_receiving_live_events_is_seamless()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        hub.Publish("orders", Event("a")); // id 1
        hub.Publish("orders", Event("b")); // id 2

        using var resumed = hub.Subscribe("orders", lastEventId: "1");
        hub.Publish("orders", Event("c")); // id 3, live

        var events = Drain(resumed);
        Assert.Equal("b,c", Datas(events));
        Assert.Equal("2,3", Ids(events));
    }

    [Fact]
    public void An_unknown_id_falls_back_to_a_from_now_stream()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        hub.Publish("orders", Event("a"));
        hub.Publish("orders", Event("b"));

        // "not-a-number" cannot be parsed as a hub-assigned id: from-now, no replay.
        using var resumed = hub.Subscribe("orders", lastEventId: "not-a-number");
        Assert.Empty(Drain(resumed));

        hub.Publish("orders", Event("c"));
        Assert.Equal("c", Datas(Drain(resumed)));
    }

    [Fact]
    public void An_evicted_id_falls_back_to_a_from_now_stream()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 2);

        hub.Publish("orders", Event("a")); // id 1, evicted below
        hub.Publish("orders", Event("b")); // id 2, evicted below
        hub.Publish("orders", Event("c")); // id 3
        hub.Publish("orders", Event("d")); // id 4; buffer now holds 3,4 (oldest retained is 3)

        // The client last saw id 1. Id 2 was published after it but has since been evicted, so
        // replaying "what remains" (3,4) would leave a gap the client never sees. Id 1 is older than
        // oldestRetained(3) - 1, so the documented contract is from-now: no replay.
        using var resumed = hub.Subscribe("orders", lastEventId: "1");
        Assert.Empty(Drain(resumed));

        hub.Publish("orders", Event("e")); // id 5, live
        Assert.Equal("e", Datas(Drain(resumed)));
    }

    [Fact]
    public void Resuming_from_the_oldest_retained_id_replays_the_rest_of_the_buffer()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 2);

        hub.Publish("orders", Event("a")); // id 1, evicted below
        hub.Publish("orders", Event("b")); // id 2 (oldest retained)
        hub.Publish("orders", Event("c")); // id 3

        // id 2 is the oldest entry the buffer still holds, so its wire id matches and everything
        // after it (3) replays exactly, with no gap.
        using var resumed = hub.Subscribe("orders", lastEventId: "2");
        Assert.Equal("c", Datas(Drain(resumed)));
    }

    [Fact]
    public void A_null_last_event_id_starts_from_now()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        hub.Publish("orders", Event("a"));

        using var fresh = hub.Subscribe("orders", lastEventId: null);
        Assert.Empty(Drain(fresh));
    }

    [Fact]
    public void The_replay_buffer_respects_its_capacity_bound()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 3);

        for (var i = 1; i <= 10; i++)
        {
            hub.Publish("orders", Event(i.ToString(CultureInfo.InvariantCulture)));
        }

        // Only the newest 3 (ids 8,9,10) survive. Resuming from id 8 (the oldest retained id) matches
        // its wire id and replays everything after it (9,10). An evicted id such as "7" or "0" would
        // instead fall back to from-now (covered separately).
        using var resumed = hub.Subscribe("orders", lastEventId: "8");

        var replayed = Drain(resumed);
        Assert.Equal(2, replayed.Count);
        Assert.Equal("9,10", Datas(replayed));
        Assert.Equal("9,10", Ids(replayed));
    }

    [Fact]
    public void A_zero_capacity_replay_buffer_disables_replay_but_still_stamps_ids()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 0);

        // A live subscriber keeps the topic (and its id sequence) alive across publishes.
        using var sub = hub.Subscribe("orders");
        hub.Publish("orders", Event("a")); // id 1, delivered but not retained for replay
        hub.Publish("orders", Event("b")); // id 2
        Assert.Equal("1,2", Ids(Drain(sub)));

        // Resuming finds nothing buffered: replay is disabled, so it is a from-now stream.
        using var resumed = hub.Subscribe("orders", lastEventId: "1");
        Assert.Empty(Drain(resumed));

        hub.Publish("orders", Event("c")); // id 3, live to both subscribers
        Assert.True(resumed.Reader.TryRead(out var evt));
        Assert.Equal("3", evt!.EffectiveId);
    }

    [Fact]
    public void A_hub_assigned_sequence_id_renders_as_the_wire_id_line()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", Event("payload"));

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Equal("id: 1\ndata: payload\n\n", SseFormatter.Format(evt!));
    }

    [Fact]
    public void A_producer_id_takes_precedence_over_the_sequence_id_on_the_wire()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "payload", Id = "custom" });

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Equal("id: custom\ndata: payload\n\n", SseFormatter.Format(evt!));
    }

    [Fact]
    public void Replay_respects_the_subscriber_buffer_bound()
    {
        using var diag = new StreamDiagnostics();
        // Replay holds 5 but each subscriber can buffer only 2; the oldest replayed events drop.
        var hub = NewHub(diag, capacity: 2, replayCapacity: 5);

        for (var i = 1; i <= 5; i++)
        {
            hub.Publish("orders", Event(i.ToString(CultureInfo.InvariantCulture)));
        }

        // Resume from id 1 (still buffered): everything after it (2,3,4,5) is replayed, but the
        // subscriber buffer of 2 with DropOldest keeps only the newest 2.
        using var resumed = hub.Subscribe("orders", lastEventId: "1");

        // DropOldest keeps the newest 2 of the 4 replayed events.
        Assert.Equal("4,5", Datas(Drain(resumed)));
    }

    [Fact]
    public void Resuming_from_a_producer_supplied_custom_id_replays_only_subsequent_events()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        // Each event carries its own producer-supplied wire id; the browser would reconnect with the
        // exact custom id it last saw, not the hub sequence.
        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "evt-a" });
        hub.Publish("orders", new ServerSentEvent { Data = "b", Id = "evt-b" });
        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "evt-c" });

        using var resumed = hub.Subscribe("orders", lastEventId: "evt-a");

        var replayed = Drain(resumed);
        // The custom id round-trips: replay starts AFTER evt-a, not from-now and not from nothing.
        Assert.Equal("b,c", Datas(replayed));
        Assert.Equal("evt-b,evt-c", Ids(replayed));
    }

    [Fact]
    public void Resuming_from_a_producer_id_then_receiving_live_events_is_seamless()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "evt-a" });
        hub.Publish("orders", new ServerSentEvent { Data = "b", Id = "evt-b" });

        using var resumed = hub.Subscribe("orders", lastEventId: "evt-a");
        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "evt-c" }); // live

        var events = Drain(resumed);
        Assert.Equal("b,c", Datas(events));
        Assert.Equal("evt-b,evt-c", Ids(events));
    }

    [Fact]
    public void An_unknown_producer_custom_id_falls_back_to_a_from_now_stream()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "evt-a" });
        hub.Publish("orders", new ServerSentEvent { Data = "b", Id = "evt-b" });

        // A custom id that no buffered entry ever emitted: from-now, no replay.
        using var resumed = hub.Subscribe("orders", lastEventId: "evt-never");
        Assert.Empty(Drain(resumed));

        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "evt-c" });
        Assert.Equal("c", Datas(Drain(resumed)));
    }

    [Fact]
    public void An_evicted_producer_custom_id_falls_back_to_a_from_now_stream()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 2);

        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "evt-a" }); // evicted below
        hub.Publish("orders", new ServerSentEvent { Data = "b", Id = "evt-b" }); // evicted below
        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "evt-c" });
        hub.Publish("orders", new ServerSentEvent { Data = "d", Id = "evt-d" }); // buffer holds c,d

        // evt-a was published but has since been evicted; replaying what remains (c,d) would leave a
        // gap (b) the client never sees, so the contract is from-now.
        using var resumed = hub.Subscribe("orders", lastEventId: "evt-a");
        Assert.Empty(Drain(resumed));

        hub.Publish("orders", new ServerSentEvent { Data = "e", Id = "evt-e" }); // live
        Assert.Equal("e", Datas(Drain(resumed)));
    }
}
