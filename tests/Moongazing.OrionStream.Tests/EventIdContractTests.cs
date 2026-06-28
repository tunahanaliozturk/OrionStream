namespace Moongazing.OrionStream.Tests;

using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

/// <summary>
/// Asserts the documented event-id allocation contract on <see cref="ISseHub"/>: a per-topic,
/// strictly-increasing, gap-free hub sequence; producer id always wins on the wire; and the documented
/// behavior when producer ids and hub sequences mix on one topic.
/// </summary>
public sealed class EventIdContractTests
{
    private static SseHub NewHub(StreamDiagnostics diagnostics, int replayCapacity = 256) =>
        new(new StreamOptions { ReplayBufferCapacity = replayCapacity }, diagnostics);

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

    private static string Ids(IEnumerable<ServerSentEvent> events) =>
        string.Join(",", events.Select(e => e.EffectiveId));

    private static string Datas(IEnumerable<ServerSentEvent> events) =>
        string.Join(",", events.Select(e => e.Data));

    [Fact]
    public void Hub_sequence_starts_at_one_and_increments_by_one_with_no_gaps()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        for (var i = 0; i < 5; i++)
        {
            hub.Publish("orders", Event("e"));
        }

        // The contract: first event is sequence 1, each subsequent +1, no gaps.
        Assert.Equal("1,2,3,4,5", Ids(Drain(sub)));
    }

    [Fact]
    public void Hub_sequence_is_assigned_even_when_a_producer_id_is_present()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        // Setting a producer id must not skip or perturb the sequence: it is still assigned underneath.
        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "custom-a" }); // seq 1, wire custom-a
        hub.Publish("orders", Event("b")); // seq 2, wire 2

        var events = Drain(sub);
        Assert.Equal(1, events[0].SequenceId);
        Assert.Equal("custom-a", events[0].EffectiveId);
        Assert.Equal(2, events[1].SequenceId);
        Assert.Equal("2", events[1].EffectiveId);
    }

    [Fact]
    public void Monotonicity_scope_is_per_topic_and_topics_are_independent()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var orders = hub.Subscribe("orders");
        using var invoices = hub.Subscribe("invoices");

        hub.Publish("orders", Event("o1"));   // orders seq 1
        hub.Publish("invoices", Event("i1")); // invoices seq 1
        hub.Publish("orders", Event("o2"));   // orders seq 2
        hub.Publish("invoices", Event("i2")); // invoices seq 2

        // Each topic has its own independent sequence; both reuse the same values 1,2.
        Assert.Equal("1,2", Ids(Drain(orders)));
        Assert.Equal("1,2", Ids(Drain(invoices)));
    }

    [Fact]
    public void Producer_id_always_wins_on_the_wire()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "x", Id = "producer-chose-this" });

        Assert.True(sub.Reader.TryRead(out var evt));
        // The producer id is the wire id; the sequence is assigned underneath but not what the client sees.
        Assert.Equal("producer-chose-this", evt!.EffectiveId);
        Assert.Equal(1, evt.SequenceId);
        Assert.Equal("id: producer-chose-this\ndata: x\n\n", SseFormatter.Format(evt));
    }

    [Fact]
    public void Delivery_order_equals_publish_order_within_a_topic()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", Event("first"));
        hub.Publish("orders", Event("second"));
        hub.Publish("orders", Event("third"));

        // The ordering guarantee: ascending sequence, equal to publish order.
        var events = Drain(sub);
        Assert.Equal("first,second,third", Datas(events));
        Assert.Equal("1,2,3", Ids(events));
    }

    [Fact]
    public void Mixed_producer_ids_and_hub_sequences_each_round_trip_through_resume()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        // A single topic carries a mixture, interleaved: producer id, hub sequence, producer id, ...
        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "evt-a" }); // seq 1, wire evt-a
        hub.Publish("orders", Event("b"));                                       // seq 2, wire 2
        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "evt-c" }); // seq 3, wire evt-c
        hub.Publish("orders", Event("d"));                                       // seq 4, wire 4

        // Resuming from a producer wire id replays the suffix by sequence, mixing both kinds.
        using (var fromProducerId = hub.Subscribe("orders", lastEventId: "evt-a"))
        {
            var replayed = Drain(fromProducerId);
            Assert.Equal("b,c,d", Datas(replayed));
            Assert.Equal("2,evt-c,4", Ids(replayed));
        }

        // Resuming from a hub-sequence wire id replays the suffix the same way.
        using var fromSequence = hub.Subscribe("orders", lastEventId: "2");
        var rest = Drain(fromSequence);
        Assert.Equal("c,d", Datas(rest));
        Assert.Equal("evt-c,4", Ids(rest));
    }

    [Fact]
    public void A_reused_producer_wire_id_resumes_from_the_oldest_matching_entry()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        // The producer reuses "dup" for two distinct events. The contract: resume matches the OLDEST
        // retained entry carrying that wire id (the scan runs oldest to newest), so the replayed suffix
        // is everything after the first "dup".
        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "dup" });   // seq 1
        hub.Publish("orders", Event("b"));                                       // seq 2
        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "dup" });   // seq 3, reused id
        hub.Publish("orders", Event("d"));                                       // seq 4

        using var resumed = hub.Subscribe("orders", lastEventId: "dup");
        Assert.Equal("b,c,d", Datas(Drain(resumed)));
    }

    [Fact]
    public void Producer_id_does_not_consume_a_sequence_value_from_a_later_event()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        // Interleaving producer-id and sequence-only events must keep the sequence gap-free: 1,2,3,4.
        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "x" });
        hub.Publish("orders", Event("b"));
        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "y" });
        hub.Publish("orders", Event("d"));

        var seqs = Drain(sub).Select(e => e.SequenceId).ToArray();
        Assert.Equal(new long?[] { 1, 2, 3, 4 }, seqs);
    }

    [Fact]
    public void The_replay_buffer_retains_in_ascending_sequence_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, replayCapacity: 3);

        for (var i = 1; i <= 6; i++)
        {
            hub.Publish("orders", Event(i.ToString(CultureInfo.InvariantCulture)));
        }

        // Only the newest 3 (seq 4,5,6) survive, in ascending order. Resuming from seq 4 (oldest
        // retained) replays 5,6 in order.
        using var resumed = hub.Subscribe("orders", lastEventId: "4");
        var replayed = Drain(resumed);
        Assert.Equal("5,6", Datas(replayed));
        Assert.Equal("5,6", Ids(replayed));
    }
}
