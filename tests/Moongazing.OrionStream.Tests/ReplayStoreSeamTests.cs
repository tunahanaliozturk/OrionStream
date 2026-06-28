namespace Moongazing.OrionStream.Tests;

using System.Collections.Generic;
using System.Linq;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

/// <summary>
/// Asserts the pluggable replay store seam: resume works identically through <see cref="IReplayStore"/>
/// with the in-memory default, and a custom store double can be plugged so the hub resumes from
/// wherever the backlog lives without knowing where that is.
/// </summary>
public sealed class ReplayStoreSeamTests
{
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

    private static string Datas(IEnumerable<ServerSentEvent> events) =>
        string.Join(",", events.Select(e => e.Data));

    private static string Ids(IEnumerable<ServerSentEvent> events) =>
        string.Join(",", events.Select(e => e.EffectiveId));

    [Fact]
    public void Default_hub_uses_the_in_memory_store_factory()
    {
        using var diag = new StreamDiagnostics();
        var factory = new RecordingReplayStoreFactory(new InMemoryReplayStoreFactory());
        var hub = new SseHub(new StreamOptions(), diag, factory);

        hub.Publish("orders", Event("a"));

        // Publishing to a replay-enabled topic creates exactly one store through the seam.
        Assert.Equal(1, factory.CreatedCount);
        Assert.Equal("orders", factory.LastTopic);
        Assert.Equal(256, factory.LastCapacity);
    }

    [Fact]
    public void Resume_through_the_in_memory_default_replays_only_subsequent_events()
    {
        // The 3-arg constructor with the explicit default factory must behave identically to the
        // 2-arg constructor: resume replays the suffix after the matched id.
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag, new InMemoryReplayStoreFactory());

        hub.Publish("orders", Event("a")); // id 1
        hub.Publish("orders", Event("b")); // id 2
        hub.Publish("orders", Event("c")); // id 3

        using var resumed = hub.Subscribe("orders", lastEventId: "1");
        var replayed = Drain(resumed);
        Assert.Equal("b,c", Datas(replayed));
        Assert.Equal("2,3", Ids(replayed));
    }

    [Fact]
    public void No_store_is_created_for_a_replay_disabled_topic()
    {
        using var diag = new StreamDiagnostics();
        var factory = new RecordingReplayStoreFactory(new InMemoryReplayStoreFactory());
        var hub = new SseHub(new StreamOptions { ReplayBufferCapacity = 0 }, diag, factory);

        using var sub = hub.Subscribe("orders");
        hub.Publish("orders", Event("a"));

        // Replay disabled: the hub never asks the factory for a store, keeping the wire path light.
        Assert.Equal(0, factory.CreatedCount);
    }

    [Fact]
    public void A_custom_store_double_is_appended_to_on_publish()
    {
        using var diag = new StreamDiagnostics();
        var store = new FakeReplayStore();
        var hub = new SseHub(new StreamOptions(), diag, new SingleStoreFactory(store));

        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "evt-a" });
        hub.Publish("orders", Event("b"));

        // The hub appends every delivery to the custom store, carrying the wire id and the gap-free
        // sequence assigned under the topic gate.
        Assert.Equal(2, store.Appended.Count);
        Assert.Equal(1, store.Appended[0].Sequence);
        Assert.Equal("evt-a", store.Appended[0].WireId);
        Assert.Equal(2, store.Appended[1].Sequence);
        Assert.Equal("2", store.Appended[1].WireId);
    }

    [Fact]
    public void Resume_reads_the_backlog_from_a_custom_store()
    {
        using var diag = new StreamDiagnostics();
        var store = new FakeReplayStore();
        var hub = new SseHub(new StreamOptions(), diag, new SingleStoreFactory(store));

        // The hub never published these: they live ONLY in the custom store. Resume must read them from
        // the seam, proving the hub does not know where the backlog lives.
        store.Seed(new ReplayEntry(7, "server-7", Event("from-store-7")));
        store.Seed(new ReplayEntry(8, "server-8", Event("from-store-8")));

        using var resumed = hub.Subscribe("orders", lastEventId: "client-marker");

        // The hub called GetReplay with the client's Last-Event-ID and streamed whatever the store
        // returned, in order.
        Assert.Equal("client-marker", store.LastGetReplayArg);
        Assert.Equal("from-store-7,from-store-8", Datas(Drain(resumed)));
    }

    [Fact]
    public void An_empty_custom_store_result_is_the_from_now_fallback()
    {
        using var diag = new StreamDiagnostics();
        var store = new FakeReplayStore(); // GetReplay returns empty by default
        var hub = new SseHub(new StreamOptions(), diag, new SingleStoreFactory(store));

        using var resumed = hub.Subscribe("orders", lastEventId: "anything");
        Assert.Empty(Drain(resumed));

        // Live events still flow after the from-now fallback.
        hub.Publish("orders", Event("live"));
        Assert.Equal("live", Datas(Drain(resumed)));
    }

    [Fact]
    public void A_custom_store_keeps_the_topic_alive_while_it_reports_backlog()
    {
        using var diag = new StreamDiagnostics();
        var store = new FakeReplayStore();
        var factory = new SingleStoreFactory(store);
        var hub = new SseHub(new StreamOptions(), diag, factory);

        store.Seed(new ReplayEntry(1, "1", Event("a")));

        // A subscriber that comes and goes must not reclaim the topic while the store still has backlog,
        // because a later client could resume from it. The hub reads HasBacklog through the seam. The
        // store is created the first time the topic is seen.
        store.HasBacklogOverride = true;
        using (var sub = hub.Subscribe("orders"))
        {
            Assert.Equal(1, hub.SubscriberCount("orders"));
        }

        Assert.Equal(1, factory.CreatedCount);

        // The subscriber has left, but the store still reports backlog through the seam, so the topic
        // must NOT have been reclaimed. Asserting the topic survived (rather than only that resume
        // returned data) is what makes this prove the seam kept the topic alive: a SingleStoreFactory
        // would hand the same store back even to a freshly recreated topic, so a resume-returns-"a"
        // assertion alone would pass even if the topic had been reclaimed and rebuilt.
        Assert.Equal(1, hub.TopicCount);

        // Resume still finds the seeded backlog, and it came through the seam (GetReplay was called),
        // without the factory ever being asked to create a second store for a recreated topic.
        using var resumed = hub.Subscribe("orders", lastEventId: "marker");
        Assert.Equal("a", Datas(Drain(resumed)));
        Assert.Equal(1, store.GetReplayCount);
        Assert.Equal(1, factory.CreatedCount);
    }

    [Fact]
    public void A_failed_resume_setup_does_not_leak_the_subscriber()
    {
        using var diag = new StreamDiagnostics();
        var store = new ThrowingGetReplayStore();
        var hub = new SseHub(new StreamOptions(), diag, new SingleStoreFactory(store));

        // Seed backlog so the topic survives subscriber departures and HasBacklog is true; the resume
        // path will reach GetReplay, which throws.
        store.Seed(new ReplayEntry(1, "1", Event("a")));

        // Resume setup throws after the subscriber was registered under the topic gate. The hub must
        // unwind that registration before the exception escapes, otherwise the subscriber is leaked:
        // registered on the topic but never streamed (Subscribe never returned a subscription whose
        // disposal could remove it).
        Assert.Throws<InvalidOperationException>(() => hub.Subscribe("orders", lastEventId: "marker"));

        // No leak: the failed resume left no subscriber behind on the topic.
        Assert.Equal(0, hub.SubscriberCount("orders"));

        // And the topic is still usable: a later publish does not fan out to a dead, leaked channel,
        // and a fresh subscriber receives live events normally.
        store.ThrowOnGetReplay = false;
        using var live = hub.Subscribe("orders");
        hub.Publish("orders", Event("after"));
        Assert.Equal("after", Datas(Drain(live)));
    }

    [Fact]
    public void A_pre_existing_backlog_replays_in_sequence_order_even_when_the_store_returns_it_unordered()
    {
        using var diag = new StreamDiagnostics();
        var store = new UnorderedBacklogStore();
        var hub = new SseHub(new StreamOptions(), diag, new SingleStoreFactory(store));

        // A custom store holds a pre-existing backlog the hub never published, and returns it OUT of
        // sequence order (e.g. an external query with no ORDER BY). The hub must replay it in ascending
        // Sequence order regardless, so the client never sees a reordered backlog.
        store.Seed(new ReplayEntry(3, "s3", Event("c")));
        store.Seed(new ReplayEntry(1, "s1", Event("a")));
        store.Seed(new ReplayEntry(2, "s2", Event("b")));

        using var resumed = hub.Subscribe("orders", lastEventId: "marker");
        Assert.Equal("a,b,c", Datas(Drain(resumed)));
    }

    [Fact]
    public void In_memory_store_resolves_a_duplicate_wire_id_to_the_oldest_entry()
    {
        // The seam contract: when two retained entries share a wire id (only possible for a reused
        // producer id), resume matches the OLDEST and replays the suffix after it. Drive the in-memory
        // default through the hub with a producer that reuses an id.
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag, new InMemoryReplayStoreFactory());

        hub.Publish("orders", new ServerSentEvent { Data = "a", Id = "dup" }); // seq 1, wire id "dup"
        hub.Publish("orders", Event("b"));                                     // seq 2, wire id "2"
        hub.Publish("orders", new ServerSentEvent { Data = "c", Id = "dup" }); // seq 3, wire id "dup"
        hub.Publish("orders", Event("d"));                                     // seq 4, wire id "4"

        // Last-Event-ID "dup" matches the OLDEST entry carrying it (seq 1), so the suffix after seq 1 is
        // replayed: b, c, d. Matching the newest "dup" instead would have silently dropped b and c.
        using var resumed = hub.Subscribe("orders", lastEventId: "dup");
        Assert.Equal("b,c,d", Datas(Drain(resumed)));
    }

    /// <summary>An <see cref="IReplayStoreFactory"/> that records each create and delegates to an inner factory.</summary>
    private sealed class RecordingReplayStoreFactory(IReplayStoreFactory inner) : IReplayStoreFactory
    {
        public int CreatedCount { get; private set; }

        public string? LastTopic { get; private set; }

        public int LastCapacity { get; private set; }

        public IReplayStore Create(string topic, int capacity)
        {
            CreatedCount++;
            LastTopic = topic;
            LastCapacity = capacity;
            return inner.Create(topic, capacity);
        }
    }

    /// <summary>
    /// An <see cref="IReplayStoreFactory"/> that always hands back the same supplied store instance and
    /// counts how many times the hub asked it to create a store. A second create for the same topic
    /// means the hub reclaimed and recreated the topic, which a test can assert did NOT happen.
    /// </summary>
    private sealed class SingleStoreFactory(IReplayStore store) : IReplayStoreFactory
    {
        public int CreatedCount { get; private set; }

        public IReplayStore Create(string topic, int capacity)
        {
            CreatedCount++;
            return store;
        }
    }

    /// <summary>
    /// A hand-written <see cref="IReplayStore"/> double. It records appends and the GetReplay argument,
    /// and returns a seeded backlog so a test can prove resume reads through the seam rather than from
    /// any hub-internal buffer.
    /// </summary>
    private sealed class FakeReplayStore : IReplayStore
    {
        private readonly List<ReplayEntry> seeded = [];

        public List<ReplayEntry> Appended { get; } = [];

        public string? LastGetReplayArg { get; private set; }

        public int GetReplayCount { get; private set; }

        public bool? HasBacklogOverride { get; set; }

        public bool HasBacklog => HasBacklogOverride ?? (seeded.Count > 0 || Appended.Count > 0);

        public void Seed(ReplayEntry entry) => seeded.Add(entry);

        public void Append(ReplayEntry entry) => Appended.Add(entry);

        public IReadOnlyList<ReplayEntry> GetReplay(string lastEventId)
        {
            GetReplayCount++;
            LastGetReplayArg = lastEventId;
            return seeded;
        }
    }

    /// <summary>
    /// An <see cref="IReplayStore"/> whose <see cref="GetReplay"/> throws, modelling a custom store that
    /// fails (a broken external query, a transient backend error) during resume setup. Reports backlog
    /// so the hub keeps the topic and actually reaches the resume path.
    /// </summary>
    private sealed class ThrowingGetReplayStore : IReplayStore
    {
        private readonly List<ReplayEntry> seeded = [];

        public bool ThrowOnGetReplay { get; set; } = true;

        public bool HasBacklog => seeded.Count > 0;

        public void Seed(ReplayEntry entry) => seeded.Add(entry);

        public void Append(ReplayEntry entry) => seeded.Add(entry);

        public IReadOnlyList<ReplayEntry> GetReplay(string lastEventId)
        {
            if (ThrowOnGetReplay)
            {
                throw new InvalidOperationException("simulated replay-store failure during resume setup");
            }

            return [];
        }
    }

    /// <summary>
    /// An <see cref="IReplayStore"/> that returns its seeded backlog in insertion order, which a test
    /// deliberately seeds OUT of sequence order, to prove the hub re-orders the backlog by
    /// <see cref="ReplayEntry.Sequence"/> at the seam read.
    /// </summary>
    private sealed class UnorderedBacklogStore : IReplayStore
    {
        private readonly List<ReplayEntry> seeded = [];

        public bool HasBacklog => seeded.Count > 0;

        public void Seed(ReplayEntry entry) => seeded.Add(entry);

        public void Append(ReplayEntry entry) => seeded.Add(entry);

        public IReadOnlyList<ReplayEntry> GetReplay(string lastEventId) => seeded;
    }
}
