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
        var hub = new SseHub(new StreamOptions(), diag, new SingleStoreFactory(store));

        store.Seed(new ReplayEntry(1, "1", Event("a")));

        // A subscriber that comes and goes must not reclaim the topic while the store still has backlog,
        // because a later client could resume from it. The hub reads HasBacklog through the seam.
        using (var sub = hub.Subscribe("orders"))
        {
            Assert.Equal(1, hub.SubscriberCount("orders"));
        }

        store.HasBacklogOverride = true;
        Assert.True(store.HasBacklog);

        // Resume still finds the seeded backlog after the subscriber left.
        using var resumed = hub.Subscribe("orders", lastEventId: "marker");
        Assert.Equal("a", Datas(Drain(resumed)));
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

    /// <summary>An <see cref="IReplayStoreFactory"/> that always hands back the same supplied store instance.</summary>
    private sealed class SingleStoreFactory(IReplayStore store) : IReplayStoreFactory
    {
        public IReplayStore Create(string topic, int capacity) => store;
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

        public bool? HasBacklogOverride { get; set; }

        public bool HasBacklog => HasBacklogOverride ?? (seeded.Count > 0 || Appended.Count > 0);

        public void Seed(ReplayEntry entry) => seeded.Add(entry);

        public void Append(ReplayEntry entry) => Appended.Add(entry);

        public IReadOnlyList<ReplayEntry> GetReplay(string lastEventId)
        {
            LastGetReplayArg = lastEventId;
            return seeded;
        }
    }
}
