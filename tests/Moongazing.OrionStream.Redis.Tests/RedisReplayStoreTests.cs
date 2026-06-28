namespace Moongazing.OrionStream.Redis.Tests;

using System.Linq;

using Moongazing.OrionStream.Streaming;

using StackExchange.Redis;

/// <summary>
/// Behavioural conformance for <see cref="RedisReplayStore"/> against a REAL Redis (Testcontainers),
/// covering the v0.6.0 scope: cross-instance resume (an append on one store instance is replayed in
/// sequence order by a second store pointed at the same Redis), the bounded drop-oldest capacity, the
/// documented duplicate-WireId resolution, and the resume-by-Last-Event-ID slice.
/// </summary>
/// <remarks>
/// Each test uses a fresh Guid topic so cases never alias each other while sharing the one container
/// from <see cref="RedisContainerFixture"/>. The store interface is synchronous (the seam the hub
/// calls), so these drive it synchronously and let StackExchange.Redis multiplex the calls.
/// </remarks>
public sealed class RedisReplayStoreTests : IClassFixture<RedisContainerFixture>
{
    private readonly RedisContainerFixture fixture;

    public RedisReplayStoreTests(RedisContainerFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        this.fixture = fixture;
    }

    private static string NewTopic() => "topic-" + Guid.NewGuid().ToString("N");

    private static ReplayEntry Entry(long sequence, string wireId, string data) =>
        new(sequence, wireId, new ServerSentEvent { Data = data, Id = wireId });

    private RedisReplayStore NewStore(string topic, int capacity, IConnectionMultiplexer? mux = null) =>
        new((mux ?? fixture.Mux).GetDatabase(), (RedisKey)("orionstream:replay:" + topic), capacity);

    private static string Datas(IEnumerable<ReplayEntry> entries) => string.Join(",", entries.Select(e => e.Event.Data));

    private static string WireIds(IEnumerable<ReplayEntry> entries) => string.Join(",", entries.Select(e => e.WireId));

    private static string Sequences(IEnumerable<ReplayEntry> entries) =>
        string.Join(",", entries.Select(e => e.Sequence.ToString(System.Globalization.CultureInfo.InvariantCulture)));

    [Fact]
    public void Append_then_resume_returns_only_the_subsequent_entries()
    {
        var topic = NewTopic();
        var store = NewStore(topic, capacity: 256);

        store.Append(Entry(1, "1", "a"));
        store.Append(Entry(2, "2", "b"));
        store.Append(Entry(3, "3", "c"));

        var replay = store.GetReplay("1");
        Assert.Equal("b,c", Datas(replay));
        Assert.Equal("2,3", WireIds(replay));
        Assert.Equal("2,3", Sequences(replay));
    }

    [Fact]
    public void Cross_instance_resume_replays_in_sequence_order_from_a_second_store_on_the_same_redis()
    {
        var topic = NewTopic();

        // Instance A appends the backlog. A second store instance - modelling a DIFFERENT hub process
        // behind a load balancer - is pointed at the SAME Redis key and must replay what A retained, in
        // order. This is the cross-instance resume the package exists for.
        var instanceA = NewStore(topic, capacity: 256);
        instanceA.Append(Entry(1, "1", "a"));
        instanceA.Append(Entry(2, "2", "b"));
        instanceA.Append(Entry(3, "3", "c"));

        var instanceB = NewStore(topic, capacity: 256);
        var replay = instanceB.GetReplay("1");

        Assert.Equal("b,c", Datas(replay));
        Assert.Equal("2,3", Sequences(replay));
    }

    [Fact]
    public void Cross_instance_resume_works_over_a_second_independent_multiplexer()
    {
        var topic = NewTopic();

        var instanceA = NewStore(topic, capacity: 256);
        instanceA.Append(Entry(1, "1", "a"));
        instanceA.Append(Entry(2, "2", "b"));

        // A genuinely separate connection to the same Redis, not just a second store over the shared
        // multiplexer: proves the backlog crosses process boundaries, not just object boundaries.
        var secondMux = ConnectionMultiplexer.Connect(fixture.ConnectionString);
        try
        {
            var instanceB = NewStore(topic, capacity: 256, mux: secondMux);
            Assert.Equal("b", Datas(instanceB.GetReplay("1")));
        }
        finally
        {
            secondMux.Dispose();
        }
    }

    [Fact]
    public void Backlog_is_bounded_to_capacity_dropping_the_oldest()
    {
        var topic = NewTopic();
        var store = NewStore(topic, capacity: 3);

        // Five appends into a capacity-3 store: the two oldest (1, 2) are evicted, leaving 3, 4, 5.
        for (var i = 1; i <= 5; i++)
        {
            store.Append(Entry(i, i.ToString(System.Globalization.CultureInfo.InvariantCulture), "e" + i));
        }

        // Resuming from id "3" (the oldest still retained) replays the suffix after it: 4, 5.
        Assert.Equal("e4,e5", Datas(store.GetReplay("3")));

        // Resuming from an EVICTED id ("1") matches nothing, so the result is the from-now fallback
        // (empty), never a partial backlog.
        Assert.Empty(store.GetReplay("1"));
    }

    [Fact]
    public void A_duplicate_wire_id_resolves_to_the_oldest_matching_entry()
    {
        var topic = NewTopic();
        var store = NewStore(topic, capacity: 256);

        // A producer reused the id "dup" on two events (seq 1 and seq 3). The contract: resume matches
        // the OLDEST entry carrying it (seq 1) and replays the suffix after it (b, c, d), never skipping
        // events by matching the newest.
        store.Append(Entry(1, "dup", "a"));
        store.Append(Entry(2, "2", "b"));
        store.Append(Entry(3, "dup", "c"));
        store.Append(Entry(4, "4", "d"));

        Assert.Equal("b,c,d", Datas(store.GetReplay("dup")));
    }

    [Fact]
    public void An_unknown_id_is_the_from_now_fallback()
    {
        var topic = NewTopic();
        var store = NewStore(topic, capacity: 256);

        store.Append(Entry(1, "1", "a"));
        store.Append(Entry(2, "2", "b"));

        Assert.Empty(store.GetReplay("nope"));
    }

    [Fact]
    public void Has_backlog_reflects_whether_any_entry_is_retained()
    {
        var topic = NewTopic();
        var store = NewStore(topic, capacity: 256);

        Assert.False(store.HasBacklog);
        store.Append(Entry(1, "1", "a"));
        Assert.True(store.HasBacklog);
    }

    [Fact]
    public void Backlog_survives_a_new_store_instance_modelling_a_process_restart()
    {
        var topic = NewTopic();

        // First "process": append a backlog, then drop the store object entirely.
        NewStore(topic, capacity: 256).Append(Entry(1, "1", "a"));
        NewStore(topic, capacity: 256).Append(Entry(2, "2", "b"));

        // A brand-new store over the same Redis key - the post-restart process - still finds the
        // backlog, because it lives in Redis, not in the previous store object.
        var afterRestart = NewStore(topic, capacity: 256);
        Assert.True(afterRestart.HasBacklog);
        Assert.Equal("b", Datas(afterRestart.GetReplay("1")));
    }
}
