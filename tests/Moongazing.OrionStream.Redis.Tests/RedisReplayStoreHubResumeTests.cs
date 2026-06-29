namespace Moongazing.OrionStream.Redis.Tests;

using System.Collections.Generic;
using System.Linq;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// End-to-end resume through the full hub seam over a real Redis: two independent <see cref="SseHub"/>
/// instances, each with its own <see cref="RedisReplayStoreFactory"/> pointed at the same Redis,
/// modelling two app instances behind a load balancer. An event published on instance A must be
/// resumable by a client that reconnects to instance B by <c>Last-Event-ID</c>, even though B never saw
/// the live publish: that is the whole point of the backplane replay store.
/// </summary>
public sealed class RedisReplayStoreHubResumeTests : IClassFixture<RedisContainerFixture>
{
    private readonly RedisContainerFixture fixture;

    public RedisReplayStoreHubResumeTests(RedisContainerFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        this.fixture = fixture;
    }

    private static string NewTopic() => "topic-" + Guid.NewGuid().ToString("N");

    private static List<ServerSentEvent> Drain(StreamSubscription sub)
    {
        var events = new List<ServerSentEvent>();
        while (sub.Reader.TryRead(out var evt))
        {
            events.Add(evt!);
        }
        return events;
    }

    private static string Datas(IEnumerable<ServerSentEvent> events) => string.Join(",", events.Select(e => e.Data));

    private SseHub NewHubOnSharedRedis(StreamDiagnostics diag)
    {
        var factory = new RedisReplayStoreFactory(fixture.Mux, new RedisReplayStoreOptions());
        return new SseHub(new StreamOptions(), diag, factory);
    }

    [Fact]
    public void A_client_resumes_on_instance_B_from_a_backlog_published_on_instance_A()
    {
        using var diag = new StreamDiagnostics();
        var topic = NewTopic();

        // Instance A publishes the backlog. The hub stamps gap-free sequences 1..3 and retains each to
        // Redis through the seam.
        var instanceA = NewHubOnSharedRedis(diag);
        instanceA.Publish(topic, new ServerSentEvent { Data = "a" }); // wire id "1"
        instanceA.Publish(topic, new ServerSentEvent { Data = "b" }); // wire id "2"
        instanceA.Publish(topic, new ServerSentEvent { Data = "c" }); // wire id "3"

        // Instance B never saw those live publishes. A client reconnects to B with Last-Event-ID "1";
        // B reads the backlog from the SAME Redis through the seam and replays the suffix b, c.
        var instanceB = NewHubOnSharedRedis(diag);
        using var resumed = instanceB.Subscribe(topic, lastEventId: "1");

        Assert.Equal("b,c", Datas(Drain(resumed)));
    }

    [Fact]
    public void A_producer_supplied_id_round_trips_through_cross_instance_resume()
    {
        using var diag = new StreamDiagnostics();
        var topic = NewTopic();

        var instanceA = NewHubOnSharedRedis(diag);
        instanceA.Publish(topic, new ServerSentEvent { Data = "a", Id = "evt-a" });
        instanceA.Publish(topic, new ServerSentEvent { Data = "b", Id = "evt-b" });
        instanceA.Publish(topic, new ServerSentEvent { Data = "c", Id = "evt-c" });

        // The client last saw the producer id "evt-a"; resuming on a different instance must match that
        // exact wire id and replay the suffix, exactly as a hub-sequence resume would.
        var instanceB = NewHubOnSharedRedis(diag);
        using var resumed = instanceB.Subscribe(topic, lastEventId: "evt-a");

        Assert.Equal("b,c", Datas(Drain(resumed)));
    }

    [Fact]
    public void An_unknown_last_event_id_falls_back_to_a_from_now_stream_on_instance_B()
    {
        using var diag = new StreamDiagnostics();
        var topic = NewTopic();

        var instanceA = NewHubOnSharedRedis(diag);
        instanceA.Publish(topic, new ServerSentEvent { Data = "a" });

        var instanceB = NewHubOnSharedRedis(diag);
        using var resumed = instanceB.Subscribe(topic, lastEventId: "999");

        // Unknown id: no replay. Live events on B still flow after the from-now fallback.
        Assert.Empty(Drain(resumed));
        instanceB.Publish(topic, new ServerSentEvent { Data = "live-on-b" });
        Assert.Equal("live-on-b", Datas(Drain(resumed)));
    }
}
