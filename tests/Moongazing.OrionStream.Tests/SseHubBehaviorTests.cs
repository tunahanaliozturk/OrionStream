namespace Moongazing.OrionStream.Tests;

using System;
using System.Threading.Tasks;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseHubBehaviorTests
{
    private static SseHub NewHub(StreamDiagnostics diagnostics, int capacity = 256) =>
        new(new StreamOptions { SubscriberCapacity = capacity }, diagnostics);

    private static ServerSentEvent Event(string data) => new() { Data = data };

    [Fact]
    public void Constructor_rejects_null_options()
    {
        using var diag = new StreamDiagnostics();
        Assert.Throws<ArgumentNullException>(() => new SseHub(null!, diag));
    }

    [Fact]
    public void Constructor_rejects_null_diagnostics()
    {
        Assert.Throws<ArgumentNullException>(() => new SseHub(new StreamOptions(), null!));
    }

    [Fact]
    public void Constructor_validates_options_eagerly()
    {
        using var diag = new StreamDiagnostics();
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new SseHub(new StreamOptions { SubscriberCapacity = 0 }, diag));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Subscribe_rejects_null_or_empty_topic(string? topic)
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        Assert.ThrowsAny<ArgumentException>(() => hub.Subscribe(topic!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Publish_rejects_null_or_empty_topic(string? topic)
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        Assert.ThrowsAny<ArgumentException>(() => hub.Publish(topic!, Event("x")));
    }

    [Fact]
    public void Publish_rejects_null_event()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        Assert.Throws<ArgumentNullException>(() => hub.Publish("orders", null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void SubscriberCount_rejects_null_or_empty_topic(string? topic)
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        Assert.ThrowsAny<ArgumentException>(() => hub.SubscriberCount(topic!));
    }

    [Fact]
    public void SubscriberCount_of_an_unknown_topic_is_zero()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        Assert.Equal(0, hub.SubscriberCount("never-seen"));
    }

    [Fact]
    public void Topic_is_case_sensitive()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("Orders");

        var delivered = hub.Publish("orders", Event("x"));

        Assert.Equal(0, delivered);
        Assert.False(sub.Reader.TryRead(out _));
        Assert.Equal(0, hub.SubscriberCount("orders"));
        Assert.Equal(1, hub.SubscriberCount("Orders"));
    }

    [Fact]
    public void The_subscriber_subscribes_more_than_once_each_subscription_is_independent()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var a = hub.Subscribe("orders");
        using var b = hub.Subscribe("orders");

        Assert.Equal(2, hub.SubscriberCount("orders"));

        var delivered = hub.Publish("orders", Event("x"));

        Assert.Equal(2, delivered);
        Assert.True(a.Reader.TryRead(out _));
        Assert.True(b.Reader.TryRead(out _));
    }

    [Fact]
    public void Disposing_one_of_two_subscribers_leaves_the_topic_and_the_other_alive()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        var a = hub.Subscribe("orders");
        using var b = hub.Subscribe("orders");

        a.Dispose();

        Assert.Equal(1, hub.SubscriberCount("orders"));
        Assert.Equal(1, hub.Publish("orders", Event("x")));
        Assert.True(b.Reader.TryRead(out _));
    }

    [Fact]
    public void Disposing_the_last_subscriber_removes_the_topic()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        var sub = hub.Subscribe("orders");

        sub.Dispose();

        // Topic gone: a re-subscribe rebuilds it and a fresh subscriber is the only one.
        using var again = hub.Subscribe("orders");
        Assert.Equal(1, hub.SubscriberCount("orders"));
    }

    [Fact]
    public void Disposing_a_subscription_completes_its_reader()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        var sub = hub.Subscribe("orders");

        sub.Dispose();

        Assert.True(sub.Reader.Completion.IsCompleted);
    }

    [Fact]
    public void A_disposed_subscription_no_longer_receives_published_events()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        var sub = hub.Subscribe("orders");
        sub.Dispose();

        var delivered = hub.Publish("orders", Event("x"));

        Assert.Equal(0, delivered);
    }

    [Fact]
    public void Events_are_delivered_in_publish_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", Event("1"));
        hub.Publish("orders", Event("2"));
        hub.Publish("orders", Event("3"));

        Assert.True(sub.Reader.TryRead(out var a));
        Assert.True(sub.Reader.TryRead(out var b));
        Assert.True(sub.Reader.TryRead(out var c));
        Assert.Equal("1", a!.Data);
        Assert.Equal("2", b!.Data);
        Assert.Equal("3", c!.Data);
    }

    [Fact]
    public void A_full_buffer_keeps_exactly_capacity_newest_events()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, capacity: 3);
        using var sub = hub.Subscribe("orders");

        for (var i = 1; i <= 6; i++)
        {
            hub.Publish("orders", Event(i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        }

        Assert.True(sub.Reader.TryRead(out var a));
        Assert.True(sub.Reader.TryRead(out var b));
        Assert.True(sub.Reader.TryRead(out var c));
        Assert.False(sub.Reader.TryRead(out _));
        Assert.Equal("4", a!.Data);
        Assert.Equal("5", b!.Data);
        Assert.Equal("6", c!.Data);
    }

    [Fact]
    public async Task A_subscriber_can_await_an_event_published_later()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        var readTask = sub.Reader.ReadAsync().AsTask();
        Assert.False(readTask.IsCompleted);

        hub.Publish("orders", Event("late"));

        var evt = await readTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("late", evt.Data);
    }

    [Fact]
    public void The_same_per_delivery_event_is_broadcast_to_every_subscriber()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var a = hub.Subscribe("orders");
        using var b = hub.Subscribe("orders");
        var evt = Event("shared");

        hub.Publish("orders", evt);

        Assert.True(a.Reader.TryRead(out var fromA));
        Assert.True(b.Reader.TryRead(out var fromB));

        // Instance identity is intentionally NOT preserved: the hub stamps a per-delivery copy
        // rather than mutating the caller's instance, which is what stops a re-published instance
        // from rewriting an older delivery's wire id. The producer's instance is never handed out.
        Assert.NotSame(evt, fromA);
        // One publish is one delivery, so every subscriber sees the very same stamped copy.
        Assert.Same(fromA, fromB);
        Assert.Equal("shared", fromA!.Data);
        Assert.Equal("1", fromA.EffectiveId);
    }

    [Fact]
    public void Republishing_the_same_instance_yields_independent_wire_ids()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");
        var evt = Event("reused");

        hub.Publish("orders", evt); // delivery sequence 1
        hub.Publish("orders", evt); // same instance, delivery sequence 2

        Assert.True(sub.Reader.TryRead(out var first));
        Assert.True(sub.Reader.TryRead(out var second));
        Assert.Equal("1", first!.EffectiveId);
        Assert.Equal("2", second!.EffectiveId);
        // The earlier delivery's id is not retroactively rewritten by the later publish.
        Assert.NotSame(first, second);
        // And the caller's own instance was never stamped at all.
        Assert.Null(evt.EffectiveId);
    }
}
