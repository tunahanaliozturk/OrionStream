namespace Moongazing.OrionStream.Tests;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseHubTests
{
    private static SseHub NewHub(StreamDiagnostics diagnostics, int capacity = 256) =>
        new(new StreamOptions { SubscriberCapacity = capacity }, diagnostics);

    private static ServerSentEvent Event(string data) => new() { Data = data };

    [Fact]
    public void A_subscriber_receives_a_published_event()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        var delivered = hub.Publish("orders", Event("first"));

        Assert.Equal(1, delivered);
        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Equal("first", evt!.Data);
    }

    [Fact]
    public void Every_subscriber_of_a_topic_receives_the_event()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var a = hub.Subscribe("orders");
        using var b = hub.Subscribe("orders");

        var delivered = hub.Publish("orders", Event("x"));

        Assert.Equal(2, delivered);
        Assert.True(a.Reader.TryRead(out _));
        Assert.True(b.Reader.TryRead(out _));
    }

    [Fact]
    public void A_subscriber_does_not_receive_events_from_another_topic()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        using var sub = hub.Subscribe("orders");

        var delivered = hub.Publish("invoices", Event("x"));

        Assert.Equal(0, delivered);
        Assert.False(sub.Reader.TryRead(out _));
    }

    [Fact]
    public void Disposing_a_subscription_unsubscribes_it()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        var sub = hub.Subscribe("orders");

        Assert.Equal(1, hub.SubscriberCount("orders"));
        sub.Dispose();
        Assert.Equal(0, hub.SubscriberCount("orders"));

        Assert.Equal(0, hub.Publish("orders", Event("x")));
    }

    [Fact]
    public void Publishing_to_a_topic_with_no_subscribers_delivers_to_none()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);

        Assert.Equal(0, hub.Publish("empty", Event("x")));
    }

    [Fact]
    public void A_full_buffer_drops_the_oldest_event()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag, capacity: 2);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", Event("1"));
        hub.Publish("orders", Event("2"));
        hub.Publish("orders", Event("3")); // evicts "1"

        Assert.True(sub.Reader.TryRead(out var first));
        Assert.Equal("2", first!.Data);
        Assert.True(sub.Reader.TryRead(out var second));
        Assert.Equal("3", second!.Data);
        Assert.False(sub.Reader.TryRead(out _));
    }

    [Fact]
    public void Double_dispose_is_safe()
    {
        using var diag = new StreamDiagnostics();
        var hub = NewHub(diag);
        var sub = hub.Subscribe("orders");

        sub.Dispose();
        sub.Dispose();

        Assert.Equal(0, hub.SubscriberCount("orders"));
    }
}
