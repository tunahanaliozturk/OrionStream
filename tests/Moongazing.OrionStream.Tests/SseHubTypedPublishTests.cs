namespace Moongazing.OrionStream.Tests;

using System.Text.Json;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseHubTypedPublishTests
{
    private sealed record Order(int Id, string Customer);

    private static readonly JsonSerializerOptions WebOptions = new(JsonSerializerDefaults.Web);

    [Fact]
    public void Typed_publish_serializes_the_payload_into_the_event_data()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new Order(7, "ada"));

        Assert.True(sub.Reader.TryRead(out var evt));

        // Round-trip the data back through the serializer to assert the payload, not a brittle string.
        var roundTripped = JsonSerializer.Deserialize<Order>(evt!.Data, WebOptions);
        Assert.Equal(new Order(7, "ada"), roundTripped);
    }

    [Fact]
    public void Typed_publish_uses_web_defaults_so_property_names_are_camel_case()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new Order(7, "ada"));

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Contains("\"id\":7", evt!.Data, System.StringComparison.Ordinal);
        Assert.Contains("\"customer\":\"ada\"", evt.Data, System.StringComparison.Ordinal);
    }

    [Fact]
    public void Typed_publish_honors_a_supplied_serializer_options()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        // Default (non-web) options keep PascalCase property names.
        hub.Publish("orders", new Order(7, "ada"), new JsonSerializerOptions());

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Contains("\"Id\":7", evt!.Data, System.StringComparison.Ordinal);
        Assert.Contains("\"Customer\":\"ada\"", evt.Data, System.StringComparison.Ordinal);
    }

    [Fact]
    public void Typed_publish_carries_event_name_id_and_retry_onto_the_wire()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new Order(1, "x"), eventName: "created", id: "abc", retryMilliseconds: 5000);

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Equal("created", evt!.EventName);
        Assert.Equal("abc", evt.Id);
        Assert.Equal(5000, evt.RetryMilliseconds);
    }

    [Fact]
    public void Typed_publish_returns_the_delivered_subscriber_count()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var a = hub.Subscribe("orders");
        using var b = hub.Subscribe("orders");

        var delivered = hub.Publish("orders", new Order(1, "x"));

        Assert.Equal(2, delivered);
    }
}
