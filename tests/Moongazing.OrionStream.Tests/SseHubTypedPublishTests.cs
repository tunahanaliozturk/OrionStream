namespace Moongazing.OrionStream.Tests;

using System.Text.Json;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseHubTypedPublishTests
{
    private sealed record Order(int Id, string Customer);

    private sealed record Account(int AccountId);

    private static readonly JsonSerializerOptions WebOptions = new(JsonSerializerDefaults.Web);

    // A naming policy distinct from both web-default camelCase and PascalCase, so a payload using it
    // is unambiguous evidence the CONFIGURED options were applied rather than a default.
    private sealed class UpperSnakeNamingPolicy : JsonNamingPolicy
    {
        public override string ConvertName(string name)
        {
            var builder = new System.Text.StringBuilder(name.Length + 4);
            for (var i = 0; i < name.Length; i++)
            {
                var c = name[i];
                if (i > 0 && char.IsUpper(c))
                {
                    builder.Append('_');
                }
                builder.Append(char.ToUpperInvariant(c));
            }
            return builder.ToString();
        }
    }

    private static StreamOptions OptionsWithUpperSnakeNaming() => new()
    {
        SerializerOptions = new JsonSerializerOptions { PropertyNamingPolicy = new UpperSnakeNamingPolicy() },
    };

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
    public void Typed_publish_uses_the_hubs_configured_serializer_options_when_no_override_is_passed()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(OptionsWithUpperSnakeNaming(), diag);
        using var sub = hub.Subscribe("accounts");

        // No per-call serializer override: the configured naming policy must reach the wire.
        hub.Publish("accounts", new Account(42));

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Contains("\"ACCOUNT_ID\":42", evt!.Data, System.StringComparison.Ordinal);
    }

    [Fact]
    public void Typed_publish_uses_web_defaults_when_the_hub_configures_no_options()
    {
        // Stock StreamOptions leaves SerializerOptions at the web default.
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("accounts");

        hub.Publish("accounts", new Account(42));

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Contains("\"accountId\":42", evt!.Data, System.StringComparison.Ordinal);
    }

    [Fact]
    public void Typed_publish_per_call_override_wins_over_the_hubs_configured_options()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(OptionsWithUpperSnakeNaming(), diag);
        using var sub = hub.Subscribe("accounts");

        // Explicit override beats the configured upper-snake policy: PascalCase on the wire.
        hub.Publish("accounts", new Account(42), new JsonSerializerOptions());

        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Contains("\"AccountId\":42", evt!.Data, System.StringComparison.Ordinal);
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
