namespace Moongazing.OrionStream.Tests;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class StreamSubscriptionAsyncEnumerableTests
{
    private sealed record Tick(int N);

    private sealed record Account(int AccountId);

    private static readonly string[] ExpectedAbc = { "a", "b", "c" };
    private static readonly int[] ExpectedOneTwo = { 1, 2 };
    private static readonly int[] ExpectedOneTwoThree = { 1, 2, 3 };

    // A naming policy distinct from web-default camelCase and PascalCase, so a payload using it is
    // unambiguous evidence the CONFIGURED options were applied rather than a default.
    private sealed class UpperSnakeNamingPolicy : System.Text.Json.JsonNamingPolicy
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

    [Fact]
    public async Task Read_all_async_yields_published_events_in_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "a" });
        hub.Publish("orders", new ServerSentEvent { Data = "b" });
        hub.Publish("orders", new ServerSentEvent { Data = "c" });

        var seen = new List<string>();
        await foreach (var evt in sub.ReadAllAsync())
        {
            seen.Add(evt.Data);
            if (seen.Count == 3)
            {
                break; // all expected events drained; stop the otherwise-open stream
            }
        }

        Assert.Equal(ExpectedAbc, seen);
    }

    [Fact]
    public async Task Typed_read_all_async_deserializes_each_payload_in_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("ticks");

        hub.Publish("ticks", new Tick(1));
        hub.Publish("ticks", new Tick(2));

        var seen = new List<int>();
        await foreach (var tick in sub.ReadAllAsync<Tick>())
        {
            seen.Add(tick!.N);
            if (seen.Count == 2)
            {
                break;
            }
        }

        Assert.Equal(ExpectedOneTwo, seen);
    }

    [Fact]
    public async Task Read_all_async_completes_when_cancellation_is_requested()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        using var cts = new CancellationTokenSource();

        // Signalled exactly once, when the first event has been consumed, so the test can cancel
        // deterministically instead of polling a shared list with Task.Delay.
        var firstConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumed = new List<string>();
        var consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var evt in sub.ReadAllAsync(cts.Token))
                {
                    consumed.Add(evt.Data);
                    firstConsumed.TrySetResult();
                }
            }
            catch (OperationCanceledException)
            {
                // Cancelling an in-flight WaitToReadAsync surfaces as cancellation; that is the
                // expected completion path, not a failure.
            }
        });

        hub.Publish("orders", new ServerSentEvent { Data = "a" });

        // Wait deterministically for the first event to be consumed, then cancel.
        await firstConsumed.Task;

        await cts.CancelAsync();
        await consumer; // completes rather than hanging

        Assert.Contains("a", consumed);
    }

    [Fact]
    public async Task Publish_all_async_drains_an_async_stream_into_a_topic_in_order()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("ticks");

        static async IAsyncEnumerable<Tick> Source()
        {
            for (var n = 1; n <= 3; n++)
            {
                await Task.Yield();
                yield return new Tick(n);
            }
        }

        var published = await hub.PublishAllAsync("ticks", Source());

        Assert.Equal(3, published);

        var seen = new List<int>();
        await foreach (var tick in sub.ReadAllAsync<Tick>())
        {
            seen.Add(tick!.N);
            if (seen.Count == 3)
            {
                break;
            }
        }

        Assert.Equal(ExpectedOneTwoThree, seen);
    }

    [Fact]
    public async Task Publish_all_async_uses_the_hubs_configured_serializer_options_when_no_override_is_passed()
    {
        using var diag = new StreamDiagnostics();
        var options = new StreamOptions
        {
            SerializerOptions = new System.Text.Json.JsonSerializerOptions
            {
                PropertyNamingPolicy = new UpperSnakeNamingPolicy(),
            },
        };
        var hub = new SseHub(options, diag);
        using var sub = hub.Subscribe("accounts");

        static async IAsyncEnumerable<Account> Source()
        {
            await Task.Yield();
            yield return new Account(42);
        }

        // No per-call serializer override: the configured naming policy must reach the wire.
        var published = await hub.PublishAllAsync("accounts", Source());

        Assert.Equal(1, published);
        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Contains("\"ACCOUNT_ID\":42", evt!.Data, System.StringComparison.Ordinal);
    }

    [Fact]
    public async Task Publish_all_async_uses_web_defaults_when_the_hub_configures_no_options()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("accounts");

        static async IAsyncEnumerable<Account> Source()
        {
            await Task.Yield();
            yield return new Account(42);
        }

        var published = await hub.PublishAllAsync("accounts", Source());

        Assert.Equal(1, published);
        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Contains("\"accountId\":42", evt!.Data, System.StringComparison.Ordinal);
    }
}
