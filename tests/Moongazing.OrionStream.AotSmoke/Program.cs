// NativeAOT smoke test. Publishing this with PublishAot=true must produce zero trim/AOT warnings,
// and running it must exit 0 - OrionStream's AOT exit criterion. Runtime checks, not a framework:
// the point is to prove the core broadcast path (DI + subscribe + raw publish + channel read)
// survives trimming in a real native binary. The typed JSON publish is intentionally not used.
using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionStream;
using Moongazing.OrionStream.Streaming;

var services = new ServiceCollection();
services.AddOrionStream();

using var provider = services.BuildServiceProvider();
var hub = provider.GetRequiredService<ISseHub>();

using var subscription = hub.Subscribe("smoke");

var delivered = hub.Publish("smoke", new ServerSentEvent { Data = "hello", EventName = "greeting" });
Check(delivered == 1, $"expected 1 subscriber, delivered to {delivered}");

var read = subscription.Reader.ReadAsync().AsTask();
Check(read.Wait(TimeSpan.FromSeconds(5)), "event was not delivered to the subscriber");
var evt = read.Result;
Check(evt.Data == "hello" && evt.EventName == "greeting", "event round-trip mismatch");

Console.WriteLine("OrionStream AOT smoke test passed.");
return 0;

static void Check(bool condition, string message)
{
    if (!condition)
    {
        Console.Error.WriteLine($"AOT smoke test failed: {message}");
        Environment.Exit(1);
    }
}
