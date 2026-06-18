namespace Moongazing.OrionStream.Demo;

/// <summary>
/// Runnable console tour of OrionStream's HTTP-independent surface: the SSE wire formatter, the
/// in-memory broadcast hub (publish/subscribe), the DropOldest back-pressure policy, subscriber
/// lifecycle, and a concurrent async producer/consumer. It runs to completion and terminates; it
/// does NOT start a Kestrel server.
/// </summary>
internal static class Program
{
    private static async Task Main()
    {
        Console.WriteLine("OrionStream demo - Server-Sent Events for ASP.NET Core");
        Console.WriteLine("Showcasing the in-memory hub and formatter (no web server is started).");

        FormatterDemo.Run();
        HubPublishSubscribeDemo.Run();
        BackpressureDemo.Run();
        SubscriberLifecycleDemo.Run();
        await ConcurrentStreamDemo.RunAsync().ConfigureAwait(false);

        Console.WriteLine();
        Console.WriteLine("All demos completed.");
    }
}
