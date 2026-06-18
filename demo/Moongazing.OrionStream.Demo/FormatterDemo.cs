namespace Moongazing.OrionStream.Demo;

using Moongazing.OrionStream.Streaming;

/// <summary>
/// Demonstrates <see cref="SseFormatter.Format"/> rendering a <see cref="ServerSentEvent"/> to the
/// text/event-stream wire format: data-only, full-metadata (id/event/retry/data in canonical
/// order), and a multi-line payload split across multiple data: lines.
/// </summary>
internal static class FormatterDemo
{
    public static void Run()
    {
        DemoConsole.Header("1. SseFormatter: ServerSentEvent -> SSE wire format");

        DemoConsole.Step("Data-only event (just a 'data:' line plus the blank terminator):");
        var dataOnly = SseFormatter.Format(new ServerSentEvent { Data = "hello" });
        DemoConsole.Wire("wire", dataOnly);

        DemoConsole.Step("Full metadata (id, event, retry, data render in canonical order):");
        var full = SseFormatter.Format(new ServerSentEvent
        {
            Id = "42",
            EventName = "tick",
            RetryMilliseconds = 3000,
            Data = "payload",
        });
        DemoConsole.Wire("wire", full);

        DemoConsole.Step("Multi-line data becomes one 'data:' line per line, still one event:");
        var multiLine = SseFormatter.Format(new ServerSentEvent
        {
            EventName = "order.created",
            Data = "line one\nline two\r\nline three",
        });
        DemoConsole.Wire("wire", multiLine);

        DemoConsole.Step("Heartbeat constant (a comment line that carries no event):");
        DemoConsole.Wire("wire", SseFormatter.Heartbeat);
    }
}
