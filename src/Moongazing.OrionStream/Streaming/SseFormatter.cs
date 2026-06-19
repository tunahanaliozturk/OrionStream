namespace Moongazing.OrionStream.Streaming;

using System.Text;

/// <summary>
/// Renders a <see cref="ServerSentEvent"/> to the <c>text/event-stream</c> wire format defined by
/// the HTML SSE specification: each field on its own <c>field: value</c> line, multi-line data
/// split across multiple <c>data:</c> lines, and a blank line terminating the event.
/// </summary>
public static class SseFormatter
{
    /// <summary>The comment line written as a heartbeat to keep an idle connection open.</summary>
    public const string Heartbeat = ": heartbeat\n\n";

    /// <summary>Render an event to its wire representation.</summary>
    /// <param name="evt">The event to render.</param>
    /// <returns>The SSE-formatted text, ending with the blank-line terminator.</returns>
    public static string Format(ServerSentEvent evt)
    {
        ArgumentNullException.ThrowIfNull(evt);

        var builder = new StringBuilder();

        if (evt.EffectiveId is { } effectiveId)
        {
            builder.Append("id: ").Append(SingleLine(effectiveId)).Append('\n');
        }
        if (evt.EventName is not null)
        {
            builder.Append("event: ").Append(SingleLine(evt.EventName)).Append('\n');
        }
        if (evt.RetryMilliseconds is { } retry)
        {
            builder.Append("retry: ").Append(retry).Append('\n');
        }

        // Split on any newline style so a multi-line payload stays a single event.
        var lines = evt.Data.Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace('\r', '\n')
            .Split('\n');
        foreach (var line in lines)
        {
            builder.Append("data: ").Append(line).Append('\n');
        }

        builder.Append('\n');
        return builder.ToString();
    }

    private static string SingleLine(string value) =>
        value.Replace("\r", string.Empty, StringComparison.Ordinal)
            .Replace("\n", string.Empty, StringComparison.Ordinal);
}
