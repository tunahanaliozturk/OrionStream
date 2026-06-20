namespace Moongazing.OrionStream.Streaming;

using System;
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

        // Pre-size to cover the framing for every field plus the data payload, so the StringBuilder's
        // internal buffer is allocated once for the common single-line event rather than grown.
        var builder = new StringBuilder(evt.Data.Length + 32);

        if (evt.EffectiveId is { } effectiveId)
        {
            builder.Append("id: ");
            AppendSingleLine(builder, effectiveId);
            builder.Append('\n');
        }
        if (evt.EventName is { } eventName)
        {
            builder.Append("event: ");
            AppendSingleLine(builder, eventName);
            builder.Append('\n');
        }
        if (evt.RetryMilliseconds is { } retry)
        {
            builder.Append("retry: ").Append(retry).Append('\n');
        }

        AppendDataLines(builder, evt.Data);

        builder.Append('\n');
        return builder.ToString();
    }

    /// <summary>
    /// Append the payload as one or more <c>data:</c> lines, splitting on any newline style
    /// (<c>\r\n</c>, lone <c>\r</c>, lone <c>\n</c>) so a multi-line payload stays a single event. The
    /// common single-line payload takes the no-split, no-allocation path.
    /// </summary>
    private static void AppendDataLines(StringBuilder builder, string data)
    {
        var span = data.AsSpan();
        var start = 0;
        while (start <= span.Length)
        {
            var remaining = span[start..];
            var breakIndex = remaining.IndexOfAny('\r', '\n');
            if (breakIndex < 0)
            {
                builder.Append("data: ").Append(remaining).Append('\n');
                return;
            }

            builder.Append("data: ").Append(remaining[..breakIndex]).Append('\n');

            // Advance past the break. A CR followed by LF is a single break (\r\n), matching the
            // original Replace("\r\n", "\n") normalization; a lone CR or LF is one break too.
            var breakChar = remaining[breakIndex];
            var next = start + breakIndex + 1;
            if (breakChar == '\r' && next < span.Length && span[next] == '\n')
            {
                next++;
            }
            start = next;
        }
    }

    /// <summary>
    /// Append <paramref name="value"/> with any <c>\r</c> and <c>\n</c> removed, matching the SSE rule
    /// that an id or event name occupies a single line. The common newline-free value is appended in
    /// one call with no intermediate allocation.
    /// </summary>
    private static void AppendSingleLine(StringBuilder builder, string value)
    {
        var span = value.AsSpan();
        var breakIndex = span.IndexOfAny('\r', '\n');
        if (breakIndex < 0)
        {
            builder.Append(value);
            return;
        }

        var start = 0;
        while (breakIndex >= 0)
        {
            builder.Append(span.Slice(start, breakIndex));
            start += breakIndex + 1;
            breakIndex = span[start..].IndexOfAny('\r', '\n');
        }
        builder.Append(span[start..]);
    }
}
