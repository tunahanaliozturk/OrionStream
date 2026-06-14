namespace Moongazing.OrionStream.Tests;

using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseFormatterTests
{
    [Fact]
    public void Data_only_event_renders_a_single_data_line_and_terminator()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = "hello" });
        Assert.Equal("data: hello\n\n", output);
    }

    [Fact]
    public void All_fields_render_in_canonical_order()
    {
        var output = SseFormatter.Format(new ServerSentEvent
        {
            Id = "42",
            EventName = "tick",
            RetryMilliseconds = 3000,
            Data = "payload",
        });

        Assert.Equal("id: 42\nevent: tick\nretry: 3000\ndata: payload\n\n", output);
    }

    [Fact]
    public void Multi_line_data_becomes_multiple_data_lines()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = "line1\nline2\r\nline3" });
        Assert.Equal("data: line1\ndata: line2\ndata: line3\n\n", output);
    }

    [Fact]
    public void Newlines_in_id_and_event_are_stripped()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Id = "a\nb", EventName = "e\rv", Data = "x" });
        Assert.Equal("id: ab\nevent: ev\ndata: x\n\n", output);
    }

    [Fact]
    public void Heartbeat_is_a_comment_line()
    {
        Assert.StartsWith(":", SseFormatter.Heartbeat, StringComparison.Ordinal);
        Assert.EndsWith("\n\n", SseFormatter.Heartbeat, StringComparison.Ordinal);
    }
}
