namespace Moongazing.OrionStream.Tests;

using System;

using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseFormatterEdgeTests
{
    [Fact]
    public void Null_event_throws()
    {
        Assert.Throws<ArgumentNullException>(() => SseFormatter.Format(null!));
    }

    [Fact]
    public void Empty_data_renders_a_single_empty_data_line()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = string.Empty });
        Assert.Equal("data: \n\n", output);
    }

    [Fact]
    public void Event_name_only_renders_event_then_data()
    {
        var output = SseFormatter.Format(new ServerSentEvent { EventName = "tick", Data = "x" });
        Assert.Equal("event: tick\ndata: x\n\n", output);
    }

    [Fact]
    public void Id_only_renders_id_then_data()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Id = "7", Data = "x" });
        Assert.Equal("id: 7\ndata: x\n\n", output);
    }

    [Fact]
    public void Retry_only_renders_retry_then_data()
    {
        var output = SseFormatter.Format(new ServerSentEvent { RetryMilliseconds = 5000, Data = "x" });
        Assert.Equal("retry: 5000\ndata: x\n\n", output);
    }

    [Fact]
    public void Retry_of_zero_is_rendered_because_it_is_not_null()
    {
        var output = SseFormatter.Format(new ServerSentEvent { RetryMilliseconds = 0, Data = "x" });
        Assert.Equal("retry: 0\ndata: x\n\n", output);
    }

    [Fact]
    public void Negative_retry_is_rendered_verbatim()
    {
        // The formatter does not validate retry; it renders whatever int it is given.
        var output = SseFormatter.Format(new ServerSentEvent { RetryMilliseconds = -1, Data = "x" });
        Assert.Equal("retry: -1\ndata: x\n\n", output);
    }

    [Fact]
    public void Crlf_in_data_does_not_double_split()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = "a\r\nb" });
        Assert.Equal("data: a\ndata: b\n\n", output);
    }

    [Fact]
    public void Lone_cr_in_data_splits_the_line()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = "a\rb" });
        Assert.Equal("data: a\ndata: b\n\n", output);
    }

    [Fact]
    public void Trailing_newline_in_data_produces_a_trailing_empty_data_line()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = "a\n" });
        Assert.Equal("data: a\ndata: \n\n", output);
    }

    [Fact]
    public void Leading_newline_in_data_produces_a_leading_empty_data_line()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = "\na" });
        Assert.Equal("data: \ndata: a\n\n", output);
    }

    [Fact]
    public void Special_characters_in_data_pass_through_unescaped()
    {
        const string payload = "{\"k\":\"v\",\"u\":\"üñê\",\"emoji\":\"\U0001F600\"}";
        var output = SseFormatter.Format(new ServerSentEvent { Data = payload });
        Assert.Equal("data: " + payload + "\n\n", output);
    }

    [Fact]
    public void Colon_in_data_is_not_treated_as_a_field_separator()
    {
        var output = SseFormatter.Format(new ServerSentEvent { Data = "key: value" });
        Assert.Equal("data: key: value\n\n", output);
    }

    [Fact]
    public void Newlines_inside_id_and_event_are_stripped_but_data_is_split()
    {
        var output = SseFormatter.Format(new ServerSentEvent
        {
            Id = "1\r\n2",
            EventName = "a\nb",
            Data = "p\nq",
        });
        Assert.Equal("id: 12\nevent: ab\ndata: p\ndata: q\n\n", output);
    }

    [Fact]
    public void Heartbeat_has_the_expected_exact_value()
    {
        Assert.Equal(": heartbeat\n\n", SseFormatter.Heartbeat);
    }
}
