namespace Moongazing.OrionStream.Tests;

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;

using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseResponseWriterExtraTests
{
    private sealed class CapturingStream : Stream
    {
        private readonly object gate = new();
        private readonly MemoryStream inner = new();

        public string Text
        {
            get { lock (gate) { return Encoding.UTF8.GetString(inner.ToArray()); } }
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => throw new NotSupportedException(); }

        public override void Write(byte[] buffer, int offset, int count)
        {
            lock (gate) { inner.Write(buffer, offset, count); }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            lock (gate) { inner.Write(buffer, offset, count); }
            return Task.CompletedTask;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            lock (gate) { inner.Write(buffer.Span); }
            return ValueTask.CompletedTask;
        }

        public override void Flush() { }
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        for (var i = 0; i < 300 && !condition(); i++)
        {
            await Task.Delay(10);
        }
    }

    [Fact]
    public async Task Null_response_throws()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ((HttpResponse)null!).WriteStreamAsync(sub, TimeSpan.FromSeconds(1), CancellationToken.None));
    }

    [Fact]
    public async Task Null_subscription_throws()
    {
        var context = new DefaultHttpContext();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            context.Response.WriteStreamAsync(null!, TimeSpan.FromSeconds(1), CancellationToken.None));
    }

    [Fact]
    public async Task The_sse_headers_are_set()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        var context = new DefaultHttpContext();
        context.Response.Body = new CapturingStream();

        using var cts = new CancellationTokenSource();
        var writeTask = context.Response.WriteStreamAsync(sub, TimeSpan.FromSeconds(30), cts.Token);

        await cts.CancelAsync();
        await writeTask;

        Assert.Equal("text/event-stream", context.Response.ContentType);
        Assert.Equal("no-cache", context.Response.Headers.CacheControl.ToString());
        Assert.Equal("no", context.Response.Headers["X-Accel-Buffering"].ToString());
    }

    [Fact]
    public async Task An_already_cancelled_token_writes_no_events_but_sets_headers()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");
        hub.Publish("orders", new ServerSentEvent { Data = "never-read" });

        var context = new DefaultHttpContext();
        var capture = new CapturingStream();
        context.Response.Body = capture;

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await context.Response.WriteStreamAsync(sub, TimeSpan.FromSeconds(30), cts.Token);

        Assert.Equal("text/event-stream", context.Response.ContentType);
        Assert.DoesNotContain("data:", capture.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_idle_stream_emits_a_heartbeat()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        var context = new DefaultHttpContext();
        var capture = new CapturingStream();
        context.Response.Body = capture;

        using var cts = new CancellationTokenSource();
        var writeTask = context.Response.WriteStreamAsync(sub, TimeSpan.FromMilliseconds(20), cts.Token);

        await WaitUntil(() => capture.Text.Contains(": heartbeat", StringComparison.Ordinal));

        await cts.CancelAsync();
        await writeTask;

        Assert.Contains(": heartbeat\n\n", capture.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_multi_line_event_is_written_in_wire_format()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");
        hub.Publish("orders", new ServerSentEvent
        {
            Id = "9",
            EventName = "update",
            Data = "line1\nline2",
        });

        var context = new DefaultHttpContext();
        var capture = new CapturingStream();
        context.Response.Body = capture;

        using var cts = new CancellationTokenSource();
        var writeTask = context.Response.WriteStreamAsync(sub, TimeSpan.FromSeconds(30), cts.Token);

        await WaitUntil(() => capture.Text.Contains("data: line2", StringComparison.Ordinal));

        await cts.CancelAsync();
        await writeTask;

        Assert.Contains("id: 9\nevent: update\ndata: line1\ndata: line2\n\n", capture.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Completing_the_subscription_ends_the_stream()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        var sub = hub.Subscribe("orders");
        hub.Publish("orders", new ServerSentEvent { Data = "one" });

        var context = new DefaultHttpContext();
        var capture = new CapturingStream();
        context.Response.Body = capture;

        // A long heartbeat ensures the loop is waiting on the reader, not the timer, when we dispose.
        using var cts = new CancellationTokenSource();
        var writeTask = context.Response.WriteStreamAsync(sub, TimeSpan.FromSeconds(30), cts.Token);

        await WaitUntil(() => capture.Text.Contains("data: one", StringComparison.Ordinal));

        // Disposing completes the channel; WaitToReadAsync resolves false and the writer returns.
        sub.Dispose();

        await writeTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(writeTask.IsCompletedSuccessfully);
        Assert.Contains("data: one\n\n", capture.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Events_published_after_streaming_starts_are_delivered()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");

        var context = new DefaultHttpContext();
        var capture = new CapturingStream();
        context.Response.Body = capture;

        using var cts = new CancellationTokenSource();
        var writeTask = context.Response.WriteStreamAsync(sub, TimeSpan.FromSeconds(30), cts.Token);

        hub.Publish("orders", new ServerSentEvent { Data = "live" });
        await WaitUntil(() => capture.Text.Contains("data: live", StringComparison.Ordinal));

        await cts.CancelAsync();
        await writeTask;

        Assert.Contains("data: live\n\n", capture.Text, StringComparison.Ordinal);
    }
}
