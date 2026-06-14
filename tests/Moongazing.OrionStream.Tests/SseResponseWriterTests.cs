namespace Moongazing.OrionStream.Tests;

using System.Text;

using Microsoft.AspNetCore.Http;

using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseResponseWriterTests
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

    [Fact]
    public async Task Streams_buffered_events_in_the_sse_wire_format()
    {
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);
        using var sub = hub.Subscribe("orders");
        hub.Publish("orders", new ServerSentEvent { Id = "1", Data = "one" });
        hub.Publish("orders", new ServerSentEvent { Id = "2", Data = "two" });

        var context = new DefaultHttpContext();
        var capture = new CapturingStream();
        context.Response.Body = capture;

        using var cts = new CancellationTokenSource();
        var writeTask = context.Response.WriteStreamAsync(sub, TimeSpan.FromSeconds(30), cts.Token);

        for (var i = 0; i < 200 && !capture.Text.Contains("data: two", StringComparison.Ordinal); i++)
        {
            await Task.Delay(10);
        }

        await cts.CancelAsync();
        await writeTask;

        Assert.Equal("text/event-stream", context.Response.ContentType);
        Assert.Contains("id: 1\ndata: one\n\n", capture.Text, StringComparison.Ordinal);
        Assert.Contains("id: 2\ndata: two\n\n", capture.Text, StringComparison.Ordinal);
    }
}
