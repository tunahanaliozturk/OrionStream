namespace Moongazing.OrionStream.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

using Moongazing.OrionStream;
using Moongazing.OrionStream.AspNetCore;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class SseEndpointRouteBuilderTests
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

    private static (IServiceProvider Services, RequestDelegate Handler) MapEndpoint(
        Action<IEndpointRouteBuilder> map)
    {
        var services = new ServiceCollection();
        services.AddOrionStream();
        services.AddRouting();
        var provider = services.BuildServiceProvider();

        var builder = new TestEndpointRouteBuilder(provider);
        map(builder);

        // The mapped endpoint is a RouteEndpoint; pull its request delegate out to drive directly.
        var endpoint = builder.DataSources
            .SelectMany(ds => ds.Endpoints)
            .OfType<RouteEndpoint>()
            .Single();

        return (provider, endpoint.RequestDelegate!);
    }

    private static async Task<(string Text, DefaultHttpContext Context)> DriveAsync(
        IServiceProvider services,
        RequestDelegate handler,
        Action<HttpContext> arrange,
        Func<HttpContext, Task> afterStart,
        Func<string, bool> until)
    {
        var context = new DefaultHttpContext { RequestServices = services };
        var capture = new CapturingStream();
        context.Response.Body = capture;

        using var cts = new CancellationTokenSource();
        context.RequestAborted = cts.Token;
        arrange(context);

        var handlerTask = handler(context);

        await afterStart(context);

        for (var i = 0; i < 300 && !until(capture.Text); i++)
        {
            await Task.Delay(10);
        }

        await cts.CancelAsync();
        await handlerTask;

        return (capture.Text, context);
    }

    [Fact]
    public async Task Fixed_topic_endpoint_serves_an_sse_stream_with_the_right_headers()
    {
        var (services, handler) = MapEndpoint(e => e.MapServerSentEvents("/events/orders", "orders"));
        var hub = services.GetRequiredService<ISseHub>();

        var (text, context) = await DriveAsync(
            services,
            handler,
            arrange: _ => { },
            // Publish AFTER the handler started so the endpoint's subscribe is live for the event.
            afterStart: async _ =>
            {
                await WaitForSubscriberAsync(hub, "orders");
                hub.Publish("orders", new ServerSentEvent { Id = "1", Data = "hello" });
            },
            until: t => t.Contains("data: hello", StringComparison.Ordinal));

        Assert.Equal("text/event-stream", context.Response.ContentType);
        Assert.Equal("no-cache", context.Response.Headers.CacheControl.ToString());
        Assert.Contains("id: 1\ndata: hello\n\n", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Route_topic_selector_streams_the_per_request_topic()
    {
        var (services, handler) = MapEndpoint(e =>
            e.MapServerSentEvents("/events/{topic}", ctx => (string?)ctx.Request.RouteValues["topic"]));
        var hub = services.GetRequiredService<ISseHub>();

        var (text, _) = await DriveAsync(
            services,
            handler,
            arrange: ctx => ctx.Request.RouteValues["topic"] = "invoices",
            afterStart: async _ =>
            {
                await WaitForSubscriberAsync(hub, "invoices");
                hub.Publish("invoices", new ServerSentEvent { Data = "inv" });
            },
            until: t => t.Contains("data: inv", StringComparison.Ordinal));

        Assert.Contains("data: inv", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Endpoint_resumes_from_last_event_id_header()
    {
        var (services, handler) = MapEndpoint(e => e.MapServerSentEvents("/events/orders", "orders"));
        var hub = services.GetRequiredService<ISseHub>();

        // Publish a backlog BEFORE the request subscribes, so resume must replay from the buffer.
        hub.Publish("orders", new ServerSentEvent { Data = "one" });   // id 1
        hub.Publish("orders", new ServerSentEvent { Data = "two" });   // id 2
        hub.Publish("orders", new ServerSentEvent { Data = "three" }); // id 3

        var (text, _) = await DriveAsync(
            services,
            handler,
            arrange: ctx => ctx.Request.Headers["Last-Event-ID"] = "1",
            afterStart: _ => Task.CompletedTask,
            until: t => t.Contains("data: three", StringComparison.Ordinal));

        // Resume after id 1 replays 2 and 3, and not 1.
        Assert.Contains("data: two", text, StringComparison.Ordinal);
        Assert.Contains("data: three", text, StringComparison.Ordinal);
        Assert.DoesNotContain("data: one", text, StringComparison.Ordinal);
    }

    private static async Task WaitForSubscriberAsync(ISseHub hub, string topic)
    {
        for (var i = 0; i < 300 && hub.SubscriberCount(topic) == 0; i++)
        {
            await Task.Delay(10);
        }
    }

    [Fact]
    public async Task Empty_topic_from_selector_is_a_bad_request()
    {
        var (services, handler) = MapEndpoint(e =>
            e.MapServerSentEvents("/events/{topic}", _ => null));

        var context = new DefaultHttpContext { RequestServices = services };
        context.Response.Body = new CapturingStream();
        await handler(context);

        Assert.Equal(StatusCodes.Status400BadRequest, context.Response.StatusCode);
    }

    /// <summary>
    /// A minimal <see cref="IEndpointRouteBuilder"/> for collecting mapped endpoints without spinning
    /// up a host, so the endpoint helper's request delegate can be driven directly in-process.
    /// </summary>
    private sealed class TestEndpointRouteBuilder : IEndpointRouteBuilder
    {
        public TestEndpointRouteBuilder(IServiceProvider serviceProvider)
        {
            ServiceProvider = serviceProvider;
        }

        public IServiceProvider ServiceProvider { get; }

        public ICollection<EndpointDataSource> DataSources { get; } = new List<EndpointDataSource>();

        public IApplicationBuilder CreateApplicationBuilder() => new ApplicationBuilder(ServiceProvider);
    }
}
