namespace Moongazing.OrionStream.AspNetCore;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

using Moongazing.OrionStream.Streaming;

/// <summary>
/// Minimal-API helpers that map an SSE endpoint to the hub in one line, so a consumer does not
/// hand-write the subscribe-then-write handler. The endpoint reads <c>Last-Event-ID</c> off the
/// request, subscribes (resuming when the header is present), and streams the subscription to the
/// response with the correct SSE headers, heartbeats, and cancellation on client disconnect.
/// </summary>
public static class SseEndpointRouteBuilderExtensions
{
    /// <summary>The request header a browser <c>EventSource</c> sends to resume after a reconnect.</summary>
    private const string LastEventIdHeader = "Last-Event-ID";

    /// <summary>
    /// Map a GET endpoint that streams a fixed topic as Server-Sent Events.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder to map onto.</param>
    /// <param name="pattern">The route pattern, for example <c>/events/orders</c>.</param>
    /// <param name="topic">The topic every request to this endpoint subscribes to.</param>
    /// <returns>The mapped endpoint convention builder, for further configuration.</returns>
    public static IEndpointConventionBuilder MapServerSentEvents(
        this IEndpointRouteBuilder endpoints,
        string pattern,
        string topic)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrEmpty(topic);

        return endpoints.MapServerSentEvents(pattern, _ => topic);
    }

    /// <summary>
    /// Map a GET endpoint that streams Server-Sent Events for a topic derived per request, for
    /// example from a route value: <c>MapServerSentEvents("/events/{topic}", ctx =&gt; (string)ctx.Request.RouteValues["topic"]!)</c>.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder to map onto.</param>
    /// <param name="pattern">The route pattern, for example <c>/events/{topic}</c>.</param>
    /// <param name="topicSelector">
    /// Resolves the topic from the request. Return a non-empty topic; an empty or null result is a bad
    /// request.
    /// </param>
    /// <returns>The mapped endpoint convention builder, for further configuration.</returns>
    public static IEndpointConventionBuilder MapServerSentEvents(
        this IEndpointRouteBuilder endpoints,
        string pattern,
        Func<HttpContext, string?> topicSelector)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(topicSelector);

        return endpoints.MapGet(pattern, async (HttpContext context) =>
        {
            var topic = topicSelector(context);
            if (string.IsNullOrEmpty(topic))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var hub = context.RequestServices.GetRequiredService<ISseHub>();
            var options = context.RequestServices.GetRequiredService<StreamOptions>();

            var lastEventId = context.Request.Headers[LastEventIdHeader].ToString();

            using var subscription = hub.Subscribe(topic, string.IsNullOrEmpty(lastEventId) ? null : lastEventId);
            await context.Response
                .WriteStreamAsync(subscription, options.HeartbeatInterval, context.RequestAborted)
                .ConfigureAwait(false);
        });
    }
}
