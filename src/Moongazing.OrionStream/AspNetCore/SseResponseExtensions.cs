namespace Moongazing.OrionStream.AspNetCore;

using System.Text;

using Microsoft.AspNetCore.Http;

using Moongazing.OrionStream.Streaming;

/// <summary>
/// Writes a subscription's events to an HTTP response as a <c>text/event-stream</c>, sending a
/// heartbeat comment when the stream is idle so intermediaries do not close the connection. The
/// call returns when the client disconnects (the cancellation token trips) or the subscription
/// completes.
/// </summary>
public static class SseResponseExtensions
{
    /// <summary>Stream a subscription to the response until the client disconnects.</summary>
    /// <param name="response">The response to write to.</param>
    /// <param name="subscription">The subscription to drain.</param>
    /// <param name="heartbeatInterval">How long to wait before writing a heartbeat on an idle stream.</param>
    /// <param name="cancellationToken">Trips when the client disconnects; ends the stream.</param>
    public static async Task WriteStreamAsync(
        this HttpResponse response,
        StreamSubscription subscription,
        TimeSpan heartbeatInterval,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentNullException.ThrowIfNull(subscription);

        response.ContentType = "text/event-stream";
        response.Headers.CacheControl = "no-cache";
        response.Headers["X-Accel-Buffering"] = "no";

        await response.Body.FlushAsync(cancellationToken).ConfigureAwait(false);

        var reader = subscription.Reader;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var readTask = reader.WaitToReadAsync(cancellationToken).AsTask();
                var delayTask = Task.Delay(heartbeatInterval, cancellationToken);
                var completed = await Task.WhenAny(readTask, delayTask).ConfigureAwait(false);

                if (completed == delayTask)
                {
                    await WriteAsync(response, SseFormatter.Heartbeat, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (!await readTask.ConfigureAwait(false))
                {
                    break; // the subscription completed
                }

                while (reader.TryRead(out var evt))
                {
                    await WriteAsync(response, SseFormatter.Format(evt), cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The client disconnected; ending the stream is the expected outcome.
        }
    }

    private static async Task WriteAsync(HttpResponse response, string payload, CancellationToken cancellationToken)
    {
        await response.Body.WriteAsync(Encoding.UTF8.GetBytes(payload), cancellationToken).ConfigureAwait(false);
        await response.Body.FlushAsync(cancellationToken).ConfigureAwait(false);
    }
}
