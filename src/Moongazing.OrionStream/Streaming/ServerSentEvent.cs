namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// One Server-Sent Event: the data payload plus the optional metadata fields the SSE protocol
/// defines. A heartbeat is represented as a comment line and produced by the writer, not by this
/// type.
/// </summary>
public sealed class ServerSentEvent
{
    /// <summary>The event payload. Multi-line data is emitted as multiple <c>data:</c> lines.</summary>
    public required string Data { get; init; }

    /// <summary>
    /// The event name a browser <c>EventSource</c> dispatches on (the <c>event:</c> field), or
    /// null for the default <c>message</c> event.
    /// </summary>
    public string? EventName { get; init; }

    /// <summary>
    /// The event id (the <c>id:</c> field). When set, the browser sends it back as
    /// <c>Last-Event-ID</c> on reconnect so a server can resume.
    /// </summary>
    public string? Id { get; init; }

    /// <summary>
    /// The reconnection time in milliseconds (the <c>retry:</c> field) the client should wait
    /// before reconnecting, or null to leave the client default.
    /// </summary>
    public int? RetryMilliseconds { get; init; }
}
