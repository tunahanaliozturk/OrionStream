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
    /// <remarks>
    /// A producer may set this explicitly. When left null, <see cref="SseHub"/> stamps a
    /// topic-monotonic id on publish (see <see cref="SequenceId"/>) so that resume via
    /// <c>Last-Event-ID</c> works without the producer assigning ids by hand. A producer-supplied id
    /// always takes precedence over the assigned one on the wire.
    /// </remarks>
    public string? Id { get; init; }

    /// <summary>
    /// The topic-monotonic id <see cref="SseHub"/> assigns to this event on publish, or null if it
    /// has not been published through a hub. The hub stamps this in place so the very same instance
    /// is broadcast to every subscriber and retained for replay. <see cref="Id"/> takes precedence
    /// over this value when both are present; <see cref="EffectiveId"/> resolves the wire id.
    /// </summary>
    internal long? SequenceId { get; set; }

    /// <summary>
    /// The id rendered on the wire: the producer-supplied <see cref="Id"/> if present, otherwise the
    /// hub-assigned <see cref="SequenceId"/>, otherwise null (no <c>id:</c> line).
    /// </summary>
    internal string? EffectiveId =>
        Id ?? SequenceId?.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// The reconnection time in milliseconds (the <c>retry:</c> field) the client should wait
    /// before reconnecting, or null to leave the client default.
    /// </summary>
    public int? RetryMilliseconds { get; init; }
}
