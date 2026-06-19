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
    /// <para>
    /// Resume contract: only the hub-assigned monotonic <see cref="SequenceId"/> is a valid resume
    /// cursor. A producer-supplied <see cref="Id"/> is emitted verbatim on the wire but is NOT
    /// parsed back into a buffer position, so a client that reconnects with a producer id resumes
    /// from now (no replay) rather than risking a wrong or partial backfill. See
    /// <see cref="ISseHub.Subscribe(string, string?)"/>.
    /// </para>
    /// </remarks>
    public string? Id { get; init; }

    /// <summary>
    /// The topic-monotonic id <see cref="SseHub"/> assigns to this delivery on publish, or null if
    /// this instance has not been stamped by a hub. The hub never mutates the producer's instance:
    /// it stamps a per-delivery copy (see <see cref="WithSequence(long)"/>) so the same source
    /// instance published more than once cannot have its id overwritten. <see cref="Id"/> takes
    /// precedence over this value when both are present; <see cref="EffectiveId"/> resolves the wire
    /// id.
    /// </summary>
    internal long? SequenceId { get; private init; }

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

    /// <summary>
    /// Produce a per-delivery copy of this event carrying the hub-assigned monotonic sequence. The
    /// source instance is never mutated, so publishing the same instance again (to this or another
    /// topic) yields an independent delivery with its own correct wire id.
    /// </summary>
    /// <param name="sequence">The topic-monotonic sequence assigned to this delivery.</param>
    /// <returns>A new event identical to this one but stamped with <paramref name="sequence"/>.</returns>
    internal ServerSentEvent WithSequence(long sequence) => new()
    {
        Data = Data,
        EventName = EventName,
        Id = Id,
        RetryMilliseconds = RetryMilliseconds,
        SequenceId = sequence,
    };
}
