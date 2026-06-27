namespace Moongazing.OrionStream;

using System.Text.Json;

/// <summary>
/// Configuration for the broadcast hub and the SSE writer: how much each subscriber may buffer
/// and how often a heartbeat is sent on an idle connection.
/// </summary>
public sealed class StreamOptions
{
    /// <summary>
    /// The serializer used by the typed publish helpers to render a payload to the SSE
    /// <c>data:</c> field. Defaults to <see cref="JsonSerializerDefaults.Web"/> options. Replace it to
    /// control naming, casing, or converters; the typed publish helpers also accept a per-call
    /// override.
    /// </summary>
    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// The bounded buffer size per subscriber. When a subscriber falls this far behind, the
    /// oldest buffered event is dropped to make room for the newest. Default 256.
    /// </summary>
    public int SubscriberCapacity { get; set; } = 256;

    /// <summary>
    /// How often a heartbeat comment is written to an idle connection to keep proxies from closing
    /// it. Default 15 seconds.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// How many of the most recent events per topic are retained for replay when a client resumes
    /// with a <c>Last-Event-ID</c>. The hub keeps the newest <see cref="ReplayBufferCapacity"/>
    /// events; resume matches the client's <c>Last-Event-ID</c> against the id each retained event
    /// emitted on the wire (the producer-supplied <see cref="Streaming.ServerSentEvent.Id"/> if set,
    /// otherwise the hub-assigned topic-monotonic sequence). A match replays only the events after
    /// that entry, so both producer ids and hub sequences round-trip through resume; an id that
    /// matches no retained entry (unknown or evicted) falls back to a from-now stream with no replay.
    /// Set to 0 to disable replay entirely. Default 256.
    /// </summary>
    public int ReplayBufferCapacity { get; set; } = 256;

    internal void Validate()
    {
        if (SubscriberCapacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(SubscriberCapacity), SubscriberCapacity,
                "SubscriberCapacity must be at least 1.");
        }
        if (HeartbeatInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(HeartbeatInterval), HeartbeatInterval,
                "HeartbeatInterval must be positive.");
        }
        if (ReplayBufferCapacity < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(ReplayBufferCapacity), ReplayBufferCapacity,
                "ReplayBufferCapacity must be zero or greater.");
        }
    }
}
