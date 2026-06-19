namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// A topic-based broadcast hub for Server-Sent Events. Producers publish to a topic; every current
/// subscriber to that topic receives the event through its own bounded buffer, so one slow client
/// cannot block the producer or the other clients.
/// </summary>
public interface ISseHub
{
    /// <summary>
    /// Subscribe to a topic. The returned subscription delivers events through its reader; dispose
    /// it to unsubscribe.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    StreamSubscription Subscribe(string topic);

    /// <summary>
    /// Subscribe to a topic, optionally resuming after a client-supplied <c>Last-Event-ID</c>.
    /// </summary>
    /// <remarks>
    /// Resume matches <paramref name="lastEventId"/> against the id that was emitted on the wire for
    /// each buffered event: the producer-supplied <see cref="ServerSentEvent.Id"/> if the event set
    /// one, otherwise the hub-assigned monotonic sequence. When <paramref name="lastEventId"/>
    /// exactly equals the wire id of some retained entry, the events published after that entry are
    /// replayed into the subscription before live events flow, so the client misses nothing across a
    /// reconnect. This is true whether the producer relied on the hub sequence or set its own ids:
    /// the id the browser sends back is the id it last saw on the wire, and that is exactly what is
    /// matched here. In every other case the subscription starts from now with no replay (the
    /// from-now fallback):
    /// <list type="bullet">
    /// <item><description><paramref name="lastEventId"/> is null or empty.</description></item>
    /// <item><description>It matches no retained entry's wire id: the entry was evicted (older than
    /// the buffer still holds), or the id names a position the buffer never saw.</description></item>
    /// </list>
    /// The from-now fallback never replays a partial or gapped backlog: a client either resumes
    /// exactly or starts clean. Replay is bounded by
    /// <see cref="StreamOptions.ReplayBufferCapacity"/>; replayed events count against the
    /// subscriber buffer like any other event.
    /// </remarks>
    /// <param name="topic">The topic to subscribe to.</param>
    /// <param name="lastEventId">The client's last seen event id, or null to start from now.</param>
    StreamSubscription Subscribe(string topic, string? lastEventId);

    /// <summary>
    /// Publish an event to every current subscriber of a topic. Never blocks on a slow subscriber:
    /// when a subscriber buffer is full the oldest event in it is dropped to admit the newest.
    /// </summary>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="evt">The event to publish.</param>
    /// <returns>The number of subscribers the event was delivered to.</returns>
    int Publish(string topic, ServerSentEvent evt);

    /// <summary>The number of current subscribers to a topic.</summary>
    /// <param name="topic">The topic to count.</param>
    int SubscriberCount(string topic);
}
