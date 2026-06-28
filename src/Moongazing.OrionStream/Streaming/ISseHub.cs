namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// A topic-based broadcast hub for Server-Sent Events. Producers publish to a topic; every current
/// subscriber to that topic receives the event through its own bounded buffer, so one slow client
/// cannot block the producer or the other clients.
/// </summary>
/// <remarks>
/// <para>
/// <b>Event-id allocation contract.</b> This is a stated contract, not an implementation detail, and
/// resume (here and in any replay store behind <see cref="IReplayStore"/>) relies on it.
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Hub sequence.</b> On every <see cref="Publish(string, ServerSentEvent)"/> the hub assigns the
/// topic a sequence number that is <i>strictly increasing by one with no gaps</i>, starting at 1 for
/// the first event published to that topic. The first event is sequence 1, the second is 2, and so on.
/// A sequence is assigned to every published event whether or not the producer also set its own
/// <see cref="ServerSentEvent.Id"/>; setting a producer id does not skip or perturb the sequence.
/// </description></item>
/// <item><description>
/// <b>Monotonicity scope is per topic.</b> Each topic has its own independent sequence. Sequence
/// numbers are comparable <i>only within one topic</i>; comparing a sequence from topic A against one
/// from topic B is meaningless. Two topics can and do reuse the same sequence values.
/// </description></item>
/// <item><description>
/// <b>Producer id always wins on the wire.</b> The id rendered in the SSE <c>id:</c> field (the
/// "wire id") is the producer-supplied <see cref="ServerSentEvent.Id"/> when the event set one, and
/// the hub sequence (as a decimal string) otherwise. The hub never overwrites a producer id and never
/// emits both; the sequence is still assigned underneath but is not what the client sees when a
/// producer id is present.
/// </description></item>
/// <item><description>
/// <b>Ordering guarantee.</b> Within one topic, the hub delivers and retains events in ascending
/// sequence order, which is the order <see cref="Publish(string, ServerSentEvent)"/> was called for
/// that topic. The sequence is the ordering key for resume regardless of which wire id an event
/// carried.
/// </description></item>
/// </list>
/// <para>
/// <b>What a consumer may assume when producer ids and hub sequences mix on one topic.</b> A single
/// topic may carry a mixture: some events with producer-supplied wire ids, others with hub-sequence
/// wire ids, interleaved in any order. In that case:
/// </para>
/// <list type="bullet">
/// <item><description>
/// The wire id a client last saw, sent back as <c>Last-Event-ID</c>, is matched for resume against the
/// exact wire id each retained event emitted, whichever kind it was (see
/// <see cref="Subscribe(string, string?)"/>). Both kinds round-trip through resume identically.
/// </description></item>
/// <item><description>
/// A consumer must <i>not</i> assume wire ids are globally ordered, numeric, or comparable across the
/// two kinds. A producer id is an opaque string the producer chose; only the underlying hub sequence is
/// ordered, and it is not on the wire when a producer id is present. So a consumer cannot infer "later"
/// from one wire id versus another when the two ids are of different kinds, nor parse a producer id as
/// a number. Ordering is the hub's responsibility (delivery order equals publish order); the wire id is
/// for resume matching, not for client-side ordering.
/// </description></item>
/// <item><description>
/// Producer ids are not required to be unique. If a producer reuses a wire id, resume matches the
/// <i>oldest</i> retained entry carrying that id, since the match scans from oldest to newest. A
/// producer that wants deterministic resume should keep its ids unique within the topic's replay
/// window.
/// </description></item>
/// </list>
/// </remarks>
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
    /// Subscribe to a topic with an optional <paramref name="filter"/> and optional resume. The
    /// filter is evaluated once per publish for this subscriber, before the event enters the
    /// subscriber's buffer, so events the filter rejects never consume buffer space.
    /// </summary>
    /// <remarks>
    /// The filter is evaluated against the per-delivery event the wire would carry (id stamped),
    /// before the buffer admits it, so a chatty topic does not fill a slow client's buffer with events
    /// it would discard. A null filter accepts every event, matching the unfiltered overloads. The
    /// filter also applies to replayed events on resume: only matching backlog entries are replayed.
    /// Resume semantics are otherwise identical to <see cref="Subscribe(string, string?)"/>. The
    /// filter should be cheap and must not throw; it runs inside the publish path.
    /// </remarks>
    /// <param name="topic">The topic to subscribe to.</param>
    /// <param name="lastEventId">The client's last seen event id, or null to start from now.</param>
    /// <param name="filter">
    /// A predicate run per event; only events for which it returns true are delivered to this
    /// subscriber. Null delivers every event.
    /// </param>
    StreamSubscription Subscribe(string topic, string? lastEventId, Func<ServerSentEvent, bool>? filter);

    /// <summary>
    /// Publish an event to every current subscriber of a topic. By default never blocks on a slow
    /// subscriber: when a subscriber buffer is full the oldest event in it is dropped to admit the
    /// newest. A non-default <see cref="StreamOptions.FullBufferPolicy"/> can change that to dropping
    /// the newest or, for <see cref="FullBufferPolicy.Wait"/>, waiting up to
    /// <see cref="StreamOptions.MaxPublishWait"/> (which can apply back-pressure to this call).
    /// </summary>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="evt">The event to publish.</param>
    /// <returns>The number of subscribers the event was delivered to.</returns>
    int Publish(string topic, ServerSentEvent evt);

    /// <summary>The number of current subscribers to a topic.</summary>
    /// <param name="topic">The topic to count.</param>
    int SubscriberCount(string topic);
}
