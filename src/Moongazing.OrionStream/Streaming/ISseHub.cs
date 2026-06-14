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
