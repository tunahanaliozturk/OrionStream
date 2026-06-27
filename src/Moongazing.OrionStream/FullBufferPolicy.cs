namespace Moongazing.OrionStream;

/// <summary>
/// What the hub does when a subscriber's bounded buffer is full at publish time. The default
/// <see cref="DropOldest"/> preserves the historical never-blocks behavior; the other policies are
/// opt-in.
/// </summary>
public enum FullBufferPolicy
{
    /// <summary>
    /// Evict the oldest buffered event to admit the newest. The publish never blocks. This is the
    /// default and the behavior every version before 0.4.0 had.
    /// </summary>
    DropOldest = 0,

    /// <summary>
    /// Keep the buffered events and discard the newest event instead. The publish never blocks; a
    /// saturated subscriber holds the events it already has rather than rolling forward to the latest.
    /// </summary>
    DropNewest = 1,

    /// <summary>
    /// Wait for room in the subscriber buffer rather than dropping, up to
    /// <see cref="StreamOptions.MaxPublishWait"/>, then give up on that subscriber for this publish.
    /// </summary>
    /// <remarks>
    /// This is the one policy that can apply back-pressure to the publisher: a slow reader can stall
    /// <see cref="Streaming.ISseHub.Publish(string, Streaming.ServerSentEvent)"/> for up to the
    /// configured cap per saturated subscriber. It deliberately breaks the never-blocks guarantee that
    /// <see cref="DropOldest"/> and <see cref="DropNewest"/> keep, so it requires an explicit
    /// <see cref="StreamOptions.MaxPublishWait"/> and should only be chosen when slowing the producer is
    /// preferable to losing events. When the cap elapses with the buffer still full, the event is
    /// dropped for that subscriber (counted like any other drop) and the publish proceeds.
    /// </remarks>
    Wait = 2,
}
