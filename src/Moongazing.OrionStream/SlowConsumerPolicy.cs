namespace Moongazing.OrionStream;

/// <summary>
/// An opt-in policy that disconnects a subscriber whose buffer stays saturated past a threshold,
/// so a wedged client is shed instead of fed a permanently lossy stream. Disabled by default: a
/// saturated subscriber keeps dropping events under its <see cref="FullBufferPolicy"/> and the hub
/// never disconnects it.
/// </summary>
/// <remarks>
/// The hub counts how many consecutive publishes found a given subscriber's buffer full. The count
/// resets to zero the moment a publish finds the buffer with room, so a subscriber that briefly
/// saturates and then catches up is never disconnected. Only a subscriber that is full on
/// <see cref="MaxConsecutiveFullPublishes"/> publishes in a row is disconnected: its channel is
/// completed and its subscription's reader observes completion, exactly as if the client had
/// disposed. The window is measured in publishes rather than wall-clock time so the behavior is
/// deterministic and independent of publish rate.
/// </remarks>
public sealed class SlowConsumerPolicy
{
    /// <summary>
    /// The number of consecutive publishes that must each find a subscriber's buffer full before that
    /// subscriber is disconnected. Must be at least 1. A single publish that finds room resets the run.
    /// </summary>
    public int MaxConsecutiveFullPublishes { get; set; } = 64;

    internal void Validate()
    {
        if (MaxConsecutiveFullPublishes < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxConsecutiveFullPublishes),
                MaxConsecutiveFullPublishes,
                "MaxConsecutiveFullPublishes must be at least 1.");
        }
    }
}
