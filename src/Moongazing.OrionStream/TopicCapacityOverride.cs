namespace Moongazing.OrionStream;

/// <summary>
/// A per-topic override of the global subscriber and replay buffer sizes, so a busy topic can carry
/// a larger buffer than the global default without raising it for every topic. A null member leaves
/// that dimension at the global <see cref="StreamOptions"/> default.
/// </summary>
public sealed class TopicCapacityOverride
{
    /// <summary>
    /// The subscriber buffer size for this topic, or null to use
    /// <see cref="StreamOptions.SubscriberCapacity"/>. When set, must be at least 1.
    /// </summary>
    public int? SubscriberCapacity { get; set; }

    /// <summary>
    /// The replay buffer size for this topic, or null to use
    /// <see cref="StreamOptions.ReplayBufferCapacity"/>. When set, must be zero or greater (0 disables
    /// replay for this topic).
    /// </summary>
    public int? ReplayBufferCapacity { get; set; }

    internal void Validate(string topic)
    {
        // CA2208 wants paramName to name one of this method's parameters, but the genuinely invalid
        // argument is the override property the caller set on this instance, not the topic key. Naming
        // the property is what makes the thrown ParamName actionable for the caller, so the rule is
        // suppressed for these two throws only.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
        if (SubscriberCapacity is { } sub && sub < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(SubscriberCapacity), sub,
                $"SubscriberCapacity override for topic '{topic}' must be at least 1.");
        }
        if (ReplayBufferCapacity is { } replay && replay < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ReplayBufferCapacity), replay,
                $"ReplayBufferCapacity override for topic '{topic}' must be zero or greater.");
        }
#pragma warning restore CA2208 // Instantiate argument exceptions correctly
    }
}
