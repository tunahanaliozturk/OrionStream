namespace Moongazing.OrionStream;

using System.Collections.Generic;
using System.Text.Json;

/// <summary>
/// Configuration for the broadcast hub and the SSE writer: how much each subscriber may buffer
/// and how often a heartbeat is sent on an idle connection.
/// </summary>
public sealed class StreamOptions
{
    private readonly Dictionary<string, TopicCapacityOverride> topicOverrides =
        new(StringComparer.Ordinal);

    /// <summary>
    /// The serializer used by the typed publish helpers to render a payload to the SSE
    /// <c>data:</c> field. Defaults to <see cref="JsonSerializerDefaults.Web"/> options. Replace it to
    /// control naming, casing, or converters; the typed publish helpers also accept a per-call
    /// override.
    /// </summary>
    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// The bounded buffer size per subscriber. When a subscriber falls this far behind, the
    /// configured <see cref="FullBufferPolicy"/> decides what happens (drop the oldest by default).
    /// Default 256. A per-topic override (see <see cref="ConfigureTopic"/>) can raise or lower it for
    /// one topic.
    /// </summary>
    public int SubscriberCapacity { get; set; } = 256;

    /// <summary>
    /// What the hub does when a subscriber buffer is full at publish time. Default
    /// <see cref="FullBufferPolicy.DropOldest"/>, which is the historical never-blocks behavior.
    /// Choosing <see cref="FullBufferPolicy.Wait"/> can apply back-pressure to the publisher and
    /// requires <see cref="MaxPublishWait"/> to be set.
    /// </summary>
    public FullBufferPolicy FullBufferPolicy { get; set; } = FullBufferPolicy.DropOldest;

    /// <summary>
    /// The maximum time a single <see cref="Streaming.ISseHub.Publish(string, Streaming.ServerSentEvent)"/>
    /// will wait for room in a saturated subscriber buffer before giving up on that subscriber, when
    /// <see cref="FullBufferPolicy"/> is <see cref="FullBufferPolicy.Wait"/>. Required and must be
    /// positive for that policy; ignored for the drop policies. This is the explicit cap on how long
    /// a slow reader can stall the producer.
    /// </summary>
    public TimeSpan? MaxPublishWait { get; set; }

    /// <summary>
    /// An optional policy that disconnects a subscriber whose buffer stays saturated past a threshold.
    /// Null (the default) means a saturated subscriber is never disconnected by the hub; it keeps
    /// dropping under its <see cref="FullBufferPolicy"/>.
    /// </summary>
    public SlowConsumerPolicy? SlowConsumerPolicy { get; set; }

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
    /// Set to 0 to disable replay entirely. Default 256. A per-topic override (see
    /// <see cref="ConfigureTopic"/>) can raise or lower it for one topic.
    /// </summary>
    public int ReplayBufferCapacity { get; set; } = 256;

    /// <summary>
    /// Override the subscriber and/or replay buffer capacity for a single topic, leaving every other
    /// topic at the global default. Call more than once for the same topic to amend the override.
    /// </summary>
    /// <param name="topic">The topic to configure. Case-sensitive, matching the hub's topic comparison.</param>
    /// <param name="configure">Mutates the override for this topic.</param>
    /// <returns>This instance, for chaining.</returns>
    public StreamOptions ConfigureTopic(string topic, Action<TopicCapacityOverride> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(configure);

        if (!topicOverrides.TryGetValue(topic, out var ovr))
        {
            ovr = new TopicCapacityOverride();
            topicOverrides[topic] = ovr;
        }
        configure(ovr);
        return this;
    }

    /// <summary>The effective subscriber buffer capacity for a topic, honoring any per-topic override.</summary>
    internal int SubscriberCapacityFor(string topic) =>
        topicOverrides.TryGetValue(topic, out var ovr) && ovr.SubscriberCapacity is { } c
            ? c
            : SubscriberCapacity;

    /// <summary>The effective replay buffer capacity for a topic, honoring any per-topic override.</summary>
    internal int ReplayBufferCapacityFor(string topic) =>
        topicOverrides.TryGetValue(topic, out var ovr) && ovr.ReplayBufferCapacity is { } c
            ? c
            : ReplayBufferCapacity;

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
        if (FullBufferPolicy == FullBufferPolicy.Wait && MaxPublishWait is not { } wait)
        {
            throw new ArgumentException(
                "FullBufferPolicy.Wait requires MaxPublishWait to be set: a wait policy must cap how long a slow subscriber can stall the publisher.",
                nameof(MaxPublishWait));
        }
        if (MaxPublishWait is { } configured && configured <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxPublishWait), configured,
                "MaxPublishWait must be positive when set.");
        }
        SlowConsumerPolicy?.Validate();
        foreach (var (topic, ovr) in topicOverrides)
        {
            ovr.Validate(topic);
        }
    }
}
