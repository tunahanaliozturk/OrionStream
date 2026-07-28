namespace Moongazing.OrionStream.Diagnostics;

using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

using Moongazing.Orion.Abstractions.Diagnostics;

/// <summary>
/// OpenTelemetry instrumentation for the broadcast hub. Built on the Orion family's
/// <see cref="OrionInstrumentation"/> spine, so it shares the family's naming and static-tag
/// conventions: a <see cref="Meter"/> named <c>Moongazing.OrionStream</c> with published and dropped
/// counters (<c>orion.stream.published</c> / <c>orion.stream.dropped</c>) and a current-subscribers
/// gauge (<c>orion.stream.subscribers</c>), plus an <see cref="System.Diagnostics.ActivitySource"/>
/// of the same name carrying a span around publish and subscribe. The published and dropped counters
/// carry the <c>orion.stream.topic</c> tag so they can be sliced per topic, and multi-tenant /
/// multi-region labels configured through <see cref="OrionInstrumentation.SetStaticTags"/> are
/// stamped onto every measurement. Registered as a singleton; dispose it to release the meter and the
/// activity source.
/// </summary>
public sealed class StreamDiagnostics : OrionInstrumentation
{
    /// <summary>The meter and activity-source name OpenTelemetry consumers subscribe to.</summary>
    public const string MeterName = "Moongazing.OrionStream";

    /// <summary>The tag key carrying the topic on published and dropped measurements and on spans.</summary>
    public const string TopicTagName = "orion.stream.topic";

    private int subscribers;

    /// <summary>Create the meter, its instruments, and the activity source.</summary>
    public StreamDiagnostics()
        : base(OrionTelemetry.ScopeName("OrionStream"), MeterVersion.Value)
    {
        Published = Meter.CreateCounter<long>(
            OrionTelemetry.MetricName("stream", "published"),
            unit: "{event}",
            description: "Events published to the hub (counted once per publish, not per subscriber).");

        Dropped = Meter.CreateCounter<long>(
            OrionTelemetry.MetricName("stream", "dropped"),
            unit: "{event}",
            description: "Events dropped because a subscriber buffer was full at publish time.");

        Meter.CreateObservableGauge(
            OrionTelemetry.MetricName("stream", "subscribers"),
            () => new Measurement<int>(Volatile.Read(ref subscribers), StaticTags),
            unit: "{subscriber}",
            description: "Currently connected subscribers across all topics.");
    }

    /// <summary>Counts published events.</summary>
    public Counter<long> Published { get; }

    /// <summary>Counts dropped events.</summary>
    public Counter<long> Dropped { get; }

    /// <summary>
    /// Record one published event tagged with its topic. Counted once per publish, regardless of how
    /// many subscribers the event reached.
    /// </summary>
    /// <param name="topic">The topic the event was published to.</param>
    public void RecordPublished(string topic) =>
        Published.Add(1, Tag(new KeyValuePair<string, object?>(TopicTagName, topic)));

    /// <summary>
    /// Record <paramref name="count"/> dropped events on a topic, tagged with that topic. A drop is
    /// counted per subscriber buffer that was full at publish time.
    /// </summary>
    /// <param name="topic">The topic the drops occurred on.</param>
    /// <param name="count">The number of drops to record.</param>
    public void RecordDropped(string topic, long count)
    {
        if (count > 0)
        {
            Dropped.Add(count, Tag(new KeyValuePair<string, object?>(TopicTagName, topic)));
        }
    }

    /// <summary>
    /// Start a span around a publish to a topic, or null if no listener is sampling the source. The
    /// caller disposes the returned activity when the publish completes.
    /// </summary>
    /// <param name="topic">The topic being published to.</param>
    public Activity? StartPublish(string topic)
    {
        var activity = ActivitySource.StartActivity("OrionStream.Publish", ActivityKind.Producer);
        activity?.SetTag(TopicTagName, topic);
        return activity;
    }

    /// <summary>
    /// Start a span around a subscribe to a topic, or null if no listener is sampling the source. The
    /// caller disposes the returned activity when the subscribe completes.
    /// </summary>
    /// <param name="topic">The topic being subscribed to.</param>
    public Activity? StartSubscribe(string topic)
    {
        var activity = ActivitySource.StartActivity("OrionStream.Subscribe", ActivityKind.Consumer);
        activity?.SetTag(TopicTagName, topic);
        return activity;
    }

    /// <summary>Record a new subscriber.</summary>
    public void IncrementSubscribers() => Interlocked.Increment(ref subscribers);

    /// <summary>Record a departed subscriber.</summary>
    public void DecrementSubscribers() => Interlocked.Decrement(ref subscribers);
}
