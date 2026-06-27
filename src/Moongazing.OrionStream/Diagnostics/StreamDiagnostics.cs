namespace Moongazing.OrionStream.Diagnostics;

using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

/// <summary>
/// OpenTelemetry instrumentation for the broadcast hub. Exposes a <see cref="Meter"/> named
/// <c>Moongazing.OrionStream</c> with published and dropped counters and a current-subscribers
/// gauge, and an <see cref="System.Diagnostics.ActivitySource"/> of the same name carrying a span
/// around publish and subscribe. The published and dropped counters carry the <c>orionstream.topic</c>
/// tag so they can be sliced per topic. Registered as a singleton; dispose it to release the meter and
/// the activity source.
/// </summary>
public sealed class StreamDiagnostics : IDisposable
{
    /// <summary>The meter and activity-source name OpenTelemetry consumers subscribe to.</summary>
    public const string MeterName = "Moongazing.OrionStream";

    /// <summary>The tag key carrying the topic on published and dropped measurements and on spans.</summary>
    public const string TopicTagName = "orionstream.topic";

    private readonly Meter meter;
    private readonly ActivitySource activitySource;
    private int subscribers;

    /// <summary>Create the meter, its instruments, and the activity source.</summary>
    public StreamDiagnostics()
    {
        meter = new Meter(MeterName, "0.3.0");
        activitySource = new ActivitySource(MeterName, "0.3.0");

        Published = meter.CreateCounter<long>(
            "orionstream.published",
            unit: "{event}",
            description: "Events published to the hub (counted once per publish, not per subscriber).");

        Dropped = meter.CreateCounter<long>(
            "orionstream.dropped",
            unit: "{event}",
            description: "Events dropped because a subscriber buffer was full at publish time.");

        meter.CreateObservableGauge(
            "orionstream.subscribers",
            () => Volatile.Read(ref subscribers),
            unit: "{subscriber}",
            description: "Currently connected subscribers across all topics.");
    }

    /// <summary>Counts published events.</summary>
    public Counter<long> Published { get; }

    /// <summary>Counts dropped events.</summary>
    public Counter<long> Dropped { get; }

    /// <summary>The activity source carrying publish and subscribe spans.</summary>
    public ActivitySource ActivitySource => activitySource;

    /// <summary>
    /// Record one published event tagged with its topic. Counted once per publish, regardless of how
    /// many subscribers the event reached.
    /// </summary>
    /// <param name="topic">The topic the event was published to.</param>
    public void RecordPublished(string topic) =>
        Published.Add(1, new KeyValuePair<string, object?>(TopicTagName, topic));

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
            Dropped.Add(count, new KeyValuePair<string, object?>(TopicTagName, topic));
        }
    }

    /// <summary>
    /// Start a span around a publish to a topic, or null if no listener is sampling the source. The
    /// caller disposes the returned activity when the publish completes.
    /// </summary>
    /// <param name="topic">The topic being published to.</param>
    public Activity? StartPublish(string topic)
    {
        var activity = activitySource.StartActivity("OrionStream.Publish", ActivityKind.Producer);
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
        var activity = activitySource.StartActivity("OrionStream.Subscribe", ActivityKind.Consumer);
        activity?.SetTag(TopicTagName, topic);
        return activity;
    }

    /// <summary>Record a new subscriber.</summary>
    public void IncrementSubscribers() => Interlocked.Increment(ref subscribers);

    /// <summary>Record a departed subscriber.</summary>
    public void DecrementSubscribers() => Interlocked.Decrement(ref subscribers);

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
        activitySource.Dispose();
    }
}
