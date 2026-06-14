namespace Moongazing.OrionStream.Diagnostics;

using System.Diagnostics.Metrics;

/// <summary>
/// OpenTelemetry instrumentation for the broadcast hub. Exposes a <see cref="Meter"/> named
/// <c>Moongazing.OrionStream</c> with published and dropped counters and a current-subscribers
/// gauge. Registered as a singleton; dispose it to release the meter.
/// </summary>
public sealed class StreamDiagnostics : IDisposable
{
    /// <summary>The meter name OpenTelemetry consumers subscribe to.</summary>
    public const string MeterName = "Moongazing.OrionStream";

    private readonly Meter meter;
    private int subscribers;

    /// <summary>Create the meter and its instruments.</summary>
    public StreamDiagnostics()
    {
        meter = new Meter(MeterName, "0.1.0");

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

    /// <summary>Record a new subscriber.</summary>
    public void IncrementSubscribers() => Interlocked.Increment(ref subscribers);

    /// <summary>Record a departed subscriber.</summary>
    public void DecrementSubscribers() => Interlocked.Decrement(ref subscribers);

    /// <inheritdoc />
    public void Dispose() => meter.Dispose();
}
