namespace Moongazing.OrionStream.Tests;

using System.Collections.Generic;
using System.Diagnostics.Metrics;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class StreamDiagnosticsTests
{
    /// <summary>
    /// Records counter and observable-gauge measurements for one specific <see cref="StreamDiagnostics"/>
    /// instance. Every instance shares the meter name, so to keep tests isolated the listener filters
    /// to the exact <see cref="Meter"/> object owned by this diagnostics instance, obtained via
    /// reflection over its private field (test-only).
    /// </summary>
    private sealed class MeterCapture : System.IDisposable
    {
        private readonly MeterListener listener;
        private readonly Meter ownerMeter;
        private readonly Dictionary<string, long> counters = new(System.StringComparer.Ordinal);

        public MeterCapture(StreamDiagnostics owner)
        {
            ownerMeter = OwnerMeterOf(owner);
            listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (ReferenceEquals(instrument.Meter, ownerMeter))
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                },
            };

            // Counters are Counter<long>; the subscribers gauge is observable<int>. Register a
            // callback for each numeric type so both kinds of instrument are captured.
            listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
                Record(instrument, measurement));
            listener.SetMeasurementEventCallback<int>((instrument, measurement, _, _) =>
                Record(instrument, measurement));

            listener.Start();
        }

        private void Record(Instrument instrument, long measurement)
        {
            if (!ReferenceEquals(instrument.Meter, ownerMeter))
            {
                return;
            }

            lock (counters)
            {
                counters[instrument.Name] = counters.GetValueOrDefault(instrument.Name) + measurement;
            }
        }

        private static Meter OwnerMeterOf(StreamDiagnostics owner)
        {
            var field = typeof(StreamDiagnostics).GetField(
                "meter",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (Meter)field!.GetValue(owner)!;
        }

        public long Counter(string name)
        {
            lock (counters)
            {
                return counters.GetValueOrDefault(name);
            }
        }

        public long ObserveGauge(string name)
        {
            // Reset the accumulator for the gauge so we read the latest observed value, not a sum.
            lock (counters)
            {
                counters.Remove(name);
            }

            listener.RecordObservableInstruments();

            lock (counters)
            {
                return counters.GetValueOrDefault(name);
            }
        }

        public void Dispose() => listener.Dispose();
    }

    [Fact]
    public void The_meter_name_constant_is_the_documented_value()
    {
        Assert.Equal("Moongazing.OrionStream", StreamDiagnostics.MeterName);
    }

    [Fact]
    public void Disposing_diagnostics_disposes_the_meter()
    {
        var diag = new StreamDiagnostics();
        diag.Dispose();

        // Double dispose must not throw (Meter.Dispose is idempotent).
        diag.Dispose();
    }

    [Fact]
    public void Publishing_increments_the_published_counter_once_per_publish()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new MeterCapture(diag);
        var hub = new SseHub(new StreamOptions(), diag);
        using var a = hub.Subscribe("orders");
        using var b = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "x" });
        hub.Publish("orders", new ServerSentEvent { Data = "y" });

        // Two subscribers but two publishes: published is counted per-publish, not per-delivery.
        Assert.Equal(2, capture.Counter("orionstream.published"));
    }

    [Fact]
    public void Publishing_to_an_empty_topic_still_counts_as_published()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new MeterCapture(diag);
        var hub = new SseHub(new StreamOptions(), diag);

        hub.Publish("nobody", new ServerSentEvent { Data = "x" });

        Assert.Equal(1, capture.Counter("orionstream.published"));
        Assert.Equal(0, capture.Counter("orionstream.dropped"));
    }

    [Fact]
    public void Overflowing_a_buffer_increments_the_dropped_counter()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new MeterCapture(diag);
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 2 }, diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "1" });
        hub.Publish("orders", new ServerSentEvent { Data = "2" }); // buffer now full
        hub.Publish("orders", new ServerSentEvent { Data = "3" }); // evicts oldest -> 1 drop

        Assert.Equal(1, capture.Counter("orionstream.dropped"));
        Assert.Equal(3, capture.Counter("orionstream.published"));
    }

    [Fact]
    public void No_drops_are_counted_while_the_buffer_has_room()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new MeterCapture(diag);
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 8 }, diag);
        using var sub = hub.Subscribe("orders");

        for (var i = 0; i < 8; i++)
        {
            hub.Publish("orders", new ServerSentEvent { Data = "x" });
        }

        Assert.Equal(0, capture.Counter("orionstream.dropped"));
    }

    [Fact]
    public void The_subscribers_gauge_tracks_live_subscribers()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new MeterCapture(diag);
        var hub = new SseHub(new StreamOptions(), diag);

        Assert.Equal(0, capture.ObserveGauge("orionstream.subscribers"));

        var a = hub.Subscribe("orders");
        var b = hub.Subscribe("invoices");
        Assert.Equal(2, capture.ObserveGauge("orionstream.subscribers"));

        a.Dispose();
        Assert.Equal(1, capture.ObserveGauge("orionstream.subscribers"));

        b.Dispose();
        Assert.Equal(0, capture.ObserveGauge("orionstream.subscribers"));
    }

    [Fact]
    public void Increment_and_decrement_subscribers_are_reflected_in_the_gauge()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new MeterCapture(diag);

        diag.IncrementSubscribers();
        diag.IncrementSubscribers();
        diag.DecrementSubscribers();

        Assert.Equal(1, capture.ObserveGauge("orionstream.subscribers"));
    }
}
