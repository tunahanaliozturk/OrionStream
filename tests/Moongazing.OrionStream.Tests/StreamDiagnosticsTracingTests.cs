namespace Moongazing.OrionStream.Tests;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class StreamDiagnosticsTracingTests
{
    /// <summary>
    /// Captures the topic tag carried on each counter measurement for one specific diagnostics
    /// instance, filtering to the exact meter that instance owns (so concurrent instances do not
    /// bleed into each other).
    /// </summary>
    private sealed class TaggedMeterCapture : IDisposable
    {
        private readonly MeterListener listener;
        private readonly Meter ownerMeter;
        private readonly List<(string Instrument, long Value, string? Topic)> measurements = new();

        public TaggedMeterCapture(StreamDiagnostics owner)
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
            listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, _) =>
            {
                if (!ReferenceEquals(instrument.Meter, ownerMeter))
                {
                    return;
                }
                string? topic = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == StreamDiagnostics.TopicTagName)
                    {
                        topic = tag.Value as string;
                    }
                }
                lock (measurements)
                {
                    measurements.Add((instrument.Name, measurement, topic));
                }
            });
            listener.Start();
        }

        public List<(string Instrument, long Value, string? Topic)> ForInstrument(string name)
        {
            lock (measurements)
            {
                return measurements.Where(m => m.Instrument == name).ToList();
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

        public void Dispose() => listener.Dispose();
    }

    /// <summary>Captures activities started on the OrionStream source for the duration of the test.</summary>
    private sealed class ActivityCapture : IDisposable
    {
        private readonly ActivityListener listener;
        private readonly List<Activity> started = new();

        public ActivityCapture()
        {
            listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == StreamDiagnostics.MeterName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStarted = activity =>
                {
                    lock (started)
                    {
                        started.Add(activity);
                    }
                },
            };
            ActivitySource.AddActivityListener(listener);
        }

        public IReadOnlyList<Activity> Started
        {
            get { lock (started) { return started.ToList(); } }
        }

        public void Dispose() => listener.Dispose();
    }

    [Fact]
    public void Publish_tags_the_published_counter_with_the_topic()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new TaggedMeterCapture(diag);
        var hub = new SseHub(new StreamOptions(), diag);

        hub.Publish("orders", new ServerSentEvent { Data = "x" });

        var published = capture.ForInstrument("orionstream.published");
        Assert.Single(published);
        Assert.Equal("orders", published[0].Topic);
        Assert.Equal(1, published[0].Value);
    }

    [Fact]
    public void Dropped_counter_is_tagged_with_the_topic_it_dropped_on()
    {
        using var diag = new StreamDiagnostics();
        using var capture = new TaggedMeterCapture(diag);
        var hub = new SseHub(new StreamOptions { SubscriberCapacity = 1 }, diag);
        using var sub = hub.Subscribe("orders");

        hub.Publish("orders", new ServerSentEvent { Data = "1" }); // fills buffer
        hub.Publish("orders", new ServerSentEvent { Data = "2" }); // evicts -> one drop

        var dropped = capture.ForInstrument("orionstream.dropped");
        Assert.Equal(1, dropped.Sum(m => m.Value));
        Assert.All(dropped.Where(m => m.Value > 0), m => Assert.Equal("orders", m.Topic));
    }

    [Fact]
    public void Publish_emits_an_activity_tagged_with_the_topic()
    {
        // The activity source is process-wide; a unique topic isolates this assertion from any other
        // test publishing concurrently into the same listener.
        var topic = "publish-" + Guid.NewGuid().ToString("N");
        using var capture = new ActivityCapture();
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);

        hub.Publish(topic, new ServerSentEvent { Data = "x" });

        var publish = capture.Started.Single(a =>
            a.OperationName == "OrionStream.Publish" &&
            Equals(a.GetTagItem(StreamDiagnostics.TopicTagName), topic));
        Assert.Equal(ActivityKind.Producer, publish.Kind);
    }

    [Fact]
    public void Subscribe_emits_an_activity_tagged_with_the_topic()
    {
        var topic = "subscribe-" + Guid.NewGuid().ToString("N");
        using var capture = new ActivityCapture();
        using var diag = new StreamDiagnostics();
        var hub = new SseHub(new StreamOptions(), diag);

        using var sub = hub.Subscribe(topic);

        var subscribe = capture.Started.Single(a =>
            a.OperationName == "OrionStream.Subscribe" &&
            Equals(a.GetTagItem(StreamDiagnostics.TopicTagName), topic));
        Assert.Equal(ActivityKind.Consumer, subscribe.Kind);
    }
}
