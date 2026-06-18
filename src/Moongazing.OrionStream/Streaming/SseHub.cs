namespace Moongazing.OrionStream.Streaming;

using System.Collections.Concurrent;
using System.Globalization;
using System.Threading.Channels;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;

/// <summary>
/// Default <see cref="ISseHub"/>. Each subscriber gets a bounded channel in
/// <see cref="BoundedChannelFullMode.DropOldest"/> mode, so a publish is never blocked by a slow
/// reader; a full buffer drops its oldest event. Topics are tracked lazily and removed once their
/// last subscriber leaves.
/// </summary>
/// <remarks>
/// The hub stamps every published event with a topic-monotonic id (the SSE <c>id:</c> field) when
/// the producer did not supply one, and keeps the newest
/// <see cref="StreamOptions.ReplayBufferCapacity"/> events per topic so a client reconnecting with a
/// <c>Last-Event-ID</c> can resume without gaps. See <see cref="Subscribe(string, string?)"/> for
/// the resume policy.
/// </remarks>
public sealed class SseHub : ISseHub
{
    private readonly ConcurrentDictionary<string, Topic> topics = new(StringComparer.Ordinal);
    private readonly StreamOptions options;
    private readonly StreamDiagnostics diagnostics;

    /// <summary>Create a hub.</summary>
    /// <param name="options">Buffer sizing. Validated on construction.</param>
    /// <param name="diagnostics">The shared metrics instance.</param>
    public SseHub(StreamOptions options, StreamDiagnostics diagnostics)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(diagnostics);
        options.Validate();
        this.options = options;
        this.diagnostics = diagnostics;
    }

    /// <inheritdoc />
    public StreamSubscription Subscribe(string topic) => Subscribe(topic, lastEventId: null);

    /// <inheritdoc />
    public StreamSubscription Subscribe(string topic, string? lastEventId)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);

        var channel = Channel.CreateBounded<ServerSentEvent>(new BoundedChannelOptions(options.SubscriberCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        var id = Guid.NewGuid();
        var topicState = topics.GetOrAdd(topic, t => new Topic(t, options.ReplayBufferCapacity));

        // Register the subscriber and replay any backlog under the topic lock, so a publish racing
        // with this subscribe either lands fully in the replay we copy out or fully in the live
        // channel we just registered. That ordering is what prevents a missed or duplicated event.
        topicState.AddSubscriber(id, channel, lastEventId);
        diagnostics.IncrementSubscribers();

        return new StreamSubscription(topic, channel.Reader, () => Unsubscribe(topic, id, channel));
    }

    /// <inheritdoc />
    public int Publish(string topic, ServerSentEvent evt)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(evt);

        diagnostics.Published.Add(1);

        Topic? topicState;
        if (options.ReplayBufferCapacity > 0)
        {
            // Retain (or create) the topic so its bounded replay buffer accumulates even with no
            // live subscriber. That is what lets a client that fully disconnected resume after it
            // reconnects with a Last-Event-ID. The buffer is bounded, so per-topic memory is capped.
            topicState = topics.GetOrAdd(topic, t => new Topic(t, options.ReplayBufferCapacity));
        }
        else if (!topics.TryGetValue(topic, out topicState))
        {
            return 0;
        }

        return topicState.Publish(evt, options.SubscriberCapacity, diagnostics);
    }

    /// <inheritdoc />
    public int SubscriberCount(string topic)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return topics.TryGetValue(topic, out var topicState) ? topicState.SubscriberCount : 0;
    }

    private void Unsubscribe(string topic, Guid id, Channel<ServerSentEvent> channel)
    {
        if (topics.TryGetValue(topic, out var topicState) && topicState.RemoveSubscriber(id))
        {
            channel.Writer.TryComplete();
            diagnostics.DecrementSubscribers();

            // With replay disabled, drop the topic once empty so the map does not grow unbounded
            // with idle topics. With replay enabled the topic is kept so its bounded buffer survives
            // a zero-subscriber gap and a reconnecting client can still resume.
            if (options.ReplayBufferCapacity == 0 && topicState.IsEmpty)
            {
                topics.TryRemove(new KeyValuePair<string, Topic>(topic, topicState));
            }
        }
    }

    /// <summary>
    /// Per-topic state: its live subscribers, the monotonic id sequence, and a bounded replay ring
    /// of the most recently published events. A single lock guards id assignment, the replay ring,
    /// and the subscriber set together so publish and subscribe interleave consistently.
    /// </summary>
    private sealed class Topic
    {
        private readonly object gate = new();
        private readonly ConcurrentDictionary<Guid, Channel<ServerSentEvent>> subscribers = new();
        private readonly int replayCapacity;

        // Ring buffer of the newest events, oldest at the front. Holds at most replayCapacity items.
        private readonly Queue<ReplayEntry> replay;
        private long sequence;

        public Topic(string name, int replayCapacity)
        {
            Name = name;
            this.replayCapacity = replayCapacity;
            replay = new Queue<ReplayEntry>(replayCapacity > 0 ? replayCapacity : 0);
        }

        public string Name { get; }

        public int SubscriberCount => subscribers.Count;

        public bool IsEmpty => subscribers.IsEmpty;

        public void AddSubscriber(Guid id, Channel<ServerSentEvent> channel, string? lastEventId)
        {
            lock (gate)
            {
                subscribers[id] = channel;

                if (replayCapacity == 0 || !TryParseId(lastEventId, out var resumeFrom))
                {
                    return;
                }

                // Replay only events strictly after the client's last seen id that we still hold.
                // An evicted or unknown id yields nothing here, which is the documented from-now
                // fallback. TryWrite on a DropOldest channel always succeeds.
                foreach (var entry in replay)
                {
                    if (entry.Sequence > resumeFrom)
                    {
                        channel.Writer.TryWrite(entry.Event);
                    }
                }
            }
        }

        public bool RemoveSubscriber(Guid id) => subscribers.TryRemove(id, out _);

        public int Publish(ServerSentEvent evt, int subscriberCapacity, StreamDiagnostics diagnostics)
        {
            lock (gate)
            {
                var assigned = ++sequence;

                // Stamp the monotonic sequence id in place so the very same instance is broadcast to
                // every subscriber (and held for replay). The producer's own Id, if any, still wins
                // on the wire via ServerSentEvent.EffectiveId; this never overwrites it.
                evt.SequenceId = assigned;

                if (replayCapacity > 0)
                {
                    if (replay.Count == replayCapacity)
                    {
                        replay.Dequeue();
                    }
                    replay.Enqueue(new ReplayEntry(assigned, evt));
                }

                var delivered = 0;
                foreach (var channel in subscribers.Values)
                {
                    // DropOldest means TryWrite always succeeds; count a drop when the buffer was
                    // already full so the oldest event is being evicted.
                    if (channel.Reader.Count >= subscriberCapacity)
                    {
                        diagnostics.Dropped.Add(1);
                    }
                    if (channel.Writer.TryWrite(evt))
                    {
                        delivered++;
                    }
                }

                return delivered;
            }
        }

        private static bool TryParseId(string? lastEventId, out long value)
        {
            value = 0;
            return !string.IsNullOrEmpty(lastEventId)
                && long.TryParse(lastEventId, NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
        }
    }

    private readonly record struct ReplayEntry(long Sequence, ServerSentEvent Event);
}
