namespace Moongazing.OrionStream.Streaming;

using System.Collections.Concurrent;
using System.Threading.Channels;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;

/// <summary>
/// Default <see cref="ISseHub"/>. Each subscriber gets a bounded channel in
/// <see cref="BoundedChannelFullMode.DropOldest"/> mode, so a publish is never blocked by a slow
/// reader; a full buffer drops its oldest event. Topics are tracked lazily and reclaimed once their
/// last subscriber leaves and no replay backlog needs to survive the gap.
/// </summary>
/// <remarks>
/// The hub stamps every published event with a topic-monotonic id (the SSE <c>id:</c> field) when
/// the producer did not supply one, and keeps the newest
/// <see cref="StreamOptions.ReplayBufferCapacity"/> events per topic so a client reconnecting with a
/// <c>Last-Event-ID</c> can resume without gaps. The sequence is associated with each per-delivery
/// copy and replay-buffer entry, never with the producer's shared instance, so the same instance
/// published twice yields independent, correct wire ids. Resume matches the returning
/// <c>Last-Event-ID</c> against the exact value each buffered entry emitted on the wire (the producer
/// id if the event set one, otherwise the hub sequence), so producer-supplied ids round-trip through
/// resume just like hub sequences. See <see cref="Subscribe(string, string?)"/> for the resume
/// policy.
/// </remarks>
public sealed class SseHub : ISseHub
{
    private readonly ConcurrentDictionary<string, Topic> topics = new(StringComparer.Ordinal);

    // Serializes topic creation, subscriber registration, and topic removal so a removal can never
    // race a concurrent subscribe into orphaning a subscriber on a detached Topic instance.
    private readonly object lifecycle = new();
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

        // Register the subscriber and replay any backlog under the lifecycle lock, so a publish
        // racing with this subscribe either lands fully in the replay we copy out or fully in the
        // live channel we just registered, and so a concurrent topic removal cannot drop the topic
        // we are attaching to. That ordering is what prevents a missed, duplicated, or orphaned
        // event.
        lock (lifecycle)
        {
            var topicState = topics.GetOrAdd(topic, t => new Topic(t, options.ReplayBufferCapacity));
            topicState.AddSubscriber(id, channel, lastEventId);
        }

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
            // GetOrAdd is taken under the lifecycle lock so a concurrent unsubscribe-driven removal
            // cannot delete the entry between create and use.
            lock (lifecycle)
            {
                topicState = topics.GetOrAdd(topic, t => new Topic(t, options.ReplayBufferCapacity));
            }
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

    /// <summary>
    /// The number of topics the hub currently tracks. Exposed for tests and diagnostics to assert
    /// that idle topics with no replay backlog are reclaimed rather than leaked.
    /// </summary>
    internal int TopicCount => topics.Count;

    private void Unsubscribe(string topic, Guid id, Channel<ServerSentEvent> channel)
    {
        lock (lifecycle)
        {
            if (!topics.TryGetValue(topic, out var topicState) || !topicState.RemoveSubscriber(id))
            {
                return;
            }

            channel.Writer.TryComplete();
            diagnostics.DecrementSubscribers();

            // Reclaim the topic once its last subscriber leaves UNLESS it still holds replay backlog
            // that a reconnecting client might resume from. An empty replay buffer (replay disabled,
            // or enabled but nothing retained) carries no resume value, so the topic is dropped to
            // keep the map from leaking one entry per short-lived, distinct topic name. Removal runs
            // under the same lock as subscribe registration, so it cannot orphan a concurrent
            // subscribe: that subscribe either ran first (topic non-empty, not removed) or runs
            // after and re-adds the topic via GetOrAdd.
            if (topicState.IsEmpty && !topicState.HasReplayBacklog)
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

        /// <summary>True while the replay buffer holds at least one event a reconnecting client could resume from.</summary>
        public bool HasReplayBacklog
        {
            get
            {
                lock (gate)
                {
                    return replay.Count > 0;
                }
            }
        }

        public void AddSubscriber(Guid id, Channel<ServerSentEvent> channel, string? lastEventId)
        {
            lock (gate)
            {
                subscribers[id] = channel;

                if (replayCapacity == 0 || replay.Count == 0 || string.IsNullOrEmpty(lastEventId))
                {
                    return;
                }

                // Locate the resume point by matching the client's Last-Event-ID against the EXACT
                // value each buffered entry emitted on the wire (its WireId: the producer-supplied id
                // if the event set one, otherwise the hub sequence). This makes resume correct whether
                // the producer relied on the hub sequence or set its own ids: the id the browser sends
                // back is always the id it last saw on the wire, and that is what we match here.
                //
                // The monotonic Sequence remains the ordering key: once the matching entry is found we
                // replay every entry published AFTER it, in buffer order. An id that matches no
                // retained entry (unknown / evicted / never-existed) yields no resume point, so the
                // subscription starts from now with no replay. TryWrite on a DropOldest channel always
                // succeeds.
                long? resumeAfter = null;
                foreach (var entry in replay)
                {
                    if (string.Equals(entry.WireId, lastEventId, StringComparison.Ordinal))
                    {
                        resumeAfter = entry.Sequence;
                        break;
                    }
                }

                if (resumeAfter is not { } afterSequence)
                {
                    return; // unknown / evicted: from-now fallback.
                }

                foreach (var entry in replay)
                {
                    if (entry.Sequence > afterSequence)
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

                // Stamp the monotonic sequence onto a per-delivery COPY, never the producer's shared
                // instance, so re-publishing the same instance cannot retroactively rewrite the wire
                // id of an older buffered or in-flight delivery. The producer's own Id, if any, still
                // wins on the wire via ServerSentEvent.EffectiveId. The same stamped copy is the one
                // broadcast to every subscriber and retained for replay.
                var delivery = evt.WithSequence(assigned);

                if (replayCapacity > 0)
                {
                    if (replay.Count == replayCapacity)
                    {
                        replay.Dequeue();
                    }
                    // Capture the value emitted on the wire for this delivery (producer id if set, else
                    // the hub sequence) so resume can match a returning Last-Event-ID against it.
                    replay.Enqueue(new ReplayEntry(assigned, delivery.EffectiveId, delivery));
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
                    if (channel.Writer.TryWrite(delivery))
                    {
                        delivered++;
                    }
                }

                return delivered;
            }
        }
    }

    /// <summary>
    /// A buffered delivery retained for replay. <see cref="Sequence"/> is the monotonic ordering key;
    /// <see cref="WireId"/> is the exact value emitted as this delivery's SSE <c>id:</c> (producer id
    /// if the event set one, else the hub sequence) and is what a returning Last-Event-ID is matched
    /// against to locate the resume point.
    /// </summary>
    private readonly record struct ReplayEntry(long Sequence, string? WireId, ServerSentEvent Event);
}
