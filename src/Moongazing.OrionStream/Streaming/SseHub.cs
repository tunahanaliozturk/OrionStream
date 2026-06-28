namespace Moongazing.OrionStream.Streaming;

using System.Collections.Concurrent;
using System.Threading.Channels;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;

/// <summary>
/// Default <see cref="ISseHub"/>. Each subscriber gets a bounded channel whose full-buffer behavior
/// follows <see cref="StreamOptions.FullBufferPolicy"/> (drop-oldest by default, so a publish is never
/// blocked by a slow reader). Topics are tracked lazily and reclaimed once their last subscriber
/// leaves and no replay backlog needs to survive the gap.
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
/// <para>
/// New in 0.4.0: a subscriber may carry a filter evaluated before the buffer admits an event; a
/// topic may override the global subscriber and replay capacities; the full-buffer policy may drop
/// the newest event or wait (bounded) instead of dropping the oldest; and a slow-consumer policy may
/// disconnect a subscriber whose buffer stays saturated past a threshold.
/// </para>
/// </remarks>
public sealed class SseHub : ISseHub, IStreamSerializerOptionsProvider
{
    private readonly ConcurrentDictionary<string, Topic> topics = new(StringComparer.Ordinal);

    // Serializes topic creation, subscriber registration, and topic removal so a removal can never
    // race a concurrent subscribe into orphaning a subscriber on a detached Topic instance.
    private readonly object lifecycle = new();
    private readonly StreamOptions options;
    private readonly StreamDiagnostics diagnostics;
    private readonly IReplayStoreFactory replayStoreFactory;

    /// <summary>Create a hub with the default in-memory replay store.</summary>
    /// <param name="options">Buffer sizing. Validated on construction.</param>
    /// <param name="diagnostics">The shared metrics instance.</param>
    public SseHub(StreamOptions options, StreamDiagnostics diagnostics)
        : this(options, diagnostics, new InMemoryReplayStoreFactory())
    {
    }

    /// <summary>Create a hub backed by a specific replay store factory.</summary>
    /// <param name="options">Buffer sizing. Validated on construction.</param>
    /// <param name="diagnostics">The shared metrics instance.</param>
    /// <param name="replayStoreFactory">
    /// The factory that creates the per-topic <see cref="IReplayStore"/> the hub retains and resumes a
    /// backlog through. Pass <see cref="InMemoryReplayStoreFactory"/> (the default) for the in-process
    /// ring, or a custom factory to swap in an external store without the hub knowing where the backlog
    /// lives.
    /// </param>
    public SseHub(StreamOptions options, StreamDiagnostics diagnostics, IReplayStoreFactory replayStoreFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(diagnostics);
        ArgumentNullException.ThrowIfNull(replayStoreFactory);
        options.Validate();
        this.options = options;
        this.diagnostics = diagnostics;
        this.replayStoreFactory = replayStoreFactory;
    }

    /// <inheritdoc />
    System.Text.Json.JsonSerializerOptions IStreamSerializerOptionsProvider.SerializerOptions => options.SerializerOptions;

    /// <inheritdoc />
    public StreamSubscription Subscribe(string topic) => Subscribe(topic, lastEventId: null, filter: null);

    /// <inheritdoc />
    public StreamSubscription Subscribe(string topic, string? lastEventId) =>
        Subscribe(topic, lastEventId, filter: null);

    /// <inheritdoc />
    public StreamSubscription Subscribe(string topic, string? lastEventId, Func<ServerSentEvent, bool>? filter)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);

        var capacity = options.SubscriberCapacityFor(topic);

        // A wait policy needs the native channel to refuse a write when full (TryWrite returns false)
        // so we can wait for room deterministically. Drop policies map straight onto the channel's
        // own full-mode and keep the never-blocks guarantee.
        var fullMode = options.FullBufferPolicy switch
        {
            FullBufferPolicy.DropNewest => BoundedChannelFullMode.DropNewest,
            FullBufferPolicy.Wait => BoundedChannelFullMode.Wait,
            _ => BoundedChannelFullMode.DropOldest,
        };

        var channel = Channel.CreateBounded<ServerSentEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = fullMode,
            SingleReader = true,
            SingleWriter = false,
        });

        var id = Guid.NewGuid();
        var subscriber = new Subscriber(id, channel, filter);

        // Register the subscriber and replay any backlog under the lifecycle lock, so a publish
        // racing with this subscribe either lands fully in the replay we copy out or fully in the
        // live channel we just registered, and so a concurrent topic removal cannot drop the topic
        // we are attaching to. That ordering is what prevents a missed, duplicated, or orphaned
        // event.
        using (diagnostics.StartSubscribe(topic))
        {
            lock (lifecycle)
            {
                var topicState = topics.GetOrAdd(topic, CreateTopic);
                topicState.AddSubscriber(subscriber, lastEventId);
            }

            diagnostics.IncrementSubscribers();
        }

        return new StreamSubscription(topic, channel.Reader, () => Unsubscribe(topic, id, channel));
    }

    /// <inheritdoc />
    public int Publish(string topic, ServerSentEvent evt)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(evt);

        using var activity = diagnostics.StartPublish(topic);

        diagnostics.RecordPublished(topic);

        Topic? topicState;
        if (options.ReplayBufferCapacityFor(topic) > 0)
        {
            // Retain (or create) the topic so its bounded replay buffer accumulates even with no
            // live subscriber. That is what lets a client that fully disconnected resume after it
            // reconnects with a Last-Event-ID. The buffer is bounded, so per-topic memory is capped.
            // GetOrAdd is taken under the lifecycle lock so a concurrent unsubscribe-driven removal
            // cannot delete the entry between create and use.
            lock (lifecycle)
            {
                topicState = topics.GetOrAdd(topic, CreateTopic);
            }
        }
        else if (!topics.TryGetValue(topic, out topicState))
        {
            return 0;
        }

        var delivered = topicState.Publish(evt, diagnostics, out var disconnected);

        // Reconcile any subscribers the slow-consumer policy shed during this publish. Their channels
        // were already completed inside Publish; here we account for the departure and reclaim the
        // topic if it is now empty, under the same lifecycle lock that guards subscribe and removal.
        if (disconnected is { Count: > 0 })
        {
            foreach (var _ in disconnected)
            {
                diagnostics.DecrementSubscribers();
            }

            lock (lifecycle)
            {
                if (topicState.IsEmpty && !topicState.HasReplayBacklog)
                {
                    topics.TryRemove(new KeyValuePair<string, Topic>(topic, topicState));
                }
            }
        }

        activity?.SetTag("orionstream.delivered", delivered);
        return delivered;
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

    private Topic CreateTopic(string topic)
    {
        // Create the per-topic replay store only when replay is enabled for this topic, through the
        // pluggable factory so the in-memory ring is one implementation behind the seam. A topic with
        // replay disabled gets no store at all (null), which is what keeps the wire path allocation-light
        // and behavior identical to before for replay-off topics.
        var replayCapacity = options.ReplayBufferCapacityFor(topic);
        var replayStore = replayCapacity > 0 ? replayStoreFactory.Create(topic, replayCapacity) : null;

        return new Topic(
            topic,
            replayStore,
            options.SubscriberCapacityFor(topic),
            options.FullBufferPolicy,
            options.MaxPublishWait,
            options.SlowConsumerPolicy);
    }

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
    /// One live subscriber: its delivery channel, an optional pre-buffer filter, and a saturation run
    /// counter the slow-consumer policy uses. The run counter is only touched under the owning
    /// <see cref="Topic"/>'s gate, so it needs no synchronization of its own.
    /// </summary>
    private sealed class Subscriber(Guid id, Channel<ServerSentEvent> channel, Func<ServerSentEvent, bool>? filter)
    {
        public Guid Id { get; } = id;

        public Channel<ServerSentEvent> Channel { get; } = channel;

        public Func<ServerSentEvent, bool>? Filter { get; } = filter;

        /// <summary>Consecutive publishes that found this subscriber's buffer full. Reset on any publish that found room.</summary>
        public int ConsecutiveFull { get; set; }
    }

    /// <summary>
    /// Per-topic state: its live subscribers, the monotonic id sequence, and the pluggable
    /// <see cref="IReplayStore"/> that retains the most recently published events (null when replay is
    /// disabled for this topic). A single lock guards id assignment, every store call, and the
    /// subscriber set together so publish and subscribe interleave consistently and the store sees a
    /// gap-free, serialized sequence.
    /// </summary>
    private sealed class Topic
    {
        private readonly object gate = new();
        private readonly ConcurrentDictionary<Guid, Subscriber> subscribers = new();
        private readonly IReplayStore? replayStore;
        private readonly int subscriberCapacity;
        private readonly FullBufferPolicy fullBufferPolicy;
        private readonly TimeSpan? maxPublishWait;
        private readonly SlowConsumerPolicy? slowConsumerPolicy;

        private long sequence;

        public Topic(
            string name,
            IReplayStore? replayStore,
            int subscriberCapacity,
            FullBufferPolicy fullBufferPolicy,
            TimeSpan? maxPublishWait,
            SlowConsumerPolicy? slowConsumerPolicy)
        {
            Name = name;
            this.replayStore = replayStore;
            this.subscriberCapacity = subscriberCapacity;
            this.fullBufferPolicy = fullBufferPolicy;
            this.maxPublishWait = maxPublishWait;
            this.slowConsumerPolicy = slowConsumerPolicy;
        }

        public string Name { get; }

        public int SubscriberCount => subscribers.Count;

        public bool IsEmpty => subscribers.IsEmpty;

        /// <summary>True while the replay store holds at least one event a reconnecting client could resume from.</summary>
        public bool HasReplayBacklog
        {
            get
            {
                lock (gate)
                {
                    return replayStore is { HasBacklog: true };
                }
            }
        }

        public void AddSubscriber(Subscriber subscriber, string? lastEventId)
        {
            lock (gate)
            {
                subscribers[subscriber.Id] = subscriber;

                if (replayStore is null || string.IsNullOrEmpty(lastEventId))
                {
                    return;
                }

                // Ask the store to locate the resume point and hand back the entries to replay. The
                // store owns the match (against each entry's wire id) and the ordering (ascending
                // sequence after the match); resume is correct whether the producer relied on the hub
                // sequence or set its own ids, and an id that matches no retained entry yields an empty
                // result, which is the from-now fallback. Reading through the seam here is what lets a
                // custom store serve the backlog without the hub knowing where it lives.
                var replay = replayStore.GetReplay(lastEventId);
                if (replay.Count == 0)
                {
                    return; // unknown / evicted: from-now fallback.
                }

                foreach (var entry in replay)
                {
                    // A subscriber filter applies to replayed events too, so only matching backlog is
                    // replayed. Replay through the SAME policy-honoring enqueue as live publish, so a
                    // DropNewest subscriber drops the NEWEST replayed events under a full buffer
                    // (keeping the oldest that fit) exactly as it would for live events, rather than
                    // letting the channel's native full-mode rewrite the buffered backlog. The Wait
                    // policy degrades to a single non-blocking attempt here on purpose: back-pressure is
                    // a live-publish concern and we must not block under the gate while attaching a
                    // subscriber. A full buffer simply refuses the replayed entry, which is acceptable
                    // for backlog catch-up.
                    if (Accepts(subscriber, entry.Event))
                    {
                        TryEnqueue(subscriber, entry.Event);
                    }
                }
            }
        }

        public bool RemoveSubscriber(Guid id) => subscribers.TryRemove(id, out _);

        public int Publish(ServerSentEvent evt, StreamDiagnostics diagnostics, out List<Subscriber>? disconnected)
        {
            disconnected = null;

            lock (gate)
            {
                var assigned = ++sequence;

                // Stamp the monotonic sequence onto a per-delivery COPY, never the producer's shared
                // instance, so re-publishing the same instance cannot retroactively rewrite the wire
                // id of an older buffered or in-flight delivery. The producer's own Id, if any, still
                // wins on the wire via ServerSentEvent.EffectiveId. The same stamped copy is the one
                // broadcast to every subscriber and retained for replay.
                var delivery = evt.WithSequence(assigned);

                // Retain this delivery through the replay store (when replay is enabled for the topic)
                // so a reconnecting client can resume from it. Capture the value emitted on the wire
                // (producer id if set, else the hub sequence) so resume can match a returning
                // Last-Event-ID against it. The store call runs under the same gate that assigned the
                // sequence, so the store sees a gap-free, strictly increasing sequence and never has a
                // publish race a resume read.
                replayStore?.Append(new ReplayEntry(assigned, delivery.EffectiveId, delivery));

                var delivered = 0;
                var dropped = 0L;
                foreach (var subscriber in subscribers.Values)
                {
                    // Evaluate the per-subscriber filter BEFORE the buffer so a rejected event never
                    // consumes buffer space or counts as a drop: from this subscriber's point of view
                    // the event does not exist.
                    if (!Accepts(subscriber, delivery))
                    {
                        continue;
                    }

                    var outcome = Deliver(subscriber, delivery);
                    if (outcome.Delivered)
                    {
                        delivered++;
                    }
                    if (outcome.Dropped)
                    {
                        dropped++;
                    }

                    // Slow-consumer policy: track the consecutive-full run and shed a subscriber that
                    // stays saturated past the threshold. A publish that found room resets the run.
                    if (slowConsumerPolicy is { } policy)
                    {
                        subscriber.ConsecutiveFull = outcome.WasFull ? subscriber.ConsecutiveFull + 1 : 0;
                        if (subscriber.ConsecutiveFull >= policy.MaxConsecutiveFullPublishes
                            && subscribers.TryRemove(subscriber.Id, out _))
                        {
                            subscriber.Channel.Writer.TryComplete();
                            (disconnected ??= []).Add(subscriber);
                        }
                    }
                }

                // Record drops once per publish, tagged with this topic, rather than one counter add
                // per evicting subscriber: the count is the same and the per-publish call is cheaper.
                diagnostics.RecordDropped(Name, dropped);

                return delivered;
            }
        }

        private static bool Accepts(Subscriber subscriber, ServerSentEvent evt) =>
            subscriber.Filter is null || subscriber.Filter(evt);

        private DeliveryOutcome Deliver(Subscriber subscriber, ServerSentEvent delivery)
        {
            // The Wait policy is the only one that can apply back-pressure, and only on live publish.
            // Every other case (and the first non-blocking attempt of Wait) is the shared enqueue used
            // by both live publish and replay, so a subscriber's full-buffer policy is honored
            // identically on both paths.
            if (fullBufferPolicy == FullBufferPolicy.Wait)
            {
                var writer = subscriber.Channel.Writer;
                var wasFull = subscriber.Channel.Reader.Count >= subscriberCapacity;
                return DeliverWaiting(writer, delivery, wasFull);
            }

            return TryEnqueue(subscriber, delivery);
        }

        /// <summary>
        /// Enqueue one event honoring the configured drop policy, without ever blocking. Shared by
        /// live publish and backlog replay so both apply the SAME full-buffer behavior. For the Wait
        /// policy this performs a single non-blocking attempt (the bounded wait lives only in the live
        /// publish path); for the drop policies it reproduces their semantics exactly:
        /// DropNewest refuses the incoming event when full (keeping the oldest that fit), DropOldest
        /// admits it and evicts the oldest buffered event.
        /// </summary>
        private DeliveryOutcome TryEnqueue(Subscriber subscriber, ServerSentEvent delivery)
        {
            var writer = subscriber.Channel.Writer;
            var wasFull = subscriber.Channel.Reader.Count >= subscriberCapacity;

            switch (fullBufferPolicy)
            {
                case FullBufferPolicy.DropNewest:
                    // DropNewest: a full buffer discards the incoming (newest) event rather than
                    // letting the channel's native full-mode evict a buffered entry. When the buffer
                    // had room the event is genuinely buffered and delivered.
                    if (wasFull)
                    {
                        return new DeliveryOutcome(Delivered: false, Dropped: true, WasFull: true);
                    }
                    writer.TryWrite(delivery);
                    return new DeliveryOutcome(Delivered: true, Dropped: false, WasFull: false);

                default: // DropOldest (and the non-blocking attempt for Wait): TryWrite always
                         // succeeds; a full buffer evicts its oldest event.
                    var delivered = writer.TryWrite(delivery);
                    return new DeliveryOutcome(Delivered: delivered, Dropped: wasFull, WasFull: wasFull);
            }
        }

        private DeliveryOutcome DeliverWaiting(
            ChannelWriter<ServerSentEvent> writer, ServerSentEvent delivery, bool wasFull)
        {
            // The channel is in Wait mode, so TryWrite returns false when the buffer is full instead of
            // blocking. Try once; if it succeeds the buffer had room. Otherwise wait for the reader to
            // drain, bounded by maxPublishWait, then try once more. This is the only path that can
            // apply back-pressure to the publisher, and the cap is what keeps a wedged reader from
            // stalling it forever.
            if (writer.TryWrite(delivery))
            {
                return new DeliveryOutcome(Delivered: true, Dropped: false, WasFull: wasFull);
            }

            // maxPublishWait is guaranteed non-null for the Wait policy by StreamOptions.Validate.
            var cap = maxPublishWait ?? TimeSpan.Zero;
            using var cts = new CancellationTokenSource(cap);
            try
            {
                // WaitToWriteAsync completes as soon as the single reader frees a slot, so this returns
                // the instant room appears rather than sleeping the whole cap; the cap only bounds the
                // worst case. Block synchronously because Publish is a synchronous API and the Wait
                // policy's contract is exactly that it can stall the calling publisher.
                while (writer.WaitToWriteAsync(cts.Token).AsTask().GetAwaiter().GetResult())
                {
                    if (writer.TryWrite(delivery))
                    {
                        return new DeliveryOutcome(Delivered: true, Dropped: false, WasFull: true);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Cap elapsed with the buffer still full: give up on this subscriber for this publish.
            }

            return new DeliveryOutcome(Delivered: false, Dropped: true, WasFull: true);
        }

        private readonly record struct DeliveryOutcome(bool Delivered, bool Dropped, bool WasFull);
    }
}
