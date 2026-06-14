namespace Moongazing.OrionStream.Streaming;

using System.Collections.Concurrent;
using System.Threading.Channels;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;

/// <summary>
/// Default <see cref="ISseHub"/>. Each subscriber gets a bounded channel in
/// <see cref="BoundedChannelFullMode.DropOldest"/> mode, so a publish is never blocked by a slow
/// reader; a full buffer drops its oldest event. Topics are tracked lazily and removed once their
/// last subscriber leaves.
/// </summary>
public sealed class SseHub : ISseHub
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Channel<ServerSentEvent>>> topics =
        new(StringComparer.Ordinal);
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
    public StreamSubscription Subscribe(string topic)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);

        var channel = Channel.CreateBounded<ServerSentEvent>(new BoundedChannelOptions(options.SubscriberCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        var id = Guid.NewGuid();
        var subscribers = topics.GetOrAdd(topic, _ => new ConcurrentDictionary<Guid, Channel<ServerSentEvent>>());
        subscribers[id] = channel;
        diagnostics.IncrementSubscribers();

        return new StreamSubscription(topic, channel.Reader, () => Unsubscribe(topic, id, channel));
    }

    /// <inheritdoc />
    public int Publish(string topic, ServerSentEvent evt)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(evt);

        diagnostics.Published.Add(1);
        if (!topics.TryGetValue(topic, out var subscribers))
        {
            return 0;
        }

        var delivered = 0;
        foreach (var channel in subscribers.Values)
        {
            // DropOldest means TryWrite always succeeds; count a drop when the buffer was already
            // full so the oldest event is being evicted.
            if (channel.Reader.Count >= options.SubscriberCapacity)
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

    /// <inheritdoc />
    public int SubscriberCount(string topic)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        return topics.TryGetValue(topic, out var subscribers) ? subscribers.Count : 0;
    }

    private void Unsubscribe(string topic, Guid id, Channel<ServerSentEvent> channel)
    {
        if (topics.TryGetValue(topic, out var subscribers) && subscribers.TryRemove(id, out _))
        {
            channel.Writer.TryComplete();
            diagnostics.DecrementSubscribers();

            // Drop the topic once empty so the map does not grow unbounded with idle topics.
            if (subscribers.IsEmpty)
            {
                topics.TryRemove(new KeyValuePair<string, ConcurrentDictionary<Guid, Channel<ServerSentEvent>>>(topic, subscribers));
            }
        }
    }
}
