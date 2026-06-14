namespace Moongazing.OrionStream.Streaming;

using System.Threading.Channels;

/// <summary>
/// A subscription to a topic. Read events from <see cref="Reader"/> (typically with
/// <c>await foreach</c>); dispose it to unsubscribe and release the buffer. Disposal is what the
/// SSE writer does when the client disconnects.
/// </summary>
public sealed class StreamSubscription : IDisposable
{
    private readonly Action onDispose;
    private int disposed;

    internal StreamSubscription(string topic, ChannelReader<ServerSentEvent> reader, Action onDispose)
    {
        Topic = topic;
        Reader = reader;
        this.onDispose = onDispose;
    }

    /// <summary>The topic this subscription is attached to.</summary>
    public string Topic { get; }

    /// <summary>The reader delivering events published to the topic.</summary>
    public ChannelReader<ServerSentEvent> Reader { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            onDispose();
        }
    }
}
