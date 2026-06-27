namespace Moongazing.OrionStream.Streaming;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;

/// <summary>
/// Async-enumerable sugar for consuming and producing events without hand-writing a
/// <c>TryRead</c>/<c>WaitToReadAsync</c> loop. Consumers can <c>await foreach</c> a subscription, and
/// producers can drain an <see cref="IAsyncEnumerable{T}"/> into a topic. These wrap the existing
/// <see cref="ISseHub"/> and <see cref="StreamSubscription"/> surface; they add no buffering of their
/// own and respect the same DropOldest delivery model.
/// </summary>
public static class StreamSubscriptionAsyncEnumerableExtensions
{
    private static readonly JsonSerializerOptions DefaultSerializerOptions = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Read the subscription's events as an async stream, yielding each event as it arrives and
    /// completing when the subscription completes or <paramref name="cancellationToken"/> trips. This
    /// is a direct view over <see cref="StreamSubscription.Reader"/>; disposing the subscription is
    /// still the caller's responsibility (typically a <c>using</c> around the loop).
    /// </summary>
    /// <param name="subscription">The subscription to read from.</param>
    /// <param name="cancellationToken">Ends the enumeration when tripped.</param>
    /// <returns>An async stream of the subscription's events.</returns>
    public static IAsyncEnumerable<ServerSentEvent> ReadAllAsync(
        this StreamSubscription subscription,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        return subscription.Reader.ReadAllAsync(cancellationToken);
    }

    /// <summary>
    /// Read the subscription's events as an async stream, deserializing each event's <c>data:</c>
    /// payload from JSON to <typeparamref name="T"/>. Completes when the subscription completes or
    /// <paramref name="cancellationToken"/> trips.
    /// </summary>
    /// <typeparam name="T">The payload type to deserialize each event's data into.</typeparam>
    /// <param name="subscription">The subscription to read from.</param>
    /// <param name="serializerOptions">
    /// The serializer options, or null to use <see cref="JsonSerializerDefaults.Web"/> defaults.
    /// </param>
    /// <param name="cancellationToken">Ends the enumeration when tripped.</param>
    /// <returns>An async stream of deserialized payloads.</returns>
    public static async IAsyncEnumerable<T?> ReadAllAsync<T>(
        this StreamSubscription subscription,
        JsonSerializerOptions? serializerOptions = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        var serializer = serializerOptions ?? DefaultSerializerOptions;
        await foreach (var evt in subscription.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return JsonSerializer.Deserialize<T>(evt.Data, serializer);
        }
    }

    /// <summary>
    /// Publish every item of an async stream to a topic, serializing each item to JSON. Returns when
    /// the source stream completes or <paramref name="cancellationToken"/> trips. Each item is one
    /// publish, so back-pressure and the DropOldest model apply per item exactly as with a manual
    /// publish loop.
    /// </summary>
    /// <typeparam name="T">The item type to serialize.</typeparam>
    /// <param name="hub">The hub to publish through.</param>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="source">The async stream of items to publish.</param>
    /// <param name="serializerOptions">
    /// The serializer options, or null to use the hub's configured
    /// <see cref="StreamOptions.SerializerOptions"/> (falling back to
    /// <see cref="JsonSerializerDefaults.Web"/> defaults only when the hub exposes none).
    /// </param>
    /// <param name="eventName">The optional SSE <c>event:</c> name applied to every published item.</param>
    /// <param name="cancellationToken">Stops draining the source when tripped.</param>
    /// <returns>The number of items published.</returns>
    public static async Task<long> PublishAllAsync<T>(
        this ISseHub hub,
        string topic,
        IAsyncEnumerable<T> source,
        JsonSerializerOptions? serializerOptions = null,
        string? eventName = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(hub);
        ArgumentNullException.ThrowIfNull(source);

        // Resolve once: an explicit override wins, else the hub's configured options, else the web
        // default. The resolved instance is passed to each per-item publish as a non-null override so
        // every item serializes with exactly these options and the resolution is not repeated per item.
        var serializer = StreamSerializerOptionsResolver.Resolve(hub, serializerOptions);
        var published = 0L;
        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            hub.Publish(topic, item, serializer, eventName);
            published++;
        }
        return published;
    }
}
