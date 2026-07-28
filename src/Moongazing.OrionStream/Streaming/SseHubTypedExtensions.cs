namespace Moongazing.OrionStream.Streaming;

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

/// <summary>
/// Typed publish helpers over <see cref="ISseHub"/>: serialize a payload to the SSE <c>data:</c>
/// field with <see cref="System.Text.Json"/> instead of calling <see cref="JsonSerializer"/> by hand
/// at each publish site. These are thin wrappers over <see cref="ISseHub.Publish(string, ServerSentEvent)"/>;
/// the raw string publish is unchanged and remains the lower-level primitive.
/// </summary>
public static class SseHubTypedExtensions
{
    /// <summary>
    /// Serialize <paramref name="payload"/> to JSON and publish it as the <c>data:</c> of a new event.
    /// </summary>
    /// <typeparam name="T">The payload type to serialize.</typeparam>
    /// <param name="hub">The hub to publish through.</param>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="payload">The payload to serialize into the event data.</param>
    /// <param name="serializerOptions">
    /// The serializer options, or null to use the hub's configured
    /// <see cref="StreamOptions.SerializerOptions"/> (falling back to
    /// <see cref="JsonSerializerDefaults.Web"/> defaults only when the hub exposes none).
    /// </param>
    /// <param name="eventName">The optional SSE <c>event:</c> name.</param>
    /// <param name="id">The optional producer-supplied SSE <c>id:</c>.</param>
    /// <param name="retryMilliseconds">The optional SSE <c>retry:</c> reconnection hint.</param>
    /// <returns>The number of subscribers the event was delivered to.</returns>
    /// <remarks>
    /// Serializes <typeparamref name="T"/> with reflection-based <see cref="JsonSerializer"/>, so it is
    /// not trim/NativeAOT-safe out of the box. To publish under trimming or AOT, pass a
    /// <see cref="JsonSerializerOptions"/> backed by a source-generated <c>JsonSerializerContext</c>, or
    /// serialize yourself and publish the raw <see cref="ServerSentEvent"/> via
    /// <see cref="ISseHub.Publish(string, ServerSentEvent)"/> (which is fully AOT-clean).
    /// </remarks>
    [RequiresUnreferencedCode("JSON serialization of the payload uses reflection; supply a source-generated JsonSerializerContext, or publish a pre-serialized ServerSentEvent via ISseHub.Publish, to stay trim-safe.")]
    [RequiresDynamicCode("JSON serialization of the payload may require runtime code generation; supply a source-generated JsonSerializerContext, or publish a pre-serialized ServerSentEvent via ISseHub.Publish, to stay NativeAOT-safe.")]
    public static int Publish<T>(
        this ISseHub hub,
        string topic,
        T payload,
        JsonSerializerOptions? serializerOptions = null,
        string? eventName = null,
        string? id = null,
        int? retryMilliseconds = null)
    {
        ArgumentNullException.ThrowIfNull(hub);

        var serializer = StreamSerializerOptionsResolver.Resolve(hub, serializerOptions);
        var data = JsonSerializer.Serialize(payload, serializer);
        var evt = new ServerSentEvent
        {
            Data = data,
            EventName = eventName,
            Id = id,
            RetryMilliseconds = retryMilliseconds,
        };
        return hub.Publish(topic, evt);
    }
}
