namespace Moongazing.OrionStream.Streaming;

using System.Text.Json;

/// <summary>
/// Internal capability a hub exposes so the typed publish helpers can serialize with the
/// configured <see cref="StreamOptions.SerializerOptions"/> when the caller passes no per-call
/// override. The hub owns its options; the static extension methods only see an
/// <see cref="ISseHub"/>, so this is how the configured serializer is threaded through to them
/// without widening the public <see cref="ISseHub"/> surface.
/// </summary>
internal interface IStreamSerializerOptionsProvider
{
    /// <summary>The serializer the hub was configured with for typed publish data.</summary>
    JsonSerializerOptions SerializerOptions { get; }
}
