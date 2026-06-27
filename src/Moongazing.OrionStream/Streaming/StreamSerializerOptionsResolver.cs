namespace Moongazing.OrionStream.Streaming;

using System.Text.Json;

/// <summary>
/// Resolves the <see cref="JsonSerializerOptions"/> the typed publish helpers serialize with, so
/// both typed entry points (<see cref="SseHubTypedExtensions.Publish{T}"/> and
/// <see cref="StreamSubscriptionAsyncEnumerableExtensions.PublishAllAsync{T}"/>) resolve identically:
/// an explicit per-call override wins; otherwise the hub's configured
/// <see cref="StreamOptions.SerializerOptions"/> is used when the hub exposes it; the
/// <see cref="JsonSerializerDefaults.Web"/> default is the fallback only when neither is present.
/// </summary>
internal static class StreamSerializerOptionsResolver
{
    /// <summary>The web default used only when no override and no hub-configured options are present.</summary>
    internal static readonly JsonSerializerOptions WebDefault = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Resolve the effective serializer options: <paramref name="perCallOverride"/> if the caller
    /// supplied one, else the configured options the <paramref name="hub"/> exposes, else the web default.
    /// </summary>
    internal static JsonSerializerOptions Resolve(ISseHub hub, JsonSerializerOptions? perCallOverride)
    {
        if (perCallOverride is not null)
        {
            return perCallOverride;
        }

        return hub is IStreamSerializerOptionsProvider provider ? provider.SerializerOptions : WebDefault;
    }
}
