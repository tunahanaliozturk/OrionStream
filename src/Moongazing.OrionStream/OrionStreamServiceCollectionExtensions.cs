namespace Moongazing.OrionStream;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

/// <summary>
/// Registration helpers for OrionStream.
/// </summary>
public static class OrionStreamServiceCollectionExtensions
{
    /// <summary>
    /// Register the broadcast hub, its options, and diagnostics as singletons.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional buffering and heartbeat configuration.</param>
    public static IServiceCollection AddOrionStream(
        this IServiceCollection services,
        Action<StreamOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new StreamOptions();
        configure?.Invoke(options);
        options.Validate();

        services.TryAddSingleton(options);
        services.TryAddSingleton<StreamDiagnostics>();

        // The in-memory ring stays the default replay store and the only one with no dependencies.
        // TryAdd means a caller can register their own IReplayStoreFactory before AddOrionStream to swap
        // in an external store without the hub knowing where the backlog lives; the durable / backplane
        // store is still planned and ships as a separate opt-in package behind this same seam.
        services.TryAddSingleton<IReplayStoreFactory, InMemoryReplayStoreFactory>();
        services.TryAddSingleton<ISseHub, SseHub>();

        return services;
    }
}
