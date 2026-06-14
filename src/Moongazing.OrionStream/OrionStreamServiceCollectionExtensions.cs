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
        services.TryAddSingleton<ISseHub, SseHub>();

        return services;
    }
}
