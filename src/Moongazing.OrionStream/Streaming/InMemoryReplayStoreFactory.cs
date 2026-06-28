namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// The default <see cref="IReplayStoreFactory"/>: hands out one <see cref="InMemoryReplayStore"/> per
/// topic. Registered by <see cref="Moongazing.OrionStream.OrionStreamServiceCollectionExtensions.AddOrionStream(Microsoft.Extensions.DependencyInjection.IServiceCollection, System.Action{StreamOptions})"/>
/// unless a caller has already registered a factory of their own, so the in-memory ring stays the
/// default and the only one with no dependencies.
/// </summary>
public sealed class InMemoryReplayStoreFactory : IReplayStoreFactory
{
    /// <inheritdoc />
    public IReplayStore Create(string topic, int capacity) => new InMemoryReplayStore(capacity);
}
