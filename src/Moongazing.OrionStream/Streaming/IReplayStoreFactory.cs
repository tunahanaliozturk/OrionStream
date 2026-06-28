namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// Creates the per-topic <see cref="IReplayStore"/> the hub uses to retain and resume a topic's
/// backlog. Registered as a singleton; the hub asks the factory for one store the first time a topic
/// with replay enabled is seen. Replace the registration to swap the in-memory ring for an external
/// store without the hub knowing where the backlog lives.
/// </summary>
/// <remarks>
/// The default is <see cref="InMemoryReplayStoreFactory"/>, which hands out an
/// <see cref="InMemoryReplayStore"/> per topic and is the only implementation with no dependencies. A
/// durable, cross-instance store is still planned and will ship as a separate opt-in package behind
/// this same seam; it is explicitly out of scope for the core package.
/// </remarks>
public interface IReplayStoreFactory
{
    /// <summary>
    /// Create the replay store for one topic. Called at most once per topic per hub instance, under the
    /// hub's topic-lifecycle lock, so the factory need not be thread-safe against itself for the same
    /// topic.
    /// </summary>
    /// <param name="topic">The topic the store backs. Case-sensitive, matching the hub's topic comparison.</param>
    /// <param name="capacity">
    /// The maximum number of newest events the store retains for this topic, already resolved from the
    /// global default and any per-topic override. Always greater than zero: the hub does not create a
    /// store for a topic with replay disabled.
    /// </param>
    /// <returns>A store dedicated to <paramref name="topic"/>.</returns>
    IReplayStore Create(string topic, int capacity);
}
