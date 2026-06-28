namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// The default <see cref="IReplayStore"/>: a bounded in-process ring of the newest events per topic.
/// This is the implementation the hub has always used for replay, now behind the
/// <see cref="IReplayStore"/> seam. It has no dependencies and stays the default. It is in-process and
/// bounded, so per-topic memory is capped and a backlog does not survive a process restart; a durable,
/// cross-instance store is still planned as a separate opt-in package behind the same seam.
/// </summary>
/// <remarks>
/// Not internally synchronized: the hub serializes every call to a given instance under the owning
/// topic's lock (see <see cref="IReplayStore"/>), the same lock under which it assigns the monotonic
/// sequence, so this store sees a strictly increasing, gap-free sequence on <see cref="Append"/> and
/// never has a publish race a <see cref="GetReplay"/>. Adding a lock here would be redundant with that
/// gate and would only slow the wire path.
/// </remarks>
public sealed class InMemoryReplayStore : IReplayStore
{
    // Ring of the newest entries, oldest (lowest sequence) at the front. Holds at most capacity items.
    private readonly Queue<ReplayEntry> ring;
    private readonly int capacity;

    /// <summary>Create an in-memory store retaining at most <paramref name="capacity"/> newest events.</summary>
    /// <param name="capacity">The retention bound. Must be greater than zero.</param>
    public InMemoryReplayStore(int capacity)
    {
        if (capacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity,
                "Replay store capacity must be at least 1; the hub does not create a store for a topic with replay disabled.");
        }

        this.capacity = capacity;
        ring = new Queue<ReplayEntry>(capacity);
    }

    /// <inheritdoc />
    public bool HasBacklog => ring.Count > 0;

    /// <inheritdoc />
    public void Append(ReplayEntry entry)
    {
        // Evict the oldest entry once full, then enqueue the newest, keeping the ring ordered by
        // ascending sequence with the oldest at the front. Identical to the ring the hub kept inline
        // before the seam existed.
        if (ring.Count == capacity)
        {
            ring.Dequeue();
        }
        ring.Enqueue(entry);
    }

    /// <inheritdoc />
    public IReadOnlyList<ReplayEntry> GetReplay(string lastEventId)
    {
        // Locate the resume point by matching the client's Last-Event-ID against the EXACT value each
        // retained entry emitted on the wire (its WireId: the producer-supplied id if the event set one,
        // otherwise the hub sequence). This is what makes resume correct whether the producer relied on
        // the hub sequence or set its own ids: the id the browser sends back is always the id it last
        // saw on the wire, and that is what we match here.
        long? resumeAfter = null;
        foreach (var entry in ring)
        {
            if (string.Equals(entry.WireId, lastEventId, StringComparison.Ordinal))
            {
                resumeAfter = entry.Sequence;
                break;
            }
        }

        if (resumeAfter is not { } afterSequence)
        {
            // Unknown / evicted: no resume point, so the caller starts the subscription from now with no
            // replay. Returning an empty list (never a partial backlog) is the from-now fallback.
            return [];
        }

        // The monotonic Sequence is the ordering key: replay every retained entry published AFTER the
        // matched one, in ring order. Sized to the suffix length so the common single-reconnect case
        // does not over-allocate.
        var replay = new List<ReplayEntry>();
        foreach (var entry in ring)
        {
            if (entry.Sequence > afterSequence)
            {
                replay.Add(entry);
            }
        }

        return replay;
    }
}
