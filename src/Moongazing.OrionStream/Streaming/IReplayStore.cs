namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// The per-topic backlog a reconnecting client resumes from. Abstracts <em>where</em> the retained
/// events live so the in-memory ring is one implementation behind this seam and a caller can swap in an
/// external store (see <see cref="IReplayStoreFactory"/>) without the hub knowing where the backlog is
/// kept. The hub owns event-id allocation and live delivery; a store owns only retention and the
/// resume-point lookup.
/// </summary>
/// <remarks>
/// <para>
/// One store instance backs one topic. The hub serializes every call to a given instance under that
/// topic's lock, and pairs each store with the same lock it uses to assign sequences, so a store sees a
/// strictly increasing, gap-free <see cref="ReplayEntry.Sequence"/> on <see cref="Append"/> and never
/// has a publish interleave with a <see cref="GetReplay"/> on the same topic. Because of that, the
/// in-memory implementation needs no internal synchronization. A custom store that is reached only
/// through the hub may likewise assume single-threaded access per topic; a store shared across
/// processes (a future durable backplane) must provide its own consistency, but that is out of scope
/// for the in-memory default and ships separately.
/// </para>
/// <para>
/// The contract a store must honor for resume to stay correct, regardless of where it keeps the
/// backlog:
/// </para>
/// <list type="bullet">
/// <item><description>Retain entries in ascending <see cref="ReplayEntry.Sequence"/> order and return
/// them from <see cref="GetReplay"/> in that same order.</description></item>
/// <item><description>Bound retention: keep at most the newest <c>capacity</c> entries (the value
/// passed to <see cref="IReplayStoreFactory.Create"/>), evicting the lowest sequence first.</description></item>
/// <item><description>Resume is all-or-nothing: <see cref="GetReplay"/> replays the suffix after an
/// exactly matched <see cref="ReplayEntry.WireId"/>, or nothing at all when the id matches no retained
/// entry. It must never return a partial or gapped backlog.</description></item>
/// </list>
/// <para>
/// The default in-memory implementation is <see cref="InMemoryReplayStore"/>; it is the only one with
/// no dependencies and stays the default. A durable, cross-instance store is still planned and will
/// ship as a separate opt-in package behind this same seam.
/// </para>
/// </remarks>
public interface IReplayStore
{
    /// <summary>
    /// Retain a delivery for later resume. Called once per published event whose topic has replay
    /// enabled, in ascending <see cref="ReplayEntry.Sequence"/> order. The store keeps at most the
    /// configured capacity, evicting the oldest (lowest sequence) entry when full.
    /// </summary>
    /// <param name="entry">The stamped delivery to retain.</param>
    void Append(ReplayEntry entry);

    /// <summary>
    /// Locate the resume point for a returning <c>Last-Event-ID</c> and return the entries to replay.
    /// </summary>
    /// <remarks>
    /// When <paramref name="lastEventId"/> exactly equals the <see cref="ReplayEntry.WireId"/> of some
    /// retained entry, the result is every retained entry after it in ascending
    /// <see cref="ReplayEntry.Sequence"/> order. When it matches no retained entry (unknown, or evicted
    /// because it is older than the buffer still holds), the result is empty: the caller falls back to a
    /// from-now stream. The result is never a partial backlog. The caller is responsible for applying
    /// any per-subscriber filter and buffer policy to the returned entries; the store only locates and
    /// orders them.
    /// </remarks>
    /// <param name="lastEventId">The non-empty wire id the client last saw.</param>
    /// <returns>The entries to replay, in order, or an empty list for the from-now fallback.</returns>
    IReadOnlyList<ReplayEntry> GetReplay(string lastEventId);

    /// <summary>
    /// True while the store holds at least one entry a reconnecting client could resume from. The hub
    /// reads it to decide whether an otherwise-idle topic must be kept alive for its backlog rather than
    /// reclaimed.
    /// </summary>
    bool HasBacklog { get; }
}
