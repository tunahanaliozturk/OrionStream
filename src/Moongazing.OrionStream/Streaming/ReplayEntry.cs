namespace Moongazing.OrionStream.Streaming;

/// <summary>
/// One retained delivery a replay store holds so a reconnecting client can resume from it. An entry is
/// the unit a store both retains (on <see cref="IReplayStore.Append(ReplayEntry)"/>) and returns (from
/// <see cref="IReplayStore.GetReplay(string)"/>).
/// </summary>
/// <param name="Sequence">
/// The hub-assigned topic-monotonic sequence for this delivery. It is the ordering key: entries are
/// retained and replayed in ascending <see cref="Sequence"/> order, and resume replays every entry
/// whose sequence is greater than the matched one. The sequence is per topic and strictly increasing
/// with no gaps, even when the producer also set its own <see cref="WireId"/>. See the event-id
/// allocation contract on <see cref="ISseHub"/>.
/// </param>
/// <param name="WireId">
/// The exact value this delivery emitted as its SSE <c>id:</c> field: the producer-supplied
/// <see cref="ServerSentEvent.Id"/> if the event set one, otherwise the hub sequence rendered as a
/// string. A returning <c>Last-Event-ID</c> is matched against this value to locate the resume point,
/// which is why a producer id and a hub sequence both round-trip through resume. Null only when replay
/// is meaningless for the delivery (no id at all), which the hub never produces because it always
/// stamps a sequence.
/// </param>
/// <param name="Event">
/// The stamped per-delivery event to replay on the wire. It already carries the hub sequence (the hub
/// stamps a per-delivery copy, never the producer's shared instance), so a store may return it as-is
/// for delivery.
/// </param>
public readonly record struct ReplayEntry(long Sequence, string? WireId, ServerSentEvent Event);
