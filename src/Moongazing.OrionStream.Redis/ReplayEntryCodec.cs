namespace Moongazing.OrionStream.Redis;

using System.Text.Json;
using System.Text.Json.Serialization;

using Moongazing.OrionStream.Streaming;

/// <summary>
/// Serializes a <see cref="ReplayEntry"/> to and from the compact JSON form the Redis backlog stores,
/// so an entry retained by one process is reconstructed faithfully by another (cross-instance resume)
/// or by the same process after a restart.
/// </summary>
/// <remarks>
/// <para>
/// The wire contract the codec preserves: the value a delivery emitted as its SSE <c>id:</c> field
/// (<see cref="ReplayEntry.WireId"/>) must survive the round-trip exactly, because a returning
/// <c>Last-Event-ID</c> is matched against it. The hub renders the wire id from the event's producer
/// <see cref="ServerSentEvent.Id"/> when set, otherwise from the hub sequence it stamped on a
/// per-delivery copy. That stamped sequence is an <em>internal</em> field this package cannot set from
/// outside the core assembly, so the codec rebuilds the event with its <see cref="ServerSentEvent.Id"/>
/// pinned to the stored <see cref="ReplayEntry.WireId"/>. The reconstructed event therefore renders the
/// identical <c>id:</c> on the wire, whether the original id came from the producer or the hub
/// sequence: resume stays correct without the package reaching into the hub's id allocation.
/// </para>
/// <para>
/// <see cref="ReplayEntry.Sequence"/> is stored alongside as the ordering key. It is the hub's
/// gap-free per-topic sequence and is what the backlog is ordered and resumed by, independent of the
/// rendered wire id.
/// </para>
/// </remarks>
internal static class ReplayEntryCodec
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Serialize an entry to the JSON payload stored as one Redis list element.</summary>
    public static string Serialize(ReplayEntry entry)
    {
        var dto = new ReplayEntryDto
        {
            Sequence = entry.Sequence,
            WireId = entry.WireId,
            Data = entry.Event.Data,
            EventName = entry.Event.EventName,
            RetryMilliseconds = entry.Event.RetryMilliseconds,
        };

        return JsonSerializer.Serialize(dto, SerializerOptions);
    }

    /// <summary>
    /// Reconstruct an entry from a stored JSON payload. The rebuilt event pins its
    /// <see cref="ServerSentEvent.Id"/> to the stored <see cref="ReplayEntry.WireId"/> so it renders
    /// the same <c>id:</c> on the wire as the original delivery.
    /// </summary>
    /// <exception cref="FormatException">The payload is not a valid stored entry.</exception>
    public static ReplayEntry Deserialize(string payload)
    {
        ReplayEntryDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize<ReplayEntryDto>(payload, SerializerOptions);
        }
        catch (JsonException ex)
        {
            throw new FormatException("A stored OrionStream replay entry could not be parsed as JSON.", ex);
        }

        if (dto is null || dto.Data is null)
        {
            throw new FormatException("A stored OrionStream replay entry was missing its required payload.");
        }

        var evt = new ServerSentEvent
        {
            Data = dto.Data,
            EventName = dto.EventName,
            // Pin the wire id: the reconstructed event renders the stored WireId as its id: field,
            // whether the original came from a producer id or the hub sequence. This is what lets the
            // returning Last-Event-ID match a backlog rebuilt in a different process.
            Id = dto.WireId,
            RetryMilliseconds = dto.RetryMilliseconds,
        };

        return new ReplayEntry(dto.Sequence, dto.WireId, evt);
    }

    /// <summary>The on-the-wire shape of a stored entry. Property names are serialized in camelCase via the web defaults.</summary>
    private sealed class ReplayEntryDto
    {
        public long Sequence { get; set; }

        public string? WireId { get; set; }

        public string? Data { get; set; }

        public string? EventName { get; set; }

        public int? RetryMilliseconds { get; set; }
    }
}
