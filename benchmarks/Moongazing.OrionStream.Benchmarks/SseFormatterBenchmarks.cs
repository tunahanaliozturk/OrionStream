namespace Moongazing.OrionStream.Benchmarks;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

using Moongazing.OrionStream.Streaming;

/// <summary>
/// Measures the SSE wire-format renderer (<see cref="SseFormatter.Format"/>): the per-event string
/// building, newline normalization, and multi-line <c>data:</c> splitting that runs once for every
/// event written to every connected client.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net80)]
[SimpleJob(RuntimeMoniker.Net90)]
public class SseFormatterBenchmarks
{
    private ServerSentEvent dataOnly = null!;
    private ServerSentEvent fullMetadata = null!;
    private ServerSentEvent multiLine = null!;

    [GlobalSetup]
    public void Setup()
    {
        dataOnly = new ServerSentEvent
        {
            Data = "{\"id\":42,\"status\":\"created\"}",
        };

        fullMetadata = new ServerSentEvent
        {
            Id = "42",
            EventName = "order.created",
            RetryMilliseconds = 3000,
            Data = "{\"id\":42,\"status\":\"created\"}",
        };

        multiLine = new ServerSentEvent
        {
            Id = "99",
            EventName = "order.updated",
            Data = "line one\nline two\r\nline three\rline four\nline five",
        };
    }

    [Benchmark(Baseline = true)]
    public string FormatDataOnly() => SseFormatter.Format(dataOnly);

    [Benchmark]
    public string FormatFullMetadata() => SseFormatter.Format(fullMetadata);

    [Benchmark]
    public string FormatMultiLine() => SseFormatter.Format(multiLine);
}
