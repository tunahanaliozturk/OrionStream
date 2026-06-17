# Benchmarks

Micro-benchmarks for OrionStream's in-memory hot paths, built with
[BenchmarkDotNet](https://benchmarkdotnet.org/). They cover only the parts that run with no network,
no browser, and no external service: the SSE wire-format renderer and the in-process broadcast hub.
Nothing here touches HTTP, a real `EventSource` client, or a proxy.

The project lives in `benchmarks/Moongazing.OrionStream.Benchmarks` and references the library
directly, so it always exercises the real public API.

## What is measured

| Benchmark class | Hot path | What it tells you |
| --- | --- | --- |
| `SseFormatterBenchmarks` | `SseFormatter.Format` | Per-event cost of building the `text/event-stream` wire output: field lines, newline normalization, and multi-line `data:` splitting. Runs once per event per client. |
| `SseHubPublishBenchmarks` | `SseHub.Publish` | Fan-out cost of one publish across a topic's subscribers, parameterized by subscriber count (`1`, `10`, `100`, `1000`), with buffers large enough to avoid drops so this isolates the iteration plus channel-write cost. |
| `SseHubThroughputBenchmarks` | `SseHub.Publish` plus reader drain | Single-subscriber throughput over a burst of events (`1k`, `10k`, `100k`). One variant keeps the reader ahead (no drops); the other forces the `DropOldest` back-pressure path that a slow client triggers. |
| `SseHubSubscriptionBenchmarks` | `SseHub.Subscribe` plus `StreamSubscription.Dispose` | Connect/disconnect churn: bounded-channel allocation, the per-topic concurrent-dictionary insert/remove, and lazy topic teardown when the last subscriber leaves. |

Every class uses `[MemoryDiagnoser]`, so allocations per operation are reported alongside time.

## Running

From the repository root:

```sh
dotnet run -c Release --project benchmarks/Moongazing.OrionStream.Benchmarks
```

That launches the BenchmarkDotNet switcher. Pass `--filter` to pick benchmarks, for example:

```sh
# everything
dotnet run -c Release --project benchmarks/Moongazing.OrionStream.Benchmarks -- --filter "*"

# just the formatter
dotnet run -c Release --project benchmarks/Moongazing.OrionStream.Benchmarks -- --filter "*SseFormatterBenchmarks*"

# just the publish fan-out
dotnet run -c Release --project benchmarks/Moongazing.OrionStream.Benchmarks -- --filter "*SseHubPublishBenchmarks*"
```

## Runtimes

Each benchmark runs on both **.NET 8** and **.NET 9** via `[SimpleJob]` so you can compare runtimes
side by side. Building those jobs requires the .NET 8 and .NET 9 SDKs (or runtimes) installed in
addition to the SDK used to launch the harness.

## Notes

- Run on a quiet machine; close other load so the numbers are stable.
- These are relative micro-benchmarks for spotting regressions in the in-memory paths, not an
  end-to-end measurement of SSE delivery over a real HTTP connection.
- No result numbers are committed here on purpose: they are machine-specific. Run the suite
  locally to get figures for your hardware.
