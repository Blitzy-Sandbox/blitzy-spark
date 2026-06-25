# Streaming Shuffle Configuration

This page is the authoritative reference for every configuration key that governs the opt-in **streaming shuffle** path. All streaming keys are registered in `core/src/main/scala/org/apache/spark/internal/config/package.scala`, immediately after the existing `SHUFFLE_MANAGER` entry, and each is declared with `.version("4.2.0")` and range `checkValue` validation so that invalid values fail fast at configuration construction.

## Configuration keys

| Key | Type | Default | Range / Notes |
|-----|------|---------|---------------|
| `spark.shuffle.manager` | String | `sort` | `sort` / `tungsten-sort` / `streaming` — selector for the ShuffleManager; resolves via the `shortShuffleMgrNames` alias map. |
| `spark.shuffle.streaming.enabled` | Boolean | `false` | Opt-in master switch for the streaming path. |
| `spark.shuffle.streaming.bufferSizePercent` | Int | `20` | **1–50** — percent of executor memory used for per-partition in-memory buffers. |
| `spark.shuffle.streaming.spillThreshold` | Int | `80` | **50–95** — percent buffer utilization that triggers a disk spill. |
| `spark.shuffle.streaming.maxBandwidthMBps` | Int | `0` | `0` = unlimited; otherwise a per-executor rate limit in MB/s. |
| `spark.shuffle.streaming.debug` | Boolean | `false` | Enables verbose debug logging for the streaming path. |

The five `spark.shuffle.streaming.*` keys map one-to-one to new `ConfigEntry` values added after `SHUFFLE_MANAGER`; `spark.shuffle.manager` is the pre-existing selector whose `shortShuffleMgrNames` alias map gains the `"streaming"` short name. Because every default preserves current behavior, simply upgrading does not change how any existing application shuffles.

## Activation contract

Streaming shuffle engages **only when both** of the following are set:

- `spark.shuffle.manager=streaming`, **and**
- `spark.shuffle.streaming.enabled=true`.

If either flag is absent (or set to any other value), the inner `SortShuffleManager` handles **all** shuffle and behavior is identical to a stock Spark deployment. The `streaming` selector resolves to `StreamingShuffleManager`, which composes — and falls back to — an inner `SortShuffleManager`.

Key guarantees for this configuration surface:

- **Default unchanged.** `spark.shuffle.manager` keeps its default of `sort`, so existing deployments are unaffected and incur zero behavioral change.
- **Immutable for the application lifetime.** These keys are read once during `ShuffleManager` construction. There is **no dynamic reconfiguration in v1** — changing a value requires restarting the application.
- **Fail-fast range validation.** `bufferSizePercent ∈ [1, 50]` and `spillThreshold ∈ [50, 95]` are enforced by `checkValue` in `internal/config/package.scala`. An out-of-range value raises an error at config construction rather than being silently clamped.
- **Opt-in gating.** Setting only `spark.shuffle.manager=streaming` without `spark.shuffle.streaming.enabled=true` keeps the streaming data path dormant; the master switch must be explicitly enabled.

## Example

Enable streaming shuffle for a single job via `spark-submit`:

```bash
spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=20 \
  --conf spark.shuffle.streaming.spillThreshold=80 \
  --conf spark.shuffle.streaming.maxBandwidthMBps=0 \
  --class com.example.MyJob my-app.jar
```

Or programmatically via `SparkConf`:

```scala
val conf = new SparkConf()
  .set("spark.shuffle.manager", "streaming")
  .set("spark.shuffle.streaming.enabled", "true")
  .set("spark.shuffle.streaming.bufferSizePercent", "20")
  .set("spark.shuffle.streaming.spillThreshold", "80")
```

Both forms set the dual-flag activation contract; the remaining keys fall back to their defaults when omitted.

## See also

The companion streaming-shuffle TechDocs and end-user guides accompany this configuration reference:

- [index.md](index.md) — the streaming-shuffle documentation landing page (including the ten zero-data-loss failure scenarios).
- [architecture.md](architecture.md) — the component and protocol overview with the three Mermaid architecture diagrams.
- [observability.md](observability.md) — metrics, the MDC correlation schema, JMX/Prometheus exposition, and the dashboard.
- [decision-log.md](decision-log.md) — the architecture decision (ADR) table and the requirement → source → test traceability matrix.
- [executive-summary.html](executive-summary.html) — the reveal.js executive presentation deck.
- [dashboard.json](dashboard.json) — the Grafana dashboard template.
- Jekyll end-user docs: [architecture](../../docs/streaming-shuffle-architecture.md), [guide](../../docs/streaming-shuffle-guide.md), [tuning](../../docs/streaming-shuffle-tuning.md), and [troubleshooting](../../docs/streaming-shuffle-troubleshooting.md).
