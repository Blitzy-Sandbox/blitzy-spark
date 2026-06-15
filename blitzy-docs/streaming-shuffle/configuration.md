# Configuration

This page is the canonical configuration reference for the **streaming shuffle** backend. The backend is **opt-in and off by default**: it engages only when it is explicitly selected *and* explicitly enabled, so the default behavior of every existing deployment is unchanged. The sections below describe the configuration keys, how to activate the backend, its immutability guarantees, the buffer-sizing formula, and end-to-end examples.

For a high-level introduction, see the [overview](index.md). For how these settings drive buffering, backpressure, spill, and fallback at runtime, see the [Architecture](architecture.md) page.

## Configuration keys

The streaming shuffle backend introduces five `spark.shuffle.streaming.*` keys. All five default to a safe, conservative value, and the feature is inert unless it is explicitly activated (see [Activation](#activation)).

| Key | Type | Default | Range/Notes |
|-----|------|---------|-------------|
| `spark.shuffle.streaming.enabled` | Boolean | `false` | Opt-in feature flag. Must be `true` (together with `spark.shuffle.manager=streaming`) to engage streaming shuffle. |
| `spark.shuffle.streaming.bufferSizePercent` | Integer | `20` | Valid range **1–50**. Percent of executor memory used for per-partition streaming buffers. |
| `spark.shuffle.streaming.spillThreshold` | Integer | `80` | Valid range **50–95**. Percent buffer utilization that triggers disk spill. |
| `spark.shuffle.streaming.maxBandwidthMBps` | Integer | `unlimited (≤ 0)` | Per-executor streaming bandwidth cap (MB/s), enforced by the token-bucket rate limiter. A value **≤ 0 means unlimited**. |
| `spark.shuffle.streaming.debug` | Boolean | `false` | Enables additional verbose debug logging for the streaming path. |

> **Note on `maxBandwidthMBps`.** The default is **unlimited**. Any value **less than or equal to `0`** (including `0`) disables the per-executor cap entirely; a positive value sets the cap, in megabytes per second, applied by the token-bucket rate limiter.

## Activation

Activating the streaming shuffle backend requires **two** independent configuration signals — a *dual-flag opt-in*.

- **Select the manager.** `spark.shuffle.manager` is an existing Spark property (default `sort`). Set it to **`streaming`** to select the `StreamingShuffleManager`. This is wired in through the new `streaming` alias registered in the `ShuffleManager` factory's `shortShuffleMgrNames` map; no scheduler or environment change is involved.
- **Enable the feature.** Set `spark.shuffle.streaming.enabled=true`.

**Dual-flag opt-in (BOTH are required):** streaming shuffle engages **only** when

```text
spark.shuffle.manager=streaming   AND   spark.shuffle.streaming.enabled=true
```

If **either** flag is unset or `false`, Spark uses the existing **sort-based shuffle**. Setting `spark.shuffle.manager=streaming` without `spark.shuffle.streaming.enabled=true` (or vice versa) does **not** activate the streaming path.

Because **both flags default to off**, the **default behavior of every existing deployment is byte-for-byte unchanged**. Clusters that do not opt in are entirely unaffected.

Even when both flags are set, the backend may **automatically fall back** to sort-based shuffle under adverse runtime conditions (for example, sustained consumer slowness, memory pressure, network saturation, or a producer/consumer version mismatch). This automatic fallback is the feature's **zero-regression guarantee**. See the [Architecture](architecture.md) page for the four fallback conditions and how the decision is made.

## Configuration immutability

In v1, streaming shuffle configuration is **immutable for the application lifetime**. Changing any `spark.shuffle.streaming.*` value — or switching `spark.shuffle.manager` — takes effect only after **restarting the application/executors**.

There is **no dynamic reconfiguration in v1**: values are read once at startup and are not re-read while the application is running. Plan configuration changes as a restart, not a live update.

## Buffer sizing

Each partition receives an in-memory streaming buffer. The per-partition buffer size is derived from executor memory, the configured `bufferSizePercent`, and the number of shuffle partitions:

```text
per-partition buffer = (executorMemory * bufferSizePercent / 100) / numPartitions
```

A **2 MB floor** applies: the per-partition buffer is **never smaller than 2 MB**, regardless of the computed value.

Briefly:

- A **higher** `bufferSizePercent` allocates more executor memory to streaming buffers, which **reduces spill frequency** but leaves less memory for other executor needs.
- With **many partitions**, the per-partition share **shrinks toward the 2 MB floor**, after which adding partitions no longer reduces individual buffer size.

For deeper tuning guidance, see the [Architecture](architecture.md) page and the Jekyll tuning guide; this reference keeps tuning detail intentionally light.

## Example

The following examples enable streaming shuffle with the default buffer and spill settings and an unlimited bandwidth cap.

### `conf/spark-defaults.conf`

```properties
spark.shuffle.manager                         streaming
spark.shuffle.streaming.enabled               true
spark.shuffle.streaming.bufferSizePercent     20
spark.shuffle.streaming.spillThreshold        80
spark.shuffle.streaming.maxBandwidthMBps      0
```

### `spark-submit`

```bash
./bin/spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=20 \
  --conf spark.shuffle.streaming.spillThreshold=80 \
  ...
```

The same keys can also be set programmatically on a `SparkConf` before the `SparkContext` is created:

```scala
val conf = new SparkConf()
  .set("spark.shuffle.manager", "streaming")
  .set("spark.shuffle.streaming.enabled", "true")
  .set("spark.shuffle.streaming.bufferSizePercent", "20")
  .set("spark.shuffle.streaming.spillThreshold", "80")
  .set("spark.shuffle.streaming.maxBandwidthMBps", "0")

val sc = new SparkContext(conf)
```

Because configuration is immutable for the application lifetime (see [Configuration immutability](#configuration-immutability)), set these values before the context starts.

## See also

- [Overview](index.md) — what streaming shuffle is and when to use it.
- [Observability](observability.md) — the metrics emitted for these settings and how to monitor them.
- [Architecture](architecture.md) — how these settings drive buffering, backpressure, spill, and automatic fallback.
