# Configuration

This page is the canonical configuration reference for the **streaming shuffle** backend. It describes the five `spark.shuffle.streaming.*` keys, the `spark.shuffle.manager=streaming` activation alias, the dual-flag opt-in, configuration immutability, and the per-partition buffer-sizing formula. The streaming backend is **opt-in and off by default**, so unless you explicitly enable it the cluster continues to use the existing sort-based shuffle with byte-for-byte identical behavior.

For a high-level overview of the feature, see the [overview](index.md). For how these settings drive the runtime, see [Architecture](architecture.md) and [Observability](observability.md).

## Configuration keys

The streaming backend adds the following five configuration keys. All keys are read once at executor startup (see [Configuration immutability](#configuration-immutability)) and every key has a safe, off-by-default value.

| Key | Type | Default | Range/Notes |
|-----|------|---------|-------------|
| `spark.shuffle.streaming.enabled` | Boolean | `false` | Opt-in feature flag. Must be `true` (together with `spark.shuffle.manager=streaming`) to engage streaming shuffle. |
| `spark.shuffle.streaming.bufferSizePercent` | Integer | `20` | Valid range **1–50**. Percent of executor memory used for per-partition streaming buffers. |
| `spark.shuffle.streaming.spillThreshold` | Integer | `80` | Valid range **50–95**. Percent buffer utilization that triggers disk spill. |
| `spark.shuffle.streaming.maxBandwidthMBps` | Integer | `unlimited (≤ 0)` | Per-executor streaming bandwidth cap (MB/s), enforced by the token-bucket rate limiter. A value **≤ 0 means unlimited**. |
| `spark.shuffle.streaming.debug` | Boolean | `false` | Enables additional verbose debug logging for the streaming path. |

!!! note
    `spark.shuffle.streaming.maxBandwidthMBps` has **no fixed upper bound**: any value less than or equal to `0` disables rate limiting entirely (unlimited bandwidth). Set a positive value only when you need to cap per-executor streaming throughput.

## Activation

Activating the streaming shuffle backend requires **two** configuration signals working together.

- **`spark.shuffle.manager`** is an existing Spark property (default `sort`). Set it to **`streaming`** to select the `StreamingShuffleManager`. This value is the new alias registered in the `ShuffleManager` factory's `shortShuffleMgrNames` map; Spark resolves and instantiates the manager reflectively, so no scheduler or environment change is involved.
- **`spark.shuffle.streaming.enabled`** is the per-feature opt-in flag (default `false`). It must be set to **`true`** for the streaming path to engage.

### Dual-flag opt-in (BOTH required)

Streaming shuffle engages **only** when **both** of the following are true:

```
spark.shuffle.manager           = streaming
spark.shuffle.streaming.enabled = true
```

If **either** flag is unset or false, Spark uses the sort-based shuffle. Setting `spark.shuffle.manager=streaming` alone (without `enabled=true`), or setting `enabled=true` while leaving the manager at its `sort` default, does **not** activate streaming.

Because both flags default to off, the **default behavior of every existing deployment is byte-for-byte unchanged** — you must consciously opt in on both axes.

### Automatic fallback

Even when both flags are set, the backend may **automatically fall back** to the sort-based shuffle under adverse runtime conditions. Fallback is evaluated against four conditions (slow consumer, memory pressure, network saturation, and producer/consumer version mismatch) — see [Architecture](architecture.md) for the full description of the four fallback conditions and how the decision is made. This automatic revert is what provides the feature's **zero-regression guarantee**: workloads that are unsuitable for streaming transparently use the proven sort-based path.

## Configuration immutability

Streaming shuffle configuration is **immutable for the lifetime of the application**. Changing any `spark.shuffle.streaming.*` value — or changing `spark.shuffle.manager` — requires **restarting the application (and therefore its executors)** for the new values to take effect.

There is **no dynamic reconfiguration in v1**: values are read once when each executor starts and are not re-read while the application runs. Plan configuration changes as part of a normal application restart.

## Buffer sizing

Each reduce partition is backed by an in-memory streaming buffer. The per-partition buffer size is derived from executor memory, the configured percentage, and the partition count:

```
per-partition buffer = (executorMemory * bufferSizePercent / 100) / numPartitions
```

A **2 MB floor** applies: the per-partition buffer is **never smaller than 2 MB**, regardless of what the formula produces. If the computed share falls below 2 MB, the buffer is clamped up to the 2 MB floor.

Tuning guidance, in brief:

- A higher `spark.shuffle.streaming.bufferSizePercent` enlarges each buffer, which **reduces spill frequency** but **consumes more executor memory**.
- With **many partitions**, the per-partition share shrinks toward the **2 MB floor**, so very high partition counts effectively pin buffers at the floor.

This page keeps tuning detail light; see the [Architecture](architecture.md) page for how buffering, spill, and backpressure interact in depth.

## Example

The following examples enable streaming shuffle with the default tuning values. The values are consistent with the [Configuration keys](#configuration-keys) table above.

### `conf/spark-defaults.conf`

```properties
spark.shuffle.manager                         streaming
spark.shuffle.streaming.enabled               true
spark.shuffle.streaming.bufferSizePercent     20
spark.shuffle.streaming.spillThreshold        80
spark.shuffle.streaming.maxBandwidthMBps      0
```

Here `spark.shuffle.streaming.maxBandwidthMBps 0` leaves per-executor streaming bandwidth **unlimited** (any value `≤ 0` disables the rate limiter).

### `spark-submit`

```bash
./bin/spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  --conf spark.shuffle.streaming.bufferSizePercent=20 \
  --conf spark.shuffle.streaming.spillThreshold=80 \
  ...
```

### Programmatic configuration

The same keys can be set programmatically on a `SparkConf` **before** the `SparkContext` is created (the values are still immutable for the application lifetime once the context starts):

```scala
val conf = new SparkConf()
  .set("spark.shuffle.manager", "streaming")
  .set("spark.shuffle.streaming.enabled", "true")
  .set("spark.shuffle.streaming.bufferSizePercent", "20")
  .set("spark.shuffle.streaming.spillThreshold", "80")
  .set("spark.shuffle.streaming.maxBandwidthMBps", "0")

val sc = new SparkContext(conf)
```

## See also

- [overview](index.md) — what the streaming shuffle feature is and when to use it.
- [Observability](observability.md) — the metrics emitted for these settings (buffer utilization, spill count, backpressure events, partial-read invalidations).
- [Architecture](architecture.md) — how these settings drive buffering, backpressure, spill, and the automatic fallback to sort-based shuffle.
