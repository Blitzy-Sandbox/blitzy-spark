# Streaming Shuffle Configuration

Streaming shuffle is an **opt-in** shuffle backend that pipelines shuffle data
directly from producer (map) executors to consumer (reduce) executors through
bounded in-memory buffers, reducing shuffle-materialization latency while keeping
the production-stable sort-based shuffle as an always-available fallback. It is
tuned through five keys in the `spark.shuffle.streaming.*` namespace, all
introduced in Spark **4.2.0**. This page is the authoritative reference for those
keys: the values documented here mirror the registered `ConfigEntry` definitions
exactly — the same values published in the Jekyll `docs/configuration.md`
"Shuffle Behavior" table — so the documentation and the engine never diverge.

See also: [Architecture](architecture.md) — how the streaming backend is
structured and how it coexists with sort-based shuffle — and
[Observability](observability.md) — the metrics, MDC logging schema, and
Prometheus/Grafana wiring.

## Dual activation gate

Streaming shuffle is active **if and only if BOTH** of the following are set:

- `spark.shuffle.manager=streaming` — selects the streaming `ShuffleManager`.
- `spark.shuffle.streaming.enabled=true` — the opt-in flag.

If **only one** of the two is set, Spark silently uses its default
**sort-based** shuffle — **no error is raised**. Requiring both properties is
**defense-in-depth**: it prevents accidental enablement, so neither selecting the
manager alone nor flipping the flag alone can switch a workload onto the streaming
path unintentionally. Read (and set) this gate first — the most common
misconfiguration is setting only one of the two required properties.

Enable it in `conf/spark-defaults.conf`:

```
spark.shuffle.manager           streaming
spark.shuffle.streaming.enabled true
```

…or on the `spark-submit` command line:

```
spark-submit \
  --conf spark.shuffle.manager=streaming \
  --conf spark.shuffle.streaming.enabled=true \
  ...
```

## Configuration properties

All five keys live in the `spark.shuffle.streaming.*` namespace and were
introduced in Spark 4.2.0.

| Property Name | Default | Meaning | Range / Since |
|---|---|---|---|
| `spark.shuffle.streaming.enabled` | `false` | Opt-in flag for the streaming shuffle backend. Takes effect only when combined with `spark.shuffle.manager=streaming` (the dual activation gate). | Boolean · 4.2.0 |
| `spark.shuffle.streaming.bufferSizePercent` | `20` | Percent of executor memory used for per-partition streaming buffers. | Integer `[1, 50]` · 4.2.0 |
| `spark.shuffle.streaming.spillThreshold` | `80` | Buffer-utilization percent at which the largest / least-recently-used buffered partitions spill to disk (`DISK_ONLY`). | Integer `[50, 95]` · 4.2.0 |
| `spark.shuffle.streaming.maxBandwidthMBps` | `0` | Per-executor streaming rate limit in MB/s. `0` means **unlimited**. | Integer (`0` = unlimited) · 4.2.0 |
| `spark.shuffle.streaming.debug` | `false` | Elevates the `org.apache.spark.shuffle.streaming` logger to DEBUG (via the log4j2 `Configurator`) at manager construction. Disabled by default; increases log volume. | Boolean · 4.2.0 |

## Immutability and executor restart

Streaming-shuffle configuration is **immutable for the application lifetime**.
There is **no dynamic reconfiguration in v1** — changing any
`spark.shuffle.streaming.*` key **requires an executor restart** to take effect.
Set the keys before the application (and its executors) start; altering them on a
running application has no effect until the executors are restarted with the new
values.

## Buffer sizing

Each per-partition streaming buffer is sized by the formula:

```
(executorMemory × bufferPercent) / numPartitions
```

subject to a **2&nbsp;MB floor** per partition — a buffer is never sized below
2&nbsp;MB, even when the formula would yield a smaller value. Here `bufferPercent`
is `spark.shuffle.streaming.bufferSizePercent` (default `20`, range `[1, 50]`),
expressed as a percentage of the executor memory available for streaming buffers;
`numPartitions` is the number of shuffle partitions; and `executorMemory` is the
executor's memory. Increasing `bufferSizePercent` raises the share of memory each
executor devotes to buffering — trading memory headroom for a deeper pipeline —
while a larger `numPartitions` shrinks each individual buffer (down to the
2&nbsp;MB floor).

For guidance on choosing these values for a given workload, see the
[tuning guidance in the architecture page](architecture.md).

## Bandwidth and rate limiting

When `spark.shuffle.streaming.maxBandwidthMBps > 0`, streaming applies
**token-bucket** rate limiting to bound per-executor egress. The effective
per-shuffle refill rate is:

```
Refill rate = maxBandwidthMBps / numConcurrentShuffles
```

so the configured bandwidth is shared across the shuffles running concurrently on
the executor. A link-capacity **safety factor of ~80%** is applied on top, so that
streaming does not saturate the network link. Setting `maxBandwidthMBps=0` (the
default) **disables** the limit entirely — streaming is unlimited and no token
bucket is engaged.

## Validation ranges

The numeric keys are validated **at startup**; out-of-range values are
**rejected** (the application fails fast rather than silently clamping):

- `spark.shuffle.streaming.bufferSizePercent` ∈ `[1, 50]`.
- `spark.shuffle.streaming.spillThreshold` ∈ `[50, 95]`.
- `spark.shuffle.streaming.maxBandwidthMBps` ≥ `0`, where `0` is the
  **unlimited** sentinel — this key has **no** upper bound.

Note that the **spill threshold (default 80%)** is distinct from the automatic
**fallback** memory condition (**~95%**). Crossing the spill threshold makes the
`MemorySpillManager` move the largest / least-recently-used buffered partitions to
disk (`DISK_ONLY`) **while the shuffle stays on the streaming path** — spilling is
a normal memory-management response. The fallback condition, by contrast, reverts
the shuffle to **sort-based** shuffle entirely. In short: **spilling keeps the
shuffle on the streaming path; fallback switches it to sort.**

## Related pages

- [Architecture](architecture.md) — how the streaming subsystem is structured and
  how it coexists with sort-based shuffle.
- [Observability](observability.md) — streaming-shuffle metrics, the MDC logging
  schema, and Prometheus/Grafana wiring.
