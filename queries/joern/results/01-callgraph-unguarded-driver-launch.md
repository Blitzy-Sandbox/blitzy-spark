# `01-callgraph-unguarded-driver-launch` — query outcome

Query 01 of the Phase 3 capability probe, and the **call-graph formulation** of the one reachability
class the probe attempts: an RPC entry point named `receive` or `receiveAndReply`, enclosed in a
type whose full name lies under `org.apache.spark.deploy.`, reaching a privileged sink —
`createDriver`, a `DriverRunner` construction, or a process launch — along a path on which no
derived authentication or ACL predicate appears.

This write-up is rendered from `queries/joern/results/01-callgraph-unguarded-driver-launch.json` and
from nothing else. That envelope is the machine state; this file renders it. Every number, boolean,
timestamp, hash, selector and method full name below is transcribed from the envelope, so the two
agree in every value they share. Nothing here is inferred, estimated, re-measured or taken from a
plan, a brief or a source file — in particular, no method identity and no line number comes from
any document, only from the envelope's captured output.

No user rules were provided for this work, so none are cited.

---

## 1. Query

| Field | Value, as the envelope records it |
|---|---|
| Slug | `01-callgraph-unguarded-driver-launch` |
| Source | `queries/joern/01-callgraph-unguarded-driver-launch.sc` |
| Source SHA-256 (most recent attempt) | `8a43c941b79af939e21085bda3a5d44a021a2fa043d1c861d63227d9520c5b52` |
| Invocation, argv as run | `joern --script queries/joern/01-callgraph-unguarded-driver-launch.sc` |
| `--param` values | **none** — `invocation.params` is empty, and the script's `@main def exec()` takes no parameters, so none is required or accepted |
| Invocation started | `2026-08-21T08:09:23Z` |
| Invocation ended | `2026-08-21T08:09:38Z` |
| Exit code | `0` |

The two timestamps are transcribed as the envelope carries them, as the identity of the recorded
invocation. No elapsed time is derived from them and no effort measure of any kind is expressed in
wall-clock terms here.

**The graph was read, not built.** The envelope's `graph` block records `source`
`harness/cpg/spark.cpg`, `loaded_with` `importCpg`, and `built` `false`; the `diagnostics.load_mode`
is `imported_persisted_cpg`. `importCode` — the command that would construct a graph — appears
nowhere in the query source, and nothing under `harness/` was authored, edited or removed by this
query.

---

## 2. Outcome

| Fact | Value | Evidence beside it, from the envelope |
|---|---|---|
| `compiled` | `true` | `markers.start_marker_seen` is `true`: the `---BLITZY-START---` marker, which the script prints as its very first action, was seen |
| `ran` | `true` | `markers.result_region_parsed` is `true`: the region between `---BLITZY-RESULT-BEGIN---` and `---BLITZY-RESULT-END---` parsed. `invocation.exit_code` is `0` |
| `return_count` | `8` | The `returns` array carries eight entries, rendered in full below |
| `not_evaluable` | `null` | No not-evaluable condition was recorded |
| `failure_reason` | `null` | No failure was recorded |

### 2.1 Returns

One row per return, with each method full name transcribed exactly as the envelope carries it.
`Path length` is the number of nodes in that return's `path`; `Predicates on path` is that return's
`predicates_on_path` list.

| # | Handler | Sink | Path length | Predicates on path |
|---|---|---|---|---|
| 1 | `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 6 | none — the list is empty |
| 2 | `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 5 | none — the list is empty |
| 3 | `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 3 | none — the list is empty |
| 4 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 6 | none — the list is empty |
| 5 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)` | 3 | none — the list is empty |
| 6 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)` | 3 | none — the list is empty |
| 7 | `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 6 | none — the list is empty |
| 8 | `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 5 | none — the list is empty |

### 2.2 Paths as emitted

Each return's `path`, handler first and sink last, in the envelope's order. These nodes are the
evidence for the path lengths above and are the nodes the on-path test in section 3 was applied
over.

**Return 1** — `ClientEndpoint.receiveAndReply` to `ProcessBuilder.start`:

1. `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`
3. `org.apache.spark.storage.BlockManagerStorageEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
4. `org.apache.spark.storage.BlockManagerStorageEndpoint$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
5. `org.apache.spark.util.Utils$.getHeapHistogram:java.lang.String[]()`
6. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 2** — `ClientEndpoint.receiveAndReply` to `Master$$createDriver`:

1. `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`
3. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
4. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
5. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

**Return 3** — `Master.receiveAndReply` to `Master$$createDriver`:

1. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

**Return 4** — `Worker.receive` to `ProcessBuilder.start`:

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.ExecutorRunner.start:void()`
4. `org.apache.spark.deploy.worker.ExecutorRunner$$anon$1.run:void()`
5. `org.apache.spark.deploy.worker.ExecutorRunner.org$apache$spark$deploy$worker$ExecutorRunner$$fetchAndRunExecutor:void()`
6. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 5** — `Worker.receive` to the `DriverRunner` construction:

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)`

**Return 6** — `Worker.receive` to the `ExecutorRunner` construction:

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)`

**Return 7** — `WorkerWatcher.receiveAndReply` to `ProcessBuilder.start`:

1. `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`
3. `org.apache.spark.storage.BlockManagerStorageEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
4. `org.apache.spark.storage.BlockManagerStorageEndpoint$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
5. `org.apache.spark.util.Utils$.getHeapHistogram:java.lang.String[]()`
6. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 8** — `WorkerWatcher.receiveAndReply` to `Master$$createDriver`:

1. `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`
3. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
4. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
5. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

---

## 3. Spurious returns under the on-path test

**The test as applied,** in the envelope's own words in `spurious_test`: a return is spurious when
an authentication or ACL predicate from the set the query derived at execution time lies on the
emitted path between the handler entry and the sink; on-path presence is the entire test, and
control dependence of the sink on that predicate is **not** required. Nothing narrower and nothing
broader was applied — a predicate that appears on the path makes the return spurious whether or not
it gates anything, and no other consideration makes a return spurious.

**Spurious count: `0`.** The envelope's `spurious_count` is `0`, and every one of the eight returns
carries an empty `predicates_on_path` list, which is what that count is over.

### 3.1 The predicate set, derived at execution time

The set was derived from the graph during the run rather than hardcoded, and the envelope's
`diagnostics.derived_predicates` records how, so the set can be re-derived. The name selector it
applied, as an anchored full match over the methods of the resolved type declaration:

```
^(check.*Permissions|acls.*|isAuthenticationEnabled)$
```

| Step | Value |
|---|---|
| Type-declaration selector | `org\.apache\.spark\.SecurityManager` |
| Type declarations resolved | 1 — `org.apache.spark.SecurityManager` |
| Member names on that type declaration | 19, as listed in the envelope's `member_names_on_type_declaration` |
| Methods considered | 126 |
| Match mode | anchored full match |
| Names the selector matched | 7 — `aclsEnabled`, `aclsOn`, `aclsOn_$eq`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled` |
| Exclusion `scala_setter_suffix` | applied — removed `aclsOn_$eq` |
| Exclusion `field_member_name_collision` | applied — removed `aclsOn` |
| Exclusion `field_accessor_setter_evidence` | not applied — removed nothing |
| Predicates resolved | 5 |

The five resolved predicates, exactly as the envelope carries them:

1. `org.apache.spark.SecurityManager.aclsEnabled:boolean()`
2. `org.apache.spark.SecurityManager.checkAdminPermissions:boolean(java.lang.String)`
3. `org.apache.spark.SecurityManager.checkModifyPermissions:boolean(java.lang.String)`
4. `org.apache.spark.SecurityManager.checkUIViewPermissions:boolean(java.lang.String)`
5. `org.apache.spark.SecurityManager.isAuthenticationEnabled:boolean()`

Because the match mode is an anchored full match, a method of that type declaration whose name the
selector does not match in full is not in the set. The envelope's
`member_names_on_type_declaration`, `methods_considered` and `pattern_matched` fields are the record
of what was in view and what matched, so the set above can be re-derived without re-running the
query.

### 3.2 What this determination is, and what it is not

**This determination is a property of the query, not of Spark.** It records whether the query
matched what it was asked to match — whether a derived predicate lies on a path the query itself
emitted — and it says nothing about the code the graph was built over.

**This count is not reconciled against any other query's count.** Two formulations of the same
class may legitimately return different spurious counts, and each is recorded as found. Nothing in
this file compares this count with query 02's or query 03's, and no equality between them is
asserted or expected.

---

## 4. Diagnostics

The expressive limits the query recorded, transcribed from the envelope's `diagnostics`.

### 4.1 How the graph was reached

| Field | Value |
|---|---|
| `load_mode` | `imported_persisted_cpg` |
| `cpg_source` | `harness/cpg/spark.cpg` |
| `cpg_project_name` | `spark.cpg` |
| `cpg_method_count` | 445,567 |
| `workspace` | `queries/joern/.workspace` |

The workspace path is Joern scratch. Nothing under `queries/joern/.workspace/` is a deliverable,
nothing there is cited or promoted into any report, and nothing there was cleaned up.

### 4.2 Anchors the query resolved

| Anchor | Label | `kind` | Resolved |
|---|---|---|---|
| Handler | `receive` | `user_named` | 8 |
| Handler | `receiveAndReply` | `user_named` | 6 |
| Sink | `createDriver` | `user_named` | 1 |
| Sink | `DriverRunner` | `user_named` | 1 |
| Sink | `process_launch` | `user_named` | 1 |
| Sink | `ExecutorRunner` | `additional` | 1 |

Each anchor's selector, as the envelope records it:

- `receive` — method name is exactly `receive` and the enclosing type full name matches `org\.apache\.spark\.deploy\..*`
- `receiveAndReply` — method name is exactly `receiveAndReply` and the enclosing type full name matches `org\.apache\.spark\.deploy\..*`
- `createDriver` — method name matches `(.*\$\$)?createDriver` (which admits the Scala-mangled form of a private method) and the enclosing type full name matches `org\.apache\.spark\.deploy\..*`
- `DriverRunner` — method full name matches `org\.apache\.spark\.deploy\.worker\.DriverRunner\.<init>.*` (the construction of a `DriverRunner`)
- `process_launch` — method full name matches `(java\.lang\.ProcessBuilder\.start|java\.lang\.Runtime\.exec).*`
- `ExecutorRunner` — method full name matches `org\.apache\.spark\.deploy\.worker\.ExecutorRunner\.<init>.*` (the construction of an `ExecutorRunner`) — carried in addition to the three above and never in place of one

### 4.3 Bridges the traversal needed

Two structural boundaries the call graph does not carry as a direct callee edge. Each was recorded
with whether it fired, whether it produced connections, and whether an emitted path depended on it.

| Bridge | Rule, as the envelope records it | `fired` | `succeeded` | Matched types | Distinct connections | Needed by an emitted path |
|---|---|---|---|---|---|---|
| `thread_boundary` | from a frontier method to the `run` of an anonymous type it allocates, whose full name is the enclosing type followed by `$$anon$` and a number | `true` | `true` | 20 | 14 | `true` |
| `partialfunction_boundary` | from a frontier method to every method of the synthetic partial-function type it allocates, whose full name is the enclosing type followed by `$$anonfun$`, the method's own name and a number | `true` | `true` | 34 | 34 | `true` |

The connections that lie on an emitted path — one for the thread boundary, three for the
partial-function boundary:

- `org.apache.spark.deploy.worker.ExecutorRunner.start:void() ==> org.apache.spark.deploy.worker.ExecutorRunner$$anon$1.run:void()`
- `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.storage.BlockManagerStorageEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.storage.BlockManagerStorageEndpoint$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`

### 4.4 The traversal and its bounds

| Field | Value |
|---|---|
| `direction` | forward over callee edges, one traversal per resolved entry point |
| `max_call_depth` | 20 |
| `bound_reached` | `true` |
| `entry_points_truncated_at_bound` | 6 |
| `entry_points_traversed` | 14 |
| `sink_methods_resolved` | 4 |
| `methods_seen_summed_over_entry_points` | 193,135 |
| `deepest_emitted_sink_depth` | 5 |

Three limits the envelope states in prose, which bound what this formulation could express:

- **`expansion_restriction`** — "the frontier expands only through methods whose full name begins
  with `org.apache.spark.`; an operator pseudo-method and a derived predicate are never expanded, so
  no emitted path has a predicate as an intermediate node. A sink or a predicate is still recognised
  wherever it is reached, including outside that prefix."
- **`path_selection`** — "one return per (entry point, sink) pair, whose path is the breadth-first
  discovery path; successors are visited in full-name order, so the path is reproducible." One path
  per pair is emitted, not every path between that pair.
- **`predicate_check_reach`** — "the emitted path nodes, plus one outgoing call step from each of
  them." That is the reach over which the on-path test in section 3 was evaluated.

`bound_reached` `true` with `entry_points_truncated_at_bound` 6 of `entry_points_traversed` 14
records that for six entry points the frontier was still non-empty when the depth bound of 20 was
reached, so those traversals stopped at the bound.

Four returns traverse a Scala trait's default-method forwarder, which the call graph links to every
implementation of the method it forwards; the envelope records the count as
`returns_whose_path_traverses_a_trait_default_method_forwarder` and lists them as emitted rather
than filtering them out:

- `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> java.lang.ProcessBuilder.start:java.lang.Process()`
- `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`
- `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> java.lang.ProcessBuilder.start:java.lang.Process()`
- `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

### 4.5 Failure capture and the stderr reference

The envelope records `failure_reason` `null` and `not_evaluable` `null`, and its `diagnostics`
carries no error entry, so no failure was captured for this invocation. The captured stderr is
referenced by path and line range only, from the envelope's `stderr_ref`:
`queries/joern/.workspace/01-callgraph-unguarded-driver-launch.stderr.log`, lines 1–8. Its contents
are not quoted, summarized or characterized here, and that capture sits under the workspace, which
is scratch and not a deliverable.

---

## 5. Revision log

One row per recorded invocation attempt, from the envelope's `revisions`, which the Phase 3 driver
appends to across invocations.

| Executed at | Source SHA-256 | `compiled` | `ran` | Return count | Spurious count |
|---|---|---|---|---|---|
| `2026-08-21T08:09:23Z` | `8a43c941b79af939e21085bda3a5d44a021a2fa043d1c861d63227d9520c5b52` | `true` | `true` | 8 | 0 |

**Revision count: 1.** A revision is one recorded execution of a *distinct source text*, so the
count is the number of distinct source hashes in the table above — one — and not the number of rows.
The table has 1 row and 1 distinct hash, so the two coincide here; a re-run of an unchanged script
would add a row without raising the count. The envelope's `revision_count` is `1`, which agrees.

The log is **append-only across driver invocations**: a later invocation adds rows and never
rewrites or resets earlier ones, and this table is re-rendered from the accumulated `revisions`
each time.

---

## 6. What this file does not claim

- **No characterization of any finding.** Nothing above is called a real bug, a false positive, a
  duplicate of anything, important, severe or exploitable, and nothing is triaged, ranked or scored.
  The spurious determination in section 3 is the only true-or-false call made here, and it is a
  property of the query rather than of Spark.
- **No ordering and no overall answer.** This file does not rank query 01 against query 02 or query
  03 and states no conclusion about what the probe as a whole found. Which result leads, and what
  the probe concludes, belong exclusively to `oss-scan-results/joern-probe.md`.
- **No comparison against anything.** No other tool's output and no baseline is referenced,
  compared with or implied; this run compares nothing.
- **Nothing about Spark's security posture.** The returns, the paths and the predicate set describe
  what the query expressed over a graph that was read, not built. They are not a statement about the
  code the graph was built over.
- **No effect on the published dataset.** A Phase 3 failure would not invalidate it:
  `oss-scan-results/findings.json`, `oss-scan-results/findings.csv` and
  `oss-scan-results/severity-map.md` were validated and published before the Phase 3 driver started,
  and nothing in this file changes them.
