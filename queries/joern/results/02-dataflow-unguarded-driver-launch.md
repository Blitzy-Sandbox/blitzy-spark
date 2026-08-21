# `02-dataflow-unguarded-driver-launch` — query outcome

Query 02 of the Phase 3 capability probe, and the **data-flow formulation** of the one reachability
class the probe attempts: an RPC entry point named `receive` or `receiveAndReply`, enclosed in a
type whose full name lies under `org.apache.spark.deploy.`, whose driver-submission message reaches
a privileged sink's command- or jar-bearing argument — `createDriver`, a `DriverRunner`
construction, or a process launch — along a flow on which no derived authentication or ACL predicate
appears. Query 01 attempts that same whole class over the call graph; this query attempts the same
whole class over data flow, engaging the open-source data-flow layer with `run.ossdataflow` and
expressing reachability as `sink.reachableByFlows(source)`.

This write-up is rendered from `queries/joern/results/02-dataflow-unguarded-driver-launch.json` and
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
| Slug | `02-dataflow-unguarded-driver-launch` |
| Source | `queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| Source SHA-256 (most recent attempt) | `b6dc40f642b9b111840a32ac55b6452f73f438a89eeb2aa4ed0320f6555dad0f` |
| Invocation, argv as run | `joern --script queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| `--param` values | **none** — `invocation.params` is empty, and the script's `@main def exec()` takes no parameters, so none is required or accepted |
| Invocation started | `2026-08-21T09:25:14Z` |
| Invocation ended | `2026-08-21T09:29:37Z` |
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
| `return_count` | `2` | The `returns` array carries two entries, rendered in full below |
| `not_evaluable` | `null` | No not-evaluable condition was recorded |
| `failure_reason` | `null` | No failure was recorded |

### 2.1 Returns

One row per return, with each method full name transcribed exactly as the envelope carries it.
`Path length` is the number of nodes in that return's `path`; `Predicates on path` is that return's
`predicates_on_path` list.

| # | Handler | Sink | Path length | Predicates on path |
|---|---|---|---|---|
| 1 | `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 4 | none — the list is empty |
| 2 | `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 3 | none — the list is empty |

The two returns carry the same handler and the same sink and differ only in their `path`. They are
two entries rather than one because the envelope's `return_selection` rule emits one return per
distinct `(entry point, sink, path, predicates)` tuple, and its
`returns_removed_by_deduplication` is `0`.

### 2.2 Paths as emitted

Each return's `path`, handler first and sink last, in the envelope's order. These nodes are the
evidence for the path lengths above and are the nodes the on-path test in section 3 was applied
over.

**Return 1** — `Master.receiveAndReply` to `Master$$createDriver`, through the destructured
driver-submission field:

1. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription:org.apache.spark.deploy.DriverDescription()`
4. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

**Return 2** — `Master.receiveAndReply` to `Master$$createDriver`, without that node:

1. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

Both paths begin at the named entry point and end at the sink method, with the synthetic
partial-function body between them; the envelope's `path_composition` records that ordering as the
rule the paths were assembled under.

---

## 3. Spurious returns under the on-path test

**The test as applied,** in the envelope's own words in `spurious_test`: a return is spurious when
an authentication or ACL predicate from the set the query derived at execution time lies on the
emitted path between the handler entry and the sink; on-path presence is the entire test, and
control dependence of the sink on that predicate is **not** required. Nothing narrower and nothing
broader was applied — a predicate that appears on the path makes the return spurious whether or not
it gates anything, and no other consideration makes a return spurious.

**Spurious count: `0`.** The envelope's `spurious_count` is `0`, and both returns carry an empty
`predicates_on_path` list, which is what that count is over.

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

The reach the test was evaluated over is the envelope's `predicate_check_reach`, transcribed in
section 4.4 together with the separate and narrower `flow_filter_reach` the query applied to the
flows themselves.

### 3.2 What this determination is, and what it is not

**This determination is a property of the query, not of Spark.** It records whether the query
matched what it was asked to match — whether a derived predicate lies on a path the query itself
emitted — and it says nothing about the code the graph was built over.

**This count stands alongside query 01's and is not reconciled with it.** The class may be
expressible over the call graph and not over data flow, or the reverse, so the two formulations may
legitimately return different return counts and different spurious counts. One recorded reason a
difference here is legitimate rather than an error: the sink path continues past a thread boundary
which the envelope's `bridges.thread_boundary` records as `boundary_resolved` `true` with
`applied_by_this_query` `false`, so a derived predicate reached only beyond that boundary — on the
launch continuation through `CommandUtils` — lies on no path this formulation emitted, while a
formulation that does cross the boundary can carry it. Nothing in this file averages, adjusts,
explains away or harmonizes any difference against query 01 or query 03, and no equality between
their counts is asserted or expected.

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

The data-flow layer was engaged after that load, on the graph the load returned, and its outcome was
read back from the graph rather than assumed:

| Field | Value |
|---|---|
| `command` | `run.ossdataflow` |
| `engaged` | `true` |
| `overlays_before_engaging` | `base`, `callgraph`, `controlflow`, `dataflowOss`, `typerel` |
| `overlays_after_engaging` | `base`, `callgraph`, `controlflow`, `dataflowOss`, `typerel` |
| `reachability_step` | `sink.reachableByFlows(source)` |
| `configured_max_call_depth` | 12 |
| `engine_default_max_call_depth` | 4 |
| `time_limit_imposed_by_this_script` | `false` |

The envelope's `outcome` for that block, in its own words: "the layer ran and the overlay set read
from the graph is unchanged, so the persisted graph already carried it and nothing was added." The
two overlay lists above are the evidence for that sentence — they are identical.

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

#### Sources the flow queries were asked from

A flow needs a source node. The envelope records three rules by which one was selected, quoted here
as it states them:

- `rule_a_direct_message_parameter` — "the parameter at the message index of the entry point itself, where its type is the erased message type — used where the entry point is not a partial function"
- `rule_b_partial_function_body_parameter` — "the parameter at the message index of the body declared by the synthetic partial-function type the entry point allocates, whose full name is the entry point's enclosing type followed by `$$anonfun$`, the entry point's own name and a number — the named entry point declares no message parameter, so this is the bridge that makes a source resolvable at all"
- `rule_c_destructured_driver_submission_field` — "a call, inside such a body, to an accessor on a deploy-message type whose return type is `org.apache.spark.deploy.DriverDescription`, excluding a default-argument supplier by its `copy$default$` name prefix"

| Field | Value |
|---|---|
| `message_parameter_index` | 1 |
| `message_parameter_type` | `java.lang.Object` |
| `message_accessor_selector` | enclosing type matches `org\.apache\.spark\.deploy\.DeployMessages\$.*` and return type is `org.apache.spark.deploy.DriverDescription` |
| `total_resolved_source_nodes` | 14 |

The two message accessors that selector resolved:

- `org.apache.spark.deploy.DeployMessages$LaunchDriver.driverDesc:org.apache.spark.deploy.DriverDescription()`
- `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription:org.apache.spark.deploy.DriverDescription()`

Per entry point, from the envelope's `source_nodes.per_handler`. `Direct` is
`direct_message_parameters`, `Bridged` is `bridged_message_parameters`, `Destructured` is the count
of `destructured_field_reads`, and `Sources` is `resolved_source_nodes`.

| Entry point | `partial_function_body_rule` | Direct | Bridged | Destructured | `bridge_needed` | `bridge_succeeded` | Sources |
|---|---|---|---|---|---|---|---|
| `org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |
| `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `erased_message_parameter` | 0 | 0 | 0 | `true` | `false` | 0 |
| `org.apache.spark.deploy.DriverRedirectConsolePlugin.receive:java.lang.Object(java.lang.Object)` | `erased_message_parameter` | 1 | 0 | 0 | `false` | `false` | 1 |
| `org.apache.spark.deploy.DriverTimeoutDriverPlugin.receive:java.lang.Object(java.lang.Object)` | `erased_message_parameter` | 1 | 0 | 0 | `false` | `false` | 1 |
| `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |
| `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |
| `org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |
| `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `named_body` | 0 | 1 | 1 | `true` | `true` | 2 |
| `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `named_body` | 0 | 1 | 1 | `true` | `true` | 2 |
| `org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |
| `org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |
| `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `erased_message_parameter` | 0 | 0 | 0 | `true` | `false` | 0 |
| `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receive:scala.PartialFunction()` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |
| `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `named_body` | 0 | 1 | 0 | `true` | `true` | 1 |

The two entry points for which no source node resolved, as the envelope lists them in
`handlers_with_no_resolved_source`:

- `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
- `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`

The two destructured field reads the table counts, each recorded against the entry point it was
found in:

- `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription:org.apache.spark.deploy.DriverDescription()` — in `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
- `org.apache.spark.deploy.DeployMessages$LaunchDriver.driverDesc:org.apache.spark.deploy.DriverDescription()` — in `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`

#### Sinks the flow queries were asked at

The method each sink anchor resolved to, exactly as the envelope carries it in that anchor's
`resolved` list and again as its `argument_selection[].sink_method`:

- `createDriver` — `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`
- `DriverRunner` — `org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)`
- `process_launch` — `java.lang.ProcessBuilder.start:java.lang.Process()`
- `ExecutorRunner` — `org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)`

The argument each sink anchor was asked about, from the envelope's `argument_selection`:

| Sink anchor | `argument_selection_rule` | Selected index | Selected parameter | `call_sites` | `sink_node_count` |
|---|---|---|---|---|---|
| `createDriver` | `command_or_jar_bearing_formal_parameter` | `1` | `1:desc:org.apache.spark.deploy.DriverDescription` | 2 | 2 |
| `DriverRunner` | `command_or_jar_bearing_formal_parameter` | `5` | `5:driverDesc:org.apache.spark.deploy.DriverDescription` | 1 | 1 |
| `process_launch` | `receiver_of_the_launch` | `0` | `0:p0:ANY` | 19 | 19 |
| `ExecutorRunner` | `command_or_jar_bearing_formal_parameter` | `3` | `3:appDesc:org.apache.spark.deploy.ApplicationDescription` | 1 | 1 |

What each anchor's flow queries returned, at the configured call depth and again at the engine
default:

| Sink anchor | Sink nodes | Flows at depth 12 | Flows at depth 4 | Filtered by the flow filter | Not attributable | Returns contributed |
|---|---|---|---|---|---|---|
| `createDriver` | 2 | 2 | 2 | 0 | 0 | 2 |
| `DriverRunner` | 1 | 0 | 0 | 0 | 0 | 0 |
| `process_launch` | 19 | 0 | 0 | 0 | 0 | 0 |
| `ExecutorRunner` | 1 | 0 | 0 | 0 | 0 | 0 |

Where an anchor returned no flow, the envelope records each resolved sink node and the expression it
arrives through, so the absence is recorded evidence rather than silence. Twenty-one such nodes are
carried — one for `DriverRunner`, nineteen for `process_launch`, one for `ExecutorRunner`:

| Sink anchor | Sink node | In method | Arrives through |
|---|---|---|---|
| `DriverRunner` | `$stack298` | `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)` | `$stack298 = driverDesc.copy(x$11, x$12, x$13, x$14, x$10, x$15)` |
| `process_launch` | `$stack1` | `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()` | `$stack1 = this.processBuilder$1` |
| `process_launch` | `$stack23` | `org.apache.spark.sql.connect.SparkSession.close:void()` | `$stack23 = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.api.r.RUtils$.isRInstalled:boolean()` | `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.deploy.PythonRunner$.main:void(java.lang.String[])` | `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.deploy.RPackageUtils$.rPackageBuilder:boolean(java.io.File,java.io.PrintStream,boolean,java.lang.String)` | `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.deploy.RRunner$.main:void(java.lang.String[])` | `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.deploy.worker.ExecutorRunner.org$apache$spark$deploy$worker$ExecutorRunner$$fetchAndRunExecutor:void()` | `builder = $stack66.buildProcessBuilder(subsCommand, $stack64, exitCode, message, $stack42, arguments, x$4)` |
| `process_launch` | `builder` | `org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingUnixNative:void(java.io.File)` | `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.sql.connect.SparkSession$$anon$2.run:void()` | `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.sql.execution.BaseScriptTransformationExec.initProc:scala.Tuple4()` | `builder = builder.directory($stack18)` and `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.util.Utils$.executeCommand:java.lang.Process(scala.collection.immutable.Seq,java.io.File,scala.collection.Map,boolean)` | `builder = builder.directory(workingDir)` and `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `builder` | `org.apache.spark.util.Utils$.getHeapHistogram:java.lang.String[]()` | `builder = new java.lang.ProcessBuilder` |
| `process_launch` | `pb` | `org.apache.spark.api.python.PythonWorkerFactory.createSimpleWorker:scala.Tuple2(boolean)` | `pb = new java.lang.ProcessBuilder` |
| `process_launch` | `pb` | `org.apache.spark.api.python.PythonWorkerFactory.startDaemon:void()` | `pb = new java.lang.ProcessBuilder` |
| `process_launch` | `pb` | `org.apache.spark.api.r.BaseRRunner$.createRProcess:org.apache.spark.api.r.BufferedStreamThread(int,java.lang.String)` | `pb = new java.lang.ProcessBuilder` |
| `process_launch` | `pb` | `org.apache.spark.launcher.SparkLauncher.launch:java.lang.Process()` | `pb = this.createBuilder()` |
| `process_launch` | `pb` | `org.apache.spark.rdd.PipedRDD.compute:scala.collection.Iterator(org.apache.spark.Partition,org.apache.spark.TaskContext)` | `pb = new java.lang.ProcessBuilder` |
| `process_launch` | `pb` | `org.apache.spark.sql.connect.SparkSession$.withLocalConnectServer:java.lang.Object(scala.Function0)` | `pb = new java.lang.ProcessBuilder` |
| `process_launch` | `pb#7` | `org.apache.spark.launcher.SparkLauncher.startApplication:org.apache.spark.launcher.SparkAppHandle(org.apache.spark.launcher.SparkAppHandle$Listener[])` | `pb#7 = this.createBuilder()` |
| `ExecutorRunner` | `$stack247` | `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)` | `$stack247 = appDesc.copy(x$2, x$3, x$10, x$4, x$5, x$6, x$7, x$8, x$9)` |

### 4.3 Bridges the traversal needed

Two structural boundaries the graph does not carry as a direct data-dependence edge. Each is
recorded with whether the boundary resolved, whether this formulation applied it, whether it
produced connections, and whether an emitted path depended on it.

| Bridge | `boundary_resolved` | `applied_by_this_query` | `succeeded` | Matched types | Distinct connections | Needed by an emitted path |
|---|---|---|---|---|---|---|
| `partialfunction_boundary` | `true` | `true` | `true` | 10 | 10 | `true` |
| `thread_boundary` | `true` | `false` | `false` | 3 | 2 | `false` |

The `partialfunction_boundary` rule, as the envelope states it: "from an entry point to the body
declared by the synthetic partial-function type it allocates, whose full name is the enclosing type
followed by `$$anonfun$`, the entry point's own name and a number — the message parameter lives on
that body and not on the named entry point, so without this the source set would be empty and the
query would return nothing for a reason that has nothing to do with the code under analysis." Its
`applied_note`: "applied at source selection: it is what makes a message source resolvable, and it
is not needed for an entry point that declares a message parameter itself." The ten connections it
produced:

- `org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`

The `thread_boundary` rule, as the envelope states it: "from a method owning a resolved sink to the
deferred body of an anonymous type it allocates, whose full name is the enclosing type followed by
`$$anon$` and a number and whose body the runtime invokes." Its `applied_note`, which is the
recorded limit of this formulation: "not applied: a flow follows data dependence, and an allocation
whose body the runtime invokes is not one, so this formulation does not continue past the boundary
and anchors instead at the sinks above it, which the class names as alternatives to the deepest one.
The boundary is resolved and recorded here so the limit is evidence rather than an assertion." The
two connections it resolved:

- `org.apache.spark.deploy.worker.DriverRunner.start:void() ==> org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
- `org.apache.spark.deploy.worker.ExecutorRunner.start:void() ==> org.apache.spark.deploy.worker.ExecutorRunner$$anon$1.run:void()`

### 4.4 The traversal and its bounds

| Field | Value |
|---|---|
| `direction` | data flow, asked backward from each sink anchor's command- or jar-bearing nodes to the resolved driver-submission message sources, one query per sink anchor over the whole source set |
| `reachability_step` | `sink.reachableByFlows(source)` |
| `max_call_depth` | 12 |
| `engine_default_max_call_depth` | 4 |
| `bound_changed_outcome_versus_engine_default` | `false` |
| `flows_at_configured_bound` | 2 |
| `flows_at_engine_default_bound` | 2 |
| `flows_filtered_by_flow_filter` | 0 |
| `flows_not_attributable` | 0 |
| `deepest_emitted_flow_distinct_methods` | 2 |
| `entry_points_resolved` | 14 |
| `entry_points_with_a_source` | 12 |
| `source_nodes_queried` | 14 |
| `sink_nodes_queried_distinct` | 23 |
| `returns_emitted` | 2 |
| `returns_removed_by_deduplication` | 0 |

The envelope's `bound_reached` field is prose rather than a boolean, and it is transcribed in full
because it is what the two flow counts above mean: "the data-flow engine exposes no signal for
having truncated at its call-depth bound, so reaching the bound cannot be read off a single query.
What is measured instead is the bound's effect: every anchor was answered a second time at the
engine default depth, and the two counts are recorded per anchor and summed below. Nothing was
truncated silently — where an anchor returned no flow, its resolved sink nodes and the expression
each arrives through are recorded." Those per-anchor counts and those sink nodes are the tables in
section 4.2, and `bound_changed_outcome_versus_engine_default` is `false`.

Four further limits the envelope states in prose, which bound what this formulation could express:

- **`path_composition`** — "the named entry point first, then the enclosing methods of the flow's
  elements in flow order — which begins in the synthetic partial-function body — then the sink
  method last; an occurrence of either end in the middle is dropped so the ordering holds exactly."
- **`return_selection`** — "one return per distinct (entry point, sink, path, predicates) tuple,
  sorted, so a flow that differs from another only below the method level is emitted once."
- **`flow_filter_reach`** — "the flow's own elements: an element that is a call to a derived
  predicate, or an element sitting inside one. A flow carrying either is discarded and counted, so
  no emitted path has a predicate among its flow elements." `flows_filtered_by_flow_filter` is `0`.
- **`predicate_check_reach`** — "the emitted path nodes, plus one outgoing call step from each of
  them — wider than the filter, and over a path that carries the entry point and the sink method
  which no flow element covered." That is the reach over which the on-path test in section 3 was
  evaluated.

### 4.5 Failure capture and the stderr reference

The envelope records `failure_reason` `null` and `not_evaluable` `null`, and its `diagnostics`
carries no error entry, so no failure was captured for this invocation. The captured stderr is
referenced by path and line range only, from the envelope's `stderr_ref`:
`queries/joern/.workspace/02-dataflow-unguarded-driver-launch.stderr.log`, lines 1–10. Its contents
are not quoted, summarized or characterized here, and that capture sits under the workspace, which
is scratch and not a deliverable.

---

## 5. Revision log

One row per recorded invocation attempt, from the envelope's `revisions`, which the Phase 3 driver
appends to across invocations.

| Executed at | Source SHA-256 | `compiled` | `ran` | Return count | Spurious count |
|---|---|---|---|---|---|
| `2026-08-21T09:25:14Z` | `b6dc40f642b9b111840a32ac55b6452f73f438a89eeb2aa4ed0320f6555dad0f` | `true` | `true` | 2 | 0 |

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
- **No ordering and no overall answer.** This file does not rank query 02 against query 01 or query
  03 and states no conclusion about what the probe as a whole found. Which result leads, and what
  the probe concludes, belong exclusively to `oss-scan-results/joern-probe.md`.
- **No comparison against anything.** No other tool's output and no baseline is referenced,
  compared with or implied; this run compares nothing.
- **No claim about data-flow or taint coverage in general.** Sections 4.2 to 4.4 record what this
  one query emitted over this one graph — the anchors it resolved, the flows it returned at two call
  depths, the sink nodes that returned none, and the boundary it did not continue past. Nothing
  there is a statement about any tool's or any language's data-flow or taint capability at large.
- **Nothing about Spark's security posture.** The returns, the paths and the predicate set describe
  what the query expressed over a graph that was read, not built. They are not a statement about the
  code the graph was built over.
- **No effect on the published dataset.** A Phase 3 failure would not invalidate it:
  `oss-scan-results/findings.json`, `oss-scan-results/findings.csv` and
  `oss-scan-results/severity-map.md` were validated and published before the Phase 3 driver started,
  and nothing in this file changes them.
