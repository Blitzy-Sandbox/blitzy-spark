# Phase 3 — the Joern capability probe

One capability question: **can an open-source tool express a missing-authorization bug at
all?** The class it has to express is the one the request defines — an RPC entry point in
Spark's standalone `deploy` package reaching a privileged sink (`DriverRunner`,
`createDriver`, or a process launch) along a path that passes no authentication or ACL
predicate. Three queries attempt that whole class, each by a different formulation.

This file is written by the Phase 3 driver from `queries/joern/results/*`, after the
controller published the dataset and exited. It judges nothing about Spark: every
statement below is a property of a query, of the graph, or of the driver's own capture.

## Derivation basis

| | |
|---|---|
| Written from | `queries/joern/results/*.json`, each written by the driver from its own capture of one invocation |
| Graph | `harness/cpg/spark.cpg`, loaded by each script with **`importCpg`**; **built: false** — `importCode` appears in none of the three sources |
| Driver precondition | the published dataset, observed at `2026-08-22T06:48:45Z`: `findings.json` 10178 rows, `findings.csv` 10178 rows, `severity-map.md` present |
| Queries committed | 3 |
| Queries that compiled | 3 |
| Queries that ran to a complete result | 3 |
| Queries producing a clean positive | 3 |

**A clean positive** is a query that compiled, ran, and returned at least one result that
is **not** spurious under the on-path test — that is, at least one handler-to-sink route
with no authentication or ACL predicate anywhere on it. The ordering of this report
follows from that definition: a clean positive leads if any query produced one, and the
negative results lead only if none did.

**On the evidence this replaces.** An earlier attempt at this milestone produced query
sources and result files outside any driver sequence, while its own controller record
stated that Phase 2 was never entered and the Phase 3 driver never launched. That
evidence is **discarded rather than merged**: the three sources were remediated and
every result file here comes from this run's driver invocations, whose hashes are in
the revision logs. The superseded hashes are therefore absent by decision, and this
is the statement of that decision. Nothing in this file describes an execution the
driver did not perform.

## 1. The leading result

### 1.1 A clean positive: `01-callgraph-unguarded-driver-launch`

`01-callgraph-unguarded-driver-launch` compiled, ran, and returned 10 result(s), of which 0 are spurious under the
on-path test — leaving **10 clean positive route(s)**.

| # | Handler | Sink | Path length | Predicates on path |
|---|---|---|---|---|
| 1 | `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 3 | none |
| 2 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 8 | none |
| 3 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 10 | none |
| 4 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 7 | none |
| 5 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 9 | none |
| 6 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 10 | none |
| 7 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 9 | none |
| 8 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 6 | none |
| 9 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)` | 3 | none |
| 10 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)` | 3 | none |

So the answer to the capability question is **yes**: an open-source tool can express
this class of reachability over this graph, and `01-callgraph-unguarded-driver-launch` is a formulation that does.

### 1.2 Does it generalize? A plain statement

`01-callgraph-unguarded-driver-launch` is written against the class rather than against one handler: its entry anchors
are derived from the graph by name, signature, package and body structure, and its
sinks by full-name selector. Applying it to another handler/sink pair therefore means
changing a selector, not rewriting the traversal — which is exactly what
`03-parameterized-unguarded-handler-sink` demonstrates by taking those selectors as
parameters. What it does **not** do is generalize across formulations: a class
expressible in the call-graph view need not be expressible in the dataflow view, which
is why all three queries are reported rather than the best one.

### 1.3 Why the leading result does not settle the question on its own

A clean positive says the tool *can express* the class. It says nothing about whether
what it matched is a bug: the on-path test is a property of the query, and this run
characterizes no finding. The counts also depend on bytecode coverage — a module with
no jar in the graph yields silence indistinguishable from an absence of findings, which
is why the gate asserted per-module coverage injectively before any of this ran.

## 2. The queries, one by one

### 2.1 `01-callgraph-unguarded-driver-launch`

*What it attempts:* the full class over the call graph: from a concrete standalone `deploy` RPC handler, traverse callees to `createDriver`, a `DriverRunner` construction or the process launch, and emit every distinct ordered route.

| | |
|---|---|
| Source | `queries/joern/01-callgraph-unguarded-driver-launch.sc` (sha256 `d387a7f7ba70804f1b0d93136c6997e6dc4e5ec6efaa70c33b55cdd652bff448`) |
| Invocation | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/01-callgraph-unguarded-driver-launch.sc` |
| Exit code | `0` |
| Elapsed | 14.2 s |
| `compiled` / `ran` | True / True |
| Returns | 10 |
| Spurious under the on-path test | 0 |
| Clean positive | True |
| Result files | `queries/joern/results/01-callgraph-unguarded-driver-launch.json`, `queries/joern/results/01-callgraph-unguarded-driver-launch.md` |

Entry anchors it accepted (8), derived by rule and not hardcoded:

* `org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()`

And 6 `receive`/`receiveAndReply` method(s) in the `deploy` package it
excluded, each with the rule that excluded it:

* `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — declares_no_partial_function_body_class_of_its_own
* `org.apache.spark.deploy.DriverRedirectConsolePlugin.receive:java.lang.Object(java.lang.Object)` — signature_is_not_an_rpc_endpoint_handler_signature
* `org.apache.spark.deploy.DriverTimeoutDriverPlugin.receive:java.lang.Object(java.lang.Object)` — signature_is_not_an_rpc_endpoint_handler_signature
* `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — declares_no_partial_function_body_class_of_its_own
* `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receive:scala.PartialFunction()` — enclosing_type_is_outside_standalone_deploy
* `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — enclosing_type_is_outside_standalone_deploy

### 2.2 `02-dataflow-unguarded-driver-launch`

*What it attempts:* the same class over data flow, using `reachableByFlows` from the driver-submission value in the handler to the command- or jar-bearing argument of the sink.

| | |
|---|---|
| Source | `queries/joern/02-dataflow-unguarded-driver-launch.sc` (sha256 `831b37459372921dabcaca89d19d4435a85814030e12986fd0ed2d6e41416b8e`) |
| Invocation | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| Exit code | `0` |
| Elapsed | 212.6 s |
| `compiled` / `ran` | True / True |
| Returns | 1 |
| Spurious under the on-path test | 0 |
| Clean positive | True |
| Result files | `queries/joern/results/02-dataflow-unguarded-driver-launch.json`, `queries/joern/results/02-dataflow-unguarded-driver-launch.md` |

Entry anchors it accepted (8), derived by rule and not hardcoded:

* `org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()`

And 6 `receive`/`receiveAndReply` method(s) in the `deploy` package it
excluded, each with the rule that excluded it:

* `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — declares_no_partial_function_body_class_of_its_own
* `org.apache.spark.deploy.DriverRedirectConsolePlugin.receive:java.lang.Object(java.lang.Object)` — signature_is_not_an_rpc_endpoint_handler_signature
* `org.apache.spark.deploy.DriverTimeoutDriverPlugin.receive:java.lang.Object(java.lang.Object)` — signature_is_not_an_rpc_endpoint_handler_signature
* `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — declares_no_partial_function_body_class_of_its_own
* `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receive:scala.PartialFunction()` — enclosing_type_is_outside_standalone_deploy
* `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — enclosing_type_is_outside_standalone_deploy

### 2.3 `03-parameterized-unguarded-handler-sink`

*What it attempts:* the same class in parameterized form, taking handler-method and sink-method patterns as `--param` inputs, so that whether the formulation generalizes is answered by its parameter list rather than by assertion.

| | |
|---|---|
| Source | `queries/joern/03-parameterized-unguarded-handler-sink.sc` (sha256 `489074c782c5deb6d3443a05834d09335c3bbda0142b547a273dc87dc37f934c`) |
| Invocation | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/03-parameterized-unguarded-handler-sink.sc` |
| Exit code | `0` |
| Elapsed | 15.2 s |
| `compiled` / `ran` | True / True |
| Returns | 10 |
| Spurious under the on-path test | 0 |
| Clean positive | True |
| Result files | `queries/joern/results/03-parameterized-unguarded-handler-sink.json`, `queries/joern/results/03-parameterized-unguarded-handler-sink.md` |

Entry anchors it accepted (8), derived by rule and not hardcoded:

* `org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
* `org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
* `org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()`

And 2 `receive`/`receiveAndReply` method(s) in the `deploy` package it
excluded, each with the rule that excluded it:

* `org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — declares_no_partial_function_body_class_of_its_own
* `org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` — declares_no_partial_function_body_class_of_its_own


## 3. Per-query spurious counts, the on-path test and the derived predicate set

### 3.1 The test as applied

A return is spurious when an authentication or ACL predicate from the set the query derived at execution time lies on the path from the handler to the sink, and for no other reason. The test is applied mechanically to `predicates_on_path`: non-empty means spurious, empty means not spurious. No broader judgement is applied, and the determination is a property of the query rather than of Spark.

It is the request's own definition, applied with no broader reading: a stricter test —
requiring the sink to be control-dependent on the predicate — would add a condition the
definition does not state and would reclassify returns the stated test marks spurious.

### 3.2 The counts

| Query | Returns | Spurious | Not spurious | Clean positive |
|---|---|---|---|---|
| `01-callgraph-unguarded-driver-launch` | 10 | 0 | 10 | True |
| `02-dataflow-unguarded-driver-launch` | 1 | 0 | 1 | True |
| `03-parameterized-unguarded-handler-sink` | 10 | 0 | 10 | True |
| **total** | **21** | **0** | **21** | — |

### 3.3 The predicate set, derived at execution time

| | |
|---|---|
| Type-declaration selector | `org\.apache\.spark\.SecurityManager` |
| Name selector | `^(check.*Permissions|acls.*|isAuthenticationEnabled)$` |
| Match mode | anchored full match |
| Methods considered on the type | 126 |
| Resolved predicates | `org.apache.spark.SecurityManager.aclsEnabled:boolean()`, `org.apache.spark.SecurityManager.checkAdminPermissions:boolean(java.lang.String)`, `org.apache.spark.SecurityManager.checkModifyPermissions:boolean(java.lang.String)`, `org.apache.spark.SecurityManager.checkUIViewPermissions:boolean(java.lang.String)`, `org.apache.spark.SecurityManager.isAuthenticationEnabled:boolean()` |
| Count | 5 |

The selector reaches `check.*Permissions`, `acls.*` and `isAuthenticationEnabled` and
therefore **excludes `isEncryptionEnabled` and `isSslRpcEnabled` by construction**:
both govern transport encryption rather than caller identity, so neither is an
authentication or ACL predicate in the sense the request's example uses. The set is
derived from the graph at execution time rather than hardcoded, so any count above can
be re-derived.

## 4. The three effort measures

### 4.1 Aggregate revision count

A revision is one recorded execution of a distinct source text for a query: the driver
hashes each script before running it and appends the hash to that query's revision log,
so the count is the number of distinct hashes and its evidence sits in the query's own
`.md`.

| Query | Distinct source texts executed by the driver | Executions recorded |
|---|---|---|
| `01-callgraph-unguarded-driver-launch` | 1 | 5 |
| `02-dataflow-unguarded-driver-launch` | 1 | 5 |
| `03-parameterized-unguarded-handler-sink` | 1 | 5 |
| **aggregate** | **3** | **15** |

The measure counts what the driver executed. The sources reached their committed state
through development iterations that preceded the driver's first invocation; those were not
driver executions and are not counted here, and this sentence is the boundary of the
measure rather than a claim that there were none.

### 4.2 Distinct Joern API constructs: 45, by name

Extraction rule, stated so the count is reproducible. Two mechanical parts, unioned over
all three final committed sources, with comment lines excluded from both:

* **(A)** every member name appearing in a traversal chain rooted at `cpg`, with
  parenthesised arguments removed first, so a name inside an argument is not miscounted
  as a step.
* **(B)** every name in a stated Joern vocabulary that the sources invoke on a node, on a
  traversal already in hand, or on the console — the constructs a `cpg`-rooted scan cannot
  see, such as `sink.reachableByFlows(source)`, `method.callee`, `run.ossdataflow` and the
  `switchWorkspace` and `importCpg` each script performs. The vocabulary scanned is:
  `importCpg`, `switchWorkspace`, `open`, `workspace`, `run.ossdataflow`, `reachableByFlows`, `callee`, `caller`, `callIn`, `argument`, `argumentIndex`, `elements`, `iterator`, `methodReturn`, `typeFullName`, `signature`, `isExternal`, `member`, `parameter`, `ast`, `metaData`, `overlays`, `projects`, `inputPath`, `code`, `filename`, `lineNumber`, `fullNameExact`, `nameExact`, `methodFullName`, `methodFullNameExact`, `typeDecl`, `method`, `call`, `size`, `dedup`, `distinctBy`.

The count is therefore of constructs the author had to use, and a reader can re-derive it
by running the same two rules over the same three files.

| Construct | Used in |
|---|---|
| `argument` | `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `argumentIndex` | `02-dataflow-unguarded-driver-launch.sc` |
| `ast` | `02-dataflow-unguarded-driver-launch.sc` |
| `call` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `callee` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `caller` | `03-parameterized-unguarded-handler-sink.sc` |
| `code` | `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `cpg` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `distinct` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `distinctBy` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `elements` | `02-dataflow-unguarded-driver-launch.sc` |
| `exists` | `01-callgraph-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `filter` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `filterNot` | `02-dataflow-unguarded-driver-launch.sc` |
| `flatMap` | `01-callgraph-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `fullName` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `fullNameExact` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `importCpg` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `inputPath` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `isCall` | `02-dataflow-unguarded-driver-launch.sc` |
| `isExternal` | `03-parameterized-unguarded-handler-sink.sc` |
| `iterator` | `02-dataflow-unguarded-driver-launch.sc` |
| `l` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `member` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `metaData` | `02-dataflow-unguarded-driver-launch.sc` |
| `method` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `methodFullName` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `methodFullNameExact` | `02-dataflow-unguarded-driver-launch.sc` |
| `methodReturn` | `02-dataflow-unguarded-driver-launch.sc` |
| `name` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `nameExact` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `open` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `overlays` | `02-dataflow-unguarded-driver-launch.sc` |
| `parameter` | `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `projects` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `reachableByFlows` | `02-dataflow-unguarded-driver-launch.sc` |
| `run.ossdataflow` | `02-dataflow-unguarded-driver-launch.sc` |
| `signature` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `size` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `sorted` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `switchWorkspace` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `typeDecl` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `typeFullName` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `where` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc` |
| `workspace` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |

### 4.3 Parameterizable: yes — query 03, with its parameter list

`03-parameterized-unguarded-handler-sink` takes its handler and sink selectors as `--param` inputs, so the answer is given
by the parameter list existing rather than by assertion:

| Parameter | Type | Generalizes over | Provenance in this run |
|---|---|---|---|
| `handlerPattern` | string | the identity of the entry point: any method the graph holds, selected by an anchored full-match regex over its full name, whatever its name, its enclosing type or its package | declared_default_not_supplied |
| `sinkPattern` | string | the identity of the privileged sink: any method the graph holds, selected the same way, including a constructor and a method with no body of its own | declared_default_not_supplied |
| `maxDepth` | string at the invocation boundary, parsed by this script as an integer within MIN_MAX_DEPTH..MAX_MAX_DEPTH | how far the traversal may follow call edges from an entry point, so that a handler and a sink further apart than the default pair can still be related | declared_default_not_supplied |

The committed evidence run supplies **no** parameter, so the declared defaults are in
force and every value in the envelope is a default authored in the file rather than a
caller-supplied one. Where a caller does supply a value, the script records the
parameter name with a length and a SHA-256 digest and withholds the value itself.

Its redaction policy, as the script enforces it:

* Policy: a value the caller supplied is never echoed. What is emitted for one is its parameter name, its length in characters and a SHA-256 digest of its UTF-8 bytes — enough to prove that a claimed invocation is the one that ran, and not enough to publish anything the caller did not intend to. A literal is emitted only where the value in force is the declared default authored in this file, which carries nothing of the caller's
* Digest algorithm: `SHA-256`
* Values withheld in this run: none
* Stated exception: the numeric bound in force is emitted as an integer — `parameters.declared[].value_parsed` and `traversal.max_call_depth` — even when the caller supplied it, because every count in this result is a count under that bound and a result that hid it could not be interpreted. The caller's raw TEXT for it is still withheld and referenced by length and digest, and no pattern value is emitted under any circumstances. This is stated rather than left to be noticed, so that a report written against this policy claims exactly what the code enforces
* Outside the script's control: the Joern script runner prints `executing <script> with params=Map(...)` to stderr before this script's first statement runs, so a captured stderr log carries the invocation as the RUNNER echoed it. That line is the interpreter's and cannot be suppressed from inside a script. Everything this script emits — the result object, every diagnostics field and every failure message — withholds a caller-supplied value and references it by length and digest, so a report that must not disclose one has to withhold or filter that captured line rather than rely on this script for it

## 5. Where the run reached, condition by condition

The controller finalizes `run-record.md` and `tool-status.md` before this driver is
launched, so those two files state conditions 1-4 and 6 and delegate condition 5 to this
driver. This is the one place all six appear together, after the fact.

| # | Condition | Verdict |
|---|---|---|
| 1 | Every tool ran once with its baked configuration, to completion or to a termination outside this run's control, each with a log carrying stdout, stderr, elapsed time and either an exit code or `exit_status: timeout`; every tool that wrote output has a raw artifact, and a tool that wrote none is recorded with parse status `absent`, its exit code and its stderr, contributing zero rows | **passed.** all 9 runners invoked once, serially, with no arguments; 9 of 9 carry stdout, stderr and a meta.json with elapsed time and an exit code; 1 wrote no artifact and is recorded with parse status `absent`, its exit code and its stderr |
| 2 | `findings.json` and `findings.csv` contain every row from every artifact, each carrying `tool`, `scanner_class`, `severity_norm` and `in_scope`, with no row dropped; row validation passes; and the per-tool reconciliation assertions pass | **passed.** `findings.json` and `findings.csv` published from one validated row list; row validation passed over 10178 rows; every evaluable per-tool reconciliation assertion passed; the CSV and JSON row counts are equal (10178 == 10178) |
| 3 | `severity-map.md` carries a row for all nine tools, including any that produced no finding | **passed.** `severity-map.md` carries one row for 9 of the nine tools, including those that produced no finding |
| 4 | `tool-status.md` lists all nine, including any that failed or timed out, each with its parse status, its records parsed and rejected, and its row-validation result | **passed.** this file carries one block for each of the nine, each with its execution state, exit status, parse status, records parsed and rejected, both reconciliation assertions and the row-validation result |
| 5 | Phase 3 delivers three or more committed queries with recorded outcomes, spurious-return counts and the three effort measures, and the graph was read rather than built | **passed.** 3 committed queries, each invoked by this driver after publication, each with a recorded outcome, a spurious-return count under the on-path test and its own result pair; the three effort measures are in `joern-probe.md` §4; the graph was read with `importCpg` and not built |
| 6 | `run-record.md` states the `$SPARK_SRC` path scanned, its commit and date, and every tool failure and missing module | **passed.** `run-record.md` states the `$SPARK_SRC` path scanned with its commit and commit date, every tool failure and termination, and the missing-module answer |

**All six conditions hold together: yes.**

## 6. What this file does not claim

It compares nothing against Apex, Cantina or any other scanner: no such results were
provided to this run, so there is no baseline here and no comparison to draw. It calls no
finding real, important, severe-in-context, duplicated or false. It draws no conclusion
about Spark's security. The spurious determination is a property of a query — whether it
matched what it was asked to match — reached mechanically by on-path predicate presence.
The graph was **read** with `importCpg` and **not built**; `importCode` appears in none of
the three committed sources, and no scanner, runner, build or smoke fallback was invoked
by any of them.
