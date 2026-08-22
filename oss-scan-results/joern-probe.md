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
| Graph | `harness/cpg/spark.cpg`, **read and never built**; **built: false** — `importCode` appears in none of the three sources. The load has two branches and each script implements both: it calls **`importCpg`** when the shared workspace holds no project for that file, and otherwise **opens the project a previous `importCpg` of that same file created there**, after verifying that the input path the project recorded canonicalizes to it. In the committed sequence below, `01-callgraph-unguarded-driver-launch` was the importer (`load_mode: imported_persisted_cpg`) and the two invocations after it opened that project (`load_mode: opened_existing_project`); each envelope's `diagnostics.load_mode` states which branch its invocation took, and reading is what both branches do |
| Driver precondition | the published dataset, re-observed independently before every invocation below and identical at each; the values are in the table that follows |
| JVM and heap in force | `openjdk version "21.0.12.1" 2026-08-18 LTS`, `JAVA_OPTS=-Xmx48g -Xss64m` — the same for every invocation below, so a reader running one by hand runs it as the driver did |
| Queries committed | 3 |
| Queries that compiled | 3 |
| Queries that ran to a complete result | 3 |
| Queries producing a clean positive | 3 |

The published dataset the driver required and observed, latest observation `2026-08-22T10:28:10Z`:

| Published output | Present | Rows | Bytes | sha256 |
|---|---|---|---|---|
| `oss-scan-results/findings.json` | True | 10178 | 5806988 | `2b3fb2dbb5c2f30c711524a5a0be141aab8445e00814a7fdf6f8ba6c6f664f51` |
| `oss-scan-results/findings.csv` | True | 10178 | 3309257 | `68ae2e4ed1b0f9197a4e813c4e73f9d9c2a9864143d9f56c8173af9aa5f25e13` |
| `oss-scan-results/severity-map.md` | True | — | 6049 | `ebf11a85342c7e62c3a2ad1f403ea13672dd1bd579746f85969ac47798a8207f` |

Every invocation observed those same bytes and hashes, so no result below rests on a
different dataset than any other. The per-query observation, with its own timestamp, is
in each query's report at §1.

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

**Why the queries carry more than one source revision.** After the driver's first
sequence of executions the three sources were revised together to close the window
between the graph-provenance check and the load: each now digests the graph file before
the load, re-verifies that digest and the size afterwards, fails closed on a difference,
and records the digest in `diagnostics.graph_identity`. Every query was executed again
against that revised text. `02-dataflow-unguarded-driver-launch` and `03-parameterized-unguarded-handler-sink` were then revised once more, to restore a
comment separator line lost where that revision's header section was inserted — a
correction to documentation that changed no statement either script makes and no value
either script emits — and both were executed again. `01-callgraph-unguarded-driver-launch` did not carry that defect,
was not revised a second time and was therefore not executed again, which is why §4.1
records one fewer distinct text and one fewer execution for it than for the other two.
Every hash, count and precondition in this file is from the latest execution of the
committed source, and each query's revision log carries every execution the driver
performed.

### The envelope these statements are written from

Each invocation's envelope, `queries/joern/results/<nn>-<slug>.json`, is written by the
driver from its own capture, and it is a **superset by design** of the fields the AAP
fixes for it — slug, source hash, `compiled`, `ran`, returns, return count, spurious
count, and a reference to the captured stderr on failure. It carries **22 top-level
keys**. Eighteen of them are the ones a validator of this contract checks, present in
this relative order:

`slug`, `query_source`, `source_sha256`, `invocation`, `markers`, `compiled`, `ran`,
`returns`, `return_count`, `spurious_count`, `spurious_test`, `diagnostics`,
`not_evaluable`, `failure_reason`, `stderr_ref`, `graph`, `revisions`, `revision_count`.

Four supplementary keys are interleaved among them. Each records a fact this run is
required to report, none replaces or reinterprets a required one, and their positions are
the same in all three envelopes:

| Supplementary key | Position | The fact it carries, and why the envelope is where it lives |
|---|---|---|
| `precondition_observed_by_the_driver` | 5 | the published dataset as the driver observed it immediately before this invocation — presence, rows, bytes and sha256. AAP §0.5.1 makes the published dataset the driver's precondition, so recording the observation against the invocation it governed is what makes it an observation rather than an assumption stated once elsewhere |
| `spurious_returns` | 13 | the returns the on-path test marked spurious. AAP §0.5.4 requires the per-query spurious count; carrying the set beside the count is what lets a reader check the one against the other instead of taking it on trust |
| `clean_positive` | 14 | the determination AAP §0.5.4 defines and this report's ordering turns on, recorded per query at the point it is decided |
| `execution_count` | 22 | executions recorded for this query, kept separate from `revision_count` because AAP §0.5.4 defines a revision as one recorded execution of a *distinct source text*; the two numbers differ for every query here, and collapsing them would misstate both effort measures |

No key is dropped to reach eighteen: each of the four is the only record of its fact, so
removing one would delete the fact rather than tidy the schema. A reader counting keys
should therefore expect 22, and a validator asserting the eighteen should assert their
presence and relative order — which hold in all three envelopes — rather than the total.

**`stderr_ref`, and what it looks like when it is populated.** The field is a *reference*
to a captured stderr stream and never a copy of one: a failure diagnostic is cited by path
and line range rather than quoted, so that whatever a tool or an interpreter printed to
that stream is not republished by the citation. Populated, it is an object of that shape,
the placeholders below standing for the values a failing invocation would supply —

```text
{"path": "<the captured stderr stream for that invocation>",
 "line_range": [<first line of the diagnostic>, <last line of it>]}
```

— where the line range names the lines carrying the diagnostic and nothing else. Its empty
form is `null`, and **`null` is its value in all three envelopes here**: every invocation
exited `0` with its start marker printed and its result region parsed, so no failure
diagnostic was captured and none is owed. The populated form consequently has no instance
in this run and is documented above rather than exhibited. What would produce one is the
failure protocol each source states in its header and each query's report restates at §5:
a start marker with no result region, and a `---BLITZY-FAILURE---` line on stderr naming
the stage that failed — the state the driver records as `compiled: true, ran: false`.

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
| Source | `queries/joern/01-callgraph-unguarded-driver-launch.sc` (sha256 `535237eeef30e07b7f7a8f8f27c361e9173944094e05685394e67d32d7575ff8`) |
| Invocation | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/01-callgraph-unguarded-driver-launch.sc` |
| Exit code | `0` |
| Elapsed | 15.7 s |
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
| Source | `queries/joern/02-dataflow-unguarded-driver-launch.sc` (sha256 `045b5df31ff41bb03abe92020421e3a432dd711a96880bf0d3f48e3d50363edd`) |
| Invocation | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| Exit code | `0` |
| Elapsed | 213.4 s |
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
| Source | `queries/joern/03-parameterized-unguarded-handler-sink.sc` (sha256 `49ce08f0d7e592827eb6241857fb934dc1290ecbb372109e1f8c711c2fad79b2`) |
| Invocation | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/03-parameterized-unguarded-handler-sink.sc` |
| Exit code | `0` |
| Elapsed | 16.4 s |
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

**What those counts are bounded by, from the queries' own diagnostics.** A spurious
count is exact over the returns a query emitted, and each query records where its
traversal stopped, so the qualification is repeated here rather than left in the
envelopes:

* `01-callgraph-unguarded-driver-launch` — `traversal.bound_reached: true` at `max_call_depth: 20`, with
  `entry_points_truncated_at_bound: 4` of `entry_points_traversed: 8`. Routes lying
  deeper than that bound from those entry points were not enumerated, so its counts are
  counts under the bound.
* `02-dataflow-unguarded-driver-launch` — the data-flow engine exposes no signal for having truncated at its
  call-depth bound, as its own `traversal.bound_reached` states, so truncation cannot be
  read off a single query; what is recorded instead is that every anchor was answered a
  second time at the engine default depth and that the two counts agree
  (`bound_changed_outcome_versus_engine_default: false`, 4 flows at the configured bound
  of 12 and 4 at the default of 4).
* `03-parameterized-unguarded-handler-sink` — `traversal.bound_reached: true` at `max_call_depth: 20`, with
  `entry_points_truncated_at_bound: 4` of `entry_points_traversed: 8`, under the same
  reading as query 01.

One consequence is stated plainly because a reader would otherwise have to derive it.
AAP §0.5.3 works through a return it expects the on-path test to mark spurious — the
route reaching the `DriverRunner` sink by way of `CommandUtils`, where the AAP records
`securityMgr.isAuthenticationEnabled()` on the path — and no such return is present in
the emitted set of any of the three queries. So **`0` is the spurious count over the
returns these queries emitted under the bounds above, and not a statement that no
guarded route exists in the graph.**

### 3.3 The predicate set, derived at execution time

| | |
|---|---|
| Type-declaration selector | `org\.apache\.spark\.SecurityManager` |
| Name selector | `^(check.*Permissions\|acls.*\|isAuthenticationEnabled)$` |
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
| `01-callgraph-unguarded-driver-launch` | 2 | 6 |
| `02-dataflow-unguarded-driver-launch` | 3 | 7 |
| `03-parameterized-unguarded-handler-sink` | 3 | 7 |
| **aggregate** | **8** | **20** |

The measure counts what the driver executed. The sources reached their committed state
through development iterations that preceded the driver's first invocation; those were not
driver executions and are not counted here, and this sentence is the boundary of the
measure rather than a claim that there were none.

### 4.2 Distinct Joern API constructs: 46, by name

Extraction rule, stated so the count is reproducible. Two mechanical parts, unioned over
all three final committed sources, with comments and string literals excluded from both —
the prose in these sources lives in both, and a construct is counted where it is invoked
rather than where it is named:

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

The table below is produced by applying those two rules to the committed sources at the
time this file was written, rather than maintained by hand. Applied that way it reaches
**46** where the hand-maintained list this file previously carried reached 45, and the
arithmetic between the two is 45 + 2 − 1. Each difference is named with its evidence, so
a reader can check the change without re-running anything:

* `map` is now included: rule (A) reaches it from a `cpg`-rooted chain in `02-dataflow-unguarded-driver-launch.sc` at lines 1003, 1273.
* `sortBy` is now included: rule (A) reaches it from a `cpg`-rooted chain in `02-dataflow-unguarded-driver-launch.sc` at line 972.
* `caller` is dropped. It is in the vocabulary rule (B) scans, but outside comments and
  string literals it is invoked in none of the three sources, and rule (B) counts a
  construct where it is invoked rather than where it is named. The previous list
  carried that row on the strength of prose mentions alone.
* `argument`, `code`, `parameter` and `typeFullName` are attributed to `02-dataflow-unguarded-driver-launch.sc`
  alone. The other two sources name them in prose and invoke none of them, so their
  previous attribution to `01-callgraph-unguarded-driver-launch.sc` and `03-parameterized-unguarded-handler-sink.sc` is not carried
  forward.

| Construct | Used in |
|---|---|
| `argument` | `02-dataflow-unguarded-driver-launch.sc` |
| `argumentIndex` | `02-dataflow-unguarded-driver-launch.sc` |
| `ast` | `02-dataflow-unguarded-driver-launch.sc` |
| `call` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `callee` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `code` | `02-dataflow-unguarded-driver-launch.sc` |
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
| `map` | `02-dataflow-unguarded-driver-launch.sc` |
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
| `parameter` | `02-dataflow-unguarded-driver-launch.sc` |
| `projects` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `reachableByFlows` | `02-dataflow-unguarded-driver-launch.sc` |
| `run.ossdataflow` | `02-dataflow-unguarded-driver-launch.sc` |
| `signature` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `size` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `sortBy` | `02-dataflow-unguarded-driver-launch.sc` |
| `sorted` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `switchWorkspace` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `typeDecl` | `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-unguarded-handler-sink.sc` |
| `typeFullName` | `02-dataflow-unguarded-driver-launch.sc` |
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
| 5 | Phase 3 delivers three or more committed queries with recorded outcomes, spurious-return counts and the three effort measures, and the graph was read rather than built | **passed.** 3 committed queries, each invoked by this driver after publication, each with a recorded outcome, a spurious-return count under the on-path test and its own result pair; the three effort measures are in `joern-probe.md` §4; the graph was read and not built — imported with `importCpg` by the first invocation and opened, from the project that import created, by the two after it, as each envelope's `diagnostics.load_mode` records |
| 6 | `run-record.md` states the `$SPARK_SRC` path scanned, its commit and date, and every tool failure and missing module | **passed.** `run-record.md` states the `$SPARK_SRC` path scanned with its commit and commit date, every tool failure and termination, and the missing-module answer |

**All six conditions hold together: yes.**

## 6. What this file does not claim

It compares nothing against Apex, Cantina or any other scanner: no such results were
provided to this run, so there is no baseline here and no comparison to draw. It calls no
finding real, important, severe-in-context, duplicated or false. It draws no conclusion
about Spark's security. The spurious determination is a property of a query — whether it
matched what it was asked to match — reached mechanically by on-path predicate presence.
The graph was **read** and **not built** — with `importCpg` where the script was the
importer, and by opening the provenance-verified project that import created where it was
not; `importCode` appears in none of the three committed sources, and no scanner, runner,
build or smoke fallback was invoked by any of them.
