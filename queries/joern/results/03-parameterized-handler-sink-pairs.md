# Joern capability probe 03-parameterized-handler-sink-pairs

Bounded **call-graph** reachability over CALL edges, **parameterized over
handler/sink pairs** and instantiated on **two** named pairs in one run, over the
code-property graph built from the pinned tree's bytecode.

This report is **observational**. It judges no finding - not real, not important,
not a false positive, not a duplicate - and makes no comparison between tools. It
contributes no row to `oss-scan-results/findings.json` and writes nothing into
`harness/artifacts/raw/`.

The slug `03-parameterized-handler-sink-pairs` is the **identifier** the plan assigns this query, and the
slugs `01-callgraph-unguarded-driver-launch` and
`02-dataflow-unguarded-driver-launch` are likewise identifiers assigned to the two
sibling queries. A slug names the question a query was written to ask. It is not a
finding, and nothing in this report should be read as an assessment of Spark, of
any Spark component or of any Spark configuration.

| | |
| --- | --- |
| Query source | `queries/joern/03-parameterized-handler-sink-pairs.sc` |
| Query source sha256 | `685f33f8b27b626778b79e1900095d28b29c90a17ed1a9dd785485285174b5f9` |
| Query source byte size | 321780 |
| Publication id | `6c485411000bfd44a0af18af73cb5367b991dd20fff509d28274260207ae0bd2` |
| Envelope | `queries/joern/results/03-parameterized-handler-sink-pairs.json` |
| Console log | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21 |
| Heap actually used | 68719476736 bytes (floor 68719476736) |
| Graph | 541309809 bytes, sha256 `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` |
| Graph identity re-verified before the load | yes, against `harness/artifacts/logs/cpg-frontend.log` |
| Graph methods / typeDecls / files | 1396899 / 119721 / 45037 |
| Compile status | compiled |
| Run status | completed |
| Pairs declared / invoked | 2 / 2 |
| Pair iteration order | `pair-one`, `pair-two` |
| Records returned | 6 (6 boundary measurement(s) plus per-pair route records) |
| Parameterizability | **passed** |
| Duplicate formulation | **partial_duplicate** |

## Which source wrote this report

This report was written by `queries/joern/03-parameterized-handler-sink-pairs.sc`, whose bytes digest to
`685f33f8b27b626778b79e1900095d28b29c90a17ed1a9dd785485285174b5f9` (321780 bytes). The script read its own file at run time
and digested it, so the digest above is a measurement of the writer rather than a
label attached to it. The same digest appears in the envelope under
`source_integrity.query_source_sha256` and in the console log, and all three
members of this publication carry the identifier `6c485411000bfd44a0af18af73cb5367b991dd20fff509d28274260207ae0bd2`.

That is what makes the relationship between a source and its results checkable
rather than assumed: digest the `.sc` file and compare. A result whose digest does
not match the source beside it was not written by that source, and no amount of
agreement in the prose changes that. The three members are published together -
each staged, fsynced and only then moved onto its final name - so a reader never
sees one member from this generation beside another from a previous one.

## The result, per pair

**Distinct routes are reported per pair and are never summed.** There is no total
distinct-route figure anywhere in this report, in the envelope or in the console
log: adding one pair's routes to the other's would describe a question neither
pair asks, and adding either to query 01's or query 02's returns would do the same
across queries.

| pair | handler | entry points (discovered / traversed / truncated) | distinct routes | spurious | boundaries | bound reached |
| --- | --- | --- | --- | --- | --- | --- |
| `pair-one` | `org.apache.spark.deploy.master.Master.receiveAndReply` | 2 / 2 / 0 | 0 | 0 | 4 | true |
| `pair-two` | `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit` | 1 / 1 / 0 | 0 | 0 | 5 | true |

### Pair `pair-one` - the standalone Master's driver-submission handler to the privileged process launch on the DriverRunner surface

- **handler**: `org.apache.spark.deploy.master.Master.receiveAndReply`, at `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409` at the pin
- **sink**: `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:240` at the pin, resolved to 2 call site(s) on the sink host surface out of 1234 call(s) named `start` scanned (scan truncated: false)
  - `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)` calls `org.apache.spark.deploy.worker.ProcessBuilderLike.start:java.lang.Process()` at graph line 240 (dispatch `DYNAMIC_DISPATCH`)
  - `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()` calls `java.lang.ProcessBuilder.start:java.lang.Process()` at graph line 276 (dispatch `DYNAMIC_DISPATCH`)
- **entry points selected** (2):
  - `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)` (2 node(s), graph line 409)
  - `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` (2 node(s), graph line 408)
- **which arm carries the body**: the declared body witness `createDriver` appears among the synthetic arm's own call sites: **true**; among the source-level arm's: **false**. Synthetic types matched by `^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$`: 1.
- **message hops on its route**: `LaunchDriver` (Master to Worker, core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1367)
- **distinct routes**: 0
  - No route from an entry point to a sink host was returned within the stated
    bound. That is a capability finding about what this formulation can express
    over this graph, and it is reported as measured: the bound was not
    loosened, removed or re-run unbounded to produce a non-empty result. The
    4 boundaries below are the measured reason.
- **walks** (its own two, never combined with the other pair's):
  - `A-follows-fan-out`: follows fan-out true, expansions 25009 across all of its entry points with 25006 the peak at any ONE entry point, call sites 33565, fan-out seen 86, fan-out not followed 0, max depth 12, depth bound reached true, per-entry expansion cap reached false, pair step budget exhausted false, route cap reached false, routes 0
  - `B-fan-out-recorded`: follows fan-out false, expansions 5598 across all of its entry points with 5595 the peak at any ONE entry point, call sites 11575, fan-out seen 55, fan-out not followed 55, max depth 12, depth bound reached true, per-entry expansion cap reached false, pair step budget exhausted false, route cap reached false, routes 0
- **route surface for its own expected-spurious basis**: `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.Worker`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`; predicate call sites on it: 0

### Pair `pair-two` - the REST submit servlet's handleSubmit to the SAME privileged process launch on the DriverRunner surface

- **handler**: `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit`, at `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268` at the pin
  - the plan names this handler **StandaloneRestServer handleSubmit**, after the file it
    lives in; the type the method is declared in, and therefore the type this
    query selects on, is `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`. Both names are recorded so the
    resolution is visible rather than looking like a slip.
  - the base declaration on `org.apache.spark.deploy.rest.SubmitRequestServlet` is present in the graph (1 node name(s)) and is **excluded** by the pair's
    exact type selector. Recorded rather than silently dropped.
- **sink**: `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:240` at the pin, resolved to 2 call site(s) on the sink host surface out of 1234 call(s) named `start` scanned (scan truncated: false)
  - `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)` calls `org.apache.spark.deploy.worker.ProcessBuilderLike.start:java.lang.Process()` at graph line 240 (dispatch `DYNAMIC_DISPATCH`)
  - `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()` calls `java.lang.ProcessBuilder.start:java.lang.Process()` at graph line 276 (dispatch `DYNAMIC_DISPATCH`)
- **entry points selected** (1):
  - `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit:org.apache.spark.deploy.rest.SubmitRestProtocolResponse(java.lang.String,org.apache.spark.deploy.rest.SubmitRestProtocolMessage,jakarta.servlet.http.HttpServletResponse)` (2 node(s), graph line 272)
- **which arm carries the body**: the declared body witness `DeployMessages$RequestSubmitDriver.<init>` appears among the synthetic arm's own call sites: **false**; among the source-level arm's: **true**. Synthetic types matched by `^org\.apache\.spark\.deploy\.rest\.StandaloneSubmitRequestServlet\$\$anonfun\$handleSubmit\$\d+$`: 0.
- **message hops on its route**: `RequestSubmitDriver` (the REST submit servlet to Master, core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:276-277), `LaunchDriver` (Master to Worker, core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1367)
- **distinct routes**: 0
  - No route from an entry point to a sink host was returned within the stated
    bound. That is a capability finding about what this formulation can express
    over this graph, and it is reported as measured: the bound was not
    loosened, removed or re-run unbounded to produce a non-empty result. The
    5 boundaries below are the measured reason.
- **walks** (its own two, never combined with the other pair's):
  - `A-follows-fan-out`: follows fan-out true, expansions 10146 across all of its entry points with 10146 the peak at any ONE entry point, call sites 9038, fan-out seen 29, fan-out not followed 0, max depth 12, depth bound reached true, per-entry expansion cap reached false, pair step budget exhausted false, route cap reached false, routes 0
  - `B-fan-out-recorded`: follows fan-out false, expansions 764 across all of its entry points with 764 the peak at any ONE entry point, call sites 1247, fan-out seen 15, fan-out not followed 15, max depth 12, depth bound reached true, per-entry expansion cap reached false, pair step budget exhausted false, route cap reached false, routes 0
- **route surface for its own expected-spurious basis**: `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`, `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.Worker`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`; predicate call sites on it: 0

## Whether the bound was reached

The primary bound is `MAX_CALL_DEPTH` = 12 call-graph hops from an
entry point, applied per pair. Every traversal in this query carries an explicit
named bound; none runs unbounded, and no bound is shared between the pairs, so one
pair cannot consume the other's budget.

- pair `pair-one`: `bound_reached` = **true**
  - walk `A-follows-fan-out`: the frontier was still non-empty at depth 12. Each figure is measured at its cap's own scope: the peak expansion count at any ONE entry point was 25006 of 200000, the counter being reset at each entry point, and the walk's total across all of its entry points was 25009, which caps nothing; this walk contributed 33565 call sites to the ONE step budget of 400000 that both of the pair's walks draw on; routes returned 0 of 64.
  - walk `B-fan-out-recorded`: the frontier was still non-empty at depth 12. Each figure is measured at its cap's own scope: the peak expansion count at any ONE entry point was 5595 of 200000, the counter being reset at each entry point, and the walk's total across all of its entry points was 5598, which caps nothing; this walk contributed 11575 call sites to the ONE step budget of 400000 that both of the pair's walks draw on; routes returned 0 of 64.
- pair `pair-two`: `bound_reached` = **true**
  - walk `A-follows-fan-out`: the frontier was still non-empty at depth 12. Each figure is measured at its cap's own scope: the peak expansion count at any ONE entry point was 10146 of 200000, the counter being reset at each entry point, and the walk's total across all of its entry points was 10146, which caps nothing; this walk contributed 9038 call sites to the ONE step budget of 400000 that both of the pair's walks draw on; routes returned 0 of 64.
  - walk `B-fan-out-recorded`: the frontier was still non-empty at depth 12. Each figure is measured at its cap's own scope: the peak expansion count at any ONE entry point was 764 of 200000, the counter being reset at each entry point, and the walk's total across all of its entry points was 764, which caps nothing; this walk contributed 1247 call sites to the ONE step budget of 400000 that both of the pair's walks draw on; routes returned 0 of 64.

A depth bound reached with a non-empty frontier says only that the walk stopped
expanding, so on its own it would leave open whether a deeper walk would reach a
sink host. What settles that here is the boundary measurement below rather than the
bound: the hops that break these routes are not CALL edges at all, and no increase
in depth introduces an edge that does not exist.

| bound | value |
| --- | --- |
| MAX_CALL_DEPTH | 12 |
| MAX_ROUTES_PER_PAIR | 64 |
| MAX_EXPANSIONS_PER_ENTRY | 200000 |
| MAX_STEPS_PER_PAIR | 400000 |
| MAX_TOTAL_RETURNS | 256 |
| MAX_ENTRY_POINTS_PER_PAIR | 16 |
| MAX_CALL_SCAN | 200000 |
| MAX_TYPE_SCAN | 200000 |
| FANOUT_CALLEE_THRESHOLD | 32 |

## The boundaries, per pair

Each hop below is measured against the graph, not asserted. `crossed` states
whether a CALL edge in fact joins the two ends. A hop on the part of the route the
two pairs **share** is measured **once** and cited by both, which is why the
boundary count per pair is larger than the number of measurements taken: 9 citations over 6 measurements.

### Pair `pair-one`: 4 boundaries

#### `B-rpc-LaunchDriver` - crossed by a call edge: **false** (cited by `pair-one`, `pair-two`)

- **hop**: RpcEndpointRef send of org.apache.spark.deploy.DeployMessages$LaunchDriver, Master to Worker, at core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1367
- **from**: `org.apache.spark.deploy.master.Master.launchDriver:void(org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.master.DriverInfo)`
- **to**: `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a message send is not a call: the sender enqueues a value on an endpoint reference and the receiving handler is dispatched later, so no CALL edge joins the two ends
- **modelling**: modelled explicitly by pairing on the MESSAGE TYPE - call sites of org.apache.spark.deploy.DeployMessages$LaunchDriver.<init> are the producer end and call sites of its field accessors (driverDesc, driverId, resources) are the consumer end, with the message type's and companion's own generated machinery excluded by owning type

#### `B-thread` - crossed by a call edge: **false** (cited by `pair-one`, `pair-two`)

- **hop**: org.apache.spark.deploy.worker.DriverRunner.start calls Thread.start(); the route continues in run() on the anonymous Thread subclass (DriverRunner.scala:123 and :90 at the pin)
- **from**: `org.apache.spark.deploy.worker.DriverRunner.start:void()`
- **to**: `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
- **reason**: Thread.start() -> run() is a JVM scheduling relation, not a call: the start frame returns immediately and run() is entered on another thread, so no CALL edge joins them
- **modelling**: not modelled - the two ends are reported as measured and the hop is left uncrossed

#### `B-interface` - crossed by a call edge: **true** (cited by `pair-one`, `pair-two`)

- **hop**: the launch call site invokes the ABSTRACT ProcessBuilderLike.start (DriverRunner.scala:270 at the pin); the JDK launch is reached only through the anonymous implementation at :276
- **from**: `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **to**: `java.lang.ProcessBuilder.start:java.lang.Process(), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **reason**: an interface invocation names the declaring type, so linking it to an implementation needs the type hierarchy rather than the call's own name
- **modelling**: not modelled by this query - whether the hop is crossed is a property of the graph's call linker and is reported as measured

#### `B-partial-function-pair-one` - crossed by a call edge: **false** (cited by `pair-one`)

- **hop**: org.apache.spark.deploy.master.Master.receiveAndReply (core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409 at the pin): where the graph carries this handler's body
- **from**: `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
- **to**: `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a Scala handler that returns PartialFunction[Any, Unit] compiles its case bodies into a synthetic class, so for that shape the graph's entry point is the synthetic applyOrElse and NOT a method of the handler's own name; an ordinary method has no such class at all
- **modelling**: modelled by selecting BOTH arms - the synthetic applyOrElse on every type matching ^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$, and the source-level receiveAndReply on org.apache.spark.deploy.master.Master - and then MEASURING which of them carries the declared body witness 'createDriver'. Resolved here as: the SYNTHETIC arm carries the route: the handler returns a partial function, its body compiles into org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1), and the source-level method of the same name only constructs the partial function. The crossed flag is read strictly: it is true only where a CALL edge joins the source-level method to the body, which is the hop this boundary names. Where no synthetic class exists the hop does not arise at all, and the flag then simply records that the source-level method - which IS the body - reaches it; hop_arises_for_this_handler distinguishes the two cases

Boundaries not crossed by a call edge: `B-rpc-LaunchDriver`, `B-thread`, `B-partial-function-pair-one`.

### Pair `pair-two`: 5 boundaries

#### `B-rpc-RequestSubmitDriver` - crossed by a call edge: **false** (cited by `pair-two`)

- **hop**: RpcEndpointRef send of org.apache.spark.deploy.DeployMessages$RequestSubmitDriver, the REST submit servlet to Master, at core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:276-277
- **from**: `org.apache.spark.deploy.ClientEndpoint.onStart:void(), org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit:org.apache.spark.deploy.rest.SubmitRestProtocolResponse(java.lang.String,org.apache.spark.deploy.rest.SubmitRestProtocolMessage,jakarta.servlet.http.HttpServletResponse)`
- **to**: `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a message send is not a call: the sender enqueues a value on an endpoint reference and the receiving handler is dispatched later, so no CALL edge joins the two ends
- **modelling**: modelled explicitly by pairing on the MESSAGE TYPE - call sites of org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.<init> are the producer end and call sites of its field accessors (driverDescription) are the consumer end, with the message type's and companion's own generated machinery excluded by owning type

#### `B-rpc-LaunchDriver` - crossed by a call edge: **false** (cited by `pair-one`, `pair-two`)

- **hop**: RpcEndpointRef send of org.apache.spark.deploy.DeployMessages$LaunchDriver, Master to Worker, at core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1367
- **from**: `org.apache.spark.deploy.master.Master.launchDriver:void(org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.master.DriverInfo)`
- **to**: `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a message send is not a call: the sender enqueues a value on an endpoint reference and the receiving handler is dispatched later, so no CALL edge joins the two ends
- **modelling**: modelled explicitly by pairing on the MESSAGE TYPE - call sites of org.apache.spark.deploy.DeployMessages$LaunchDriver.<init> are the producer end and call sites of its field accessors (driverDesc, driverId, resources) are the consumer end, with the message type's and companion's own generated machinery excluded by owning type

#### `B-thread` - crossed by a call edge: **false** (cited by `pair-one`, `pair-two`)

- **hop**: org.apache.spark.deploy.worker.DriverRunner.start calls Thread.start(); the route continues in run() on the anonymous Thread subclass (DriverRunner.scala:123 and :90 at the pin)
- **from**: `org.apache.spark.deploy.worker.DriverRunner.start:void()`
- **to**: `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
- **reason**: Thread.start() -> run() is a JVM scheduling relation, not a call: the start frame returns immediately and run() is entered on another thread, so no CALL edge joins them
- **modelling**: not modelled - the two ends are reported as measured and the hop is left uncrossed

#### `B-interface` - crossed by a call edge: **true** (cited by `pair-one`, `pair-two`)

- **hop**: the launch call site invokes the ABSTRACT ProcessBuilderLike.start (DriverRunner.scala:270 at the pin); the JDK launch is reached only through the anonymous implementation at :276
- **from**: `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **to**: `java.lang.ProcessBuilder.start:java.lang.Process(), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **reason**: an interface invocation names the declaring type, so linking it to an implementation needs the type hierarchy rather than the call's own name
- **modelling**: not modelled by this query - whether the hop is crossed is a property of the graph's call linker and is reported as measured

#### `B-partial-function-pair-two` - crossed by a call edge: **true** (cited by `pair-two`)

- **hop**: org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit (core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268 at the pin): where the graph carries this handler's body
- **from**: `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit:org.apache.spark.deploy.rest.SubmitRestProtocolResponse(java.lang.String,org.apache.spark.deploy.rest.SubmitRestProtocolMessage,jakarta.servlet.http.HttpServletResponse)`
- **to**: (none measured)
- **reason**: a Scala handler that returns PartialFunction[Any, Unit] compiles its case bodies into a synthetic class, so for that shape the graph's entry point is the synthetic applyOrElse and NOT a method of the handler's own name; an ordinary method has no such class at all
- **modelling**: modelled by selecting BOTH arms - the synthetic applyOrElse on every type matching ^org\.apache\.spark\.deploy\.rest\.StandaloneSubmitRequestServlet\$\$anonfun\$handleSubmit\$\d+$, and the source-level handleSubmit on org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet - and then MEASURING which of them carries the declared body witness 'DeployMessages$RequestSubmitDriver.<init>'. Resolved here as: the SOURCE-LEVEL arm carries the route: no type matching ^org\.apache\.spark\.deploy\.rest\.StandaloneSubmitRequestServlet\$\$anonfun\$handleSubmit\$\d+$ exists, because this handler is an ordinary method rather than a partial function, so its body is its own and the synthetic arm is legitimately empty. The crossed flag is read strictly: it is true only where a CALL edge joins the source-level method to the body, which is the hop this boundary names. Where no synthetic class exists the hop does not arise at all, and the flag then simply records that the source-level method - which IS the body - reaches it; hop_arises_for_this_handler distinguishes the two cases

Boundaries not crossed by a call edge: `B-rpc-RequestSubmitDriver`, `B-rpc-LaunchDriver`, `B-thread`.

### The one hop this query models rather than reports as not-connectable

Pair `pair-two` crosses a message-send boundary at its **first** step: the
servlet's handler does not call the Master, it **sends** a message by `askSync`
at `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:276-277` at the pin, and that is the very
message pair one's handler receives at
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala:410`. A call graph does not join a send to its
receiving handler, so this query **models the hop explicitly by pairing on the
message type** `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver`,
and the graph evidence for the model is measured rather than asserted: the
constructor call sites of that type are the producer end, its declared field
accessors' call sites are the consumer end, and the message type's and companion's
own generated machinery is excluded by owning type. The measured ends, and whether
a CALL edge joins them, are in the `B-rpc-RequestSubmitDriver`
record above and in the envelope.

What the model buys, measured: the producer end of that hop is the declared entry
point of `pair-two` and its
consumer end is the declared entry point of `pair-one`. Pairing on the
message type is
therefore what joins one pair's handler to the other's entry point, and it is the
reason this pair is reported as crossing one boundary more than pair one rather
than as a route that cannot be expressed at all.

### The partial-function boundary, and why it is measured per pair

A Scala handler that returns `PartialFunction[Any, Unit]` compiles its case bodies
into a synthetic class, so for that shape the graph's entry point is the synthetic
`applyOrElse` and **not** a method of the handler's own name. An
ordinary method has no such class at all. The parameterized selector therefore
takes the **union** of both arms and then **measures** which one carries the
declared body witness. The two pairs answer it differently, and that difference is
a capability observation in its own right:

- pair `pair-one`: synthetic types matched 1, body witness in the synthetic arm true, in the source-level arm false
- pair `pair-two`: synthetic types matched 0, body witness in the synthetic arm false, in the source-level arm true

The `crossed_by_a_call_edge` flag on that boundary is read strictly: it is true only
where a CALL edge joins the **source-level** method to the body. For a
partial-function handler it is false, which is exactly what the boundary names; for
a handler with no synthetic class the hop does not arise at all, and
`hop_arises_for_this_handler` in the record distinguishes those two cases so the
flag is never read as if the same hop had been crossed.

A selector that took only one arm would silently miss one of the two pairs - which
is exactly the kind of detail a parameterized query has to get right to generalise
past the pair it was written against.

## The predicate set, and the source types it came from

The mechanical definition: a route is spurious **only** where it passes an
authorization or ACL predicate before reaching the sink. The predicate set is
exactly these five Boolean methods, and their source is
`core/src/main/scala/org/apache/spark/SecurityManager.scala` at the pin
(457 lines), on the single source type `org.apache.spark.SecurityManager`:

| predicate | source line at the pin |
| --- | --- |
| `aclsEnabled()` | 227 |
| `checkAdminPermissions` | 234 |
| `checkUIViewPermissions` | 248 |
| `checkModifyPermissions` | 264 |
| `isAuthenticationEnabled()` | 274 |

`core/src/main/scala/org/apache/spark/deploy/master/Master.scala:411`'s `if (state != RecoveryState.ALIVE)` is a
**recovery-state** check and is deliberately not in this set.

Whether the four selector constants in this query's source carry the same literal
text as those of `queries/joern/01-callgraph-unguarded-driver-launch.sc` and
`queries/joern/02-dataflow-unguarded-driver-launch.sc` is **measured** at run time rather
than asserted here, by reading each sibling source and comparing literal to
literal; the outcome is published per sibling as `predicate_selector_literals`
`_identical`. It matters because three spurious counts are only comparable while
the definition of the term is the same text in all three files.

### How the bytecode-level selector was constrained

The anchored selector is `^(check.*Permissions|acls.*|isAuthenticationEnabled)$`, paired with a type selector on
`org.apache.spark.SecurityManager`. On **bytecode** that is not enough. `SecurityManager.scala:59`
declares `private var aclsOn`, and Scala compiles a private var into accessors, so
the graph carries both a getter and a setter whose names satisfy the `acls.*`
alternative. The narrowing is therefore three steps, and all three sets are
reported so it is auditable rather than asserted:

1. broad anchored selector on the 252 method nodes (107 distinct names) of that type: `aclsEnabled`, `aclsOn`, `aclsOn_$eq`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`
2. minus every name ending in `_$eq`, which drops `aclsOn_$eq`, leaving `aclsEnabled`, `aclsOn`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`
3. intersected with the five named source-level predicates, which drops `aclsOn` - a private-var getter, not one of the five, leaving exactly `aclsEnabled`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`

The final set is asserted against the graph, not against the source.

### The shared route surface, and each pair's own

The byte-identical block also carries `ROUTE_SURFACE_TYPE_PREFIXES` = `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.rest.StandaloneRestServer`, `org.apache.spark.deploy.worker.DriverRunner`.
Measured here rather than assumed: the handler type(s) `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet` are **not** covered by any of those prefixes, because the type a method is declared in is not always the headline class of the file it lives in.
The shared list is kept exactly as it stands so all three queries' spurious counts
remain comparable, and each pair additionally carries its **own** route surface,
derived from its own handler, the intermediate hop and its sink types, which is
what makes that pair's expected-spurious basis correct. Both counts are published.

### The intermediate route hop

Both pairs' routes run handler -> RPC -> **Worker** -> DriverRunner -> launch, so
the Worker is a *hop* of the route rather than an end of it, and it is on each
pair's own route surface for that reason. A surface naming only the handler and the
sink host would leave one hop of the route unsearched while the resulting statement
read as one about the whole route. Its anchors at the pin:

- `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:523 override def receive declares the handler that receives the launch message`
- `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:687 case LaunchDriver(driverId, driverDesc, resources_) matches it`
- `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:689 constructs the DriverRunner that hosts the sink`
- `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:701 calls driver.start(), the hop into the sink host`
- `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:736 override def receiveAndReply, the second handler on the same type and not on this route`

`core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala` is 1046 lines at the pin, and it is one of the 4 route files searched for the five names.

Every prefix on either pair's surface reports the **same** fields, the intermediate
hop included. The reach columns are what make a zero falsifiable: a zero over a
surface the graph does not carry would read exactly like a searched surface that
came back clean, so a prefix with no type declaration stops the run instead of
contributing one.

| surface prefix | type decls | methods | predicate call sites | on pairs |
| --- | --- | --- | --- | --- |
| `org.apache.spark.deploy.master.Master` | 217 | 607 | 0 | pair-one, pair-two |
| `org.apache.spark.deploy.rest.StandaloneRestServer` | 2 | 13 | 0 | shared list only |
| `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet` | 25 | 32 | 0 | pair-two |
| `org.apache.spark.deploy.worker.DriverRunner` | 21 | 93 | 0 | pair-one, pair-two |
| `org.apache.spark.deploy.worker.ProcessBuilderLike` | 6 | 9 | 0 | pair-one, pair-two |
| `org.apache.spark.deploy.worker.Worker` | 156 | 475 | 0 | pair-one, pair-two |

The sweep behind those reach columns is bounded by `MAX_TYPE_SCAN` = 200000 type declarations per prefix and reported truncated = **false**.

## Whether an expected-spurious route was absent

### Pair `pair-one`: `spurious_count` = **0**

No route in the emitted set passed an auth/ACL predicate as defined by these five
named selectors.

**The absence is structural, not a consequence of the query filtering well.**
Measured against the graph: 18 call sites of the five
predicates exist graph-wide, in 18 distinct calling
methods, and **0** of them sit on this
pair's own route surface (`org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.Worker`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`).
The predicate set exists and is invoked elsewhere in the program; it is not
invoked anywhere on this pair's route, so no route of this pair could have
passed one.

### Pair `pair-two`: `spurious_count` = **0**

No route in the emitted set passed an auth/ACL predicate as defined by these five
named selectors.

**The absence is structural, not a consequence of the query filtering well.**
Measured against the graph: 18 call sites of the five
predicates exist graph-wide, in 18 distinct calling
methods, and **0** of them sit on this
pair's own route surface (`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`, `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.Worker`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`).
The predicate set exists and is invoked elsewhere in the program; it is not
invoked anywhere on this pair's route, so no route of this pair could have
passed one.

For pair `pair-two` one further selector measurement is worth recording,
because it is what makes that pair's basis structural: in the pinned tree the file
`core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala` is matched by **none** of the
five named selectors and carries no reference to the predicate type at all. Its
only `permission` occurrence is the Apache licence boilerplate at line 14, and a
case-insensitive search additionally returns lines 209, 233 and 251, which are
false positives - the matched literal is `aCl` inside `extraClassPath` /
`driverExtraClassPath`. That is a statement about which of these selectors match
that file, and nothing more.

### What this section does not say

These are statements about **this query's own output** under **this query's own**
mechanical definition of one word. They are not an assessment of Spark, of any
Spark component, of any Spark configuration or of any submission path, and nothing
here should be read as one. In particular, nothing in this report states or implies
anything about how Spark authorizes any operation: the five selectors above are a
query-side definition used to classify this query's own returns, and where a route
count is zero there are no returns to classify at all.

## Whether this formulation duplicates another query's

`duplicate_formulation` = **partial_duplicate**. A duplicate of 01-callgraph-unguarded-driver-launch on pair-one, not a duplicate as a whole, and not a duplicate of 02-dataflow-unguarded-driver-launch in any instantiation. The scope of any duplication is stated rather than hidden: where it is one pair only, the parameterized form's remaining instantiations are the part that is new. Every clause here is computed from the per-query entries below, so it cannot disagree with them.

Aggregation: the top-level verdict aggregates the per-query entries below and names the strongest relation any one of them carries: duplicate_formulation_on_pair-one against 01-callgraph-unguarded-driver-launch, not_duplicate against 02-dataflow-unguarded-driver-launch. One entry is a duplicate at a scope NARROWER than the whole pair set, which makes the aggregate partial rather than absent. The scope is stated in that entry rather than hidden in this label. It was NOT inferred from the file names differing

Relation: a SYMMETRIC pairwise relation: the verdict this envelope states against a query is the same verdict that query's envelope states against this one. It is one measurement cited twice rather than two measurements, and here it is symmetric BY CONSTRUCTION rather than by transcription - every entry below is computed by applying ONE shared predicate to the two queries' own declared formulation identity blocks, read out of the two SOURCE files at run time under names all three queries share. Both directions therefore evaluate identical inputs through identical code, so a disagreement between them is not expressible; a transcribed verdict could disagree with the envelope it was copied from, which is exactly what this replaces

The comparison reads each sibling's **source**, not its published result. Every
entry below is produced by applying one shared predicate to the two queries' own
declared formulation identity blocks, so both directions of the relation evaluate
identical inputs through identical code and a disagreement between them is not
expressible. No sibling figure is transcribed into this file, which is what makes
the verdict incapable of drifting from the file it describes.

### Against `01-callgraph-unguarded-driver-launch`: **duplicate_formulation_on_pair-one**

- scope: pair-one only; this query additionally addresses pair-two, which 01-callgraph-unguarded-driver-launch does not
- basis: every component of the formulation identity agrees at the scope named above: the edge kinds traversed (CALL); the node kinds selected as a route's ends (METHOD); at least one handler/sink pair in common; the entry-point selector literals, byte for byte; the sink selector literals, byte for byte; the bound, as the same kind of quantity at the same value (12 call-graph hops expanded from an entry point); the Joern API construct sets, whose set difference is empty in BOTH directions. The comparison is over the two SOURCES' own declarations, so it is a property of the two formulations rather than of either run's numbers
- sibling source: `queries/joern/01-callgraph-unguarded-driver-launch.sc`, sha256 `dc7ca0fb9f8d7809afcc31602d48d568fd78bf8136a99df55ae6c1e9f6b4180b`, 213224 bytes; read note: measured from the source text at run time
- pair ids here: pair-one, pair-two; there: pair-one; shared: pair-one
- edge kinds: here CALL, there CALL (same = true)
- end node kinds: here METHOD, there METHOD (same = true)
- bound: here `MAX_CALL_DEPTH` = 12 (call-graph hops expanded from an entry point); there `MAX_CALL_DEPTH` = 12 (call-graph hops expanded from an entry point); same kind = true, same value = true
- entry selector literals identical: true; sink selector literals identical: true
- predicate selector literals identical: true (reported, not a component of the formulation predicate: the predicate set defines the word "spurious" rather than the traversal)
- API constructs: 28 shared, 0 only here, 0 only there

### Against `02-dataflow-unguarded-driver-launch`: **not_duplicate**

- scope: none
- basis: the formulations differ on the edge kinds traversed (CALL); the node kinds selected as a route's ends (METHOD); the bound, as the same kind of quantity at the same value (12 call-graph hops expanded from an entry point); the Joern API construct sets, whose set difference is empty in BOTH directions, while agreeing on at least one handler/sink pair in common; the entry-point selector literals, byte for byte; the sink selector literals, byte for byte. Neither traversal establishes the other's conclusion, so the two results are reported side by side and never summed
- sibling source: `queries/joern/02-dataflow-unguarded-driver-launch.sc`, sha256 `546003c5b35a5b1e866a0928b362ab9016fc366d1e4305c3b4d1191e557996de`, 267884 bytes; read note: measured from the source text at run time
- pair ids here: pair-one, pair-two; there: pair-one; shared: pair-one
- edge kinds: here CALL, there REACHING_DEF (same = false)
- end node kinds: here METHOD, there METHOD_PARAMETER_IN, EXPRESSION (same = false)
- bound: here `MAX_CALL_DEPTH` = 12 (call-graph hops expanded from an entry point); there `MAX_FLOW_CALL_DEPTH` = 6 (call boundaries the backward data-flow search may expand); same kind = false, same value = false
- entry selector literals identical: true; sink selector literals identical: true
- predicate selector literals identical: true (reported, not a component of the formulation predicate: the predicate set defines the word "spurious" rather than the traversal)
- API constructs: 24 shared, 4 only here, 18 only there

the entry-selector literals compared are pair one's, the pair this query and the call-graph query share. This query's second pair is expressed through FORMULATION_PAIR_IDS, and its selectors are published in full in the pairs block of this envelope rather than folded into the comparison. The consequence is stated rather than hidden: two queries declaring the same pair id set but different selectors for a pair other than the compared one would not be distinguished by the selector component alone, and a reader checking that case reads the pairs block

Pair one's figures **measured here**, published so a reader can compare them
against query 01's own published figures rather than against a copy of them made
inside this file: 2 entry point(s), 0 distinct route(s), bound value 12, boundaries not crossed `B1-rpc`, `B2-thread`, `B4-partial-function` in query 01's numbering, under the naming map `B-interface` -> `B3-interface`, `B-partial-function-pair-one` -> `B4-partial-function`, `B-rpc-LaunchDriver` -> `B1-rpc`, `B-thread` -> `B2-thread`.
A cross-run comparison of *counts* is a reader's to make from the two result
files: each query measured its own against the graph of its own run.

## The three effort measures

1. **Query revisions committed: 5.** Convention: commits touching queries/joern/03-parameterized-handler-sink-pairs.sc from its first appearance to the end of the probe, counted at run time from the repository's own history. The commit that publishes these result files is necessarily NOT among them: it cannot exist while the run that writes them is still in progress.
   Measurement: measured from the repository's own history at run time, newest first.
   The commits counted, newest first, so the number is auditable rather than
   asserted:

   - `b166213252d852cd409f3982dafcaf1cc2b04330`
   - `56d4bf10a02adb0e44b2bf59f77e9a2402965979`
   - `b562bca85a5e4a1986607b023650a6e5dcd3476b`
   - `1072fd2334fc1a1b54b62119e086920e951ac209`
   - `20a56482274ab6c5f53b45f0488d2fa37012e03f`

2. **Distinct Joern API constructs used: 28.** Listed explicitly and deduplicated so the
   count is auditable from the list rather than asserted; every entry appears
   literally in the query source:

   - `Call.code`
   - `Call.dispatchType`
   - `Call.lineNumber`
   - `Call.method`
   - `Call.methodFullName`
   - `Call.name`
   - `Call.order`
   - `Method.callIn`
   - `Method.callOut`
   - `Method.fullName`
   - `Method.lineNumber`
   - `Method.name`
   - `Method.typeDecl`
   - `NoResolve.getCalledMethodsAsTraversal`
   - `Steps.fullName`
   - `Steps.fullNameExact`
   - `Steps.l`
   - `Steps.nameExact`
   - `Steps.size`
   - `Steps.take`
   - `TypeDecl.fullName`
   - `TypeDecl.method`
   - `cpg.call`
   - `cpg.file`
   - `cpg.method`
   - `cpg.typeDecl`
   - `importCpg`
   - `switchWorkspace`

   Constructs declared here that `01-callgraph-unguarded-driver-launch` does not declare: none.
   Constructs declared here that `02-dataflow-unguarded-driver-launch` does not declare: `Call.code`, `Call.order`, `Method.callOut`, `NoResolve.getCalledMethodsAsTraversal`.

3. **Parameterizability: passed.** This file owns the measure.
   It passes ONLY where the parameterized query is actually invoked on the second named pair and that invocation's result is captured in this query's result files and console log; an empty result from a real invocation satisfies it, a skipped invocation does not, and a parameter list that merely exists does not.

   - first pair `pair-one`: invoked; entry points traversed 2 of 2; distinct routes 0; spurious 0; boundaries measured or cited 4
   - second pair `pair-two` (`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit` at `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268` to the launch at `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:240`, both at the pin): invoked; entry points traversed 1 of 1; walks run A-follows-fan-out and B-fan-out-recorded; call sites considered A-follows-fan-out=9038, B-fan-out-recorded=1247; distinct routes 0; spurious 0; boundaries measured or cited 5 (B-rpc-RequestSubmitDriver, B-rpc-LaunchDriver, B-thread, B-interface, B-partial-function-pair-two)

   The verdict rests on the second pair's invocation having actually run in this
   same run, and on its result being captured here, in the envelope and in the
   console log - not on the existence of a parameter. Its selection, its walk
   counters, its boundary measurements, its distinct-route count and its spurious
   count are all published above. An empty result from a real invocation
   satisfies the measure; a skipped invocation would not, and a malformed pair
   aborts the run rather than being passed over.

   The handler surface this parameterization draws on is ample rather than exactly
   the two cases it was written against, and that is measured in the pinned tree:
   **eight** `receive`/`receiveAndReply` declarations across **five** files under
   `core/src/main/scala/org/apache/spark/deploy` - `Client.scala:207`,
   `client/StandaloneAppClient.scala:161` and `:209`, `master/Master.scala:239` and
   `:409`, `worker/Worker.scala:523` and `:736`, `worker/WorkerWatcher.scala:66` -
   alongside the one shared sink both pairs here use.

## Modelling decisions, stated so the counts stay interpretable

- **Nothing is summed across pairs.** Routes, spurious counts, bound flags and
  entry-point counters are per pair and keyed by pair id in the envelope. The one
  overall figure, `returned_record_count`, is a count of records emitted, and the
  one overall flag, `bound_reached_any`, is a disjunction - neither is a total over
  routes.
- **A hop the two pairs share is one measurement.** The thread hop and the
  interface hop at the sink are measured once and cited by both pairs, with
  `cited_by_pairs` on the record naming who cites it. A count appearing in two
  places here is one measurement cited twice, never two measurements.
- **Operator pseudo-calls are excluded.** A CPG `<operator>.*` call is an artefact
  of the representation, not a method call, and expanding them would inflate every
  counter without adding a call-graph hop.
- **Duplicate class definitions are unioned.** The graph carries more than one node
  per class where two staged archives carried the same class, so method nodes are
  grouped by full name and their call sites unioned rather than one node being
  picked. Reachability is keyed on the method full name.
- **Callee resolution is explicit.** Each call site's callees are taken from
  `NoResolve.getCalledMethodsAsTraversal`, which is exactly the statically linked
  CALL-edge callees of that site.
- **Two walks per pair, reported side by side.** Walk `A-follows-fan-out` expands
  every call site. Walk `B-fan-out-recorded` records but does not expand a call site
  whose resolved callee set exceeds 32 distinct methods:
  expanding such a site models "any implementation in the program may be invoked
  here", which is a property of the call linker rather than of either route. Both
  walks' counters are published per pair and their routes are deduplicated within
  the pair, never summed.
- **Graph line numbers are the graph's own.** A method or call node's `lineNumber`
  comes from the bytecode line-number table and can differ by a line from the `def`
  or statement line cited from the source. Source anchors in this report are quoted
  from the pinned tree; graph lines are labelled as such.
- **A bytecode file path is not a source path.** The frontend records an extraction
  path under a temporary directory for every class, so this query reports types,
  methods and lines rather than presenting that path as a source location.

## The graph this query loaded, and its identity

- named path `$HARNESS_CPG` (repository-relative `harness/cpg/spark.cpg`), a symlink
- resolved target: a host-shared read-only file outside the repository root, reached by following the symlink, **541309809** bytes, sha256 `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`
- the link itself measures 33 bytes; that figure is recorded only to be
  discarded, because measuring the link rather than its target is the mistake this
  check exists to avoid
- no absolute host path appears in this report or in the envelope: the clone root is
  a property of the checkout rather than of the measurement, and the size-and-digest
  pair above is what the identity comparison turns on. The literals are in
  `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`, a console stream not held to byte-identity
- record of account: `harness/artifacts/logs/cpg-frontend.log`, which states bytes 541309809 and
  sha256 `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` - re-verified immediately before the load, and a
  mismatch would have halted the run
- there is **no environment override** for that record. This query reads no
  variable that could point the comparison at a different one, so the record a
  reader can read is exactly the record the comparison turned on. Where the
  host's graph and this record disagree, the run halts and the disagreement is
  reported rather than routed around
- the AAP-named path `harness/cpg/spark.cpg`: same file (equal resolved target)

## Reproducing this

```
cd <a scratch directory outside the repository> && HARNESS_REPO_ROOT=<the repository root> JAVA_HOME="$JAVA_HOME_21" JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script <the repository root>/queries/joern/03-parameterized-handler-sink-pairs.sc -J-Xmx64g < /dev/null
```

That is the **whole** command: the repository root, the JDK, the heap override,
the log level and the script path. Both pairs are declared as named constants in
the query source and both are invoked by that one command, so no per-pair
parameter has to be passed on the command line and the second pair's invocation
is reproducible from this record alone. This query reads no other environment
variable that changes what it loads or what it publishes, and in particular there
is no override for the identity record - the record of account is
`harness/artifacts/logs/cpg-frontend.log`, so a load can never be adjudicated by a record this command
does not name.

`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so
`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs
at. The query measures the heap it received and stops below the floor: raising a
heap is permitted and reported, lowering one is not.
