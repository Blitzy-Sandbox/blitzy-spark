# Joern capability probe 03-parameterized-handler-sink-pairs

Bounded **call-graph** reachability over CALL edges, **parameterized over
handler/sink pairs** and instantiated on **two** named pairs in one run, over the
code-property graph built from the pinned tree's bytecode.

This report is **observational**. It judges no finding - not real, not important,
not a false positive, not a duplicate - and makes no comparison between tools. It
contributes no row to `oss-scan-results/findings.json` and writes nothing into
`harness/artifacts/raw/`. This probe tree is Joern's deliberate **second**
appearance in the run - the Stage 3 runner is the first - and folding either
appearance into the other's numbers would corrupt both that tool's count and the
dataset total, which is why nothing here becomes a dataset row.

The slug `03-parameterized-handler-sink-pairs` is the **identifier** the plan assigns this query, and the
slugs `01-callgraph-unguarded-driver-launch` and
`02-dataflow-unguarded-driver-launch` are likewise identifiers assigned to the two
sibling queries. A slug names the question a query was written to ask. It is not a
finding, and nothing in this report should be read as an assessment of Spark, of
any Spark component or of any Spark configuration.

| | |
| --- | --- |
| Query source | `queries/joern/03-parameterized-handler-sink-pairs.sc` |
| Envelope | `queries/joern/results/03-parameterized-handler-sink-pairs.json` |
| Console log | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21, the required major (JVM `21.0.12.1+1-LTS`) |
| Heap actually used | 68719476736 bytes = 64 GiB (floor 68719476736 = 64 GiB; at or above the floor) |
| Heap-bound JVM position | the Stage 5 probe, one of 4 - the frontend build, the `importCpg` verification load, the Stage 3 Joern runner, then this probe |
| Graph | `$HARNESS_CPG` (repository-relative `harness/cpg/spark.cpg`), symlink-followed: 548118435 bytes, sha256 `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53` |
| Graph identity re-verified before the load | yes, against the record of account named by `$HARNESS_CPG_RECORD`, which owns that pair for the graph actually loaded |
| Graph methods / typeDecls / files | 1399866 / 119920 / 45037 |
| Compile status | compiled |
| Run status | completed |
| Pairs declared / invoked | 2 / 2 |
| Pair iteration order | `pair-one`, `pair-two` |
| Records returned | 6 (6 boundary measurement(s) plus per-pair route records) |
| Distinct routes | `pair-one` 0, `pair-two` 0 - reported side by side, never summed |
| Spurious routes | `pair-one` 0, `pair-two` 0 - under the five-selector definition below |
| Parameterizability | **passed** |
| Duplicate formulation | **partial_duplicate** |

The query reached the graph through **`importCpg` and nothing else**. That is a
textual property of the committed sources as well as a behavioural one about this
run: the alternative loader - the one that compiles source afresh and, on Joern's
own documented behaviour, spawns a second JVM at the same heap - is invoked in
**none** of the three committed query sources under `queries/joern/`, and the
absence was checked by searching those files rather than inferred from what this
run happened to do.

**This report measures nothing.** Every figure in it is **read from**
`queries/joern/results/03-parameterized-handler-sink-pairs.json`, which in turn
cites the run that measured them; the handful of node counts and graph line
numbers that envelope does not itself carry are cited from
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`, the same
console stream the envelope names as the source of its own measured values.
Nothing here is a second measurement: where a count appears both here and in that
envelope it is one measurement cited twice, and if the two ever disagreed the
envelope would be right and this file wrong. Source **line numbers** are a
different kind of fact - they are quoted from the pinned tree at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` and were each re-verified there.

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
  - `A-follows-fan-out`: follows fan-out true, expansions 19551, call sites 21476, fan-out seen 48, fan-out not followed 0, max depth 12, depth bound reached true, expansion budget exhausted false, step cap reached false, route cap reached false, routes 0
  - `B-fan-out-recorded`: follows fan-out false, expansions 2350, call sites 4383, fan-out seen 31, fan-out not followed 31, max depth 12, depth bound reached true, expansion budget exhausted false, step cap reached false, route cap reached false, routes 0
- **route surface for its own expected-spurious basis**: `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`; predicate call sites on it: 0

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
  - `A-follows-fan-out`: follows fan-out true, expansions 10029, call sites 8949, fan-out seen 25, fan-out not followed 0, max depth 12, depth bound reached true, expansion budget exhausted false, step cap reached false, route cap reached false, routes 0
  - `B-fan-out-recorded`: follows fan-out false, expansions 732, call sites 1230, fan-out seen 12, fan-out not followed 12, max depth 12, depth bound reached true, expansion budget exhausted false, step cap reached false, route cap reached false, routes 0
- **route surface for its own expected-spurious basis**: `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`, `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`; predicate call sites on it: 0

### Pair `pair-two`'s enclosing type, and why naming it is load-bearing

`handleSubmit` at `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268`
is **not** a member of `StandaloneRestServer`. That file declares **seven**
classes at the pin, and the handler belongs to the last of them:

| declared class | line at the pin |
| --- | --- |
| `StandaloneRestServer` | 56 |
| `StandaloneKillRequestServlet` | 81 |
| `StandaloneKillAllRequestServlet` | 99 |
| `StandaloneStatusRequestServlet` | 116 |
| `StandaloneClearRequestServlet` | 138 |
| `StandaloneReadyzRequestServlet` | 155 |
| `StandaloneSubmitRequestServlet` | 171 |

`handleSubmit` is declared in **`StandaloneSubmitRequestServlet`** - bytecode type
`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`. `StandaloneRestServer`
merely instantiates it, at `:64-65`, as `submitRequestServlet`. So this report
names the handler's enclosing type as `StandaloneSubmitRequestServlet` while
citing the **file** as `StandaloneRestServer.scala`: the plan's own label for the
pair is the file's name, and both names are recorded rather than one being
silently substituted for the other.

The consequence is measured rather than hypothetical, and it is the reason the
type is stated at all. A selector anchored on the **type name**
`StandaloneRestServer` matches nothing here - the method is not declared on that
type - so such a selector would have returned an empty entry-point set, and a
zero from that mistake would be **indistinguishable** from a zero that is a
genuine capability boundary. Stating the type is what lets a reader tell those two
apart: the entry-point count for this pair is 1 discovered and 1 traversed, so the
selection resolved and the zero route count below is a property of the traversal
rather than of a mis-anchored selector. The same distinction is why the shared
route-surface prefix list is reported as **not** covering this handler type: see
"The shared route surface, and each pair's own" below.

## Whether the bound was reached

The primary bound is `MAX_CALL_DEPTH` = 12 call-graph hops from an
entry point, applied per pair. Every traversal in this query carries an explicit
named bound; none runs unbounded, and no bound is shared between the pairs, so one
pair cannot consume the other's budget.

- pair `pair-one`: `bound_reached` = **true**
  - walk `A-follows-fan-out`: the frontier was still non-empty at depth 12. Expansion budget used 19551 of 200000 per entry point; call sites considered 21476 of 400000 for the pair; routes returned 0 of 64.
  - walk `B-fan-out-recorded`: the frontier was still non-empty at depth 12. Expansion budget used 2350 of 200000 per entry point; call sites considered 4383 of 400000 for the pair; routes returned 0 of 64.
- pair `pair-two`: `bound_reached` = **true**
  - walk `A-follows-fan-out`: the frontier was still non-empty at depth 12. Expansion budget used 10029 of 200000 per entry point; call sites considered 8949 of 400000 for the pair; routes returned 0 of 64.
  - walk `B-fan-out-recorded`: the frontier was still non-empty at depth 12. Expansion budget used 732 of 200000 per entry point; call sites considered 1230 of 400000 for the pair; routes returned 0 of 64.

A depth bound reached with a non-empty frontier says only that the walk stopped
expanding, so on its own it would leave open whether a deeper walk would reach a
sink host. What settles that here is the boundary measurement below rather than the
bound: the hops that break these routes are not CALL edges at all, and no increase
in depth introduces an edge that does not exist.

Every named bound carries a value **and** a reached flag, and the flag is reported
separately for each pair in the declared pair order. No flag is aggregated across
the two.

| bound | value | what it bounds | reached, `pair-one` | reached, `pair-two` |
| --- | --- | --- | --- | --- |
| MAX_CALL_DEPTH | 12 | maximum call-graph hops walked from an entry point, applied per pair | **true** | **true** |
| MAX_ROUTES_PER_PAIR | 64 | maximum distinct routes retained per pair; never a shared budget | false | false |
| MAX_EXPANSIONS_PER_ENTRY | 200000 | the per-entry-point step cap, counted in method expansions | false | false |
| MAX_STEPS_PER_PAIR | 400000 | the per-pair step cap across that pair's walks, counted in call sites considered | false | false |
| MAX_TOTAL_RETURNS | 256 | the total-returns cap across every record kind; a query-level cap | false | false |
| MAX_ENTRY_POINTS_PER_PAIR | 16 | maximum entry points traversed per pair; the remainder counted as truncated | false | false |
| MAX_CALL_SCAN | 200000 | cap on the indexed call-name sweeps that find the sink and message call sites | false | false |
| FANOUT_CALLEE_THRESHOLD | 32 | a threshold, not a cap: a call site with a wider resolved callee set is recorded as fan-out | **true** | **true** |

Two of those rows need their reading stated, or the flags mislead.
`FANOUT_CALLEE_THRESHOLD` is a **threshold rather than a cap**, so "reached" there
means *exceeded* - fan-out sites were encountered - and it is **not** a truncation
of either traversal. `MAX_TOTAL_RETURNS` is a **query-level** cap, so its entry is
the same measurement appearing in both pairs' columns: one measurement cited
twice, never two measurements.

The measurement each flag was read from, per pair:

- pair `pair-one`
  - `MAX_CALL_DEPTH` **reached**: `depth_bound_reached` is true across this pair's walks and the deepest walk used 12 of 12 hops
  - `MAX_ROUTES_PER_PAIR` not reached: `route_cap_reached` false in every walk, routes returned `A-follows-fan-out`=0 and `B-fan-out-recorded`=0 against a per-pair cap of 64
  - `MAX_EXPANSIONS_PER_ENTRY` not reached: `expansion_budget_exhausted` false in every walk, the highest method-expansion count 19551 of 200000
  - `MAX_STEPS_PER_PAIR` not reached: `step_cap_reached` false in every walk, the highest call-site count 21476 of 400000
  - `MAX_TOTAL_RETURNS` not reached: 6 records emitted by the query against a cap of 256
  - `MAX_ENTRY_POINTS_PER_PAIR` not reached: 2 discovered, 2 traversed, 0 truncated, against a per-pair cap of 16
  - `MAX_CALL_SCAN` not reached: 1234 calls named `start` scanned of 200000, sweep truncated false
  - `FANOUT_CALLEE_THRESHOLD` exceeded: fan-out sites encountered `A-follows-fan-out`=48, `B-fan-out-recorded`=31, a site counting as fan-out when its resolved callee set exceeds 32 distinct methods
- pair `pair-two`
  - `MAX_CALL_DEPTH` **reached**: `depth_bound_reached` is true across this pair's walks and the deepest walk used 12 of 12 hops
  - `MAX_ROUTES_PER_PAIR` not reached: `route_cap_reached` false in every walk, routes returned `A-follows-fan-out`=0 and `B-fan-out-recorded`=0 against a per-pair cap of 64
  - `MAX_EXPANSIONS_PER_ENTRY` not reached: `expansion_budget_exhausted` false in every walk, the highest method-expansion count 10029 of 200000
  - `MAX_STEPS_PER_PAIR` not reached: `step_cap_reached` false in every walk, the highest call-site count 8949 of 400000
  - `MAX_TOTAL_RETURNS` not reached: the same query-level measurement - 6 records against a cap of 256
  - `MAX_ENTRY_POINTS_PER_PAIR` not reached: 1 discovered, 1 traversed, 0 truncated, against a per-pair cap of 16
  - `MAX_CALL_SCAN` not reached: 1234 calls named `start` scanned of 200000, sweep truncated false
  - `FANOUT_CALLEE_THRESHOLD` exceeded: fan-out sites encountered `A-follows-fan-out`=25, `B-fan-out-recorded`=12, on the same 32-callee threshold

The per-pair caps are per pair by design: one pair filling a shared budget would
silently truncate the other, and a truncated traversal that passed for a complete
one is the one failure mode a bound is there to make visible.

## The chain the traversal can follow, for context

Every line below is the **pinned tree's**, re-verified against `$SPARK_SRC` at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`. The point of stating it is that
neither pair's empty result is the traversal failing to move at all: inside
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala` the call chain is
ordinary static calls, and a call-graph formulation follows it without difficulty.
Both pairs traverse this same stretch - pair two reaches it only after the
message-send hop its own handler opens with.

`:409` `override def receiveAndReply` -> `:410` `case RequestSubmitDriver(description)`
-> `:411` the recovery-state check -> `:415` the branch taken when the state is
`ALIVE` -> `:417` `val driver = createDriver(description)` (definition at `:1356`)
and `:421` `schedule()` -> `:944` `private def schedule()` -> `:967` and `:986`
`launchDriver(worker, driver)` (each inside a `canLaunchDriver` check, that method
declared at `:923` and called at `:964` and `:983`) -> `:1363`
`private def launchDriver` -> `:1367` the message send.

A second path arrives at the same place: `:1121` `private def relaunchDriver`
reaches the same `createDriver` at `:1130`.

So **at least three call hops** separate that handler from the message send -
`receiveAndReply` to `schedule` to `launchDriver` to the send - which is why the
depth bound of 12 is load-bearing rather than decorative for both pairs: a bound
of one or two would have stopped either walk before the send on arithmetic alone,
and the empty results would then have been an artefact of the bound rather than a
measurement of the graph. The walks stop where they do for the reasons below
instead.

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

Two kinds of predicate are in play and they must not be run together. The
**selection** predicates are what the parameterized body applies to find its ends,
and they are exactly the part the pair parameterizes. The **spurious** predicate
set is what classifies a returned route, and it is exactly the part the pair does
*not* parameterize. Both are named below, with the source-level construct each was
derived from.

### The selection predicates the one body applied, per pair

The same traversal body was driven by two parameter sets. No traversal in this
query names a handler or a sink itself: every selector it applies comes out of the
pair it was handed, which is what makes the second instantiation an invocation of
the same code rather than a second query.

| selection predicate | derived from | `pair-one` | `pair-two` |
| --- | --- | --- | --- |
| handler enclosing type, matched exactly | the `class`/`object` the handler method is declared in | `org.apache.spark.deploy.master.Master` (`Master.scala:409` declares the method) | `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet` (declared `StandaloneRestServer.scala:171`) |
| handler method name | the `def` at the pinned handler line | `receiveAndReply` (`Master.scala:409`) | `handleSubmit` (`StandaloneRestServer.scala:268`) |
| handler synthetic type regex | the synthetic class Scala 2.13 emits for a `PartialFunction` literal | `^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$` | `^org\.apache\.spark\.deploy\.rest\.StandaloneSubmitRequestServlet\$\$anonfun\$handleSubmit\$\d+$` |
| handler synthetic method name | the `applyOrElse` member of that synthetic class | `applyOrElse` | `applyOrElse` |
| body witness, used to decide which arm carries the body | a call the handler's own body makes | `createDriver` (called `Master.scala:417`) | `DeployMessages$RequestSubmitDriver.<init>` (constructed `StandaloneRestServer.scala:277`) |
| base declaration to exclude | the abstract or overridden declaration on a supertype | none declared for this pair | `org.apache.spark.deploy.rest.SubmitRequestServlet` |
| sink callee regex | the abstract `def start(): Process` (`DriverRunner.scala:270`) and the JDK method it delegates to | `^(java\.lang\.ProcessBuilder\.start\|org\.apache\.spark\.deploy\.worker\.ProcessBuilderLike\.start).*` | the same |
| sink call name | the call at the pinned sink line | `start` (`DriverRunner.scala:240`) | the same |
| sink host type regex | the types hosting that call - the class and the trait | `^org\.apache\.spark\.deploy\.worker\.(DriverRunner\|ProcessBuilderLike).*` | the same |
| message hop ids | the `case class` message types on the route | `LaunchDriver` | `RequestSubmitDriver`, `LaunchDriver` |
| route surface type prefixes | the types the route passes through | `Master`, `DriverRunner`, `ProcessBuilderLike` | those three plus `StandaloneSubmitRequestServlet` |
| pair label | prose, carried through to every record | "the standalone Master's driver-submission handler to the privileged process launch on the DriverRunner surface" | "the REST submit servlet's handleSubmit to the SAME privileged process launch on the DriverRunner surface" |

The two pairs differ in the handler and in the message hops; they **share** the
sink triple, which is why the sink's own measurements are one measurement cited by
both. The exact literal values, as supplied, are listed again under
parameterizability below so the measure can be checked without reading this table.

### The spurious predicate set: exactly five Boolean methods

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

The selector block in this query's source is **byte-identical** to the
corresponding block of `queries/joern/01-callgraph-unguarded-driver-launch.sc` and
`queries/joern/02-dataflow-unguarded-driver-launch.sc`. It has to be: three spurious
counts are only comparable if the definition of the term is the same text in all
three files.

### The set is exactly five, was not widened, and is identical across both pairs

Two auth-adjacent Boolean methods on the very same anchored type are deliberately
**not** selectors: `isEncryptionEnabled()` at
`core/src/main/scala/org/apache/spark/SecurityManager.scala:280` and
`isSslRpcEnabled()` at `:295`. Neither is an authorization or ACL predicate, and
adding either would change what the word "spurious" counts here.

The point specific to a parameterized query is the second half of that sentence.
**The parameterization varies the handler and the sink, never the predicate set.**
The same five selectors, the same type anchor and the same `_$eq` exclusion were
applied to both pairs; the predicate set is not among the parameters at all. Had
parameterization varied the selector set per pair, the two pairs' spurious counts
would mean different things and could not be read side by side - which is the one
way a parameterization could silently change the measurement rather than the
target. It did not: the set is identical across `pair-one` and `pair-two`, and
byte-identical to the other two queries' as well, which is what keeps all of the
probe's spurious counts comparable with one another.

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

### The over-match hazards the anchor and the exact five exclude

The type anchor alone does not exclude these. Every one is a method **on the
anchored type** whose name the broad pattern reaches, and it is the intersection
with the five exact names that removes each of them. Naming them is what makes the
selector's precision checkable rather than asserted - all lines are the pinned
tree's, in `core/src/main/scala/org/apache/spark/SecurityManager.scala`:

| excluded method | source line at the pin |
| --- | --- |
| `setViewAcls` | 123 |
| `setViewAcls` (the second overload) | 128 |
| `setViewAclsGroups` | 136 |
| `getViewAcls` | 144 |
| `getViewAclsGroups` | 152 |
| `setModifyAcls` | 164 |
| `setModifyAclsGroups` | 173 |
| `getModifyAcls` | 182 |
| `getModifyAclsGroups` | 190 |
| `setAdminAcls` | 202 |
| `setAdminAclsGroups` | 211 |
| `setAcls` | 216 |

Twelve declarations excluded by name, plus the compiler-generated setter
`aclsOn_$eq` excluded by suffix and the getter `aclsOn` excluded as non-predicate
residue. Every one of the fourteen is a setter or a getter over ACL configuration
by its own declaration, and none of them is one of the five names the definition
uses. Counting a call to one of them as "the route passed a predicate" would have
inflated this query's spurious count - on **both** pairs, since the same block
applies to both - with call sites the definition does not cover. That is a
statement about the selector, not about the code it selects over.

### The shared route surface, and each pair's own

The byte-identical block also carries `ROUTE_SURFACE_TYPE_PREFIXES` = `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.rest.StandaloneRestServer`, `org.apache.spark.deploy.worker.DriverRunner`.
Measured here rather than assumed: the handler type(s) `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet` are **not** covered by any of those prefixes, because the type a method is declared in is not always the headline class of the file it lives in.
The shared list is kept exactly as it stands so all three queries' spurious counts
remain comparable, and each pair additionally carries its **own** route surface,
derived from its own handler and sink types, which is what makes that pair's
expected-spurious basis correct. Both counts are published.

## Whether an expected-spurious route was absent

### Pair `pair-one`: `spurious_count` = **0**

No route in the emitted set passed an auth/ACL predicate as defined by these five
named selectors.

**The absence is structural, not a consequence of the query filtering well.**
Measured against the graph: 18 call sites of the five
predicates exist graph-wide, in 18 distinct calling
methods, and **0** of them sit on this
pair's own route surface (`org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`).
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
pair's own route surface (`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`, `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`).
The predicate set exists and is invoked elsewhere in the program; it is not
invoked anywhere on this pair's route, so no route of this pair could have
passed one.

For pair `pair-two` one further selector measurement is worth recording,
because it is what makes that pair's basis structural: in the pinned tree the file
`core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala` is matched by **none** of the
five named selectors and carries no reference to the predicate type at all. Its
only `permission` occurrence is the Apache licence boilerplate at line 14, and a
case-insensitive search additionally returns lines 209, 233 and 251, none of which
is a selector match at all - the matched literal there is `aCl` inside
`extraClassPath` / `driverExtraClassPath`, a substring coincidence rather than a
predicate. That is a statement about which of these selectors match that file, and
nothing more.

### The zero is scoped to the route surfaces, not to the program

An unscoped "zero call sites" claim would simply be false. `aclsEnabled()` **is
invoked** inside the anchored type's own source file, at
`core/src/main/scala/org/apache/spark/SecurityManager.scala:249`, at `:265` and at
`:407` inside the private `isUserInACL` declared at `:402`; and 18 call sites of
the five exist graph-wide across 18 distinct calling methods. The zeros above hold
for the two pairs' own route surfaces and for nothing wider.

The source-level counterpart of that graph measurement was checked directly:
searching all five names across the three route files at the pin -
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala`,
`core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala` and
`core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala` - returns
**nothing in any of the three**. The two measurements agree, and they are two
views of the same fact rather than two facts.

### Reference is not invocation

The route surface does mention the predicate type; every such mention is a
reference of a kind that invokes none of the five. Holding, importing,
constructing or passing a value on is not **invoking** a method on it, which is
why the verb throughout this report is *invoked*:

| pinned location | what it is |
| --- | --- |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:28` | imports `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:53` | declares `val securityMgr: SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:139` | reads the companion constant `SecurityManager.SPARK_AUTH_SECRET_CONF` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1429` | constructs a `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:27` | imports `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:56` | declares `val securityManager: SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:194` | passes `securityManager` on as an argument to the command builder |

`core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala`
contributes no row to that table, because at the pin it carries **no reference to
the predicate type at all** - not an import, not a field, not a constant read.
That is the detail specific to `pair-two`, and it is what makes that pair's basis
structural rather than a property of this query's filtering.

### What the definition does not evaluate

The mechanical definition evaluates **only** those five predicates, and it applies
unchanged to **both** pairs. Any other conditional on either route is outside it
and is **not assessed** by it. Concretely:

- on `pair-one`,
  `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:411`
  `if (state != RecoveryState.ALIVE)` guards the branch that reaches `createDriver`
  at `:417`. It is a recovery-state check, it is not one of the five, and it is
  therefore neither counted as a predicate nor reported as one.
- on `pair-two`, whatever request validation `handleSubmit`'s own
  `requestMessage match` performs - the method declared at
  `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268`,
  its match opening at `:272` - is likewise outside the definition and
  **unassessed** here.

So a spurious count of 0 means exactly and only what the definition says, and it
does **not** mean that either route carries no conditional. Reading it that way
would attribute to this query a claim it does not make - and on `pair-two` in
particular, where the count is also 0, it would attribute to this query a claim
about a submission path that this query neither makes nor is able to make.

### What this section does not say

These are statements about **this query's own output** under **this query's own**
mechanical definition of one word. They are not an assessment of Spark, of any
Spark component, of any Spark configuration or of any submission path, and nothing
here should be read as one. In particular, nothing in this report states or implies
anything about how Spark authorizes any operation: the five selectors above are a
query-side definition used to classify this query's own returns, and where a route
count is zero there are no returns to classify at all.

## Whether this formulation duplicates another query's

`duplicate_formulation` = **partial_duplicate**. A duplicate of query 01's formulation ON PAIR ONE, not a duplicate as a whole, and not a duplicate of query 02 in any instantiation. The scope of the duplication is stated rather than hidden: it is exactly the pair-one instantiation, and it is what makes the parameterized form's second instantiation the part that is new.

The question is live rather than rhetorical, and it is worth saying why. This
query's **`pair-one` is the same handler/sink pair both sibling queries address**,
so two readings are available and they are not the same verdict: *a third
formulation of pair one*, or *a parameterized restatement of query 01's
formulation*. The verdicts below distinguish them explicitly, and they are grounded
in four things measured rather than eyeballed - the **traversal semantics** (which
edge kind is walked and whether a flow engine is loaded), the **source and sink
node sets** (whole methods against parameters and expressions, and the entry-point
resolution each uses), the **deduplicated API-construct lists** compared as a set
difference in both directions, and whether the **route sets coincide**. Those are
the grounds used; nothing here rests on the file names differing.

**`pair-two` is not compared for duplication at all**, because no other query in
the probe addresses that pair. There is nothing to compare it against, and an
absence of comparison is recorded as exactly that rather than as a verdict of
difference.

### Against `01-callgraph-unguarded-driver-launch`: **duplicate_formulation_on_pair_one**

SAID PLAINLY: instantiated on pair one this query IS query 01's formulation restated in parameterized form, and the evidence is measured rather than asserted - the same edge kind (CALL edges only, no data edge and no flow engine), the same entry-point resolution (the synthetic partial-function method together with the source-level method), the same sink constraint, the same bound value 12, the same two walk modes, and an API construct list whose set difference against query 01's published list is empty in BOTH directions. On this run the two also agree on pair one's entry-point set, on its distinct-route count and on the four boundary verdicts after the declared id translation. WHAT IS NOT A DUPLICATE: the query as a whole. It takes the handler/sink pair as a parameter and is invoked on a SECOND pair (pair-two, org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit) that query 01 does not address at all, it measures 5 boundaries on that pair against 4 on pair one, and it models one hop query 01 never reaches - the servlet's own message send, whose producer and consumer ends are measured in stage I. RECONCILED WITH WHAT QUERY 01 PUBLISHED: its envelope records 'duplicate_formulation_on_pair_one' against this query, aggregating to 'partial_duplicate' at its top level, and it states both scopes there too - that as wholes the two are not duplicates, because this query takes the pair as a parameter and covers a second pair it does not address, and that on pair one the two formulations coincide. The verdict published from each side is therefore the SAME verdict at the same scope, which is what the symmetry of this pairwise relation requires: one measurement cited twice rather than two measurements, and a disagreement between the two directions would be a defect rather than a finding. Neither query's returns are added to the other's anywhere: they are reported side by side, per pair, and NEVER SUMMED.

| property | here (measured) | query 01 (transcribed from its envelope) | agree |
| --- | --- | --- | --- |
| API construct count | 28 | 28 | true |
| bound value | 12 | 12 | true |
| pair-one entry points | 2 | 2 | true |
| pair-one distinct routes | 0 | 0 | true |
| boundaries not crossed | B1-rpc, B2-thread, B4-partial-function | B1-rpc, B2-thread, B4-partial-function | true |

The sibling's figures are **transcribed** from its published envelope, never
re-measured here, and they were measured against the graph of its own run. The
boundary ids are translated by the mapping declared in the query source: `B-interface` -> `B3-interface`, `B-partial-function-pair-one` -> `B4-partial-function`, `B-rpc-LaunchDriver` -> `B1-rpc`, `B-thread` -> `B2-thread`.

### Against `02-dataflow-unguarded-driver-launch`: **not_duplicate**

A different formulation over different edges and different nodes. Query 02 traverses reaching-definition (data) edges through the OSS dataflow layer and selects PARAMETER and EXPRESSION nodes as its ends; this query traverses CALL edges and selects whole METHODS, and it loads no flow engine at all. Auditable corroboration, computed here as a set difference against query 02's published list rather than eyeballed: 18 of query 02's 42 API constructs do not appear in this query's list (AstNode.code, AstNode.label, AstNode.lineNumber, Call.argument, Call.receiver, CfgNode.method, EngineConfig.maxCallDepth, EngineContext.config, EngineContext.copy, EngineContext.semantics, Method.call, Method.parameter, MethodParameterIn.index, MethodParameterIn.method, MethodParameterIn.name, MethodParameterIn.typeFullName, Path.elements, Traversal.reachableByFlows), and 4 of this query's do not appear in query 02's (Call.code, Call.order, Method.callOut, NoResolve.getCalledMethodsAsTraversal). The two also carry different bounds - this query's bound value is 12 call-graph hops, query 02 published 6 for its own flow-call depth - so the two numbers are not even the same kind of quantity. Their returns are likewise never summed.

### The relation is symmetric, and both directions were checked

A duplicate-formulation verdict is a **symmetric pairwise relation**: the verdict
stated here against a sibling is the same verdict that sibling states against this
query, at the same scope. Both directions were read rather than assumed.

| relation | published here | published by the sibling | agree |
| --- | --- | --- | --- |
| 03 ↔ `01-callgraph-unguarded-driver-launch` | `duplicate_formulation_on_pair_one` | `duplicate_formulation_on_pair_one`, aggregating to `partial_duplicate` at its top level | true |
| 03 ↔ `02-dataflow-unguarded-driver-launch` | `not_duplicate` | `not_duplicate`, aggregating to `not_duplicate` at its top level | true |

Both scopes - the pair-one duplication and the whole-query difference - are named
in both envelopes, so neither reads as a contradiction of the other. One
measurement cited twice; a disagreement between the two directions would be a
defect rather than a finding. And no query's returns are added to another's
anywhere: the figures sit side by side, per pair, and are never summed.

## The three effort measures

1. **Query revisions committed: 1.** Convention: commits touching queries/joern/03-parameterized-handler-sink-pairs.sc from its first appearance to the end of the probe. This run introduces the file in a single commit.
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

   Constructs used here that query 01 does not publish: none. Constructs used here that query 02 does not publish: `Call.code`, `Call.order`, `Method.callOut`, `NoResolve.getCalledMethodsAsTraversal`.

3. **Parameterizability: passed.** This file owns the measure.
   It passes ONLY where the parameterized query is actually invoked on the second named pair and that invocation's result is captured in this query's result files and console log; an empty result from a real invocation satisfies it, a skipped invocation does not, and a parameter list that merely exists does not.

   - first pair `pair-one`: invoked; entry points traversed 2 of 2; distinct routes 0; spurious 0; boundaries measured or cited 4
   - second pair `pair-two` (`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit` at `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268` to the launch at `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:240`, both at the pin): invoked; entry points traversed 1 of 1; walks run A-follows-fan-out and B-fan-out-recorded; call sites considered A-follows-fan-out=8949, B-fan-out-recorded=1230; distinct routes 0; spurious 0; boundaries measured or cited 5 (B-rpc-RequestSubmitDriver, B-rpc-LaunchDriver, B-thread, B-interface, B-partial-function-pair-two)

   **The exact parameter values supplied**, so a reader can see one query body
   driven by two different inputs rather than two queries written. Every literal
   below was handed to the same traversal; the query source itself names no
   handler and no sink.

   `pair-one`:

   - `handler_type` = `org.apache.spark.deploy.master.Master`
   - `handler_method` = `receiveAndReply`
   - `handler_synthetic_type_regex` = `^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$`
   - `handler_synthetic_method` = `applyOrElse`
   - `handler_body_witness` = `createDriver`
   - `handler_base_type` = none declared for this pair
   - `handler_source_file_at_the_pin` = `core/src/main/scala/org/apache/spark/deploy/master/Master.scala`, `handler_source_line_at_the_pin` = 409
   - `sink_callee_regex` = `^(java\.lang\.ProcessBuilder\.start|org\.apache\.spark\.deploy\.worker\.ProcessBuilderLike\.start).*`
   - `sink_call_name` = `start`
   - `sink_host_type_regex` = `^org\.apache\.spark\.deploy\.worker\.(DriverRunner|ProcessBuilderLike).*`
   - `sink_source_file_at_the_pin` = `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala`, `sink_source_line_at_the_pin` = 240
   - `message_hop_ids` = `LaunchDriver`
   - `route_surface_type_prefixes` = `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`
   - `pair_label` = "the standalone Master's driver-submission handler to the privileged process launch on the DriverRunner surface"

   `pair-two`:

   - `handler_type` = `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`
   - `handler_method` = `handleSubmit`
   - `handler_synthetic_type_regex` = `^org\.apache\.spark\.deploy\.rest\.StandaloneSubmitRequestServlet\$\$anonfun\$handleSubmit\$\d+$`
   - `handler_synthetic_method` = `applyOrElse`
   - `handler_body_witness` = `DeployMessages$RequestSubmitDriver.<init>`
   - `handler_base_type` = `org.apache.spark.deploy.rest.SubmitRequestServlet`
   - `handler_source_file_at_the_pin` = `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala`, `handler_source_line_at_the_pin` = 268
   - `sink_callee_regex`, `sink_call_name`, `sink_host_type_regex`, `sink_source_file_at_the_pin` and `sink_source_line_at_the_pin`: the **same** five values as `pair-one`
   - `message_hop_ids` = `RequestSubmitDriver`, `LaunchDriver`
   - `route_surface_type_prefixes` = `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`, `org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`
   - `pair_label` = "the REST submit servlet's handleSubmit to the SAME privileged process launch on the DriverRunner surface"

   What differs between the two parameter sets is the handler, its base
   declaration, its body witness and its message hops; what is identical is the
   sink triple, and - stated again because it is the property that keeps the two
   spurious counts readable side by side - the five-selector predicate set, which
   is **not a parameter at all**.

   The verdict rests on the second pair's invocation having actually run in this
   same run, and on its result being captured here, in the envelope and in the
   console log - not on the existence of a parameter. Its selection, its walk
   counters, its boundary measurements, its distinct-route count and its spurious
   count are all published above. An empty result from a real invocation
   satisfies the measure; a skipped invocation would not, and a malformed pair
   aborts the run rather than being passed over.

   **The verdict, stated explicitly: passed** - on the basis of the captured
   second-pair invocation recorded in
   `queries/joern/results/03-parameterized-handler-sink-pairs.json`, whose
   `parameterizability` block names both pairs, both invocations and both
   outcomes, and whose `pairs` array carries `pair-two`'s own selection, walk
   counters and boundary measurements.

   **A zero-record outcome on `pair-two` still satisfies this measure**, and it
   did not weaken this verdict. The measure asks whether the query is
   *parameterizable* - whether the second named pair was really supplied to the
   same body and its result captured - not whether that pair is *connected* over
   this graph by this formulation. The two are reported separately for exactly
   that reason, and neither should be read as the other in either direction: a
   non-empty result would not have made the measure pass any harder, and the empty
   one does not make it fail. `pair-two`'s zero distinct routes is a capability
   finding about the traversal, published in its own per-pair object above.

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
- resolved target: a host-shared read-only file outside the repository root, reached by following the symlink, **548118435** bytes, sha256 `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53`
- the link itself measures 33 bytes; that figure is recorded only to be
  discarded, because measuring the link rather than its target is the mistake this
  check exists to avoid
- no absolute host path appears in this report or in the envelope: the clone root is
  a property of the checkout rather than of the measurement, and the size-and-digest
  pair above is what the identity comparison turns on. The literals are in
  `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`, a console stream not held to byte-identity
- record of account: `a provisioning record outside the repository root, named by $HARNESS_CPG_RECORD` (source: HARNESS_CPG_RECORD), which
  states bytes 548118435 and sha256 `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53` - re-verified immediately
  before the load, and a mismatch would have halted the run
- repo-relative record `harness/artifacts/logs/cpg-frontend.log` states bytes 541255894 and
  sha256 `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`; agrees with the graph loaded: **false**
- divergence: the repo-relative record harness/artifacts/logs/cpg-frontend.log states bytes=541255894 sha256=26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc, which is NOT the graph on this host (bytes=548118435 sha256=f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53). That record is a committed deliverable describing the graph of the provisioning that wrote it; the record of account for THIS load is a provisioning record outside the repository root, named by $HARNESS_CPG_RECORD, the frontend's own write-time record for the graph actually loaded, and the load was verified against it. Both pairs are recorded with their provenance and neither is discarded
- the AAP-named path `harness/cpg/spark.cpg`: same file (equal resolved target)

## Determinism of this report

An unchanged query source over an unchanged graph must produce a byte-identical
file, so this document carries no wall-clock timestamp, no elapsed time, no
process identifier, no host name, no host-specific scratch or workspace path and
no absolute host path. The only paths it names are repository-relative, relative to
the `$SPARK_SRC` root, or environment-variable names. Those excluded quantities are
real and are not being hidden - they live in
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`, a console
stream deliberately **not** held to byte-identity.

The requirement specific to a parameterized query is **pair order**, and it is
fixed: `pair-one` first, then `pair-two`, everywhere in this report, in the
envelope's per-pair objects and in the record grouping. The order is not incidental
- the pairs are declared as a `List` in the query source and are selected, walked,
classified, recorded and reported by index in that order, and no stage iterates a
map or a set of pairs. A pair order taken from an unspecified iteration would
reorder the per-pair sections between two runs and break byte-identity while
changing no measurement, which is precisely the kind of difference a reader would
have no way to attribute.

## Reproducing this

```
cd <a scratch directory outside the repository>
HARNESS_REPO_ROOT=<repo> JAVA_HOME="$JAVA_HOME_21" \
  JAVA_TOOL_OPTIONS="-Xmx64g" SL_LOGGING_LEVEL=WARN \
  joern --script <repo>/queries/joern/03-parameterized-handler-sink-pairs.sc -J-Xmx64g < /dev/null
```

Both pairs are declared as named constants in the query source and both are
invoked by that one command, so no per-pair parameter has to be passed on the
command line and the second pair's invocation is reproducible from this record
alone. Where the record of account above is not the repo-relative one, the
variable `HARNESS_CPG_RECORD` names it, and its value is the path printed above.

`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so
`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs
at. The query measures the heap it received and stops below the floor: raising a
heap is permitted and reported, lowering one is not.

