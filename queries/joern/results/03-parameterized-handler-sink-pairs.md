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
| Envelope | `queries/joern/results/03-parameterized-handler-sink-pairs.json` |
| Console log | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21 |
| Heap actually used | 68719476736 bytes (floor 68719476736) |
| Graph | 548118435 bytes, sha256 `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53` |
| Graph identity re-verified before the load | yes, against `cpg-frontend.log` (source: HARNESS_CPG_RECORD) |
| Graph methods / typeDecls / files | 1399866 / 119920 / 45037 |
| Compile status | compiled |
| Run status | completed |
| Pairs declared / invoked | 2 / 2 |
| Pair iteration order | `pair-one`, `pair-two` |
| Records returned | 6 (6 boundary measurement(s) plus per-pair route records) |
| Parameterizability | **passed** |
| Duplicate formulation | **partial_duplicate** |

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

| bound | value |
| --- | --- |
| MAX_CALL_DEPTH | 12 |
| MAX_ROUTES_PER_PAIR | 64 |
| MAX_EXPANSIONS_PER_ENTRY | 200000 |
| MAX_STEPS_PER_PAIR | 400000 |
| MAX_TOTAL_RETURNS | 256 |
| MAX_ENTRY_POINTS_PER_PAIR | 16 |
| MAX_CALL_SCAN | 200000 |
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

The selector block in this query's source is **byte-identical** to the
corresponding block of `queries/joern/01-callgraph-unguarded-driver-launch.sc` and
`queries/joern/02-dataflow-unguarded-driver-launch.sc`. It has to be: three spurious
counts are only comparable if the definition of the term is the same text in all
three files.

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

`duplicate_formulation` = **partial_duplicate**. A duplicate of query 01's formulation ON PAIR ONE, not a duplicate as a whole, and not a duplicate of query 02 in any instantiation. The scope of the duplication is stated rather than hidden: it is exactly the pair-one instantiation, and it is what makes the parameterized form's second instantiation the part that is new.

### Against `01-callgraph-unguarded-driver-launch`: **duplicate_formulation_on_pair_one**

SAID PLAINLY: instantiated on pair one this query IS query 01's formulation restated in parameterized form, and the evidence is measured rather than asserted - the same edge kind (CALL edges only, no data edge and no flow engine), the same entry-point resolution (the synthetic partial-function method together with the source-level method), the same sink constraint, the same bound value 12, the same two walk modes, and an API construct list whose set difference against query 01's published list is empty in BOTH directions. On this run the two also agree on pair one's entry-point set, on its distinct-route count and on the four boundary verdicts after the declared id translation. WHAT IS NOT A DUPLICATE: the query as a whole. It takes the handler/sink pair as a parameter and is invoked on a SECOND pair (pair-two, org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit) that query 01 does not address at all, it measures 5 boundaries on that pair against 4 on pair one, and it models one hop query 01 never reaches - the servlet's own message send, whose producer and consumer ends are measured in stage I. RECONCILED WITH WHAT QUERY 01 PUBLISHES: its envelope records the same scoped verdict against this query - 'duplicate_formulation_on_pair_one', aggregating to 'partial_duplicate' at the top level - and states both scopes, that as wholes the two are not duplicates because this query covers a second pair and a different target set, and that on pair one the two formulations coincide. The pairwise relation is therefore symmetric, which is what query 01's envelope requires of it: the verdict each states against the other is the same verdict, one measurement cited twice rather than two measurements. The two scopes are the same finding at two granularities rather than a disagreement, and each is named in both directions so neither reads as a contradiction of the other. Neither query's returns are added to the other's anywhere: they are reported side by side, per pair, and NEVER SUMMED.

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

- named path `/tmp/blitzy/blitzy-spark/blitzy-f38258d3-f87d-44f5-bedc-af512c69e0ab-w-002_1060ea/harness/cpg/spark.cpg`, a symlink to `/opt/blitzy-harness/cpg/spark.cpg`
- resolved target `/opt/blitzy-harness/cpg/spark.cpg`, **548118435** bytes, sha256 `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53`
- the link itself measures 33 bytes; that figure is recorded only to be
  discarded, because measuring the link rather than its target is the mistake this
  check exists to avoid
- record of account: `/opt/blitzy-harness/provision-log/cpg-frontend.log` (source: HARNESS_CPG_RECORD), which
  states bytes 548118435 and sha256 `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53` - re-verified immediately
  before the load, and a mismatch would have halted the run
- repo-relative record `harness/artifacts/logs/cpg-frontend.log` states bytes 541255894 and
  sha256 `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`; agrees with the graph loaded: **false**
- divergence: the repo-relative record harness/artifacts/logs/cpg-frontend.log states bytes=541255894 sha256=26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc, which is NOT the graph on this host (bytes=548118435 sha256=f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53). That record is a committed deliverable describing the graph of the provisioning that wrote it; the record of account for THIS load is /opt/blitzy-harness/provision-log/cpg-frontend.log, the frontend's own write-time record for the graph actually loaded, and the load was verified against it. Both pairs are recorded with their provenance and neither is discarded
- the AAP-named path `harness/cpg/spark.cpg`: same file (equal resolved target)

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

