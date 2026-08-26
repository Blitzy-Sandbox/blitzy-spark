# Joern capability probe 02-dataflow-unguarded-driver-launch

Bounded **dataflow** from the Spark standalone Master's driver-submission handler
to the privileged process launch hosted on the `DriverRunner` surface, over the
code-property graph built from the pinned tree's bytecode. This is the **same
handler/sink pair as `01-callgraph-unguarded-driver-launch`**, addressed by a
**different formulation**: reaching-definition edges rather than call edges.

This report is **observational**. It judges no finding - not real, not important,
not a false positive, not a duplicate - and makes no comparison between tools. It
contributes no row to `oss-scan-results/findings.json` and writes nothing into
`harness/artifacts/raw/`. This probe tree is Joern's deliberate **second**
appearance in the run - the Stage 3 runner is the first - and folding either
appearance into the other's numbers would corrupt both that tool's count and the
dataset total, which is why nothing here becomes a dataset row.

The slug `02-dataflow-unguarded-driver-launch` is the **identifier** the plan
assigns this query. It names the question the query was written to ask - whether a
dataflow formulation can join this handler to this sink, and whether any route it
returns passes one of five named predicates first. It is not a finding, and
nothing in this report should be read as an assessment of Spark, of any Spark
component or of any Spark configuration.

| | |
| --- | --- |
| Query source | `queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| Envelope | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` |
| Console log | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21, the required major (JVM `21.0.12.1+1-LTS`) |
| Heap actually used | 68719476736 bytes = 64 GiB (floor 68719476736 = 64 GiB; at the floor, not above it) |
| Heap-bound JVM position | one of 4 - the frontend build, the `importCpg` verification load, the Stage 3 Joern runner, then this probe |
| Graph | `$HARNESS_CPG` (repository-relative `harness/cpg/spark.cpg`), symlink-followed: 541255894 bytes, sha256 `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` |
| Graph identity re-verified before the load | yes, against `harness/artifacts/logs/cpg-frontend.log`, which owns that pair |
| Graph methods / typeDecls / files | 1397339 / 119691 / 45037 |
| Flow engine semantics | `io.joern.dataflowengineoss.semanticsloader.FullNameSemantics` |
| Compile status | compiled |
| Run status | completed |
| Records returned | 8 (4 boundary, 0 route, 2 boundary-flow, 2 liveness-control-flow) |
| Distinct routes | 0 |
| Spurious routes | 0 |
| Dataflow layer live on this sink | true, measured by the control arm rather than assumed |
| Duplicate formulation | not_duplicate, against both other queries |

The query reached the graph through **`importCpg` and nothing else**. That is a
textual property of the committed sources as well as a behavioural one about this
run: the alternative loader - the one that compiles source afresh and, on Joern's
own documented behaviour, spawns a second JVM at the same heap - is invoked in
**none** of the three committed query sources under `queries/joern/`, and the
absence was checked by searching those files rather than inferred from what this
run happened to do.

**This report measures nothing.** Every figure in it is **read from**
`queries/joern/results/02-dataflow-unguarded-driver-launch.json`, which in turn
cites the run that measured them. Nothing here is a second measurement: where a
count appears both here and in that envelope it is one measurement cited twice,
and if the two ever disagreed the envelope would be right and this file wrong.
Source **line numbers** are a different kind of fact - they are quoted from the
pinned tree at `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` and were each
re-verified there.

## The result

**Distinct routes: 0.** A route identity is the triple
(source group, sink group, element signature), the signature being the ordered
sequence of `LABEL@enclosing-method#graph-line` over the flow's elements. Two
returns with equal triples are **one** route however many traversal orders
produced them, and the route-bearing arms are deduplicated against each other on
that triple rather than added together.

Routes are **never summed**: not the arms with each other, not this query's
routes with `01-callgraph-unguarded-driver-launch`'s returns despite the pair the
two share, and not with `03-parameterized-handler-sink-pairs`' per-pair figures.
Two formulations of one pair are two results. A reader who adds them together
gets a number that means nothing.

No flow from a source node to a sink node was returned within the stated bounds.
**A zero result here is a finding, not a failure**: it is a capability finding
about what this formulation can express over this graph, and it is reported as
measured. The bound was not loosened, removed or re-run unbounded to produce a
non-empty result, the query was not widened, no arm was added to obtain a route,
and no flow was manufactured. It is not a statement about Spark, about any Spark
component or about any configuration.

Two things make the zero interpretable rather than merely empty:

1. the **engine-liveness control** returned a flow, so the zero is attributable
   to the route rather than to an engine with no reaching-definition edges to
   walk;
2. the **boundaries** below are each measured individually, and every one that is
   not crossed by a data flow is a named reason.

### The engine-liveness control

`dataflow_layer_live_on_this_sink` = **true**. A zero from a cross-boundary arm
means either that the pair is not joined by data or that the engine had no edges
to walk, and the zero alone cannot tell those apart. The control asks for a flow
that must exist if the layer is live: from
`org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry`'s own formal
parameter to the launch call in that same method, which that parameter is the
receiver of. It is intraprocedural by construction.

- source selection: formal parameters of
  `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry`, with `this`
  excluded; 1 source group, 3 source nodes, 0 truncated
- 1 sink group, 2 sink nodes, 0 truncated; 1 evaluation at call depth 2
- flows found 2, retained 2; no cap registered

The control's flows are **not routes**. They are reported under their own field,
`counted_as_routes` is false, and they are never counted among the routes above.
Because the control is non-empty, a zero from a cross-boundary arm is a statement
about the route and not about the engine.

## Whether the bound was reached

`bound_reached` = **false**. The primary bound is `MAX_FLOW_CALL_DEPTH` = **6**,
the engine's `EngineConfig.maxCallDepth`: the number of call boundaries the
backward search may expand while looking for a source. It exceeds the five method
boundaries the documented route crosses, so a flow absent within it is not an
artefact of a short bound. Every traversal in this query carries an explicit
named bound; none runs unbounded.

These are **this query's own dataflow bounds**, declared as named `val`s in its
own source; no inline literal governs behaviour. They are not
`01-callgraph-unguarded-driver-launch`'s call-graph constants, which are
different quantities with different names - that query's primary bound is a
call-graph depth of 12, and reporting one query's constants under the other's
name would make both uninterpretable.

Two kinds of bound, reported separately because only one of them is observable:

- **Observable caps** (`observable_bound_reached` = false): the per-source step
  cap, the per-pair flow cap, the flow-length cap, the code-length cap and the
  source, sink and entry-point truncation counters are all counted by this
  query's own evaluator, so whether each bit was set is measured rather than
  inferred.
- **The engine's internal call-depth bound is not observable**: the engine
  reports no truncation flag when it stops expanding callers, so no counter of
  this query's can answer it directly. Rather than guess, ARM 1 is run **twice** -
  at depth 2 and at depth 6 - and the results compared. This limitation is stated
  rather than papered over.

The two-depth comparison: `shallow_depth` 2 retained 0 flows, `primary_depth` 6
retained 0 flows, `results_differ_across_the_two_depths` = **false**. Equal
results across the two depths is evidence that the outcome does not depend on the
call-depth bound across that range; a difference would have been evidence that it
does.

Every named bound is reported with its value **and** whether it was reached, so
no bound is left as a value nobody checked:

| bound | value | reached | on what basis |
| --- | --- | --- | --- |
| MAX_FLOW_CALL_DEPTH | 6 | no, as far as this query can observe | the engine reports no truncation flag for its internal call-depth bound; what is measured instead is the two-depth comparison above - 0 flows retained at depth 2 and 0 at depth 6, `results_differ_across_the_two_depths` false |
| MAX_FLOW_CALL_DEPTH_SHALLOW | 2 | no, on the same engine limitation | the shallow depth exists precisely so that sensitivity to the bound is measured rather than assumed |
| MAX_BOUNDARY_FLOW_CALL_DEPTH | 2 | no, on the same engine limitation | what this depth does establish is that the engine was live at it - the control ran at this depth and retained 2 flows - so a zero from a boundary arm at the same depth is not a zero from an inert layer |
| MAX_FLOW_LENGTH | 64 | no | `flow_length_cap_reached` is false in all three route-bearing arms and in the control; the longest record retained carries 17 elements of 64, and `elements_truncated` is false on every record |
| MAX_FLOWS_PER_PAIR | 8 | no | `per_pair_cap_reached` is false in all three arms and in the control, the largest retention for one pair being 2 of 8 |
| MAX_STEPS_PER_SOURCE | 8 | no | `step_cap_reached` is false in all three arms and in the control, which recorded 4, 4, 2 and 1 evaluations |
| MAX_TOTAL_RETURNS | 256 | no | 8 records of 256, and `total_returns_cap_reached` is false |
| MAX_SOURCE_NODES | 64 | no | `source_nodes_truncated` is 0 in all three arms and in the control, the largest source-node set being 3 of 64 |
| MAX_SINK_NODES | 64 | no | `sink_nodes_truncated` is 0 in all three arms and in the control, the largest sink-node set being 4 of 64 |
| MAX_ENTRY_POINTS | 16 | no | 2 entry points discovered, 2 traversed, 0 truncated, against a cap of 16 |
| MAX_CALL_SCAN | 200000 | **not established** | named as such rather than filled with a plausible "no". The two indexed sweeps' truncation flags went to a console stream that is not preserved on this branch, so the envelope carries `null` and states why. A value nothing preserved can be read from is named rather than guessed |
| MAX_CODE_CHARS | 160 | no | the longest of the 35 element code strings retained is 57 characters of 160, and none carries the ellipsis the cap appends when it truncates |

Nothing this query returned was trimmed by an observable cap, so no result below
is a truncated view of a larger one. The one bound whose flag cannot be read is
named rather than assumed clear.

| arm | depth | source groups | source nodes | sink groups | sink nodes | evaluations | flows found | flows retained | step cap | per-pair cap | length cap |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ARM1-handler-parameters-shallow` | 2 | 2 of 2, 0 truncated | 3, 0 truncated | 2 | 4, 0 truncated | 4 | 0 | 0 | false | false | false |
| `ARM1-handler-parameters` | 6 | 2 of 2, 0 truncated | 3, 0 truncated | 2 | 4, 0 truncated | 4 | 0 | 0 | false | false | false |
| `ARM2-unapply-recovered-payload` | 6 | 1 of 1, 0 truncated | 1, 0 truncated | 2 | 4, 0 truncated | 2 | 0 | 0 | false | false | false |
| `CONTROL-intraprocedural-liveness` | 2 | 1 of 1, 0 truncated | 3, 0 truncated | 1 | 2, 0 truncated | 1 | 2 | 2 | false | false | false |

**Entry points: discovered 2, traversed 2, truncated 0.** The two counters exist
so that a sweep cannot run unbounded and so that a trimmed traversal cannot pass
for a complete one. A truncated count above zero is a measured property of the
traversal and would be reported as such rather than hidden; it is zero here
because 2 entry points were discovered against a cap of 16, so every entry point
discovered was traversed and none was dropped.

## Entry points, the source selection, and the unapply

`receiveAndReply` returns a `PartialFunction`, so its body compiles into a
synthetic class and the handler's formal parameter in the graph belongs to that
class's `applyOrElse`, not to a method named `receiveAndReply`. The selector
takes the synthetic `applyOrElse` on every type matching
`^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$`
**together with** the source-level `receiveAndReply`, so the difference between
them is measured rather than assumed:

- `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`

The formal parameter is `Any`-typed, and the `DriverDescription` payload is
recovered by the pattern match at
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala:410`
`case RequestSubmitDriver(description)` - an **unapply**, which in bytecode is a
type test, a cast and the case class's own `driverDescription` accessor rather
than an assignment. Selecting one side only would leave the flow count
uninterpretable, so **both** are selected, as two arms reported separately:

- **ARM 1** - every formal parameter of the entry methods, with the implicit
  receiver excluded because it carries the enclosing instance rather than the
  message. The two excluded receiver parameters are
  `...Master$$anonfun$receiveAndReply$1.applyOrElse...#0:this` and
  `...Master.receiveAndReply...#0:this`. The `Any`-typed parameter is
  `...applyOrElse...#1:x1:java.lang.Object`, identified by its **erased bytecode
  type** `java.lang.Object` rather than by position.
- **ARM 2** - the unapply-recovered payload: call sites of
  `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription`
  inside the entry methods, which resolve to one node, at graph line 410.

ARM 1 is additionally run at two call depths, which is why three route-bearing
arms appear in the table above for two source selections.

## The sink

- `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)`
  calls `org.apache.spark.deploy.worker.ProcessBuilderLike.start:java.lang.Process()`
  at graph line 240
- `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
  calls `java.lang.ProcessBuilder.start:java.lang.Process()` at graph line 276

The sink **node** set is the launch call together with its receiver and its
arguments - 2 launch call nodes, 2 receiver nodes and 2 argument nodes, 4
distinct after de-duplication, 0 truncated. A flow that reaches the value being
launched ends at the launch call, its receiver or one of its arguments, and
taking only the call node would miss a flow into the receiver.

## The chain the traversal can follow, for context

Every line below is the **pinned tree's**, re-verified against `$SPARK_SRC` at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`. Stating it is what shows that the
empty result is not the traversal failing to move at all: inside
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala` the chain is
ordinary static calls, and the shared route-surface anchors are cited from
`queries/joern/results/01-callgraph-unguarded-driver-launch.json`, whose
`route_surface` owns them, rather than re-established here.

`:409` `override def receiveAndReply` -> `:410`
`case RequestSubmitDriver(description)` -> `:411` the recovery-state check ->
`:415` the branch taken when the state is `ALIVE` -> `:417`
`val driver = createDriver(description)` (definition at `:1356`) and `:421`
`schedule()` -> `:944` `private def schedule()` -> `:967` and `:986`
`launchDriver(worker, driver)` (each inside a `canLaunchDriver` check, that method
declared at `:923` and called at `:964` and `:983`) -> `:1363`
`private def launchDriver` -> `:1367` the message send. A second path arrives at
the same place: `:1121` `private def relaunchDriver` reaches the same
`createDriver` at `:1130`.

The call chain being followable is exactly why the bound is not the explanation
here. A dataflow formulation does not walk that chain: it walks
reaching-definition edges, and those are joined or not joined independently of
how many call hops separate two methods. Where the two part company is the
subject of the boundaries below.

## The boundaries this formulation could not cross

**A zero result is a finding, not a failure.** The pair is neither
call-graph-connected nor dataflow-connected, and the boundaries are where that
lands. Four hops are modelled and measured, each with its own bounded flow
traversal and each emitted as a boundary record; a fifth boundary is specific to
this formulation and is reported after them, because it is a property of the
source selection rather than a hop between two ends. Every one is reported with
its **hop** and its **reason**. None was worked around, no query was loosened to
manufacture a flow, and `crossed by a data flow` states what the measurement gave
the hop rather than what was expected of it.

Each `measured` figure below is read from the envelope's boundary record for that
hop. Graph line numbers are the **graph's own**, from the bytecode line-number
table; source anchors are the pinned tree's.

### B1-rpc - crossed by a data flow: **false** (0 flows found)

- **hop**: `RpcEndpointRef.send` of
  `org.apache.spark.deploy.DeployMessages$LaunchDriver`, Master to Worker
- **from**: `org.apache.spark.deploy.master.Master.launchDriver:void(org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.master.DriverInfo)`
- **to**: `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a message send carries no data edge. The value is serialized out of
  one process and deserialized into another, so the sender's argument and the
  receiver's accessor result are two unrelated definitions as far as
  reaching-definition edges are concerned
- **modelling**: modelled explicitly by pairing on the **message type** - the
  arguments of call sites of
  `org.apache.spark.deploy.DeployMessages$LaunchDriver.<init>` are the producer end
  and the results of call sites of its field accessors (`driverDesc`, `driverId`,
  `resources`) are the consumer end, with the message type's and companion's own
  generated machinery excluded by owning type
- **pinned source**: the send is
  `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1367`
  `worker.endpoint.send(LaunchDriver(driver.id, driver.desc, driver.resources))`;
  the receiving end is
  `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:523`
  `override def receive` at its `:687`
  `case LaunchDriver(driverId, driverDesc, resources_)`, which constructs a
  `DriverRunner` at `:689` and calls `driver.start()` at `:701`. The message type
  is `case class LaunchDriver` at
  `core/src/main/scala/org/apache/spark/deploy/DeployMessage.scala:176`, inside the
  `object DeployMessages` declared at `:34`, so the bytecode type the query
  selects on is `org.apache.spark.deploy.DeployMessages$LaunchDriver`. Those two
  `Worker.scala` lines are the **pinned tree's**; the working checkout this report
  is committed in carries the same two constructs eleven lines lower on that one
  file, and the pinned values are the ones that were probed
- **measured**: 1 producer call site (graph line 1367) with 4 producer argument
  nodes, and 3 consumer call sites (graph line 687); flows from a producer argument
  to a consumer result: **0**

### B2-thread - crossed by a data flow: **false** (0 flows found)

- **hop**: `org.apache.spark.deploy.worker.DriverRunner.start` calls
  `Thread.start()`; the route continues in `run()` on the anonymous `Thread`
  subclass
- **from**: `org.apache.spark.deploy.worker.DriverRunner.start:void()`
- **to**: `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
- **reason**: `Thread.start()` to `run()` is a JVM scheduling relation. The start
  frame returns immediately and `run()` is entered on another thread, so no data
  edge joins a definition in the one to a use in the other
- **modelling**: not modelled - the two ends are measured as they stand, with
  every parameter of the start method **including** the receiver taken as the
  source set, because the method takes no explicit argument and the enclosing
  instance is the only value that could cross
- **pinned source**: `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:123`
  `}.start()`, which closes the
  `new Thread("DriverRunner for " + driverId) { override def run(): Unit = { ... } }`
  opened at `:89` inside `private[worker] def start()` at `:88`, the `run()` body
  beginning at `:90`. `run()` is invoked by the JVM rather than by a call from
  `start()`
- **measured**: 2 start method nodes, 1 start parameter node including the
  receiver, thread body `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
  with 1 continuation call at graph line 99; flows from the start scope into the
  thread body: **0**

### B3-interface - crossed by a data flow: **true** (2 flows found)

- **hop**: the launch call site invokes the **abstract**
  `ProcessBuilderLike.start`; the JDK launch is reached only through the anonymous
  implementation
- **from**: `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)`
  at graph line 240
- **to**: `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
  at graph line 276
- **reason**: an interface invocation names the declaring type, so joining the
  receiver at the abstract call site to a definition inside the implementation
  needs the type hierarchy; a reaching-definition edge does not cross that on its
  own
- **modelling**: not modelled by this query - the receiver and arguments at the
  abstract call site are the source end, the concrete JDK launch call and its
  receiver are the sink end, and whether a flow joins them is reported as measured
- **pinned source**: `runDriver` at
  `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:207` calls
  `runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)` at
  `:221`; `runCommandWithRetry`, declared at `:224`, reaches the sink
  `process = Some(command.start())` at `:240` through the abstract
  `def start(): Process` at `:270` on the trait declared at `:269`, whose sole
  implementation is the anonymous class created at `:275` with its
  `override def start(): Process = processBuilder.start()` at `:276`
- **measured**: 1 abstract launch call site and 1 concrete launch call site; 1
  abstract call-site receiver-and-argument node and 2 concrete call-site nodes; the
  abstract declaration named is
  `org.apache.spark.deploy.worker.ProcessBuilderLike.start:java.lang.Process()` and
  the concrete implementation named is
  `java.lang.ProcessBuilder.start:java.lang.Process()`; flows from the abstract
  receiver to the concrete launch: **2**

The two flows this hop returned are the query's two `boundary-flow` records. They
carry 5 and 6 elements of a cap of 64, `elements_truncated` false on both, and
each publishes the signature its identity was taken on: an `IDENTIFIER` in
`runCommandWithRetry` at graph line 240, a `METHOD_PARAMETER_IN` at graph line
275, a field-access `CALL` at 276 and the `$stack1` identifiers at 276, the
six-element record additionally ending on the `CALL` to
`java.lang.ProcessBuilder.start:java.lang.Process()` at 276. Neither passed an
auth/ACL predicate as defined by the five named selectors, and neither is a
route: they belong to a boundary measurement and are counted under their own
record kind.

`01-callgraph-unguarded-driver-launch` measured this same hop as **crossed by a
call edge**; this query measures it for a **data edge** and also reports crossed.
That is **two measurements of one hop under two different questions**, kept
separate rather than merged into a single verdict - which is precisely why the
agreement of the two formulations' verdicts is not evidence that they are one
formulation.

### B4-partial-function - crossed by a data flow: **false** (0 flows found)

- **hop**: `org.apache.spark.deploy.master.Master.receiveAndReply` returns a
  `PartialFunction` whose body compiles into a synthetic class
- **from**: `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
- **to**: `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: the method named `receiveAndReply` only constructs the partial
  function. The case bodies live in the synthetic class's `applyOrElse`, so a
  source selected on the source-level name is a definition in a method that
  contains none of the route, and the payload the body uses arrives through an
  unapply rather than through that parameter
- **modelling**: modelled by measuring the hop directly - the source-level
  method's parameters are the source end, and the calls the synthetic body makes
  with the recovered payload (`createDriver`, with its arguments) are the sink end.
  ARM 1 and ARM 2 select **both** sides as sources in their own right, so the
  difference between them is measured rather than assumed
- **pinned source**: `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409`
  `override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]`,
  with its first case at `:410`. `receive` and `receiveAndReply` are
  `PartialFunction[Any, Unit]` literals, and Scala 2.13 compiles such a literal
  into a synthetic `$$anonfun$` class whose `applyOrElse` carries the case bodies,
  so the handler body is not a directly named method of `Master` in bytecode
- **measured**: 1 source-level parameter node and 0 synthetic body continuation
  calls reached from it; flows from the source-level parameter into the synthetic
  body: **0**

### The fifth boundary: payload erasure, which the call-graph formulation does not face

Beyond the four hops above, a dataflow formulation faces a boundary that a
call-graph formulation never meets. It is reported here as a named boundary in its
own right, with its hop and its reason, and the envelope carries it as
`additional_dataflow_obstacle` rather than as a fifth boundary record, because it
is a property of the source selection rather than a hop between two ends.

- **hop**: the handler's own formal parameter at
  `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409` to the
  payload the body uses, recovered by the `case RequestSubmitDriver(description)`
  unapply at `:410`
- **reason**: the parameter is `Any`-typed and the bytecode carries it as
  `java.lang.Object`, so it holds no message type for a reaching-definition search
  to follow; and the payload the body uses is **recovered by the unapply** - in
  bytecode a type test, a cast and the case class's own `driverDescription`
  accessor - rather than assigned from that parameter. A flow-based formulation
  therefore has to carry a value through an untyped parameter and a case-class
  extractor before it can even begin the route a call-graph formulation starts
  from
- **how the query answers it**: by selecting **both** sides as sources in their own
  right - the `Any`-typed parameter in ARM 1 and the accessor result in ARM 2 - and
  reporting the two separately, so neither choice is hidden inside one number. It
  is reported as a property of the formulation and **not** worked around

This boundary is also **material evidence for the duplicate-formulation verdict**
below: it is one of the concrete ways the two formulations differ, since query 01
starts from the enclosing method and never has to cross it.

### Summary of the boundary verdicts

Boundaries not crossed by a data flow: `B1-rpc`, `B2-thread`,
`B4-partial-function`. `B3-interface` **is** crossed by a data flow, so three of
the four modelled hops are uncrossed and one is crossed. The fifth boundary above
is not of that kind and carries no crossed/uncrossed verdict; it is reported with
how the query answered it.

Query 01's verdicts are **transcribed** from its published envelope and this
query's are measured here. On the four modelled hops the two agree - both report
`B1-rpc`, `B2-thread` and `B4-partial-function` uncrossed, and both report
`B3-interface` crossed, by two different kinds of edge. Agreement is a result
rather than a foregone conclusion, and it does not make the two one formulation:
the verdict below rests on other grounds.

## The predicate set, and the source types it came from

The mechanical definition: a route is spurious **only** where it passes an
authorization or ACL predicate before reaching the sink. **This judges the query,
not Spark.** The predicate set is exactly these five Boolean methods, and their
source is `core/src/main/scala/org/apache/spark/SecurityManager.scala` at the pin
(457 lines), on the single source type `org.apache.spark.SecurityManager`:

| predicate | source line at the pin |
| --- | --- |
| `aclsEnabled()` | 227 |
| `checkAdminPermissions` | 234 |
| `checkUIViewPermissions` | 248 |
| `checkModifyPermissions` | 264 |
| `isAuthenticationEnabled()` | 274 |

`Master.scala:411`'s `if (state != RecoveryState.ALIVE)` is a **recovery-state**
check and is deliberately not in this set; what that exclusion does and does not
mean is stated under "What the definition does not evaluate" below.

Each predicate reaches the graph as **a composition of selectors**, and it is
worth naming which source-level construct produced which, because that is how a
Scala declaration becomes a bytecode-level predicate this query can test a flow
against:

- a **type anchor** - `org.apache.spark.SecurityManager`, taken from the class the
  five methods are declared on, which in bytecode is the owning type of their
  method nodes. This is what stops a same-named method on some unrelated type
  from matching
- a **name pattern** on the methods of that anchored type -
  `^(check.*Permissions|acls.*|isAuthenticationEnabled)$`, derived from the three
  shapes the five source-level declarations take: the `check`-prefixed,
  `Permissions`-suffixed family, the `acls`-prefixed family, and the single exact
  name `isAuthenticationEnabled`
- a **suffix exclusion** of `_$eq`, derived not from any declaration in the source
  but from what the Scala compiler emits for one, as the next sub-section sets out
- an **invocation selector** built on the result: what a route would have to pass
  is a *call* of one of the five, not a declaration of one, so the route-surface
  measurement counts call sites rather than methods

The remaining predicates this query needs are of two other kinds, and they come
from different source-level constructs again:

- the **source selectors** - `MethodParameterIn` nodes reached through
  `Method.parameter`, filtered on `MethodParameterIn.name`,
  `MethodParameterIn.index` and `MethodParameterIn.typeFullName`, which is how the
  `Any`-typed handler parameter is identified by its erased type rather than by
  position; and, for ARM 2, `Call` nodes filtered on `Call.methodFullName`
  against the message type's own accessor. These derive from the handler
  declaration at `Master.scala:409` and the `case RequestSubmitDriver` unapply at
  `:410`
- the **sink selectors** - `Call` nodes filtered on `Call.methodFullName` against
  `^(java\.lang\.ProcessBuilder\.start|org\.apache\.spark\.deploy\.worker\.ProcessBuilderLike\.start).*`
  and on the host type, together with `Call.receiver` and `Call.argument` on those
  calls. These derive from the launch statement at `DriverRunner.scala:240` and the
  trait and anonymous implementation at `:269`, `:270` and `:275`-`:276`
- the **flow step** - `Traversal.reachableByFlows` under an `EngineContext` copied
  from the console's own with only `EngineConfig.maxCallDepth` overridden, and
  `Path.elements` to read the returned flow back element by element. This is the
  only traversal primitive in the query, and it walks reaching-definition edges

### The set is exactly five, and was not widened

Two auth-adjacent Boolean methods on the very same anchored type are deliberately
**not** selectors: `isEncryptionEnabled()` at
`core/src/main/scala/org/apache/spark/SecurityManager.scala:280` and
`isSslRpcEnabled()` at `:295`. Neither is an authorization or ACL predicate, and
adding either would change what the word "spurious" counts here.

The selector block is held **byte-identical across all three probe queries** -
`01-callgraph-unguarded-driver-launch` and
`03-parameterized-handler-sink-pairs` as well as this one - so that their three
spurious counts stay comparable with one another. That byte-identity is the
reason the set is constrained rather than convenient: widening it in one query
alone would silently make one count mean something the other two do not.

A dataflow formulation is where the temptation to widen is strongest, because a
flow-based selector loosens easily - one more Boolean method on the anchored
type, or a predicate matched on any element of a flow rather than on an
invocation. Neither was done. The set is the same five and the test is the same
test, which is what keeps this query's spurious count **comparable** with the
other two rather than merely similar.

### How the bytecode-level selector was constrained

The anchored selector is
`^(check.*Permissions|acls.*|isAuthenticationEnabled)$`, paired with a type
selector on `org.apache.spark.SecurityManager`. On **bytecode** that is not
enough. `SecurityManager.scala:59` declares `private var aclsOn`, and Scala
compiles a private var into accessors, so the graph carries **both** a getter
`aclsOn()` and a setter `aclsOn_$eq(boolean)` - and both names satisfy the
`acls.*` alternative of that pattern. The setter is what the `_$eq` exclusion
removes; the getter is what the intersection with the five removes. Neither is a
predicate, and a naive `acls.*` pattern would have taken both.

The narrowing is therefore three steps, and all three sets are reported so it is
auditable rather than asserted:

1. broad anchored selector on the 252 method nodes (107 distinct names) of that
   type: `aclsEnabled`, `aclsOn`, `aclsOn_$eq`, `checkAdminPermissions`,
   `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`
2. minus every name ending in `_$eq`, which drops `aclsOn_$eq`, leaving
   `aclsEnabled`, `aclsOn`, `checkAdminPermissions`, `checkModifyPermissions`,
   `checkUIViewPermissions`, `isAuthenticationEnabled`
3. intersected with the five named source-level predicates, which drops `aclsOn` -
   a private-var getter, not one of the five - leaving exactly `aclsEnabled`,
   `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`,
   `isAuthenticationEnabled`

The final set resolves to these five method full names, and it is asserted
against **the graph, not the source**: the query halts unless the three-step
narrowing resolves to exactly the five.

- `org.apache.spark.SecurityManager.aclsEnabled:boolean()`
- `org.apache.spark.SecurityManager.checkAdminPermissions:boolean(java.lang.String)`
- `org.apache.spark.SecurityManager.checkModifyPermissions:boolean(java.lang.String)`
- `org.apache.spark.SecurityManager.checkUIViewPermissions:boolean(java.lang.String)`
- `org.apache.spark.SecurityManager.isAuthenticationEnabled:boolean()`

### The over-match hazards the anchor and the exact five exclude

The type anchor alone does not exclude these. Every one is a method **on the
anchored type** whose name the broad pattern reaches, and it is the intersection
with the five exact names that removes each of them. Naming them is what makes
the selector's precision checkable rather than asserted - all lines are the
pinned tree's, in
`core/src/main/scala/org/apache/spark/SecurityManager.scala`:

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
uses. Counting a call to one of them as "the route passed a predicate" would
therefore have inflated this query's spurious count with call sites the
definition does not cover - which is a statement about the selector, not about the
code it selects over.

### How the predicate test applies to a flow

On a flow record the test asks whether any of the five is **invoked** on the
route: as the callee of any element, as the enclosing method of any element, or
at either end group. A held reference to the anchored type is not an invocation
and does not satisfy it, which is the same distinction the route-surface search
below turns on. Each emitted record publishes the answer as
`passed_auth_or_acl_predicate`, and `spurious` carries the same value under the
name the definition uses, so the classification is readable per record rather
than only in aggregate.

## Whether an expected-spurious route was absent

`spurious_count` = **0**, and `expected_spurious_route_absent` = **true** on a
basis the envelope records as **structural**. No route in the emitted set passed
an auth/ACL predicate as defined by these five named selectors.

**The absence is structural, not a consequence of the query filtering well.**
Measured against the graph: 18 call sites of the five predicates exist graph-wide,
in 18 distinct calling methods, and **0** of them sit on the route surface
(`org.apache.spark.deploy.master.Master`,
`org.apache.spark.deploy.rest.StandaloneRestServer`,
`org.apache.spark.deploy.worker.DriverRunner`). The predicate set exists and is
invoked elsewhere in the program; it is not invoked anywhere on this route, so no
route could have passed one.

The source-level check agrees with the graph-level one. Searching the pinned tree
for all five names across the three route files, in route order -
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala`,
`core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala`,
`core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala` - returns
**no occurrence of any of the five in any of the three**. So the zero is not a
route that filtered clean; there was no call site of a predicate anywhere on the
route surface for a route to have passed.

Two zeros meet here and they are not the same statement. `distinct_routes` is 0
because no flow was returned, and `spurious_count` is 0 because none of the five
is invoked on the route surface at all. Either zero alone would leave the other
open, and the second is the one this section answers.

### The zero is scoped to the route surface, not to the program

An unscoped "zero call sites" claim would simply be false. `aclsEnabled()` **is
invoked** inside the anchored type's own source file, at
`core/src/main/scala/org/apache/spark/SecurityManager.scala:249`, at `:265` and at
`:407` inside the private `isUserInACL` declared at `:402`; and 18 call sites of
the five exist graph-wide across 18 distinct calling methods. The zero above
holds for the three route files and for nothing wider.

### Reference is not invocation

The route surface does mention the predicate type; every such mention is a
reference of a kind that **invokes** none of the five. Holding, importing,
constructing or passing a value on is not invoking a method on it:

| pinned location | what it is |
| --- | --- |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:28` | imports `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:53` | declares `val securityMgr: SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:139` | reads the companion constant `SecurityManager.SPARK_AUTH_SECRET_CONF` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1429` | constructs a `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:27` | imports `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:56` | declares `val securityManager: SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:194` | passes `securityManager` on as an argument to the command builder |

The distinction is load-bearing twice over: it is why the route-surface count is
0 rather than 7, and it is the same test the flow-level predicate applies, so a
flow whose elements merely carried a `SecurityManager` value would not have been
classified as having passed a predicate either.

### What the definition does not evaluate

The mechanical definition evaluates **only** those five predicates. Any other
conditional on the route is outside it and is **not assessed** by it. Concretely,
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala:411`
`if (state != RecoveryState.ALIVE)` guards the branch that reaches `createDriver`
at `:417` - it is a recovery-state check, it is not one of the five, and it is
therefore neither counted as a predicate nor reported as one.

So a spurious count of 0 means exactly and only what the definition says, and it
does **not** mean that the route carries no conditional. Reading it that way
would attribute to this query a claim it does not make.

This is a statement about **this query's own output** under **this query's own**
definition of the term. It is not an assessment of Spark, of any Spark component
or of any configuration, and nothing here should be read as one. In particular,
nothing in this report states or implies anything about how Spark authorizes any
operation: the five selectors are a query-side definition used to classify this
query's own returns, and where the route count is zero there are no returns to
classify at all.

## Whether this formulation duplicates another query's

`duplicate_formulation` = **not_duplicate**. The label aggregates the two pairwise
entries below and names the strongest relation either carries: `not_duplicate`
against `01-callgraph-unguarded-driver-launch` and `not_duplicate` against
`03-parameterized-handler-sink-pairs`, so the aggregate carries no scope
qualification. Neither entry carries a scoped duplication, which is what would
have made the aggregate partial. The verdict was **not** inferred from the file
names differing.

**Why the question is live rather than rhetorical.** This query and query 01
address the **same** handler/sink pair - the
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala` handler to the
launch at `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:240` -
and query 03's **pair one is that pair a third time**. Two queries over one pair
invite the reading that one restates the other, and a probe whose three queries
were one query written out three times would be a probe that measured one thing
and reported three. So "two genuine formulations, or one restated?" is a real
finding about the probe's own validity, and it is settled on grounds other than
the pair.

**The grounds relied on**, the same four in both entries: the predicate and step
vocabulary each query uses; the source and sink node sets each selects; the
traversal semantics; and whether the returned route sets coincide. Against query
03 two further grounds apply: whether the target pair sets coincide, and the two
published API construct lists compared as sets in **both** directions. Which of
them carried the verdict is stated in each entry below rather than left to a
reader to guess.

### Against `01-callgraph-unguarded-driver-launch`: not_duplicate

The same handler/sink pair addressed over **different edges**.

- *traversal semantics*: flow over **reaching-definition edges** through the OSS
  dataflow layer, selecting **PARAMETER and EXPRESSION** nodes as its ends, against
  reachability over **CALL edges**, selecting whole **METHOD** nodes as its ends.
  Neither is expressible as the other: no call-edge traversal establishes that a
  value reaches the launch, and no reaching-definition traversal establishes that
  control can arrive there. `can_differ_for_some_input` is true and
  `one_expressible_as_the_other` is false.
- *node sets*: this query's ends are the synthetic `applyOrElse`'s `Any`-typed
  formal parameter and, in ARM 2, the accessor result, against the launch call,
  its receiver and its arguments. Query 01 selects whole methods at both ends.
  The source ends are not even the same **shape**: this query's source end is the
  **synthetic** `$$anonfun$` partial-function method behind
  `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409` and its
  erased `java.lang.Object` parameter, with the payload recovered through the
  `case RequestSubmitDriver` unapply at `:410`, whereas the call-graph
  formulation starts from the enclosing method. `MethodParameterIn`,
  `Method.parameter`, `Call.argument`, `Call.receiver` and `Path.elements` are in
  this query's construct list and in neither of the other two's.
- *construct lists*: 18 of this query's 42 API constructs do not appear in query
  01's published list at all, and 4 of query 01's do not appear here, with 24
  shared. The difference is computed as a set difference from the two published
  lists rather than eyeballed. The **traversal primitive itself** is one of the
  differing entries on each side - `Traversal.reachableByFlows` here,
  `NoResolve.getCalledMethodsAsTraversal` there - which is the auditable form of
  the semantics difference above.

  - only here: `AstNode.code`, `AstNode.label`, `AstNode.lineNumber`,
    `Call.argument`, `Call.receiver`, `CfgNode.method`,
    `EngineConfig.maxCallDepth`, `EngineContext.config`, `EngineContext.copy`,
    `EngineContext.semantics`, `Method.call`, `Method.parameter`,
    `MethodParameterIn.index`, `MethodParameterIn.method`,
    `MethodParameterIn.name`, `MethodParameterIn.typeFullName`, `Path.elements`,
    `Traversal.reachableByFlows`
  - only in query 01: `Call.code`, `Call.order`, `Method.callOut`,
    `NoResolve.getCalledMethodsAsTraversal`

- *route sets*: `route_records_emitted_here` is 0 and
  `route_records_emitted_there` is 0, so both route sets are empty and they
  coincide **only by both being empty**. The measured difference is in what each
  can **return at all**: this query emitted 4 element-level flow records, each a
  sequence of `IDENTIFIER`, `METHOD_PARAMETER_IN` and `CALL` nodes with their
  graph lines - including 2 from a formal parameter to the launch call inside a
  single method - and a method-level call-edge traversal produces no such record
  for any input, which is why query 01 published none.
- *boundary verdicts*: the two formulations **agree** on all four -
  `boundary_verdicts_agree` is true and `boundary_verdict_disagreements` is empty,
  both reporting `B1-rpc`, `B2-thread` and `B4-partial-function` uncrossed - and
  agreement on a verdict is **not** identity of formulation. `B3-interface` is the
  case that shows why: both report it crossed, and they report it crossed by two
  different **kinds of edge**, which is kept as two measurements rather than
  merged into one verdict.
- returns are reported side by side and **never summed**; `results_summed` is
  false.

**The inference actually drawn, and from what.** The `not_duplicate` verdict is
drawn from the edge kinds, the node granularity, the engine and bound semantics
and the construct-set difference - all properties of the two committed **sources**,
checkable without a graph load - and **not** from the two returning zero routes.
Two queries both returning zero is **not** evidence of duplication, and two
returning the same non-empty set would not have been either; a coinciding result
set is one input to the question, and here it is the **least** informative of the
four grounds, because both sets are empty. The payload-erasure boundary named
above is one of the ways the two formulations genuinely differ, and it is
material to this verdict for the same reason: it is a difference in what each
formulation has to carry before it can begin.

### Against `03-parameterized-handler-sink-pairs`: not_duplicate

`scope_of_the_duplication` = **none**. A different formulation over different
edges and different nodes, on a target set that is not the same set.

- *traversal semantics*: that query traverses **CALL edges** and selects whole
  **METHODS**, and it **loads no flow engine at all**; this query traverses
  reaching-definition edges through the OSS dataflow layer and selects PARAMETER
  and EXPRESSION nodes. Its construct list carries no dataflow step, which is the
  auditable form of that difference.
- *target pair sets*: `same_target_pair` is true **of that query's pair one**,
  which is this query's only pair. The pair **set** still differs, because that
  query addresses a second pair this query does not address at all - the
  `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala`
  handler to the same `DriverRunner` sink. Sharing a pair is not sharing a
  formulation, which is what the status turns on.
- *neither subsumes the other*: that query cannot express this one's dataflow
  question on any of its pairs, and this query has **no pair parameter** with
  which to express that query's second instantiation.
- *construct lists*: the same set difference in both directions as against query
  01 - 18 only here, 4 only there, 24 shared - because query 03's published list
  and query 01's are the same 28 constructs.
- *bounds*: 6 here against 12 there, and
  `bound_values_are_the_same_kind_of_quantity` is **false**: 6 is a flow call
  depth the engine's backward search may expand, 12 is how far a forward
  call-graph expansion may go. Two numbers that are not the same kind of quantity
  cannot be compared, let alone added.
- *route sets*: `route_sets_coincide` is recorded as **not established here** -
  query 03 owns its own per-pair figures, and this query's returns are never added
  to them. So this ground contributed nothing to the verdict, and saying so is the
  honest report of which grounds carried it.

The inference here is drawn from the edge kinds, the node granularity, the absence
of a flow engine on that side and the construct-set difference - again properties
of the two committed sources - and not from either query's returns. Sharing pair
one is **recorded** rather than treated as evidence of duplication.

### The relation is symmetric, and both directions were checked

Duplicate formulation is a **symmetric pairwise relation**: the verdict this
envelope states against a query is the same verdict that query's envelope states
against this one. It is one measurement cited twice rather than two measurements,
so a disagreement between the two directions would be a defect.

- `queries/joern/results/01-callgraph-unguarded-driver-launch.json` states
  **not_duplicate** against this query.
- `queries/joern/results/03-parameterized-handler-sink-pairs.json` states
  **not_duplicate** against this query, with its scope recorded as none.

Both directions agree, and `verdicts_agree_in_both_directions` is true in each
entry. Those two envelopes each carry an **aggregate** label of
`partial_duplicate`, which arises from their relation **to each other** - query 01
is the pair-one instantiation of query 03's parameterized form - and not from any
relation to this query. Reading their aggregate label as their verdict against
this query would be a misreading of a field that is explicitly an aggregate.

## The three effort measures

Each is answered individually. None is omitted, and none is claimed beyond what
was actually done.

1. **Query revisions committed: 1.** The **counting convention**, stated so the
   number is interpretable rather than bare: the count of commits touching
   `queries/joern/02-dataflow-unguarded-driver-launch.sc` from its first
   appearance to the end of the probe. On that convention this run introduces the
   query source in a single commit, so the count is 1 - a low number because the
   convention counts committed revisions of the file, not the drafting behind it.
   An uninterpretable number would be worse than none, which is why the
   convention is stated rather than assumed.
2. **Distinct Joern API constructs used: 42.** The **list is the measure** and the
   count is computed from it, so the number is auditable rather than asserted. It
   is deduplicated, and every entry names a member this query's source invokes.
   Each was confirmed present in that source with **both** published construct
   lists excised - this query's own and its transcription of query 01's - so that
   no entry could satisfy itself out of a list literal. The one occurrence of
   query 01's call-edge primitive inside this source is that transcription rather
   than a use, which is why it is absent from the list below and present instead
   among the constructs recorded as only query 01's.

   - `AstNode.code`
   - `AstNode.label`
   - `AstNode.lineNumber`
   - `Call.argument`
   - `Call.dispatchType`
   - `Call.lineNumber`
   - `Call.method`
   - `Call.methodFullName`
   - `Call.name`
   - `Call.receiver`
   - `CfgNode.method`
   - `EngineConfig.maxCallDepth`
   - `EngineContext.config`
   - `EngineContext.copy`
   - `EngineContext.semantics`
   - `Method.call`
   - `Method.callIn`
   - `Method.fullName`
   - `Method.lineNumber`
   - `Method.name`
   - `Method.parameter`
   - `Method.typeDecl`
   - `MethodParameterIn.index`
   - `MethodParameterIn.method`
   - `MethodParameterIn.name`
   - `MethodParameterIn.typeFullName`
   - `Path.elements`
   - `Steps.fullName`
   - `Steps.fullNameExact`
   - `Steps.l`
   - `Steps.nameExact`
   - `Steps.size`
   - `Steps.take`
   - `Traversal.reachableByFlows`
   - `TypeDecl.fullName`
   - `TypeDecl.method`
   - `cpg.call`
   - `cpg.file`
   - `cpg.method`
   - `cpg.typeDecl`
   - `importCpg`
   - `switchWorkspace`

   The list is **expected to differ** from query 01's, and it does: 18 of these 42
   appear in neither of the other two queries' published lists, and 4 of theirs do
   not appear here. That difference is not a curiosity - it is one of the grounds
   the duplicate-formulation verdict above rests on, and it is the auditable form
   of the semantics difference, because the traversal primitive itself is one of
   the differing entries on each side.

   This is the **per-query** list only. The probe-wide **union** across the three
   queries is owned by `oss-scan-results/joern-probe.md` and is deliberately not
   computed here, so that the union is one measurement in one place rather than
   three partial ones.
3. **Parameterizability: not claimed here.** This query is a **single-pair
   formulation** - it fixes one handler and one sink and takes no pair parameter -
   so it neither claims the measure nor could satisfy it. Saying so is the honest
   answer rather than an omission or an overclaim.

   The probe's evidence for the measure is
   `03-parameterized-handler-sink-pairs` **actually invoking** its parameterized
   form on the second named handler/sink pair: the handler `handleSubmit` at
   `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268`,
   whose enclosing type `StandaloneSubmitRequestServlet` is declared at `:171` of
   that same file, to the `DriverRunner` launch at
   `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:240`.
   That invocation's result is captured in
   `queries/joern/results/03-parameterized-handler-sink-pairs.json`, which is where
   the measure is settled. A parameter list that merely exists does not satisfy it,
   and neither would a declared-but-skipped second pair.

## Modelling decisions, stated so the counts stay interpretable

- **Two source sets, two arms.** The `Any`-typed formal parameter and the
  unapply-recovered payload are different nodes, so they are evaluated separately
  and reported separately rather than unioned into one number.
- **ARM 1 is run at two depths.** That is what turns the engine's unobservable
  call-depth bound into a measured comparison rather than an assumption, and it is
  why three route-bearing arms report two source selections.
- **The implicit receiver is excluded from ARM 1 and included in B2.** It carries
  the enclosing instance rather than the message, so it is not a handler input;
  but `DriverRunner.start` takes no explicit argument, so excluding it there would
  make that measurement vacuous. Both choices are stated where they apply.
- **The sink node set includes receivers and arguments.** A flow that reaches the
  value being launched ends at the launch call, its receiver or one of its
  arguments; taking only the call node would miss a flow into the receiver.
- **The engine-liveness control is not a route.** Its flows answer whether the
  dataflow layer is live on this sink and are counted under their own record kind,
  never among the routes.
- **Operator pseudo-calls are excluded.** A CPG `<operator>.*` call is an artefact
  of the representation rather than a method call, and expanding them would inflate
  every counter without adding a step of the kind being measured.
- **Duplicate class definitions are unioned.** The graph carries more than one node
  per class where two staged archives carried the same class, so method nodes are
  grouped by full name and their parameters and calls unioned rather than one node
  being picked.
- **The flow engine's context is the console's own, copied.** Only the call-depth
  bound is overridden, so the semantics the traversals run under are the same ones
  the dataflow overlay was built with
  (`io.joern.dataflowengineoss.semanticsloader.FullNameSemantics`), and the context
  is passed **explicitly** at every call site so that no implicit resolution
  decides which context a traversal ran under.
- **Records carry a total order.** Boundary records first in the fixed order
  `B1-rpc`, `B2-thread`, `B3-interface`, `B4-partial-function`; then the
  boundary-flow records, deduplicated on (arm, element signature) and ordered by
  (arm, element count, signature); then the control's flow records ordered by
  (element count, signature). A dataflow engine is a common source of unstable
  result ordering, which is why the key is stated and why every record publishes
  the signature the key is taken on.
- **Graph line numbers are the graph's own.** A node's `lineNumber` comes from the
  bytecode line-number table and can differ by a line from the `def` or statement
  line cited from the source. Source anchors in this report are quoted from the
  pinned tree; graph lines are labelled as such.
- **A record locates itself by method, not by file.** No record carries a file
  path: it names its enclosing method full name and the graph's own line, and
  every source path in this report is expressed relative to the repository root or
  the `$SPARK_SRC` root.
- **Element code is collapsed and capped.** A flow element's `code` is put on one
  line and capped at 160 characters, so the record stays readable and the emitted
  JSON stays deterministic.

## The graph this query loaded, and its identity

- named path `$HARNESS_CPG`, repository-relative `harness/cpg/spark.cpg`, which is
  a **symlink**
- resolved target: a host-shared read-only file outside the repository root,
  reached by following that link. Measured **symlink-following**: **541255894**
  bytes, sha256
  `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`
- the link itself measures 33 bytes. That figure is recorded only to be
  discarded, because measuring the link rather than its target is exactly the
  mistake this check exists to catch
- **record of account**: `harness/artifacts/logs/cpg-frontend.log`, which computed
  that pair at write time under the same symlink-following semantics and owns it.
  This report cites that measurement rather than establishing a second one.
  Comparison result: **match**, on both the byte size and the digest, and the
  identity was **re-verified immediately before this query's load** - a mismatch
  would have halted the run rather than producing conclusions about a graph nobody
  has
- the path the plan names, `harness/cpg/spark.cpg`, and the path the environment
  exports resolve to the **same file** (equal resolved target), so there is one
  graph under two names rather than two graphs
- graph contents as loaded: 1397339 methods, 119691 type declarations, 45037 files
- the JVM was JDK major **21**, the required major, at a heap of **68719476736**
  bytes = 64 GiB - at the floor rather than above it, so no additional pre-touch
  proof is owed beyond the gate's own commit proof of that value. The floor is a
  minimum and a default, never a ceiling: a larger heap is permitted and reported,
  a smaller one is not, because a truncated result's silence cannot be told apart
  from a clean one
- no absolute host path appears in this report or in its envelope. The resolved
  target is identified by the size-and-digest pair above rather than by a host
  path, which would vary between two checkouts of one branch and so could not be
  part of a deterministic record
- the envelope additionally records a **reproduction check**: attempted, and
  halted in the identity stage before any load. That re-run compiled, ran,
  re-measured JDK major 21 on `21.0.12.1+1-LTS` at the same 68719476736 bytes of
  heap - agreeing with the runtime figures above field for field - then measured a
  resolved target of 548118435 bytes with sha256
  `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53`, reported it
  as **not** matching the identity of record above, printed its failure marker and
  emitted no result region and no envelope at all. That is the designed failure
  behaviour, observed rather than described. The shared provisioned graph had been
  rebuilt between the invocation that measured this query and that later re-run;
  both pairs are recorded with their provenance and neither is reconciled away,
  because the resolved target is a host-shared read-only file this run neither
  rebuilds nor replaces. The identity reported above remains the one the run that
  produced these measurements re-verified immediately before its load

## Determinism of this report

An unchanged query source over an unchanged graph must produce a byte-identical
file, so this document carries no wall-clock timestamp, no elapsed time, no
process identifier, no host name, no host-specific scratch or workspace path and
no absolute host path. The only paths it names are repository-relative, relative
to the `$SPARK_SRC` root, or environment-variable names. Those excluded quantities
are real and are not being hidden - they live in
`harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log`, a console
stream deliberately **not** held to byte-identity.

## Reproducing this

```
cd <a scratch directory outside the repository>
HARNESS_REPO_ROOT=<repo> JAVA_HOME="$JAVA_HOME_21" \
  JAVA_TOOL_OPTIONS="-Xmx64g" SL_LOGGING_LEVEL=WARN \
  joern --script <repo>/queries/joern/02-dataflow-unguarded-driver-launch.sc -J-Xmx64g < /dev/null
```

`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so
`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs
at. The query measures the heap it received and stops below the floor: raising a
heap is permitted and reported, lowering one is not. It also re-verifies the
graph's identity before loading it and halts on a mismatch, which is what the
reproduction check above records happening.
