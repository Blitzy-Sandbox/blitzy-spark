# Joern capability probe 02-dataflow-unguarded-driver-launch

Bounded **dataflow** from the Spark standalone Master's driver-submission handler
to the privileged process launch hosted on the `DriverRunner` surface, over the
code-property graph built from the pinned tree's bytecode. This is the **same
handler/sink pair as `01-callgraph-unguarded-driver-launch`**, addressed by a
**different formulation**: data edges rather than call edges.

This report is **observational**. It judges no finding - not real, not important,
not a false positive, not a duplicate - and makes no comparison between tools. It
contributes no row to `oss-scan-results/findings.json` and writes nothing into
`harness/artifacts/raw/`.

The slug `02-dataflow-unguarded-driver-launch` is the **identifier** the plan
assigns this query. It names the question the query was written to ask - whether a
dataflow formulation can join this handler to this sink, and whether any route it
returns passes one of five named predicates first. It is not a finding, and nothing
in this report should be read as an assessment of Spark, of any Spark component or
of any Spark configuration.

| | |
| --- | --- |
| Query source | `queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| Query source sha256 | `902b7ffe8d708d6cb4ddfc057f65b1a2a023fc90c5b55c8d3ba012885dcb3fd1` (369754 bytes) |
| Publication id | `7a07b7184af2306b1a7c97a3c02b7b53f52358b45718384814df0915e14fb559` |
| Envelope | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` |
| Console log | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21 |
| Heap actually used | 68719476736 bytes (floor 68719476736) |
| Graph | 547980224 bytes, sha256 `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` |
| Graph identity re-verified before the load | yes, against `provision-log/cpg-identity.txt` |
| Bytes actually imported | a private copy this run made, digested in the copy pass, verified against that record, and re-verified by digest and inode after the load |
| Graph methods / typeDecls / files | 1398964 / 119860 / 45037 |
| Flow engine semantics | `io.joern.dataflowengineoss.semanticsloader.FullNameSemantics` |
| Compile status | compiled |
| Run status | completed |
| Records returned | 8 (4 boundary, 0 route, 2 boundary-flow, 2 control) |
| Distinct routes | 0 |
| Spurious routes | 0 |
| Dataflow layer live on this sink | true |

## Which source wrote this report

This report was written by `queries/joern/02-dataflow-unguarded-driver-launch.sc`, whose contents at the moment
of the run digest to sha256 `902b7ffe8d708d6cb4ddfc057f65b1a2a023fc90c5b55c8d3ba012885dcb3fd1` over 369754 bytes. The
query read its own source at run time and computed that digest itself; it
verified that the file it digested declares this query's own identifier, and it
refuses to publish anything if it does not.

The envelope beside this report carries the same digest and the same publication
identifier `7a07b7184af2306b1a7c97a3c02b7b53f52358b45718384814df0915e14fb559`, as does the console log. Every figure below was
measured during that run from the graph, from this source's own text, from the
identity record or from the repository's commit history for this source path -
nothing here is transcribed from another document or from a previous run. **A
result whose digest does not match the source beside it was not written by that
source**, which makes drift between a query and its published result a
mechanical check rather than a matter of opinion.

## The result

**Distinct routes: 0.** Routes are counted distinct on
(source group, sink group, flow element signature) across the route-bearing arms
below. They are **never summed** - not across the arms, and not with
`01-callgraph-unguarded-driver-launch`, which asks a different question over
different edges. A reader who adds the two queries' routes together gets a number
that means nothing.

No flow from a source to a sink node was returned within the stated bounds. That
is a capability finding about what this formulation can express over this graph,
and it is reported as measured: no bound was loosened, removed or re-run
unbounded to produce a non-empty result. Two things make the zero interpretable
rather than merely empty:

1. the **engine-liveness control** returned true for the question "does the
   dataflow layer produce a flow on this very sink", so the zero above is
   attributable to the route rather than to an engine with nothing to walk;
2. the **four boundaries** below are measured individually, and each one that is
   not crossed by a data edge is a named reason.

### The engine-liveness control

`dataflow_layer_live_on_this_sink` = **true**. The control asks for a flow
from `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry`'s own formal parameter to the
launch call in that same method, which the parameter is the receiver of. It is
intraprocedural by construction and it is **not a route**: its flows are reported
under their own field and are never counted among the routes above.

- control flows found: 2, retained: 2
- control evaluations: 1 at call depth 2

Because the control is non-empty, a zero from a cross-boundary arm is a statement
about the route and not about the engine.

## Whether the bound was reached

`bound_reached` = **false**. The primary bound is `MAX_FLOW_CALL_DEPTH` =
6, the engine's `EngineConfig.maxCallDepth`: the number of call
boundaries the backward search may expand while looking for a source. Every
traversal in this query carries an explicit named bound; none runs unbounded.

Two kinds of bound, reported separately because only one of them is observable:

- **Observable caps** (`observable_bound_reached` = false): the
  per-source step cap, the per-pair flow cap, the flow-length cap and the source,
  sink and entry-point truncation counters are all counted by this query's own
  evaluator, so whether each bit was set is measured.
- **The engine's internal call-depth bound is not observable**: the engine reports
  no truncation flag when it stops expanding callers. Rather than guess, ARM 1 is
  run TWICE - at depth 2 and at depth 6 - and the
  results compared: `results_differ_across_the_two_depths` = false.
  Equal results are evidence that the outcome does not depend on the bound across
  that range; a difference would be evidence that it does. This is a stated
  limitation of the engine's output, not a gap in the measurement.

| bound | value |
| --- | --- |
| MAX_FLOW_CALL_DEPTH | 6 |
| MAX_FLOW_CALL_DEPTH_SHALLOW | 2 |
| MAX_BOUNDARY_FLOW_CALL_DEPTH | 2 |
| MAX_ENGINE_FLOWS_PER_EVALUATION | 64 |
| MAX_FLOW_LENGTH | 64 |
| MAX_FLOWS_PER_PAIR | 8 |
| MAX_STEPS_PER_SOURCE | 8 |
| MAX_TOTAL_RETURNS | 256 |
| MAX_SOURCE_NODES | 64 |
| MAX_SINK_NODES | 64 |
| MAX_ENTRY_POINTS | 16 |
| MAX_CALL_SCAN | 200000 |
| MAX_TYPE_SCAN | 100000 |
| MAX_CODE_CHARS | 160 |

Every bound above is published with its reached flag and its basis in the
envelope's `bounds_reached` and `bounds_reached_basis`. For the two sweep caps
the reached flag is the **disjunction over every sweep that cap governs**, and
the basis names each governed sweep with its own observed count and flag, so a
cap that governs several sweeps cannot report the state of only one of them.

### Every traversal this query materialized, and the cap that governed it

| sweep | cap | value | observed | truncated |
| --- | --- | --- | --- | --- |
| entry: synthetic partial-function type declarations | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| entry: methods on those synthetic types | `MAX_TYPE_SCAN` | 100000 | 60 | false |
| entry: source-level handler methods | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| parameters: entry | `MAX_TYPE_SCAN` | 100000 | 10 | false |
| ARM 2: calls named driverDescription | `MAX_CALL_SCAN` | 200000 | 12 | false |
| ARM 2 fallback: calls inside the entry methods | `MAX_CALL_SCAN` | 200000 | 1978 | false |
| sink: calls named start | `MAX_CALL_SCAN` | 200000 | 1233 | false |
| sink: receiver operands of the launch call sites | `MAX_CALL_SCAN` | 200000 | 2 | false |
| sink: argument operands of the launch call sites | `MAX_CALL_SCAN` | 200000 | 2 | false |
| predicate: type declarations | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| predicate: methods on that type | `MAX_TYPE_SCAN` | 100000 | 252 | false |
| predicate: call sites of the five named predicates | `MAX_CALL_SCAN` | 200000 | 36 | false |
| liveness control: host methods | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| parameters: liveness control host | `MAX_TYPE_SCAN` | 100000 | 8 | false |
| engine flows: CONTROL-intraprocedural-liveness org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean) -> org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean) | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 2 | false |
| engine flows: ARM1-handler-parameters-shallow org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1) -> org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean) | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM1-handler-parameters-shallow org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1) -> org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process() | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM1-handler-parameters-shallow org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) -> org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean) | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM1-handler-parameters-shallow org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) -> org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process() | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM1-handler-parameters org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1) -> org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean) | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM1-handler-parameters org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1) -> org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process() | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM1-handler-parameters org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) -> org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean) | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM1-handler-parameters org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) -> org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process() | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM2-unapply-recovered-payload org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1) -> org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean) | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| engine flows: ARM2-unapply-recovered-payload org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1) -> org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process() | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| B1: message type declarations | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| B1: methods on the message type | `MAX_TYPE_SCAN` | 100000 | 36 | false |
| B1: producer call sites of the message constructor | `MAX_CALL_SCAN` | 200000 | 12 | false |
| B1: consumer call sites of the message accessors | `MAX_CALL_SCAN` | 200000 | 36 | false |
| B1: arguments of the message constructor call sites | `MAX_CALL_SCAN` | 200000 | 4 | false |
| engine flows: boundary B1-rpc | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| route surface: type declarations under org.apache.spark.deploy.master.Master | `MAX_TYPE_SCAN` | 100000 | 217 | false |
| route surface: type declarations under org.apache.spark.deploy.worker.Worker | `MAX_TYPE_SCAN` | 100000 | 156 | false |
| route surface: type declarations under org.apache.spark.deploy.worker.DriverRunner | `MAX_TYPE_SCAN` | 100000 | 21 | false |
| route surface: type declarations under org.apache.spark.deploy.worker.ProcessBuilderLike | `MAX_TYPE_SCAN` | 100000 | 6 | false |
| B2: thread host methods | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| parameters: B2 thread host | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| B2: thread body methods | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| B2: calls in the thread body methods | `MAX_CALL_SCAN` | 200000 | 210 | false |
| B2: receiver operands of the thread-body continuation calls | `MAX_CALL_SCAN` | 200000 | 1 | false |
| engine flows: boundary B2-thread | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 0 | false |
| B3: receiver operands of the abstract launch call sites | `MAX_CALL_SCAN` | 200000 | 1 | false |
| B3: argument operands of the abstract launch call sites | `MAX_CALL_SCAN` | 200000 | 1 | false |
| B3: receiver operands of the concrete launch call sites | `MAX_CALL_SCAN` | 200000 | 1 | false |
| engine flows: boundary B3-interface | `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | 2 | false |
| parameters: B4 source-level handler | `MAX_TYPE_SCAN` | 100000 | 4 | false |
| B4: calls in the synthetic handler bodies | `MAX_CALL_SCAN` | 200000 | 1968 | false |
| B4: argument operands of the synthetic-body continuation calls | `MAX_CALL_SCAN` | 200000 | 0 | false |

Every materialization **outside the flow engine** goes through one bounded helper
that takes `cap + 1` elements and reports truncation when it saw more than `cap`,
so a cap applied at one site and forgotten at the next is not expressible: a
sweep absent from this table did not run.

The flow engine's **return** goes through that same helper. The iterator
`reachableByFlows` yields is materialized under
`MAX_ENGINE_FLOWS_PER_EVALUATION`, taken as `cap + 1` before the iterator becomes
a list, registered as one sweep per evaluation and published above with its own
reached flag and per-evaluation basis. So the paths this query materializes from
any one engine evaluation are bounded on the same terms as every other sweep.

The engine's own backward search **before** it yields an element is the one thing
neither mechanism reaches, and that is stated rather than papered over. It runs
inside `reachableByFlows`, the API exposes no counter for it and no cap over it,
so this query neither bounds nor counts it and claims nothing about its cost. It
is influenced only indirectly: by the `EngineConfig.maxCallDepth` this query
overrides on the console's own context (`MAX_FLOW_CALL_DEPTH`,
`MAX_FLOW_CALL_DEPTH_SHALLOW`, `MAX_BOUNDARY_FLOW_CALL_DEPTH`), by
`MAX_STEPS_PER_SOURCE` on how many sink groups one source group is evaluated
against, by `MAX_SOURCE_NODES` and `MAX_SINK_NODES` on the node sets handed to
it, and by `MAX_ENTRY_POINTS` on the source groups traversed. Each of those is
published above with its own reached flag and basis.

`MAX_FLOWS_PER_PAIR` and `MAX_FLOW_LENGTH` govern **neither** the search nor the
materialization: the first bounds how many already-materialized flows a pair
**retains**, the second how many elements of a retained flow are **reported**.

| arm | depth | evaluations | flows found | flows retained | step cap | per-pair cap | length cap | source groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ARM1-handler-parameters-shallow` | 2 | 4 | 0 | 0 | false | false | false | 2/2 |
| `ARM1-handler-parameters` | 6 | 4 | 0 | 0 | false | false | false | 2/2 |
| `ARM2-unapply-recovered-payload` | 6 | 2 | 0 | 0 | false | false | false | 1/1 |
| `CONTROL-intraprocedural-liveness` | 2 | 1 | 2 | 2 | false | false | false | 1/1 |

## Entry points, the source selection, and the unapply

Discovered 2, traversed 2, truncated 0.

`receiveAndReply` returns a `PartialFunction`, so its body compiles into a
synthetic class and the handler's formal parameter in the graph belongs to that
class's `applyOrElse`, not to a method named `receiveAndReply`. Both
are selected, so the difference between them is measured rather than assumed:

- `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)` (2 node(s), graph line 409)
- `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` (2 node(s), graph line 408)

The formal parameter is `Any`-typed, and the `DriverDescription` payload is
recovered by the pattern match at `Master.scala:410` - an **unapply**, which in
bytecode is a type test, a cast and the case class's own `driverDescription`
accessor rather than an assignment. Selecting one side only would leave the flow
count uninterpretable, so **both** are selected, as two arms:

- **ARM 1** - every formal parameter of the entry methods, with the implicit
  receiver (`this`) excluded because it carries the enclosing
  instance rather than the message. The `Any`-typed parameter is identified by its
  erased bytecode type `java.lang.Object` rather than by position;
  1 parameter(s) matched that type.
- **ARM 2** - the unapply-recovered payload. Selection used: primary: call sites of org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription inside the entry methods.

Receiver parameters excluded from ARM 1:

- `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)` index 0, name `this`, type `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1`
- `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` index 0, name `this`, type `org.apache.spark.deploy.master.Master`

## The sink

- `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)` calls `org.apache.spark.deploy.worker.ProcessBuilderLike.start:java.lang.Process()` at graph line 240 (dispatch `DYNAMIC_DISPATCH`)
- `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()` calls `java.lang.ProcessBuilder.start:java.lang.Process()` at graph line 276 (dispatch `DYNAMIC_DISPATCH`)

The sink NODE set is the launch call together with its receiver and its arguments
(2 call, 2 receiver, 2 argument; 4 distinct after de-duplication): a flow that reaches the
value being launched ends at one of those, and taking only the call node would
miss a flow into the receiver.

## The four boundaries, as capability findings

Each hop below is measured against the graph with its own bounded flow traversal,
not asserted. `crossed by a data flow` states whether a flow in fact joins the
hop's two ends.

### B1-rpc - crossed by a data flow: **false** (0 flow(s) found)

- **hop**: RpcEndpointRef.send of org.apache.spark.deploy.DeployMessages$LaunchDriver, Master to Worker
- **from**: `org.apache.spark.deploy.master.Master.launchDriver:void(org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.master.DriverInfo)`
- **to**: `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a message send carries no data edge: the value is serialized out of one process and deserialized into another, so the sender's argument and the receiver's accessor result are two unrelated definitions as far as reaching-definition edges are concerned
- **modelling**: modelled explicitly by pairing on the MESSAGE TYPE - the ARGUMENTS of call sites of org.apache.spark.deploy.DeployMessages$LaunchDriver.<init> are the producer end and the RESULTS of call sites of its field accessors (driverDesc, driverId, resources) are the consumer end, with the message type's and companion's own generated machinery excluded by owning type

### B2-thread - crossed by a data flow: **false** (0 flow(s) found)

- **hop**: org.apache.spark.deploy.worker.DriverRunner.start calls Thread.start(); the route continues in run() on the anonymous Thread subclass
- **from**: `org.apache.spark.deploy.worker.DriverRunner.start:void()`
- **to**: `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
- **reason**: Thread.start() -> run() is a JVM scheduling relation: the start frame returns immediately and run() is entered on another thread, so no data edge joins a definition in the one to a use in the other
- **modelling**: not modelled - the two ends are measured as they stand, with every parameter of the start method INCLUDING the receiver taken as the source set because the method takes no explicit argument and the enclosing instance is the only value that could cross

### B3-interface - crossed by a data flow: **true** (2 flow(s) found)

- **hop**: the launch call site invokes the ABSTRACT ProcessBuilderLike.start; the JDK launch is reached only through the anonymous implementation
- **from**: `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)#240`
- **to**: `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()#276`
- **reason**: an interface invocation names the declaring type, so joining the receiver at the abstract call site to a definition inside the implementation needs the type hierarchy; a reaching-definition edge does not cross that on its own
- **modelling**: not modelled by this query - the receiver and arguments at the abstract call site are the source end, the concrete JDK launch call and its receiver are the sink end, and whether a flow joins them is reported as measured. Query 01 measured this same hop as CROSSED by a call edge, which is why the two measurements are reported separately rather than as one verdict

### B4-partial-function - crossed by a data flow: **false** (0 flow(s) found)

- **hop**: org.apache.spark.deploy.master.Master.receiveAndReply returns a PartialFunction whose body compiles into a synthetic class
- **from**: `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
- **to**: `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: the method named receiveAndReply only constructs the partial function; the case bodies live in the synthetic class's applyOrElse, so a source selected on the source-level name is a definition in a method that contains none of the route, and the payload the body uses arrives through an unapply rather than through that parameter
- **modelling**: modelled by measuring the hop directly: the source-level method's parameters are the source end and the calls the synthetic body makes with the recovered payload (createDriver, with its arguments) are the sink end. ARM 1 and ARM 2 above select BOTH sides as sources in their own right, so the difference between them is measured rather than assumed

Boundaries not crossed by a data flow: `B1-rpc`, `B2-thread`, `B4-partial-function`.

`B3-interface` deserves one explicit note, because it is the hop on which the two
formulations could most easily have parted company. This query measures that hop
for a **data edge** and reports **true**
(2 flow(s) found). The same hop under the call-edge
question belongs to `01-callgraph-unguarded-driver-launch` and is **deliberately not
transcribed here**: that verdict is that query's own measurement, and a second
copy of a number is a number that can drift from its owner. Each query publishes
the verdict it measured, and a reader comparing the two reads them side by side
rather than merged into one verdict.

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

`Master.scala:411`'s `if (state != RecoveryState.ALIVE)` is a **recovery-state**
check and is deliberately not in this set.

The selector block in this query's source is **byte-identical** to
`01-callgraph-unguarded-driver-launch`'s. It has to be: the two spurious counts
are only comparable if the definition of the term is the same text, and the
duplicate-formulation verdict below rests on that comparability.

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

## Whether an expected-spurious route was absent

`spurious_count` = **0**. No route in the emitted set passed an
auth/ACL predicate as defined by these five named selectors.

**The absence is structural, not a consequence of the query filtering well.**
Measured against the graph: 18 call sites of the five
predicates exist graph-wide, in 18 distinct calling
methods, and **0** of them sit on the route
surface (`org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.Worker`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`).
The predicate set exists and is invoked elsewhere in the program; it is not
invoked anywhere on this route, so no route could have passed one.

This is a statement about **this query's own output** under **this query's own**
definition of the term. It is not an assessment of Spark, of any Spark component
or of any configuration, and nothing here should be read as one.

## Whether this formulation duplicates another query's

`duplicate_formulation` = **not_duplicate**.

Every verdict below is **computed at run time**, by applying one shared predicate
to the two queries' own declared formulation identity blocks read out of the two
source files. No verdict about a sibling query is written down in this source, so
there is nothing here that can drift from what that query publishes.

the top-level verdict aggregates the per-query entries below and names the strongest relation any one of them carries: not_duplicate against 01-callgraph-unguarded-driver-launch, not_duplicate against 03-parameterized-handler-sink-pairs. No entry carries a duplicate relation at any scope, so the aggregate is an absence rather than a partial. It was NOT inferred from the file names differing.

### Against `01-callgraph-unguarded-driver-launch` (source sha256 `79583377ffdc05762226f1437be94d953bf44be1ea94bbc3d9e48f072a27f4ac`): not_duplicate

- **Scope.** none.
- **Basis.** the formulations differ on the edge kinds traversed (this query traverses REACHING_DEF where 01-callgraph-unguarded-driver-launch traverses CALL); the node kinds selected as a route's ends (this query selects METHOD_PARAMETER_IN, EXPRESSION where 01-callgraph-unguarded-driver-launch selects METHOD); the bound, as a named kind of quantity and a value (this query bounds call boundaries the backward data-flow search may expand at 6 where 01-callgraph-unguarded-driver-launch bounds call-graph hops expanded from an entry point at 12); the Joern API construct sets (19 construct(s) only here and 4 only there, over 24 shared), while agreeing on the handler/sink pairs addressed (at least one pair in common: pair-one); the entry-point selector literals (identical byte for byte); the sink selector literals (identical byte for byte). Neither traversal establishes the other's conclusion, so the two results are reported side by side and never summed.
- **Edge kinds.** here 1 vs REACHING_DEF - same kinds: false. A call path does not show that a value arrives, and a data path does not show that control can, so neither traversal establishes the other's conclusion where these differ.
- **Route-end node kinds.** here METHOD_PARAMETER_IN, EXPRESSION; there METHOD - same kinds: false.
- **API construct sets.** 19 of this query's 43 constructs do not appear in that query's declared list, and 4 of that query's do not appear here; 24 are shared. The difference is computed from the two sources' own lists rather than eyeballed.
- **Predicate selector literals identical.** true.

  - only here: `AstNode.code`
  - only here: `AstNode.label`
  - only here: `AstNode.lineNumber`
  - only here: `Call.argument`
  - only here: `Call.receiver`
  - only here: `CfgNode.method`
  - only here: `EngineConfig.copy`
  - only here: `EngineConfig.maxCallDepth`
  - only here: `EngineContext.config`
  - only here: `EngineContext.copy`
  - only here: `EngineContext.semantics`
  - only here: `Method.call`
  - only here: `Method.parameter`
  - only here: `MethodParameterIn.index`
  - only here: `MethodParameterIn.method`
  - only here: `MethodParameterIn.name`
  - only here: `MethodParameterIn.typeFullName`
  - only here: `Path.elements`
  - only here: `Traversal.reachableByFlows`
  - only in 01-callgraph-unguarded-driver-launch: `Call.code`
  - only in 01-callgraph-unguarded-driver-launch: `Call.order`
  - only in 01-callgraph-unguarded-driver-launch: `Method.callOut`
  - only in 01-callgraph-unguarded-driver-launch: `NoResolve.getCalledMethodsAsTraversal`

### Against `03-parameterized-handler-sink-pairs` (source sha256 `8f67126c56185bde3221ad760130295cf9f7f64411be528e9fd578a4fbad631e`): not_duplicate

- **Scope.** none.
- **Basis.** the formulations differ on the edge kinds traversed (this query traverses REACHING_DEF where 03-parameterized-handler-sink-pairs traverses CALL); the node kinds selected as a route's ends (this query selects METHOD_PARAMETER_IN, EXPRESSION where 03-parameterized-handler-sink-pairs selects METHOD); the bound, as a named kind of quantity and a value (this query bounds call boundaries the backward data-flow search may expand at 6 where 03-parameterized-handler-sink-pairs bounds call-graph hops expanded from an entry point at 12); the Joern API construct sets (19 construct(s) only here and 4 only there, over 24 shared), while agreeing on the handler/sink pairs addressed (at least one pair in common: pair-one); the entry-point selector literals (identical byte for byte); the sink selector literals (identical byte for byte). Neither traversal establishes the other's conclusion, so the two results are reported side by side and never summed.
- **Edge kinds.** here 1 vs REACHING_DEF - same kinds: false. A call path does not show that a value arrives, and a data path does not show that control can, so neither traversal establishes the other's conclusion where these differ.
- **Route-end node kinds.** here METHOD_PARAMETER_IN, EXPRESSION; there METHOD - same kinds: false.
- **API construct sets.** 19 of this query's 43 constructs do not appear in that query's declared list, and 4 of that query's do not appear here; 24 are shared. The difference is computed from the two sources' own lists rather than eyeballed.
- **Predicate selector literals identical.** true.

  - only here: `AstNode.code`
  - only here: `AstNode.label`
  - only here: `AstNode.lineNumber`
  - only here: `Call.argument`
  - only here: `Call.receiver`
  - only here: `CfgNode.method`
  - only here: `EngineConfig.copy`
  - only here: `EngineConfig.maxCallDepth`
  - only here: `EngineContext.config`
  - only here: `EngineContext.copy`
  - only here: `EngineContext.semantics`
  - only here: `Method.call`
  - only here: `Method.parameter`
  - only here: `MethodParameterIn.index`
  - only here: `MethodParameterIn.method`
  - only here: `MethodParameterIn.name`
  - only here: `MethodParameterIn.typeFullName`
  - only here: `Path.elements`
  - only here: `Traversal.reachableByFlows`
  - only in 03-parameterized-handler-sink-pairs: `Call.code`
  - only in 03-parameterized-handler-sink-pairs: `Call.order`
  - only in 03-parameterized-handler-sink-pairs: `Method.callOut`
  - only in 03-parameterized-handler-sink-pairs: `NoResolve.getCalledMethodsAsTraversal`

**What each formulation can return, measured in this run.** Measured on this query's own output: 4 element-level flow record(s), each a sequence of IDENTIFIER, METHOD_PARAMETER_IN and CALL nodes with their graph lines - including 2 from a formal parameter to the launch call inside a single method - and 3 of the 4 measured hops not crossed by a data flow (B1-rpc, B2-thread, B4-partial-function). A record of this kind is a shape a method-level call-edge traversal does not produce for any input.

a SYMMETRIC pairwise relation: the verdict this envelope states against a query is the same verdict that query's envelope states against this one. It is one measurement cited twice rather than two measurements, and here it is symmetric BY CONSTRUCTION rather than by transcription - every entry below is computed by applying ONE shared predicate to the two queries' own declared formulation identity blocks, read out of the two SOURCE files at run time under names all three queries share. Both directions therefore evaluate identical inputs through identical code, so a disagreement between them is not expressible; a transcribed verdict could disagree with the envelope it was copied from, which is exactly what this replaces.

Their results are reported side by side and are **never summed**.

## The three effort measures

1. **Query revisions committed: 3.** Convention: commits touching queries/joern/02-dataflow-unguarded-driver-launch.sc in the history of the HEAD this run measured at, newest first, counted at run time from the repository's own history. ONE convention, with three parts that make the number reproducible: the range is HEAD's own ancestry, named explicitly rather than defaulted, and the HEAD and the branch it was on are published beside the count; every commit returned is verified to be an ancestor of that HEAD, so a commit reachable only from another ref cannot enter the count - which is what happened to earlier figures once per-clone branches were reconciled and the commits a previous run had listed stopped being ancestors of the branch carrying its files; and the commit that PUBLISHES these result files is necessarily not among them, because it cannot exist while the run that writes them is still in progress. A later reader whose git log shows one more commit than the count reconciles against that window rather than against a bare number.
   Measurement: commits touching this path in HEAD's own history, newest first, every one verified an ancestor of the HEAD published beside this count.
   The commits counted, newest first, so the number is auditable rather than
   asserted:

   - `0e3e742a5ad2cb057fd2ebafb6f6a0137c82d21b`
   - `232d0d9cca3f15d33cedb96fa18dac3c6602668b`
   - `675f691eca921b2b7114029d97103ee8838a91b8`

2. **Distinct Joern API constructs used: 43.** Listed
   explicitly and deduplicated so the count is auditable from the list rather
   than asserted. Each entry was searched for in this query's own source text
   with the list's own declaration excised first, so no entry can satisfy
   itself: 43 of 43 were
   confirmed.

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
   - `EngineConfig.copy`
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

3. **Parameterizability: not claimed here.** It is proven by
   `03-parameterized-handler-sink-pairs` actually invoking its parameterized form on the
   second named handler/sink pair (the `deploy/rest/StandaloneRestServer` handler
   to the `deploy/worker/DriverRunner` sink) and capturing that invocation's
   result. A parameter list that merely exists does not satisfy it.

## Modelling decisions, stated so the counts stay interpretable

- **Two source sets, two arms.** The `Any`-typed formal parameter and the
  unapply-recovered payload are different nodes, so they are evaluated separately
  and reported separately rather than unioned into one number.
- **The implicit receiver is excluded from ARM 1 and included in B2.** It carries
  the enclosing instance rather than the message, so it is not a handler input;
  but `DriverRunner.start` takes no explicit argument, so excluding it there would
  make that measurement vacuous. Both choices are stated where they apply.
- **Operator pseudo-calls are excluded.** A CPG `<operator>.*` call is an artefact
  of the representation rather than a method call.
- **Duplicate class definitions are unioned.** The graph carries more than one node
  per class where two staged archives carried the same class, so method nodes are
  grouped by full name and their parameters and calls unioned rather than one node
  being picked.
- **The flow engine's context is the console's own, copied.** Only the call-depth
  bound is overridden, so the semantics the traversals run under are the same ones
  the dataflow overlay was built with (`io.joern.dataflowengineoss.semanticsloader.FullNameSemantics`), and the context is
  passed explicitly at every call site so no implicit resolution decides it.
- **Graph line numbers are the graph's own.** A node's `lineNumber` comes from the
  bytecode line-number table and can differ by a line from the `def` or statement
  line cited from the source. Source anchors in this report are quoted from the
  pinned tree; graph lines are labelled as such.
- **Element code is collapsed and capped.** A flow element's `code` is put on one
  line and capped at 160 characters, so the record stays readable and
  the emitted JSON stays deterministic.

## Reproducing this

Precondition: run from a checkout of this branch after `BLITZY_CLONE_INDEX=<this clone's index> ; . harness/env.sh`, which exports $HARNESS_REPO_ROOT, $HARNESS_CPG and $HARNESS_SCRATCH_DIR - the three values the command below reads.

```
cd "$HARNESS_SCRATCH_DIR" && HARNESS_REPO_ROOT="$HARNESS_REPO_ROOT" HARNESS_CPG="$HARNESS_CPG" JAVA_HOME="$JAVA_HOME_21" JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script "$HARNESS_REPO_ROOT/queries/joern/02-dataflow-unguarded-driver-launch.sc" -J-Xmx64g < /dev/null
```

That is the **whole** command and it is runnable as written: the working
directory, the repository root, **the graph selector**, the JDK, the heap
override, the log level, the script path and the closed stdin. Every environment
value this query reads appears in it - `$HARNESS_REPO_ROOT`, `$HARNESS_CPG` and
`$HARNESS_SCRATCH_DIR` - and it reads no other. They are written as variable
references rather than as literal paths because an absolute path is a property of
a checkout rather than of the measurement, and this report is held to
byte-identity across checkouts; sourcing `harness/env.sh` exports all three.

`$HARNESS_CPG` is named explicitly because it selects the graph bytes the
query loads: a reader with it pointing at another graph reproduces a different
load, and a command that omitted it would leave its most consequential input
invisible. There is still no variable that selects the identity record. The
record of account is resolved by provenance - the in-checkout frontend log where
it carries a write-time `bytes:`/`sha256:` pair, and otherwise the provisioning
record beside the resolved graph - and both are reached through values this
command names. For this run it was `provision-log/cpg-identity.txt`.

`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so
`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs
at; where a runner defaults below the floor it is raised through its own documented
environment override. The query measures the heap it received and stops below the
floor: raising a heap is permitted and reported, lowering one is not.
