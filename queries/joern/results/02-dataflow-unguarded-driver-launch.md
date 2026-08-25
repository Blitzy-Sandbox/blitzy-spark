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
| Envelope | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` |
| Console log | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21 |
| Heap actually used | 68719476736 bytes (floor 68719476736) |
| Graph | 541255894 bytes, sha256 `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` |
| Graph identity re-verified before the load | yes, against `harness/artifacts/logs/cpg-frontend.log` |
| Graph methods / typeDecls / files | 1397339 / 119691 / 45037 |
| Flow engine semantics | `io.joern.dataflowengineoss.semanticsloader.FullNameSemantics` |
| Compile status | compiled |
| Run status | completed |
| Records returned | 8 (4 boundary, 0 route, 2 boundary-flow, 2 control) |
| Distinct routes | 0 |
| Spurious routes | 0 |
| Dataflow layer live on this sink | true |

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
| MAX_FLOW_LENGTH | 64 |
| MAX_FLOWS_PER_PAIR | 8 |
| MAX_STEPS_PER_SOURCE | 8 |
| MAX_TOTAL_RETURNS | 256 |
| MAX_SOURCE_NODES | 64 |
| MAX_SINK_NODES | 64 |
| MAX_ENTRY_POINTS | 16 |
| MAX_CALL_SCAN | 200000 |
| MAX_CODE_CHARS | 160 |

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
formulations could most easily have parted company. `01-callgraph-unguarded-driver-launch`
measured it as **crossed by a call edge**; this query measures the same hop for a
**data edge** and reports **true**
(2 flow(s) found). Two measurements of one hop under two
different questions, reported separately rather than merged into one verdict.

Across all four hops the two formulations' verdicts
**agree**. Query 01's verdicts are transcribed from its published envelope; this query's are
measured. Agreement is a result, not a foregone conclusion, and it does not make
the two one formulation - see the verdict below.

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
surface (`org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.rest.StandaloneRestServer`, `org.apache.spark.deploy.worker.DriverRunner`).
The predicate set exists and is invoked elsewhere in the program; it is not
invoked anywhere on this route, so no route could have passed one.

This is a statement about **this query's own output** under **this query's own**
definition of the term. It is not an assessment of Spark, of any Spark component
or of any configuration, and nothing here should be read as one.

## Whether this formulation duplicates another query's

`duplicate_formulation` = **not_duplicate**, answered against both
other queries on evidence rather than by assertion.

### Against `01-callgraph-unguarded-driver-launch`: not_duplicate

The same handler/sink pair, and four properties that differ:

- **Edge kinds traversed.** This query walks reaching-definition (data) edges
  through the OSS dataflow layer; query 01 walks CALL edges. Neither traversal
  establishes the other's conclusion: a call path does not show that a value
  arrives, and a data path does not show that control can.
- **Entry-point granularity.** This query's ends are PARAMETER and EXPRESSION
  nodes; query 01's are whole METHODS. The two do not select the same nodes.
- **What each can return, measured in this run.** The two formulations AGREE on all four boundary verdicts in this run (both report B1-rpc, B2-thread, B4-partial-function uncrossed), and agreement on a verdict is not identity of formulation. The measured difference is in what each can RETURN: this query emitted 4 element-level flow record(s), each a sequence of IDENTIFIER, METHOD_PARAMETER_IN and CALL nodes with their graph lines - including 2 from a formal parameter to the launch call inside a single method - and a method-level call-edge traversal produces no such record for any input, which is why query 01 published none.
- **API construct sets.** 18 of this query's 42 constructs
  do not appear in query 01's published list, and 4 of query 01's do not
  appear here. The difference is computed from the two lists rather than eyeballed:

  - only here: `AstNode.code`
  - only here: `AstNode.label`
  - only here: `AstNode.lineNumber`
  - only here: `Call.argument`
  - only here: `Call.receiver`
  - only here: `CfgNode.method`
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
  - only in query 01: `Call.code`
  - only in query 01: `Call.order`
  - only in query 01: `Method.callOut`
  - only in query 01: `NoResolve.getCalledMethodsAsTraversal`

Their results are reported side by side and are **never summed**.

### Against `03-parameterized-handler-sink-pairs`: not_duplicate

A different target set and a different formulation: 03-parameterized-handler-sink-pairs is parameterized over handler/sink pairs and covers a second pair this query does not address (the deploy/rest/StandaloneRestServer handler to the deploy/worker/DriverRunner sink). This query is fixed to one pair and to the dataflow formulation of it, so neither subsumes the other and their returns are likewise never summed.

## The three effort measures

1. **Query revisions committed: 1.** Convention: commits touching queries/joern/02-dataflow-unguarded-driver-launch.sc from its first appearance to the end of the probe. This run introduces the file in a single commit.
2. **Distinct Joern API constructs used: 42.** Listed
   explicitly and deduplicated so the count is auditable from the list rather than
   asserted; every entry appears literally in the query source:

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

```
cd <a scratch directory outside the repository>
HARNESS_REPO_ROOT=<repo> JAVA_HOME="$JAVA_HOME_21" \
  JAVA_TOOL_OPTIONS="-Xmx64g" SL_LOGGING_LEVEL=WARN \
  joern --script <repo>/queries/joern/02-dataflow-unguarded-driver-launch.sc -J-Xmx64g < /dev/null
```

`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so
`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs
at; where a runner defaults below the floor it is raised through its own documented
environment override. The query measures the heap it received and stops below the
floor: raising a heap is permitted and reported, lowering one is not.

