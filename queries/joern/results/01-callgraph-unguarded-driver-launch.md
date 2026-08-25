# Joern capability probe 01-callgraph-unguarded-driver-launch

Bounded **call-graph** reachability from the Spark standalone Master's
driver-submission handler to the privileged process launch hosted on the
`DriverRunner` surface, over the code-property graph built from the pinned tree's
bytecode.

This report is **observational**. It judges no finding - not real, not important,
not a false positive, not a duplicate - and makes no comparison between tools. It
contributes no row to `oss-scan-results/findings.json` and writes nothing into
`harness/artifacts/raw/`.

The slug `01-callgraph-unguarded-driver-launch` is the **identifier**
the plan assigns this query. It names the question the query was written to ask -
whether a call-graph formulation can join this handler to this sink, and whether
any route it returns passes one of five named predicates first. It is not a
finding, and nothing in this report should be read as an assessment of Spark, of
any Spark component or of any Spark configuration.

| | |
| --- | --- |
| Query source | `queries/joern/01-callgraph-unguarded-driver-launch.sc` |
| Envelope | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` |
| Console log | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21 |
| Heap actually used | 68719476736 bytes (floor 68719476736) |
| Graph | 541255894 bytes, sha256 `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` |
| Graph identity re-verified before the load | yes, against `harness/artifacts/logs/cpg-frontend.log` |
| Graph methods / typeDecls / files | 1397339 / 119691 / 45037 |
| Compile status | compiled |
| Run status | completed |
| Records returned | 4 (4 boundary, 0 route) |
| Distinct routes | 0 |
| Spurious routes | 0 |

## The result

**Distinct routes: 0.** Routes are counted distinct on
(entry point, sink host, hop sequence) across both walks below and are **never
summed**.

No route from an entry point to a sink host was returned within the stated
bound. That is a capability finding about what this formulation can express
over this graph, and it is reported as measured: the bound was not loosened,
removed or re-run unbounded to produce a non-empty result. The four boundaries
below are the measured reason.

## Whether the bound was reached

`bound_reached` = **true**. The primary bound is `MAX_CALL_DEPTH` = 12
call-graph hops from an entry point. Every traversal in this query carries an
explicit named bound; none runs unbounded.

Which bound bit, per walk, so the flag is interpretable rather than bare:

- walk `A-follows-fan-out`: the frontier was still non-empty at depth 12. Expansion budget used 25009 of 200000 per entry point; routes returned 0 of 64.
- walk `B-fan-out-recorded`: the frontier was still non-empty at depth 12. Expansion budget used 5598 of 200000 per entry point; routes returned 0 of 64.

A depth bound reached with a non-empty frontier says only that the walk stopped
expanding, so on its own it would leave open whether a deeper walk would reach a
sink host. What settles that here is the boundary measurement below rather than
the bound: the hops that break this route are not CALL edges at all, and no
increase in depth introduces an edge that does not exist. The bound is therefore
reported as reached, and the absence of a route is attributed to the measured
boundaries, not to the bound.

| bound | value |
| --- | --- |
| MAX_CALL_DEPTH | 12 |
| MAX_ROUTES | 64 |
| MAX_EXPANSIONS_PER_ENTRY | 200000 |
| MAX_TOTAL_RETURNS | 256 |
| MAX_ENTRY_POINTS | 16 |
| MAX_CALL_SCAN | 200000 |
| FANOUT_CALLEE_THRESHOLD | 32 |

| walk | follows fan-out | expansions | call sites | fan-out seen | fan-out not followed | max depth | depth bound reached | budget exhausted | routes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `A-follows-fan-out` | true | 25009 | 33565 | 86 | 0 | 12 | true | false | 0 |
| `B-fan-out-recorded` | false | 5598 | 11575 | 55 | 55 | 12 | true | false | 0 |

## Entry points, and how they were selected

Discovered 2, traversed 2, truncated 0.

`receiveAndReply` returns a `PartialFunction`, so its body compiles into a
synthetic class and the entry point in the graph is that class's
`applyOrElse`, not a method named `receiveAndReply`. Both are
selected, so the difference between them is measured rather than assumed:

- `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)` (2 node(s), graph line 409)
- `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` (2 node(s), graph line 408)

## The sink

- `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)` calls `org.apache.spark.deploy.worker.ProcessBuilderLike.start:java.lang.Process()` at graph line 240 (dispatch `DYNAMIC_DISPATCH`)
- `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()` calls `java.lang.ProcessBuilder.start:java.lang.Process()` at graph line 276 (dispatch `DYNAMIC_DISPATCH`)

Sink host methods a route must reach: `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)`, `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`.

## The four boundaries, as capability findings

Each hop below is measured against the graph, not asserted. `crossed` states
whether a CALL edge in fact joins the two ends.

### B1-rpc - crossed by a call edge: **false**

- **hop**: RpcEndpointRef.send of org.apache.spark.deploy.DeployMessages$LaunchDriver, Master to Worker
- **from**: `org.apache.spark.deploy.master.Master.launchDriver:void(org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.master.DriverInfo)`
- **to**: `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a message send is not a call: the sender enqueues a value on an endpoint reference and the receiving handler is dispatched later, so no CALL edge joins the two ends
- **modelling**: modelled explicitly by pairing on the MESSAGE TYPE - call sites of org.apache.spark.deploy.DeployMessages$LaunchDriver.<init> are the producer end and call sites of its field accessors (driverDesc, driverId, resources) are the consumer end, with the message type's and companion's own generated machinery excluded by owning type

### B2-thread - crossed by a call edge: **false**

- **hop**: org.apache.spark.deploy.worker.DriverRunner.start calls Thread.start(); the route continues in run() on the anonymous Thread subclass
- **from**: `org.apache.spark.deploy.worker.DriverRunner.start:void()`
- **to**: `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
- **reason**: Thread.start() -> run() is a JVM scheduling relation, not a call: the start frame returns immediately and run() is entered on another thread, so no CALL edge joins them
- **modelling**: not modelled - the two ends are reported as measured and the hop is left uncrossed

### B3-interface - crossed by a call edge: **true**

- **hop**: the launch call site invokes the ABSTRACT ProcessBuilderLike.start; the JDK launch is reached only through the anonymous implementation
- **from**: `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **to**: `java.lang.ProcessBuilder.start:java.lang.Process(), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **reason**: an interface invocation names the declaring type, so linking it to an implementation needs the type hierarchy rather than the call's own name
- **modelling**: not modelled by this query - whether the hop is crossed is a property of the graph's call linker and is reported as measured

### B4-partial-function - crossed by a call edge: **false**

- **hop**: org.apache.spark.deploy.master.Master.receiveAndReply returns a PartialFunction whose body compiles into a synthetic class
- **from**: `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
- **to**: `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: the method named receiveAndReply only constructs the partial function; the case bodies live in the synthetic class's applyOrElse, so a selector on the source-level name would traverse from a method that contains none of the route
- **modelling**: modelled by selecting BOTH: the synthetic applyOrElse on every type matching ^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$, and the source-level receiveAndReply, so the difference between them is measured rather than assumed

Boundaries not crossed: `B1-rpc`, `B2-thread`, `B4-partial-function`.

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

`duplicate_formulation` = **not_duplicate**.

- Against `02-dataflow-unguarded-driver-launch`: the **same** handler/sink pair by
  a **different** formulation. This query traverses CALL edges only and asserts
  nothing about data flow. The two are two formulations of one question, so their
  returns are reported side by side and **never summed**.
- Against `03-parameterized-handler-sink-pairs`: a different target set and a different
  formulation - that query is parameterized over handler/sink pairs and covers a
  second pair this query does not address.

## The three effort measures

1. **Query revisions committed: 1.** Convention: commits touching queries/joern/01-callgraph-unguarded-driver-launch.sc from its first appearance to the end of the probe. This run introduces the file in a single commit.
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

3. **Parameterizability: not claimed here.** It is proven by
   `03-parameterized-handler-sink-pairs` actually invoking its parameterized form on the
   second named handler/sink pair (the `deploy/rest/StandaloneRestServer` handler
   to the `deploy/worker/DriverRunner` sink) and capturing that invocation's
   result. A parameter list that merely exists does not satisfy it.

## Modelling decisions, stated so the counts stay interpretable

- **Operator pseudo-calls are excluded.** A CPG `<operator>.*` call is an
  artefact of the representation, not a method call, and expanding them would
  inflate every counter without adding a call-graph hop.
- **Duplicate class definitions are unioned.** The graph carries more than one
  node per class where two staged archives carried the same class, so method
  nodes are grouped by full name and their call sites unioned rather than one
  node being picked. Reachability is keyed on the method full name.
- **Callee resolution is explicit.** Each call site's callees are taken from
  `NoResolve.getCalledMethodsAsTraversal`, which is exactly the statically
  linked CALL-edge callees of that site.
- **Two walks, reported side by side.** Walk `A-follows-fan-out` expands every
  call site. Walk `B-fan-out-recorded` records but does not expand a call site
  whose resolved callee set exceeds 32 distinct methods:
  expanding such a site models "any implementation in the program may be
  invoked here", which is a property of the call linker rather than of this
  route. Both walks' counters are published above and their routes are
  deduplicated, never summed.
- **Graph line numbers are the graph's own.** A method or call node's
  `lineNumber` comes from the bytecode line-number table and can differ by a
  line from the `def` or statement line cited from the source. Source anchors in
  this report are quoted from the pinned tree; graph lines are labelled as such.
- **A bytecode file path is not a source path.** The frontend records an
  extraction path under a temporary directory for every class, so this query
  reports types, methods and lines rather than presenting that path as a source
  location.

## Reproducing this

```
cd <a scratch directory outside the repository>
HARNESS_REPO_ROOT=<repo> JAVA_HOME="$JAVA_HOME_21" \
  JAVA_TOOL_OPTIONS="-Xmx64g" SL_LOGGING_LEVEL=WARN \
  joern --script <repo>/queries/joern/01-callgraph-unguarded-driver-launch.sc -J-Xmx64g < /dev/null
```

`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so
`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs
at. The query measures the heap it received and stops below the floor: raising a
heap is permitted and reported, lowering one is not.

