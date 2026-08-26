# Joern capability probe 01-callgraph-unguarded-driver-launch

Bounded **call-graph** reachability from the Spark standalone Master's
driver-submission handler to the privileged process launch hosted on the
`DriverRunner` surface, over the code-property graph built from the pinned tree's
bytecode.

This report is **observational**. It judges no finding - not real, not important,
not a false positive, not a duplicate - and makes no comparison between tools. It
contributes no row to `oss-scan-results/findings.json` and writes nothing into
`harness/artifacts/raw/`. This probe tree is Joern's deliberate **second**
appearance in the run - the Stage 3 runner is the first - and folding either
appearance into the other's numbers would corrupt both that tool's count and the
dataset total, which is why nothing here becomes a dataset row.

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
| JDK major | 21, the required major (JVM `21.0.12.1+1-LTS`) |
| Heap actually used | 68719476736 bytes = 64 GiB (floor 68719476736 = 64 GiB; at or above the floor) |
| Heap-bound JVM position | 4 of 4 - the frontend build, the `importCpg` verification load, the Stage 3 Joern runner, then this probe |
| Graph | `$HARNESS_CPG` (repository-relative `harness/cpg/spark.cpg`), symlink-followed: 541255894 bytes, sha256 `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` |
| Graph identity re-verified before the load | yes, against `harness/artifacts/logs/cpg-frontend.log`, which owns that pair |
| Graph methods / typeDecls / files | 1397339 / 119691 / 45037 |
| Compile status | compiled |
| Run status | completed |
| Records returned | 4 (4 boundary, 0 route) |
| Distinct routes | 0 |
| Spurious routes | 0 |

The query reached the graph through **`importCpg` and nothing else**. That is a
textual property of the committed sources as well as a behavioural one about this
run: the alternative loader - the one that compiles source afresh and, on Joern's
own documented behaviour, spawns a second JVM at the same heap - is invoked in
**none** of the three committed query sources under `queries/joern/`, and the
absence was checked by searching those files rather than inferred from what this
run happened to do.

**This report measures nothing.** Every figure in it is **read from**
`queries/joern/results/01-callgraph-unguarded-driver-launch.json`, which in turn
cites the run that measured them; the handful of node counts and graph line
numbers that envelope does not itself carry are cited from
`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`, the same
console stream the envelope names as the source of its own measured values.
Nothing here is a second measurement: where a count appears both here and in that
envelope it is one measurement cited twice, and if the two ever disagreed the
envelope would be right and this file wrong. Source **line numbers** are a
different kind of fact - they are quoted from the pinned tree at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` and were each re-verified there.

## The result

**Distinct routes: 0.** Routes are counted distinct on
(entry point, sink host, hop sequence) across both walks below and are **never
summed** - not the two walks with each other, and not this query's routes with
`02-dataflow-unguarded-driver-launch`'s returns or with
`03-parameterized-handler-sink-pairs`' per-pair figures, despite the pair those
two share with this one. Distinct routes are reported; summed returns are not.

No route from an entry point to a sink host was returned within the stated
bound. **A zero result here is a finding, not a failure**: it is a capability
finding about what this formulation can express over this graph, and it is
reported as measured - the bound was not loosened, removed or re-run unbounded to
produce a non-empty result, and no route was manufactured. The four boundaries
below are where the measurement lands: three of them are not crossed by a call
edge, and that is the reason the pair is not call-graph-connected.

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

Every named bound the query declares is reported below with its value **and**
whether it was reached, so no bound is left as a value nobody checked. Each is a
named `val` in the query source; no inline literal governs behaviour.

| bound | value | reached | on what basis |
| --- | --- | --- | --- |
| MAX_CALL_DEPTH | 12 | **yes** | `depth_bound_reached` is true in both walks and `max_depth_used` equals 12 in both, so the frontier was still non-empty at the bound |
| MAX_ROUTES | 64 | no | `route_cap_reached` is false in both walks |
| MAX_EXPANSIONS_PER_ENTRY | 200000 | no | the per-entry-point step cap, counted in method expansions rather than edges; the highest expansion count was 25009 of 200000 |
| MAX_TOTAL_RETURNS | 256 | no | the total-returns cap across every record kind this query emits; 4 records of 256, and `total_returns_cap_reached` is false |
| MAX_ENTRY_POINTS | 16 | no | 2 entry points discovered, 2 traversed, 0 truncated |
| MAX_CALL_SCAN | 200000 | no | 1232 calls named `start` scanned of 200000, and the sweep reported `truncated=false` |
| FANOUT_CALLEE_THRESHOLD | 32 | **exceeded** | a *threshold* rather than a cap, so "reached" means exceeded: 86 fan-out sites in the walk that follows them, 55 in the walk that records them without following |

| walk | follows fan-out | expansions | call sites | fan-out seen | fan-out not followed | max depth | depth bound reached | budget exhausted | routes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `A-follows-fan-out` | true | 25009 | 33565 | 86 | 0 | 12 | true | false | 0 |
| `B-fan-out-recorded` | false | 5598 | 11575 | 55 | 55 | 12 | true | false | 0 |

Two of the seven registered: the depth bound, and the fan-out threshold, which is
a classifier rather than a limit on work. The per-entry-point step cap, the
route cap, the total-returns cap, the entry-point cap and the call-scan cap were
all well inside their limits, so nothing this query returned was trimmed by them
and no result below is a truncated view of a larger one.

## Entry points, and how they were selected

**Discovered 2, traversed 2, truncated 0.** The two counters exist so that a
sweep cannot run unbounded and so that a trimmed traversal cannot pass for a
complete one. A truncated count above zero is a measured property of the
traversal and would be reported as such rather than hidden; it is zero here
because 2 entry points were discovered against a cap of 16, so every entry point
discovered was traversed and none was dropped.

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

## The chain the traversal can follow, for context

Every line below is the **pinned tree's**, re-verified against `$SPARK_SRC` at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`. The point of stating it is that the
empty result is not the traversal failing to move at all: inside
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala` the call chain
is ordinary static calls, and a call-graph formulation follows it without
difficulty.

`:409` `override def receiveAndReply` -> `:410` `case RequestSubmitDriver(description)`
-> `:411` the recovery-state check -> `:415` the branch taken when the state is
`ALIVE` -> `:417` `val driver = createDriver(description)` (definition at `:1356`)
and `:421` `schedule()` -> `:944` `private def schedule()` -> `:967` and `:986`
`launchDriver(worker, driver)` (each inside a `canLaunchDriver` check, that method
declared at `:923` and called at `:964` and `:983`) -> `:1363`
`private def launchDriver` -> `:1367` the message send.

A second path arrives at the same place: `:1121` `private def relaunchDriver`
reaches the same `createDriver` at `:1130`.

So **at least three call hops** separate the handler from the message send -
`receiveAndReply` to `schedule` to `launchDriver` to the send - which is why the
depth bound of 12 is load-bearing rather than decorative: a bound of one or two
would have stopped the walk before the send on arithmetic alone, and the empty
result would then have been an artefact of the bound rather than a measurement of
the graph. The walk stops where it does for the reasons below instead.

## The four boundaries, as capability findings

Each hop below is measured against the graph, not asserted. `crossed` states
whether a CALL edge in fact joins the two ends.

### B1-rpc - crossed by a call edge: **false**

- **hop**: RpcEndpointRef.send of org.apache.spark.deploy.DeployMessages$LaunchDriver, Master to Worker
- **from**: `org.apache.spark.deploy.master.Master.launchDriver:void(org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.master.DriverInfo)`
- **to**: `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: a message send is not a call: the sender enqueues a value on an endpoint reference and the receiving handler is dispatched later, so no CALL edge joins the two ends
- **modelling**: modelled explicitly by pairing on the MESSAGE TYPE - call sites of org.apache.spark.deploy.DeployMessages$LaunchDriver.<init> are the producer end and call sites of its field accessors (driverDesc, driverId, resources) are the consumer end, with the message type's and companion's own generated machinery excluded by owning type
- **pinned source**: the send is
  `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1367`
  `worker.endpoint.send(LaunchDriver(driver.id, driver.desc, driver.resources))`;
  the receiving end is
  `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:523`
  `override def receive` at its `:687` `case LaunchDriver(driverId, driverDesc, resources_)`,
  which constructs a `DriverRunner` at `:689` and calls `driver.start()` at `:701`.
  The message type is `case class LaunchDriver` at
  `core/src/main/scala/org/apache/spark/deploy/DeployMessage.scala:176`, inside the
  `object DeployMessages` declared at `:34`, so the bytecode type the query selects
  on is `org.apache.spark.deploy.DeployMessages$LaunchDriver`. Those two
  `Worker.scala` lines are the pinned tree's; the working checkout this report is
  committed in carries the same two constructs eleven lines lower on that one
  file, and the pinned values are the ones that were probed
- **measured**: 1 producer call site and 3 consumer call sites; direct call edge from producer to consumer: **false**

### B2-thread - crossed by a call edge: **false**

- **hop**: org.apache.spark.deploy.worker.DriverRunner.start calls Thread.start(); the route continues in run() on the anonymous Thread subclass
- **from**: `org.apache.spark.deploy.worker.DriverRunner.start:void()`
- **to**: `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
- **reason**: Thread.start() -> run() is a JVM scheduling relation, not a call: the start frame returns immediately and run() is entered on another thread, so no CALL edge joins them
- **modelling**: not modelled - the two ends are reported as measured and the hop is left uncrossed
- **pinned source**: `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:123`
  `}.start()`, which closes the
  `new Thread("DriverRunner for " + driverId) { override def run(): Unit = { ... } }`
  opened at `:89` inside `private[worker] def start()` at `:88`, the `run()` body
  beginning at `:90`. `run()` is entered by the JVM rather than by a static call
  from `start()`
- **measured**: 1 `Thread.start()` call site, dispatch `DYNAMIC_DISPATCH`; call edge from `start` to `run`: **false**

### B3-interface - crossed by a call edge: **true**

- **hop**: the launch call site invokes the ABSTRACT ProcessBuilderLike.start; the JDK launch is reached only through the anonymous implementation
- **from**: `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **to**: `java.lang.ProcessBuilder.start:java.lang.Process(), org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
- **reason**: an interface invocation names the declaring type, so linking it to an implementation needs the type hierarchy rather than the call's own name
- **modelling**: not modelled by this query - whether the hop is crossed is a property of the graph's call linker and is reported as measured
- **pinned source**: `runDriver` at
  `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:207` calls
  `runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)` at
  `:221`; `runCommandWithRetry`, declared at `:224`, reaches the sink
  `process = Some(command.start())` at `:240` through the abstract
  `def start(): Process` at `:270` on the trait declared at `:269`, whose sole
  implementation is the anonymous class created at `:275` with its
  `override def start(): Process = processBuilder.start()` at `:276`. Crossing it
  therefore needs interface and virtual-dispatch resolution rather than the call's
  own name
- **measured**: 2 sink call sites, dispatch `DYNAMIC_DISPATCH`; 1 abstract declaration reached, 2 concrete implementations reached; call edge from interface to implementation: **true** - this is the one of the four hops the graph's linker does cross

### B4-partial-function - crossed by a call edge: **false**

- **hop**: org.apache.spark.deploy.master.Master.receiveAndReply returns a PartialFunction whose body compiles into a synthetic class
- **from**: `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
- **to**: `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
- **reason**: the method named receiveAndReply only constructs the partial function; the case bodies live in the synthetic class's applyOrElse, so a selector on the source-level name would traverse from a method that contains none of the route
- **modelling**: modelled by selecting BOTH: the synthetic applyOrElse on every type matching ^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$, and the source-level receiveAndReply, so the difference between them is measured rather than assumed
- **pinned source**: `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409`
  `override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]`,
  with its first case at `:410`. `receive` and `receiveAndReply` are
  `PartialFunction[Any, Unit]` literals, and Scala 2.13 compiles such a literal
  into a synthetic `$$anonfun$` class whose `applyOrElse` carries the case bodies,
  so the handler body is not a directly named method of `Master` in bytecode
- **measured**: the source-level name has 1 call site and its only callee is the synthetic class's constructor; the synthetic name has 131. Route body reached from the source-level name: **false**; from the synthetic name: **true** - which is why selecting the source-level name alone would have traversed from a method containing none of the route

Boundaries not crossed: `B1-rpc`, `B2-thread`, `B4-partial-function`.
`B3-interface` is crossed by a call edge, so three of the four modelled hops are
uncrossed and one is crossed. Nothing here was worked around: each hop is
reported with the verdict the graph gave it.

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

Each predicate reaches the graph as **a composition of selectors**, and it is
worth naming which source-level construct produced which, because that is how a
Scala declaration becomes a bytecode-level predicate:

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
- a **call-site selector** built on the result: the predicate's *call sites*, not
  its declarations, are what a route would have to pass, so the route-surface
  measurement below counts call sites rather than methods

### The set is exactly five, and was not widened

Two auth-adjacent Boolean methods on the very same anchored type are deliberately
**not** selectors: `isEncryptionEnabled()` at
`core/src/main/scala/org/apache/spark/SecurityManager.scala:280` and
`isSslRpcEnabled()` at `:295`. Neither is an authorization or ACL predicate, and
adding either would change what the word "spurious" counts here.

The selector block is held **byte-identical across all three probe queries** so
that their three spurious counts stay comparable with one another. That is the
reason the set is constrained rather than convenient: widening it in one query
alone would silently make one count mean something the other two do not.

### How the bytecode-level selector was constrained

The anchored selector is `^(check.*Permissions|acls.*|isAuthenticationEnabled)$`, paired with a type selector on
`org.apache.spark.SecurityManager`. On **bytecode** that is not enough. `SecurityManager.scala:59`
declares `private var aclsOn`, and Scala compiles a private var into accessors, so
the graph carries **both** a getter `aclsOn()` and a setter
`aclsOn_$eq(boolean)` - and both names satisfy the `acls.*` alternative. The
setter is what the `_$eq` exclusion removes; the getter is what the intersection
with the five removes. Neither is a predicate, and a naive `acls.*` pattern would
have taken both.

The narrowing is therefore three steps, and all three sets are
reported so it is auditable rather than asserted:

1. broad anchored selector on the 252 method nodes (107 distinct names) of that type: `aclsEnabled`, `aclsOn`, `aclsOn_$eq`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`
2. minus every name ending in `_$eq`, which drops `aclsOn_$eq`, leaving `aclsEnabled`, `aclsOn`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`
3. intersected with the five named source-level predicates, which drops `aclsOn` - a private-var getter, not one of the five, leaving exactly `aclsEnabled`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`

The final set is asserted against the graph, not against the source.

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
`aclsOn_$eq` excluded by suffix and the getter `aclsOn` excluded as
non-predicate residue. Every one of the fourteen is a setter or a getter over ACL
configuration by its own declaration, and none of them is one of the five names
the definition uses. Counting a call to one of them as "the route passed a
predicate" would therefore have inflated this query's spurious count with call
sites the definition does not cover - which is a statement about the selector,
not about the code it selects over.

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

The source-level check agrees with the graph-level one. Searching the pinned tree
for all five names across the three route files, in route order -
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala`,
`core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala`,
`core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala` - returns
**no occurrence of any of the five in any of the three**. So the zero is not a
route that filtered clean; there was no call site of a predicate anywhere on the
route surface for a route to have passed.

### The zero is scoped to the route surface, not to the program

An unscoped "zero call sites" claim would simply be false. `aclsEnabled()` **is
invoked** inside the anchored type's own source file, at
`core/src/main/scala/org/apache/spark/SecurityManager.scala:249`, at `:265` and
at `:407` inside the private `isUserInACL` declared at `:402`; and 18 call sites
of the five exist graph-wide across 18 distinct calling methods. The zero above
holds for the three route files and for nothing wider.

### Reference is not invocation

The route surface does mention the predicate type; every such mention is a
reference of a kind that invokes none of the five. Holding, importing,
constructing or passing a value on is not **invoking** a method on it:

| pinned location | what it is |
| --- | --- |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:28` | imports `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:53` | declares `val securityMgr: SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:139` | reads the companion constant `SecurityManager.SPARK_AUTH_SECRET_CONF` |
| `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:1429` | constructs a `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:27` | imports `SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:56` | declares `val securityManager: SecurityManager` |
| `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:194` | passes `securityManager` on as an argument to the command builder |

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

`duplicate_formulation` = **partial_duplicate**. The label aggregates the two
pairwise entries below and names the strongest relation either carries; the scope
of the partial duplication is stated in the entry rather than hidden in the label.

Four grounds were relied on, the same four in both entries: the predicate and
step vocabulary each query uses; the source and sink node sets each selects; the
traversal semantics; and whether the returned route sets coincide. The question is
live rather than rhetorical - query 02 addresses the **same** handler/sink pair,
and query 03's pair one **is** that pair again - so the verdict is settled on
those grounds and never on the pair.

- Against `02-dataflow-unguarded-driver-launch`: **not_duplicate**. The same
  handler/sink pair addressed over **different edges**.
  - *traversal semantics*: reachability over **CALL edges**, selecting whole
    **METHOD** nodes as its ends, against flow over **reaching-definition edges**
    through the dataflow layer, selecting **PARAMETER and EXPRESSION** nodes as
    its ends. This query asserts nothing about data flow and that query
    establishes nothing about control arriving anywhere, so neither is
    expressible as the other.
  - *node sets*: whole methods at both ends here; a formal parameter and an
    accessor result against the launch call, its receiver and its arguments
    there.
  - *construct lists*: 4 constructs appear in this query's published list and not
    in that one's - `Call.code`, `Call.order`, `Method.callOut` and
    `NoResolve.getCalledMethodsAsTraversal` - against 18 that appear only in
    that one's, with 24 shared. The traversal primitive itself is one of the
    four, which is the auditable form of the semantics difference above.
  - *route sets*: both are empty, so they coincide only by both being empty -
    which is no evidence that one formulation restates the other, and is the
    least informative of the four grounds here. The measured difference is in
    what each can **return at all**: that query emitted 4 element-level flow
    records, and a method-level call-edge traversal produces no such record for
    any input, which is why this query published none.
  - the two formulations **agree** on all four boundary verdicts, and agreement
    on a verdict is not identity of formulation. `B3-interface` shows why: both
    report it crossed, and they report it crossed by two different kinds of
    edge, which is kept as two measurements rather than merged into one.
  - returns are reported side by side and **never summed**.
- Against `03-parameterized-handler-sink-pairs`: **duplicate on that query's pair
  one, not a duplicate as a whole**, and the verdict is given at both scopes so
  neither reads as a contradiction of the other. Instantiated on its pair one -
  which is this query's only pair - the parameterized form **is** this query's
  formulation restated: the same edge kinds (CALL edges only, no data edge and no
  flow engine on either side), the same entry-point resolution under a
  byte-identical synthetic-partial-function selector, the same sink constraint,
  the same bound value 12, and Joern API construct lists whose set difference is
  empty in **both** directions. That comparison is a property of the two committed
  query sources and needed no graph load. As **wholes** the two are not
  duplicates: that query takes the handler/sink pair as a parameter and is
  additionally invoked on a second pair - the
  `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala`
  handler to the same `DriverRunner` sink - which this query does not address at
  all. Neither query's returns are added to the other's anywhere: they are
  reported side by side, per pair, and **never summed**. One direction of
  expressibility holds and the other does not: this query is the pair-one
  instantiation of the parameterized form, while the parameterized form's second
  instantiation cannot be expressed here at all, because this query has no pair
  parameter.

The relation is **symmetric**, and both directions were checked rather than
assumed: `queries/joern/results/02-dataflow-unguarded-driver-launch.json` states
`not_duplicate` against this query, and
`queries/joern/results/03-parameterized-handler-sink-pairs.json` states
`duplicate_formulation_on_pair_one` against it, scoped to pair one - the same two
verdicts recorded here. A disagreement between the two directions would be a
defect, since it is one relation cited twice and not two measurements.

## The three effort measures

1. **Query revisions committed: 1.** The **counting convention**, stated so the
   number is interpretable rather than bare: the count of commits touching
   `queries/joern/01-callgraph-unguarded-driver-launch.sc` from its first
   appearance to the end of the probe. On that convention this run introduces the
   query source in a single commit, so the count is 1 - a low number because the
   convention counts committed revisions of the file, not the drafting behind it.
2. **Distinct Joern API constructs used: 28.** The **list is the measure** and the
   count is computed from it, so the number is auditable rather than asserted;
   it is deduplicated, and every entry names a member this query's source
   invokes. This is the **per-query** list only: the probe-wide union across the
   three queries is owned by `oss-scan-results/joern-probe.md` and is
   deliberately not computed here, so that the union is one measurement in one
   place rather than three partial ones.

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

3. **Parameterizability: not claimed here.** This query is a **single-pair
   formulation** - it hard-codes one handler and one sink and takes no pair
   parameter - so it neither claims the measure nor could satisfy it, and saying
   so is the honest answer rather than an omission or an overclaim.

   The probe's evidence for the measure is
   `03-parameterized-handler-sink-pairs` **actually invoking** its parameterized
   form on the second named handler/sink pair: the handler `handleSubmit` at
   `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268`,
   whose enclosing type `StandaloneSubmitRequestServlet` is declared at `:171` of
   that same file, to the `DriverRunner` launch at
   `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:240`.
   That invocation's result is captured in
   `queries/joern/results/03-parameterized-handler-sink-pairs.json`, which is
   where the measure is settled. A parameter list that merely exists does not
   satisfy it, and neither would a declared-but-skipped second pair.

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
  would have halted the run rather than producing conclusions about a graph
  nobody has
- the path the plan names, `harness/cpg/spark.cpg`, and the path the environment
  exports resolve to the **same file** (equal resolved target), so there is one
  graph under two names rather than two graphs
- graph contents as loaded: 1397339 methods, 119691 type declarations,
  45037 files
- no absolute host path appears in this report or in its envelope. The resolved
  target is identified by the size-and-digest pair above rather than by a host
  path, which would vary between two checkouts of one branch and so could not be
  part of a deterministic record
- the envelope additionally records a **reproduction check**: attempted, and
  halted in the identity stage before any load. That re-run re-measured JDK major
  21 and the same heap, then measured a resolved target of 548118435 bytes with
  sha256 `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53`,
  reported it as **not** matching the identity of record above, printed its
  failure marker and emitted no result region at all - the designed failure
  behaviour, observed rather than described. Both pairs are recorded with their
  provenance and neither is reconciled away; the resolved target is a host-shared
  read-only file this run neither rebuilds nor replaces, and the identity reported
  above remains the one the run that produced these measurements re-verified
  immediately before its load

## Determinism of this report

An unchanged query source over an unchanged graph must produce a byte-identical
file, so this document carries no wall-clock timestamp, no elapsed time, no
process identifier, no host name, no host-specific scratch or workspace path and
no absolute host path. The only paths it names are repository-relative, relative
to the `$SPARK_SRC` root, or environment-variable names. Those excluded
quantities are real and are not being hidden - they live in
`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`, a
console stream deliberately **not** held to byte-identity.

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

