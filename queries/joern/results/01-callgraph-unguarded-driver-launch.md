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
| Query source sha256 | `79583377ffdc05762226f1437be94d953bf44be1ea94bbc3d9e48f072a27f4ac` (307625 bytes) |
| Publication id | `282448edaac93a9fcf34a7df351e5ccc32a8d8a5819451687dcd3c5fe87c2c3b` |
| Envelope | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` |
| Console log | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` |
| Loader | `importCpg` into a switched workspace (`queries/joern/.workspace`) |
| JDK major | 21 |
| Heap actually used | 68719476736 bytes (floor 68719476736) |
| Graph | 541309809 bytes, sha256 `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` |
| Graph identity re-verified before the load | yes, against `provision-log/cpg-identity.txt` |
| Bytes actually imported | a private copy this run made, digested in the copy pass, verified against that record, and re-verified by digest and inode after the load |
| Graph methods / typeDecls / files | 1396899 / 119721 / 45037 |
| Compile status | compiled |
| Run status | completed |
| Records returned | 4 (4 boundary, 0 route) |
| Distinct routes | 0 |
| Spurious routes | 0 |

## Which source wrote this report

This report was written by `queries/joern/01-callgraph-unguarded-driver-launch.sc`, whose contents at the moment
of the run digest to sha256 `79583377ffdc05762226f1437be94d953bf44be1ea94bbc3d9e48f072a27f4ac` over 307625 bytes. The
query read its own source at run time and computed that digest itself; it
verified that the file it digested declares this query's own identifier, and it
refuses to publish anything if it does not.

The envelope beside this report carries the same digest and the same publication
identifier `282448edaac93a9fcf34a7df351e5ccc32a8d8a5819451687dcd3c5fe87c2c3b`, as does the console log. Every figure below was
measured during that run from the graph, from this source's own text, from the
identity record or from the repository's commit history for this source path -
nothing here is transcribed from another document or from a previous run. **A
result whose digest does not match the source beside it was not written by that
source**, which makes drift between a query and its published result a
mechanical check rather than a matter of opinion.

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

- walk `A-follows-fan-out`: the frontier was still non-empty at depth 12. Expansions: 25006 of 200000 at the busiest single entry point, 25009 of 3200000 across the whole walk; routes returned 0 of 64.
- walk `B-fan-out-recorded`: the frontier was still non-empty at depth 12. Expansions: 5595 of 200000 at the busiest single entry point, 5598 of 3200000 across the whole walk; routes returned 0 of 64.

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
| MAX_EXPANSIONS_PER_WALK | 3200000 |
| MAX_TOTAL_RETURNS | 256 |
| MAX_ENTRY_POINTS | 16 |
| MAX_CALL_SCAN | 200000 |
| MAX_TYPE_SCAN | 100000 |
| FANOUT_CALLEE_THRESHOLD | 32 |

Every bound above is published with its reached flag and its basis in the
envelope's `bounds_reached` and `bounds_reached_basis`. `MAX_ROUTES` can bind
here: false - within one walk a route is retained per (entry point,
sink host) pair, so the most a walk can retain on this graph is
4. Alternate arrivals at a sink host already witnessed
from the same entry point are counted rather than retained: `A-follows-fan-out` 0, `B-fan-out-recorded` 0.

### Every traversal this query materialized, and the cap that governed it

| sweep | cap | value | observed | truncated |
| --- | --- | --- | --- | --- |
| entry: synthetic partial-function type declarations | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| entry: methods on those synthetic types | `MAX_TYPE_SCAN` | 100000 | 60 | false |
| entry: source-level handler methods | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| sink: calls named start | `MAX_CALL_SCAN` | 200000 | 1234 | false |
| predicate: type declarations | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| predicate: methods on that type | `MAX_TYPE_SCAN` | 100000 | 252 | false |
| predicate: call sites of the five named predicates | `MAX_CALL_SCAN` | 200000 | 36 | false |
| B1: message type declarations | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| B1: methods on the message type | `MAX_TYPE_SCAN` | 100000 | 36 | false |
| B1: producer call sites of the message constructor | `MAX_CALL_SCAN` | 200000 | 12 | false |
| B1: consumer call sites of the message accessors | `MAX_CALL_SCAN` | 200000 | 36 | false |
| route surface: type declarations under org.apache.spark.deploy.master.Master | `MAX_TYPE_SCAN` | 100000 | 217 | false |
| route surface: type declarations under org.apache.spark.deploy.worker.Worker | `MAX_TYPE_SCAN` | 100000 | 156 | false |
| route surface: type declarations under org.apache.spark.deploy.worker.DriverRunner | `MAX_TYPE_SCAN` | 100000 | 21 | false |
| route surface: type declarations under org.apache.spark.deploy.worker.ProcessBuilderLike | `MAX_TYPE_SCAN` | 100000 | 6 | false |
| B2: thread host methods | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| B2: thread body methods | `MAX_TYPE_SCAN` | 100000 | 2 | false |
| B3: JDK launch method nodes | `MAX_TYPE_SCAN` | 100000 | 1 | false |

Every materialization **outside the two walks** goes through one bounded helper
that takes `cap + 1` elements and reports truncation when it saw more than `cap`,
so a cap applied at one site and forgotten at the next is not expressible: a
sweep absent from this table did not run. The graph-wide sweeps are the ones that
matter - the sink name sweep, the predicate call-site sweep and the two message
call-site sweeps, all governed by `MAX_CALL_SCAN` - and the keyed type and method
lookups are capped under `MAX_TYPE_SCAN` so the claim holds for the whole of that
part of the query rather than for its largest pieces.

The walks' own expansions are a different mechanism and are named as such: they
are node-local (one method group's call sites, one call site's linked callees)
and are governed by `MAX_CALL_DEPTH`, `MAX_EXPANSIONS_PER_ENTRY`,
`MAX_EXPANSIONS_PER_WALK` and `MAX_ENTRY_POINTS`, each published above with its
own reached flag and basis. A reader checking the boundedness claim needs to know
which mechanism governs what, so the division is stated rather than collapsed
into one sentence about a single helper.

| walk | follows fan-out | expansions (walk) | expansions (busiest entry) | call sites | fan-out seen | fan-out not followed | max depth | depth bound reached | per-entry cap reached | walk budget exhausted | routes | alternate sink arrivals not retained |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `A-follows-fan-out` | true | 25009 | 25006 | 33565 | 86 | 0 | 12 | true | false | false | 0 | 0 |
| `B-fan-out-recorded` | false | 5598 | 5595 | 11575 | 55 | 55 | 12 | true | false | false | 0 | 0 |

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
surface (`org.apache.spark.deploy.master.Master`, `org.apache.spark.deploy.worker.Worker`, `org.apache.spark.deploy.worker.DriverRunner`, `org.apache.spark.deploy.worker.ProcessBuilderLike`).
The predicate set exists and is invoked elsewhere in the program; it is not
invoked anywhere on this route, so no route could have passed one.

This is a statement about **this query's own output** under **this query's own**
definition of the term. It is not an assessment of Spark, of any Spark component
or of any configuration, and nothing here should be read as one.

## Whether this formulation duplicates another query's

`duplicate_formulation` = **partial_duplicate**.

Every verdict below is **computed at run time**, by applying one shared predicate
to the two queries' own declared formulation identity blocks read out of the two
source files. No verdict about a sibling query is written down in this source, so
there is nothing here that can drift from what that query publishes.

the top-level verdict aggregates the per-query entries below and names the strongest relation any one of them carries: not_duplicate against 02-dataflow-unguarded-driver-launch, duplicate_formulation_on_pair-one against 03-parameterized-handler-sink-pairs. One entry is a duplicate at a scope NARROWER than the whole pair set, which makes the aggregate partial rather than absent. The scope is stated in that entry rather than hidden in this label. It was NOT inferred from the file names differing.

- Against `02-dataflow-unguarded-driver-launch` (source sha256 `902b7ffe8d708d6cb4ddfc057f65b1a2a023fc90c5b55c8d3ba012885dcb3fd1`): **not_duplicate**.
  Scope: none.
  Basis: the formulations differ on the edge kinds traversed (this query traverses CALL where 02-dataflow-unguarded-driver-launch traverses REACHING_DEF); the node kinds selected as a route's ends (this query selects METHOD where 02-dataflow-unguarded-driver-launch selects METHOD_PARAMETER_IN, EXPRESSION); the bound, as a named kind of quantity and a value (this query bounds call-graph hops expanded from an entry point at 12 where 02-dataflow-unguarded-driver-launch bounds call boundaries the backward data-flow search may expand at 6); the Joern API construct sets (4 construct(s) only here and 19 only there, over 24 shared), while agreeing on the handler/sink pairs addressed (at least one pair in common: pair-one); the entry-point selector literals (identical byte for byte); the sink selector literals (identical byte for byte). Neither traversal establishes the other's conclusion, so the two results are reported side by side and never summed.
  Joern API constructs only here: 4; only there: 19; shared: 24. Predicate selector literals identical: true.
- Against `03-parameterized-handler-sink-pairs` (source sha256 `8f67126c56185bde3221ad760130295cf9f7f64411be528e9fd578a4fbad631e`): **duplicate_formulation_on_pair-one**.
  Scope: pair-one only; 03-parameterized-handler-sink-pairs additionally addresses pair-two, which this query does not.
  Basis: every component of the formulation identity agrees at the scope named above: the edge kinds traversed (both traverse CALL); the node kinds selected as a route's ends (both select METHOD); the handler/sink pairs addressed (at least one pair in common: pair-one); the entry-point selector literals (identical byte for byte); the sink selector literals (identical byte for byte); the bound, as a named kind of quantity and a value (both bound call-graph hops expanded from an entry point at 12); the Joern API construct sets (set difference empty in BOTH directions over 28 construct(s)). The comparison is over the two SOURCES' own declarations, so it is a property of the two formulations rather than of either run's numbers.
  Joern API constructs only here: 0; only there: 0; shared: 28. Predicate selector literals identical: true.

a SYMMETRIC pairwise relation: the verdict this envelope states against a query is the same verdict that query's envelope states against this one. It is one measurement cited twice rather than two measurements, and here it is symmetric BY CONSTRUCTION rather than by transcription - every entry below is computed by applying ONE shared predicate to the two queries' own declared formulation identity blocks, read out of the two SOURCE files at run time under names all three queries share. Both directions therefore evaluate identical inputs through identical code, so a disagreement between them is not expressible; a transcribed verdict could disagree with the envelope it was copied from, which is exactly what this replaces.

## The three effort measures

1. **Query revisions committed: 3.** Convention: commits touching queries/joern/01-callgraph-unguarded-driver-launch.sc in the history of the HEAD this run measured at, newest first, counted at run time from the repository's own history. ONE convention, with three parts that make the number reproducible: the range is HEAD's own ancestry, named explicitly rather than defaulted, and the HEAD and the branch it was on are published beside the count; every commit returned is verified to be an ancestor of that HEAD, so a commit reachable only from another ref cannot enter the count - which is what happened to earlier figures once per-clone branches were reconciled and the commits a previous run had listed stopped being ancestors of the branch carrying its files; and the commit that PUBLISHES these result files is necessarily not among them, because it cannot exist while the run that writes them is still in progress. A later reader whose git log shows one more commit than the count reconciles against that window rather than against a bare number.
   Measurement: commits touching this path in HEAD's own history, newest first, every one verified an ancestor of the HEAD published beside this count.
   The commits counted, newest first, so the number is auditable rather than
   asserted:

   - `d3bc40ae290877827cbd422ba9025a4f54328ec0`
   - `232d0d9cca3f15d33cedb96fa18dac3c6602668b`
   - `1ac5915ed1535ff1ffece11b6b40b0286be74d45`

2. **Distinct Joern API constructs used: 28.** Listed explicitly and deduplicated so the
   count is auditable from the list rather than asserted. Each entry was searched
   for in this query's own source text with the list's own declaration excised
   first, so no entry can satisfy itself: 28 of
   28 were confirmed.

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

Precondition: run from a checkout of this branch after `BLITZY_CLONE_INDEX=<this clone's index> ; . harness/env.sh`, which exports $HARNESS_REPO_ROOT, $HARNESS_CPG and $HARNESS_SCRATCH_DIR - the three values the command below reads.

```
cd "$HARNESS_SCRATCH_DIR" && HARNESS_REPO_ROOT="$HARNESS_REPO_ROOT" HARNESS_CPG="$HARNESS_CPG" JAVA_HOME="$JAVA_HOME_21" JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script "$HARNESS_REPO_ROOT/queries/joern/01-callgraph-unguarded-driver-launch.sc" -J-Xmx64g < /dev/null
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
at. The query measures the heap it received and stops below the floor: raising a
heap is permitted and reported, lowering one is not.
