# Phase 3 — the Joern capability probe

Phase 3 of this run answers exactly one question:

> **Can an open-source tool express a missing-authorization bug at all?**

That is a **capability assessment of a tool**. It is not an assessment of Apache Spark. Everything
below describes what three queries written against a code-property graph were able to express, and
what they returned when run. Nothing below says that any returned path is a defect, and nothing
below rates, ranks, triages, scores or proposes a change to anything.

**The graph was read, not built.** Each of the three committed query scripts loads the persisted
code-property graph at `harness/cpg/spark.cpg` with **`importCpg`**, or opens the project a previous
script in the same workspace already created. All three envelopes record `graph.loaded_with`
`importCpg`, `graph.built` `false` and `diagnostics.load_mode` `imported_persisted_cpg`, and all
three report the same `cpg_method_count`, 445,567. **`importCode` — the command that constructs a
graph — appears in none of the three committed sources**, which is the concrete form of the
prohibition on building one: the API choice is the guardrail, and its appearance in a committed
query would itself have been the violation. Nothing under `harness/` was authored, edited or removed
by this phase, and the graph was neither rebuilt nor supplemented.

**The spurious determination is a property of the query, not of Spark.** Section 3 makes the one
true-or-false call this phase makes, and what it decides is whether a query matched what it was
asked to match — whether an authentication or ACL predicate lies on a path the query itself emitted.
It decides nothing about the code the graph was built over.

## Derivation basis

Every number, boolean, hash, selector, parameter value and method full name in this file is
transcribed from one of three sources, and from nothing else:

| Source | What comes from it |
|---|---|
| `queries/joern/results/<nn>-<slug>.json` | the three result envelopes: `compiled`, `ran`, the returns with their `path` and `predicates_on_path`, the return and spurious counts, the derived predicate set, the traversal diagnostics, the `revisions` log |
| `queries/joern/results/<nn>-<slug>.md` | the per-query revision logs, from which the aggregate revision measure is summed |
| `queries/joern/*.sc` | the final committed query sources, from which the Joern API constructs are enumerated and query 03's parameter list is read |

Nothing here is inferred, estimated or re-measured, and **no line number and no symbol identity is
taken from any source file, plan or brief** — only from the envelopes and the committed sources.
Symbols observed while the queries were being written came from a checkout other than the pinned
tree, so none of those observations is restated here as a fact about the tree the graph was built
over; the method full names below are the ones the graph itself yielded.

No user rules were provided for this work, so none is cited, paraphrased or numbered anywhere in
this file. Enterprise-standard care is the bar instead, which here means the audit disciplines
stated above rather than the reflexes a security report usually carries.

Two things are named and then left alone. `queries/joern/.workspace/` is Joern scratch: it is not a
deliverable, nothing in it is cited, summarized or promoted into this file, and nothing in it was
cleaned up. And no query recorded a failure, so no captured stderr is referenced here at all — each
envelope carries a `stderr_ref` naming its capture by path and line range, and this file neither
quotes, summarizes nor characterizes any of it. No credential value appears in this file.

## Two records that disagree, reported rather than reconciled

`oss-scan-results/run-record.md` records, in its §4.7, that nothing was published and nothing
staged, and that `queries/joern/` was empty; in its §5, that Done-when condition 5 was **never
reached**, "No query source was written and the Phase 3 driver was never launched"; and in its §6,
that no driver line follows because the driver was never launched, the probe's precondition being a
published `findings.json`.

The three result envelopes record the opposite state of affairs for the probe itself: three query
sources exist and are committed, each was invoked once as `joern --script <path>` with no `--param`,
and each compiled, ran and returned. **Both records are stated here as found, neither is
reconciled against the other, and no record outside this file was edited** — which is the same
discipline this run applies to any disagreement between what a record states and what is observed.
Two consequences follow, and they are stated rather than smoothed over:

- **This file makes no claim about the dataset's publication state.** `findings.json`,
  `findings.csv` and `severity-map.md` are not read, counted or cited here, and the only evidence in
  reach about them is `run-record.md` §4.7, which records none as published.
- **This file does not assert a verdict on Done-when condition 5.** It delivers the material that
  condition names — three or more committed queries, their recorded outcomes, their spurious-return
  counts, the three effort measures, and the graph read rather than built — and states plainly that
  `run-record.md` records that condition as never reached. Whoever reads both files has both facts.

Joern appears twice in this run, and the two appearances are distinct. `harness/bin/run-joern.sh` is
one of the nine Phase 1 runners: it has its own baked query set, writes
`harness/artifacts/raw/joern.json`, and is normalized and reported like any other tool, in
`oss-scan-results/tool-status.md`. **The Phase 3 probe reported here is separate work against the
same graph**, delivered under `queries/joern/`. Nothing below draws on the Phase 1 runner's artifact,
and the two are not combined into a single count anywhere.

---

## 1. The leading result

### 1.1 The class the queries attempt, and what a clean positive is

All three queries attempt one reachability class **in full**, each by a different formulation. The
class, in the words the probe was built against:

> "In Spark's standalone deploy mode, an RPC handler receives a driver-submission message and passes
> the caller's jar and command through to a process launcher, without ever binding the message to an
> authenticated or authorized sender. The interesting reachability is: an RPC entry point (`receive`
> / `receiveAndReply` in the `deploy` package) reaching a privileged sink (`DriverRunner`,
> `createDriver`, or a process launch) along a path that passes no authentication or ACL predicate."

**A clean positive is a query that compiled, ran, and returned at least one result that is not
spurious under the on-path test of section 3.** That definition is used here and no other.

**All three queries produced clean positives.** Each compiled, each ran, and each returned results
whose `predicates_on_path` lists are empty:

| Query | Formulation | `compiled` | `ran` | Returns | Spurious | Clean positive |
|---|---|---|---|---|---|---|
| `01-callgraph-unguarded-driver-launch` | the class over the call graph | `true` | `true` | 8 | 0 | yes |
| `02-dataflow-unguarded-driver-launch` | the class over data flow | `true` | `true` | 2 | 0 | yes |
| `03-parameterized-unguarded-handler-sink` | the class in parameterized form | `true` | `true` | 8 | 0 | yes |

This file's order is fixed in advance: a clean positive leads where any query produced one, and the
negative results lead where none did. One did, so one leads. **That order is a presentation order
and not a ranking:** no query's result is preferred, weighted or judged more important than
another's, and the two that follow in section 2 are clean positives on exactly the same definition.
The lead goes to query 02 for one stated and checkable reason — one of its emitted paths carries an
accessor on the driver-submission message the class names, so the message itself appears as a node
on the path rather than only at its ends — and for no other.

### 1.2 The leading clean positive: `02-dataflow-unguarded-driver-launch`

Query 02 expresses the class over **data flow**. It engages the open-source data-flow layer with
`run.ossdataflow` — recording the overlay set before and after, which the envelope reports unchanged,
so the persisted graph already carried the layer and nothing was added to it — and expresses
reachability as `sink.reachableByFlows(source)`, asked backward from each sink anchor's command- or
jar-bearing nodes to the resolved driver-submission message sources.

It returned **two** results, **neither spurious**, both from the same handler to the same sink:

**Return 1**, whose path carries the destructured driver-submission field:

1. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription:org.apache.spark.deploy.DriverDescription()`
4. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

**Return 2**, the same handler and sink without that node:

1. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

Both are two entries rather than one because the envelope's `return_selection` rule emits one return
per distinct `(entry point, sink, path, predicates)` tuple and its
`returns_removed_by_deduplication` is `0`.

**What the query resolved, and where it returned nothing.** The envelope records this per sink
anchor, so an absence is evidence rather than silence:

| Sink anchor | Sink nodes asked | Flows at depth 12 | Flows at engine default depth 4 | Returns contributed |
|---|---|---|---|---|
| `createDriver` | 2 | 2 | 2 | 2 |
| `DriverRunner` | 1 | 0 | 0 | 0 |
| `process_launch` | 19 | 0 | 0 | 0 |
| `ExecutorRunner` (carried in addition to the three the class names, never in place of one) | 1 | 0 | 0 | 0 |

`flows_filtered_by_flow_filter` is `0` and `flows_not_attributable` is `0`, so no flow was discarded
on the way to those counts. Of 14 resolved entry points, 12 resolved a message source;
`source_nodes_queried` is 14 and `sink_nodes_queried_distinct` is 23. The configured call depth was
12 against an engine default of 4, and `bound_changed_outcome_versus_engine_default` is `false` — the
deeper bound changed nothing about what came back. Where an anchor returned no flow, the envelope
carries each of its 21 resolved sink nodes together with the expression the value arrives through.

**The one limit this formulation recorded about itself**, in the envelope's own terms: its
`thread_boundary` bridge has `boundary_resolved` `true` and `applied_by_this_query` `false`. A flow
follows data dependence, and an allocation whose body the runtime later invokes is not one, so this
formulation does not continue past that boundary; it anchors instead at the sinks above it, which
the class names as alternatives to the deepest one. The boundary was resolved and recorded so that
the limit is evidence rather than an assertion.

### 1.3 Does query 02 generalize? A plain statement

**No — not as committed, without editing it.** `invocation.params` is the empty object and the
script's `@main def exec()` declares no parameters, so it accepts none: the entry points it looks
for, the sink anchors it asks at, the message type it treats as a source and the call depth it uses
are all fixed in the source text. Retargeting it to another handler-and-sink pair means editing
those selectors, which is a revision of the query rather than an invocation of it.

That is a statement about this one query and not about the formulation. Query 03 answers the
generalization question for the probe by construction, and its recorded parameter list is where
section 4.3 reports it.

### 1.4 Why the leading result does not settle the question on its own

No single query's outcome settles the capability question, and the order stated in 1.1 is exhaustive
over the whole query set: a clean positive leads if **any** query produced one, and the negative
answer would have required **every** genuine attempt to have returned no clean positive. A query that
failed to express its reachability would have recorded that failure and the probe would have
continued.

That matters here in both directions. The class can be expressible in a graph tool's call-graph view
and not its data-flow view, or the reverse, so reporting one formulation alone would misstate what
an open-source tool can express — which is why two further formulations follow rather than being
folded into this one. And the counts differ between them: 8 returns from the call-graph formulation
against 2 from the data-flow formulation is a difference in what each view expresses, recorded as
found. **Nothing in this file reconciles, averages or explains away that difference, and no equality
between the counts is asserted or expected.**

---

## 2. The remaining queries

Both attempt the same whole class as query 02, by two other formulations. Neither is a component of
the class, and neither is a narrowed version of it.

### 2.1 `01-callgraph-unguarded-driver-launch` — the class over the call graph

The class expressed over call edges and nothing else: an entry point named `receive` or
`receiveAndReply`, enclosed in a type whose full name lies under `org.apache.spark.deploy.`, reaching
`createDriver`, a `DriverRunner` construction, or a process launch, along a path carrying no derived
predicate. `compiled` `true`, `ran` `true`, exit code `0`, both contract markers seen. It returned
**8** results, **none spurious** — every one of the eight carries an empty `predicates_on_path`.

| # | Handler | Sink | Path length |
|---|---|---|---|
| 1 | `ClientEndpoint.receiveAndReply` | `java.lang.ProcessBuilder.start` | 6 |
| 2 | `ClientEndpoint.receiveAndReply` | `Master$$createDriver` | 5 |
| 3 | `Master.receiveAndReply` | `Master$$createDriver` | 3 |
| 4 | `Worker.receive` | `java.lang.ProcessBuilder.start` | 6 |
| 5 | `Worker.receive` | `DriverRunner.<init>` | 3 |
| 6 | `Worker.receive` | `ExecutorRunner.<init>` | 3 |
| 7 | `WorkerWatcher.receiveAndReply` | `java.lang.ProcessBuilder.start` | 6 |
| 8 | `WorkerWatcher.receiveAndReply` | `Master$$createDriver` | 5 |

The handler and sink columns are shortened to their owner type and method name for width; the
envelope and `queries/joern/results/01-callgraph-unguarded-driver-launch.md` carry every one of them
as the full method name the frontend records, together with each return's path node by node.

Its traversal, as recorded: forward over callee edges, one traversal per resolved entry point, a
call-depth bound of 20, `bound_reached` `true` with 6 of 14 traversed entry points still holding a
non-empty frontier at that bound, 4 sink methods resolved, 193,135 methods seen summed over entry
points, and a deepest emitted sink depth of 5. Its `path_selection` rule emits one return per
`(entry point, sink)` pair — the breadth-first discovery path, with successors visited in full-name
order so the output is reproducible — rather than every path between that pair. Its
`expansion_restriction` expands the frontier only through methods whose full name begins with
`org.apache.spark.`, never through an operator pseudo-method and never through a derived predicate,
while still recognising a sink or a predicate wherever it is reached. Four of the eight returns
traverse a Scala trait's default-method forwarder, which the call graph links to every implementation
of the method it forwards; the envelope counts them and lists them as emitted rather than filtering
them out.

### 2.2 `03-parameterized-unguarded-handler-sink` — the class in parameterized form

The same class again, with both of its ends lifted into `--param` inputs. `compiled` `true`, `ran`
`true`, exit code `0`. It returned **8** results, **none spurious**, and they are the same eight
handler-and-sink pairs as query 01's, with the same path lengths. Its
`returns_whose_emitted_path_carries_a_derived_predicate` is `0` — the same fact counted from the
traversal's side rather than from the returns'.

`invocation.params` is the empty object, so **the script ran on its three declared defaults**, each
recorded with `origin` `default_value`; every default reproduces the class above so that a reader can
run the script by hand exactly as it was run here. The parameter list is section 4.3's answer and is
reproduced there in full.

What the parameterization buys, in the envelope's own words rather than in a claim of this file's:
its `generality` field records that "the traversal reads both ends from the resolved node sets and
tests membership by method full name, so no step of it depends on an entry point's name, a sink's
name, or the package either lies in", and its `relation_expressed` field records what a return
therefore means — "reachability over call edges: a return says a chain of calls, plus the bridges
recorded above, runs from the entry point to the sink. Whether a value the entry point received
arrives at an argument of the sink is a different relation, over data dependence, which this
formulation does not express and does not claim to."

Two further recorded properties bound it. Its call depth came from the `maxDepth` parameter at 20,
with `bound_reached` `true` and 6 of 14 traversed entry points truncated there, and the envelope
states the consequence rather than smoothing it over: the paths emitted are the paths reachable
within that bound, so the predicates found on them are a property of the bound as well as of the
graph, and a caller changing `maxDepth` changes which paths exist to be checked. And its
`expansion_restriction` is measured rather than asserted — 380,022 methods the graph carries a body
for, 65,545 it carries none for, and `0` of those body-less methods holding a callee edge, so the
bodied methods are the whole of what any callee traversal could expand through.

### 2.3 The vocabulary the three queries share

All three resolve the two entry points and the three sinks the class names, and all three carry one
additional anchor that never substitutes for those three:

| Anchor | Role in the class | Methods resolved |
|---|---|---|
| `receive` | RPC entry point | 8 |
| `receiveAndReply` | RPC entry point | 6 |
| `createDriver` | privileged sink | 1 |
| `DriverRunner` | privileged sink (its construction) | 1 |
| `process_launch` | privileged sink ("a process launch") | 1 |
| `ExecutorRunner` | carried **in addition**, labelled as additional in queries 01 and 02 and as `ExecutorRunner_additional` in query 03 | 1 |

Queries 01 and 02 select the two handler anchors by an exact method name plus an enclosing-type
pattern; query 03 selects them by the anchored full-name patterns its `handlerPattern` default
carries. Each envelope lists every method each anchor resolved to.

---

## 3. Per-query spurious counts, the on-path test, and the derived predicate set

### 3.1 The test as applied

A return is spurious **only if** the handler it names does pass an authentication or ACL check before
reaching the sink, and that is applied mechanically: **check whether an auth or ACL predicate lies on
the path, and apply no broader judgement.** In the envelopes' own identical wording, a return is
spurious when an authentication or ACL predicate from the set the query derived at execution time
lies on the emitted path between the handler entry and the sink; **on-path presence is the entire
test, and control dependence of the sink on that predicate is not required.**

Three properties of that test are stated because each is a place the count could otherwise drift:

- **Nothing narrower.** A stricter reading — requiring the sink to be control-dependent on the
  predicate, so that only a predicate actually gating the path counted — adds a condition the
  definition does not state, and it would reclassify returns the stated test marks spurious. It is
  not applied here.
- **Nothing broader.** *Only* means the criterion is sole and exhaustive: **no return is called
  spurious for any other reason, and in particular not because a reader finds it implausible.**
- **Recorded, not reviewed.** *Mechanically* means the query applies the test and this file
  transcribes the result. No return was re-examined by hand and no count was adjusted.

The reach the test was evaluated over is recorded per query. Queries 01 and 03 evaluate it over "the
emitted path nodes, plus one outgoing call step from each of them". Query 02 evaluates it over the
same reach — deliberately wider than the separate flow filter it applies to the flows themselves,
because the emitted path carries the entry point and the sink method, which no flow element covered.

### 3.2 The per-query counts

| Query | Returns | Spurious returns | Non-spurious returns | How the count is re-derivable |
|---|---|---|---|---|
| `01-callgraph-unguarded-driver-launch` | 8 | **0** | 8 | all eight `predicates_on_path` lists are empty |
| `02-dataflow-unguarded-driver-launch` | 2 | **0** | 2 | both `predicates_on_path` lists are empty; `flows_filtered_by_flow_filter` is `0` |
| `03-parameterized-unguarded-handler-sink` | 8 | **0** | 8 | all eight `predicates_on_path` lists are empty; `returns_whose_emitted_path_carries_a_derived_predicate` is `0` |

Each count is re-derivable without re-running anything: take the predicate set listed in 3.3, take
each return's recorded `predicates_on_path` from that query's envelope, and count the returns whose
list is non-empty. **These counts are not reconciled against one another.** Two formulations of one
class may legitimately return different spurious counts, and each is recorded as found.

### 3.3 The predicate set, derived at execution time

The set is **derived from the graph during each run rather than hardcoded**, which is what stops a
predicate added or renamed since the queries were written from being missed. All three envelopes
record the same derivation and the same result:

| Step | Value |
|---|---|
| Type-declaration selector | `org\.apache\.spark\.SecurityManager` |
| Type declarations resolved | 1 — `org.apache.spark.SecurityManager` |
| Member names on that type declaration | 19 |
| Methods considered | 126 |
| Name selector | `^(check.*Permissions\|acls.*\|isAuthenticationEnabled)$` (the alternation separators are escaped for this table only; the envelopes carry them unescaped) |
| Match mode | anchored full match |
| Names the selector matched | 7 — `aclsEnabled`, `aclsOn`, `aclsOn_$eq`, `checkAdminPermissions`, `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled` |
| Exclusion `scala_setter_suffix` | applied — removed `aclsOn_$eq` |
| Exclusion `field_member_name_collision` | applied — removed `aclsOn` |
| Exclusion `field_accessor_setter_evidence` | not applied — removed nothing |
| **Predicates resolved** | **5** |

The five, as concrete call targets in the graph and exactly as the envelopes carry them:

1. `org.apache.spark.SecurityManager.aclsEnabled:boolean()`
2. `org.apache.spark.SecurityManager.checkAdminPermissions:boolean(java.lang.String)`
3. `org.apache.spark.SecurityManager.checkModifyPermissions:boolean(java.lang.String)`
4. `org.apache.spark.SecurityManager.checkUIViewPermissions:boolean(java.lang.String)`
5. `org.apache.spark.SecurityManager.isAuthenticationEnabled:boolean()`

**`isEncryptionEnabled` and `isSslRpcEnabled` are excluded by construction.** The selector is an
anchored full match, and neither name can match `check.*Permissions`, `acls.*` or
`isAuthenticationEnabled`; neither appears among the seven names the envelopes record as matched, so
neither ever entered the set. That exclusion is deliberate and not an oversight: those two govern
transport encryption, which is not a check on caller identity, and the class is about binding a
message to an authenticated or authorized sender.

**The SASL and auth server bootstraps are excluded too**, and for a reason worth stating: they are
channel-setup constructors, never called on a handler-to-sink path, so under the on-path criterion
they could not qualify. Including them would add nothing and would imply a broader test than the one
specified. Mechanically they are out of reach in any case — the selector is confined to the members
of the resolved `org.apache.spark.SecurityManager` type declaration, so no constructor elsewhere
could enter the set.

The set is listed above so that any count in 3.2 can be re-derived. `exposed_as_a_parameter` is
`false` in query 03's envelope: the two ends of the reachability were lifted into parameters and the
predicate set deliberately was not, so a caller cannot narrow or widen the test by passing something.

### 3.4 Two consequences of the test, stated so the counts are reproducible

- **An `isAuthenticationEnabled` call lying on a `DriverRunner` sink path makes a return traversing
  it spurious** under this test — on-path presence is enough, whether or not the call gates anything.
  No return in any of the three queries carries such a call on its emitted path, which is what the
  three zeroes in 3.2 record. Query 02's envelope names the mechanism by which a predicate on the
  launch continuation stays off its paths: the continuation lies past a thread boundary its data-flow
  formulation resolved but did not cross, so a predicate reachable only beyond that boundary lies on
  no path this formulation emitted. Queries 01 and 03 do cross that boundary — the `thread_boundary`
  bridge fired and one of its connections lies on an emitted path — and their emitted paths still
  carry no predicate, within the recorded call-depth bound and under the `path_selection` rule that
  emits one path per `(entry point, sink)` pair.
- **A predicate reached only through RPC-environment construction is not on the handler-to-sink path
  and does not make a return spurious.** Reachability from somewhere else in the program is not
  on-path presence, and no such predicate appears in any return's `predicates_on_path`.

### 3.5 What this determination is, and what it is not

**It is a property of the query, not of Spark.** A spurious count of zero says the queries did not
find a derived predicate on the paths they emitted; it does not say the code lacks such a check, and
it is not a finding about the code the graph was built over. Equally, a non-spurious return is not
thereby a defect: this file does not call any return a real bug, a false positive, a duplicate of
anything, exploitable, important or severe, and it neither remediates nor proposes a change to
anything. Whether any path here corresponds to a defect is not assessed in this phase and is out of
its scope.

---

## 4. The three effort measures

Each measure is defined so that it is reproducible from the committed files, and **none of them is
expressed in wall-clock time or in expert-hours** — no such figure appears anywhere in this file.

### 4.1 Aggregate revision count: 3

A **revision** is one recorded execution of a **distinct source text** for a query. Each script is
hashed before it is run and the hash is appended to that query's revision log, so a query's revision
count is the number of **distinct hashes** in its log — not the number of rows in it, since a re-run
of an unchanged script adds a row without raising the count. The aggregate is the sum over the three
per-query logs:

| Query | Rows in its revision log | Distinct source hashes | Hash of the final committed source |
|---|---|---|---|
| `01-callgraph-unguarded-driver-launch` | 1 | 1 | `8a43c941b79af939e21085bda3a5d44a021a2fa043d1c861d63227d9520c5b52` |
| `02-dataflow-unguarded-driver-launch` | 1 | 1 | `b6dc40f642b9b111840a32ac55b6452f73f438a89eeb2aa4ed0320f6555dad0f` |
| `03-parameterized-unguarded-handler-sink` | 1 | 1 | `d7850cc1d123e612fe6a0badf26b2319f2186f4384c56ec4a6f55ba5a63af6cc` |
| **Aggregate** | **3** | **3** | — |

Each query's `revision_count` field agrees with its log, and the hash recorded for each most recent
attempt is the hash of the source committed under `queries/joern/`, so the sources in the repository
are the sources that produced the outcomes above.

Two properties of this measure matter to anyone re-reading it later. The logs are **append-only
across driver invocations**: a further invocation adds rows and never rewrites or resets earlier
ones, so a re-run for a query revision **updates this aggregate rather than resetting it**, and this
file is rewritten from the accumulated state each time. And **query iteration is not a re-scan**: a
query reads a graph that already exists, runs no scanner and writes no artifact into
`harness/artifacts/raw/`, which is why revision is permitted at all and why the probe has its own
driver — so that revising a query can never re-enter Phase 1.

### 4.2 Distinct Joern API constructs: 34, by name

A construct is counted when it is a **CPGQL step or a Joern command name invoked in the final
committed sources**, and the measure is the **union across all three** — a construct used by one
query counts once for the set. Two exclusions are stated so the count can be checked: a name
appearing only in a comment or inside a string literal is not counted, and a Scala or Java library
operation available on any collection — `filter`, `map`, `flatMap`, `size`, `distinct`, `sorted`,
`nonEmpty`, `count`, `mkString` and the like — is not a CPGQL step and is not counted either.

| # | Construct | Kind | 01 | 02 | 03 |
|---|---|---|:--:|:--:|:--:|
| 1 | `switchWorkspace` | command — selects the workspace, before anything is loaded | ✓ | ✓ | ✓ |
| 2 | `workspace.projects` | command — reads the workspace's project list | ✓ | ✓ | ✓ |
| 3 | `open` | command — opens an existing project | ✓ | ✓ | ✓ |
| 4 | `importCpg` | command — **loads the persisted graph** | ✓ | ✓ | ✓ |
| 5 | `cpg` | the loaded graph every traversal starts from | ✓ | ✓ | ✓ |
| 6 | `run.ossdataflow` | command — engages the open-source data-flow layer | | ✓ | |
| 7 | `method` | node-type step | ✓ | ✓ | ✓ |
| 8 | `typeDecl` | node-type step | ✓ | ✓ | ✓ |
| 9 | `member` | node-type step | ✓ | ✓ | ✓ |
| 10 | `call` | node-type step | ✓ | ✓ | ✓ |
| 11 | `callee` | call-graph traversal step | ✓ | ✓ | ✓ |
| 12 | `parameter` | node-type step | | ✓ | |
| 13 | `argument` | call-argument step | | ✓ | |
| 14 | `methodReturn` | node-type step | | ✓ | |
| 15 | `metaData` | node-type step | | ✓ | |
| 16 | `ast` | AST traversal step | | ✓ | |
| 17 | `isCall` | node-kind filter step | | ✓ | |
| 18 | `name` | property step | ✓ | ✓ | ✓ |
| 19 | `nameExact` | property filter step | ✓ | ✓ | |
| 20 | `fullName` | property / regex filter step | ✓ | ✓ | ✓ |
| 21 | `fullNameExact` | property filter step | ✓ | ✓ | ✓ |
| 22 | `methodFullName` | property step | ✓ | ✓ | ✓ |
| 23 | `methodFullNameExact` | property filter step | | ✓ | |
| 24 | `typeFullName` | property step | | ✓ | |
| 25 | `code` | property step | | ✓ | |
| 26 | `index` | parameter-index property step | | ✓ | |
| 27 | `argumentIndex` | argument-index property step | | ✓ | |
| 28 | `id` | node-identity property step | | ✓ | |
| 29 | `isExternal` | property filter step | | | ✓ |
| 30 | `overlays` | property step on the graph's metadata | | ✓ | |
| 31 | `where` | nested-traversal filter step | ✓ | ✓ | |
| 32 | `l` | execution step — runs the traversal to a list | ✓ | ✓ | ✓ |
| 33 | `reachableByFlows` | data-flow reachability step | | ✓ | |
| 34 | `elements` | step over a returned flow's elements | | ✓ | |

**Totals: 34 distinct constructs across the three sources** — 17 in query 01, 33 in query 02 and 16
in query 03, with fifteen common to all three: `switchWorkspace`, `workspace.projects`, `open`,
`importCpg` and `cpg`, plus the ten steps `method`, `typeDecl`, `member`, `call`, `callee`, `name`,
`fullName`, `fullNameExact`, `methodFullName` and `l`. **`importCode` is absent from all three**,
which is the guardrail restated as a count: the set above contains the command that loads a graph
and does not contain the command that builds one.

One construct the data-flow formulation needed sits outside this count under the stated rule and is
named rather than hidden: `EngineContext`, with the engine configuration it carries, which query 02
reads for the engine's default call depth and copies to set its own. It is a data-flow engine type
rather than a CPGQL step or a command name, so it is not in the table.

### 4.3 Parameterizable: yes — query 03, with its parameter list

The measure is answered by the parameterized query **existing**, and its parameter list is the
answer. `03-parameterized-unguarded-handler-sink.sc` declares three parameters on its
`@main def exec`, each a string at the invocation boundary:

**`handlerPattern`** — the identity of the entry point. Declared default, and the value in force for
the recorded invocation:

```
receive=org\.apache\.spark\.deploy\..*\.receive:.*;receiveAndReply=org\.apache\.spark\.deploy\..*\.receiveAndReply:.*
```

**`sinkPattern`** — the identity of the privileged sink:

```
createDriver=org\.apache\.spark\.deploy\..*createDriver:.*;DriverRunner=org\.apache\.spark\.deploy\.worker\.DriverRunner\.<init>:.*;process_launch=(java\.lang\.ProcessBuilder\.start|java\.lang\.Runtime\.exec):.*;ExecutorRunner_additional=org\.apache\.spark\.deploy\.worker\.ExecutorRunner\.<init>:.*
```

**`maxDepth`** — how far the traversal may follow call edges from an entry point. Declared default
and value in force: `20`, parsed as the integer 20.

The pattern-list format is one or more alternatives separated by `;`, each `<label>=<regex>`, with
the label everything before the first `=` and the regex everything after it, matched as an anchored
full match against a method full name as the frontend records it. Two limits of that surface are
recorded rather than left to be discovered: the format defines no escape for its own separator, so a
`;` cannot appear inside a pattern; and a label is a name and nothing more — the script resolves and
reports every alternative identically, whatever its label says.

What each parameter was free to vary, in the envelope's own words: `handlerPattern` — "any method the
graph holds, selected by an anchored full-match regex over its full name, whatever its name, its
enclosing type or its package"; `sinkPattern` — "the identity of the privileged sink: any method the
graph holds, selected the same way, including a constructor and a method with no body of its own";
`maxDepth` — "how far the traversal may follow call edges from an entry point, so that a handler and
a sink further apart than the default pair can still be related".

`invocation.params` is empty for the recorded invocation, so all three defaults were in force and
each is recorded with `origin` `default_value`. The predicate set is **not** a parameter
(`exposed_as_a_parameter` `false`), so the two ends of the reachability are what a caller may vary
and the auth-or-ACL test is not.

---

## 5. What this file does not claim

- **No characterization of any finding.** No return above is called a real bug, a false positive, a
  duplicate of anything, important, severe or exploitable; nothing is triaged, ranked or scored; and
  no remediation, patch or configuration change is proposed for anything. The spurious determination
  in section 3 is the only true-or-false call made here, and it is **a property of the query rather
  than of Spark**.
- **No comparison against anything.** No other tool's output, no baseline and no other scanner is
  referenced, compared with or implied. **This run compares nothing** — the dataset it belongs to is
  the open-source half of a comparison a human assembles afterwards — and this file offers no
  judgement of Joern against any alternative.
- **No claim about capability beyond what these three queries expressed.** The three formulations
  are three attempts at one class over one graph. Nothing here generalizes to another class, another
  code base, another graph, or to what any tool could express in principle.
- **Nothing about the pinned tree's security posture.** The returns, the paths, the anchors and the
  predicate set describe what queries expressed over **a graph that was read, not built**. They are
  not a statement about the code the graph was built over, and no line number from any source file
  appears above.
- **No effort figure in time.** The revision count, the construct enumeration and the
  parameterizability answer are the three measures reported; no wall-clock duration and no
  expert-hour estimate appears anywhere in this file.
- **Nothing taken from scratch or from stderr.** `queries/joern/.workspace/` is scratch and nothing
  in it is cited, summarized or promoted here; no captured stderr is quoted, and no credential value
  appears in this file or in the result envelopes it renders.
- **Nothing under `harness/` was touched.** The graph at `harness/cpg/spark.cpg` was loaded and never
  rebuilt, regenerated or supplemented; `harness/artifacts/raw/` was not created or written by this
  phase; `harness/artifacts/smoke/` was never read, and is never a fallback for anything.

