# Joern capability probe

Three hand-written queries were run against the code-property graph built over the
pinned tree's bytecode. **All three compiled and completed.** Every one of them
returned **zero routes** — query 01 zero, query 02 zero, query 03 zero on each of
its two pairs — and in every case the zero is accompanied by a measured account of
*where* the traversal stopped. Of the six distinct boundaries the three queries
identify between a driver-submission handler and the privileged process launch,
**the abstract-interface hop is crossed — by a call edge and, separately, by a
data flow — while the two message-send hops, the thread hop and the
partial-function hop of the partial-function handler are not**; the sixth, the
partial-function boundary of the ordinary-method handler, **does not arise for
that handler at all**, which its record marks with a flag of its own rather than
letting a crossed verdict stand in for it. So the outcome this report leads with
is not an absence of findings; it is a *capability* result. What a human could
express here was:

- an entry-point selector that resolves a Scala `PartialFunction` handler to the
  synthetic class its case bodies compile into, and an ordinary method to itself —
  measured per handler rather than assumed, and it answered differently for the
  two handlers the probe named;
- a bounded call-graph reachability traversal and a bounded data-flow traversal
  over the *same* handler/sink pair, established as two formulations rather than
  one restated, on grounds checkable from the two committed sources;
- a message-send hop modelled explicitly by pairing on the message *type*, which
  is what let the second handler/sink pair be expressed at all rather than
  reported as not-connectable;
- a control arm that proves the data-flow layer was live on the sink, so that
  formulation's zeros are attributable rather than ambiguous;
- one query parameterized over handler/sink pairs and **actually invoked on both**,
  which is what settles the parameterizability measure below.

What could **not** be expressed over this graph, measured rather than assumed: no
call edge and no data flow crosses the RPC message send, the `Thread.start()` to
`run()` hop, or — for the partial-function handler — the source-level method to
its synthetic body. Those verdicts, not the zeros, are the substance of this
report.

**This judges the queries, not Spark.** Nothing here is an assessment of Spark, of
any Spark component or of any Spark configuration, and nothing here is a finding.

---

## What this report is, and what it is not

**It is** the report of the Stage 5 capability probe, and the owner of the probe's
per-query results. The question the probe exists to answer is *what a human can
express in Joern's query language against this graph* — a question the Stage 3
Joern runner's baked query bundle cannot answer, because that bundle was not
written to ask it.

**It is not** any of the following, and each exclusion is deliberate:

- **No comparison between tools.** This report does not compare Joern with
  Opengrep, Semgrep, `datadog-static-analyzer`, Trivy, Gitleaks, Checkov,
  OSV-Scanner, Dependency-Check, or with any commercial scanner, and it does not
  characterise what any tool's output demonstrates about that tool. No
  cross-tool interpretation of any kind appears below.
- **No judgement of any finding.** Nothing the probe reached is called real,
  important, a false positive, or a duplicate of anything else.
- **No remediation.** No patch, no mitigation and no exploit is proposed for
  anything the probe reached or for anything it could not reach.
- **No dataset row.** The probe writes nothing into `harness/artifacts/raw/` and
  contributes no row to `oss-scan-results/findings.json`. This tree is Joern's
  deliberate **second** appearance in the run — the Stage 3 runner is the first —
  and folding either appearance into the other's numbers would corrupt both that
  tool's count and the dataset total.

Where a value could not be established it is **named as such** rather than
omitted; see "Values that could not be established".

## Inputs, and the one-measurement rule

Every figure below is **cited** from one of six files. This report measures
nothing of its own:

| Query | Envelope (machine-readable) | Prose result |
| --- | --- | --- |
| 01 | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` | `queries/joern/results/01-callgraph-unguarded-driver-launch.md` |
| 02 | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` | `queries/joern/results/02-dataflow-unguarded-driver-launch.md` |
| 03 | `queries/joern/results/03-parameterized-handler-sink-pairs.json` | `queries/joern/results/03-parameterized-handler-sink-pairs.md` |

The query sources are `queries/joern/01-callgraph-unguarded-driver-launch.sc`,
`queries/joern/02-dataflow-unguarded-driver-launch.sc` and
`queries/joern/03-parameterized-handler-sink-pairs.sc`; the console evidence is
`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`,
`harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` and
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`.

Each of the six result files states, in its own words, that this document must
**cite its measurements rather than re-measure them**. That is the
one-measurement-cited-twice rule, and it runs in one direction: where a count
appears both here and in an envelope it is one measurement cited twice, and if the
two ever disagreed **the envelope is right and this file is wrong**. Two
consequences worth stating plainly:

- No figure here is a second measurement of the graph, of a query's return set or
  of a source file.
- Exactly one quantity in this report is **computed here rather than cited**: the
  probe-wide **union** of the three per-query Joern API construct lists. Both
  `01-callgraph-unguarded-driver-launch.md` and
  `02-dataflow-unguarded-driver-launch.md` state that the union is owned by this
  file and is deliberately not computed in theirs, so that it is one aggregate in
  one place rather than three partial ones. It is an aggregation over published
  lists, not a new measurement, and the lists it aggregates are reproduced below
  so the arithmetic is auditable.

Source **line numbers** are a different kind of fact from a measurement. Every
line cited below is a line of the **pinned tree** at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, which is the only tree the probe
reads, and each was verified there. Envelope 01 publishes one caveat that a reader
must not "correct" against the working checkout: the checkout this report is
committed in differs from the pin on
`core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala` by a uniform
**+11** — `receive` at `:534` and `case LaunchDriver` at `:698` there, against
`:523` and `:687` at the pin. The other anchors coincide, which is exactly what
makes those two easy to get wrong. **The pinned numbers are the ones reported.**

## How the graph was loaded

**`importCpg` only, and the check is textual as well as behavioural.** All three
queries load the graph with `importCpg` into a switched workspace at
`queries/joern/.workspace`; each envelope publishes
`runtime.loader_is_importcpg_only = true`, and envelopes 02 and 03 additionally
publish `runtime.loader_import_code_absent_from_the_source = true`. The
appearance of the alternative loader in a committed query source would itself be
the violation — not merely its execution — so the absence was confirmed by
**searching the committed sources textually**: `importCode` occurs **zero** times
in each of the three `.sc` files under `queries/joern/`, and all three prose
results record that the absence was established by searching those files rather
than inferred from what the run happened to do. **The textual check was
performed, and it passed.**

The graph's identity was **re-verified immediately before every load**, by byte
size and sha256 with the symlink **followed** — the named path
`harness/cpg/spark.cpg` is a small symlink, so measuring the link itself would
record a few dozen bytes rather than the graph. Each load was compared against the
record of account for the bytes it was about to read, and each comparison matched:

| Query | Graph, symlink-followed | sha256 | Verified against | Result |
| --- | --- | --- | --- | --- |
| 01 | 541,255,894 bytes | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | `harness/artifacts/logs/cpg-frontend.log`, which owns that pair | match on both values |
| 02 | 541,255,894 bytes | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | `harness/artifacts/logs/cpg-frontend.log`, which owns that pair | match on both values |
| 03 | 548,118,435 bytes | `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53` | the record of account named by `$HARNESS_CPG_RECORD`, the frontend's own write-time record for the graph actually loaded | match on both values |

Both names for the graph — the plan's `harness/cpg/spark.cpg` and the
environment's exported variable — resolve to the same file for every one of the
three loads; each envelope publishes that reconciliation as *same file (equal
resolved target)*. No absolute host path is emitted by any envelope.

**A divergence between the two records is published rather than reconciled away,
and it is the reason the table above has two identity pairs in it.** Envelope 03
records that the repository-relative record
`harness/artifacts/logs/cpg-frontend.log` states 541,255,894 bytes with sha256
`26d327cc…`, which is **not** the graph its own load read (548,118,435 bytes,
sha256 `f8c71562…`): that committed record describes the graph of the provisioning
that wrote it, while the record of account for query 03's load is the frontend's
write-time record for the graph actually loaded, outside the repository root.
Envelope 03 keeps both pairs with their provenance and discards neither. The
graph is a host-shared read-only file that this run neither rebuilds nor
replaces.

The strongest evidence that the re-verification gate is real rather than
decorative is also published, by envelope 02: its reproduction check was
**attempted** and **halted**. That invocation compiled, ran, re-measured JDK major
21 on `21.0.12.1+1-LTS` at 68,719,476,736 bytes of heap — agreeing with its
runtime block field for field — then measured the resolved target at 548,118,435
bytes with sha256 `f8c71562…`, reported it as **not** matching the identity of
record it verifies against (541,255,894 bytes, sha256 `26d327cc…`), printed its
failure marker, and emitted **no result region and no envelope**. A load against
different bytes than the record describes was refused rather than weakened.

Per-query runtime, each field published by that query's own envelope:

| Query | JDK major | JVM | Heap actually used | Relative to the floor | Loader |
| --- | --- | --- | --- | --- | --- |
| 01 | 21 (the required major) | `21.0.12.1+1-LTS` | 68,719,476,736 bytes = 64 GiB | at the floor of 68,719,476,736 bytes = 64 GiB | `importCpg` into `queries/joern/.workspace` |
| 02 | 21 (the required major) | `21.0.12.1+1-LTS` | 68,719,476,736 bytes = 64 GiB | at the floor, not above it | `importCpg` into `queries/joern/.workspace` |
| 03 | 21 (the required major) | `21.0.12.1+1-LTS` | 68,719,476,736 bytes = 64 GiB | at the floor of 68,719,476,736 bytes = 64 GiB | `importCpg` into `queries/joern/.workspace` |

The floor is a minimum and a default rather than a ceiling: a larger heap is
permitted and reported, a smaller one is not, because a truncated result's silence
cannot be told apart from a clean one. Each envelope records the heap as
**measured** rather than as requested — the launcher's `-J-Xmx` reaches the
launcher only, so the heap the query actually runs at is measured from inside the
child JVM and the query halts below the floor rather than trusting the flag it was
given. Each query is one of the run's four heap-bound JVM invocations: the
frontend build, the `importCpg` verification load, the Stage 3 Joern runner, then
this probe.

The graph counts each query read, as published by its envelope:

| Query | Methods | Type declarations | Files |
| --- | --- | --- | --- |
| 01 | 1,397,339 | 119,691 | 45,037 |
| 02 | 1,397,339 | 119,691 | 45,037 |
| 03 | 1,399,866 | 119,920 | 45,037 |

---

## The per-query result contract, at a glance

One row per query, and — for query 03 — the two pairs kept side by side. Every
figure is cited from that query's envelope.

| | 01 callgraph | 02 dataflow | 03 parameterized |
| --- | --- | --- | --- |
| Compile status | compiled | compiled | compiled |
| Run status | completed | completed | completed |
| Returned record count | 4 | 8 | 6 |
| Record kinds | 4 boundary, 0 route | 4 boundary, 0 route, 2 boundary-flow, 2 liveness-control-flow | 6 boundary; per-pair route records 0 and 0 |
| Distinct routes | 0 | 0 | `pair-one` 0, `pair-two` 0 |
| Spurious count | 0 | 0 | `pair-one` 0, `pair-two` 0 |
| Bound value | 12 (`MAX_CALL_DEPTH`) | 6 (`MAX_FLOW_CALL_DEPTH`) | 12 (`MAX_CALL_DEPTH`), applied per pair |
| Bound reached | yes | no | `pair-one` yes, `pair-two` yes |
| Entry points discovered | 2 | 2 | `pair-one` 2, `pair-two` 1 |
| Entry points traversed | 2 | 2 | `pair-one` 2, `pair-two` 1 |
| Entry points truncated | 0 | 0 | `pair-one` 0, `pair-two` 0 |
| Expected-spurious route absent | yes, basis structural | yes, basis structural | `pair-one` yes, `pair-two` yes, both structural |
| Duplicate formulation (aggregate) | partial_duplicate | not_duplicate | partial_duplicate |

**No total appears in that table, by construction.** Routes are never summed:
not across queries — 01 and 02 address the *same* handler/sink pair by two
different formulations, so adding their returns would double-count one pair — and
not across query 03's two pairs, whose figures are reported side by side. Each
envelope publishes its own `never_summed_with` list naming exactly what its
figures must not be added to, and query 03's list names *the other pair in this
query* first.

**The route-identity function differs per query, and each is stated so
distinctness is auditable rather than asserted:**

- **Query 01** — a route identity is the triple *(entry-point method full name,
  sink-host method full name, the ordered sequence of method full names from the
  entry point to the sink)*. Both walks' returns are deduplicated on that triple
  and never summed.
- **Query 02** — a route identity is the triple *(source group, sink group,
  element signature)*, the signature being the flow's own element sequence. The
  route-bearing arms' flows are deduplicated on that triple; the arms' returns are
  never summed.
- **Query 03** — the same triple as query 01 *(entry-point method full name,
  sink-host method full name, hop sequence)*, deduplicated **within a pair**
  across that pair's own two walks. The two pairs are never combined.

**Entry-point counters are reported as two separate numbers on purpose.** A
traversed count exists so a sweep cannot run unbounded; a truncated count exists
so a trimmed traversal cannot pass for a complete one. Every truncated count in
this probe is **0**, and in query 01's case the envelope states why it is zero
rather than leaving it bare: 2 entry points were discovered against a cap of 16.

## "Spurious", defined mechanically — and it judges the query

**The definition, exactly as the three queries implement it:** a route is
spurious **only** where the handler *does* pass an authorization or ACL predicate
before reaching the sink, the predicate set being exactly the five named selectors
below.

**The five selectors, repeated from the queries.** All five are anchored on the
bytecode type `org.apache.spark.SecurityManager` and were verified present at the
pin in `core/src/main/scala/org/apache/spark/SecurityManager.scala`, a 457-line
file:

| Selector | Line at the pin | Resolved bytecode full name |
| --- | --- | --- |
| `aclsEnabled()` | `:227` | `org.apache.spark.SecurityManager.aclsEnabled:boolean()` |
| `checkAdminPermissions` | `:234` | `org.apache.spark.SecurityManager.checkAdminPermissions:boolean(java.lang.String)` |
| `checkUIViewPermissions` | `:248` | `org.apache.spark.SecurityManager.checkUIViewPermissions:boolean(java.lang.String)` |
| `checkModifyPermissions` | `:264` | `org.apache.spark.SecurityManager.checkModifyPermissions:boolean(java.lang.String)` |
| `isAuthenticationEnabled()` | `:274` | `org.apache.spark.SecurityManager.isAuthenticationEnabled:boolean()` |

The selector is a type anchor plus a name pattern with the setter suffix `_$eq`
**excluded**:

```text
type       : org.apache.spark.SecurityManager
name regex : ^(check.*Permissions|acls.*|isAuthenticationEnabled)$
excluded   : any name ending in _$eq
```

On bytecode the anchored pattern alone is not enough, and the envelopes publish
the three-step narrowing that gets from the pattern to the five: step 1 matched
seven names on the anchored type; step 2 excluded the one setter, `aclsOn_$eq`,
leaving six; step 3 dropped the one non-predicate residue, `aclsOn` — the private
`var` Scala compiles into accessors — leaving exactly **five**. For scale, the
anchored type carries 252 method nodes under 107 distinct names. Two Boolean
methods on the same type are named as **deliberate non-selectors** rather than
quietly dropped: `isEncryptionEnabled()` at `:280` and `isSslRpcEnabled()` at
`:295`. The set **was not widened**, and envelope 03 records that its selector
block is **byte-identical** to the blocks in the other two sources, so the three
spurious counts stay comparable. Within query 03 the parameterization varies the
handler and the sink and **never** the predicate set — varying it per pair is the
one way a parameterization could silently change what a spurious count means.

**This judges the query, not Spark.** The measure exists to say whether a
traversal that returned routes would have been returning routes it should have
filtered. It says nothing about whether Spark authorizes anything, and no sentence
in this report should be read as an assessment of Spark's security posture. No
finding is judged real, important or a false positive here.

**Was an expected-spurious route absent? Yes, in every case, and the basis is
structural in every case.** Per query and per pair:

| Query / pair | Spurious count | Expected-spurious route absent | Basis | Predicate call sites on its own route surface |
| --- | --- | --- | --- | --- |
| 01 | 0 | yes | structural | 0 |
| 02 | 0 | yes | structural | 0 |
| 03 `pair-one` | 0 | yes | structural | 0 |
| 03 `pair-two` | 0 | yes | structural | 0 |

*Structural* is a stronger statement than *the filter found none*, which is why
the envelopes distinguish them: **no call site of any of the five exists on the
route surface at all**, so no route of these pairs could have passed one. The
absence is therefore a property of the route surface rather than evidence that the
query filtered well.

**The zero is scoped to the route surfaces, not to the program.** The five
predicates *are* invoked elsewhere: **18 call sites graph-wide in 18 distinct
callers**, a figure every envelope publishes, including call sites inside the
anchored type itself. The per-pair figure is 0 for both of query 03's pairs, and
the shared route-surface figure is 0.

**The definition's limit, stated so a zero is not over-read.** The definition
evaluates **only** those five predicates. Any other conditional on a route is
outside it and is not assessed by it. The concrete case the envelopes name:
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala:411`,
`if (state != RecoveryState.ALIVE)`, guards the branch that reaches `createDriver`
at `:417` — a recovery-state check rather than one of the five, so it is neither
counted as a predicate nor reported as one. A spurious count of 0 therefore means
exactly and only what the definition says, and **does not** mean the route carries
no conditional.

## The target surface, verified at the pin

Every line below is a line of the pinned tree at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, cited as the queries measured it.

| Role | Anchor at the pin |
| --- | --- |
| Pair one handler | `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409` — `override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]` |
| Its submit case | `Master.scala:410` — `case RequestSubmitDriver(description) =>` |
| The conditional that is *not* a predicate | `Master.scala:411` — `if (state != RecoveryState.ALIVE)`, with the continuing branch at `:415` |
| Driver creation | `Master.scala:417` — `val driver = createDriver(description)`, also called at `:1130` from `relaunchDriver` (`:1121`), against the definition at `:1356` |
| The RPC send | `Master.scala:1367` — `worker.endpoint.send(LaunchDriver(driver.id, driver.desc, driver.resources))`, reached through `schedule()` (`:944`), `canLaunchDriver` (`:923`) and `launchDriver` (`:1363`) |
| The relay handler | `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:523` — `override def receive`, with `case LaunchDriver` at `:687` |
| The thread hop | `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:123` — `}.start()`, closing the `Thread` opened at `:89` whose `run()` body is at `:90` |
| **The sink** | `DriverRunner.scala:240` — `process = Some(command.start())` |
| Its abstract declaration | `DriverRunner.scala:270` — `def start(): Process`, on the trait declared at `:269` |
| Its concrete implementation | `DriverRunner.scala:276` — `override def start(): Process = processBuilder.start()`, the anonymous implementation created at `:275` |
| Pair two handler | `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268` — `handleSubmit` |
| Pair two's RPC send | `StandaloneRestServer.scala:276-277` — `masterEndpoint.askSync[DeployMessages.SubmitDriverResponse](DeployMessages.RequestSubmitDriver(driverDescription))` |
| The message types | `org.apache.spark.deploy.DeployMessages$LaunchDriver` and `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver` |

**Type precision on pair two, because getting it wrong would be invisible.**
`handleSubmit` at `StandaloneRestServer.scala:268` is **not** a member of
`StandaloneRestServer`. Envelope 03 records that the file declares seven classes
at the pin — `StandaloneRestServer` at `:56`, `StandaloneKillRequestServlet` at
`:81`, `StandaloneKillAllRequestServlet` at `:99`,
`StandaloneStatusRequestServlet` at `:116`, `StandaloneClearRequestServlet` at
`:138`, `StandaloneReadyzRequestServlet` at `:155` and
`StandaloneSubmitRequestServlet` at `:171` — and that the handler belongs to the
last of them, bytecode type
`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`.
`StandaloneRestServer` merely instantiates it, at `:64-65`. The consequence is
measured rather than hypothetical: a selector anchored on the type name
`StandaloneRestServer` would have matched nothing, returned an empty entry-point
set, and produced a zero **indistinguishable** from a genuine capability boundary.
Pair two's entry-point count is 1 discovered and 1 traversed, so the selection
resolved and its zero route count is a property of the traversal rather than of a
mis-anchored selector. Envelope 03 also records that the query's type selector
excluded the base declaration
`org.apache.spark.deploy.rest.SubmitRequestServlet.handleSubmit`, and that the
handler type is **not** covered by the shared route-surface prefix list — both
recorded rather than smoothed over. This report names the enclosing type as
`StandaloneSubmitRequestServlet` while citing the file as
`StandaloneRestServer.scala`; both names are kept, neither is substituted for the
other.


## The boundaries, and why the bounds are load-bearing

A bound is only meaningful if the traversal could in principle have run away, and
a zero is only meaningful if the place it stopped is named. Both queries 01 and 02
address pair one and each identifies **four** boundaries on it; query 03 identifies
**four** on its pair one and **five** on its pair two, the extra one being pair
two's own first step.

**Pair one's four boundaries**, with the verdict each formulation measured. Query
01 asks whether a **call edge** joins the two ends; query 02 asks whether a **data
flow** does. Both verdicts are kept, and neither is merged into the other:

| Boundary | The hop | Crossed by a call edge (01) | Crossed by a data flow (02) |
| --- | --- | --- | --- |
| B1 rpc | `Master.scala:1367` sends `LaunchDriver` over an `RpcEndpointRef`; `Worker.scala:523` / `:687` receives it. A message send is not a call | no | no, 0 flows found |
| B2 thread | `DriverRunner.scala:123` calls `Thread.start()`; the route continues in the `run()` body at `:90` on another thread. `start()` to `run()` is a JVM scheduling relation | no | no, 0 flows found |
| B3 interface | the launch call site invokes the abstract `ProcessBuilderLike.start` declared at `DriverRunner.scala:270`; the JDK launch is reached only through the anonymous implementation at `:276` | **yes** | **yes**, 2 flows found |
| B4 partial function | the handler at `Master.scala:409` returns a `PartialFunction`, so its case bodies compile into a synthetic class and the graph's entry point is the synthetic `applyOrElse` rather than any method named `receiveAndReply` | no | no, 0 flows found |

B3 is the case that shows why agreement is not identity: both formulations report
it crossed, and they report it crossed by **two different kinds of edge**.
Envelope 02 keeps that as two measurements of one hop under two different
questions — transcribing 01's call-edge verdict alongside its own data-edge
verdict — rather than merging them into a single word.

**Query 02 faces a fifth obstacle that query 01 does not: payload erasure.** The
handler signature at `Master.scala:409` is `PartialFunction[Any, Unit]`, so the
message payload's type is erased and the payload arrives at `:410` through a
pattern match, which in bytecode is a type test, a cast and the case class's own
accessor rather than an assignment. The query addresses that by selecting **two
separate arms** rather than choosing one:

- **ARM 1** takes every formal parameter of the two entry methods with the
  implicit receiver excluded, and identifies the `Any`-typed message parameter by
  its **erased bytecode type** `java.lang.Object` rather than by position.
- **ARM 2** takes the payload as the handler body sees it *after* the match — the
  call sites of `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription`
  inside the entry methods.

Reporting them separately is what keeps the flow counts interpretable: neither
choice is hidden inside one number.

**Pair two's five boundaries**, and the one this probe **models** rather than
reporting as not-connectable. Pair two crosses a message-send boundary at its
**first** step: the servlet's handler does not call the Master, it *sends* by
`askSync` at `StandaloneRestServer.scala:276-277`, and that is the very message
pair one's handler receives at `Master.scala:410`. A call graph does not join a
send to its receiving handler, so query 03 **models the hop explicitly by pairing
on the message type** `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver`
— the constructor's call sites are the producer end, its declared field accessors'
call sites are the consumer end, and the message type's and companion's own
generated machinery is excluded by owning type. What the model buys is measured:
pair two's producer end is its declared entry point and its consumer end is pair
one's entry point, which is why pair two is reported as crossing **one boundary
more** than pair one rather than as a route that cannot be expressed at all.
**The hop is modelled, not worked around silently.**

| Boundary | Kind | Cited by | Crossed by a call edge |
| --- | --- | --- | --- |
| `B-rpc-RequestSubmitDriver` | rpc | `pair-two` | no |
| `B-rpc-LaunchDriver` | rpc | `pair-one`, `pair-two` | no |
| `B-thread` | thread | `pair-one`, `pair-two` | no |
| `B-interface` | interface | `pair-one`, `pair-two` | **yes** |
| `B-partial-function-pair-one` | partial function | `pair-one` | no |
| `B-partial-function-pair-two` | partial function | `pair-two` | yes — but see below |

Three notes that keep those verdicts honest, each published by envelope 03:

- **Shared hops are one measurement cited once per citing pair.** `B-rpc-LaunchDriver`,
  `B-thread` and `B-interface` are cited by both pairs and measured once; pair one
  therefore counts 4 boundaries and pair two 5, from a set of 6 distinct ones, with
  no hop measured twice.
- **The partial-function boundary answers differently for the two handlers, and
  that difference is itself the capability observation.** The parameterized
  selector takes the union of a synthetic arm and a source-level arm and then
  *measures* which one carries the pair's declared body witness. For `pair-one`:
  synthetic types matched 1, body witness in the synthetic arm true, in the
  source-level arm false. For `pair-two`: synthetic types matched 0, body witness
  in the synthetic arm false, in the source-level arm true. A selector that took
  only one arm would have silently missed one of the two pairs.
- **`crossed_by_a_call_edge` on `B-partial-function-pair-two` must be read with
  the record's `hop_arises_for_this_handler` flag, which is `false`.** For a
  handler with no synthetic class the hop does not arise at all; the flag is
  therefore not evidence that the same hop was crossed for pair two that was
  uncrossed for pair one. Envelope 03 keeps those two cases distinct for exactly
  that reason.

Boundaries **not** crossed, as each envelope publishes them:

| Query / pair | Not crossed |
| --- | --- |
| 01 (call edges) | B1-rpc, B2-thread, B4-partial-function |
| 02 (data flows) | B1-rpc, B2-thread, B4-partial-function |
| 03 `pair-one` (call edges) | `B-rpc-LaunchDriver`, `B-thread`, `B-partial-function-pair-one` |
| 03 `pair-two` (call edges) | `B-rpc-RequestSubmitDriver`, `B-rpc-LaunchDriver`, `B-thread` |

---

## Query 01 — `01-callgraph-unguarded-driver-launch`

Result files:
`queries/joern/results/01-callgraph-unguarded-driver-launch.json` and
`queries/joern/results/01-callgraph-unguarded-driver-launch.md`.
Source: `queries/joern/01-callgraph-unguarded-driver-launch.sc`.
Console: `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`.

**Formulation.** Bounded call-graph reachability over CALL edges, from the
standalone Master's driver-submission handler to the privileged process launch
hosted on the `DriverRunner` surface. It asserts nothing about data flow.

| Field | Value |
| --- | --- |
| Compile status | compiled |
| Run status | completed |
| Returned record count | 4 — 4 boundary records, 0 route records |
| Distinct routes | 0 |
| Route identity | (entry-point method full name, sink-host method full name, ordered hop sequence); both walks deduplicated on it, never summed |
| Spurious count | 0 |
| Bound value | 12 — `MAX_CALL_DEPTH`, the maximum call-graph hops walked from an entry point |
| Bound reached | **yes** |
| Entry points discovered / traversed / truncated | 2 / 2 / 0 |
| Duplicate formulation (aggregate) | partial_duplicate |

**Entry points**, both traversed:
`org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse` and
`org.apache.spark.deploy.master.Master.receiveAndReply`. **Sink hosts**:
`org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry` and
`org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start`, at graph lines
`#240` and `#276` respectively.

**All seven bounds, with whether each was reached** — the depth bound was reached,
so the traversal was genuinely bounded rather than nominally so:

| Bound | Value | Reached |
| --- | --- | --- |
| `MAX_CALL_DEPTH` | 12 | **yes** |
| `MAX_ROUTES` | 64 | no |
| `MAX_EXPANSIONS_PER_ENTRY` | 200000 | no |
| `MAX_TOTAL_RETURNS` | 256 | no |
| `MAX_ENTRY_POINTS` | 16 | no |
| `MAX_CALL_SCAN` | 200000 | no |
| `FANOUT_CALLEE_THRESHOLD` | 32 | **yes** — a threshold rather than a cap: a call site whose resolved callee set is wider is recorded as a dynamic-dispatch fan-out site |

**Two walks, reported separately.** The difference between them is whether
dynamic-dispatch fan-out is followed, and neither walk's returns are added to the
other's:

| Walk | Follows fan-out | Method expansions | Methods visited | Call sites considered | Fan-out sites (not followed) | Depth used | Routes returned |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `A-follows-fan-out` | yes | 25,009 | 27,956 | 33,565 | 86 (0) | 12 | 0 |
| `B-fan-out-recorded` | no | 5,598 | 7,092 | 11,575 | 55 (55) | 12 | 0 |

**The four reporting requirements, for this query:**

1. **The predicate set and the source types it came from** — the five selectors
   above, anchored on `org.apache.spark.SecurityManager`, from
   `core/src/main/scala/org/apache/spark/SecurityManager.scala` at `:227`, `:234`,
   `:248`, `:264` and `:274`, with `_$eq` setters excluded.
2. **Whether the bound was reached** — yes: `MAX_CALL_DEPTH` = 12 was reached, and
   so was the `FANOUT_CALLEE_THRESHOLD` of 32. No route cap, expansion budget,
   total-returns cap, entry-point cap or call-scan cap was reached.
3. **Whether the formulation duplicates another query's** — partial_duplicate as
   an aggregate: not_duplicate against query 02, and a duplicate **at the pair-one
   scope** against query 03. See the matrix below.
4. **Whether an expected-spurious route was absent** — yes, on a **structural**
   basis: no call site of any of the five predicates exists on this query's route
   surface at all, so no route of this pair could have passed one.

**What the zero means, as the envelope states it.** The pair is not
call-graph-connected across those hops, so a bounded reachability walk over CALL
edges returns none. The bound was not loosened or removed, the query was not
widened, and no route was manufactured.

## Query 02 — `02-dataflow-unguarded-driver-launch`

Result files:
`queries/joern/results/02-dataflow-unguarded-driver-launch.json` and
`queries/joern/results/02-dataflow-unguarded-driver-launch.md`.
Source: `queries/joern/02-dataflow-unguarded-driver-launch.sc`.
Console: `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log`.

**Formulation.** Bounded **dataflow** over reaching-definition edges through the
OSS dataflow layer, over the **same** handler/sink pair as query 01 — a different
formulation of one question, not a restatement of it. Flow-engine semantics:
`io.joern.dataflowengineoss.semanticsloader.FullNameSemantics`.

| Field | Value |
| --- | --- |
| Compile status | compiled |
| Run status | completed |
| Returned record count | 8 — 4 boundary, 0 route, 2 boundary-flow, 2 liveness-control-flow |
| Distinct routes | 0 |
| Route identity | (source group, sink group, element signature); the route-bearing arms' flows deduplicated on it, never summed |
| Spurious count | 0 |
| Bound value | 6 — `MAX_FLOW_CALL_DEPTH`, the engine's `EngineConfig.maxCallDepth` |
| Bound reached | **no**; the observable-bound conjunction is also no |
| Entry points discovered / traversed / truncated | 2 / 2 / 0 |
| Duplicate formulation (aggregate) | not_duplicate, against both other queries |
| Dataflow layer live on this sink | **true**, measured by a control arm rather than assumed |

**How "bound reached" is established here, and the limitation stated with it.**
The engine's internal call-depth bound is **not observable from its output** — it
reports no truncation flag — so the query does not claim to have observed it.
Instead it reports the conjunction of the caps its own evaluator counts (the
per-source step cap, the per-pair flow cap, the flow-length cap and the source,
sink and entry-point truncation counters), none of which was reached, **and**
addresses depth by running one arm at two depths and comparing:

| Depth-sensitivity check | Value |
| --- | --- |
| Shallow depth | 2 |
| Primary depth | 6 |
| Flows retained, shallow | 0 |
| Flows retained, primary | 0 |
| Results differ across the two depths | no |

Equal results across the two depths is evidence that the result does not depend on
the call-depth bound across that range; a difference would have been evidence that
it does. The limitation is stated rather than papered over.

**Three route-bearing arms, plus one control arm, each reported separately:**

| Arm | Depth | Source groups (traversed) | Source nodes | Sink groups | Sink nodes | Evaluations | Flows found / retained |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ARM1-handler-parameters-shallow` | 2 | 2 (2) | 3 | 2 | 4 | 4 | 0 / 0 |
| `ARM1-handler-parameters` | 6 | 2 (2) | 3 | 2 | 4 | 4 | 0 / 0 |
| `ARM2-unapply-recovered-payload` | 6 | 1 (1) | 1 | 2 | 4 | 2 | 0 / 0 |
| `CONTROL-intraprocedural-liveness` | 2 | 1 (1) | 3 | 1 | 2 | 1 | **2 / 2** |

**The control arm is why this query's zeros are attributable.** A zero from a
cross-boundary arm means either that the route is not connected by data *or* that
the engine had no reaching-definition edges to walk, and the zero alone cannot
tell those apart. The control asks for a flow that must exist if the layer is
live — from the launch's own enclosing method's formal parameters to the launch
call it is the receiver of, intraprocedural by construction — and it found 2. So
`dataflow_layer_live_on_this_sink` is **true**, measured. The control's flows are
**not counted as routes**.

**Sink node composition**, published so the sink set is not mistaken for a single
node: 2 launch-call nodes, 2 receiver nodes and 2 argument nodes give 4 distinct
sink nodes used, 0 truncated. A flow that reaches the value being launched can end
at the launch call, its receiver or one of its arguments; taking only the call
node would miss a flow into the receiver.

**The four reporting requirements, for this query:**

1. **The predicate set and the source types it came from** — the same five
   selectors, from the same file at the same five lines, in a block the envelopes
   record as byte-identical across the three sources. For a flow, the predicate
   test asks whether the flow passes one of those five before reaching the sink.
2. **Whether the bound was reached** — **no**, on both the headline bound
   (`MAX_FLOW_CALL_DEPTH` = 6) and the observable-bound conjunction. Of the twelve
   named bounds, eleven are published as not reached and one — `MAX_CALL_SCAN` —
   is published as **not established**; see "Values that could not be
   established".
3. **Whether the formulation duplicates another query's** — **not_duplicate**
   against both. Against query 01 the grounds are the edge kinds
   (reaching-definition against CALL), the node granularity (parameter and
   expression nodes against whole methods), the engine and bound semantics, and a
   construct-set difference in both directions: 18 constructs only here, 4 only
   there, 24 shared. The envelope states explicitly that the verdict is drawn from
   properties of the two committed **sources** — checkable without a graph load —
   and **not** from both queries returning zero.
4. **Whether an expected-spurious route was absent** — yes, on a **structural**
   basis: no call site of any of the five exists on the route surface.

## Query 03 — `03-parameterized-handler-sink-pairs`

Result files:
`queries/joern/results/03-parameterized-handler-sink-pairs.json` and
`queries/joern/results/03-parameterized-handler-sink-pairs.md`.
Source: `queries/joern/03-parameterized-handler-sink-pairs.sc`.
Console: `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`.

**Formulation.** Bounded call-graph reachability over CALL edges, **parameterized**
over handler/sink pairs and instantiated on two named pairs **in one run**: the
standalone Master's handler, and the REST submit servlet's `handleSubmit`, both to
the same `DriverRunner` launch.

| Field | Value |
| --- | --- |
| Compile status | compiled |
| Run status | completed |
| Pairs declared / invoked | 2 / 2, in the fixed order `pair-one` then `pair-two` |
| Returned record count | 6 — 6 boundary records; per-pair route records 0 for `pair-one` and 0 for `pair-two` |
| Distinct routes | `pair-one` 0, `pair-two` 0 — side by side, never summed |
| Route identity | (entry-point method full name, sink-host method full name, hop sequence), deduplicated **within** a pair across its own two walks |
| Spurious count | `pair-one` 0, `pair-two` 0 |
| Bound value | 12 — `MAX_CALL_DEPTH`, applied **per pair** |
| Bound reached | `pair-one` **yes**, `pair-two` **yes** (the any-pair flag is a disjunction, never an arithmetic total) |
| Entry points discovered / traversed / truncated | `pair-one` 2 / 2 / 0; `pair-two` 1 / 1 / 0 |
| Duplicate formulation (aggregate) | partial_duplicate |
| Parameterizability | **passed** — this query owns the measure |

**Per-pair detail.** `pair-one`'s handler is
`org.apache.spark.deploy.master.Master.receiveAndReply`
(`Master.scala:409` at the pin), resolved through a synthetic-type regex on
`Master$$anonfun$receiveAndReply$N` with `applyOrElse` and the body witness
`createDriver`. `pair-two`'s handler is
`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit`
(`StandaloneRestServer.scala:268` at the pin) with the body witness
`DeployMessages$RequestSubmitDriver.<init>`. Both pairs share the sink at
`DriverRunner.scala:240`, reached through a callee regex on
`java.lang.ProcessBuilder.start` and
`org.apache.spark.deploy.worker.ProcessBuilderLike.start`; the sink scan
considered 1,234 calls named `start` without truncating, finding 52 call sites on
any host and 2 on the sink host.

**Eight bounds, per pair.** `MAX_CALL_DEPTH` (12) and `FANOUT_CALLEE_THRESHOLD`
(32) were reached on both pairs; `MAX_ROUTES_PER_PAIR` (64),
`MAX_EXPANSIONS_PER_ENTRY` (200000), `MAX_STEPS_PER_PAIR` (400000),
`MAX_TOTAL_RETURNS` (256), `MAX_ENTRY_POINTS_PER_PAIR` (16) and `MAX_CALL_SCAN`
(200000) were reached on neither. The route cap is **per pair rather than shared**,
because one pair filling a shared budget would silently truncate the other.

**Four walks — two per pair — reported separately and never combined:**

| Pair | Walk | Follows fan-out | Method expansions | Methods visited | Call sites considered | Fan-out sites (not followed) | Depth used | Routes returned |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `pair-one` | `A-follows-fan-out` | yes | 19,551 | 20,125 | 21,476 | 48 (0) | 12 | 0 |
| `pair-one` | `B-fan-out-recorded` | no | 2,350 | 2,511 | 4,383 | 31 (31) | 12 | 0 |
| `pair-two` | `A-follows-fan-out` | yes | 10,029 | 10,106 | 8,949 | 25 (0) | 12 | 0 |
| `pair-two` | `B-fan-out-recorded` | no | 732 | 766 | 1,230 | 12 (12) | 12 | 0 |

**The four reporting requirements, for this query** — answered per pair where the
measurement is per pair:

1. **The predicate set and the source types it came from** — the same five
   selectors on `org.apache.spark.SecurityManager` from the same file and the same
   five lines, **identical across both pairs**: the parameterization varies the
   handler and the sink and never the predicate set. Predicate call sites on each
   pair's own route surface: `pair-one` 0, `pair-two` 0; graph-wide 18 in 18
   distinct callers.
2. **Whether the bound was reached** — yes on both pairs: `MAX_CALL_DEPTH` = 12
   and the fan-out threshold were reached for each, and no cap was.
3. **Whether the formulation duplicates another query's** — **partial_duplicate**:
   `duplicate_formulation_on_pair_one` against query 01, whose scope is stated as
   pair-one only, and `not_duplicate` against query 02. Instantiated on pair one
   this query *is* query 01's formulation restated in parameterized form, and the
   evidence is measured: the same edge kind, the same entry-point resolution, the
   same sink constraint, the same bound value 12, and API construct sets that are
   **identical in both directions**. As wholes the two are not duplicates, because
   this query answers for a second pair query 01 cannot express.
4. **Whether an expected-spurious route was absent** — yes on both pairs, on a
   **structural** basis for each: no call site of any of the five exists on
   `pair-one`'s route surface (`Master`, `DriverRunner`, `ProcessBuilderLike`) or
   on `pair-two`'s (`StandaloneSubmitRequestServlet`, `Master`, `DriverRunner`,
   `ProcessBuilderLike`).


---

## Duplicate formulation, as a symmetric matrix

The relation is **pairwise and symmetric**: the verdict one envelope states
against a query is the verdict that query's envelope states against it, and each
of the three publishes `verdicts_agree_in_both_directions = true`. The matrix
below is therefore one relation read three ways, not three opinions:

| | 01 | 02 | 03 |
| --- | --- | --- | --- |
| **01** | — | not_duplicate | duplicate_formulation_on_pair_one |
| **02** | not_duplicate | — | not_duplicate |
| **03** | duplicate_formulation_on_pair_one | not_duplicate | — |

Aggregate verdicts, each envelope naming the strongest relation any of its entries
carries: **01 partial_duplicate**, **02 not_duplicate**, **03
partial_duplicate**. The aggregates are consistent with the matrix — 01 and 03
each carry the scoped duplication, 02 carries none — and the scope is stated in
the entry rather than hidden in the label.

**The evidential basis for each verdict**, as the envelopes state it. Every ground
below is a property of the **committed sources**, checkable without loading the
graph, which matters because all three queries returned zero and a verdict drawn
from the returns would have been drawn from that coincidence:

- **01 against 02, and 02 against 01 — not_duplicate.** Same target pair,
  different edges. 01 traverses CALL edges and selects whole **method** nodes as
  its ends; 02 flows over reaching-definition edges through the dataflow layer and
  selects **parameter and expression** nodes. Grounds relied on: the predicate and
  step vocabulary each uses, the source and sink node sets each selects, the
  traversal semantics, and whether the returned route sets coincide. They do
  coincide — both are empty — and the envelopes state that coinciding *by both
  being empty* is not evidence that one restates the other. The measured
  difference is in what each can **return at all**: 02 emitted 4 element-level
  flow records and a method-level call-edge traversal produces no such record for
  any input. Construct-set difference in both directions: 18 only in 02, 4 only in
  01, 24 shared. Neither is expressible as the other. Their four boundary verdicts
  **agree**, and the envelopes say plainly that agreement on a verdict is not
  identity of formulation — B3-interface being the case that shows it, crossed by
  a call edge and crossed by a data flow, kept as two measurements.
- **01 against 03, and 03 against 01 — duplicate_formulation_on_pair_one.** Said
  plainly by envelope 03: instantiated on pair one, that query **is** query 01's
  formulation restated in parameterized form. Measured grounds: the same edge kind
  (CALL edges only, no data edge and no flow engine on either side), the same
  entry-point resolution under a byte-identical synthetic-type selector with
  `applyOrElse`, the same sink constraint, the same bound value 12, and API
  construct sets whose difference is **empty in both directions** (28 shared, 0
  only here, 0 only there). The relation is one-directional in expressibility:
  query 01 is the pair-one instantiation of the parameterized form, and the
  converse does not hold, because query 01 has no pair parameter and cannot
  express the second instantiation. As wholes the two are not duplicates — the
  target pair **set** differs.
- **02 against 03, and 03 against 02 — not_duplicate.** Different edges, different
  node granularity, and no flow engine loaded on 03's side at all; the bound values
  (6 and 12) are published as **not the same kind of quantity**. Construct-set
  difference in both directions: 18 only in 02, 4 only in 03, 24 shared. Sharing
  pair one is not sufficient for duplication, and the pair **set** differs too.

**No returns are summed anywhere in that comparison.** Where a sibling's figure
appears in an envelope it is transcribed from that sibling's published envelope and
labelled as transcribed, never re-measured and never added.

## The three effort measures

The three are answered **individually** below, not as a group.

### 1. Query revisions committed

**The counting convention, stated so the numbers are interpretable rather than
bare:** a query's revision count is **the number of commits touching that `.sc`
file, from its first appearance to the end of the probe**. On that convention the
count is a count of *committed revisions of the file*, not of the drafting behind
it — which is why a value of 1 means "introduced in a single commit" rather than
"written without iteration".

| Query | `.sc` file | Revisions committed |
| --- | --- | --- |
| 01 | `queries/joern/01-callgraph-unguarded-driver-launch.sc` | 1 |
| 02 | `queries/joern/02-dataflow-unguarded-driver-launch.sc` | 1 |
| 03 | `queries/joern/03-parameterized-handler-sink-pairs.sc` | 1 |

Each figure is the value that query's own envelope publishes, under that query's
own statement of the same convention.

### 2. Distinct Joern API constructs used

**The list is the measure and the count is computed from it**, so each number is
auditable from its list rather than asserted. Each per-query list is deduplicated,
and every entry names a member that query's source invokes.

| Query | Constructs |
| --- | --- |
| 01 | 28 |
| 02 | 42 |
| 03 | 28 |

**Query 01 — 28 constructs**, as its envelope publishes them:

`Call.code`, `Call.dispatchType`, `Call.lineNumber`, `Call.method`,
`Call.methodFullName`, `Call.name`, `Call.order`, `Method.callIn`,
`Method.callOut`, `Method.fullName`, `Method.lineNumber`, `Method.name`,
`Method.typeDecl`, `NoResolve.getCalledMethodsAsTraversal`, `Steps.fullName`,
`Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`,
`TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`,
`cpg.typeDecl`, `importCpg`, `switchWorkspace`.

**Query 02 — 42 constructs**, as its envelope publishes them:

`AstNode.code`, `AstNode.label`, `AstNode.lineNumber`, `Call.argument`,
`Call.dispatchType`, `Call.lineNumber`, `Call.method`, `Call.methodFullName`,
`Call.name`, `Call.receiver`, `CfgNode.method`, `EngineConfig.maxCallDepth`,
`EngineContext.config`, `EngineContext.copy`, `EngineContext.semantics`,
`Method.call`, `Method.callIn`, `Method.fullName`, `Method.lineNumber`,
`Method.name`, `Method.parameter`, `Method.typeDecl`, `MethodParameterIn.index`,
`MethodParameterIn.method`, `MethodParameterIn.name`,
`MethodParameterIn.typeFullName`, `Path.elements`, `Steps.fullName`,
`Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`,
`Traversal.reachableByFlows`, `TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`,
`cpg.file`, `cpg.method`, `cpg.typeDecl`, `importCpg`, `switchWorkspace`.

**Query 03 — 28 constructs**, as its envelope publishes them:

`Call.code`, `Call.dispatchType`, `Call.lineNumber`, `Call.method`,
`Call.methodFullName`, `Call.name`, `Call.order`, `Method.callIn`,
`Method.callOut`, `Method.fullName`, `Method.lineNumber`, `Method.name`,
`Method.typeDecl`, `NoResolve.getCalledMethodsAsTraversal`, `Steps.fullName`,
`Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`,
`TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`,
`cpg.typeDecl`, `importCpg`, `switchWorkspace`.

**The probe-wide union: 46 distinct constructs.** This is the one quantity this
document computes rather than cites, because both `01`'s and `02`'s prose results
state that the union is owned by this file and is deliberately not computed in
theirs. It is a set union over the three published lists — an aggregation, not a
new measurement — and it decomposes exactly, so the arithmetic can be checked
against the three lists above:

| Partition | Count | Members |
| --- | --- | --- |
| Shared by all three queries | 24 | `Call.dispatchType`, `Call.lineNumber`, `Call.method`, `Call.methodFullName`, `Call.name`, `Method.callIn`, `Method.fullName`, `Method.lineNumber`, `Method.name`, `Method.typeDecl`, `Steps.fullName`, `Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`, `TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`, `cpg.typeDecl`, `importCpg`, `switchWorkspace` |
| Used only by query 02 | 18 | `AstNode.code`, `AstNode.label`, `AstNode.lineNumber`, `Call.argument`, `Call.receiver`, `CfgNode.method`, `EngineConfig.maxCallDepth`, `EngineContext.config`, `EngineContext.copy`, `EngineContext.semantics`, `Method.call`, `Method.parameter`, `MethodParameterIn.index`, `MethodParameterIn.method`, `MethodParameterIn.name`, `MethodParameterIn.typeFullName`, `Path.elements`, `Traversal.reachableByFlows` |
| Used by queries 01 and 03 but not 02 | 4 | `Call.code`, `Call.order`, `Method.callOut`, `NoResolve.getCalledMethodsAsTraversal` |
| **Union** | **46** | 24 + 18 + 4 |

Queries 01 and 03 publish **identical** construct sets, which is why the union is
46 rather than larger and is itself part of the pair-one duplication evidence
above. The 18 and the 4 are the per-query difference figures the envelopes
publish — 02's `not used by 01` list, and 03's `not used by 02` list — so the
decomposition is cited rather than derived independently.

The full union, listed so the count is auditable:

`AstNode.code`, `AstNode.label`, `AstNode.lineNumber`, `Call.argument`,
`Call.code`, `Call.dispatchType`, `Call.lineNumber`, `Call.method`,
`Call.methodFullName`, `Call.name`, `Call.order`, `Call.receiver`,
`CfgNode.method`, `EngineConfig.maxCallDepth`, `EngineContext.config`,
`EngineContext.copy`, `EngineContext.semantics`, `Method.call`, `Method.callIn`,
`Method.callOut`, `Method.fullName`, `Method.lineNumber`, `Method.name`,
`Method.parameter`, `Method.typeDecl`, `MethodParameterIn.index`,
`MethodParameterIn.method`, `MethodParameterIn.name`,
`MethodParameterIn.typeFullName`, `NoResolve.getCalledMethodsAsTraversal`,
`Path.elements`, `Steps.fullName`, `Steps.fullNameExact`, `Steps.l`,
`Steps.nameExact`, `Steps.size`, `Steps.take`, `Traversal.reachableByFlows`,
`TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`,
`cpg.typeDecl`, `importCpg`, `switchWorkspace`.

### 3. Parameterizability

**Verdict: passed — and the pass is attributed solely to query 03's captured
invocation.** Queries 01 and 02 neither claim the measure nor could satisfy it:
each is a single-pair formulation that hard-codes one handler and one sink and
takes no pair parameter, and both envelopes say so explicitly rather than leaving
it blank.

**The pass condition, as query 03's envelope states it:** the measure passes
**only** where the parameterized query is actually invoked on the second named
pair *and* that invocation's result is captured in that query's result files and
console log. An empty result from a real invocation satisfies it; a skipped
invocation does not; **a parameter list that merely exists does not**.

**What actually happened.** Both pairs were declared and both were invoked, in one
run, in the declared order:

| | `pair-one` | `pair-two` |
| --- | --- | --- |
| Invoked | yes | **yes** |
| Handler | `org.apache.spark.deploy.master.Master.receiveAndReply` (`Master.scala:409` at the pin) | `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit` (`StandaloneRestServer.scala:268` at the pin) |
| Sink | `DriverRunner.scala:240` at the pin | `DriverRunner.scala:240` at the pin — the same sink |
| Entry points traversed | 2 of 2 | 1 of 1 |
| Walks run | `A-follows-fan-out`, `B-fan-out-recorded` | `A-follows-fan-out`, `B-fan-out-recorded` |
| Call sites considered | 21,476 and 4,383 | 8,949 and 1,230 |
| Distinct routes | 0 | 0 |
| Spurious | 0 | 0 |
| Boundaries measured or cited | 4 | 5 |

The second pair is the one the plan names — the
`StandaloneRestServer` / `StandaloneSubmitRequestServlet` `handleSubmit` handler to
the `DriverRunner` sink — and its result is captured in
`queries/joern/results/03-parameterized-handler-sink-pairs.json`,
`queries/joern/results/03-parameterized-handler-sink-pairs.md` and
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`. The
envelope publishes the exact parameter literals supplied for each pair — handler
type, handler method, synthetic-type regex, synthetic method, body witness, base
type, source file and line, sink callee regex, sink call name, sink host type
regex, sink file and line, message-hop identifiers and route-surface prefixes — so
a reader can see **one query body driven by two different inputs** rather than two
queries written. The query source itself names no handler and no sink.

**A zero on the second pair does not weaken the verdict, and did not affect it.**
The measure asks whether the second named pair was really supplied to the same
body and its result captured; it does not ask whether that pair is connected over
this graph by this formulation. Those two questions are reported separately for
exactly that reason: the verdict here, and the pair's own route and boundary
figures in its section above, where a zero is a capability observation about the
traversal rather than a failure of either.

## Values that could not be established

One value in the probe is published as **not established**, and it is named here
rather than omitted:

- **Query 02's `MAX_CALL_SCAN` reached-flag.** The envelope carries `null` for it
  and states the reason: the query prints its two indexed sweeps' truncation flags
  to its console stream rather than into the envelope, and the console stream of
  the invocation that produced its figures is not preserved on this branch —
  `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` records a
  **later** invocation of the same source, which halted before the sweeps ran. A
  value nothing preserved can be read from is named rather than guessed. The other
  eleven of that query's twelve bounds are published as not reached.

Two further facts belong here because they are limits rather than measurements,
and reading them as measurements would overstate what the probe established:

- **Query 02's engine-internal call-depth bound is not observable from the
  engine's output.** The engine reports no truncation flag for it, so the query
  reports the conjunction of the caps its own evaluator counts and addresses depth
  by the two-depth comparison above, rather than claiming to have observed the
  internal bound.
- **The graph-identity divergence between the two records is recorded, not
  resolved.** Queries 01 and 02 loaded 541,255,894 bytes / `26d327cc…`, verified
  against `harness/artifacts/logs/cpg-frontend.log`; query 03 loaded 548,118,435
  bytes / `f8c71562…`, verified against the record of account for the graph it
  actually read. Both pairs are kept with their provenance and neither is
  discarded. The graph is a host-shared read-only file this run neither rebuilds
  nor replaces, and every load was verified against the record of account for the
  bytes it read before reading them.

## Provenance — every figure to its file

| Figures | Cited from |
| --- | --- |
| Query 01: statuses, records, distinct routes, route identity, spurious count, all seven bounds and their reached flags, entry-point counters, both walks' counters, boundary verdicts, duplicate-formulation detail, graph identity and counts, JDK major and heap, revisions, 28-construct list | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` and `queries/joern/results/01-callgraph-unguarded-driver-launch.md` |
| Query 02: statuses, records and their four kinds, distinct routes, route identity, spurious count, twelve bounds with the one not-established flag, depth-sensitivity figures, three arms and the control arm, sink-node composition, boundary verdicts including B3's two verdicts, duplicate-formulation detail, graph identity and counts, JDK major and heap, revisions, 42-construct list, the halted reproduction check | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` and `queries/joern/results/02-dataflow-unguarded-driver-launch.md` |
| Query 03: statuses, pairs declared and invoked, records, per-pair distinct routes and spurious counts, eight bounds per pair, per-pair entry-point counters, four walks' counters, the six boundaries and their per-pair citation, the modelled message-type hop, the seven-class declaration table and the enclosing-type precision, graph identity and counts including the recorded divergence, JDK major and heap, revisions, 28-construct list, the parameterizability verdict and its captured second-pair invocation | `queries/joern/results/03-parameterized-handler-sink-pairs.json` and `queries/joern/results/03-parameterized-handler-sink-pairs.md` |
| The five predicate selectors, their pinned lines, the three-step narrowing, the deliberate non-selectors, the 18 graph-wide call sites, the per-route-surface zeros | all three envelopes, whose selector blocks are recorded as byte-identical |
| The probe-wide union of 46 API constructs and its 24 / 18 / 4 decomposition | computed here from the three published lists, as `01`'s and `02`'s prose results direct |
| Pinned source line numbers, and the `+11` offset caveat on `Worker.scala` in the working checkout | the pinned tree at `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, and the caveat as published in envelope 01's route surface |

Console evidence for each query is
`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`,
`harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` and
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`. Both
artifact trees are git-ignored and are published by manifest with per-file byte
size and sha256 in `oss-scan-results/run-record.md`, which indexes this report but
does not substitute for it: the per-query probe results are owned here.
