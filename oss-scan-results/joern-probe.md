# Joern capability probe

Three hand-written queries were run against the code-property graph over the pinned
tree's bytecode, one at a time, each against a graph whose identity was re-verified
immediately before it loaded.

**Which execution this report publishes, stated before anything else.** Every figure
below belongs to the **2026-09-03** execution of the three committed queries, against
the graph the re-provisioning of 2026-09-03T01:17:07Z left at the sanctioned path —
547,980,224 bytes, sha256
`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`, 1,398,964 methods.
The **2026-09-01 generation** of these same three probes ran against a graph of
541,309,809 bytes and sha256 `4616845a…` that the re-provisioning replaced; those bytes
are on no disk this checkout can reach, so wherever that generation's figure differs it
is kept **labelled with its date** and is never asserted of the graph on disk now. That
is AAP §0.1.3's retention rule applied to a superseded measurement rather than a
correction that discards one.

**All three compiled and completed.** Every one of them
returned **zero routes** — query 01 zero, query 02 zero, query 03 zero on each of its
two pairs — and in every case the zero is accompanied by a measured account of *where*
the traversal stopped. Of the six distinct boundaries the three queries identify
between a driver-submission handler and the privileged process launch, **the
abstract-interface hop is crossed — by a call edge and, separately, by a data flow —
while the two message-send hops, the thread hop and the partial-function hop of the
partial-function handler are not**; the sixth, the partial-function boundary of the
ordinary-method handler, **does not arise for that handler at all**, which its record
marks with a flag of its own rather than letting a crossed verdict stand in for it. So
the outcome this report leads with is not an absence of findings; it is a *capability*
result. What a human could express here was:

- an entry-point selector that resolves a Scala `PartialFunction` handler to the
  synthetic class its case bodies compile into, and an ordinary method to itself —
  measured per handler rather than assumed, and it answered differently for the two
  handlers the probe named;
- a bounded call-graph reachability traversal and a bounded data-flow traversal over
  the *same* handler/sink pair, established as two formulations rather than one
  restated, on grounds checkable from the two committed sources;
- a message-send hop modelled explicitly by pairing on the message *type*, which is
  what let the second handler/sink pair be expressed at all rather than reported as
  not-connectable;
- a control arm that proves the data-flow layer was live on the sink, so that
  formulation's zeros are attributable rather than ambiguous;
- one query parameterized over handler/sink pairs and **actually invoked on both**,
  which is what settles the parameterizability measure below.

What could **not** be expressed over this graph, measured rather than assumed: no call
edge and no data flow crosses the RPC message send, the `Thread.start()` to `run()`
hop, or — for the partial-function handler — the source-level method to its synthetic
body. Those verdicts, not the zeros, are the substance of this report.

**One further measured property explains why a selector on a source-level name would
have found nothing**, and it is stated here so the zeros are not read as the queries
having looked in the wrong place: Scala 2.13 mangles a private method's name in
bytecode, this graph is built from bytecode, and the plan's `createDriver` anchor is
consequently present only as
`org$apache$spark$deploy$master$Master$$createDriver`, with **zero** call sites keyed
on that mangled name. That is why the queries key on the RPC boundary at
`Master.scala:1367` instead. It is a property of the frontend and of the language, not
a query defect and not a finding about Spark; "Why a source-level `createDriver`
selector would have found nothing" below carries the measurement and its owner.

**This judges the queries, not Spark.** Nothing here is an assessment of Spark, of any
Spark component or of any Spark configuration, and nothing here is a finding.

---

## Provenance disclosure — the three executions this report cites

**Every figure in this report comes from the three invocations recorded below, and
each of them ran against the query source committed beside it.** The sources went
through four rounds of correction, enumerated under "What changed in the sources"
below and ending with the retention of the private graph copy that AAP §0.8.1
requires. Each round left the envelopes then on the branch describing **superseded**
bytes rather than merely older ones, because an envelope publishes the digest of the
text that actually ran. The three queries were therefore executed against the sources
as finished and committed, one at a time, each under a JDK reporting major 21 at a
64 GiB heap, and each gated on the graph's re-verified identity immediately before its
load.

**Those three invocations are the 2026-09-03 ones, and the reason there are three
again is the graph rather than the sources.** The sources did not move: each of the
three `.sc` files carries the byte size and sha256 the table below publishes, and the
2026-09-01 generation ran those same bytes. What moved is the artefact underneath —
the host was re-provisioned at 2026-09-03T01:17:07Z and the graph at the sanctioned
path was rebuilt, so the identity every 2026-09-01 envelope published ceased to
describe anything on disk. A probe result is a measurement *over a graph*, so a
re-provisioned graph invalidates the measurement even where the query text is
untouched, and the answer was to execute the three committed sources again rather
than to re-date the old figures. Each of the three 2026-09-03 invocations verified
the new artefact's identity itself, immediately before its own load, and every
envelope publishes `graph.identity_reverified_before_load = true` together with the
new pair. The 2026-09-01 figures that differ are retained under that date wherever
they appear below.

**The boundary a still earlier generation was re-executed under is not withdrawn.** The
generation preceding the 2026-09-01 one had its three invocations performed under a
review boundary that forbade them, stated verbatim in the scope those edits were made
under: *"Do not install,
upgrade, substitute, provision credentials, clear artifacts, trim graph inputs, rerun
scanners/build/graph/probe, or execute Spark tests. Static review only."* That is
recorded here as the violation it was, and no case is made that it was justified. The
figures this report publishes are **not** those figures: they belong to the 2026-09-03
executions recorded below, each cited from the
envelope, prose result, console stream and completion manifest of the invocation that
produced it.

**What is the product of those three invocations**, so a reader can quarantine the
set rather than hunt for it:

| Product of the three invocations | Where it is |
| --- | --- |
| the three machine-readable envelopes | `queries/joern/results/01-callgraph-unguarded-driver-launch.json`, `queries/joern/results/02-dataflow-unguarded-driver-launch.json`, `queries/joern/results/03-parameterized-handler-sink-pairs.json` |
| the three prose results | the same three stems with the `.md` extension |
| the three console streams | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`, `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log`, `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` |
| the three publication manifests | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.publication.json`, `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.publication.json`, `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.publication.json` |
| every figure this report publishes from them | the elapsed times 577,899 / 652,495 / 567,063 ms (`probe-01-…log` line 156, `probe-02-…log` line 183, `probe-03-…log` line 229; the 2026-09-01 generation's own streams stated 704,629 / 836,873 / 690,631 ms at those same three lines, and that generation's streams were replaced by these); the per-query source bytes and sha256 as executed; the entry-point traversed and truncated counters and every per-walk counter; every bound and its reached flag, including query 02's `MAX_CALL_SCAN`; the returned-record, distinct-route and spurious counts; the boundary verdicts; the graph identity verified before and after each load and the private-copy identity pairs; the per-invocation workspaces; the revision counts **3, 3, 4** (each envelope's `effort_query_revisions_committed`, and `probe-01-…log`, `probe-02-…log` and `probe-03-…log` line 25 apiece, each published beside the HEAD it was measured at and that HEAD's ancestry verdict); and the per-query API-construct lists of 28, 43 and 28 with the probe-wide union of 47 computed below from them |

**What a human still decides.** Whether a checkpoint's evidence may rest on an
execution performed while its records were being corrected is a question about process
rather than about measurement, and this report does not claim the authority to settle
it. What it does guarantee is narrower and checkable: no figure in it describes a
source, a graph or a run other than the one named beside it.
`oss-scan-results/run-record.md` carries the same disclosure as divergence **D17**.

### What changed in the sources, and the agreement re-established over them

**The three `.sc` files on the branch are the bytes the three invocations ran**, and
the pairs below are measured on both sides. Four rounds of correction changed all
three before that execution. The first — exact role-to-path binding in the completion-manifest reader with the
basename alternative removed, one NOFOLLOW open per member for both the containment
decision and the digest, and the removal of the private graph copy's deletion. The
second — the member walk and the member open in that same reader now descend through
**held directory handles** (`SecureDirectoryStream`, one component at a time from the
verified repository root) rather than resolving an absolute pathname, so an ancestor
rename can no longer redirect the open instead of merely being detected afterwards.
The third closes what the second left open, in three parts:

- **Fail closed where descriptor-relative traversal is unavailable.** The pathname
  route is **gone** rather than demoted: where the filesystem provider supplies no
  `SecureDirectoryStream` the run now refuses, naming the provider limitation,
  instead of measuring a member whose identity it cannot establish.
  `measureMemberByPathname` and the witness machinery that existed only for it were
  deleted, so there is no second route for anyone to try to force.
- **The root open bound to the root identity already verified.** The repository
  root's directory handle is opened **once**, at the moment the root is verified,
  and bound there to that root's `fileKey` — read a second time through the
  stream's own attribute view, so the binding describes the directory the
  descriptor holds rather than a name resolved again. No member measurement
  re-resolves the root by name, so a rename or substitution of the repository-root
  ancestor between verification and use can no longer send a safe-looking
  descriptor-relative descent into the wrong tree. That one handle is held for the
  life of the reader.
- **No measurement disclosed on any mismatch.** A manifest mismatch publishes
  **no** observed byte size and **no** observed sha256, whatever route produced the
  measurement, and the `identityEstablished` flag that used to license disclosure
  is gone. The held-handle walk still cannot rule out a substitution of the final
  component made after the pre-open attribute read and reverted before the
  post-digest read, and a same-size alternate member would otherwise turn the
  diagnostic into a digest oracle. The refusal keeps its precision — the member,
  its role, the route it was reached through, and the figures the manifest itself
  records.

The fourth restores what AAP §0.8.1 requires of the private graph copy: the copy and
its exclusive directory are **retained** after the load, at the read-only file mode and
non-writable directory mode the copy step set, so the identity figures this report
publishes can be re-measured from the bytes the engine actually read. The form this
replaced deleted both, which had to widen the directory to unlink and left every
private-copy figure unverifiable.

**All four rounds were then executed — twice, over two graphs.** The four rounds were
finished and committed before the 2026-09-01 execution, and the same committed bytes
were executed again on 2026-09-03 once the re-provisioning had replaced the graph. Each
query was run against its committed
source, and each envelope's `source_integrity.query_source_sha256` equals the digest of
the file beside it — the same three digests both times, which is what establishes that
only the graph moved:

| Query | Committed and executed | sha256 |
| --- | ---: | --- |
| 01 | **307,625** bytes | `79583377ffdc05762226f1437be94d953bf44be1ea94bbc3d9e48f072a27f4ac` |
| 02 | **369,754** bytes | `902b7ffe8d708d6cb4ddfc057f65b1a2a023fc90c5b55c8d3ba012885dcb3fd1` |
| 03 | **428,057** bytes | `8f67126c56185bde3221ad760130295cf9f7f64411be528e9fd578a4fbad631e` |

Each pair was measured twice and by two different readers: with `sha256sum` and
`stat -c%s` over the committed file, and by the running script over the file it read,
which prints it in its stream as *query source bytes* / *query source sha256*. The
envelopes are never rewritten to match a source — an envelope's digest records what
ran, so editing it would assert a run that never happened — and they did not need to
be: the agreement above is the consequence of executing the committed bytes.

**What this replaces.** An earlier revision of this section recorded a divergence: the
sources had been hardened after the generation then on the branch was executed, so each
envelope published a digest for text that no longer existed, and closing it required an
execution of the committed sources. That is what the three invocations recorded above
are. **The history is kept and the exact figures are not**, because they have no owner
this report can point a reader at. The superseded state was a pair per query — the bytes
and digest each envelope then published for its source, against the bytes and digest of
the source that had actually run — and both sides of all three pairs are **not
re-measurable in this tree**: the pre-hardening sources were never committed, so no
revision of `queries/joern/01-callgraph-unguarded-driver-launch.sc`,
`queries/joern/02-dataflow-unguarded-driver-launch.sc` or
`queries/joern/03-parameterized-handler-sink-pairs.sc` reachable from this branch carries
either side of a pair, and the superseded envelopes that published the committed side were
replaced rather than retained under a second name. Publishing the digits without an owner
would give a reader six figures no command here can check, so what is published instead is
the fact that each pair moved, in all three queries, across the four correction rounds
above. **What a human would need to reproduce them**: the working tree of the clone lane
that held the pre-hardening sources, at the revision immediately before each hardening
round, together with that lane's envelopes as they stood before the 2026-09-01 executions.
Neither is present in this checkout and neither is recoverable from its history.
`oss-scan-results/run-record.md` carries the divergence itself as **D19** with the same
closure.

### The private graph copies these three invocations retained

**Each of the three invocations left the private graph copy it loaded, and that copy's
exclusive directory, in place.** AAP §0.8.1 is explicit — "Do not tear anything down. No
cleanup, no reset, no temp purging. What the run built stays where it is" — and the
consequence of honouring it is that every private-copy identity pair this report cites
is **re-measurable from the bytes the engine read**, rather than only readable from the
stream that printed it. Each copy is retained at the mode the copy step set, `0400`
inside a `0500` directory, so retention does not widen what a later process may do to it:

| Query | Retained private copy (2026-09-03) | Inode of the bytes loaded |
| --- | --- | --- |
| 01 | `/tmp/blitzy-harness-scratch/424/probe-graph-input-c5f2f2b6ce1292ac40424cd9/spark.cpg` | `(dev=10301,ino=112547941)` |
| 02 | `/tmp/blitzy-harness-scratch/424/probe-graph-input-d6353bd7ff69afb7d0c52e69/spark.cpg` | `(dev=10301,ino=112807597)` |
| 03 | `/tmp/blitzy-harness-scratch/424/probe-graph-input-7e4dabb2ef5ef2e81bd0af3c/spark.cpg` | `(dev=10301,ino=112807645)` |

The paths and inodes are each stream's *private input (created)* and *private input after
load* lines — `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`
lines 38 and 69, `…probe-02-dataflow-unguarded-driver-launch.log` lines 38 and 69, and
`…probe-03-parameterized-handler-sink-pairs.log` lines 38 and 88 — and each stream's
*private input retained* line states the retention and its §0.8.1 ground. Every envelope
publishes `graph.private_copy_retained_after_verification = true`. All three copies
re-measure to **547,980,224** bytes and sha256
`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`, which is the graph's
own identity, so the copy each load read is the graph and not a truncation of it. The
directory index is the clone index the 2026-09-03 lane ran under, which is why the three
paths sit under `424` rather than under the `0` the 2026-09-01 generation's own streams
named; that generation's three copies were copies of the graph the re-provisioning has
since replaced, so their retained bytes are neither the current graph nor reachable
through this checkout, and no figure here is taken from them.

**What this replaces.** An earlier generation of these queries deleted both the copy and
its directory at the end of the run, and had to widen the directory back to writable in
order to unlink — a teardown §0.8.1 forbids, whose consequence was that the loaded bytes
could not be re-measured at all. `oss-scan-results/run-record.md` records that as
divergence **D18** with the same closure.

---

## What this report is, and what it is not

**It is** the report of the Stage 5 capability probe, and the owner of the probe's
per-query results. The question the probe exists to answer is *what a human can express
in Joern's query language against this graph* — a question the Stage 3 Joern runner's
baked query bundle cannot answer, because that bundle was not written to ask it.

**It is not** any of the following, and each exclusion is deliberate:

- **No comparison between tools.** This report does not compare Joern with Opengrep,
  Semgrep, `datadog-static-analyzer`, Trivy, Gitleaks, Checkov, OSV-Scanner,
  Dependency-Check, or with any commercial scanner, and it does not characterise what
  any tool's output demonstrates about that tool. No cross-tool interpretation of any
  kind appears below.
- **No judgement of any finding.** Nothing the probe reached is called real, important,
  a false positive, or a duplicate of anything else.
- **No remediation.** No patch, no mitigation and no exploit is proposed for anything
  the probe reached or for anything it could not reach.
- **No dataset row.** The probe writes nothing into `harness/artifacts/raw/` and
  contributes no row to `oss-scan-results/findings.json`. This tree is Joern's
  deliberate **second** appearance in the run — the Stage 3 runner is the first — and
  folding either appearance into the other's numbers would corrupt both that tool's
  count and the dataset total. Each envelope publishes
  `contributes_dataset_rows = false`.

Where a value could not be established it is **named as such** rather than omitted; see
"Values that could not be established".

## Inputs, and the one-measurement rule

Every figure below is **cited** from one of the files named in this section. This report
computes exactly one quantity of its own — the exception is named at the end of this
section, together with the two confirmations that publish no new value:

| Query | Envelope (machine-readable) | Prose result | Console stream | Standalone identity capture | Completion manifest |
| --- | --- | --- | --- | --- | --- |
| 01 | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` (72,781 B) | `queries/joern/results/01-callgraph-unguarded-driver-launch.md` (28,045 B) | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` (17,519 B) | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.identity.txt` (1,068 B) | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.publication.json` |
| 02 | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` (129,051 B) | `queries/joern/results/02-dataflow-unguarded-driver-launch.md` (43,014 B) | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` (22,707 B) | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.identity.txt` (1,067 B) | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.publication.json` |
| 03 | `queries/joern/results/03-parameterized-handler-sink-pairs.json` (142,173 B) | `queries/joern/results/03-parameterized-handler-sink-pairs.md` (65,870 B) | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` (31,791 B) | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.identity.txt` (1,067 B) | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.publication.json` |

Every byte size in that table is the size of the file as this generation published it.
For the three publication members — envelope, prose result and console stream — each
size is stated a second time by that query's completion manifest, which measures its
members from the disk after writing them; the identity capture is the driver's file
rather than a member, so its size has no manifest entry and is measured directly. The
2026-09-01 generation published 72,782 / 28,046 / 17,516 B for query 01,
129,051 / 43,014 / 22,704 B for 02 and 142,174 / 65,872 / 31,790 B for 03. Seven of
those nine sizes moved and two did not, which is the reason a size is never the test
of whether two files are the same generation: query 02's envelope and prose result are
byte-for-byte the same **length** across the two generations while their contents
differ, because the graph figures inside them changed digit for digit. The digests do
the work a length cannot, and the completion manifest carries them.

**Every published member of this generation, by byte size and sha256.** Fifteen files
carry the probe: for each query an envelope, a prose result and a console stream — the
three members of one publication — plus the driver's standalone identity capture and
the completion manifest itself. The digest column is the first sixteen hex characters;
each full digest is in the file named in the last column:

| Query | Member | Role | Bytes | sha256 (first 16) | Digest owner |
| --- | --- | --- | --- | --- | --- |
| 01 | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` | envelope | 72,781 | `80f49183faf24c33` | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.publication.json` |
| 01 | `queries/joern/results/01-callgraph-unguarded-driver-launch.md` | prose result | 28,045 | `2ee87f3576a06740` | same completion manifest |
| 01 | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` | console stream | 17,519 | `4b1643938846f1d5` | same completion manifest |
| 01 | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.identity.txt` | driver's identity capture | 1,068 | `ee4ae888944efb21` | not a publication member — read from the file |
| 01 | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.publication.json` | completion manifest | 2,312 | `1029a43aaef8ddec` | cannot carry its own digest — read from the file |
| 02 | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` | envelope | 129,051 | `2519dd0cfd36da45` | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.publication.json` |
| 02 | `queries/joern/results/02-dataflow-unguarded-driver-launch.md` | prose result | 43,014 | `dcf63ae62d3662b1` | same completion manifest |
| 02 | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` | console stream | 22,707 | `d99793ea46f3c833` | same completion manifest |
| 02 | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.identity.txt` | driver's identity capture | 1,067 | `332e346ccd96719e` | not a publication member — read from the file |
| 02 | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.publication.json` | completion manifest | 2,305 | `19ee1539ae0cb6f2` | cannot carry its own digest — read from the file |
| 03 | `queries/joern/results/03-parameterized-handler-sink-pairs.json` | envelope | 142,173 | `9b965f26c3e4aed7` | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.publication.json` |
| 03 | `queries/joern/results/03-parameterized-handler-sink-pairs.md` | prose result | 65,870 | `1232ca7704e6f788` | same completion manifest |
| 03 | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` | console stream | 31,791 | `d4aee84bee60f86f` | same completion manifest |
| 03 | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.identity.txt` | driver's identity capture | 1,067 | `4c21c2edd0146566` | not a publication member — read from the file |
| 03 | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.publication.json` | completion manifest | 2,305 | `d03b75eaa709298c` | cannot carry its own digest — read from the file |

The nine content members' sizes and digests are **cited**, each from the completion
manifest that owns it — the manifest names each member with its `path`, `bytes` and
`sha256`, and its own `member_set_id` is derived from exactly those filename-and-digest
pairs, so the table above and the identifier are two renderings of one measurement. The
six remaining rows have no such owner by construction: a completion manifest cannot
carry its own digest, and the identity capture is written by the invocation's driver
rather than by the query, so it is not a member of the publication. Those six sizes and
digests are read directly from the published files, which is the same kind of
confirmation as the `.sc` comparison described below rather than a new measurement of
anything the probe produced.

One cross-reference does **not** yet agree, and it is recorded rather than smoothed
over. The tree-wide per-file manifest at `harness/artifacts/MANIFEST.json`, read on
2026-09-03, still carries the preceding generation's figures for those six driver files
and for the three console streams — 339 B identity captures against the 1,068 / 1,067 /
1,067 B on disk now, console streams of 17,516 / 22,704 / 31,790 B against
17,519 / 22,707 / 31,791 B, and three different completion-manifest digests. That file
is not this report's to write; the values above are the files as this generation
published them, measured from the same disk, and the disagreement is named here so a
reader comparing the two documents finds it stated rather than has to discover it.

The query sources are `queries/joern/01-callgraph-unguarded-driver-launch.sc`,
`queries/joern/02-dataflow-unguarded-driver-launch.sc` and
`queries/joern/03-parameterized-handler-sink-pairs.sc`. The console evidence is
`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`,
`harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` and
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` — **each the
stream of the invocation that produced that query's figures**, each having run to
completion and emitted its result region, and each ending in the success marker.

**The standalone identity captures were re-anchored by the 2026-09-03 driver to the
identity each probe actually verified, and each states its own supersession.** Each
`probe-<query>.identity.txt` is written by the *driver* of an invocation rather than by
the query, so it is not one of the three publication members; what it carries is the
pre-load pair, the check's elapsed time, the verdict, the record of account, the clone
the invocation ran in, and a `supersedes` line naming the same file as the 2026-09-01
lane's driver wrote it — which stated 541,309,809 / `4616845a…`, a graph provisioning
has since replaced. A capture whose whole value is that it was taken before a load
cannot be re-dated after one, so these were not re-dated: they were **rewritten by the
driver of the load they gated**, and each names that load. The pre-load check that
gated each of the three invocations this report cites is nevertheless the one in that
invocation's **own console stream**, cited line by line under "How the graph was
loaded"; the capture is a second rendering of it and not its owner.

The binding is not an assertion: every query publishes its envelope, its prose
result and its console log as the three members of **one publication**, each member
re-read from disk and digested after writing. **Two identifiers do two different
jobs, and each is cited from the file that owns it.** The `publication_id` in the
envelope is derived from the query id, its source digest and the graph's digest, size
and method count — so it is fixed before any member exists and identifies **that
input tuple and nothing more**: `efc857cebf2044a1…` for 01, `7a07b7184af2306b…` for 02,
`58d6c2734d126da3…` for 03. All three moved with the graph rather than with the
queries: the graph's digest, byte size and method count are three of the identifier's
inputs, so the 2026-09-03 re-provisioning changed every one of them even though the
three sources are byte-identical to what the 2026-09-01 generation ran, which
published `282448edaac93a9f…`, `4f331b6f1163bdb3…` and `89c38f6d823f564b…`. That is the
derivation behaving as documented and not a second kind of identifier. It is
**repeatable across separate invocations
by design**, and that follows from the derivation each envelope publishes rather than
from any observation: every input to it — the query id, the source digest, the graph
digest, the graph's byte size and its method count — is identical for two invocations
of an unchanged source over an unchanged graph, so the identifier cannot distinguish
them. That repeatability is the byte-identity contract each envelope states under
`determinism`, not a weakness — a nonce would satisfy uniqueness and destroy it — but
it means `publication_id` names **no execution**, and nothing here claims it does.

The `member_set_id` lives in that query's completion manifest rather than in its
envelope, because an envelope cannot carry an identifier derived from its own bytes;
it is taken over each member's target filename and sha256 and so identifies the
**exact set of member bytes on disk**: `2420e60ab58cabe1263565fa3693e4bf` for 01,
`d9315f57751cad5002c21986426c3c21` for 02, `6313d5848fd13db48cdfc808f006e23c` for 03.
The 2026-09-01 generation's three sets were `e1ae3483641fff3baf04175896209e3f`,
`e6a828678e4bace49f39aa9b74eb77ee` and `b0b9e4d66582c84fa0de94bf6e66c646`; no member of
any of them is on disk now, which is precisely what this identifier is for.
This is what it adds over the publication identifier: because it depends on the
member bytes, it changes whenever any member changes — even when the publication
identifier cannot, because its own inputs did not. So the member-set identifier is
what distinguishes two generations that share a publication identifier.

**What the pair does and does not establish.** Differing publication identifiers
across two members prove two generations; **equal ones do not prove one**, and the
converse is not asserted anywhere. What binds this log to this envelope is the
completion manifest: it names each member with its byte size and sha256, it is renamed
only after every content member is in place, and the producer re-measures every member
against it immediately after publishing and stops the run on any disagreement. That
establishes the set on disk is exactly the set the producer published and that it is
complete. It does **not** encode an invocation nonce, so two invocations over an
unchanged source and graph that produce byte-identical members are indistinguishable —
which is the determinism contract holding, and is stated here rather than hidden
behind a uniqueness claim the derivation cannot support.

Each of the six result files states, in its own words, that this document must
**cite its measurements rather than re-measure them**. That is the
one-measurement-cited-twice rule, and it runs in one direction: where a count
appears both here and in an envelope it is one measurement cited twice, and if the
two ever disagreed **the envelope is right and this file is wrong**. Two
consequences worth stating plainly:

- No figure here is a second measurement of the graph, of a query's return set or
  of a source file. The one comparison this document performs against the branch —
  `sha256sum` and `stat -c%s` over each committed `.sc` — publishes **no new value**:
  it agrees with the pair the envelope already published, and is reported as a
  confirmation that the bytes on the branch are the bytes that ran. The six
  driver-file sizes and digests in the member table above — the three identity
  captures and the three completion manifests, which no envelope or manifest can own —
  are read the same way and are confirmations of what this generation wrote, not
  measurements of anything the probe produced.
- Exactly one quantity in this report is **computed here rather than cited**: the
  probe-wide **union** of the three per-query Joern API construct lists. It is a
  **document-side aggregation**, and that is stated as this file's own choice rather
  than as a delegation: no per-query result file asks this file to compute it, and
  none says the union is owned here. Each query owns and publishes its own
  deduplicated list and its own count; the union exists because three partial lists
  in three files answer the probe-wide question less well than one aggregate in one
  place. It is an aggregation over published lists, not a new measurement, and the
  lists it aggregates are reproduced below so the arithmetic is auditable.

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

**`importCpg` only, and the check is textual as well as behavioural.** All three queries
load the graph with `importCpg` into a switched workspace at
`queries/joern/.workspace`; each envelope publishes
`runtime.loader_is_importcpg_only = true`, and **all three** additionally publish
`runtime.loader_alternative_absent_from_the_source = true` — the key's actual name,
alongside `source_integrity.alternative_loader_occurrences_in_the_source = 0` and
`source_integrity.alternative_loader_absence_is_measured = true`. The
appearance of the alternative loader in a committed query source would itself be
the violation — not merely its execution — so the absence was confirmed by
**searching the committed sources textually**: `importCode` occurs **zero** times
in each of the three `.sc` files under `queries/joern/`, and all three prose
results record that the absence was established by searching those files rather
than inferred from what the run happened to do. **The textual check was
performed, and it passed.**

The graph's identity was **re-verified immediately before each of the three probe
loads this report owns**, by byte size and sha256 with the symlink **followed** —
the named path `harness/cpg/spark.cpg` is a small symlink, so measuring the link
itself would record a few dozen bytes rather than the graph. Each of those three
loads was compared against the record of account for the bytes it was about to
read, and each comparison matched. The run's other two loads are not this report's
to certify and no claim is made here about them: the Stage 2 verification load is
`cpg-verify.log`'s to record, and the Stage 3 Joern runner's belongs to
`oss-scan-results/build-record.md` §5 and to `oss-scan-results/run-record.md` §13
**D4**, where the ordering of that check against its load is stated in full.

| Query | Graph, symlink-followed | sha256 | Verified against | Result | Check took |
| --- | --- | --- | --- | --- | ---: |
| 01 | 547,980,224 bytes | `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` | `provision-log/cpg-identity.txt`, the record of account resolved by provenance, corroborated by `cpg-record.txt` | match on both values | 3,947 ms |
| 02 | 547,980,224 bytes | `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` | the same record of account, resolved the same way | match on both values | 4,112 ms |
| 03 | 547,980,224 bytes | `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` | the same record of account, resolved the same way | match on both values | 3,995 ms |

That pair is the graph the 2026-09-03 re-provisioning left at the sanctioned path.
The 2026-09-01 generation of these three probes verified and loaded 541,309,809 bytes
and sha256 `4616845a…` — correctly, against the record of account as it then stood —
and the re-provisioning replaced those bytes, so that pair is kept here as **that
generation's** and is not restated as a property of the graph a reader can measure
today. Each of the three checks above resolved its own record of account and compared
both values before its own load, so what the table establishes is a property of the
check rather than of which generation happened to be on disk.

**One graph, three loads, and the copy each load actually read.** No query imports
the shared file directly. Each copies it once into a private input under
`$HARNESS_SCRATCH_DIR` created at mode 0700, digests it during the copy pass,
compares that digest against the record of account, imports **only** the copy, and
then re-measures the copy's size, sha256 and inode after the load — all three
publish `private_copy_verified_after_load = true`. So the identity is checked on
the bytes the engine received rather than on a path that could change between the
check and the read.

**Why the record of account is not `cpg-frontend.log`.** That file is this run's own
frontend invocation over the complete 191-artifact input set — the invocation that
failed in serialization, divergence **D1** in `oss-scan-results/run-record.md` §13 —
and it carries **no** write-time `bytes:`/`sha256:` pair at all. Each query resolves
its record of account by **provenance** rather than by a fixed path: the in-checkout
frontend log when it carries exactly one strict pair, and otherwise the provisioning
record beside the resolved graph. Here the first candidate supplies nothing, so the
second governs, and each envelope publishes which record it used, that record's
provenance, and the corroborating record that agrees with it
(`identity_record_candidates_read = 2`, `identity_comparison_result = match`).
Ambiguity inside one record and disagreement between two are both fatal to the
query, so a silently-chosen pair is not expressible.

**This path has held three generations across this run's lanes, and each is kept with
the record that states it.** The earliest, 541,255,894 / `26d327cc…` with 1,397,339
methods, was stated in this tree by the provisioning record `harness/ENVIRONMENT.md`
§7 and by nothing this run measured; the second, 541,309,809 / `4616845a…`, is what
the 2026-09-01 probe generation verified and loaded; and the third is the one on disk
now, 547,980,224 / `325887cf…`, written by the 2026-09-03T01:17:07Z re-provisioning
and measured by all three probe envelopes this report cites. `harness/ENVIRONMENT.md`
§7 states that third pair as re-anchored on 2026-09-03 and keeps the two earlier ones
in its own supersession appendix; that document owns its own state and this report
does not restate it beyond naming which pair it carries.
`harness/artifacts/logs/cpg-verify.log` records the **current** pair —
547,980,224 / `325887cf…`, at its lines 40-41 and again at 54-57 — and names the
earliest one only at its lines 84-88, as the record the filesystem contradicted;
`harness/artifacts/logs/joern-preflight.log` records the same current pair as MATCH on
both values with `VERDICT: PASS`; and `harness/artifacts/logs/joern.status` is the
runner's seven-line `scope_finish` trailer and states **no graph identity at all**.
Neither of the two earlier pairs is what any load reported here read.

Both names for the graph — the plan's `harness/cpg/spark.cpg` and the
environment's exported variable — resolve to the same file for every one of the
three loads; each envelope publishes that reconciliation as *same file (equal
resolved target)*. No absolute host path is emitted by any envelope.

**The table above carries one pair, and that is a fact about the graph on disk
rather than about the method.** The path is a host-shared read-only file this run
neither rebuilds nor replaces, and the three generations it has held are enumerated
once, in the paragraph above, with the record that states each; `run-record.md`
**D4** is the register entry that keeps them. A generation no record states is not
restated as though it were measured, and a generation a record stated for bytes that
are gone is not restated as though it described the graph now. Every load reported
here verified against the record of account for the bytes it was about to read and
matched it, which is a property of the check rather than of which generation happened
to be on disk.

**On the reproduction check, this file follows its owners exactly.** Each envelope
publishes `determinism.reproduction_check_status` as **not attempted from inside this
run** — a run cannot launch and compare a second copy of itself without becoming the
thing it measures — so the reproduction contract is published as a contract, with the
command, the working directory and the graph selector a reader needs to execute it,
and nothing is claimed about having run it. **No reproduction check was attempted, no
comparison of two envelopes was made, and no such invocation is described here.**

**That the identity gate can refuse is established by a committed negative test,
not by narrative.** `harness/artifacts/logs/joern-preflight-negative-test.log`
perturbs the record of account, drives the **wrapper** rather than the gate alone, and
records the refusal end to end: `sha256 … MISMATCH`, `VERDICT: HALT`, gate exit
**77**, `wrapper exit status : 77`, and the runner having produced no output and left
its artifact untouched. It then restores the record and re-runs, reporting
`VERDICT: PASS` at exit 0 — so the refusal is attributed to the perturbation and
nothing else. In the three runs this report describes, the same mechanism resolved the
record of account and matched it before loading. One mechanism, both directions
evidenced by a tracked file, which is why its passing means something. **No earlier
refused or halted probe invocation is described here**: any such run left no committed
artifact, so there is nothing to attribute it to and nothing is asserted about it.

The pre-load check itself, per query, as its own console stream records it:

| Query | Measured before the load (symlink followed) | Compared against | Verdict | Stream lines |
| --- | ---: | --- | --- | --- |
| 01 | 547,980,224 / `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` | `provision-log/cpg-identity.txt`, corroborated by `provision-log/cpg-record.txt` | `PASS - re-verified immediately before the load` | 36, 45-51, 53 |
| 02 | 547,980,224 / `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` | the same two records, which agree | `PASS - re-verified immediately before the load` | 36, 45-51, 53 |
| 03 | 547,980,224 / `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` | the same two records, which agree | `PASS - re-verified immediately before the load` | 36, 45-53, 55 |

Each check is in the stream of the invocation it gated, in the same terms in all three:
`size WITH following : 547980224  (the measurement of record)`,
`recorded at write time : bytes=547980224 sha256=325887cf…`,
`record of account : provision-log/cpg-identity.txt`,
`corroborated by : provision-log/cpg-record.txt, which agree`, `byte size matches : YES`,
`sha256 matches : YES`, `AAP-named path : same file (equal resolved target)` and
`graph identity : PASS - re-verified immediately before the load`. The comparison is
therefore performed by the query that is about to load, against a record it resolves from
its own source, and its result precedes the load in the same stream. The 2026-09-01
generation's three streams carried the same seven lines with `541309809` and
`4616845a…` in place of the two figures above; those streams were replaced by these,
and the quoted lines here are the 2026-09-03 ones.

**Each load read a private copy, and the binding to it is measured too.** Every stream
records the copy step — `bytes copied : 547980224` in 3,840, 3,973 and 3,890 ms for
queries 01, 02 and 03 — then re-measures the copy after the load and states
`import binding : PASS - the imported copy is byte-for-byte and inode-for-inode the
input the identity check measured`. So the identity is bound to the bytes the engine
received and not merely to a path.

**The three ran one at a time, each in its own JVM.** Each invocation copied the graph
into a directory it created and retained, and the three retained copies are distinct
files — inodes `(dev=10301,ino=112547941)`, `…112807597` and `…112807645`, whose
modification times order the runs — while each run additionally held an **exclusive
lock** inside a
workspace directory it created for itself, published as `runtime.workspace_lock_held` and
`runtime.workspace_run_directory`. Two Joern processes sharing one workspace is what
corrupts a workspace, and no two of these three shared one.

**The record of account is a `cpg-identity.txt`, resolved by provenance and never by
which candidate matched, and no environment variable can select another.** Each query
source fixes the resolution in its own text — envelope 03 publishes
`graph.identity_record_override_exists = false` and
`graph.identity_record_source` states that no environment variable selects it — and each
run publishes the record it resolved as `graph.identity_record = provision-log/cpg-identity.txt`,
having read **two** candidates and required them to agree
(`graph.identity_record_candidates_read = 2`). `harness/artifacts/logs/cpg-identity.txt`
is the in-checkout transcription of a record of account, written by the same
`record_of_account()` function; the pair every figure in this report rests on is the one
the three queries resolved and re-verified for themselves, published in their own
envelopes and streams, so a transcription is a second rendering rather than this
report's source. The record is deliberately
**not** the frontend's run log: `harness/artifacts/logs/cpg-frontend.log` carries no
write-time `bytes:`/`sha256:` pair at all, because the invocation it records produced no
accepted graph.

**Who wrote these bytes.** Not this run, and not the provisioning the earlier
generations read. The graph these three loads read was written by the re-provisioning
of 2026-09-03T01:17:07Z, whose own write-time record of account states the pair, the
frontend invocation and the window — `jimple2cpg` (Joern 4.0.607 bundled) over the
provisioned input directory with `--recurse` under Temurin 21.0.12.1+1 at
68,719,476,736 bytes of heap, `FRONTEND_EXIT=0`, 31 m 23 s from
2026-09-03T01:40:31Z to 02:11:54Z — and whose `importCpg` verification recorded
1,398,964 methods, 119,860 type declarations and 45,037 files. Each query resolved
that record by `harness/lib/preflight_graph_identity.py`'s own `record_of_account()`,
the same function the Stage 3 preflight gate uses, so the record and that gate cannot
state different pairs, and each stream prints what it read as
`recorded at write time`. That record is the provisioning's own file beside the graph
and sits outside this checkout, so its two figures that no stream prints are given
their in-checkout owners here: the window is stated in-tree by `harness/ENVIRONMENT.md`
§7, and the verification triple by `harness/artifacts/logs/cpg-verify.log`.

**One in-tree file must not be mistaken for that record.**
`harness/artifacts/logs/cpg-graph-record.log` is the **earlier** generation's graph
record — its own lines state `jimple2cpg` over the provisioned input directory at
its line 3, `50 m 42 s (18:28:00Z -> 19:18:42Z), FRONTEND_EXIT=0` at its line 6,
sha256 `4616845a…` at its line 12 and 541,309,809 bytes at its line 63 — so it
describes the bytes the 2026-09-03 re-provisioning replaced and not the ones these
three loads read. It is named here, with its generation, for exactly the reason every
superseded figure in this report is: an unlabelled record of a replaced graph reads as
a current one.

The 2026-09-01 generation resolved the same way and read the
record as it then stood, which named a graph written at 2026-08-30T19:18:37Z with its
identity recorded beside it at 19:19:09Z; that graph is the one the re-provisioning
replaced. This run's own frontend was invoked over the complete staged input manifest and
terminated in the serialization phase at a fixed JVM array-length bound inside
flatgraph's graph writer — `java.lang.OutOfMemoryError: Required array length
2147483639 + 72 is too large`, `harness/artifacts/logs/cpg-frontend.log`, exit 1 —
**writing no accepted graph**; `harness/artifacts/logs/cpg-ceiling-reverify.log`
establishes first-hand in this clone that the bound is an array length rather than a
heap shortage, by running the same probe at **three** heaps — `-Xmx8g`, `-Xmx64g` and
`-Xmx128g`, a sixteenfold span of reported `maxMemory` — and observing the identical
failure at the identical 2,147,483,639 buffered bytes in all three. The run record's divergence register carries that as **D1**. The
consequence for this report is stated rather than smoothed over: the graph these three
queries read is the provisioning graph at the sanctioned path, and every count below is
a count over those bytes.

Both names for the graph — the plan's `harness/cpg/spark.cpg` and the environment's
exported variable — resolve to the same file for every one of the three loads; each
envelope publishes that reconciliation as *same file (equal resolved target)*. No
absolute host path is emitted by any envelope, and each publishes that as a measured
result rather than an assertion: of the 8 absolute paths the run resolved, **0** occur
in the rendered envelope.

Per-query runtime, each field published by that query's own envelope and printed on its
console stream:

| Query | JDK major | JVM | Heap actually used | Relative to the floor | Loader | Load | Total |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 01 | 21 (the required major) | `21.0.12.1+1-LTS` | 68,719,476,736 bytes = 64 GiB | at the floor of 68,719,476,736 bytes | `importCpg` into `queries/joern/.workspace` | 570,738 ms | 577,899 ms |
| 02 | 21 (the required major) | `21.0.12.1+1-LTS` | 68,719,476,736 bytes = 64 GiB | at the floor, not above it | `importCpg` into `queries/joern/.workspace` | 547,825 ms | 652,495 ms |
| 03 | 21 (the required major) | `21.0.12.1+1-LTS` | 68,719,476,736 bytes = 64 GiB | at the floor of 68,719,476,736 bytes | `importCpg` into `queries/joern/.workspace` | 560,129 ms | 567,063 ms |

The six elapsed figures are the 2026-09-03 ones. The 2026-09-01 generation's own
streams stated loads of 697,972 / 695,506 / 683,810 ms and totals of
704,629 / 836,873 / 690,631 ms over the graph the re-provisioning replaced; an elapsed
time is excluded from every envelope's byte-identity contract as a non-deterministic
quantity, so the two generations' times are recorded rather than reconciled.

The floor is a minimum and a default rather than a ceiling: a larger heap is permitted
and reported, a smaller one is not, because a truncated result's silence cannot be told
apart from a clean one. Each envelope records the heap as **measured** rather than as
requested — the launcher's `-J-Xmx` reaches the launcher only, so the heap the query
actually runs at is measured from inside the child JVM and the query halts below the
floor rather than trusting the flag it was given. Each query is the fourth of the run's
four heap-bound JVM invocations: the frontend build, the `importCpg` verification load,
the Stage 3 Joern runner, then this probe.

**The invocation, as executed on 2026-09-03.** Each envelope publishes the command it
was run under twice — as `runtime.command` and, in identical text, as
`determinism.reproduction_command` — and each is complete and runnable as written, in
the envelope's own terms: *every environment value this query reads appears in it, and
it reads no other*. For query 03 the form is

```text
cd "$HARNESS_SCRATCH_DIR" && HARNESS_REPO_ROOT="$HARNESS_REPO_ROOT" HARNESS_CPG="$HARNESS_CPG" \
  JAVA_HOME="$JAVA_HOME_21" JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN \
  joern --script "$HARNESS_REPO_ROOT/queries/joern/03-parameterized-handler-sink-pairs.sc" \
  -J-Xmx64g < /dev/null
```

and queries 01 and 02 differ from it only in the `--script` path. Four properties of
that form are load-bearing rather than incidental. The working directory is
`$HARNESS_SCRATCH_DIR`, the clone-private scratch root **outside** the repository,
because Joern eagerly creates `./workspace` in its own working directory and nothing
named `workspace` is ignored by the repository's root `.gitignore` — running from a
checkout would leave engine state inside it. Standard input is closed with `< /dev/null`
because Joern's REPL blocks on an open stdin, which is also why its version is read from
its startup banner rather than from a `--version` flag it does not expose
(`probe-01-…log` line 15, `probe-02-…log` line 16, `probe-03-…log` line 15). The graph
is selected explicitly by `$HARNESS_CPG` rather than left to a default, because that
value selects the bytes the query loads — the one set of bytes both of query 03's pairs
are measured against — and each envelope publishes which of the variable and the
repository-relative default actually applied as `graph.path_source`, which reads
`HARNESS_CPG` in all three. And the three `HARNESS_*`
values appear as variable references rather than as literal paths, because an absolute
path is a property of a checkout rather than of the measurement; the precondition
published beside each command names what exports them
(`BLITZY_CLONE_INDEX=<this clone's index> ; . harness/env.sh`).

Query 03 passes **no parameter on the command line**: both pairs are declared as named
constants in the query source and both are invoked in the single run the command above
performs, which is what makes the parameterizability verdict reproducible from that
command alone rather than from an argument a reader would have to reconstruct.

**Every JVM argument observed is accounted for, and none was redacted.** Each query
counts the JVM arguments its own child process was started with, publishes them under a
whitelist, and reports the reduction so it cannot pass unnoticed:

| Query | Observed | Kept as written | Reduced to their keys | Redacted keys |
| --- | --- | --- | --- | --- |
| 01 | 1 | `-Xmx64g` | 0 | none |
| 02 | 1 | `-Xmx64g` | 0 | none |
| 03 | 1 | `-Xmx64g` | 0 | none |

The figures are each query's `runtime.jvm_arguments_observed_count`,
`jvm_arguments_kept`, `jvm_arguments_redacted_count` and `jvm_arguments_redacted_keys`,
and the same three lines are printed on each console stream — `probe-01-…log` lines
12 to 14, `probe-02-…log` lines 13 to 15 and `probe-03-…log` lines 12 to 14 — as
`JVM memory/stack args : -Xmx64g`, `JVM other args (reduced) : <none>` and
`JVM args kept / reduced : 1 logged as written, 0 reduced to their keys, 1 observed`.
The policy each envelope states is a whitelist: an argument whose key is one of the
memory or stack flags the query names is logged as written, *because the heap it
establishes is the evidence*, and every other argument is reduced to its key with its
value replaced by `<redacted>`. Nothing was reduced here because the only argument
observed was the heap flag itself, and that is the argument the whitelist exists to
publish — so the disclosure and the heap evidence are the same line, with no value
outside the whitelist reaching any log, status field or published record.

The graph counts each query read, as published by its envelope — **one graph, so one
triple, identical in all three**:

| Query | Methods | Type declarations | Files |
| --- | --- | --- | --- |
| 01 | 1,398,964 | 119,860 | 45,037 |
| 02 | 1,398,964 | 119,860 | 45,037 |
| 03 | 1,398,964 | 119,860 | 45,037 |

All three agree, because all three loaded the same generation — 547,980,224 /
`325887cf…` — each verifying it against its record of account before its own load.
Three identical readings from three loads in three separate JVMs is a reproducibility
check on the counts rather than one measurement restated three times, and the triple is
the same one the re-provisioning's own `importCpg` verification recorded at write time
and `harness/artifacts/logs/cpg-verify.log` publishes against the AAP's expected
values. The 2026-09-01 generation read 1,396,899 methods and 119,721 type declarations
over the graph it loaded, with the same file count of 45,037; those two figures are
that generation's and are not properties of the graph on disk now.

---

## The per-query result contract, at a glance

One row per query, and — for query 03 — the two pairs kept side by side. Every figure is
cited from that query's envelope.

| | 01 callgraph | 02 dataflow | 03 parameterized |
| --- | --- | --- | --- |
| Compile status | compiled | compiled | compiled |
| Run status | completed | completed | completed |
| Returned record count | 4 | 8 | 6 |
| Record kinds | 4 boundary, 0 route | 4 boundary, 0 route, 2 boundary-flow, 2 liveness-control-flow | 6 boundary; per-pair route records 0 and 0 |
| Distinct routes | 0 | 0 | `pair-one` 0, `pair-two` 0 |
| Spurious count | 0 | 0 | `pair-one` 0, `pair-two` 0 |
| Bound value | 12 (`MAX_CALL_DEPTH`) | 6 (`MAX_FLOW_CALL_DEPTH`) | 12 (`MAX_CALL_DEPTH`), applied per pair |
| Bound reached | **yes** | **no** | `pair-one` **yes**, `pair-two` **yes** |
| Entry points discovered | 2 | 2 | `pair-one` 2, `pair-two` 1 |
| Entry points traversed | 2 | 2 | `pair-one` 2, `pair-two` 1 |
| Entry points truncated | 0 | 0 | `pair-one` 0, `pair-two` 0 |
| Expected-spurious route absent | yes, basis structural | yes, basis structural | `pair-one` yes, `pair-two` yes, both structural |
| Duplicate formulation (aggregate) | partial_duplicate | not_duplicate | partial_duplicate |

**No total appears in that table, by construction.** Routes are never summed: not across
queries — 01 and 02 address the *same* handler/sink pair by two different formulations,
so adding their returns would double-count one pair — and not across query 03's two
pairs, whose figures are reported side by side. Each envelope publishes its own
`never_summed_with` list naming exactly what its figures must not be added to, and query
03's list names *the other pair in this query* first.

**The route-identity function differs per query, and each is stated so distinctness is
auditable rather than asserted:**

- **Query 01** — the triple *(entry point method full name, sink host method full
  name, the ordered sequence of **(from method, call site callee, to method) hops**
  from the entry point to the sink)*. The two walks are deduplicated against each
  other on it rather than added together, "which is where the hop sequence does the
  work, since the two walks expand dynamic-dispatch fan-out differently". Its own
  envelope also states the limit: **within** one walk the retained chain is the
  shortest the BFS parent map yields, so a second arrival at a sink host already
  witnessed from the same entry point is counted under
  `alternate_sink_arrivals_not_retained` rather than retained as a second identity.
- **Query 02** — the triple *(source group, sink group, element signature)*, the
  signature being the ordered sequence of `LABEL@enclosing-method#graph-line` over
  the flow's elements. The three route-bearing arms are deduplicated on that triple
  and never summed; the unit is the distinct **flow**, and every record publishes
  the signature its identity was taken on.
- **Query 03** — the triple *(entry point method full name, sink host method full
  name, the ordered sequence of **method full names** from the entry point to the
  sink)*, evaluated **within one pair** and never applied across pairs. It carries
  the same enumerated-versus-counted limit as query 01, published per walk and per
  pair.

The three are **not** interchangeable, and the difference is the point: 01 keys on a
hop triple, 03 on a method-name sequence, 02 on a flow-element signature. Each
sentence above is that envelope's own `distinct_routes_identity_function`.

**Entry-point counters are reported as two separate numbers on purpose.** A
traversed count exists so a sweep cannot run unbounded; a truncated count exists
so a trimmed traversal cannot pass for a complete one. Every truncated count in
this probe is **0**, and in query 01's case the envelope states why it is zero
rather than leaving it bare: 2 entry points were discovered against a cap of 16.

## "Spurious", defined mechanically — and it judges the query

**The definition, exactly as the three queries implement it:** a route is spurious
**only** where the handler *does* pass an authorization or ACL predicate before reaching
the sink, the predicate set being exactly the five named selectors below.

**The five selectors, repeated from the queries.** All five are anchored on the bytecode
type `org.apache.spark.SecurityManager` and were verified present at the pin in
`core/src/main/scala/org/apache/spark/SecurityManager.scala`, a 457-line file:

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

On bytecode the anchored pattern alone is not enough, and the envelopes publish the
three-step narrowing that gets from the pattern to the five: step 1 matched seven names
on the anchored type — `aclsEnabled`, `aclsOn`, `aclsOn_$eq`, `checkAdminPermissions`,
`checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled`; step 2
excluded the one setter, `aclsOn_$eq`, leaving six; step 3 dropped the one non-predicate
residue, `aclsOn` — the private `var` Scala compiles into accessors — leaving exactly
**five**. For scale, the anchored type carries 252 method nodes under 107 distinct
names. Two Boolean methods on the same type are named as **deliberate non-selectors**
rather than quietly dropped: `isEncryptionEnabled()` at `:280` and `isSslRpcEnabled()`
at `:295`. The set **was not widened** (`selector_set_was_widened = false`), and
envelope 03 records that its selector block is **byte-identical** to the blocks in the
other two sources, so the three spurious counts stay comparable. Within query 03 the
parameterization varies the handler and the sink and **never** the predicate set —
varying it per pair is the one way a parameterization could silently change what a
spurious count means.

**This judges the query, not Spark.** The measure exists to say whether a traversal that
returned routes would have been returning routes it should have filtered. It says
nothing about whether Spark authorizes anything, and no sentence in this report should be
read as an assessment of Spark. No finding is judged real, important or a false positive
here.

**Was an expected-spurious route absent? Yes, in every case, and the basis is structural
in every case.** Per query and per pair:

| Query / pair | Spurious count | Expected-spurious route absent | Basis | Predicate call sites on its own route surface |
| --- | --- | --- | --- | --- |
| 01 | 0 of 0 | yes | structural | 0 |
| 02 | 0 | yes | structural | 0 |
| 03 `pair-one` | 0 | yes | structural | 0 |
| 03 `pair-two` | 0 | yes | structural | 0 |

*Structural* is a stronger statement than *the filter found none*, which is why the
envelopes distinguish them: **no call site of any of the five exists on the route surface
at all**, so no route of these pairs could have passed one. The absence is therefore a
property of the route surface rather than evidence that the query filtered well.

**The zero is scoped to the route surfaces, not to the program.** The five predicates
*are* invoked elsewhere: **18 call sites graph-wide in 18 distinct callers**, a figure
every envelope publishes, including three call sites inside the anchored type itself
(`SecurityManager.scala:249`, `:265`, and `:407` inside the private `isUserInACL`
declared at `:402`). Query 03's console stream publishes the per-prefix sweep behind its
zero, over the six route-surface prefixes its two pairs name:

| Route-surface prefix | Type declarations | Methods | Predicate call sites | Cited by |
| --- | --- | --- | --- | --- |
| `org.apache.spark.deploy.master.Master` | 217 | 607 | 0 | `pair-one`, `pair-two` |
| `org.apache.spark.deploy.rest.StandaloneRestServer` | 2 | 13 | 0 | the shared prefix list only |
| `org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet` | 25 | 32 | 0 | `pair-two` |
| `org.apache.spark.deploy.worker.DriverRunner` | 21 | 93 | 0 | `pair-one`, `pair-two` |
| `org.apache.spark.deploy.worker.ProcessBuilderLike` | 6 | 9 | 0 | `pair-one`, `pair-two` |
| `org.apache.spark.deploy.worker.Worker` | 156 | 475 | 0 | `pair-one`, `pair-two` |

Every one of the six is present in the graph, none is present-but-method-less, the type
declaration sweep did not truncate, and the total predicate call sites on them is 0.
Envelope 03 adds the source-level counterpart: searching all five names across the four
route files at the pin — `Master.scala`, `StandaloneRestServer.scala`,
`DriverRunner.scala` and `Worker.scala` — returns nothing in any of them, and
`StandaloneRestServer.scala` carries no reference to `org.apache.spark.SecurityManager`
at all at the pin.

**The definition's limit, stated so a zero is not over-read.** The definition evaluates
**only** those five predicates. Any other conditional on a route is outside it and is not
assessed by it. The concrete case the envelopes name:
`core/src/main/scala/org/apache/spark/deploy/master/Master.scala:411`,
`if (state != RecoveryState.ALIVE)`, guards the branch that reaches `createDriver` at
`:417` — a recovery-state check rather than one of the five, so it is neither counted as
a predicate nor reported as one. A spurious count of 0 therefore means exactly and only
what the definition says, and **does not** mean the route carries no conditional.

## The target surface, verified at the pin

Every line below is a line of the pinned tree at
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, cited as the queries measured it.

| Role | Anchor at the pin |
| --- | --- |
| Pair one handler | `core/src/main/scala/org/apache/spark/deploy/master/Master.scala:409` — `override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]` |
| Its submit case | `Master.scala:410` — `case RequestSubmitDriver(description) =>` |
| The conditional that is *not* a predicate | `Master.scala:411` — `if (state != RecoveryState.ALIVE)`, with the continuing branch at `:415` |
| Driver creation | `Master.scala:417` — `val driver = createDriver(description)`, and the second call at `:1130` inside `relaunchDriver` (declared at `:1121`), both against the private definition at `:1356` |
| The RPC send | `Master.scala:1367` — `worker.endpoint.send(LaunchDriver(driver.id, driver.desc, driver.resources))`, reached through `schedule()` (`:944`), `canLaunchDriver` (`:923`) and `launchDriver` (`:1363`) |
| The relay handler | `core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala:523` — `override def receive`, with `case LaunchDriver` at `:687` |
| The thread hop | `core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala:123` — `}.start()`, closing the `Thread` opened at `:89` whose `run()` body is at `:90` |
| **The sink** | `DriverRunner.scala:240` — `process = Some(command.start())` |
| Its abstract declaration | `DriverRunner.scala:270` — `def start(): Process`, on the trait declared at `:269` |
| Its concrete implementation | `DriverRunner.scala:276` — `override def start(): Process = processBuilder.start()`, the anonymous implementation |
| Pair two handler | `core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala:268` — `handleSubmit` |
| Pair two's RPC send | `StandaloneRestServer.scala:276-277` — the `askSync` of `DeployMessages.RequestSubmitDriver(driverDescription)` |
| The message types | `org.apache.spark.deploy.DeployMessages$LaunchDriver` (`DeployMessage.scala:176`) and `org.apache.spark.deploy.DeployMessages$RequestSubmitDriver` |

**Type precision on pair two, because getting it wrong would be invisible.**
`handleSubmit` at `StandaloneRestServer.scala:268` is **not** a member of
`StandaloneRestServer`. **The seven-class table below is read from the pinned source
file itself, which is its owner; envelope 03 does not carry it.** What envelope 03
publishes is the type it actually selected on —
`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet`, in
`parameter_values_supplied.pair-two.handler_type` — and the table is here because it
is what makes that selection checkable. At the pin the file declares seven classes —
`StandaloneRestServer` at `:56`, `StandaloneKillRequestServlet` at
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

### That surface is present in the graph, measured in two independent loads

The zeros below are only interpretable if the types the queries reason about are in the
graph at all, so their presence is measured rather than assumed. The Stage 2
verification of this graph swept the six types the probe names and published the counts
in `harness/artifacts/logs/cpg-verify.log`, PART 3.3, from two separate loads of the
same bytes:

| Type | Type declarations | Methods |
| --- | ---: | ---: |
| `org.apache.spark.deploy.master.Master` | 2 | 624 |
| `org.apache.spark.deploy.worker.DriverRunner` | 2 | 162 |
| `org.apache.spark.deploy.rest.StandaloneRestServer` | 2 | 26 |
| `org.apache.spark.deploy.worker.ProcessBuilderLike` | 2 | 6 |
| `org.apache.spark.SecurityManager` | 2 | 252 |
| `org.apache.spark.storage.DiskStore` | 2 | 114 |

Two type declarations per type is the duplicate-definition property this graph's input
set carries throughout, and each query groups method nodes by full name and unions
their call sites rather than picking one node — which is what the envelopes publish as
`duplicate_class_definitions_unioned`. The `SecurityManager` row is the same 252
method nodes under 107 distinct names that the five predicate selectors were narrowed
out of, in the three steps recorded above.

### Why a source-level `createDriver` selector would have found nothing

**A graph search for the source-level name `createDriver` returns nothing, and that is
not an absence.** The plan's deploy surface names a `createDriver` call in
`Master.scala` at the pin's lines 417 and 1130 against a definition at 1356; both call
lines are real Scala calls at the pin, and 1356 is the private definition they reach.
What this graph holds is the
name-mangled form Scala 2.13 compiles a private method into, measured on 2026-09-03
and published in `harness/artifacts/logs/cpg-verify.log`, PART 3.3:

| Measured | Value |
| --- | --- |
| method name in the graph | `org$apache$spark$deploy$master$Master$$createDriver` |
| full name | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` |
| call sites keyed on the mangled name | **0** |

This graph is built from bytecode by `jimple2cpg`, and the intra-class calls to a
private mangled method are not reachable here as CALL nodes keyed on that name. Three
consequences, each of them the reason a figure elsewhere in this report reads as it
does. It is why the queries key on **what the graph contains** — the RPC boundary
`B-rpc-LaunchDriver` at `Master.scala:1367`, `RpcEndpointRef.send` of
`DeployMessages$LaunchDriver` from the Master to the Worker — rather than on a
source-level method name. It is why `createDriver` appears in this report as a
**body witness** for pair one's handler, where what is asked of it is whether the
handler body reaches it at all, rather than as a hop on an enumerated route. And it is
the measured reason every query reports `distinct_routes` 0 with
`expected_spurious_route_absent` true: a route the graph cannot express through that
call is a route the traversal could not have found, and reporting one would have
required manufacturing it.

**This is a property of the frontend and of Scala's name mangling. It is not a query
defect and it is not a finding about Spark** — nothing here says anything about whether
Spark's own code is correct, safe or otherwise, and AAP §0.3.2 forbids judging any
finding. The connect-shims collision is resolved the same way, by querying the graph
rather than inferring a winner, and PART 3.3 records that too: `SparkConf`,
`SparkContext` and `RDD` are present with 298, 1,100 and 1,022 methods, so the
definition that survived the frontend's replacement carries full bodies. Per-class
provenance for an overwritten class remains not measurable from this frontend's output,
which is stated wherever an overwrite count is cited.

## The boundaries, and why the bounds are load-bearing

A bound is only meaningful if the traversal could in principle have run away, and a zero
is only meaningful if the place it stopped is named. Both queries 01 and 02 address pair
one and each identifies **four** boundaries on it; query 03 identifies **four** on its
pair one and **five** on its pair two, from a set of **six** distinct boundaries, the
extra one being pair two's own first step.

**Pair one's four boundaries**, with the verdict each formulation measured. Query 01 asks
whether a **call edge** joins the two ends; query 02 asks whether a **data flow** does.
Both verdicts are kept, and neither is merged into the other:

| Boundary | The hop | Crossed by a call edge (01) | Crossed by a data flow (02) |
| --- | --- | --- | --- |
| B1 rpc | `Master.scala:1367` sends `LaunchDriver` over an `RpcEndpointRef`; `Worker.scala:523` / `:687` receives it. A message send is not a call | no | no, 0 flows found |
| B2 thread | `DriverRunner.scala:123` calls `Thread.start()`; the route continues in the `run()` body at `:90` on another thread. `start()` to `run()` is a JVM scheduling relation | no | no, 0 flows found |
| B3 interface | the launch call site invokes the abstract `ProcessBuilderLike.start` declared at `DriverRunner.scala:270`; the JDK launch is reached only through the anonymous implementation at `:276` | **yes** | **yes**, 2 flows found |
| B4 partial function | the handler at `Master.scala:409` returns a `PartialFunction`, so its case bodies compile into a synthetic class and the graph's entry point is the synthetic `applyOrElse` rather than any method named `receiveAndReply` | no | no, 0 flows found |

B3 is the case that shows why agreement is not identity: both formulations report
it crossed, and they report it crossed by **two different kinds of edge**. **The two
columns above have two different owners, and placing them side by side is this
file's aggregation rather than either envelope's.** Envelope 01 owns the call-edge
column; envelope 02's `B3-interface` record owns the data-flow column and carries
only its own measurement — `crossed_by_a_data_flow = true`, `flows_found = 2`, with
its own from-end and to-end — and it does **not** restate the sibling's call-edge
verdict. Envelope 02 is explicit that it does not transcribe a sibling's finding:
where it does compare formulations, it publishes
`verdict_computed_not_transcribed = true`. So the two verdicts are kept as two
measurements of one hop under two different questions, and neither is merged into a
single word.

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

Reporting them separately is what keeps the flow counts interpretable: neither choice is
hidden inside one number.

**Pair two's five boundaries**, and the one this probe **models** rather than reporting as
not-connectable. Pair two crosses a message-send boundary at its **first** step: the
servlet's handler does not call the Master, it *sends* by `askSync` at
`StandaloneRestServer.scala:276-277`, and that is the very message pair one's handler
receives at `Master.scala:410`. A call graph does not join a send to its receiving
handler, so query 03 **models the hop explicitly by pairing on the message type**
`org.apache.spark.deploy.DeployMessages$RequestSubmitDriver` — the constructor's call
sites are the producer end, its declared accessor's call sites are the consumer end, and
the message type's own generated machinery is excluded by owning type. What the model
buys is measured: pair two's producer end is its declared entry point and its consumer
end is pair one's entry point, which is why pair two is reported as crossing **one
boundary more** than pair one rather than as a route that cannot be expressed at all.
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
  `B-thread` and `B-interface` are cited by both pairs and measured once, each record
  carrying `one_measurement_cited_once_per_citing_pair = true`; pair one therefore counts
  4 boundaries and pair two 5, from a set of 6 distinct ones, with no hop measured twice.
- **The partial-function boundary answers differently for the two handlers, and that
  difference is itself the capability observation.** The parameterized selector takes the
  union of a synthetic arm and a source-level arm and then *measures* which one carries
  the pair's declared body witness. For `pair-one`: synthetic types matched 1, body
  witness in the synthetic arm true, in the source-level arm false. For `pair-two`:
  synthetic types matched 0, and the body witness is carried by the source-level method.
  A selector that took only one arm would have silently missed one of the two pairs.
- **`crossed_by_a_call_edge` on `B-partial-function-pair-two` must be read with the
  record's `hop_arises_for_this_handler` flag, which is `false`** (its companion
  `synthetic_class_exists_for_this_handler` is `false` too). For a handler with no
  synthetic class the hop does not arise at all; the flag is therefore not evidence that
  the same hop was crossed for pair two that was uncrossed for pair one. Envelope 03
  keeps those two cases distinct for exactly that reason.

Boundaries **not** crossed, as each envelope publishes them:

| Query / pair | Not crossed |
| --- | --- |
| 01 (call edges) | `B1-rpc`, `B2-thread`, `B4-partial-function` |
| 02 (data flows) | `B1-rpc`, `B2-thread`, `B4-partial-function` |
| 03 `pair-one` (call edges) | `B-rpc-LaunchDriver`, `B-thread`, `B-partial-function-pair-one` |
| 03 `pair-two` (call edges) | `B-rpc-RequestSubmitDriver`, `B-rpc-LaunchDriver`, `B-thread` |

---

## Query 01 — `01-callgraph-unguarded-driver-launch`

Result files:
`queries/joern/results/01-callgraph-unguarded-driver-launch.json` and
`queries/joern/results/01-callgraph-unguarded-driver-launch.md`.
Source: `queries/joern/01-callgraph-unguarded-driver-launch.sc`
(sha256 `79583377ffdc05762226f1437be94d953bf44be1ea94bbc3d9e48f072a27f4ac`, 307,625 B,
digested at run time by the running script).
Console: `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`.

**Formulation.** Bounded call-graph reachability over CALL edges, from the standalone
Master's driver-submission handler to the privileged process launch hosted on the
`DriverRunner` surface. It asserts nothing about data flow.

| Field | Value |
| --- | --- |
| Compile status | compiled |
| Run status | completed |
| Returned record count | 4 — 4 boundary records, 0 route records |
| Distinct routes | 0 |
| Route identity | (entry point method full name, sink host method full name, the ordered sequence of (from method, call site callee, to method) hops); both walks deduplicated on it, never summed. **Within** a walk the retained chain is the BFS parent map's shortest witness, so a repeat arrival at an already-witnessed sink host is counted rather than retained — the last column of the walk table below — and the envelope publishes `route_cap_reachable_maximum` 4 with `route_cap_can_bind` false, so the cap's `no` is not read as a complete enumeration |
| Spurious count | 0 |
| Bound value | 12 — `MAX_CALL_DEPTH`, the maximum call-graph hops walked from an entry point |
| Bound reached | **yes** |
| Entry points discovered / traversed / truncated | 2 / 2 / 0 |
| Duplicate formulation (aggregate) | partial_duplicate |
| Graph identity re-verified before the load | 547,980,224 / `325887cf…`, `PASS`, the check itself taking 3,947 ms |
| Total elapsed | 577,899 ms, of which 570,738 ms was the load |

**Entry points**, both traversed:
`org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse` and
`org.apache.spark.deploy.master.Master.receiveAndReply`; the synthetic type sweep matched
2 type declarations against the declared regex. **Sink hosts**:
`org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry` and
`org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start`, at graph lines `#240`
and `#276` respectively; the sink sweep scanned 1,233 calls named `start` without
truncating, finding 52 call sites on any host and 2 on the sink host.

**All 9 bounds, with whether each was reached** — the depth bound was reached,
so the traversal was genuinely bounded rather than nominally so. This is the envelope's
complete `bounds` key set; each row's flag is its `bounds_reached` entry and each has a
`bounds_reached_basis` entry naming the counter or the sweeps the flag was derived from:

| Bound | Value | Reached |
| --- | --- | --- |
| `MAX_CALL_DEPTH` | 12 | **yes** |
| `MAX_ROUTES` | 64 | no |
| `MAX_EXPANSIONS_PER_ENTRY` | 200000 | no |
| `MAX_EXPANSIONS_PER_WALK` | 3200000 | no |
| `MAX_TOTAL_RETURNS` | 256 | no |
| `MAX_ENTRY_POINTS` | 16 | no |
| `MAX_CALL_SCAN` | 200000 | no |
| `MAX_TYPE_SCAN` | 100000 | no |
| `FANOUT_CALLEE_THRESHOLD` | 32 | **yes** — a threshold rather than a cap: a call site whose resolved callee set is wider is recorded as a dynamic-dispatch fan-out site |

**Two walks, reported separately.** The difference between them is whether
dynamic-dispatch fan-out is followed, and neither walk's returns are added to the
other's:

| Walk | Follows fan-out | Method expansions | Methods visited | Call sites considered | Fan-out sites (not followed) | Depth used | Routes returned | Alternate sink arrivals not retained |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `A-follows-fan-out` | yes | 19,551 | 20,125 | 21,476 | 48 (0) | 12 | 0 | 0 |
| `B-fan-out-recorded` | no | 2,350 | 2,511 | 4,383 | 31 (31) | 12 | 0 | 0 |

Every counter in that table is this graph's. The 2026-09-01 generation walked the
graph the re-provisioning replaced and its own envelope published
25,009 / 27,956 / 33,565 with 86 fan-out sites for walk A and 5,598 / 7,092 / 11,575
with 55 for walk B; the two sets of counters describe two different graphs and are not
reconciled with each other. What did not move is what the walks concluded: routes
returned 0 on both walks, the depth bound reached on both, and no cap or budget
reached on either.

**The four reporting requirements, for this query:**

1. **The predicate set and the source types it came from** — the five selectors above,
   anchored on `org.apache.spark.SecurityManager`, from
   `core/src/main/scala/org/apache/spark/SecurityManager.scala` at `:227`, `:234`,
   `:248`, `:264` and `:274`, with `_$eq` setters excluded.
2. **Whether the bound was reached** — yes: `MAX_CALL_DEPTH` = 12 was reached, and so was
   the `FANOUT_CALLEE_THRESHOLD` of 32. No route cap, expansion budget, total-returns
   cap, entry-point cap or call-scan cap was reached.
3. **Whether the formulation duplicates another query's** — partial_duplicate as an
   aggregate: `not_duplicate` against query 02, and
   `duplicate_formulation_on_pair-one` against query 03. See the matrix below.
4. **Whether an expected-spurious route was absent** — yes, on a **structural** basis: no
   call site of any of the five predicates exists on this query's route surface at all,
   so no route of this pair could have passed one.

**What the zero means, as the envelope states it.** The pair is not call-graph-connected
across those hops, so a bounded reachability walk over CALL edges returns none. The bound
was not loosened or removed, the query was not widened, and no route was manufactured.

## Query 02 — `02-dataflow-unguarded-driver-launch`

Result files:
`queries/joern/results/02-dataflow-unguarded-driver-launch.json` and
`queries/joern/results/02-dataflow-unguarded-driver-launch.md`.
Source: `queries/joern/02-dataflow-unguarded-driver-launch.sc`.
Console: `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log`,
the stream of the invocation that produced the figures below — published as a member
of the same publication as the envelope and the prose result
(`publication_id 7a07b7184af2306b…`, with `member_set_id`
`d9315f57751cad5002c21986426c3c21` in that publication's completion manifest). Every figure in this
section is cited from that query's envelope.

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
| Graph identity re-verified before the load | 547,980,224 / `325887cf…`, `PASS`, the check itself taking 4,112 ms |
| Total elapsed | 652,495 ms, of which 547,825 ms was the load |

**How "bound reached" is established here, and the limitation stated with it.** The
engine's internal call-depth bound is **not observable from its output** — it reports no
truncation flag — so the query does not claim to have observed it, and publishes
`not observable from the engine's output` for the three depth bounds rather than a
reached/not-reached value it cannot honestly hold. Of the **fourteen** named bounds —
the envelope's complete `bounds` key set — **eleven are published as not reached** and
**three as not observable**:

| Bound | Value | Published flag | Basis as published |
| --- | --- | --- | --- |
| `MAX_FLOW_CALL_DEPTH` | 6 | not observable | the engine exposes no truncation flag for its internal call-depth bound |
| `MAX_FLOW_CALL_DEPTH_SHALLOW` | 2 | not observable | the same limitation, for the same reason |
| `MAX_BOUNDARY_FLOW_CALL_DEPTH` | 2 | not observable | the same limitation; the boundary probes ask about one hop each |
| `MAX_FLOW_LENGTH` | 64 | not reached | 0 of 4 published flow records exceeded 64 elements |
| `MAX_FLOWS_PER_PAIR` | 8 | not reached | 0 flows retained of 0 found in each route-bearing arm |
| `MAX_ENGINE_FLOWS_PER_EVALUATION` | 64 | not reached | the paths materialized from each `reachableByFlows` return, taken as cap + 1 before the iterator becomes a list — one sweep per engine evaluation, every one observed well under the cap and none truncated |
| `MAX_STEPS_PER_SOURCE` | 8 | not reached | 4, 4 and 2 evaluations across the three arms |
| `MAX_TOTAL_RETURNS` | 256 | not reached | 8 records of 256 |
| `MAX_SOURCE_NODES` | 64 | not reached | 3, 3 and 1 source nodes taken, 0 truncated |
| `MAX_SINK_NODES` | 64 | not reached | 4 sink nodes taken per arm, 0 truncated |
| `MAX_ENTRY_POINTS` | 16 | not reached | 2 discovered, 2 traversed, 0 truncated |
| `MAX_CALL_SCAN` | 200,000 | not reached | sixteen sweeps, the widest being 1,978 calls inside the entry methods and 1,968 in the synthetic handler bodies, and the sink sweep 1,233 calls named `start`, every one reported `truncated=false` |
| `MAX_TYPE_SCAN` | 100,000 | not reached | eighteen sweeps, the widest being 252 methods on the predicate type and 217 type declarations under the `Master` prefix, every one reported `truncated=false` |
| `MAX_CODE_CHARS` | 160 | not reached | measured by looking for the truncation marker on every code string published, over 35 flow elements |

Depth is then addressed by running one arm at two depths and comparing:

| Depth-sensitivity check | Value |
| --- | --- |
| Shallow depth | 2 |
| Primary depth | 6 |
| Flows retained, shallow | 0 |
| Flows retained, primary | 0 |
| Results differ across the two depths | no |

Equal results across the two depths is evidence that the result does not depend on the
call-depth bound across that range; a difference would have been evidence that it does.
The limitation is stated rather than papered over.

**Three route-bearing arms, plus one control arm, each reported separately:**

| Arm | Depth | Source groups (traversed) | Source nodes | Sink groups | Sink nodes | Evaluations | Flows found / retained |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ARM1-handler-parameters-shallow` | 2 | 2 (2) | 3 | 2 | 4 | 4 | 0 / 0 |
| `ARM1-handler-parameters` | 6 | 2 (2) | 3 | 2 | 4 | 4 | 0 / 0 |
| `ARM2-unapply-recovered-payload` | 6 | 1 (1) | 1 | 2 | 4 | 2 | 0 / 0 |
| `CONTROL-intraprocedural-liveness` | 2 | 1 (1) | 3 | 1 | 2 | 1 | **2 / 2** |

**The control arm is why this query's zeros are attributable.** A zero from a
cross-boundary arm means either that the route is not connected by data *or* that the
engine had no reaching-definition edges to walk, and the zero alone cannot tell those
apart. The control asks for a flow that must exist if the layer is live — from the
launch's own enclosing method's formal parameters, `this` excluded, to the launch call in
that same method, intraprocedural by construction — and it found 2, of 7 and 17 elements.
So `dataflow_layer_live_on_this_sink` is **true**, measured. The control's flows are
**not counted as routes** (`counted_as_routes = false`).

**The two boundary-flow records are the B3 measurement**, of 5 and 6 elements, neither
truncated, neither passing an auth/ACL predicate and neither marked spurious — which is
how this query reports the interface hop crossed by a data flow while its route count
stays 0.

**Sink node composition**, published so the sink set is not mistaken for a single node: 2
launch-call nodes, 2 receiver nodes and 2 argument nodes give 4 distinct sink nodes used,
0 truncated. A flow that reaches the value being launched can end at the launch call, its
receiver or one of its arguments; taking only the call node would miss a flow into the
receiver.

**The four reporting requirements, for this query:**

1. **The predicate set and the source types it came from** — the same five
   selectors, from the same file at the same five lines, in a block the envelopes
   record as byte-identical across the three sources. For a flow, the predicate
   test asks whether the flow passes one of those five before reaching the sink.
2. **Whether the bound was reached** — **the observable-cap conjunction was not
   reached**. The headline bound `MAX_FLOW_CALL_DEPTH` = 6 is a different answer and
   is not folded into that one: its reached state is ***not observable from the
   engine's output***, because the engine reports no depth it actually used, so
   nothing here reads it as a "no". What stands in its place is separate evidence
   rather than a substitute: the same pair was evaluated at depth 2 and at depth 6 and
   the results did not differ, which is evidence the outcome does not depend on the
   bound across that range — not evidence the bound went unreached. All **14**
   named bounds now carry a value: **11** published as *not reached*, and **3** —
   the engine's three call-depth overlays `MAX_FLOW_CALL_DEPTH`,
   `MAX_FLOW_CALL_DEPTH_SHALLOW` and `MAX_BOUNDARY_FLOW_CALL_DEPTH` — as *not
   observable from the engine's output*, because the flow engine reports no depth
   actually used. Each flag is the disjunction over every bounded sweep its cap
   governs, and each `bounds_reached_basis` entry names those sweeps with their own
   observed counts; the query publishes all **48** sweeps, none truncated. The
   fourteenth is `MAX_ENGINE_FLOWS_PER_EVALUATION` = 64, which bounds the paths
   materialized from a single `reachableByFlows` return — taken as `cap + 1` before
   the returned iterator becomes a list — and contributes 14 of those 48 sweeps, one
   per engine evaluation, each observed well under the cap and none truncated.
3. **Whether the formulation duplicates another query's** — **not_duplicate**
   against both. Against query 01 the grounds are the edge kinds
   (reaching-definition against CALL), the node granularity (parameter and
   expression nodes against whole methods), the engine and bound semantics, and a
   construct-set difference in both directions: **19** constructs only here, **4**
   only there, **24** shared. The envelope states explicitly that the verdict is drawn from
   properties of the two committed **sources** — checkable without a graph load —
   and **not** from both queries returning zero.
4. **Whether an expected-spurious route was absent** — yes, on a **structural**
   basis: no call site of any of the five exists on the route surface.

## Query 03 — `03-parameterized-handler-sink-pairs`

Result files:
`queries/joern/results/03-parameterized-handler-sink-pairs.json` and
`queries/joern/results/03-parameterized-handler-sink-pairs.md`.
Source: `queries/joern/03-parameterized-handler-sink-pairs.sc`
(sha256 `8f67126c56185bde3221ad760130295cf9f7f64411be528e9fd578a4fbad631e`, 428,057 B).
Console: `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`.

**Formulation.** Bounded call-graph reachability over CALL edges, **parameterized** over
handler/sink pairs and instantiated on two named pairs **in one run**: the standalone
Master's handler, and the REST submit servlet's `handleSubmit`, both to the same
`DriverRunner` launch.

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
| Bound reached | `pair-one` **yes**, `pair-two` **yes** (`bound_reached_any` is a disjunction, never an arithmetic total) |
| Entry points discovered / traversed / truncated | `pair-one` 2 / 2 / 0; `pair-two` 1 / 1 / 0 |
| Boundaries cited | `pair-one` 4, `pair-two` 5, of 6 distinct |
| Duplicate formulation (aggregate) | partial_duplicate |
| Parameterizability | **passed** — this query owns the measure |
| Graph identity re-verified before the load | 547,980,224 / `325887cf…`, `PASS`, the check itself taking 3,995 ms |
| Total elapsed | 567,063 ms, of which 560,129 ms was the load |

**Per-pair detail.** `pair-one`'s handler is
`org.apache.spark.deploy.master.Master.receiveAndReply` (`Master.scala:409` at the pin),
resolved through a synthetic-type regex on `Master$$anonfun$receiveAndReply$N` with
`applyOrElse` and the body witness `createDriver`; its synthetic type sweep matched 1.
`pair-two`'s handler is
`org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet.handleSubmit`
(`StandaloneRestServer.scala:268` at the pin) with the body witness
`DeployMessages$RequestSubmitDriver.<init>`; its synthetic type sweep matched 0, so the
source-level arm is the one that carries its body. Both pairs share the sink at
`DriverRunner.scala:240`, reached through a callee regex on
`java.lang.ProcessBuilder.start` and
`org.apache.spark.deploy.worker.ProcessBuilderLike.start`; the sink scan
considered 1,233 calls named `start` without truncating, finding 52 call sites on
any host and 2 on the sink host — the same sweep, with the same three figures, that
queries 01 and 02 report over this graph.

**9 bounds, per pair.** `MAX_CALL_DEPTH` (12) and `FANOUT_CALLEE_THRESHOLD` (32) were reached on both pairs; `MAX_ROUTES_PER_PAIR` (64), `MAX_EXPANSIONS_PER_ENTRY` (200000), `MAX_STEPS_PER_PAIR` (400000), `MAX_TOTAL_RETURNS` (256), `MAX_ENTRY_POINTS_PER_PAIR` (16), `MAX_CALL_SCAN` (200000) and `MAX_TYPE_SCAN` (200000) were reached on neither. This is the envelope's complete
`bounds` key set, and every entry appears in `bounds_reached_by_pair` and
`bounds_reached_basis_by_pair` for **both** pairs, each basis naming the sweeps or the
counter its flag was derived from. The route cap is **per pair rather than shared**,
because one pair filling a shared budget would silently truncate the other.

**Four walks — two per pair — reported separately and never combined:**

| Pair | Walk | Follows fan-out | Method expansions | Methods visited | Call sites considered | Fan-out sites (not followed) | Depth used | Routes returned | Alternate sink arrivals not retained |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `pair-one` | `A-follows-fan-out` | yes | 19,551 | 20,125 | 21,476 | 48 (0) | 12 | 0 | 0 |
| `pair-one` | `B-fan-out-recorded` | no | 2,350 | 2,511 | 4,383 | 31 (31) | 12 | 0 | 0 |
| `pair-two` | `A-follows-fan-out` | yes | 10,029 | 10,106 | 8,949 | 25 (0) | 12 | 0 | 0 |
| `pair-two` | `B-fan-out-recorded` | no | 732 | 766 | 1,230 | 12 (12) | 12 | 0 | 0 |

`pair-one`'s two rows are the same two walks query 01 runs, over the same graph, and
they agree with query 01's counters figure for figure. That agreement is a
**consequence** of the scoped duplication between the two queries and is not evidence
for it: the duplication verdict is drawn from the two committed sources, as the
duplicate-formulation section states, and no verdict here is derived from what either
query returned.
The 2026-09-01 generation published 25,009 / 27,956 / 33,565 and 5,598 / 7,092 / 11,575
for `pair-one` and 10,146 / 10,329 / 9,038 and 764 / 855 / 1,247 for `pair-two` over
the graph the re-provisioning replaced; those counters belong to that graph.

**The four reporting requirements, for this query** — answered per pair where the
measurement is per pair:

1. **The predicate set and the source types it came from** — the same five selectors on
   `org.apache.spark.SecurityManager` from the same file and the same five lines,
   **identical across both pairs**: the parameterization varies the handler and the sink
   and never the predicate set. Predicate call sites on each pair's own route surface:
   `pair-one` 0, `pair-two` 0; graph-wide 18 in 18 distinct callers.
2. **Whether the bound was reached** — yes on both pairs: `MAX_CALL_DEPTH` = 12 and the
   fan-out threshold were reached for each, and no cap was.
3. **Whether the formulation duplicates another query's** — **partial_duplicate**:
   `duplicate_formulation_on_pair-one` against query 01, whose scope is stated as
   pair-one only, and `not_duplicate` against query 02. Instantiated on pair one
   this query *is* query 01's formulation restated in parameterized form, and the
   evidence is measured: the same edge kind, the same entry-point resolution, the
   same sink constraint, the same bound value 12, and API construct sets that are
   **identical in both directions**. As wholes the two are not duplicates, because
   this query answers for a second pair query 01 cannot express.
4. **Whether an expected-spurious route was absent** — yes on both pairs, on a
   **structural** basis for each: no call site of any of the five exists on `pair-one`'s
   route surface (`Master`, `Worker`, `DriverRunner`, `ProcessBuilderLike`) or on
   `pair-two`'s (`StandaloneSubmitRequestServlet`, `Master`, `Worker`, `DriverRunner`,
   `ProcessBuilderLike`).

---

## Duplicate formulation, as a symmetric matrix

The relation is **pairwise and symmetric**, and each envelope says how rather than
asserting it. `duplicate_formulation_relation` states that it is symmetric **by
construction rather than by transcription**: every entry is computed by applying one
shared predicate to the two queries' own declared formulation-identity blocks, read
out of the two **source** files at run time under names all three queries share, so
both directions evaluate identical inputs through identical code and a disagreement
between them is not expressible. Each entry additionally publishes
`verdict_symmetry_basis`, `verdict_computed_not_transcribed = true`, and the sibling
source's path, byte size and sha256 it read. **The symmetry is verified here by
comparing the reciprocal entries** — `duplicate_formulation_detail[].against` and
`.status` in each of the three envelopes, which agree in all three directions — and
not from any single field asserting agreement. The matrix below is therefore one
relation read three ways, not three opinions:

| | 01 | 02 | 03 |
| --- | --- | --- | --- |
| **01** | — | not_duplicate | duplicate_formulation_on_pair-one |
| **02** | not_duplicate | — | not_duplicate |
| **03** | duplicate_formulation_on_pair-one | not_duplicate | — |

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
  any input. Construct-set difference in both directions: **19** only in 02, **4** only in
  01, **24** shared. Neither is expressible as the other. Their four boundary verdicts
  **agree**, and the envelopes say plainly that agreement on a verdict is not
  identity of formulation — B3-interface being the case that shows it, crossed by
  a call edge and crossed by a data flow, kept as two measurements.
- **01 against 03, and 03 against 01 — duplicate_formulation_on_pair-one.** Said
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
  end-node kinds, and no flow engine on 03's side at all; the bound values (6 and 12) are
  published as **not the same kind of quantity**. Construct-set difference in both
  directions: 19 only in 02, 4 only in 03, 24 shared. Sharing pair one is not sufficient
  for duplication, and the pair **set** differs too.

**No returns are summed anywhere in that comparison**, and no sibling's numbers are
transcribed into another's record: envelope 03 publishes
`provenance.sibling_figures = NONE is transcribed`, and what each query reads of another
is its **source text**, never its results.

## The three effort measures

The three are answered **individually** below, not as a group.

### 1. Query revisions committed

**One convention, one owner, and the owner is each query itself.** A query's
revision count is the number of commits touching that query's own `.sc` file **in
the history of the HEAD the measurement was taken at**, newest first. Three
properties make the number checkable rather than merely stated:

- The range names **`HEAD` explicitly** rather than relying on an implicit default,
  so what the count is relative to is part of the measurement.
- **Every commit returned is asserted to be an ancestor of that HEAD**, and the
  measurement is rejected outright if any is not. A count therefore cannot include a
  commit this branch does not contain.
- The HEAD, the branch and that ancestry verdict are published **beside** the count,
  as `effort_query_revisions_measured_at_head`, `_measured_on_branch` and
  `_ancestry_verified`, so a later reader reconciles against a stated window instead
  of guessing which one produced the number.

All three counts below were taken at HEAD **`d933940aa3bb6e3a81fb464114dad922cae76346`**
on branch `blitzy-f38258d3-f87d-44f5-bedc-af512c69e0ab`, with ancestry verified for every
commit counted.

| Query | `.sc` file | Revisions committed | The commits counted |
| --- | --- | --- | --- |
| 01 | `queries/joern/01-callgraph-unguarded-driver-launch.sc` | 3 | `0e3e742a5ad`, `232d0d9cca3`, `1ac5915ed15` |
| 02 | `queries/joern/02-dataflow-unguarded-driver-launch.sc` | 3 | `0e3e742a5ad`, `232d0d9cca3`, `675f691eca9` |
| 03 | `queries/joern/03-parameterized-handler-sink-pairs.sc` | 4 | `0e3e742a5ad`, `232d0d9cca3`, `1072fd2334f`, `20a56482274` |

Each count is relative to the HEAD it was taken at, and that is what makes it
reproducible rather than a running tally: the range is that HEAD's own ancestry, every
commit in it is asserted to be an ancestor, and the HEAD, the branch and the ancestry
verdict are published beside the count. One window is disclosed rather than smoothed
over. The measurement was taken while the commit that publishes this generation was
itself the branch tip, so that commit **is** counted here — and folding this
publication into a single commit gives it a new identifier. The counts and the ancestry
are unaffected, and `git log` over any of these three paths returns exactly the number
in the table; what the recorded
`effort_query_revisions_measured_at_head` names is the tip as it stood when the
measurement was taken rather than the identifier a later reader holds.

The three figures are **per query and are never added**: no probe-wide revision
total is published here or in any envelope, because a sum across three files answers
a question no query asks.

**What this replaces, recorded because the numbers changed.** Earlier revisions of
this report published **1, 1, 2** under a different convention — commits "from the
file's first appearance to the end of the probe" — which required a reader to subtract
later commits by hand and produced counts that no longer matched what the envelopes
published. Under the convention above, later revisions published **3, 3, 4**, then
**4, 5, 5**, and a measurement taken while this generation's sources were being
finished published **14, 15, 15**. Each was correct at the HEAD it named, and none of
those HEADs is in this branch's history now — which is precisely why the HEAD is
published beside the count. The three counts are unchanged from the 2026-09-01
generation's **3, 3, 4**, because the sources did not change; what moved is the window,
from HEAD `d3bc40ae290` to HEAD `d933940aa3b`, and with it the newest commit each
count names — `d3bc40ae290` there, `0e3e742a5ad` here.
`harness/artifacts/logs/probe-query-revisions.json` is a
rendering of the measurement taken at the earlier of those two HEADs; it states plainly
that nothing reads it and that the envelope governs any disagreement, so the table
above is the three envelopes' 2026-09-03 measurement and that file is neither its owner
nor a second opinion on it. Every figure in the table is one
measurement cited, not a second one taken.

### 2. Distinct Joern API constructs used

**The list is the measure and the count is computed from it**, so each number is
auditable from its list rather than asserted. Each per-query list is deduplicated,
and every query **audits its own list against its own source text** with the list's
own declaration excised first, so no entry can satisfy itself by appearing in the
list; each envelope publishes `declared_entries`, `distinct_entries` and
`confirmed_in_the_source` together with the limitation that a member-name search
establishes that the source names the member rather than that every occurrence
invokes that type's member.

| Query | Constructs |
| --- | --- |
| 01 | 28 |
| 02 | 43 |
| 03 | 28 |

**Query 01 — 28 constructs**, as its envelope publishes them:

`Call.code`, `Call.dispatchType`, `Call.lineNumber`, `Call.method`, `Call.methodFullName`, `Call.name`, `Call.order`, `Method.callIn`, `Method.callOut`, `Method.fullName`, `Method.lineNumber`, `Method.name`, `Method.typeDecl`, `NoResolve.getCalledMethodsAsTraversal`, `Steps.fullName`, `Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`, `TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`, `cpg.typeDecl`, `importCpg`, `switchWorkspace`.

**Query 02 — 43 constructs**, as its envelope publishes them:

`AstNode.code`, `AstNode.label`, `AstNode.lineNumber`, `Call.argument`, `Call.dispatchType`, `Call.lineNumber`, `Call.method`, `Call.methodFullName`, `Call.name`, `Call.receiver`, `CfgNode.method`, `EngineConfig.copy`, `EngineConfig.maxCallDepth`, `EngineContext.config`, `EngineContext.copy`, `EngineContext.semantics`, `Method.call`, `Method.callIn`, `Method.fullName`, `Method.lineNumber`, `Method.name`, `Method.parameter`, `Method.typeDecl`, `MethodParameterIn.index`, `MethodParameterIn.method`, `MethodParameterIn.name`, `MethodParameterIn.typeFullName`, `Path.elements`, `Steps.fullName`, `Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`, `Traversal.reachableByFlows`, `TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`, `cpg.typeDecl`, `importCpg`, `switchWorkspace`.

**Query 03 — 28 constructs**, as its envelope publishes them:

`Call.code`, `Call.dispatchType`, `Call.lineNumber`, `Call.method`, `Call.methodFullName`, `Call.name`, `Call.order`, `Method.callIn`, `Method.callOut`, `Method.fullName`, `Method.lineNumber`, `Method.name`, `Method.typeDecl`, `NoResolve.getCalledMethodsAsTraversal`, `Steps.fullName`, `Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`, `TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`, `cpg.typeDecl`, `importCpg`, `switchWorkspace`.

Queries 01 and 03 declare the **same set**, which is what makes their
`api_construct_set_difference_both_directions_empty` verdict true and is one of the
grounds for the scoped duplication between them.

**The probe-wide union: 47 distinct constructs.** This is the one quantity
this document computes rather than cites, and it is **this file's own aggregation**:
no per-query result delegates it here or claims the union is owned here. It is a set
union over the three published lists — an aggregation, not a new measurement — and it
decomposes exactly, so the arithmetic can be checked against the three lists above:

| Partition | Count | Members |
| --- | --- | --- |
| Shared by all three queries | 24 | `Call.dispatchType`, `Call.lineNumber`, `Call.method`, `Call.methodFullName`, `Call.name`, `Method.callIn`, `Method.fullName`, `Method.lineNumber`, `Method.name`, `Method.typeDecl`, `Steps.fullName`, `Steps.fullNameExact`, `Steps.l`, `Steps.nameExact`, `Steps.size`, `Steps.take`, `TypeDecl.fullName`, `TypeDecl.method`, `cpg.call`, `cpg.file`, `cpg.method`, `cpg.typeDecl`, `importCpg`, `switchWorkspace` |
| Used only by query 02 | 19 | `AstNode.code`, `AstNode.label`, `AstNode.lineNumber`, `Call.argument`, `Call.receiver`, `CfgNode.method`, `EngineConfig.copy`, `EngineConfig.maxCallDepth`, `EngineContext.config`, `EngineContext.copy`, `EngineContext.semantics`, `Method.call`, `Method.parameter`, `MethodParameterIn.index`, `MethodParameterIn.method`, `MethodParameterIn.name`, `MethodParameterIn.typeFullName`, `Path.elements`, `Traversal.reachableByFlows` |
| Used by queries 01 and 03 but not 02 | 4 | `Call.code`, `Call.order`, `Method.callOut`, `NoResolve.getCalledMethodsAsTraversal` |
| **Union** | **47** | 24 + 19 + 4 |

### 3. Parameterizability

**Verdict: passed — and the pass is attributed solely to query 03's captured
invocation.** Queries 01 and 02 neither claim the measure nor could satisfy it: each is a
single-pair formulation that hard-codes one handler and one sink and takes no pair
parameter, and both envelopes say so explicitly rather than leaving it blank, naming
query 03 as the owner.

**The pass condition, as query 03's envelope states it:** the measure passes **only**
where the parameterized query is actually invoked on the second named pair *and* that
invocation's result is captured in that query's result files and console log. An empty
result from a real invocation satisfies it; a skipped invocation does not; **a parameter
list that merely exists does not**.

**It was invoked.** Both pairs were declared and both were invoked, in one run, in the
declared order `pair-one` then `pair-two`; the envelope publishes `pairs_declared = 2`,
`pairs_invoked = 2` and `second_pair_invoked = true`, and the console stream's result
region prints
`parameterizability : passed (second pair pair-two invoked: true)`:

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

The second pair is the one the plan names — the `StandaloneRestServer.scala`
`handleSubmit` handler, on the enclosing type `StandaloneSubmitRequestServlet`, to the
`DriverRunner.scala:240` sink in
`core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala` — and **its
result is captured**, in
`queries/joern/results/03-parameterized-handler-sink-pairs.json`,
`queries/joern/results/03-parameterized-handler-sink-pairs.md` and
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`, the three files
the envelope names as `second_pair_result_captured_in` and the three members of that
query's completion manifest. The captured outcome, in the envelope's own words:
*invoked; entry points traversed 1 of 1; walks run A-follows-fan-out and
B-fan-out-recorded; call sites considered A-follows-fan-out=8949,
B-fan-out-recorded=1230; distinct routes 0; spurious 0; boundaries measured or cited 5
(B-rpc-RequestSubmitDriver, B-rpc-LaunchDriver, B-thread, B-interface,
B-partial-function-pair-two)*. The envelope also publishes the
exact parameter literals supplied for each pair — handler type, handler method,
synthetic-type regex, synthetic method, body witness, base type, source file and line,
sink callee regex, sink call name, sink host type regex, sink file and line, message-hop
identifiers and route-surface prefixes — so a reader can see **one query body driven by
two different inputs** rather than two queries written. Pair two's base declaration
`org.apache.spark.deploy.rest.SubmitRequestServlet` is recorded as its declared base
type, and no handler or sink is named in the query body itself.

**A zero on the second pair does not weaken the verdict, and did not affect it.** The
measure asks whether the second named pair was really supplied to the same body and its
result captured; it does not ask whether that pair is connected over this graph by this
formulation. Those two questions are reported separately for exactly that reason: the
verdict here, and the pair's own route and boundary figures in its section above, where a
zero is a capability observation about the traversal rather than a failure of either.

## Values that could not be established

**No value in the probe is now published as not established, and the one that was is
recorded here with what closed it** rather than dropped silently:

- **Query 02's `MAX_CALL_SCAN` reached-flag** was published as `null`. Its flag had
  been derived from a single sweep's truncation variable while the same cap governed
  further sweeps whose flags reached only the console stream, so the envelope could
  not state a value it had not collected. The query now registers **every** bounded
  materialization in one place and derives each cap's flag as the disjunction over
  the sweeps that cap governs, publishing all 48 sweeps with their observed counts
  and truncation flags. The flag is consequently a measured **not reached**, its
  `bounds_reached_basis` entry names all 16 sweeps `MAX_CALL_SCAN` governs with each
  one's own count, and all 14 of that query's bounds now carry a value: 3 as *not
  observable from the engine's output* — the flow engine reports no depth actually
  used — and 11 as *not reached*.

Two further facts belong here because they are limits rather than measurements,
and reading them as measurements would overstate what the probe established:

- **Query 02's engine-internal call-depth bound is not observable from the
  engine's output.** The engine reports no truncation flag for it, so the query
  reports the conjunction of the caps its own evaluator counts and addresses depth
  by the two-depth comparison above, rather than claiming to have observed the
  internal bound.
- **Which generation of the graph a figure belongs to is recorded rather than
  flattened.** All three queries as this report publishes them loaded the same bytes —
  547,980,224 / `325887cf…`, the graph the 2026-09-03T01:17:07Z re-provisioning
  wrote — each verified against the record of account resolved
  by provenance, `provision-log/cpg-identity.txt` corroborated by `cpg-record.txt`.
  This path has held two earlier generations across this run's lanes: 541,255,894 /
  `26d327cc…` with 1,397,339 methods, stated in this tree by
  `harness/ENVIRONMENT.md` §7 and by nothing this run measured; and
  541,309,809 / `4616845a…` with 1,396,899 methods and 119,721 type declarations,
  which the 2026-09-01 probe generation verified and loaded and which the
  re-provisioning replaced. Both are kept with their provenance — in
  `harness/ENVIRONMENT.md` §7's supersession appendix, in `run-record.md` **D4**, and
  under their own date wherever they appear above — rather than restated as though they
  described the current graph. The graph is a
  host-shared read-only file this run neither rebuilds nor replaces, and each of the
  three loads reported here was verified against the record of account for the bytes
  it read before reading them.

## Provenance — every figure to its file

| Figures | Cited from |
| --- | --- |
| Query 01: statuses, records, distinct routes and their identity function, alternate sink arrivals not retained, the route cap's reachable maximum and whether it can bind, spurious count, **all nine bounds** with their reached flags and each flag's basis, the 18 bounded sweeps, entry-point counters, both walks' counters, boundary verdicts, duplicate-formulation detail, the route surface and its per-prefix reach evidence, graph identity and counts, JDK major and heap, revisions with their HEAD and ancestry verdict, 28-construct list | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` and `queries/joern/results/01-callgraph-unguarded-driver-launch.md` |
| Query 02: statuses, records and their four kinds, distinct routes and their identity function, spurious count, **all fourteen bounds** with a value for every one — three *not observable from the engine's output* and eleven *not reached* — each flag's basis, the 48 bounded sweeps, depth-sensitivity figures, three arms and the control arm, sink-node composition, boundary verdicts including its own B3 data-flow verdict, duplicate-formulation detail, the route surface and its reach evidence, graph identity and counts, JDK major and heap, revisions with their HEAD and ancestry verdict, 43-construct list, and `determinism.reproduction_check_status` = *not attempted* | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` and `queries/joern/results/02-dataflow-unguarded-driver-launch.md` |
| Query 03: statuses, pairs declared and invoked, records, per-pair distinct routes and spurious counts, **nine bounds per pair** with per-pair flags and bases, the 34 bounded sweeps, per-pair entry-point counters, four walks' counters, the six boundaries and their per-pair citation, the modelled message-type hop, the enclosing-type precision on pair two — `parameter_values_supplied.pair-two.handler_type` — graph identity and counts, JDK major and heap, revisions with their HEAD and ancestry verdict, 28-construct list, the parameterizability verdict and its captured second-pair invocation | `queries/joern/results/03-parameterized-handler-sink-pairs.json` and `queries/joern/results/03-parameterized-handler-sink-pairs.md` |
| The five predicate selectors, their pinned lines, the three-step narrowing, the deliberate non-selectors, the graph-wide call sites, the per-route-surface zeros | all three envelopes, whose selector blocks each publish `predicate_selector_literals_identical` as a measured boolean against both siblings |
| The probe-wide union of 47 API constructs and its 24 / 19 / 4 decomposition | computed here from the three published lists. **This file's own aggregation** — no per-query result delegates it here or claims to own it |
| The seven-class declaration table for `StandaloneRestServer.scala` | the pinned source file itself at `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, read at lines 56, 81, 99, 116, 138, 155 and 171. **No query-03 owner file records this table**; envelope 03 records only the type it selected on |
| Pinned source line numbers, and the `+11` offset caveat on `Worker.scala` in the working checkout | the pinned tree at `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, and the caveat as published in envelope 01's route surface |
| The byte size and sha256 of each `.sc` source, in the provenance disclosure — published from the envelope like every other figure, and additionally **confirmed** against the branch, which is the one comparison this report performs itself | the three envelopes' `source_integrity` and `provenance` blocks and the three streams' *query source bytes* / *query source sha256* lines, confirmed with `sha256sum` and `stat -c%s` over `queries/joern/01-callgraph-unguarded-driver-launch.sc`, `queries/joern/02-dataflow-unguarded-driver-launch.sc` and `queries/joern/03-parameterized-handler-sink-pairs.sc` |
| The three retained private graph copies' paths, inodes and re-measured identity | the three streams' *private input (created)*, *private input after load* and *private input retained* lines, and each envelope's `graph.private_copy_retained_after_verification` |
| Each query's pre-load identity check as a standalone capture — the pair, the check's elapsed time, the verdict, the record of account, the clone it ran in, and the `supersedes` line naming the 2026-09-01 figures | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.identity.txt`, `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.identity.txt` and `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.identity.txt`, each a second rendering of the check its own console stream owns |
| The presence and method counts of the six probe-surface types, the mangled `createDriver` name with its zero call sites, and the connect-shims collision resolved by query rather than by inference | `harness/artifacts/logs/cpg-verify.log`, PART 3.3, which is that sweep's owner; this report cites it and takes no measurement of the graph itself |
| That the same current pair gates Stage 3, measured by the same `record_of_account()` resolution as the probe's own checks | `harness/artifacts/logs/joern-preflight.log`, which records the recorded and observed size and sha256 as MATCH on both values and `VERDICT: PASS` |
| That the identity gate refuses a graph its record does not describe | `harness/artifacts/logs/joern-preflight-negative-test.log`, whose perturbation, `VERDICT: HALT`, gate exit 77 and restored-record re-run belong to an earlier lane and a different graph, and which is cited for the mechanism rather than for any figure of this graph |
| Each query's invocation as executed — the command, its working directory, the closed stdin, the explicit graph selector and `graph.path_source` — and the JVM-argument disclosure of 1 observed, `-Xmx64g` kept as written, 0 reduced and 0 redacted | each envelope's `runtime.command`, `command_precondition`, `command_working_directory`, `command_graph_selector`, `command_completeness`, `jvm_arguments_observed_count`, `jvm_arguments_kept`, `jvm_arguments_redacted_count`, `jvm_arguments_redacted_keys` and `jvm_arguments_redaction_policy`, with `determinism.reproduction_command` carrying the same command text, and the three streams' `JVM memory/stack args`, `JVM other args (reduced)` and `JVM args kept / reduced` lines |
| The byte size and sha256 of each of the nine publication members | the three completion manifests, each naming its members' `path`, `bytes` and `sha256`, and each deriving its own `member_set_id` from exactly those filename-and-digest pairs |
| The frontend window of the graph these three loads read, and the `importCpg` verification triple taken at write time | `harness/ENVIRONMENT.md` §7 for the window and `harness/artifacts/logs/cpg-verify.log` for the triple; the provisioning's own record beside the graph states both but sits outside this checkout |
| That the **earlier** generation's graph record is a separate in-tree file, kept with its generation rather than read as current | `harness/artifacts/logs/cpg-graph-record.log`, whose own lines 3, 6, 12 and 63 state its invocation, its `50 m 42 s` window with `FRONTEND_EXIT=0`, its sha256 `4616845a…` and its 541,309,809 bytes — the graph the 2026-09-03 re-provisioning replaced |

Console evidence is
`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`,
`harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` and
`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` — **each the
stream of the invocation that produced that query's figures**, each a completed
invocation ending in its success marker, and each published as the third member of
its query's publication alongside that query's envelope and prose result under one
`publication_id`. No query's figures are cited from an envelope alone, and no log
here records a halted invocation. Both artifact trees are git-ignored and are
published by manifest with per-file byte size and sha256 in
`harness/artifacts/MANIFEST.json` and `oss-scan-results/run-record.md` §16, which
indexes this report but does not substitute for it: the per-query probe results are
owned here. For the probe's own fifteen files the sizes and digests of this generation
are tabulated above under "Every published member of this generation", together with
the disagreement that tree-wide manifest still carries for nine of them.
