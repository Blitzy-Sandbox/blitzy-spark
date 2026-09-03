# Run record — the index to every stage

> ## ⛔ STATUS: HALTED — NOT A COMPLIANT GENERATION
>
> **Read this before any figure below.** The Stage 0 gate returned verdict **`halt`** and
> authorises **`nothing`**. Every stage after it ran after an unmet precondition, and **two**
> further halt-class conditions stand unrepaired: the mandated graph over every JAR the build
> produced **could not be persisted at all**, and the mandated taint A/B **does not
> discriminate** on its subject. A third stood for most of this run's history — the provisioned
> record contradicting the filesystem on the graph's identity — and is **now closed at the
> record**, re-anchored on 2026-09-02 and again on 2026-09-03 after a re-provisioning rebuilt
> the graph beneath the first correction, with the graph itself untouched in both (**D4**).
>
> **Consequently: no stage of this run is certified complete, and no artifact it produced —
> not `findings.json`, not `findings.csv`, not the probe results, not the per-tool records —
> is offered as satisfying the requirement it was meant to satisfy.** Every figure in this
> file is a real measurement with a citable source, and none of them is a pass. The blocking
> conditions, and the specific permission each would require to clear, are stated in
> [§18](#18-where-the-run-reached); the full divergence set is in
> [§13](#13-divergence-register).
>
> **Runtime testing of 2026-09-03 raised twenty-seven findings, of which nine are answered by
> this document.** One is resolved by this run — AAP §0.9.2's method-count floor is now a
> declared constant and an adjudicated verdict — one was resolved at the record, and the rest are
> **published records rather than repairs**, because each has its root cause in a file AAP §0.6.1
> marks REFERENCE and §0.3.2 makes such a condition *a condition to record, not a defect to
> repair*. Every one of the nine is mapped to the entry that carries its measurement in
> [§13's 2026-09-03 register](#qa-testing-findings-of-2026-09-03--the-nine-this-document-answers-and-where-each-is-answered).
> That checkpoint invoked **no tool of any kind** — no runner, no normalizer, no `joern`, no
> `java`, no scanner — so `raw/` is unchanged member for member, no dataset row was produced and
> nothing in it lifts the halt.
>
> **Runtime testing of 2026-09-02 raised five blocking findings against this checkpoint, and
> all five were re-executed from their own reproduction steps in this clone on that date: all
> five still reproduce.** Four of them are the halt-class conditions already named above and the
> fifth is the per-module coverage witness rule, which is now measured over the input set the
> plan requires as well as over the graph's own — **30 of 38** modules carry an accepted witness
> there and **8** carry none. Each finding is answered one by one, with the evidence file this
> pass measured it into and the decision it needs from a human, in
> [§13's closing register](#qa-testing-findings-f1f5-2026-09-02--first-hand-re-verification-and-where-each-is-answered).
> That pass performed **measurement only** — no runner was invoked, `raw/` is unchanged byte for
> byte, no dataset row was produced and no stage was advanced — so nothing in it lifts the halt.
>
> **Six lanes, not one, and not a monotonic stage order.** Stage 1's build half and the
> superseded Stage 2 frontend attempt are inherited evidence from lane `w-005` (2026-08-30/31)
> and were **not** re-executed here; Stage 0, Stage 1's dynamic half and Stages 2 through 5 were
> executed in lane `w-013` on 2026-09-01; Stage 4 alone was re-executed in lanes `w-020` and
> `w-025` on 2026-09-02; and **the graph was replaced by a re-provisioning at
> `2026-09-03T01:17:07Z`**, so lane **`w424`** re-executed everything that reads it — both
> preflight gates, three `importCpg` verification loads, Stage 3, Stage 4 and all three Stage 5
> probes — between `09:07:46Z` and `10:16:53Z` that day. **Every graph-derived figure in this
> document is `w424`'s**, and every superseded figure is retained beside it with its generation
> and date. The stages did **not** run in numeric order — §18 publishes the measured instants
> and names the inversions. The table under *How to read a citation in this file* says which
> file belongs to which lane.

Every number in this document names the file it came from, and every one of those
files exists on disk. That is this file's organising rule (AAP §0.6.2), and it is
why the durable evidence files under `harness/artifacts/logs/` are deliverables
rather than conveniences: a figure whose source cannot be opened is a figure
nothing downstream can check.

## What this file is, and what it is not

It is the **index** across every stage of the run: the gate, the pinned tree, the
build, the graph, the nine runners, normalization, the capability probe, and the
record itself.

**It is an index, not an owner** (AAP §0.6.4). It may point at a value another
document owns; it may not substitute for that document's required content.
Ownership is fixed elsewhere and is not relocated here:

| Value | Owner — cited here, never restated as this file's own measurement |
| --- | --- |
| The Maven pre-check verdict, the per-project JAR outcome for all 40 reactor projects, the JAR inventory and staging manifest, and **the per-module graph coverage verdict with its evidence** | `oss-scan-results/build-record.md` |
| The per-tool status contract — version, ruleset or feed identity, feed state, baked flags, exit code, elapsed time, finding count, parse status, records parsed and rejected, reconciliation, reach | `oss-scan-results/tool-status.md` |
| The severity mapping as policy, and every native literal observed with its row count | `oss-scan-results/severity-map.md` |
| The probe's per-query results, its four reporting requirements and its three effort measures | `oss-scan-results/joern-probe.md` and the six files under `queries/joern/results/` |
| Which fixture came from which artifact, and what each test module asserts | `oss-scan-results/adapter-tests/README.md` |

A count appearing in two documents is **one measurement cited twice, never two
measurements**. Where this file states a figure that another document owns, the
citation names that document, and the figure is the one that document carries.

Three things this file does not do, stated here rather than left to be inferred
from its absence — the full statement is at the end:

- It draws **no comparison between tools**. Indexing nine outcomes side by side is
  an index, not a comparison.
- It **judges no finding** — not real, not important, not a false positive, not a
  duplicate of another tool's.
- It reads **no elapsed time as a budget**. There is no time limit anywhere in
  this run (AAP §0.8.1), so every duration below is a fact.

## Governing rules

**No user-specified rules were provided for this project.** `review_rules`
returned exactly the single line `No user rules provided.` — the complete
document, not a truncated or failed read. AAP §0.7 and §0.10.2 corroborate it
independently. Enterprise-standard best practice applies in their place, and the
absence is **not** licence to lower the bar: every constraint this file is held to
below is an AAP *requirement* cited by section, not a rule.

## How to read a citation in this file

**Paths are repository-relative**, because that is the form a reader can resolve
in any clone carrying this branch. The absolute form of every deliverable is in
[§11](#11-deliverable-inventory-with-resolved-absolute-paths), written as `<repo>`
followed by the repository-relative path, where `<repo>` is whatever this command
prints in the checkout you are reading from:

```text
git rev-parse --show-toplevel
```

**This file states no clone-specific checkout root, deliberately.** Three earlier
editions each printed one — first `…-af512c69e0ab-w-013_59d11b`, then
`…-af512c69e0ab_a424a0`, then `…-af512c69e0ab-w-019_b09f94` — and all three were
wrong for the same structural reason: a
checkout root is correct only in the clone that wrote it, so the identical
committed bytes named a directory that does not exist as soon as the file was read
from a sibling clone or a fresh checkout of the same commit. The remedy could not
be to keep the literal current, because there is no single current value: this
branch is read from many checkouts at once and each has its own root. So the root
is defined by the command above and by the `<repo>` placeholder, which resolve
correctly everywhere, and `harness/lib/verify_publication_owners.py` enforces the
absence of a literal rather than the presence of one. The evidence files under
`harness/artifacts/logs/` do still name their own lane roots verbatim, and must:
each records the lane it was written in, which is a historical fact about where a
measurement was taken rather than a path a reader resolves.

**The evidence files name their own lanes.** This record is **not** a single
end-to-end measured generation, and it is important not to read it as one. The
evidence in `harness/artifacts/logs/` was produced in **six lanes across four
dates**, and every file states which:

| Lane | Date | What it produced | Files that name it |
| --- | --- | --- | --- |
| **`w-013`** — this clone, `run_id` `w013-20260901T132807Z` | 2026-09-01 | Stage 0 gate; **the dynamic half of Stage 1** — the runner scan-target value in force and the root each runner resolves, verified at `13:49:39Z`; the Stage 2 *verification and measurement* of the graph then in use; Stage 3, all nine runners; Stage 4 normalization and the adapter-test suite; Stage 5, all three probes. **Its Stage 2 measurements, its Stage 3 Joern invocation, its normalization and its three probes have since been superseded by the `w424` lane** — see that row — because the graph they were taken over was replaced; its other eight runner invocations stand | **18** log files, including `gate-record.json`, `runner-sequence.json`, `runner-metadata.json`, `cpg-input-inventory.json`, `cpg-verify.log`, the nine `<tool>.runner-console.log`, `adapter-tests-run.json`; plus `normalize-run.json`, `cpg-identity.txt`, `cpg-module-coverage.json`, `cpg-shims-collision-measurement.log` and the three `probe-*.log`, all written on 2026-09-01 in this clone. The `taint-ab-*` arms were written in this lane too but have since been superseded — see the `w-022` row |
| **`w-020`** — a later clone, `run_id` `w020-20260902T151108Z` | 2026-09-02 | **Stage 4 re-executed after this checkpoint's QA-driven fixes to `harness/lib/normalize/cli.py`** — the normalizer over the unchanged canonical inputs, and the adapter-test suite over the grown corpus. Nothing else: no runner, no build, no graph operation, no taint A/B, no probe | **3** log files — `normalize-run.json` and `findings-publication.json`, rewritten by that lane's canonical invocation, and `adapter-tests-run.json`, re-measured `15:11:08Z → 15:11:19Z`, whose scratch and repository paths name this lane's root. `harness/artifacts/MANIFEST.json` was re-measured against the trees on the same date and moved exactly those three members. `oss-scan-results/findings.json` and `findings.csv` were reproduced **byte-identically** by that run and are unchanged. **The two normalizer records were rewritten again in the `w-025` and then the `w424` lane** — see those rows — so `adapter-tests-run.json` is the member this lane still owns |
| **`w-005`** — a different clone's private scratch, **inherited, not re-executed here** | 2026-08-30 / 2026-08-31 | **The build half of Stage 1** — not Stage 1 entire, because Stage 1's other half, the runner scan-target and resolved-root verification, ran in `w-013` at `2026-09-01T13:49:39Z`: the private clone of the pinned commit, the Maven pre-check, and the 40 m 55 s full-reactor build that finished `2026-08-30T20:59:38Z` with exit 0. Also the Stage 2 *frontend write attempts*: the 8 h 01 m 191-archive attempt that ended at the flatgraph ceiling, and the 1 h 42 m 30 s narrowed witness attempt | `build-reactor.log`, `maven-preflight.log`, `cpg-frontend.log` — 13,166 occurrences of the string `w-005` across the three, every scratch path in them naming that lane |
| **`w-022`** — a security-checkpoint lane, `run_id` `w022-20260902T144244Z` | 2026-09-02 | **The Opengrep taint A/B, re-executed in full** — all twelve arms from `cd /opt/spark-src` at the pinned HEAD, between `14:42:44Z` and `14:50:34Z`, replacing arms that had been measured in a `w-001` lane against a scratch scan root. **The `importCpg` load of the narrowed witness graph** (`cpg-verify.log` PART 2, exit 0 in 458 s, 38 of 38 module witnesses). **The third flatgraph ceiling arm** at 8 GiB, completing the 8/64/128 GiB span. **A live census of `/opt/blitzy-harness/cpg-input`** at `14:44:09Z`. Plus corrections of record across the evidence files, each stating what it replaced | the twenty `taint-ab-*.log` and `taint-ab-*.sarif` arms; `cpg-verify.log` PART 2; `cpg-ceiling-reverify.log` and `cpg-frontend-ceiling-probe.txt`; the census node in `cpg-input-inventory.json`; and the corrected fields in `runner-metadata.json`, `cpg-frontend.log`, `cpg-graph-record.log`, `cpg-module-coverage.json`, `cpg-frontend-input-manifest.json`, `cpg-identity.txt`, `cpg-shims-collision-measurement.log`, `build-reactor.log` and `gate-record.json` |
| **`w-025`** — a remediation lane, **two of whose three code changes were reverted the next day** | 2026-09-02 | **Three QA findings of that checkpoint, fixed and re-verified.** The environment record re-anchored to the graph's write-time record of account — that change stands, and was re-anchored again by `w424`. **Two runner-side changes did not stand**: `harness/bin/run-joern.sh` was given a pre-load identity gate and an enforced child-JVM heap floor, and `harness/lib/joern-scan.sc` was made to measure its own heap. **Both files were reverted to their provisioned bytes on 2026-09-03** (**D26**), because AAP §0.6.1 marks every entry in `harness/bin/` REFERENCE, §0.8.1 states that no runner file is edited, and §0.3.2 makes a runner whose reach differs from expectation *a condition to record, not a defect to repair* — so the defects are published as provisioning patches instead. That lane's corrected runner was executed with its raw and log output redirected into private scratch, so **no canonical artifact was overwritten** by it. **Stage 4 re-executed** at `22:07:24Z → 22:07:30Z` over the then-unchanged raw tree, and again at `22:56:48Z → 22:56:54Z` once the security checkpoint's changes to `shape.py` and `paths.py` landed; **both are superseded by `w424`'s normalization** | `harness/ENVIRONMENT.md`; `harness/artifacts/logs/cpg-identity.txt`, `gate-record.json`, `runner-metadata.json`, `reverification-f2-graph-identity.txt`; the `22:56:48Z` `normalize-run.json` and `findings-publication.json`, which superseded that lane's own `22:07:24Z` pair and were themselves superseded by `w424`; and `harness/artifacts/MANIFEST.json` re-measured against both trees |
| **`w424`** — **the lane of record for the graph and everything measured over it**, clone index **424**, `run_id` `w424-20260903T093613Z` | 2026-09-03 | **The response to a re-provisioning.** The host was re-provisioned at `2026-09-03T01:17:07Z` and the frontend rebuilt the graph, which falsified every figure taken over its predecessor. This lane re-established them: **both preflight gates re-run outside every runner**, each exiting 0 with `VERDICT: PASS`; **three independent `importCpg` verification loads** of the new graph, agreeing on all four counts; **Stage 3 re-invoked** directly and with no arguments, `09:07:47Z → 09:17:43Z`, exit 0, rewriting `harness/artifacts/raw/joern.json`; **Stage 4 re-normalized** `09:45:11Z → 09:45:16Z`, exit 0; and **all three Stage 5 probes re-executed** between `09:45:34Z` and `10:16:53Z`. The other eight runners were **not** re-invoked, because nothing they read changed | `cpg-verify.log` PART 3 and the `w424-20260903T093613Z` run id it stamps; `joern-preflight.log` and `sec-gate-scan-target.log` with their `Clone index 424`; `joern.stdout.log`, `joern.stderr.log`, `joern.status` and `raw/joern.json`; `normalize-run.json` and `findings-publication.json`; the three `probe-*.log` and `probe-*.identity.txt`, each carrying a `supersedes` field naming its 2026-09-01 predecessor; and the re-anchored `cpg-identity.txt`, `gate-record.json`, `runner-metadata.json`, `runner-sequence.json`, `cpg-frontend-input-manifest.json` and `cpg-module-coverage.json` |

**What that means for the reader, stated plainly.** Stage 0, Stage 1's dynamic half,
and Stages 2 through 5 were executed in the `w-013` lane on 2026-09-01, one at a time
with no two running concurrently. Stage 4 alone was then re-executed in the `w-020`
lane on 2026-09-02, after this checkpoint's QA-driven fixes to the normalizer, and
reproduced both dataset files byte for byte — and once more in the `w-025` lane the
same day, after that lane's record corrections, reproducing them byte for byte again.
**Then the graph changed under all of it.** The re-provisioning of
`2026-09-03T01:17:07Z` replaced `/opt/blitzy-harness/cpg/spark.cpg`, so the `w424`
lane re-executed everything that reads it — the identity gate, the verification
loads, Stage 3, Stage 4 and all three probes — and this document's figures for those
stages are `w424`'s. *Serial is a statement about concurrency, not about
order*: the instants are in the ledger in [§18](#18-where-the-run-reached) and they are
not in numeric stage sequence, which that section states outright rather than smoothing
over. For the stages listed here every retained artifact, stream, status and record
does describe one measured generation, and where two generations exist both are carried
with their dates rather than blended.
**Stage 1's build half and the Stage 2 frontend write were not re-executed by any lane
of this run**; their evidence is the `w-005` lane's, retained verbatim and cited as that
lane's, and nothing in this file presents either as this clone's measurement. Re-running
them was not undertaken: the build's own figure is 40 m 55 s and the frontend's is
8 h 01 m, and neither would change the graph this record measures, because that graph is
**provisioning's** — `/opt/blitzy-harness/cpg/spark.cpg`, a host-shared read-only
artifact both lanes load rather than write, written by provisioning's own frontend
invocation of `2026-09-03T01:40:31Z → 02:11:54Z` and carrying the mtime
`2026-09-03 02:11:49Z`. Its predecessor carried the mtime `2026-08-30 19:18:37Z`, which
predated the `w-005` build; the current generation does not, and neither fact makes the
graph this run's output — it may not be rebuilt from here at all, because
`/opt/blitzy-harness/cpg/**` is shared read-only across up to 64 clones. The `w-005`
frontend attempt produced no accepted graph at all (§5, §13 **D3**, **D1**).

Two older lanes survive only as **labelled supersessions**, never as live claims:
`cpg-module-coverage.json` names clone `w-001` in its own `supersedes` field to say what
it replaced, and `probe-query-revisions.json` is labelled superseded and
non-reproducible in `joern-probe.md` because its `measured_at_head` is not an ancestor
of this branch. Where a figure anywhere in this file belongs to a superseded state it is
labelled as such and attributed to the record that carries it. The repository-relative
path is the stable identity throughout, and no clone's root is presented here as *the*
run's root.

---

## 1. Gate verdicts

Source: `harness/artifacts/logs/gate-record.json` (**148,734** bytes as re-measured for this record, up from the 100,747 an earlier edition stated, the growth being this checkpoint's `state_now` re-anchoring and its `this_runs_gates_2026_09_03` node; digest in
[§16](#16-manifest-of-the-two-git-ignored-artifact-trees)), `run_id`
`w013-20260901T132807Z`, clone index 13. Overall verdict **halt**, and its
`authorises` field is the single word **`nothing`**. Forty-three checks: **38 pass,
3 recorded difference, 2 halt**.

**What that verdict means for everything below.** No stage was authorised by this
gate. Two of the conditions AAP §0.9.2 lists among those that stop the run were
observed, both inherited. **One is still live** — the two artifact trees arrived
non-empty, which this run may neither create nor clear. **The other was re-anchored
twice**: the environment record disagreed with the graph on disk because the record was
stale, and the graph's own write-time record of account adjudicates the disagreement, so
the record was re-anchored to it rather than reported against it (**D4**) — on 2026-09-02,
and again on 2026-09-03 after the host was re-provisioned and the graph rebuilt beneath
the first correction. The counts above are the gate's own at its own instant and are not
restated; the verdict stands at `halt` on the one live condition. Every stage this
generation performed after the gate is therefore recorded as **work done after an unmet
precondition** — never as a compliant stage completion — and the artifacts it produced
are retained as evidence rather than presented as a passing pipeline. That framing is the
gate record's own, quoted here rather than softened.

**The live condition was re-censused on 2026-09-03 and is present again, larger.**
`ls -A harness/artifacts/raw | wc -l` returns **8** and `harness/artifacts/logs`
**129** top-level entries over **163** files — so the tree the gate found non-empty at 8
and 85 entries is non-empty at 8 and 129 now, and `gate-record.json`'s `state_now` node
carries that re-census. Nothing about the growth is illicit: `raw/` holds exactly the
eight runner artifacts it held then, member for member, and every addition is under
`logs/`, which AAP §0.8.1 says accumulates this run's own durable evidence
([§16](#16-manifest-of-the-two-git-ignored-artifact-trees)). What the growth does **not**
do is clear the halt, because the halt is about the trees being non-empty *before the run
began*, and that is a fact about an instant already past. **An execution cannot be
un-run**: Stages 2 through 5 ran after a gate that authorised `nothing`, and this record
reports that rather than repairing it — there is no action available to any clone that
retroactively supplies the precondition. What a human must do is present a provisioning
whose two artifact trees are empty; until then this verdict stands as measured.

**Two gates of this run's own ran outside every runner, and both passed.** They do not
supersede the verdict above — they are narrower checks, and neither is the Stage 0 gate —
but they are what bound the Stage 3 load on 2026-09-03, and nothing in `harness/bin/`
invokes either:

| Gate | Command | Outcome |
| --- | --- | --- |
| scan target | `python3 harness/lib/preflight_scan_target.py --check-only` | exit **0**, `VERDICT: PASS` at `sec-gate-scan-target.log` line 194, with `[PASS] smoke-override-absent` at its line 31 and `[PASS] artifact-tree:HARNESS_RAW_DIR` and `[PASS] artifact-tree:HARNESS_LOG_DIR` at its lines 145 and 155; structured form in `sec-gate-scan-target.json` |
| graph identity | `python3 harness/lib/preflight_graph_identity.py --check-only` | exit **0**, `VERDICT: PASS` at `joern-preflight.log` line 96, `Adjudicated count : 1,398,964 methods, agreed by 2 record(s) of account` at its line 88 and `Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420` at its line 89 |

**The artifact-tree check in the first of those deliberately does not refuse a non-empty
tree**, and that is a design decision rather than an omission. Refusing would leave a
caller only two ways forward — create the tree or clear it — and AAP §0.8.1 forbids this
run from doing either. So the check **censuses** both trees, publishes what it found, and
passes; the halt on emptiness belongs to the Stage 0 gate, which recorded it and which no
clone can clear. **D28** carries that reasoning with the check's own location.

| The two halts | What was measured |
| --- | --- |
| `gate.artifact_trees_exist_and_empty` | At `2026-09-01T13:28:07.612Z`, **before this run wrote anything**: `harness/artifacts/raw/` present with **8 entries**, `harness/artifacts/logs/` present with **85 entries**. The rule is emptiness and both trees hold entries. Attribution does not make a non-empty tree empty — the entries are committed deliverables of earlier clones of this code generation, which is exactly the case the rule exists to catch, because an artifact already in place is indistinguishable from this run's. **Reported, not repaired**: neither tree was cleared and no entry was deleted to manufacture a pass |
| `gate.environment_record_graph_identity_agreement` | **At the gate:** `harness/ENVIRONMENT.md` §7 stated the graph as 541,255,894 bytes / `26d327cc…`, 1,397,339 methods, 119,691 type declarations; the filesystem then held 541,309,809 / `4616845a…`, and the load measured 1,396,899 / 119,721. The gate read that as AAP §0.1.3's fourth case and recorded both values without repairing either. **A correction of 2026-09-02** re-anchored the record to the filesystem pair of the day and this record published it as `status_now: resolved`. **That claim was WITHDRAWN on 2026-09-03**, falsified not by an error but by an event: the host was re-provisioned at `2026-09-03T01:17:07Z` and the frontend rebuilt the graph, so the pair the correction anchored to stopped being the pair on disk. The literal is retained as `status_now_superseded_value`, and the record was **re-anchored again** to the graph now on disk — 547,980,224 / `325887cf…3dc6`, 1,398,964 methods, 119,860 type declarations — which is the state an identity gate exiting **0** with `VERDICT: PASS` evidences (`joern-preflight.log` line 96). **The gate's other stopping condition — non-empty artifact trees — is untouched, so `gate_verdict.overall` remains `halt`** (§5, **D4**) |

| The three recorded differences | Expected → observed |
| --- | --- |
| `datadog-static-analyzer` ruleset digest and rule count | table `e70ede30…`, 48 rulesets, 1,093 rules → observed `c5fd464c…`, **53 rulesets, 1,147 rules**. Counts marked **not comparable with the rehearsal**, and the API-time capture with no publisher digest recorded as a named reproducibility gap |
| Trivy vulnerability and java database timestamps | table v2 `2026-08-23T06:56:50Z` / v1 `2026-08-23T01:05:59Z` → observed v2 **`2026-08-30T13:05:01Z`** / v1 **`2026-08-30T01:07:49Z`**. Trivy's counts marked not comparable with the rehearsal |
| Dependency-Check NVD datafeed timestamp | table `2026-08-23T08:00:06-04`, keyless → observed **`2026-08-30T12:00:19-04`**, a 260,005,888-byte `odc.mv.db` |

In each case the **expected-values table governs**, all values are recorded with
their provenance, and the difference is record-and-continue under AAP §0.9.3 rather
than a halt. All three are the same inherited cause as the graph identity: the host
was re-provisioned on 2026-08-30.

| Gate condition | Verdict, as recorded |
| --- | --- |
| The environment record read **first** | `harness/ENVIRONMENT.md`, **923 lines, sha256 `5aa68b255295e26ae129b9159e32ea76b33d1d66f835aa9a3625b040f5ecb140` when the gate read it**, in full before any other gate command ran — the pair `gate-record.json` recorded, retained here as that instant's reading. **It has since been re-anchored** and is now **1,044 lines, sha256 `e296c55602176883e3026e5884a83241c86db2228de207e0d537d94004a9a40c`**: on 2026-09-02 its §7 and its inline-values Graph block were corrected to the graph's own write-time record of account, and a supersession appendix was appended carrying both values for every corrected field — see [§13, D4](#13-every-divergence-with-both-values), and the `post_gate_re_anchoring` field of the gate record for the pair on both sides. **No content at or before what was then its final line changed position** — the correction rewrote values in place and appended the appendix after them — so every line citation into it from this document and from `build-record.md` still resolves. Correcting it was a deliberate departure from AAP §0.6.1's REFERENCE marking, taken because `git log -- harness/` shows the file authored and committed by `Blitzy Agent <agent@blitzy.com>` — it is this project's own record *describing* the provisioning, not inherited provisioning surface — and because leaving it false meant every load ran against a record describing a different graph. What every citation of it below is a citation of is a **record of the provisioning**, governed by the authority rule in [§17](#17-the-authority-rule-and-where-it-does-and-does-not-reach): a statement about the weight the record carries against an observation, never about whether a reader can open it. The paths this document cites that are genuinely not resolvable here are listed in [§11](#paths-this-document-cites-that-are-not-resolvable-from-this-clone), and this file is not among them |
| The environment file **the record names**, never assumed | The record — `harness/ENVIRONMENT.md` — names it twice at that file's lines 6–13 — the sourcing command `. harness/env.sh` and the sentence naming `harness/env.sh` as the environment file. Present, 4,515 bytes, 91 lines, mode 755 |
| Sourced in a **fresh non-login shell** | `env -i BLITZY_CLONE_INDEX=13 bash --noprofile --norc -c '. harness/env.sh'` → exit 0, stdout empty, stderr empty |
| All nine tools resolve | Eight by bare name under `/opt/blitzy-tools/bin`, plus the `jimple2cpg` wrapper; `dependency-check` resolves through `$DEPENDENCY_CHECK_HOME/bin/dependency-check.sh` rather than by bare name. Zero `NOT-ON-PATH` results |
| Nine versions against their pins | opengrep 1.27.1, semgrep 1.173.0, joern 4.0.607 (read from the startup banner with stdin closed, there being no `--version`), datadog-static-analyzer 0.9.1 revision `f76636e43554f7f9a8e3984a31d03ec8dea5489f`, gitleaks 8.30.1, checkov 3.3.12, trivy 0.74.0, osv-scanner 2.5.1 (osv-scalibr 0.5.2), dependency-check 13.0.0 — **every one equal to its pin** |
| The Python interpreter's absolute path and version | `/usr/bin/python3`, 3.13.7, build string `3.13.7 (main, Mar  3 2026, 12:19:54) [GCC 15.2.0]` — equal to the expected 3.13.7. The two scanner virtualenvs `/opt/blitzy-tools/venvs/semgrep/bin/python` and `/opt/blitzy-tools/venvs/checkov/bin/python` each report 3.13.7 as well |
| Both JDKs present | Temurin-17.0.20+8 (major 17) and Temurin-21.0.12.1+1 (major 21), each reporting its own version |
| The heap **commit** proof | `"$JAVA_HOME/bin/java" -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version` → **exit 0**, and a second arm under the 21 JDK → exit 0 in 9.250 s. `-Xms` equal to `-Xmx` with `+AlwaysPreTouch` touches every page at startup, so a zero exit is **strictly stronger than a reservation** |
| `harness/bin/` enumerated and classified | Nine entries, all mode 755, all `run-<tool>.sh`: **9 runners, 0 helpers, 0 orchestrators**, each mapping to one canonical identifier in the AAP §0.5.4 class table, and **no entry naming a scanner that table does not carry** |
| Each runner's argument guard | Established **by inspection** for all nine: the guard is the first executable statement and exits 64, and it precedes environment sourcing, shared-library sourcing, target resolution and tool invocation in every one. **No rejection probe was run**, and none was permitted, because inspection settled the ordering |
| Both artifact trees exist and are empty | **HALT.** Both exist; **neither is empty**. At `2026-09-01T13:28:07.612Z`, before this run wrote a byte, `raw/` held **8 entries** and `logs/` held **85**. Reported, not repaired — see the halt table above |
| The smoke override unset | `HARNESS_SMOKE_TARGET` unset in the fresh non-login shell **and** in the ambient shell, so a value inherited from the invoking environment could not hide behind `env -i`'s stripping |
| Credentials | `SEMGREP_APP_TOKEN`, `DD_API_KEY`, `DD_APP_KEY`, `NVD_API_KEY` and `BC_API_KEY` absent in both arms. `GITHUB_TOKEN` reads set in the ambient shell and is read by no runner. Every runner reports credential state through `scope_cred_state` (`harness/lib/scope.sh` lines 105–109), which expands `${VAR:+set}` only and can print nothing but the fixed tokens `set` and `absent` |
| The allowlist byte-exact | sha256 `0013edf6cdc3a48d69aed5d7db41cc6647cfd461d348f5e1d563ba85664143d1`, 12 lines, byte-identical to the twelve authoritative globs in the AAP §0.3.1 order. Transformation mode **REFERENCE** — read as-is, left exactly as found, **nothing written** |
| Maven identity, and that no download would trigger | `/usr/local/bin/mvn`, Apache Maven 3.9.11 (`3e54c93a704957b63ee3494413a2b544fd3d825b`), home `/opt/blitzy-tools/apache-maven-3.9.11`, running on Temurin 17.0.20 — exactly the version the pinned pom requires |
| Scala, git, pinned HEAD | Scala 2.13.17; git 2.51.0; `git -C /opt/spark-src rev-parse HEAD` equal to the pin |

**The gate record cannot be the thing that made `logs/` non-empty.** The emptiness
check was taken **first**, at `2026-09-01T13:28:07.612Z`, before this run wrote a
single byte into either tree, and this record was written afterwards from a result
held in memory. The ordering is not merely satisfied but **moot in this case**:
`logs/` already held 85 entries before the gate record existed, so writing it cannot
have caused the condition. The record states the ordering as a field of its own,
with the timestamp.

**One state worth naming precisely.** The emptiness rule covers `raw/` and `logs/`
only, and **both were already non-empty** — 8 and 85 entries, the committed
deliverables of earlier clones of this code generation. That is a halt, recorded
above and not repaired: neither tree was cleared and no entry was deleted to
manufacture a pass. Stated in the form the halt's own disposition field uses:
**both trees existed before this run began, so this run created neither and
cleared neither** — unlike the inherited gate record of clone 9, this run did not
even bring one of them into existence as a side effect of sourcing the environment
file.

**The allowlist's fourth state would have halted, and is named because it did not
arise.** `harness/ENVIRONMENT.md` section 5 **does** state this file's digest, so
had the file's content differed, AAP §0.6.3's record-contradiction case would have
applied and the run would have stopped rather than correcting it. The content did
not differ, so no write was needed and none was made.

---

## 2. The pinned tree

| Field | Value | Source |
| --- | --- | --- |
| Repository | `https://github.com/blitzy-public-samples/blitzy-spark` | AAP §0.10.3 |
| Commit (the pin) | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` | `gate-record.json`, check `gate.pinned_tree_head`: `git -C "$SPARK_SRC" rev-parse HEAD` equals the pin and equals `SPARK_SRC_COMMIT` as exported |
| Commit date | **Thu Oct 23 15:31:06 2025 -0400** (`2025-10-23T19:31:06Z`) | measured here with `git -C /opt/spark-src log -1 --format=%ad` |
| Divergence from `apache/spark` | **`identical`** — **taken as recorded, not re-derived** | `harness/ENVIRONMENT.md` line 161, which states it and states that no `apache` remote was added to re-derive it. Confirmed non-invasively: `git -C /opt/spark-src remote -v` lists only `origin`, pointing at the repository above, so no upstream remote exists in that tree to have derived it from |
| Tree on disk (`SPARK_SRC`) | `/opt/spark-src` — the **scanned** tree: what all nine runners resolved and read, **never edited and never built by this run**, and shared read-only with concurrent clones | `runner-metadata.json` `spark_src`; `gate-record.json` check `gate.pinned_tree_head` |
| Tree the build ran in | a **private by-SHA clone** of the same commit at `/tmp/blitzy/scratch/f38258d3-f87d-44f5-bedc-af512c69e0ab/w-005/build/spark-src`, proved equal to the shared pinned clone — `git rev-parse HEAD` equal to the pin, working tree clean, and the sha256 of the sha256sums of every tracked file equal to `/opt/spark-src`'s, reported as `BUILD_TREE_MATCHES_SPARK_SRC=yes`. The shared clone was left untouched because it is read concurrently and carries only a narrowed build | owned by `oss-scan-results/build-record.md` §2; the equality proof is `harness/artifacts/logs/build-reactor.log` STEP 4 |
| Version at the pin | Spark **4.1.0-SNAPSHOT** | `pom.xml:29` |
| Java | **17** | `pom.xml:120` |
| Maven | **3.9.11** | `pom.xml:123` |
| Scala | **2.13.17** | `pom.xml:178` |

**The working checkout is neither built nor scanned** (AAP §0.3.2). Its tip is a
different commit on a later snapshot version; `harness/artifacts/logs/maven-preflight.log`
STEP 7 read its pom once as a labelled contrast — `3.9.12` at the tip against
`3.9.11` at the pin — and used it for nothing.

---

## 3. The compiled glob expansion

The twelve authoritative globs in `harness/scope/allowlist.txt` expand on the
pinned tree to **18 directories covering 4,095 files**.

- **18 directories** — `harness/artifacts/logs/normalize-run.json`
  `allowlist.expansion`, which records `directory_count` 18 against
  `expected_directory_count` 18 and lists all eighteen by name.
- **4,095 files** — the expected value carried by `harness/ENVIRONMENT.md`, and
  the figure Opengrep's own summary line independently reports over the same
  scope: `Ran 1138 rules on 4095 files: 1319 findings.`
  — `harness/artifacts/logs/opengrep.stderr.log` line 31, the tool's own stream,
  quoted verbatim; its line 7 states the same denominator on the way in, `Scanning
  4095 files tracked by git with 2006 Code rules:`. The runner's `.status` file
  carries no such field: it is the seven-line `scope_finish` trailer and states
  the tool, exit code, elapsed seconds, artifact path and byte size, and the scan
  root with its source, and nothing else.
  Re-measured for this record with a matcher implementing true zero-or-more-segment
  `**` semantics over `/opt/spark-src`, excluding every path containing
  `src/test`: **4,095**.

Ten globs map one-to-one. Two multiply, and both were confirmed on disk in the
pinned tree:

| Glob | Expands to | Directories |
| --- | --- | --- |
| `sql/connect/**/src/main/**` | **five** | `sql/connect/client/jdbc/src/main`, `sql/connect/client/jvm/src/main`, `sql/connect/common/src/main`, `sql/connect/server/src/main`, `sql/connect/shims/src/main` |
| `resource-managers/kubernetes/**/src/main/**` | **three** | `resource-managers/kubernetes/core/src/main`, `resource-managers/kubernetes/core/volcano/src/main`, `resource-managers/kubernetes/docker/src/main` |

**Eighteen and twelve are the same scope.** Expanding a glob is arithmetic on the
allowlist, never an extension of it (AAP §0.8.2), and the allowlist stayed
byte-exact regardless of how many consumers read it.

**The test-source exclusion is literal.** A path containing `src/test` is out of
scope, which removes every Scala and Java test tree. It removes nothing from
`python/pyspark/**`, whose test modules carry no such segment, sit inside the
authoritative glob and are part of the 4,095. **No Spark test suite was executed,
in any language, and no Spark test source was modified.** This run's own adapter
and reconciliation tests did execute — they are new files under
`oss-scan-results/adapter-tests/`, and their outcome is in
[§9](#9-normalization-and-the-dataset).

**832 of the 4,095 in-scope files are `python/pyspark` test modules, and all 832
are in scope.** The glob `python/pyspark/**` contributes **1,203** files, of which
**832** are test modules — under a `tests/` directory or named `test_*.py` — and
**none** of the 1,203 contains a `src/test` path segment, which is why the literal
exclusion removes none of them.

- **832 and 1,203** — `harness/artifacts/logs/gate-record.json`, check
  `gate.allowlist_byte_exact` (`checks[36]`), field `scope_arithmetic_note`, which
  records the twelve globs expanding to 18 directories and 4,095 files "of which 832
  are python/pyspark test modules that are IN scope because none contains a src/test
  path segment". Re-measured for this record over the pinned tree at
  `/opt/spark-src` by walking `python/pyspark/**`, excluding any path containing
  `src/test` and counting the modules under a `tests/` directory or named `test_*.py`:
  **1,203 files, of which 832 are `python/pyspark` test modules** — the two
  measurements agree.

Reading "tests are out of scope" loosely would drop these 832 and make the 4,095
irreproducible: they are **read** by the scanners exactly as any other in-scope
source is, and **never executed**.

---

## 4. The build

Owned by `oss-scan-results/build-record.md`, whose producer logs are
`harness/artifacts/logs/maven-preflight.log` and
`harness/artifacts/logs/build-reactor.log`. Cited here, not re-derived:

| Indexed value | As `build-record.md` records it |
| --- | --- |
| Maven pre-check | **PASS — no download would be triggered.** Required and detected versions both normalize to `003009011`, so `build/mvn:126`'s conditional evaluated false; no distribution of any version exists under `build/`, so the early return at `build/mvn:119`–`121` was not taken either. The download branch was therefore unreachable, established **before** the build ran |
| Build command | The full reactor under the five mandated profile flags, explicitly not narrowed with `-pl`; `build-record.md` §2 carries the command and the five-flags-add-four-modules arithmetic |
| Result | `BUILD SUCCESS`, Maven exit **0**, **40 of 40** reactor projects `SUCCESS`, 0 `FAILURE`, 0 `SKIPPED` |
| Wall clock | Maven's own `Total time:  40:55 min` (**2,455 s**), finished `2026-08-30T20:59:38Z`; the runner's independent measurement of the same build is **2,460 s**, five seconds longer because it brackets the wrapper and JVM startup. Two readings of one build, and **no other duration for this build exists in this run's evidence** — a fact, not a figure measured against a budget |
| Reactor arithmetic | 35 unconditional child modules + 4 profile-added = 39 children, + the root parent project = **40 Maven projects: 38 packaging a JAR, 2 packaging none** |
| Own artifacts on disk | **38 of 38** JAR-packaging projects produced their own main artifact; **none** did not |
| Diagnostic pass | Not needed and not written — the reactor succeeded, so no `build-<module>.log` exists and none is invented |
| JDK the build ran under | major **17** |

### The expected non-JAR outcomes

Three, and all three are expected rather than failures:

1. **The root parent project** — `spark-parent_2.13`, `<packaging>pom</packaging>`
   at `pom.xml:30`. `build-record.md` §3 records `own artifact: NONE - EXPECTED,
   packaging=pom`; the attached test-jar in its build directory is listed as
   *also*, never as a main artifact.
2. **`assembly`** — `spark-assembly_2.13`, `packaging=pom` in its own pom, and
   Maven's own `[pom]` marker. The 340 JARs in its build directory are all copied
   runtime dependencies, counted and excluded.
3. **`python/pyspark`** — one of the twelve authoritative scope roots, **scanned**,
   and **no Maven module at all**: it appears in no reactor, `grep -c
   '<module>python'` against the root pom is 0, and `python/pyspark/pom.xml` does
   not exist. It therefore has no reactor row and none is invented for it. The
   same holds for `resource-managers/kubernetes/docker`, which the file-based
   tools reach through the mid-path `**` of the Kubernetes glob.

Walking every child pom for `<packaging>` confirms **only `assembly`** is `pom`
besides the root parent — the two, and no others.

---

## 5. The graph — its counts, its bytes, and the one-sided floor

**Which graph these counts describe, and whose measurement they are.** Two statements, and
both must travel together. **The graph is not this run's output**: per **D1** this run invoked
the frontend over its complete 191-artifact input set and the invocation failed in
serialization at a fixed array-length bound, producing nothing, so every count in this
section describes the graph at the sanctioned path — the one provisioning wrote and every
stage of this run loaded. **The counts, however, are this run's own measurement of that
artefact**, taken from the bytes on disk rather than read out
of any record: `harness/artifacts/logs/cpg-verify.log` names itself "this generation's graph
verification record", re-verified the pair **547,980,224 / `325887cf…3dc6`** immediately
before its load at `2026-09-03T09:36:13Z` (its lines 54-57), and re-derived each figure
from the artefact it then opened. Both halves are stated at the top of the section rather
than in a footnote, because a count read as describing a graph this run *built* would be the
single most misleading number in the record, and a count read as quoted from a record rather
than measured would be the second.

**One generation of these counts has been superseded, and by a rebuild rather than by a
correction.** Until 2026-09-03 every figure in this section described a **different
artefact**: the graph of 2026-09-01, **541,309,809** bytes / sha256 `4616845a…4730c7`, whose
counts were **1,396,899** methods / **119,721** type declarations / 45,037 files, loaded in
clone 13 in 885,009 ms. The host was re-provisioned at `2026-09-03T01:17:07Z` and the
frontend rebuilt the graph, so those bytes are on no disk this checkout can reach. A record
describing bytes that are not there is not history but a false live claim, so every live
figure below is the artefact actually present and every superseded figure keeps its place
labelled with its generation (AAP §0.1.3). `cpg-verify.log`'s **PART 3** states the same
supersession in its own words at its lines 821-833.

The counts come from `harness/artifacts/logs/cpg-verify.log`'s **PHASE 1** (its lines
106-113), which re-derived them from the
artefact itself by loading it with `importCpg`. **Three separate JVMs did that on
2026-09-03**, each recomputing the graph's size and digest itself before importing —
the verification load of record, a per-witness detail load and a corroboration load,
at import elapsed **526,605 / 557,041 / 545,644 ms** (that log's lines 847-853) — and
**all four counts are identical across all three** (its lines 884-887). A count measured
once is a count with no corroboration, and these counts are cited in eight published
documents.

| Count | Expected | Observed | Delta | Halt semantics |
| --- | --- | --- | --- | --- |
| methods (anchor) | 898,336 | **1,398,964** | +500,628, +55.73 % | **one-sided: no upper bound** |
| methods (floor) | **853,420** | **1,398,964** | +545,544, +63.92 % — **1.64×** the floor | **below the floor HALTS** |
| type declarations | 87,381 | **119,860** | +32,479, +37.17 % | **never halts** |
| files | 38,818 | **45,037** | +6,219, +16.02 % | **never halts** |

`methods > 0` was confirmed explicitly, and 1,398,964 is not zero — a graph that
loads with zero methods is the signature that check exists to catch. The load also
split the total two ways, and the two parts add back to it exactly: **1,308,974
internal** methods and **89,990 external**, summing to 1,398,964. The superseded
generation's split was 1,307,112 internal and 89,787 external over its own 1,396,899.

**Which bytes these three counts belong to.** The bytes on disk: **547,980,224 /
`325887cf…3dc6`**, the pair `cpg-verify.log` re-measured and matched against the record
of account immediately before its load (its lines 54-57, and again at its lines 877-880 for
all three loads) and the pair every other load of
this run measured too — the Stage 3 Joern runner, which printed it from its own recompute,
and the three probe queries. The three probe envelopes measure the same artefact, and the
Stage 3 artifact's own envelope reports **1,398,964 methods, 119,860 type declarations and
45,037 files**, so the table above and the envelopes are one measurement of one file rather
than two of two. **Two other pairs appear in this record and neither is on any disk it can
reach**: 541,309,809 / `4616845a…4730c7`, the graph of 2026-09-01 that the re-provisioning
replaced; and 541,255,894 / `26d327cc…`, with 1,397,339 methods and 119,691 type
declarations, which `harness/ENVIRONMENT.md` §7 stated alone at the gate and which was
the second of the gate's two stopping conditions. **Since 2026-09-03 that record states the
pair on disk**, re-anchored to the graph's write-time record of account; §13 **D4** keeps
every statement with its provenance, as does that document's own supersession appendix.

**The method bound is a floor and nothing else.** The floor is the 5 % lower bound
around the anchor: 0.95 × 898,336 = 853,419.2 → **853,420**. At or above it, the
count is **recorded**; below it, the run **halts**, because fewer methods from more
JARs is the truncation signature a lowered heap produces, and a truncated graph's
silence about a module is indistinguishable from a clean result. There is **no
upper bound**: the anchor was measured over the 32 JAR producers the
expected-values table names while a full reactor supplies 38, and more JARs cannot
yield fewer methods. **The input set was never trimmed in either direction to
bring a count inside a window** (AAP §0.3.2).

The type-declaration and file counts are reported as expected against observed and
**neither halts** (AAP §0.9.3).

**The cause of the excess is recorded as not established, not guessed.**
`cpg-verify.log` states this against its own interest: the AAP's stated rationale for
an above-anchor count is the six extra JAR producers, and the closing block of that
log's **PHASE 2** measures **7 of the 38 JAR-packaging projects — the six among them —
as absent** from this graph's input set entirely, so that mechanism cannot be the cause
here. What is measurable was measured instead, in **PHASE 1** at that log's line 112 —
**927,304** methods (**66.29 %**) under `org.apache.spark` and therefore **471,660**
(33.71 %) outside it, vendored by Spark's own shading — and the file stops there rather
than reporting a plausible cause as a finding. Those two figures sum to **1,398,964**,
which is the method count in the table above: the split is of **the bytes on disk**,
measured by the corroboration load of the same three, and it is not a superseded
generation's. The superseded generation's split of its own 1,396,899 was 925,445
(66.25 %) against 471,454. `harness/ENVIRONMENT.md` §7's former 1,397,339 was a different
statement about a different artefact and is attributed to that record wherever it appears;
since the 2026-09-03 re-anchoring that document states 1,398,964, the count measured here
(**D4**).

**Per-module coverage is owned by `build-record.md` §6** and is not restated here.
Cited: the graph at the sanctioned path was built over an input set spanning **31
modules**, and of those **26 are COVERED on injective evidence** — each on a class
present in that module's primary artifact and absent from every other module's, then
observed in the graph as a type declaration under its exact full name — while **5
carry NO VERDICT OBTAINABLE**, each named individually with the witness tried and why
it failed the test. **Zero** verdicts rest on presence in the input set and **zero**
on a shared package prefix; **zero** winner maps are claimed.

The five are `common/network-common`, `common/network-shuffle`, `common/utils-java`,
`sql/api` and `sql/connect/common`, and the reason is one measured fact in each case:
every class of that module's primary artifact also appears in another module's shaded
archive — 2,170, 92 and 40 classes respectively into
`common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar`, and 1,203 into
`sql_connect_client_jvm__spark-connect-client-jvm_2.13-4.1.0-SNAPSHOT.jar` — so no
class is exclusively theirs, and the AAP's named weaker witness is vendored too.
Presence is **not** substituted for a verdict, because presence would let the shaded
archive vouch for a module whose own artifact might be absent entirely.

Separately, **7 of the reactor's 38 JAR-producing projects are absent from this
graph's input set altogether** — `sql/connect/shims`, `tools`, `examples` and the four
`connector/kafka-0-10*` projects — so no witness for them could be queried in this
graph at all. That is an input-set fact and it is exactly what **D1** leaves open: a
coverage verdict for them against a graph built over every JAR the build produced,
which does not exist. **No narrowed graph is presented here as a substitute for the
mandated one**, and `build-record.md` §6 presents none either.

### The graph's byte size and sha256, and the identity re-verified before every load

**There is exactly one graph identity in the generation on record, and every load of it
measured that identity.** Seven loads read the graph on 2026-09-03 — the three `importCpg`
verification loads of §5's count table, the Stage 3 Joern runner, and each of the three
Stage 5 probe queries — and all seven measured the same
pair from the bytes on disk, with the symlink followed, **immediately before reading
them**. On the invocation of record the Stage 3 *comparison against the record of account*
also preceded its load, which is the ordering an earlier generation missed; both readings
are in the table below and in [§6.3](#63-the-stage-3-joern-runner--third-of-four) rather
than folded into this sentence:

| Field | Value |
| --- | --- |
| Name the plan gives it | `harness/cpg/spark.cpg` — a **33-byte symlink** |
| Name the environment exports | `$HARNESS_CPG`, which `harness/env.sh` line 28 defaults to that same path |
| Both resolve to | `/opt/blitzy-harness/cpg/spark.cpg` |
| Byte size | **547,980,224** (measured with the symlink **followed**) |
| sha256 | **`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`** |
| `dev:inode` of the resolved file | `2097196:105362303` |
| mtime of the resolved file | `2026-09-03 02:11:49Z` — provisioning's re-write of 2026-09-03, not this run's |
| Record of account | `harness/artifacts/logs/cpg-identity.txt` |
| The generation this replaced | **541,309,809** / `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`, `dev:inode` `1048701:89451825`, mtime `2026-08-30 19:18:37Z` — the graph every load of 2026-09-01 read, replaced in place by the `2026-09-03T01:17:07Z` re-provisioning and recoverable from no path this checkout can reach |

The 33-byte no-follow reading is recorded and explicitly discarded: it is the length
of the target path string, and a record carrying 33 would describe nothing at all.

**Why the record of account is `cpg-identity.txt` and not the frontend log.** AAP
§0.5.4 requires the identity recorded at write time, and this run's frontend
produced no graph (**D1**), so it has no write-time pair of its own to record.
`harness/artifacts/logs/cpg-frontend.log` states **no** `bytes:`/`sha256:` pair at
all — verified by reading it — so a gate pointed at that file cannot adjudicate any
load, which is precisely what it refuses to do rather than guessing. The record of
account was therefore produced by **calling the committed
`harness/lib/preflight_graph_identity.py`'s own `record_of_account()`** — the same
function the Stage 3 pre-load gate calls — so the record and the gate cannot state
different pairs by construction. That function prefers this checkout's own
frontend write-time pair, and falls back to the record written beside the graph at
write time: `/opt/blitzy-harness/provision-log/cpg-identity.txt`, corroborated by
`cpg-record.txt`, both read and **in agreement** at 547,980,224 /
`325887cf…3dc6`. Disagreement between candidate records is fatal to that function
rather than resolved by preference. Since the security checkpoint it also reads
`harness/ENVIRONMENT.md` section 7 as an unconditional third candidate, so the one record
that had contradicted the disk can no longer be absent from the comparison; and it
adjudicates the graph's **method count** against AAP §0.9.2's floor from those same
records, reporting `Adjudicated count : 1,398,964 methods, agreed by 2 record(s) of
account` and `Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420` at `joern-preflight.log`
lines 88-89.

**Identity re-measured for every load and each check logged. On the generation on
record the Stage 3 comparison ran before its load; on the superseded generation of
2026-09-01 it ran after, and both readings are kept:**

| Load | Where the check is logged | Result |
| --- | --- | --- |
| The three `importCpg` verification loads | `cpg-verify.log`, section "GRAPH IDENTITY, RE-VERIFIED IMMEDIATELY BEFORE THE LOAD" at its lines 54-57, and the per-load block at its lines 877-880 | match on byte size and sha256 against the record of account, in all three, each JVM recomputing both values itself before importing |
| The Stage 3 Joern runner — **two invocations, and on the one of record the comparison ran first** | The **recompute at load time** is the runner's own: `harness/bin/run-joern.sh` prints its input's identity at its lines 56-58, from a `stat` and a `sha256sum` taken on the resolved target, and prints it without comparing it. For the **2026-09-01** invocation that print survives verbatim in `joern.runner-console.log` lines 14-15 — `cpg bytes : 541309809` and `cpg sha256 : 4616845a…4730c7` — quoted as **that generation's** console and superseded as a description of any graph now on disk. For the **2026-09-03 invocation of record** no member of `logs/` captured the runner's own console stream at all ([§14](#14-values-that-could-not-be-established)); the pair it printed is recorded in `runner-metadata.json` field `tools.joern.stage3_invocation_2026_09_03.graph_identity_the_runner_printed` as **547,980,224 / `325887cf…3dc6`**. The **comparison against the record of account** is `joern-preflight.log` — the gate's own report, resolving the record, re-measuring each subject in its own right and printing **`VERDICT: PASS`** at its line 96 — stamped `Checked at (UTC) 2026-09-03T09:07:46Z` with `Clone index 424`, **one second before** the invocation it governs | **On the invocation of record the mandated ordering held**: the comparison preceded the load, the recorded pair sits at `joern-preflight.log` lines 47-48 and every subject re-measured `MATCH` on both values at its lines 56-57. **On 2026-09-01 it did not**: that report was stamped `14:52:54Z` in clone 0, about 11½ minutes after the load it adjudicated, so there the measurement was contemporaneous and the comparison was not. Both readings are carried in [§13](#13-divergence-register) **D4**'s adjudication row |
| Probe query 01 | Inside the query's own stream, `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` lines 32-53, taken by the loading process before it loaded. The standalone capture `…probe-01-callgraph-unguarded-driver-launch.identity.txt` is stamped `2026-09-03T09:45:34Z` and states the same pair, and its own `supersedes` field records that the file written by the `w-013` driver on 2026-09-01 stated 541309809 / `4616845a…4730c7` | In `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`: `size WITH following 547980224` at its line 36, `byte size matches: YES` and `sha256 matches: YES` at its lines 49-50, `graph identity: PASS — re-verified immediately before the load` at its line 53, and an `import binding: PASS` at its line 70 establishing that the imported private copy is the very bytes the check measured; the 33-byte no-follow reading recorded only to discard it |
| Probe query 02 | the same, in `probe-02-dataflow-unguarded-driver-launch.log`, whose standalone capture is stamped `2026-09-03T09:55:51Z` | the same pair, the same verdicts, at that log's lines 36, 49-50, 53 and 70 |
| Probe query 03 | the same, in `probe-03-parameterized-handler-sink-pairs.log`, whose standalone capture is stamped `2026-09-03T10:07:18Z` | the same pair, the same verdicts, at that log's lines 36, 49-50, 55 and 89 |

Each probe's identity file also records the name it resolved — `HARNESS_CPG` in this
checkout resolving to `/opt/blitzy-harness/cpg/spark.cpg` — so the check is against
the bytes that load, not against a path that might have pointed elsewhere.

**The graph was not replaced during the generation on record, and that is a measurement
rather than an assumption.** The seven loads span `2026-09-03T09:07:46Z` — the Stage 3
pre-load gate — through the three verification loads from `09:36:13Z` to the last probe
load, which finished `10:16:53Z` ([§18](#18-where-the-run-reached) publishes each instant).
Every one of them
re-measured the bytes on disk immediately before reading them and got the same pair. The
resolved file's mtime is `2026-09-03 02:11:49Z` — earlier than every one of those readings
and unchanged across them — so the bytes each load read are the same bytes, and the counts
in this section are all attributable to one artefact. **It *was* replaced between
generations**, which is the whole of **D4**'s current content: the `2026-09-03T01:17:07Z`
re-provisioning rebuilt it, so the seven loads of 2026-09-03 read different bytes from the
five loads of 2026-09-01, and each set is attributed to its own generation rather than
averaged with the other. The resolved path is host-global
and shared read-only with concurrent clones, and this run neither rebuilt it nor
replaced it.

**One record contradicted the filesystem about this graph, that is what stopped the
gate, and it has been re-anchored twice — the second time because the graph itself
changed under the first.** `harness/ENVIRONMENT.md` §7 **stated** the identity as
**541,255,894** bytes / sha256 **`26d327cc…fcffc`**, with **1,397,339** methods (internal
1,307,552) and **119,691** type declarations, against the 541,309,809 / `4616845a…4730c7` /
1,396,899 / 119,721 that every load of 2026-09-01 measured. A correction of **2026-09-02**
re-anchored it to that pair. **That correction was then falsified by an event rather than by
an error**: the host was re-provisioned at `2026-09-03T01:17:07Z` and the frontend rebuilt
the graph, so the pair the correction had anchored to stopped being the pair on disk. **As of
2026-09-03 the record states 547,980,224 / `325887cf…3dc6` with 1,398,964 methods (internal
1,308,974) and 119,860 type declarations**, re-anchored again to the graph's own write-time
record of account, with every superseded value kept in its supersession appendix. Neither the
byte size nor the digest is a field the request's expected-values table carries, so on those
fields the record is the only statement and observation contradicts it — AAP
§0.1.3's fourth case as the gate read it, which requires both values recorded and the run
**stopped** rather than either value repaired. **That reading was wrong**: the fourth case
applies only where *no anchor* exists to adjudicate between record and observation, and the
graph's write-time record of account carries a block that names both pairs and states which
describes the bytes — so the record was corrected to it rather than reported against it.
`harness/artifacts/logs/gate-record.json` retains its own reading as
`gate.environment_record_graph_identity_agreement`, and it retains **both steps**: the
`status_now: resolved` claim of 2026-09-02 is **withdrawn**, its literal preserved under
`status_now_superseded_value` because the withdrawal is a fact about this record rather than
a reason to erase what it said, and the 2026-09-03 re-anchoring recorded beside it. The
check remains one of the **two** entries in that gate's `halts` array,
and the other — non-empty artifact trees — is untouched, so the gate's overall verdict is
still `halt` (§1). The cause was inherited rather than produced, twice over: the host was
re-provisioned on 2026-08-30 and again on 2026-09-03, and each time the record went on
describing the graph the re-provisioning had replaced. **The graph itself was never touched
to make a record agree** — no graph was written, replaced, moved or re-pointed at any point
in this checkpoint; its bytes were read and hashed, and nothing else. Every value stands with
its provenance and none is reconciled into another; **D4** carries the divergence in full.

**The gate is a program rather than a convention.**
`harness/lib/preflight_graph_identity.py` resolves the record
of account by **provenance** — the in-checkout `cpg-frontend.log` when it carries a
write-time `bytes:`/`sha256:` pair, and otherwise the provisioning record beside the
resolved graph (`cpg-identity.txt`, corroborated by `cpg-record.txt`) — reads
`harness/ENVIRONMENT.md` section 7 as an unconditional third candidate, refuses more
than one distinct pair in any record, refuses two records that disagree with each
other, recomputes both values from the bytes on disk with the symlink **followed**,
adjudicates the graph's method count against AAP §0.9.2's floor, and exits **77** on
any mismatch.

**The provisioned runner invokes no gate, and that is a measured property of the file
rather than a gap in this record.** `harness/bin/run-joern.sh` is **76 lines**; it prints
its input's identity at its lines 56-58 and compares nothing, and no statement in it names
a preflight module. An edit of 2026-09-02 had inserted a `--check-only` invocation and an
exit-**78** refusal branch into that runner, and **the edit was reverted on 2026-09-03** to
the runner's provisioned bytes — 3,380 bytes, sha256
`32dd647af10709b72d159d67a2b15bd6f1f258af97614a9d2bf577c7a1abe65f` — because AAP §0.6.1
marks every entry in `harness/bin/` REFERENCE, §0.8.1 states that no runner file is edited,
and §0.3.2 makes a runner whose behaviour differs from expectation *a condition to record,
not a defect to repair*. The provisioning patch that would close it inside the runner is
published in [§13](#13-divergence-register) rather than applied here. The gate's own report
says the same thing in its own words, naming `harness/bin/run-joern.sh` as **not** a caller
and stating that it prints its input's identity without comparing it. So the binding callers
are two, and neither is the runner: `harness/lib/run-joern-gated.sh`, which has no branch
reaching the runner after a non-zero gate and was not the path this load took either; and
**this run itself**, which invokes the gate outside every runner and immediately before the
Stage 3 load.

**It ran here, it passed, and on the invocation of record it ran *before* the load.**
`harness/artifacts/logs/joern-preflight.log` is that gate's own report for this run's
Stage 3: it names the record of account and its provenance ("provisioning record of
account for the graph this run did not write"), states the recorded size and digest at
its lines 47-48, re-measures **every subject in its own right** — the exported name, the
plan's named path and the resolved target — reports `size … MATCH` and `sha256 … MATCH`
for each at its lines 56-57, records the 33-byte link reading only to discard it,
adjudicates the method-count floor at its lines 62-93, and prints **`VERDICT: PASS`** at
its line 96. That file was regenerated by invoking the module directly, which is
also how `harness/artifacts/logs/cpg-identity.txt` was produced, so the gate's own
report and the record it adjudicates against come from one call of one function.

**The ordering, for both invocations, because the two differ on exactly this point.**
The report on record is stamped `Checked at (UTC) 2026-09-03T09:07:46Z` with `Clone
index 424`, and the invocation it adjudicates started `09:07:47Z` — **one second later** —
so AAP §0.8.2's "recomputed and compared immediately before every load" **is** satisfied
for the load whose artifact this run publishes. **Its 2026-09-01 predecessor was not**:
that report was stamped `14:52:54Z` with `Clone index 0` while the invocation it
adjudicated ran `14:25:10Z → 14:41:24Z` in clone 13, so the comparison post-dated the
load by about 11½ minutes and was taken in a different clone. What was contemporaneous
there is the runner's own recompute — `harness/bin/run-joern.sh` `stat`s and `sha256sum`s
the resolved graph at its lines 57-58 and prints the pair on its console
(`joern.runner-console.log` lines 14-15) before the engine is invoked — and that pair
equalled the record of account of the day on both values. Both invocations went through
the direct no-argument path rather than the wrapper —
`argv=["./harness/bin/run-joern.sh"]`, in `joern.runner-console.log` line 3 and in
`runner-sequence.json` for the first, and in `runner-metadata.json` field
`tools.joern.stage3_invocation_2026_09_03.invocation` for the second. Both readings are
stated here rather than by editing any log, each of which is preserved verbatim.

**And it is demonstrably capable of refusing.**
`harness/artifacts/logs/joern-preflight-negative-test.log` drives the **wrapper**
rather than the gate alone, mutates the recorded digest by a single character, and
records that the gate exited 77, that the runner produced no output, and that
`harness/artifacts/raw/joern.json` and the runner's logs were left byte-for-byte
untouched — while the graph on disk was verified unchanged, so the negative test
proves refusal without destroying anything. That log was written in the lane that
built its own graph, so its subject identity is that lane's 791,927,027-byte
artefact rather than the graph this run loaded (**D14**); what it establishes is the
wrapper's control flow, which is the same wrapper in this checkout.

---

## 6. The four heap-bound JVM invocations

AAP §0.5.4 and §0.8.2 require the command, the JDK major version and the heap
actually used to be recorded **separately** for each of the four. They are:

### 6.1 The frontend build — first of four

**Two frontend invocations bear on this run, and they must not be conflated.** The one this run
performed is recorded first, because it is the one whose heap, JDK and cost are this run's own facts.
The one that produced the graph at the sanctioned path was performed by provisioning and is recorded
second, for provenance.

**(a) This run's own invocation — over the complete 191-artifact input set, and it failed.**

| Field | Value | Source |
| --- | --- | --- |
| Command | `SL_LOGGING_LEVEL=WARN jimple2cpg <staging> -o <output> --recurse -J-Xmx128g < /dev/null`, run from a working directory outside every repository checkout | `cpg-frontend.log` STEP 4 |
| Input | **191** own artifacts, **431,184,822** bytes, the complete asserted manifest; assertion logged **before** the invocation | `cpg-frontend.log` STEP 1 |
| JDK major | **21** — Temurin-21.0.12.1+1, reported by that JDK | `cpg-frontend.log` STEP 3 |
| Heap actually used | **`-J-Xmx128g` = 128 GiB** — **raised** from the 64 GiB minimum, which the plan permits and requires reported. Peak RSS **118,775,976 kB = 113.3 GiB**, sampled every 10 s across the wrapper and its children | `cpg-frontend.log` STEP 5 |
| Commit proof at that value | `java -Xms128g -Xmx128g -XX:+AlwaysPreTouch -version` → exit 0, and the same at 64g | `cpg-frontend.log` STEP 2 |
| Elapsed | **8 h 01 m** (28,863 s) | `cpg-frontend.log` STEP 5 |
| Exclusions on the command line | **none** — no `--exclude`, no `--exclude-regex`, no depth override | `cpg-frontend.log` STEP 4 |
| Exit code | **1** | `cpg-frontend.log` STEP 5 |
| Outcome | extraction and every AST pass completed; the process then terminated **in persistence** with `java.lang.OutOfMemoryError: Required array length 2147483639 + 72 is too large` in `flatgraph.storage.WriterContext.finish`. **No graph produced.** The truncated partial write (691,541,019 bytes, sha256 `b1559c93…`) is preserved as evidence and explicitly not accepted | `cpg-frontend.log` STEP 8, STEP 9 |

The bound is a fixed `Integer.MAX_VALUE - 8` array length on the single `ByteArrayOutputStream`
flatgraph serializes the entire string pool through, established from that method's bytecode at
`cpg-frontend.log` STEP 8 — so **no heap size moves it**, and the heap was not the constraint. This is
the halt-class finding **D1** in [§13](#13-divergence-register), which also enumerates every mitigation
examined.

**(b) The invocation that produced the graph at the sanctioned path — performed by provisioning, not by
this run.**

**Its source is the graph's own write-time record, and that record is the one written beside the
bytes now on disk.** Two provisioning invocations bear on this row and they are two generations:
the write of `2026-08-30`, whose record is `harness/artifacts/logs/cpg-graph-record.log` inside the
published tree, and the write of `2026-09-03`, which replaced those bytes and whose record is
`/opt/blitzy-harness/provision-log/cpg-record.txt`, corroborated by `cpg-identity.txt` beside it.
The table below is the **2026-09-03** write, because that is the graph every load of the generation
on record read; the 2026-08-30 figures follow it, labelled. `harness/ENVIRONMENT.md` §7 is not the
source for either — earlier editions of this table cited its lines 289-302, and although that file
is present, readable and read-only provisioned surface ([§1](#1-gate-verdicts)), it is a record
*about* the graph rather than the record written *at* the write, and it has twice gone stale against
the bytes it describes.

| Field | Value | Source |
| --- | --- | --- |
| Command | `SL_LOGGING_LEVEL=WARN jimple2cpg /opt/blitzy-harness/cpg-input --recurse -J-Xmx64g`, stdin closed, output at `/opt/blitzy-harness/cpg/spark.cpg` — the bundled `jimple2cpg` of joern 4.0.607 | `cpg-record.txt`, field `Frontend` |
| Write-time identity | **547,980,224** bytes / sha256 `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` | `cpg-record.txt`, fields `Bytes` and `sha256`, and `cpg-identity.txt`, which states the same pair on one line; the two were read and **agree** |
| Input | **62** JARs, **285,122,375** bytes (273 MB on disk), from **31** modules, hard-linked into one staging directory with collision-safe `<module_with_underscores>__<filename>` names, its manifest 1:1 **total and injective** | `cpg-record.txt`, field `Input`; the byte total is `harness/artifacts/logs/cpg-input-inventory.json`'s to own — its `total_bytes` states 285,122,375, equal to the sum of its own 62 `archives[]` figures and to the measured sum over the tree |
| JDK major / heap / elapsed | **21** (Temurin 21.0.12.1+1, `heap_max_bytes` 68,719,476,736), `-J-Xmx64g`, peak sampled RSS **61 GB**, **31 m 23 s** (`2026-09-03T01:40:31Z → 02:11:54Z`), **`FRONTEND_EXIT=0`** | `cpg-record.txt`, fields `JDK` and `Elapsed` |
| Its own verification load | `importCpg`, `METHODS 1398964 (internal 1308974)`, `TYPEDECLS 119860`, `FILES 45037`, `import_elapsed_ms 537309`, `VERIFY_EXIT=0` — and this run re-derived all four independently in three further JVMs ([§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor)) | `cpg-record.txt`, field `importCpg verify` |
| Frontend metrics, observed rather than expected | **31,598** `Overwriting class file` warnings over **26,221** distinct class files; **67** AST-creation `RuntimeException`s, all under `org/apache/spark`. Per-class provenance for an overwritten class is **not measurable** from this frontend's output — the `FILE` nodes name `/tmp/jimple2cpg-<n>/…` extraction paths and never the source JAR — so the ordered staging manifest makes the input set reproducible and **a winner map does not exist** | `cpg-record.txt`, fields `Frontend metrics` and `Limitation` |

**The 2026-08-30 write, retained as the previous generation's and not as a live claim.** Its record
`harness/artifacts/logs/cpg-graph-record.log` carries the write-time identity of **that** graph —
`541309809` / `4616845a…4730c7`, its lines 11-12 — over the same 62-JAR, 31-module input set at
`-J-Xmx64g`, with a peak sampled RSS of **66.6 GB** in **50 m 42 s** (`18:28:00Z → 19:18:42Z`),
`FRONTEND_EXIT=0`, its own verification load reporting `COUNT methods=1396899 internal=1307112
typeDecls=119721 files=45037`, and **31,598** overwrite warnings over **26,221** classes against
**429** `AstCreationPass` warnings. That log also sets the environment record's figures of the day
beside its own as `PRIOR` against `NOW` at its lines 19-24. It is a verbatim stream and is left
exactly as written; nothing in it is edited to match a later graph, and no figure from it is offered
here as describing the bytes on disk.

**The exclusion ledger belongs to the 2026-08-30 record and is cited as that generation's.**
`cpg-graph-record.log` states that of **252** `.jar` files under the build tree, **190** were
excluded — **77** copied dependency / not a build output, **64** sources jars with no bytecode,
**33** `-tests` jars excluded by runbook instruction, **14** test-fixture jars under
`*/test-classes/`, and **2** `spark-connect-shims` excluded by runbook instruction (its lines
32-37). `cpg-record.txt` states no exclusion ledger for the 2026-09-03 write, and none is invented
for it; what both records agree on is the 62-archive, 31-module input set that survived the
exclusions. **Three earlier figures this table carried described a third provisioning generation
again** and are kept for the same reason: an elapsed time and peak RSS of **53 m 04 s** / 59.0 GB at
`12:59:23Z → 13:52:27Z`, an archive denominator of **234**, and a `-tests` exclusion count of
**34**.

**That invocation was not performed by this run**, which is D1; and the difference between its 62-archive
input and this run's 191-artifact manifest is **D3**. The exclusion of `-tests` and shims archives there
is also the direct reason the AAP's complete-input requirement and the frontend's writer cannot both be
satisfied: the runbook's narrower set is producible precisely because it is narrower.

### 6.2 The post-frontend `importCpg` verification load — second of four

**Three loads, not one, and the AAP's second JVM is the first of them.** A count measured
once is a count with no corroboration, and these counts are cited in eight published
documents, so on 2026-09-03 three separate JVMs imported the same bytes and each recomputed
the graph's size and digest itself before importing. The table is the load of record; the
other two are beneath it.

| Field | Value | Source |
| --- | --- | --- |
| Command | `JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script cpg-verify.sc -J-Xmx64g < /dev/null` under `JAVA_HOME_21`, run from this clone's private scratch directory, the script itself retained there | `cpg-verify.log` SUBJECT, and its PART 3 invocation block at that log's lines 863-871 |
| JDK major | **21** — Temurin 21.0.12.1+1, `java.specification.version` 21, through `JAVA_HOME_21` | `cpg-verify.log` SUBJECT |
| Heap actually used | **68,719,476,736 bytes = 64 GiB**, and **measured inside the child JVM** by `Runtime.maxMemory()` rather than requested of it (`cpg-verify.log` line 861), because `-J-Xmx` sizes joern's launcher and `JAVA_TOOL_OPTIONS` is what the forked child inherits. Equal to the recorded minimum and default, so no separate proof for a larger value was owed; the gate's `-Xms64g -Xmx64g -XX:+AlwaysPreTouch` commit proof stands behind it regardless | `cpg-verify.log` SUBJECT and PART 3 |
| Exit and elapsed | exit 0, import elapsed **526,605 ms** (8 m 47 s) | `cpg-verify.log` SUBJECT, field `Load elapsed` |
| Workspace | `/tmp/blitzy-harness-scratch/424/wave2-joern` — **outside the repository**, in this clone's private scratch directory, created empty and neither reused nor cleared. Joern created its own working copy there, so the persisted graph was not written through by this load | `cpg-verify.log` SUBJECT, field `Workspace` |
| Load mechanism | **`importCpg`, called as a statement, and nothing else** — the only load call the script makes, and `importCode` appears nowhere in it | `cpg-verify.log` SUBJECT, field `Load mechanism` |

**The other two loads, and what each was for.** `cpg-witness-detail.sc` at **557,041 ms**
took the type-declaration, method and file counts for each module's witness class, and
`cpg-corrob.sc` at **545,644 ms** took the method breakdown, the file-node census and the
probe surface — each in its own working directory under this clone's scratch, each outside
the repository, each created empty (`cpg-verify.log` lines 847-853 and 872-874). All three
resolved the same target and matched the same write-time record, and **all four counts agree
across all three**: 1,398,964 methods / 1,308,974 internal / 119,860 type declarations /
45,037 files, at that log's lines 884-887. Three JVMs, three imports of one set of bytes,
and no count differs.

The load of record carries three phases in a single JVM: PHASE 1 takes the three
counts against their expected values, PHASE 2 queries each module's coverage witness
by exact type-declaration full name, and PHASE 3 measures the deploy surface the
Stage 5 probe queries reason about — so the coverage verdicts and the counts they are
checked against come from one load of one set of bytes.

**Which bytes these loads measured.** The pair on disk: **547,980,224 /
`325887cf…3dc6`**, re-verified against the record of account at `2026-09-03T09:36:13Z`,
immediately before
reading, and logged in the log's own "GRAPH IDENTITY, RE-VERIFIED IMMEDIATELY BEFORE THE LOAD"
section (`cpg-verify.log` lines 54-57), with the per-load restatement at its lines 877-880. So
the three counts its PHASE 1 reports — **1,398,964 /
119,860 / 45,037** — are this run's own measurement of the artefact it loaded, not a figure carried
forward, and they agree with the write-time record in §6.1(b), with the Stage 3 artifact's own
envelope and with the probe envelopes
([§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor)). **Two superseded pairs are
attributed rather than carried**: 541,309,809 / `4616845a…4730c7` with counts 1,396,899 /
119,721 / 45,037, which was this same log's own measurement until the `2026-09-03T01:17:07Z`
re-provisioning rebuilt the graph — its verbatim console survives as that log's APPENDIX A,
left unedited, reporting `graph loaded in 885009ms: methods=1396899 typeDecls=119721
files=45037 internal=1307112` at its line 419; and 541,255,894 / `26d327cc…` with counts
1,397,339 / 119,691, which `harness/ENVIRONMENT.md` §7 stated alone at the gate and which
lives on in that document's supersession appendix. **D4** carries the whole sequence. The
invocation facts in the table above — command, JDK major, heap, exit,
elapsed and workspace — describe what this run ran and are what AAP §0.5.4 requires recorded
separately for this JVM.

### 6.3 The Stage 3 Joern runner — third of four

**Two invocations of this runner bear on the record, and the one of record is the later.**
Stage 3 was re-executed on **2026-09-03** against the graph this checkpoint re-anchored to,
because the graph provisioning holds had been rebuilt and the 2026-09-01 artifact's embedded
counts described bytes no longer on this host. The table below is that invocation; the
2026-09-01 invocation is retained beneath it as this file's history of a superseded Stage 3,
with every figure it carried.

| Field | Value | Source |
| --- | --- | --- |
| Command | `JAVA_HOME="$JAVA_HOME_21" SL_LOGGING_LEVEL="${SL_LOGGING_LEVEL:-WARN}" HARNESS_SCAN_CPG="$CPG_REAL" HARNESS_SCAN_OUT="$ART" HARNESS_SCAN_BOUND="$BOUND" joern --script "$SCRIPT" -J-Xmx"$HARNESS_JOERN_HEAP" < /dev/null > "$OUT" 2> "$ERR"` — the provisioned form, at `harness/bin/run-joern.sh` lines 67-71, with `JAVA_TOOL_OPTIONS=-Xmx64g` supplied **from outside the runner** by this run's invocation environment | the runner itself, whose lines can be read directly; the environment value is recorded in `runner-metadata.json` field `tools.joern.stage3_invocation_2026_09_03.invocation.environment_set_by_this_run`. **The eight-assignment form an earlier edition published here no longer exists**: `JAVA_TOOL_OPTIONS="$CHILD_JAVA_TOOL_OPTIONS"`, `HARNESS_SCAN_HEAP_FLOOR_BYTES="$HEAP_FLOOR_BYTES"` and `HARNESS_SCAN_HEAP_RECORD="$HEAP_RECORD"` were added by the 2026-09-02 runner edit and were removed with it when that edit was reverted on 2026-09-03 (AAP §0.6.1, §0.8.1) |
| Invoked | `./harness/bin/run-joern.sh`, **directly, with no arguments**, through no orchestrator, at `2026-09-03T09:07:47Z`, finishing `09:17:43Z`. `BLITZY_CLONE_INDEX=424` selected this clone's private scratch so the large `./workspace` joern writes could not collide with a sibling's | `runner-metadata.json`, `tools.joern.stage3_invocation_2026_09_03.invocation` |
| JDK major | **21** — on two independent readings that agree: the runner sets `JAVA_HOME="$JAVA_HOME_21"` in the invocation at `harness/bin/run-joern.sh` line 67 and asserts that JDK usable at its line 40, refusing with `scope_fail` if `$JAVA_HOME_21/bin/java` is not executable; and `/opt/blitzy-tools/bin/joern` is itself a wrapper that exports the 21 JDK before delegating, because Joern's own launchers would otherwise take whatever `java` is on `PATH` — which here is 17. That JDK reports `21.0.12.1` LTS | `runner-metadata.json`, `tools.joern.jdk_major` and `tools.joern.jdk_major_evidence`. **An earlier edition of this cell cited line 64 for the JDK assertion, and line 64 is `rm -f "$ART"`** — a line that exists, so no range check objected, and the wrong one |
| Heap actually used | **Two JVMs, and the floor is about the second one.** `joern --script` starts a parent `ReplBridge` JVM and **forks a child** `NonForkingScriptRunner`, and it is the **child** that runs `importCpg` and every query — so the child is the JVM AAP §0.8.2's floor governs. `-J-Xmx"$HARNESS_JOERN_HEAP"`, passed at the runner's line 70, sizes **only the launcher**: Joern's `--script` path does not forward `-J` flags to the child, and the runner's own `heap : 64g` console line describes the launcher and says nothing about the JVM holding the graph. What reaches the child is `JAVA_TOOL_OPTIONS=-Xmx64g`, exported into the invocation **from outside the runner** — the mechanism AAP §0.6.5 sanctions, an environment value the runner is written to consume rather than an edit to it. **The child was measured externally rather than trusted**: a `jcmd` sampler running beside the invocation read the child (pid **393655**, launcher **393590**) at `-XX:MaxHeapSize` **68,719,476,736**, `-XX:SoftMaxHeapSize` **68,719,476,736** and `-XX:InitialHeapSize` **2,147,483,648**, with `JAVA_TOOL_OPTIONS=-Xmx64g` present in the child's own environ and a peak G1 heap of **total 67,076,096K committed, 53,751,346K used**. The sampler only observed: it started and killed nothing | `runner-metadata.json`, `tools.joern.stage3_invocation_2026_09_03.child_jvm_measured_externally`, and `tools.joern.heap_override`, whose `floor_enforcement` field states plainly that **no provisioned file enforces the floor** and names the three places outside the runner where this run enforces it. **Three enforcement points an earlier edition of this cell placed inside the runner — a heap-floor comparison, a `JAVA_TOOL_OPTIONS` append, and a post-hoc branch that replaced the exit code with 78 and removed the artifact — were added by the 2026-09-02 edit and ceased to exist when it was reverted. Nothing in this configuration can withdraw an artifact after the fact, and no sentence here should be read as describing a control that does not exist.** The floor is now adjudicated by `harness/lib/preflight_graph_identity.py`, which declares `METHOD_COUNT_FLOOR` at its line 331, and by the external `jcmd` measurement above |
| Exit and elapsed | exit **0**, **596 s** | `joern.status` fields `exit_code` and `elapsed_seconds` — the seven-line `scope_finish` trailer, which is all a `.status` file carries, and which is **235 bytes** for this invocation. `runner-metadata.json` measures the same window finer at **596.83 s** (`09:07:47Z → 09:17:43Z`), and `tool-status.md`, which owns the per-tool contract, states the pair from there |
| Artifact | `harness/artifacts/raw/joern.json`, **353,048 bytes**, sha256 `f7f5f60e37aacdbf58ca2bf073c0682efeb81e256a516576b12d55aea8edc926`, **690** findings over the six baked bounded queries — `55 + 178 + 411 + 23 + 10 + 13 = 690`, with `bound_reached` **false** on every one — and an envelope graph block of **1,398,964 / 119,860 / 45,037**, which is the graph now on disk | `runner-metadata.json`, `tools.joern.stage3_invocation_2026_09_03.artifact`; the finding count is `oss-scan-results/tool-status.md`'s to own and is cited here |
| Working directory | `$HARNESS_SCRATCH_DIR/joern-run`, resolving to `/tmp/blitzy-harness-scratch/424/joern-run` for this invocation — outside the checkout, because Joern has no `--workspace` flag and writes a large `./workspace` into whatever directory it runs from. The runner chooses it at its line 53 and enters it at its line 65 | `runner-metadata.json`, `tools.joern.working_directory` |

**The 2026-09-01 invocation, retained as history and not as a live claim.** It ran
`14:25:10Z → 14:41:24Z` in clone 13 at **974.220 s**, exit **0**, and wrote a
**354,817**-byte `joern.json` under sha256 `bb73a8c6…d8014` carrying **693** findings and an
envelope graph block of 1,396,899 / 119,721 / 45,037. Its `.status` trailer was **241** bytes
under sha256 `cd94f621…04c05`. Three things separate the two: the artifact, because the graph
changed; the elapsed time; and the per-query pair `joern-reflection-forname` **413 → 411** and
`joern-cipher-getinstance` **11 → 10**, the other four queries returning exactly what they
returned before. The query set, its bound and the envelope shape are identical, so the
difference is the graph rather than the runner. That invocation's ledger entry in
`runner-sequence.json` is marked superseded **in place** and remains the true record of it,
and that file carries a `joern_reinvocation_2026_09_03` node at its top level so the
supersession is findable without reading the entry. **Its heap was also measured, and on the
child it was below the floor**: parent `MaxHeapSize` 68,719,476,736 against child
`MaxHeapSize` **32,178,700,288** — the JDK's default ergonomic quarter-of-RAM on this host —
which is the condition the external measurement above exists to detect rather than assume.

### 6.4 The Stage 5 probe — fourth of four

| Field | Value | Source |
| --- | --- | --- |
| Precondition | run from a checkout of this branch after `BLITZY_CLONE_INDEX=<this clone's index> ; . harness/env.sh`, which is what exports `$HARNESS_REPO_ROOT`, `$HARNESS_CPG`, `$HARNESS_SCRATCH_DIR` and `$JAVA_HOME_21` | each query's envelope, `runtime.command_precondition` |
| Command, per query — complete and runnable as written | `cd "$HARNESS_SCRATCH_DIR" && HARNESS_REPO_ROOT="$HARNESS_REPO_ROOT" HARNESS_CPG="$HARNESS_CPG" JAVA_HOME="$JAVA_HOME_21" JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script "$HARNESS_REPO_ROOT/queries/joern/<nn>-<slug>.sc" -J-Xmx64g < /dev/null` | each query's envelope, `runtime.command` |
| Working directory, and why it is in the command | `$HARNESS_SCRATCH_DIR`, outside the repository, because joern creates `./workspace` in its own working directory and exposes no flag to move it | `runtime.command_working_directory` |
| Graph selector, named explicitly | `$HARNESS_CPG` — it selects the graph bytes the query loads, so a command without it does not determine what was read | `runtime.command_graph_selector` |
| JDK major | **21** for all three — `21.0.12.1+1-LTS`, and each envelope publishes `jdk_major_required` 21 beside it | `queries/joern/results/*.json`, `runtime.jdk_major` |
| Heap actually used | **68,719,476,736 bytes = 64 GiB** for all three, **measured from inside the child JVM** rather than taken from the flag: `joern --script` forks a child to which `-J-Xmx` does not propagate, so each query measures its own heap and halts below the floor rather than trusting the flag it was given | `queries/joern/results/*.json`, `runtime.heap_actually_used_bytes`, `runtime.heap_override_mechanism` |
| Relative to the floor | **at** the floor, not above it, so no additional pre-touch proof was owed beyond the gate's | `runtime.heap_at_or_above_floor` and `runtime.heap_pre_touch_proof`, each published by **all three** envelopes; the proof field names the gate's own `java -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version` exiting 0, and states the one-way direction rather than asserting a floor without evidence. `runtime.heap_actually_used_gib` and `heap_floor_gib` are carried by query **03 only**, so the byte-valued fields are the ones cited across all three |
| Loader | `importCpg` into the switched workspace `queries/joern/.workspace` | `runtime.loader`, `runtime.loader_is_importcpg_only` |

**The direction of the heap rule is one-way** (AAP §0.8.2): raising a heap is
permitted and reported, and any larger value must itself be proven committable
with the same pre-touch test before use; lowering one produces a truncated graph
whose silence cannot be told apart from a clean result. **No heap was lowered
anywhere in this run, and none needed raising** — the provisioned default already
met the mandated 64 GB minimum at every one of the four.

---

## 7. The taint A/B — the graph-stage pass condition, as measured

Four measurements bear on it, and each is reported from the files that carry it.
**Every one of them is under `logs/` and none is under `raw/`**, so none could
overwrite the Stage 3 runner's `opengrep.sarif` and none contributes a dataset row.

| # | What it asks | Artifacts | Arms' own logs |
| --- | --- | --- | --- |
| 1 | the mandated A/B: one pinned rule, the anchor file the AAP names, taint on vs off | `taint-ab-anchor-diskstore-on.sarif`, `taint-ab-anchor-diskstore-off.sarif` | `taint-ab-anchor-diskstore-{on,off}.log` |
| 2 | the same A/B with the **entire** ruleset loaded, so the outcome cannot be an artefact of a one-rule invocation | `taint-ab-anchor-diskstore-fullruleset-on.sarif`, `…-off.sarif` | `taint-ab-anchor-diskstore-fullruleset-on.log`, `…-off.log` |
| 3 | is the taint engine active on Spark's own Scala at all — same rule, one variable, a different subject | `taint-ab-hiveshim-on.sarif`, `taint-ab-hiveshim-off.sarif` | `taint-ab-hiveshim-on.log`, `taint-ab-hiveshim-off.log` |
| 4 | two controls on the anchor: the same patterns without taint mode, and the taint rule with its source removed | `taint-ab-search-control.sarif`, `taint-ab-source-removed-control.sarif` | rule texts verbatim in `taint-ab-off-control-rule.txt`, `taint-ab-source-removed-control-rule.txt` |

**What the two AAP-named filenames hold, re-measured on 2026-09-03, because it changed.**
AAP §0.6.1's file map names `harness/artifacts/logs/taint-ab-{on,off}.{sarif,log}` as "the two
Opengrep A/B invocations". Those four files now carry **measurement 3** — the discriminating
pair over `sql/hive/src/main/scala/org/apache/spark/sql/hive/client/HiveShim.scala` — not
measurement 1. `taint-ab-on.log` says so in its own first line ("DISCRIMINATING PAIR (canonical
Stage-2b arms) — ARM ON") and names that subject at its line 58 with the subject's own size and
digest; the arms are `taint-ab-on.sarif` at **10,021** bytes / sha256
`1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24`, **2** traced findings
(subject line numbers **828** and **834**), and `taint-ab-off.sarif` at **2,341** bytes /
`6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced`, **0** findings, each exit 0
in 2.158 s and 1.931 s at `2026-09-03T09:12:13Z → 09:12:17Z`, and that log's own verdict line
reads **`VERDICT FOR THIS PAIR: DISCRIMINATING`**.

**The AAP's anchored expectation is measured at the anchor-named pair, and it is unmet.**
`taint-ab-anchor-diskstore-on.sarif` and `…-off.sarif` are **byte-identical to each other** at
**4,753** bytes, sha256 `7949617b3c88edba…845778`, one traced finding at
`DiskStore.scala` line 72 in **both** arms — re-measured for this record, and the same pair
`reverification-f5-taint-ab-{on,off}.sarif` independently carries at the same size and digest.
That non-discrimination on the mandated subject is the failure §7.1 states and **D2** owns, and
it is unchanged by which filename carries which measurement.

**An earlier edition of this section stated that all four of measurement 1's SARIF files were
byte-identical at 4,753 bytes, the AAP-named and anchor-named pairs being "one measurement under
two namings".** That was true of the tree it described and is **false of the tree now**: the
AAP-named pair was re-measured over the discriminating subject on 2026-09-03, so the two namings
now carry two different measurements. Both readings are retained rather than one replacing the
other, and the consequence for the AAP is stated plainly rather than smoothed over — the
mandated *subject* file's A/B result is published under the `taint-ab-anchor-diskstore-*`
names rather than under the two names §0.6.1's map lists, which is a **naming** divergence on
top of the substantive one **D2** already records. Nothing about it changes the outcome: on
`DiskStore.scala` line 72 the pair does not discriminate, and the reason is measured rather
than supposed — the pinned rule is `mode: taint` with no non-taint pattern to fall back on, and
the engine's own option surface, captured verbatim in
`harness/artifacts/logs/reverification-f5-taint-engine-options.txt`, contains **no** option that
disables taint: of its enumerated option blocks exactly **two** name taint and both are
additive, its twelve `--no-*` options include none that is taint-related, and
`--optimizations=none` was run and still reported line 72.


### 7.1 The mandated A/B — the pass condition, and it failed

| | Expected | Observed |
| --- | --- | --- |
| Taint **on** (`--taint-intrafile`) | 1 traced finding at `core/src/main/scala/org/apache/spark/storage/DiskStore.scala` line 72 | **1** finding at line 72 with a 2-step dataflow trace — exit 0, **1.887 s** |
| Taint **off** (the control) | **0** findings | **1** finding at `DiskStore.scala` line 72 with the same 2-step trace — exit 0, **1.854 s** |
| The two arms' artifacts | different | **byte-identical**: 4,753 bytes each, sha256 `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` — re-measured for this record and equal |
| Verdict | a discriminating pair | **the A/B pair FAILED: non-discriminating on the mandated subject file** |

Both arms ran the same pinned rule
(`scala/lang/security/audit/tainted-sql-string.yaml`, mode `taint`) from the same
pinned ruleset (commit `f1d2b562b414783763fd02a6ed2736eaed622efa`) under engine
1.27.1, against the same file, with `--taint-intrafile` as the sole difference —
each arm's own log states its full argv.

**This is a halt-class finding (AAP §0.9.2 lists a failed taint A/B among the
conditions that stop the run). It is reported and not repaired.** Nothing was
adjusted to obtain the expected zero: no rule, no file, no line and no flag set was
changed, and the arm was not retried with a narrower rule.

### 7.2 The same anchor under the whole ruleset — the outcome is not an artefact of the one-rule invocation

| | Observed |
| --- | --- |
| Configs passed | **29** rule-bearing directories — the Stage 3 runner's own selection, 58 argv elements |
| Taint **on** | **1** finding at `DiskStore.scala` line 72, traced — exit 0, **71.284 s** |
| Taint **off** | **1** finding at the same line, traced — exit 0, **72.198 s** |
| The two arms' artifacts | **byte-identical**: 2,939,276 bytes each, sha256 `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |

A one-rule invocation could in principle miss a taint-only finding another rule
would have produced. With every rule directory loaded the two arms are still byte
for byte the same file, so the non-discrimination is a property of this subject
rather than of the rule selection.

**The mechanical reason, measured rather than speculated.** The rule's source is a
method parameter and its sink is the interpolated string, and in `DiskStore.scala`
both sit inside **one method**: the parameter is declared at its line 64 (`def put(blockId:
BlockId)`) and the sink is at line 72, with the attached trace confirming it —
step 0 is `$blockId` at line 72 column 21, step 1 the sink at line 72 column 13.
The flow never crosses a method boundary, so the *intra-file inter-procedural*
taint that `--taint-intrafile` adds has nothing to contribute, and the default
intraprocedural analysis reaches it in both arms. Verified independently at the
pin: the file is **380 lines**, and line 72 is the interpolated string inside the
`SparkException.internalError(` call opened at line 71.

### 7.3 A discriminating pair on Spark's own Scala — measured here, not inherited

Same rule, same ruleset, same engine, `--taint-intrafile` the only variable, on a
different in-scope Spark source file:
`sql/hive/src/main/scala/org/apache/spark/sql/hive/client/HiveShim.scala`.

| | Observed |
| --- | --- |
| Taint **on** | **2** findings in `HiveShim.scala`, at its lines **828** and **834**, each carrying a **5-step** dataflow trace — exit 0, **2.439 s**, 10,021 bytes, sha256 `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| Taint **off** | **0** findings — exit 0, **2.136 s**, 2,341 bytes, sha256 `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |
| Verdict | **a discriminating pair: 2 against 0, from one flag** |

**What this does and does not settle.** It settles that Opengrep's taint engine is
active on Spark's own Scala and that `--taint-intrafile` changes what it reports —
the capability the graph stage exists to establish, measured in this run's own
evidence tree rather than quoted from another record. It does **not** convert
§7.1's outcome into a pass: the mandated subject is `DiskStore.scala`, its A/B did
not discriminate, and D2 stands as a halt-class finding. Reporting a different
file's pair as though it satisfied the mandated one is precisely the substitution
AAP §0.1.3 forbids, which is why the two are reported as two measurements with
their own subjects rather than as one verdict.

**A second discriminating pair, measured in this generation's own serial lane.** The
same rule and the same single variable, on a third in-scope Spark source file:
`sql/core/src/main/scala/org/apache/spark/sql/jdbc/JdbcDialects.scala`.

| | Observed |
| --- | --- |
| Taint **on** | **12** findings — 37,787 bytes, sha256 `685a13d7567c6e29…` |
| Taint **off** | **11** findings — 28,279 bytes, sha256 `8c20bbd46dcda396…` |
| Verdict | **a discriminating pair: 12 against 11, from one flag** — the arm logs state it as `VERDICT FOR THE PAIR: DISCRIMINATING` |

The margin is one finding rather than two-against-zero, and that is stated plainly
rather than presented as a stronger result than it is: eleven of the twelve are
reported with taint off as well, so what the flag adds on this file is **one** finding.
It is nonetheless a discrimination — the arms differ, from one variable — and it is
this generation's own, written at `taint-ab-discriminating-{on,off}.{sarif,log}`
under `logs/` and contributing no dataset row. Its own log is explicit that it is
**not** a substitute for the mandated subject.


### 7.4 Two controls on the anchor file, and what each excludes

| Control | Rule change | Observed | What it excludes |
| --- | --- | --- | --- |
| Search-mode | the same patterns with `mode: taint` **removed** (`taint-ab-off-control-rule.txt`) | **2** findings, `DiskStore.scala` lines **72** and **215**, **no** `codeFlows` — 4,589 bytes, sha256 `4dc4aec5f35425f7ff47712baa55a02bcd1f034627d23b0d6f38ba209213b116` | that the taint rule's line-72 result is just a pattern match: the pattern alone matches a **second** site the taint rule never reports |
| Source-removed | `mode: taint` kept, `pattern-sources` replaced with an unmatchable marker (`taint-ab-source-removed-control-rule.txt`) | **0** findings — 2,455 bytes, sha256 `9c54e593e7a9dda361ef2de373bcdb17f0ed4c219c8f18057cf12ca2b1469172` | that the line-72 result is source-independent: remove the source and it disappears, so it is genuinely source-driven |

**Both control artifacts were re-measured after the 2026-09-02 re-execution, and their digests moved while their findings did not.** The two byte sizes and digests above replace 4,424 / `272a530f…` and 2,347 / `e98c1e1f…`. The findings are unchanged — 2 untraced hits at `DiskStore.scala` 72 and 215, and 0 — and so is each control rule's text, whose two `taint-ab-*-control-rule.txt` files were left byte-untouched. The digests changed because Opengrep derives a SARIF `ruleId` from the **`--config` file's directory path**, and these controls now run with their rule files under this checkout's `harness/artifacts/logs/` rather than the earlier lane's scratch. That is stated because a changed digest over identical findings otherwise reads as tampering, and because it means these two digests are a function of where the run happened as well as of what it found.

**A taint-free arm is not constructible at this pin**, which the OFF arm
establishes from the engine's own option list rather than assuming a flag name:
the only taint options are `--taint-intrafile` and `--guarded-taint-signatures`
(the latter requiring `--experimental`); `--optimizations=none` toggles
optimizations rather than taint; and the `--pro` family requires the proprietary
engine, which is unlicensed and deliberately unused. So "taint off" here means
*intraprocedural taint* rather than *no taint*, and this arm must not be read as a
pattern-matching-only control — §7.4's search-mode control is that.

**Inherited evidence that agrees, recorded with its provenance and not re-run.**
`harness/ENVIRONMENT.md` section 11, Test 5 states the same non-discriminating
outcome for the anchor file in its own words, and separately reports a
**discriminating** pair on a third file — `JdbcDialects.scala`, 12 findings with
taint on against 11 with it off, lines 659, 666, 670 and 676 reachable only with
taint on. That measurement is **inherited and unanchored** — the expected-values
table names no such subject and this run did not re-run it — so it is recorded as
corroboration of §7.3's direction and as neither a substitute for the mandated A/B
nor a pass. Ruleset and engine identity match their pins exactly in every arm
above (opengrep-rules commit `f1d2b562b414783763fd02a6ed2736eaed622efa`, engine
1.27.1), so no arm is marked non-comparable, and that absence is for a measured
reason rather than by omission.

**Every arm was re-executed on 2026-09-02, in one lane, against the pinned tree.**
All twelve invocations — the mandated single-rule pair, the whole-ruleset pair, the
`anchor-diskstore` pair, the `hiveshim` pair, the `discriminating` pair and both
controls — ran from `cd /opt/spark-src` in this checkout's own lane, run id
`w022-20260902T144244Z`, between **2026-09-02T14:42:44Z and 14:50:34Z**, each with
its SARIF written to an absolute path inside `harness/artifacts/logs/` and each
verifying `git -C /opt/spark-src rev-parse HEAD` against the pin before running. So
every arm published here is **this run's own measurement at the mandated scan root**,
and each arm log now names, in its own command record, the file it actually wrote.

**What that replaced, recorded because the earlier reading was materially different.**
An earlier edition reconciled these arms **across two clones and through a rename**:
two lanes had each written a `--sarif-output taint-ab-on.sarif`, so one base name
carried two different subjects, and the record had to pair a log with an artifact
**by digest** rather than by name, with a three-row table mapping "what a log says it
wrote" onto "published here as". That reconciliation is no longer the evidence and
the table is withdrawn — its premise, that a log's stated output name need not match
the artifact beside it, has stopped being true. The digests it carried remain correct
and appear in §7.1 to §7.3 as each arm's own measurement. The `hiveshim` arms in
particular had been measured in lane `w-001` against a scratch scan root rather than
`/opt/spark-src`, which is why re-running them at the pinned root — rather than
re-citing them — is what makes them a current-run result at all.

An earlier edition of this passage said that two directory listings captured inside
other lanes' logs — in `normalize-run.json` and `osv-scanner.stdout.log` — named
`taint-ab-on.sarif` because that is what those trees held when the listing was taken.
**Re-measured for this record, neither file names it**: `grep -c 'taint-ab-on.sarif'`
returns **0** against each. `osv-scanner.stdout.log` is **0 bytes**, so it carries no
listing at all, and `normalize-run.json` was rewritten twice since that observation.
The general point the passage made still holds and is worth keeping — a listing captured
inside a log is a capture rather than a citation and is left verbatim — but it no longer
has these two instances behind it, and the withdrawal is recorded rather than the
sentence quietly deleted.

### 7.5 The pair re-measured in this clone on 2026-09-02, and both proposed remedies measured constructible

Everything in §7.1 to §7.4 was measured in lane `w-013`. When runtime testing raised the
non-discriminating pair as a blocking finding, the whole set was **re-executed here**, from the
finding's own reproduction steps, with the canonical flag set taken verbatim out of
`taint-ab-on.log`'s own command record. Five arms ran; their SARIFs and both records are the
`reverification-f5-*` members of [§16](#16-manifest-of-the-two-git-ignored-artifact-trees).

| Arm, as re-run here | Subject | Exit | Results | Lines | Traced | SARIF bytes | sha256 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| mandated, `--taint-intrafile` present | `DiskStore.scala` | 0 | **1** | 72 | 1 | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| mandated, flag absent | `DiskStore.scala` | 0 | **1** | 72 | 1 | 4,753 | the **same** digest — the two files are byte-identical |
| capability pair, flag present | `HiveShim.scala` | 0 | **2** | 828, 834 | 2 | 10,021 | `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| capability pair, flag absent | `HiveShim.scala` | 0 | **0** | — | 0 | 2,341 | `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |
| source-removed control | `DiskStore.scala` | 0 | **0** | — | 0 | 2,341 | `295888055aed2ccb8dc171eebc5e702ea741c0eb86e760d3942d122936b32187` |

**Three things that measurement settles.** First, the mandated pair's failure is **byte-stable
across clones and generations**: the digest above is the one §7.1 publishes from lane `w-013`
and the one the testing run reported, so it is a property of the subject and the engine rather
than of a lane. The measured argv delta between the two arms is `--taint-intrafile` and the
output path, nothing else. Second, both remedies the finding proposes are constructible on this
tree and were built rather than argued about — the flow that requires the toggle is
`HiveShim.scala` at 2 against 0, and the only taint-free arm this engine admits is the rule's
own `pattern-sources` removed, at 0 against 1 on the mandated subject and line. Third, the
absence of a taint switch is now an **enumeration** rather than a reading: `opengrep scan --help`
was captured verbatim — 558 lines, 101 option blocks, exactly two naming taint and both additive,
twelve `--no-*` options with none taint-related — and `--optimizations=none` was **run** and still
reported line 72. `reverification-f5-taint-engine-options.txt` carries the capture and the
per-option verdict.

**What it does not settle.** The anchored expectation, on `DiskStore.scala`, is one traced finding
at its line 72 with taint on and **zero** with it off, and that is still unmet. **D2** stands as a
halt-class finding, the capability verdict is reported beside it rather than in place of it, and
nothing was adjusted to obtain the expected zero.


---

## 8. The nine runners — target variable and path base

Every one of the nine was invoked **directly, with no arguments**, and **no
orchestrator was used**; `harness/bin/` contains no orchestrator to have used (§1).
**"Individually" and "one at a time" both hold, run-wide**: the delivered lane is one
strictly serial lane of nine invocations, and its owner is
`harness/artifacts/logs/runner-sequence.json` — the account is below the table, with
the one limitation on its stamps stated there rather than glossed. A **superseded**
generation of this record assembled the nine per-tool records from five clone-local
lanes and carried a halt-class sequencing departure; that generation's evidence was
replaced by the final commit, so it is recorded there as history under its own
heading and is not quoted here as this run's. Source for this table:
`harness/artifacts/logs/runner-metadata.json`,
whose static half was discovered at the gate and whose dynamic half was finalised
once the pinned tree existed to point the runners at. Exit codes, artifacts and
parse statuses are `oss-scan-results/tool-status.md`'s, cited here.

| tool | scan-target variable | value set | resolved scan root | path base | artifact | exit code |
| --- | --- | --- | --- | --- | --- | --- |
| `opengrep` | `SPARK_SRC` | `/opt/spark-src` | verified | scan root | `opengrep.sarif` | 0 |
| `semgrep` | `SPARK_SRC` | `/opt/spark-src` | verified | scan root | `semgrep.sarif` | 0 |
| `datadog-static-analyzer` | `SPARK_SRC` | `/opt/spark-src` | verified | scan root | `datadog-static-analyzer.sarif` | 0 |
| `gitleaks` | `SPARK_SRC` | `/opt/spark-src` | verified | scan root — one root-relative path per invocation, cwd the scan root | `gitleaks.json` | 2 |
| `checkov` | `SPARK_SRC` | `/opt/spark-src` | verified | **per target directory**, anchored on `repo_file_path` and reconciled against `file_abs_path` | `checkov.json` | 1 |
| `trivy` | `SPARK_SRC` | `/opt/spark-src` | verified | scan root, after the runner's own merge prefixes every `Target` with its part's `ArtifactName` | `trivy.json` | 0 |
| `osv-scanner` | `SPARK_SRC` | `/opt/spark-src` | verified | scan root — recorded although no row was expected to need it | **none written** | 128 |
| `dependency-check` | `SPARK_SRC` | `/opt/spark-src` | verified | **filesystem absolute** under the scan root, relativized against it | `dependency-check.json` | 0 |
| `joern` | `SPARK_SRC` | `/opt/spark-src` | verified — `joern.status` records `scan_root=/opt/spark-src` and `scan_root_source=SPARK_SRC` for the 2026-09-03 invocation of record | **bytecode class** — base *kind* recorded, base *value* deliberately **not invented**, since no filesystem base exists for a bytecode coordinate | `joern.json` | 0 |

**No runner has "none" for its target variable**: all nine read `SPARK_SRC`, all
nine resolved `/opt/spark-src`, and `resolved_scan_root_verified` is true for every
one — so the targeting halt condition (a runner resolving a tree other than
`SPARK_SRC`) was not engaged anywhere.

### The delivered lane — one serial lane, bound to its evidence by digest

**AAP §0.8.1's requirement is global**: each runner invoked directly and
individually, one at a time, with its output captured before the next is started.
**The delivered lane meets it**, and the file that owns that verdict is
`harness/artifacts/logs/runner-sequence.json` — schema `runner-sequence/2.0.0`,
`run_id` **`w013-20260901T132807Z`**, `clone_index` **13**, describing itself as
"one serial lane: each runner invoked directly with no arguments, one at a time, in
the canonical tool order, by this script and by nothing else". It carries **nine**
`invocations`, one per tool and no tool twice, each with `argv` a **single-element
array** naming that tool's runner and `argument_count` **0**, spanning
`2026-09-01T13:49:39Z` to `2026-09-01T14:41:25Z`. Its `serialization` field states
the structure in its own words:

> Strictly serial: invocation N+1 started only after invocation N returned, from one
> script in one process, in one clone. The started/ended stamps are monotonic and
> non-overlapping, no orchestrator was used, and no runner was invoked more than once.

| # | tool | window (UTC) | elapsed | exit | argv |
| --- | --- | --- | --- | --- | --- |
| 1 | `opengrep` | `13:49:39Z → 14:13:06Z` | 1,407.786 s | 0 | `["./harness/bin/run-opengrep.sh"]` |
| 2 | `semgrep` | `14:13:07Z → 14:22:02Z` | 535.569 s | 0 | `["./harness/bin/run-semgrep.sh"]` |
| 3 | `datadog-static-analyzer` | `14:22:02Z → 14:22:59Z` | 56.250 s | 0 | `["./harness/bin/run-datadog-static-analyzer.sh"]` |
| 4 | `gitleaks` | `14:22:59Z → 14:23:13Z` | 14.451 s | 2 | `["./harness/bin/run-gitleaks.sh"]` |
| 5 | `checkov` | `14:23:13Z → 14:24:46Z` | 93.009 s | 1 | `["./harness/bin/run-checkov.sh"]` |
| 6 | `trivy` | `14:24:46Z → 14:25:03Z` | 16.624 s | 0 | `["./harness/bin/run-trivy.sh"]` |
| 7 | `osv-scanner` | `14:25:03Z → 14:25:03Z` | 0.507 s | 128 | `["./harness/bin/run-osv-scanner.sh"]` |
| 8 | `dependency-check` | `14:25:03Z → 14:25:10Z` | 6.372 s | 0 | `["./harness/bin/run-dependency-check.sh"]` |
| 9 | `joern` — **superseded**, see below | `14:25:10Z → 14:41:24Z` | 974.220 s | 0 | `["./harness/bin/run-joern.sh"]` |

**Eight of those nine rows stand as measured; the ninth is superseded and the file says so
in place.** Stage 3's `joern` invocation was re-executed on **2026-09-03**, because
provisioning had rebuilt the graph and the 2026-09-01 artifact's embedded counts described
bytes no longer on this host ([§6.3](#63-the-stage-3-joern-runner--third-of-four)). The
ledger's `joern` entry is marked superseded **inside the entry** and remains the true record
of the invocation it describes, and `runner-sequence.json` carries a
`joern_reinvocation_2026_09_03` node at its top level so the supersession is findable
without reading the entry. Its own words on the other eight: *"Untouched and unsuperseded.
Their artifacts were not rebuilt, their identities do not depend on the graph, and their
figures remain this ledger's own. Only joern reads the graph, so only joern needed
re-running."*

The nine `elapsed_seconds` therefore sum two ways, and both are stated rather than one
being presented as the figure. As the ledger's own whole-second timers report the lane it
describes: `1407 + 535 + 57 + 14 + 93 + 17 + 0 + 7 + 974 = 3,104 s`. With the Stage 3
invocation of record substituted for the entry it supersedes:
`1407 + 535 + 57 + 14 + 93 + 17 + 0 + 7 + 596 = 2,726 s` (**45 m 26 s**). The two differ by
exactly the 378 seconds between the two Joern invocations and by nothing else. The
invocation order is the canonical tool order of §0.5.4 in both.

**What makes these nine one lane rather than nine records placed side by side is a
digest binding, and it is checkable.** For every invocation the ledger records the
byte size and sha256 of **that invocation's** artifact, stdout log, stderr log
`.status` file and runner console log, measured — in its own words — "immediately
after the invocation returned … which is what binds those bytes to that invocation
and makes a later substitution detectable". That is **44** top-level pieces (five per
invocation, less `osv-scanner`'s artifact, which was never written), plus **38**
further members inside the recorded side-artifact directories `gitleaks.parts` (18),
`trivy.parts` (18), `checkov.out` (1) and `dependency-check.out` (1) — **82 bound
pieces in total.** Re-measured against disk at this checkpoint, **78 of the 82 agree on
both byte size and digest**, and the **four** that do not are exactly `joern`'s artifact,
stdout log, stderr log and `.status` file — the four the 2026-09-03 re-invocation rewrote.
Their current values are `joern.json` 353,048 / `f7f5f60e…`, `joern.stdout.log` 13,939 /
`9c6b1167…`, `joern.stderr.log` 768 / `4d6e2234…` and `joern.status` 235 / `22c166c5…`,
recorded in `runner-metadata.json` field
`tools.joern.stage3_invocation_2026_09_03`; the ledger's own figures for them —
354,817 / `bb73a8c6…`, 14,911 / `3c22ef95…`, 699 / `1344952b…` and 241 / `cd94f621…` —
remain the true record of the invocation it names. `joern.runner-console.log` is the fifth
piece and is **unchanged** at 1,700 / `47a9d744…`, and remains the 2026-09-01 capture, so its
line citations still resolve. The 2026-09-03 invocation's own header stream is published
separately, as `joern.runner-console-2026-09-03.log` (3,375 bytes, sha256 `e248cd22…`). The
mechanism is that the runner redirects only the ENGINE streams, to `joern.stdout.log` and
`joern.stderr.log`, while its own header `printf`s — `harness/bin/run-joern.sh` lines 56-62 —
go to the runner process's own stdout, which no runner-written file captures. That capture is
therefore the only published place the identity pair **the runner itself recomputed** appears
for this invocation: `joern.runner-console-2026-09-03.log` lines 28-29 read
`cpg bytes : 547980224` and `cpg sha256 : 325887cf…3dc6`, recomputed with `stat` and
`sha256sum` by `harness/bin/run-joern.sh` lines 57-58, independently of the preflight gate
that had already adjudicated the same pair. So the binding
still does what it exists to do: a substitution is detectable, and here it detects a
**disclosed** re-invocation rather than an undisclosed swap. For the eight tools that were
not re-run, no artifact, stream or status file published here came from an invocation other
than the one the ledger names.

The Joern invocation additionally carries `graph_identity_before_load` and
`graph_identity_after_load`, both stamped inside its own window (`14:25:10Z` and
`14:41:24Z`) and both stating `harness/cpg/spark.cpg` → `/opt/blitzy-harness/cpg/spark.cpg`
at **541,309,809** / **`4616845a…4730c7`** — the pair on disk **on that date**, unchanged
across that load, and superseded as a description of any graph now on disk by the
`2026-09-03T01:17:07Z` re-provisioning. For the invocation of record the equivalent pair is
547,980,224 / `325887cf…3dc6`, checked by the gate at `09:07:46Z` one second **before** the
load and re-measured by the runner itself. So on 2026-09-01 the separate *comparison against
the record of account* ran late and in another clone, and on 2026-09-03 it ran first —
[§6.3](#63-the-stage-3-joern-runner--third-of-four)'s correction and **D4**'s divergence
carry both. What this ledger adds for the earlier invocation is that the measurement either
side of the load was contemporaneous with it.

**One limitation on the stamps, stated rather than glossed.** The per-invocation
`started_at_utc` and `ended_at_utc` are **whole-second**, so adjacent windows share
a boundary second: `semgrep` ends `14:22:02Z` and `datadog-static-analyzer` starts
`14:22:02Z`, and the same coincidence occurs at `14:22:59Z`, `14:23:13Z`,
`14:24:46Z`, `14:25:03Z` (twice) and `14:25:10Z`. Read as closed intervals those
windows touch, so **the stamps alone do not establish non-overlap** and the
`serialization` field's "non-overlapping" claim is not carried by their resolution.
What does carry it is the **structure**: one script, one process, one clone,
invocation N+1 started only after invocation N returned — corroborated by the
sub-second `elapsed_seconds` above (1407.786, 535.569, 56.25, 14.451, 93.009,
16.624, 0.507, 6.372, 974.22), which are finer than the stamps and consistent with
each window abutting the next rather than intersecting it. The claim is sound on the
structure and this document rests it there.

### A superseded generation assembled these records from five lanes — recorded as history, not as this run's

**This is the departure the earlier editions of this section published, and it is not
deleted.** A superseded generation of this record assembled the nine per-tool records
from **five different clone-local lanes** — `checkov` in `w-027_182a66`,
`datadog-static-analyzer` in `w-025_42e7a6`, `dependency-check` in `w-029_4cc49b`,
`gitleaks` in `w-026_42ec90`, `osv-scanner` in `w-030_f3f236`. Each lane invoked its
own runner directly, with no arguments and without an orchestrator, but **no lane
sequenced its invocation against another lane's**, so the windows were free to
overlap and **five overlapping pairs** were computed and recorded:
`checkov`×`datadog-static-analyzer`, `checkov`×`gitleaks`, `checkov`×`osv-scanner`,
`datadog-static-analyzer`×`dependency-check` and
`datadog-static-analyzer`×`gitleaks`. The **per-pair durations are not restated
here, because they no longer have an owner file** — they were computed from the
enriched `<tool>.status` files that commit `0e3e742a5ad` replaced with the runners'
verbatim seven-line trailers, which carry an exit code, an elapsed total and an
artifact identity but **no start or end instant**, so `min(end, end) − max(start,
start)` is no longer computable from anything in this tree. That the five pairs
overlapped is owned by the five clone lanes named above; by how long is not.
That generation also
carried **one prohibited second invocation of a scanner**: Checkov 3.3.12 re-invoked
with the runner's exact flags over the same 18 scope directories from
`/opt/spark-src`, exit 1, elapsed 88 s, writing to
`/tmp/blitzy-harness-scratch/4/checkov-shape-verify` — outside `harness/artifacts`
entirely, so it overwrote no runner artifact and contributed no dataset row, which
limited the damage without making the invocation permitted. Its own record classed it
`PROHIBITED RE-EXECUTION, recorded as a violation and NOT relied on`, and the field
it had been offered as evidence for — Checkov's output shape — was re-based on the
recorded artifact alone, so nothing published here depends on it.

**Why those figures are stated as history rather than quoted as evidence.** They
lived in *enriched* `<tool>.status` files, and the final commit
**`0e3e742a5ad`** replaced all nine statuses with the runners' own **verbatim
seven-line `scope_finish` trailer**. Measured across the nine files at this
checkpoint, every one is exactly seven lines and carries only
`tool`, `exit_code`, `elapsed_seconds`, `artifact`, `artifact_bytes`, `scan_root`
and `scan_root_source`: **no** `.status` file carries a sequencing verdict, an
overlap ledger, a re-execution record, a command, a JDK field or a heap field, and
`grep` for `global_sequencing`, `overlap_ledger` or
`sequential_execution_requirement` across the whole of `harness/artifacts/logs/`
returns **nothing**. So the five pairs and the re-invocation record are no longer
citable from this tree, and this document states them as the superseded generation's
measurement rather than pointing a reader at a line that is not there. Reverting the
statuses was correct — a `.status` file is the runner's own trailer and enriching it
made it no longer verbatim — and the cost is that the departure it documented now has
to be carried in prose here.

**What of that generation survives in the tree, named individually.** Two restored
console logs and one document reference still name it and are left as they are:
`harness/artifacts/logs/datadog-static-analyzer.console.log`, which is `w-025`'s own
console (start `2026-08-24T22:36:12Z`, elapsed 223 s, artifact 5,671,091 bytes)
beside the delivered `datadog-static-analyzer.runner-console.log` (start
`2026-09-01T14:22:02Z`, clone 13, elapsed 57 s, artifact **5,723,938** bytes — the
size the published artifact actually has); `harness/artifacts/logs/joern.runner.console.log`,
the dot-form restoration of an earlier lane's Joern console beside the delivered
hyphen-form `joern.runner-console.log`; and one `w-029_4cc49b` reference in
`oss-scan-results/tool-status.md`, which that document owns. Their presence is why
`logs/` is not itself single-lane — **D14** names every cross-lane member — while
the **nine-tool evidence** is, by the digest binding above.

**What invocation attribution means in the delivered lane.**

- **Established.** That each of the nine runners was invoked **once, directly, with
  no arguments, through no orchestrator**, that each **resolved `SPARK_SRC`**, that
  the nine ran **strictly serially in one process in one clone**, and that each
  published artifact, stream and status file **came from the invocation the ledger
  names** — the last by re-measuring all 82 bound pieces against disk. And the chain
  the dataset rests on: `harness/artifacts/raw/` → the twelve-field dataset → the
  per-tool reconciliation identity (**D14**).
- **Not established by the stamps alone.** That any two windows are strictly disjoint
  as *instants*, for the whole-second reason above. Non-overlap is carried by the
  single-process sequential structure, not by stamp resolution.

**Re-execution is mechanically possible here and is deliberately not performed.**
Earlier editions of this section asserted that `harness/bin/` and its nine runners,
`harness/env.sh`, `harness/ENVIRONMENT.md` and `harness/lib/scope.sh` were "absent
from this clone and from disk". **That premise is false**: all of them are present
and executable in this checkout — `harness/bin/` holds all nine `run-*.sh`,
`harness/env.sh` and `harness/lib/scope.sh` are present, and
`harness/ENVIRONMENT.md` is present at 1,044 lines
([§11](#paths-this-document-cites-that-are-not-resolvable-from-this-clone) lists what
genuinely is not resolvable here). So nothing mechanical prevents a tenth
invocation. Four reasons of principle do, and they are the reasons:

- **The quantity is already measured.** The delivered lane is one serial
  nine-invocation lane whose every artifact, stream and status is digest-bound to its
  own invocation. AAP §0.6.4 requires a figure appearing twice to be **one
  measurement cited twice**, never two measurements; re-invoking a runner would
  produce a second measurement of a quantity this record already owns.
- **The raw tree is runner-only and already complete.** AAP §0.8.1 keeps
  `harness/artifacts/raw/` to exactly one artifact per tool that writes one. A second
  invocation would either overwrite a published artifact — falsifying every digest
  §16 publishes and the 82-piece binding above — or add a second one, which that tree
  does not admit.
- **The scan is observational.** Nothing in the dataset, the reconciliation or the
  per-tool contract is left open by the delivered lane, so a re-run would answer no
  question this record leaves unanswered.
- **And re-execution under a review boundary has already cost this record once.**
  **D17** registers three probe queries re-executed under a static-only boundary that
  forbade it. Doing the same to a runner would be the same violation against a tree
  whose raw artifacts are digest-published.

The superseded generation's sequencing departure therefore stands as **D15** in
[§13](#13-divergence-register), restated there as a superseded generation's
halt-class departure whose evidence was replaced, with the delivered lane's verdict
owned by `runner-sequence.json`.

Per-tool version, ruleset or feed identity, feed state, baked flags, elapsed time,
finding count, records parsed and rejected, reconciliation and reach are the
**per-tool status contract owned by `oss-scan-results/tool-status.md`** and are not
duplicated here.

---

## 9. Normalization and the dataset

Source: `harness/artifacts/logs/normalize-run.json`, written by
`harness/lib/normalize/cli.py`. Command
`/usr/bin/python3 <repo>/harness/lib/normalize/cli.py` with an empty argument
vector, interpreter `/usr/bin/python3`
reporting **3.13.7** against the expected 3.13.7, run from the repository root,
`2026-09-03T09:45:11Z → 09:45:16Z`, **exit 0**, `halt` null. The normalizer uses
the standard library only, so it runs on the base interpreter independently of any
scanner's virtualenv. Every figure in this section is that record's, re-measured on
**2026-09-03** over the raw tree as it stands; the figures the superseded generation
of 2026-09-02 produced are kept beneath the table with their cause, because they were
correct about a raw tree whose `joern.json` has since been replaced.

| Indexed value | Figure |
| --- | --- |
| Artifacts routed | **9** — 8 present, 1 absent; every one routed by **detected shape**, never by filename |
| Dataset rows | **9,427** |
| Raw finding records traversed | **10,013**, by a traversal that walks the count units and **builds no rows** |
| Rejected records | **586**, all under the single named class `unresolvable_path` |
| Dataset-level reconciliation | `10013 = 9427 + 586` — **pass**, and every per-artifact identity held individually |
| Parsed `findings.json` rows against the dataset | 9,427 against 9,427 — pass |
| Parsed `findings.csv` rows against the dataset | 9,427 against 9,427 — pass, asserted **separately** rather than inferred from the JSON |
| Parsed JSON rows against parsed CSV rows | 9,427 against 9,427 — pass, as a third assertion |
| Typed field-for-field comparison | **9,427 rows / 113,124 fields**, `first_mismatch` null |
| Row validation | all 9,427 rows carry exactly the twelve fields in order; `path` absent **0**, `severity_norm` absent **0**, absolute paths **0**; absence appears only in `cve` (9,427), `package_coordinate` (9,427), `cwe` (8,671), `severity_native` (2,488) and `start_line` (3) |
| Rows by tool | `opengrep` 1,319, `semgrep` 1,162, `datadog-static-analyzer` 6,832, `gitleaks` 1, `checkov` 6, `trivy` 3, `osv-scanner` 0, `dependency-check` 0, `joern` 104 — summing to 9,427 |
| Parse status | `clean` ×7, `partial` ×1 (`joern`, 690 raw records → 104 rows, 586 rejected), `absent` ×1 (`osv-scanner`) |
| `osv-scanner`'s reconciliation | the literal **`not applicable — artifact absent`**, not a zero-equals-zero pass |
| Output files | `findings.json` 4,407,339 bytes, sha256 `339732ed19fad926cb0d6f3da769bc073c5d61585cf82cbefc9c2d0080d8438f`; `findings.csv` 2,081,058 bytes, sha256 `895d250f120894dcb51ab3e8a006e1ee3bcb1cb2f23a54f343fdfb3069983657` — both re-measured for this record, and both agreeing field for field with `harness/artifacts/logs/findings-publication.json`, the manifest the normalizer wrote beside them, whose `publication_id` `de9ba4ad9855fd92d439de4da7b159d3` is carried by both records |

**One generation of these figures has been superseded, and by a changed input rather
than by a corrected method.** The generation of `2026-09-02T22:56:48Z → 22:56:54Z`
recorded **9,430** rows, `10016 = 9430 + 586`, a typed comparison over **113,160**
fields, `cwe` absent on **8,674** rows, `joern` **693 raw records → 107 rows**, and
outputs `findings.json` 4,408,640 bytes / `d4e28c823fd1e76c2158130dc941762e0c6cf23424c0c990c930cc84ece6fc54`
and `findings.csv` 2,081,618 bytes / `9f646532494fcba3ad95a8e10f15f77957b9f16bea0b486b513e2a830f5445e6`.
Every one of those figures was correct about the raw tree of that day. The whole
difference is one artifact: `harness/artifacts/raw/joern.json` was rewritten by the
Stage 3 re-invocation of 2026-09-03 over the re-provisioned graph, from 693 findings
to **690** — `joern-reflection-forname` 413 → 411 and `joern-cipher-getinstance`
11 → 10 — and three fewer raw records with the rejection count unmoved carry through
to three fewer rows, three fewer `cwe` absences and 36 fewer compared fields
(12 × 3). No other artifact changed, and the eight other tools' tallies are identical
across the two generations. Both sets are retained per AAP §0.1.3; neither is averaged
with the other.

**On reproducibility, what is established and what is not.** The dataset is
**derivable** from the retained artifacts rather than merely accompanying them: the
run of record is itself the final normalization re-run of this run's sequence
([§18](#18-where-the-run-reached)), it consumed nothing but the eight artifacts in
`harness/artifacts/raw/` and the metadata record beside them, and its own manifest
recomputes both members' digests from the files on disk. Byte-identical
**repetition** was established within the superseded generation, where two
normalizer runs over one unchanged raw tree wrote the same two byte sizes and
digests. It is **not** re-established for the generation on record: no tool of any
kind was invoked in this checkpoint, so no second pass over the current raw tree
exists to compare against, and that gap is carried in
[§14](#14-values-that-could-not-be-established) rather than asserted away. What the
two generations do establish jointly is the property that matters more here — the
outputs track their inputs: one artifact changed, and the dataset changed by exactly
the amount that artifact changed.

**Row counts are parsed, never counted as physical lines.** Both files were parsed
to obtain every figure above; a message field carrying an embedded newline makes a
line count over-report, which is the method AAP §0.5.4 prohibits.

**The raw tree is read as runner-only, and an unexpected direct child stops the
run.** AAP §0.8.1 makes `harness/artifacts/raw/` runner-only — exactly one artifact
per tool that writes one, and nothing else ever — and §0.5.4 makes an artifact
matching neither the SARIF shape nor a known native shape a **halt** rather than a
best-effort parse. The normalizer enumerates that tree **before** it builds a source
index, before any adapter runs and long before either output file is written, and a
direct child outside the nine fixed artifact filenames now **stops the run** under
the named reason `raw-directory-unexpected-entry`, which
`harness/lib/normalize/cli.py:578` declares as `HALT_RAW_DIRECTORY_UNEXPECTED` and
which takes the halt-reason vocabulary `normalize-run.json` publishes from 37 names to **38**. An
expected name standing there as something other than a regular file — a directory,
or a symlink — halts under the same reason, with the condition naming which it was.
The evidence the halt records is filesystem-level only: the entry's name, whether it
is a directory, whether it is a symlink, its byte size where it has one, and whether
it carries an expected artifact name. Nothing reads into the document to guess a
writer for it, because a document in that tree has no writer to attribute it to and
fingerprinting one is what §0.8.1 forbids. Both dataset deliverables already on disk
are left byte-for-byte as they were, so a halt cannot half-publish. The eight
canonical artifacts hold no such entry, which is why the run above exits 0 with
`halt` null; **D22** records the departure this boundary closes.

**The dataset's owner root is declared, not inherited.** Every input the normalizer
consumes is an explicit argument with a documented default, and the root that decides
where the dataset pair may be written is now among them: `--repo-root DIR` declares
it on the command line and wins over `$HARNESS_REPO_ROOT`, which in turn wins over
the install root derived from the module's own location. Which of the three supplied
the root is recorded rather than left to inference: the canonical invocation above
passes no flag and takes the value the sourced environment file exports, so its record
carries `output_guards.repository_root_source` as `$HARNESS_REPO_ROOT`, and the same
field reads `--repo-root` when the flag is used. The flag exists because a caller whose
environment file **overwrites** a `$HARNESS_REPO_ROOT` it had already set had no other
way to name the root it owns, which **D23** records — and because the canonical
invocation's root is the checkout, nothing about it moves.

### The non-filesystem path count and proportion

From `normalize-run.json` `totals.path_kinds`:

| Path kind | Rows |
| --- | --- |
| `tree_file` | 9,323 |
| `bytecode_source` | 104 |
| `outside_root` | 0 |
| `archive_member` | 0 |
| **Non-filesystem total** | **0 of 9,427 — proportion 0.0** |

No row in this dataset names an archive member, a container outside the root or any
other non-filesystem coordinate, so the serialization those forms would have taken
was not exercised. `in_scope` is false on **29** rows, all of them `joern`'s, and
those rows are **kept** and counted; every other tool's rows are in scope. The
superseded generation of 2026-09-02 recorded `bytecode_source` **107** against a
9,430-row total, from the three additional `joern` findings its raw artifact carried;
`tree_file` and the two non-filesystem kinds are identical across both generations,
and so is the out-of-scope count, because the three findings that went away resolved
into the pinned tree's `src/main` and were in scope.

### The adapter and reconciliation tests

Source: `harness/artifacts/logs/adapter-tests-run.json`. Command, quoted from that
record's own `command` field rather than reconstructed:

```text
/usr/bin/python3 -m unittest discover -s oss-scan-results/adapter-tests -p 'test_*.py' -v
```

run from the repository root under interpreter `/usr/bin/python3` version
`3.13.7`, on the standard library's `unittest` — no third-party runner, no plugin
and no install step. It ran from **2026-09-02T21:19:32Z to 21:19:44Z**.
**1361 tests and 26,198 subTests, 0 failures, 0 errors, 0 skipped, 0 expected
failures, 0 unexpected successes, result `OK`, exit 0**,
11.970 s as `unittest` reported it and 12,183 ms wall, the second measured around the
process rather than derived from the first. The zero skip, expected-failure and
unexpected-success counters are reported rather than omitted, so a green result
cannot have been obtained by excusing a test.

The command, the window and both elapsed figures above are **projections of that
record**, not restatements of it: `harness/lib/verify_publication_owners.py`
re-reads `adapter-tests-run.json` and fails if any of the four differs from what
the owner carries, so the discover pattern and the verbosity flag appear here for
one reason only — the owner carries them. They were absent from both files until
this checkpoint, when the owner's `command` was found to omit the two arguments its
own captured verbose stream proves the invocation used; the owner now records the
invocation `oss-scan-results/adapter-tests/README.md` documents, and
`harness/lib/verify_status_figures.py` compares the documented command with the
recorded one after resolving `python3` to the interpreter path the record names, so
a future divergence between them fails a gate rather than being read past. The
owner states the correction under `correction_this_record_makes`.

Three figures are required to agree and do: the runner's own reported total, the sum
of the ten per-module totals, and the length of the per-test enumeration —
`127 + 228 + 107 + 75 + 93 + 117 + 162 + 132 + 121 + 199 = 1361`, and
`per_test_outcomes` carries **1361** entries, each a fully qualified test identifier
with its status. A module that silently stopped running a method would show as three
disagreeing numbers rather than as a passing total.

The committed tree holds `README.md`, **10 test modules, 106 fixtures and 106
expected-row files**, of which **73 are negative fixtures** cross-checked against
the nine rejection conditions AAP §0.5.4 enumerates, so that **every rejection
condition each exercised adapter can produce is fixture-backed** — for all six
exercised adapters, with the constructed-record assertions kept beside the fixtures
rather than in place of them. The corpus also carries the documents that reach an
already-covered class by a different route: a wrong `file_line_range` container
against a wrong first element, an empty message against an absent one, a zero line
against a boolean one. Which fixture came from which artifact, and what each module
asserts, is owned by `oss-scan-results/adapter-tests/README.md`.

**The one captured-positive-mapping requirement that needed a second capture:
`dependency-check`, and its disposition is `SATISFIED`.** It is stated here because
this file is the index and a reader should not have to open four documents to learn
one verdict. The owner is `adapter-tests-run.json`,
`positive_mapping.per_adapter.dependency-check.aap_0_6_2_captured_positive_mapping_requirement`.

| | |
| --- | --- |
| Status | **SATISFIED**, by genuine unmodified tool output — never by an exception, a waiver, or derived data relabelled as a capture |
| Why a second capture was needed | The scan-run artifact `harness/artifacts/raw/dependency-check.json` holds **32 dependency records, zero vulnerability records and zero package objects**. That measurement stands, is asserted by `RawArtifactProvenanceTest`, and is exactly why no excerpt of that artifact can exercise a single positive field |
| What satisfies it | `harness/artifacts/logs/dependency-check-positive-capture.json` — **46,684 bytes, 2 dependencies, 5 vulnerability records**, from the same tool build, same JDK 17 and same seeded feed, over input that resolves to packages the feed carries advisories for. Its exact command is in the accompanying `.log` |
| Where it is asserted | Copied byte-for-byte to `oss-scan-results/adapter-tests/fixtures/captured-dependency-check-vulnerabilities.json` and asserted field by field against five hand-verified rows in `oss-scan-results/adapter-tests/expected/captured-dependency-check-vulnerabilities.rows.json` by `test_dependency_check_adapter.py`'s `CapturedVulnerabilityFixtureTest` |
| What it contributes to the dataset | **Nothing.** It was taken outside `harness/artifacts/raw/` over input that is not the pinned tree, so it produces no row in `findings.json` or `findings.csv` and cannot alter any count |
| The superseded verdict | `FAILED`, retained beside the current one as `status_superseded_value`. It was correct while the scan-run artifact was the only candidate capture, and it is kept rather than deleted so the change of verdict is visible |

---

## 10. The Joern capability probe

Owned by `oss-scan-results/joern-probe.md` and the six per-query result files
under `queries/joern/results/`. Indexed here:

| Indexed value | Figure |
| --- | --- |
| Queries | **3**, hand-written, each with explicit traversal bounds: `01-callgraph-unguarded-driver-launch.sc`, `02-dataflow-unguarded-driver-launch.sc`, `03-parameterized-handler-sink-pairs.sc` |
| Result files | **2 per query** — a machine-readable `.json` envelope and a prose `.md`, six in all |
| Loader | `importCpg` only, into the switched workspace `queries/joern/.workspace` |
| `importCode` | **zero occurrences in each of the three committed `.sc` sources**, established by searching those files **textually** rather than inferred from what the run happened to do — the appearance of the alternative loader in a committed source would itself be the violation |
| JDK major and heap | 21 and 64 GiB for all three — [§6.4](#64-the-stage-5-probe--fourth-of-four) |
| Graph identity before each load | re-verified and matched for all three — [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) |
| Query revisions committed | **3, 3, 4**, on one convention with one owner: commits touching that query's own `.sc` file in the history of the HEAD the run measured at, newest first, with every returned commit asserted to be an ancestor of that HEAD and the measurement rejected if any is not. Each envelope publishes the HEAD (`d3bc40ae290877827cbd422ba9025a4f54328ec0`), the branch and `effort_query_revisions_ancestry_verified` beside the count. The commit that publishes these result files was the tip when the measurement was taken and is therefore counted; a later reader whose `git log` shows a different tip identifier reconciles against the published window rather than against a bare number |
| Distinct Joern API constructs | **28, 43 and 28**, each computed from a published deduplicated list that the query audits against its own source text, with a probe-wide union of **47**. Queries 01 and 03 declare the **same** 28-construct set; 02 shares 24 of them and adds 19, which is where the union's 47 comes from (28 + 19) |
| Parameterizability | **passed**, attributed solely to query 03 having **actually invoked** the named second pair — the `StandaloneSubmitRequestServlet.handleSubmit` handler at `StandaloneRestServer.scala:268` to the `DriverRunner.scala:240` sink — with that invocation's result captured in both of its result files and in `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` |
| Workspace hygiene | `queries/joern/.workspace/.gitignore` is exactly `*` and `!.gitignore`, so probe scratch stays out of the commit **without editing upstream Spark's root `.gitignore`**, which is left byte-identical |

Per-query compile and run status, returned records, distinct routes, the
mechanically defined spurious counts, every bound with its reached flag, the
entry-point traversed and truncated counters, and the duplicate-formulation matrix
are the report's own content and are not restated here.

Console evidence per query:
`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`,
`probe-02-dataflow-unguarded-driver-launch.log` and
`probe-03-parameterized-handler-sink-pairs.log`.

**The pre-load identity check is in each query's own stream**, at
`byte size matches: YES`, `sha256 matches: YES` and
`graph identity: PASS - re-verified immediately before the load`, each compared against
the record of account the query resolved from its own source
(`provision-log/cpg-identity.txt`, corroborated by `provision-log/cpg-record.txt`) and
each reading `547980224` / `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`.
Three separate checks rather than one shared reading, so a replacement between two
queries would have been caught by the later one. Each stream additionally records an
`import binding: PASS`, establishing that the private copy the engine imported is
byte-for-byte and inode-for-inode the input the identity check measured.

**Three standalone captures sit beside those streams, written by the probe re-run's own
driver.** `probe-01-callgraph-unguarded-driver-launch.identity.txt` is stamped
`2026-09-03T09:45:34Z`, `probe-02-…identity.txt` `2026-09-03T09:55:51Z` and
`probe-03-…identity.txt` `2026-09-03T10:07:18Z`, and each states the pair its query then
verified together with the query's finish instant — `09:55:20Z`, `10:06:51Z` and
`10:16:53Z`. Each also carries its own `supersedes` field naming the file the `w-013`
driver wrote on 2026-09-01, which stated `541309809` /
`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` — a graph provisioning
has since replaced, so that identity is on no disk this checkout can reach. The three
files name their own clone root, and must: a capture's value is that it was taken before a
load, and where it was taken is a historical fact about the measurement rather than a path
a reader resolves.

---

## 11. Deliverable inventory with resolved absolute paths

Resolved against the root of the checkout this file is read from, which is what
`git rev-parse --show-toplevel` prints there. That root is written `<repo>` in the
absolute column below, and every path is that root followed by the
repository-relative path in the first column. The placeholder is not shorthand for
a literal this file omits: no clone-specific root is stated anywhere in this
document, for the reason [§0](#how-to-read-a-citation-in-this-file) gives, and
`<repo>` is therefore the resolved absolute form — it resolves in this checkout, in
a sibling clone, and in a fresh checkout of the same commit. Every deliverable is
committed, so the relative paths in the first column resolve under whichever
checkout root the file is read from.

### The eight result-deliverable categories under `oss-scan-results/`

| Deliverable | Absolute path | State |
| --- | --- | --- |
| `oss-scan-results/findings.json` | `<repo>/oss-scan-results/findings.json` | present, 4,408,640 bytes |
| `oss-scan-results/findings.csv` | `<repo>/oss-scan-results/findings.csv` | present, 2,081,618 bytes |
| `oss-scan-results/severity-map.md` | `<repo>/oss-scan-results/severity-map.md` | present |
| `oss-scan-results/tool-status.md` | `<repo>/oss-scan-results/tool-status.md` | present |
| `oss-scan-results/build-record.md` | `<repo>/oss-scan-results/build-record.md` | present |
| `oss-scan-results/joern-probe.md` | `<repo>/oss-scan-results/joern-probe.md` | present |
| `oss-scan-results/run-record.md` | `<repo>/oss-scan-results/run-record.md` | **this file** |
| `oss-scan-results/adapter-tests/` | `<repo>/oss-scan-results/adapter-tests/` | present — `README.md`, **10** `test_*.py`, `fixtures/` (**105**), `expected/` (**105**) |

### The three deliverable trees

| Tree | Absolute path | State |
| --- | --- | --- |
| `queries/joern/` | `<repo>/queries/joern/` | present — 3 `.sc`, `results/` with 6 files, `.workspace/.gitignore` |
| `harness/artifacts/raw/` | `<repo>/harness/artifacts/raw/` | present — **8** artifacts, one per tool that wrote one, and nothing else |
| `harness/artifacts/logs/` | `<repo>/harness/artifacts/logs/` | present — **163** files, counted recursively: **129** top-level entries of which 4 are directories (`checkov.out/`, `dependency-check.out/`, `gitleaks.parts/`, `trivy.parts/`) holding the side artifacts their runners wrote. **18** of the 163 are the `reverification-*` evidence the remediation passes measured on 2026-09-02 ([§16](#16-manifest-of-the-two-git-ignored-artifact-trees)). An earlier edition of this row stated **151** files and 117 top-level entries, which was correct before this checkpoint's re-anchoring added twelve members; the tree is git-tracked, so `git ls-files harness/artifacts/logs \| wc -l` returns the same **163** |

### Scope, staging, graph and normalizer

| Path | Absolute path | State |
| --- | --- | --- |
| `harness/scope/allowlist.txt` | `<repo>/harness/scope/allowlist.txt` | present, 343 bytes, 12 globs, sha256 `0013edf6…4143d1`, left exactly as found |
| `harness/cpg/spark.cpg` | `<repo>/harness/cpg/spark.cpg` | present — a 33-byte symlink resolving to `/opt/blitzy-harness/cpg/spark.cpg` |
| `harness/lib/normalize/` | `<repo>/harness/lib/normalize/` | present — 6 modules plus `adapters/` |
| The frontend staging directory of the failed attempt | `harness/artifacts/cpg-input-attempt1-full-191` | **Not present in this checkout now** and stated as such. Proved absent before use, created by this run's inventory lane, never cleared, and supplied to this run's frontend invocation in full: **191 archives, 431,184,822 bytes** — one measurement, cited here and at §6.1(a), **D1** and **D3**, and owned by **`harness/artifacts/logs/build-reactor.log` STEP 13**, whose census prints `own artifacts total : 191` and `own artifact bytes : 431184822` at its lines 10035-10036, with `harness/artifacts/logs/cpg-frontend.log` STEP 1 stating the same pair independently at its lines 134-135. Both locators were corrected on 2026-09-02: an earlier edition cited `build-reactor.log` lines 10025-10026 and `cpg-frontend.log` line 38, which are the `print(` statements that emit the census and the frontend's heap-and-JDK line respectively, not the two figures themselves. **It is not owned by `cpg-input-inventory.json`, and a citation of that file for it is stale**: that file was regenerated for this generation at schema `cpg-input-inventory/2.0.0` and now describes the **62**-archive set of the graph actually loaded — `archive_count` 62, `total_bytes` **285,122,375**, `staging_tree` `/opt/blitzy-harness/cpg-input`. That total is the corrected one: the same file published **285,122,371** until the four-byte correction of 2026-09-03, which its own `byte_total_correction_2026-09-03` node states with the arithmetic (two members at +2 each), and the row below at "The staging tree of the graph in use" is where that figure is owned and cross-checked three ways. `harness/artifacts/MANIFEST.json` records the tree rather than publishing it — `cpg_input_records.not_present_in_this_checkout` names it and `why_no_per_file_entries` states why per-file copies are deliberately **not** duplicated there (AAP §0.6.4: one measurement cited twice, never two) — and its `owners` list was **corrected on 2026-09-02 to name this same owner**: `owners[0]` is now `build-reactor.log` STEP 13, whose `states` field carries exactly the pair above with both addend chains closed (`422 + 14 = 436`, `191 + 436 = 627`) and whose `per_file_ledger` field records that no ordered per-entry manifest of the 191-archive set survives, so no per-file identity may be cited for it. An earlier edition of that manifest attributed these figures to `cpg-input-inventory.json` and described the 436 exclusions as carrying "a reason per file"; both were wrong, and a reader following the old pointer landed on a narrower tree whose numbers did not match the claim. The manifest's own `note` field records the correction, so the pointer and the figure now agree in the evidence rather than only here. Excluded from git collection by `.gitignore:31`, which is why an owner log rather than the tree is the deliverable |
| The staging tree of the graph in use | `/opt/blitzy-harness/cpg-input` | Host-global, written by provisioning. **As inventoried at write time: 62 archives, 285,122,375 bytes, 62 distinct sha256, the archive-to-digest mapping total and injective both ways.** That total is the graph-in-use figure re-established on 2026-09-03 and true three ways — the sum of the 62 members, a `du -sb` over the staged tree, and the write-time record `/opt/blitzy-harness/provision-log/cpg-record.txt`. An earlier edition stated **285,122,371**, four bytes low; the three verbatim streams `build-reactor.log` line 10096, `cpg-frontend.log` line 128 and `cpg-graph-record.log` line 28 still carry that figure and are left unedited, because a runner's own words are preserved rather than corrected ([§13](#13-divergence-register) **D8**). **That tree has since drifted and no longer matches its own inventory** — a live census on **2026-09-02T14:44:09Z** measured the same 62 names at **234,609,958 bytes**, with **39 matching and 23 drifted**, only **45 distinct digests**, and **32 members still sharing an inode with a file under `/opt/spark-src`**. The cause is that provisioning staged the tree as **hard links into a shared mutable build tree** rather than as immutable copies, so the injectivity assertion is marked superseded for the live tree rather than deleted, and **the surviving tree cannot prove the graph's input bytes or recreate the graph**. Both the write-time inventory and the census are in `harness/artifacts/logs/cpg-input-inventory.json`, which also carries the per-module coverage witnesses: **31** of the reactor's **38** JAR-packaging projects present, **7** absent (divergence **D3**) |
| This run's frontend output path | `<scratch>/cpg/spark.cpg.PARTIAL-TRUNCATED-DO-NOT-LOAD`, in the `w-005` lane's private scratch | **present as evidence and explicitly not accepted**, re-measured for this edition: 691,541,019 bytes, sha256 `b1559c930a7b9ced717a0babf9a7e172d2b93d2cdef45a959304f063aedfe408`, the truncated write left by the serialization failure in D1. Renamed to make loading it impossible by accident, never linked at `harness/cpg/spark.cpg`, and loaded by nothing. Its sibling `witness.cpg`, 418,777,229 bytes, sha256 `8d3462b78d3c4b009c994d1ae838b6266aa2af3e68b3c0fbdcbd3b3f630ad41d`, is the same lane's shims witness graph (**D12**) and is likewise retained. Both sit outside every repository checkout, so they are published by record rather than by path |
| The `importCpg` verification workspace | `/tmp/blitzy/scratch/<run>/w-013/joern-verify/workspace/` | outside the checkout, in this clone's private scratch directory, proved absent before use and neither reused nor cleared. `cpg-verify.log` records the load reading its working copy from that workspace, so the persisted graph at the sanctioned path was never written through by the verification load |


**Nothing this generation loaded was torn down, and one thing an earlier one tore
down is named rather than covered by a general claim.** Each of the three probe
invocations recorded here **retained** the private graph copy it loaded and the
exclusive directory holding it, at the read-only mode the copy step set — the three
retained paths, their inodes and their re-measured identity are divergence **D18** in
[§13](#13-divergence-register), which also records the superseded generation that
deleted them. Everything else stands where the run left it (AAP §0.8.1): no artifact
tree was cleared, no staging tree was purged, the verification workspace that is gone
was not removed by this run, and the truncated partial write was renamed rather than
deleted so that the failure it evidences stays checkable.

### Paths this document cites that are NOT resolvable from this clone

**Stated once, in one place, because AAP §0.9.4 requires every cited number to name a file that
exists and this is where a reader checks that.** Every path in the document was extracted and
**re-tested for existence when this edition was written** — not carried forward from an earlier
edition, which is how the previous table came to list six files that are in fact present. The
following are cited as sources and are **not on disk here**; each is named with what depends on it
and what carries the same fact instead. Nothing was created, restored or substituted to make any of
them resolve, and no figure was silently dropped because its source moved.

Two distinctions govern the table, because both were previously got wrong. A path is listed here
only if it is **absent from the filesystem of the host this edition was written on**; a citation
expressed *relative to another root* — a Spark source anchor under `/opt/spark-src`, a JAR inside
the private by-SHA build clone, a staging entry inside another lane's scratch — resolves against
that root and is **not** "absent from this clone". And a path present on this host but outside every
repository checkout is **resolvable, not absent**: it is recorded in the preceding tables with its
location rather than here.

| Cited path | Status | What depends on it, and what carries the fact instead |
| --- | --- | --- |
| `harness/artifacts/cpg/` and `harness/artifacts/cpg/spark.cpg` | absent, **correctly** | No graph was written inside this checkout (**D1**). The gate reports the absence explicitly rather than treating it as a mismatch, and `MANIFEST.json`'s `cpg` block says the same: this checkout's own frontend invocation produced no graph, so there is no such directory to publish and none is invented. What is published instead is `harness/cpg/spark.cpg`, the 33-byte symlink, measured with the link followed |
| `harness/artifacts/raw/osv-scanner.json` | absent, **correctly** | `osv-scanner` wrote no artifact: exit 128 with its own stated reason. AAP §0.5.4 classes that as completion with nothing in scope to work on, so the absence is the expected outcome rather than a missing file. **Which stream carries the reason is stated exactly, because the classification turns on the tool having stated one:** it is `harness/artifacts/logs/osv-scanner.stderr.log`, **967 bytes over 21 lines**, whose last line is `No package sources found, --help for usage information.`, preceded by **18** `Scanning dir …` lines — one per in-scope directory the runner passed — a `Starting filesystem walk for root: /` line and `End status: 640 dirs visited, 4735 inodes visited, 0 Extract calls, 296.87925ms elapsed, 296.87957ms wall time`. `harness/artifacts/logs/osv-scanner.stdout.log` is **0 bytes** and carries nothing at all, so a reader looking for the reason on stdout will not find it there. `osv-scanner.status` records `exit_code=128` with `artifact_bytes=MISSING`, and the stream is quoted verbatim in `oss-scan-results/tool-status.md`, which owns the per-tool contract. `runner-sequence.json` records the invocation with no `artifact` bound — the one place among the 44 top-level bindings where none exists. **The ground for there being nothing in scope is structural and recorded rather than assumed**: the twelve authoritative roots contain **zero dependency manifests**, and AAP §0.3.2 makes that a finding to report rather than a scope to widen ([§15](#15-the-october-2025-caveat)) |
| `harness/artifacts/cpg-input` and `harness/artifacts/cpg-input-attempt1-full-191` | absent | The two frontend staging trees. `MANIFEST.json` `cpg_input_records` records rather than publishes them, and `not_present_in_this_checkout` names both. Its `owners` list now names **three** owners, one per tree actually measured: **`build-reactor.log` STEP 13** (lines 10035-10036) for the 191-archive / 431,184,822-byte set of the invocation on record, with `cpg-frontend.log` STEP 1 (lines 134-135) stating that pair independently; **`cpg-frontend-input-manifest.json`** for the 189-archive / 308,385,184-byte set handed to the **superseded** w-000 attempt, labelled there as invocation C (**D20**, which also adjudicates the 141-byte difference between the two totals); and **`cpg-input-inventory.json`** for the 62-archive / 285,122,375-byte provisioned tree the graph that actually loads was built over, marked as holding no exclusion ledger of any kind. That third attribution was the defect: an earlier edition gave the first set's figures to that file and described the 436 exclusions as carrying a reason per file, and both were wrong. The manifest's own `note` field records the correction |
| `/tmp/blitzy-harness-scratch/0/cpg-verify-descriptors` | absent | The second verification workspace. `cpg-verify.log` STEP 5 and STEP 11 preserve its name and its absence-before-use proof. Its sibling `/tmp/blitzy-harness-scratch/0/cpg-verify` was present when an earlier edition tested it and **is absent now**: clone 0's scratch directory holds only `joern-run`, re-measured for this edition. The three verification workspaces of the generation on record are `/tmp/blitzy-harness-scratch/424/wave2-{joern,detail,corrob}`, and all three **are** present |
| `/tmp/blitzy-harness-scratch/4/checkov-shape-verify` | absent — the whole `…/4` scratch root | Where the **superseded** generation's prohibited second Checkov invocation wrote its output. That invocation is recorded in full in [§8](#a-superseded-generation-assembled-these-records-from-five-lanes--recorded-as-history-not-as-this-runs) and as **D15**; the figures were carried in enriched `.status` files that the final commit replaced with the runners' verbatim seven-line trailers, so §8's prose is now their custodian. The Checkov output-shape conclusion rests on the recorded 8,380-byte artifact alone and does not depend on that scratch tree |

**Six paths the previous edition listed here are present and have been removed from the table**, each
re-tested with `test -e` for this edition: `harness/ENVIRONMENT.md` (1,044 lines, sha256
`e296c556…04a9a40c`; 923 lines and `5aa68b25…5ecb140` when the gate read it, re-anchored
2026-09-02 — **D4**), `harness/env.sh`, `harness/bin/` **including all nine `run-*.sh`** and
`run-joern.sh` in particular, `harness/lib/scope.sh` (whose `scope_cred_state` at lines 105-109 can
be read directly, and which uses `${VAR:+set}` only, with its own comment stating that
`${VAR:-absent}` would emit the variable's own value into a log this pipeline preserves verbatim),
`harness/lib/joern-scan.sc` (whose lines 1-8 describe the baked set directly as **six structural
queries**, each keyed on an indexed call name and each capped by an explicit traversal bound, against
Joern's default 59-query bundle — a `.status` file carries no such count and never did), and
`harness/artifacts/logs/joern.preflight.log` (present at 321 lines; it is the **2026-08-24 lane's**
gate report and states that lane's superseded identity pair, which is why this document cites the
44-line `joern-preflight.log` for the current pair — **D13** records the restoration).

Two consequences follow and are stated because earlier editions drew the opposite conclusion. The
per-tool runner facts are readable **from the runners themselves** as well as from
`runner-metadata.json`, so no fact in this document rests on a file a reader cannot open. And
**re-execution is not blocked by absence** — the harness is here — which is why
[§8](#a-superseded-generation-assembled-these-records-from-five-lanes--recorded-as-history-not-as-this-runs)
now rests the decision not to re-invoke a runner on AAP §0.6.4 and §0.8.1 rather than on a missing
directory.

**Every other path this document cites was tested and resolves**, including all **145** members of
the two artifact trees (8 in `raw/`, 163 in `logs/`), the eight result deliverables, the three `.sc`
sources and their six result files, `harness/scope/allowlist.txt`, `harness/cpg/spark.cpg` and its
target, `harness/lib/normalize/**`, `harness/lib/preflight_graph_identity.py`,
`harness/lib/run-joern-gated.sh`, `harness/lib/verify_status_figures.py`,
`harness/lib/verify_publication_owners.py`, `harness/artifacts/MANIFEST.json`, the Spark source
anchors under `/opt/spark-src`, the pinned Opengrep rule file and `/opt/blitzy-harness/cpg-input`.
The two `<scratch>/cpg/` artefacts of **D1** also resolve — `spark.cpg.PARTIAL-TRUNCATED-DO-NOT-LOAD`
at 691,541,019 bytes / sha256 `b1559c93…dfe408` and `witness.cpg` at 418,777,229 bytes / sha256
`8d3462b7…30ad41d`, both re-measured for this edition in the `w-005` lane's private scratch — so
they are recorded above with their location rather than listed as unresolvable; they sit outside
every repository checkout, which is why they are published by record rather than by path.
`witness.cpg` is no longer only a retained artefact, either: it was **loaded with `importCpg` on
2026-09-02**, and that load is `harness/artifacts/logs/cpg-verify.log` **PART 2** — exit 0 in 458 s
under JDK 21 at `-J-Xmx64g`, its identity re-measured before and after and unchanged, reporting
994,192 methods / 97,292 type declarations / 45,680 files with **38 of 38** module witnesses present.
That load is what closed two citations in `cpg-frontend.log` which had pointed at a `PART 2` that did
not yet exist. It is a load of the **narrowed** witness graph — one primary artifact per module — so
nothing in it is a verdict about the graph the runners read, and none of the counts in
[§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) comes from it.

---

## 12. Every failure or termination

**No invocation anywhere in this run terminated without an exit code, so
`exit_status: timeout` appears nowhere and no entry carries that status** — including
this run's 8-hour frontend invocation, which ran to its own failure and exited **1**
rather than being terminated on a clock. There is no time limit anywhere in this run,
and none was applied to it. Every
one of the nine runners ended with its own exit code
(`oss-scan-results/tool-status.md`, "Artifact status and exit status are
independent"), and **exit 78 — the harness's configuration-fault status — was
never observed**.

Two entries below belong to a **superseded generation** rather than to the delivered
run, and each says so in its own row: the five-lane nine-runner assembly whose
enriched `.status` evidence the final commit replaced, and the withheld frontend
input manifest of a w-000 attempt. Neither is deleted and no delivered measurement
rests on either; both are registered in [§13](#13-divergence-register) with what a
human must do.

| Event | What it was | Disposition |
| --- | --- | --- |
| **The gate** | Halted on **two** conditions. `gate.artifact_trees_exist_and_empty`: measured at `2026-09-01T13:28:07.612Z`, **before this run wrote anything**, `harness/artifacts/raw/` was present with **8** entries and `harness/artifacts/logs/` present with **85** — both already non-empty, being the committed deliverables of earlier clones of this code generation. And `gate.environment_record_graph_identity_agreement`, the record-versus-disk contradiction of **D4** — **re-anchored at the record on 2026-09-02 and again on 2026-09-03**, with the gate's reading retained, the 2026-09-02 `status_now: resolved` claim **withdrawn** (its literal kept as `status_now_superseded_value`) after the re-provisioning falsified it, and the identity gate now exiting 0 with `VERDICT: PASS`. **Re-censused 2026-09-03, the emptiness condition is present again** — `raw/` 8 entries, `logs/` 129 top-level entries over 163 files. Verdict **halt**, `authorises` **nothing**, **38 pass / 3 recorded difference / 2 halt of 43** as the gate counted them | **Halt-class finding, one condition still live and reported, the other repaired** — [§1](#1-gate-verdicts), divergence **D0** in [§13](#13-divergence-register). No stage was gate-authorised; every stage after it ran and is recorded as work done after an unmet precondition. The live condition is not repairable here: the trees are committed deliverables and AAP §0.8.1/§0.9.2 forbid creating or clearing either, so this run created neither and cleared neither. **D4** records the other |
| **The nine-runner lane, as delivered** | **One strictly serial lane**: nine invocations, one per tool and no tool twice, each with zero arguments, from one script in one process in clone 13, `13:49:39Z → 14:41:25Z` in canonical tool order, with every artifact, stream, `.status` and console log digest-bound to its own invocation — **82 bound pieces, all re-measured and all agreeing** | **No failure.** Owner `harness/artifacts/logs/runner-sequence.json`; the account and the one limitation on its whole-second stamps are in [§8](#the-delivered-lane--one-serial-lane-bound-to-its-evidence-by-digest) |
| **The nine-runner lane of a superseded generation** | Assembled from **five** clone-local lanes with **five overlapping pairs** across 5 of the 9, plus **one prohibited second invocation of Checkov**. Those figures were carried in *enriched* `<tool>.status` files that commit `0e3e742a5ad` replaced with the runners' verbatim seven-line trailers, so they are no longer citable from this tree and are stated as that generation's measurement | **Halt-class departure of a superseded generation, reported and not repaired** — [§8](#a-superseded-generation-assembled-these-records-from-five-lanes--recorded-as-history-not-as-this-runs), divergence **D15**, where what a human must do is retained. Re-execution is **mechanically possible** here — the harness is present — and is deliberately not performed, for the AAP §0.6.4 and §0.8.1 reasons §8 states |
| **The frontend input set of a superseded attempt** | `harness/artifacts/logs/cpg-frontend-input-manifest.json`, written in a **w-000** clone, records **189** archives supplied against a **191**-archive full inventory, with **2 withheld** for a witness-preserving reason. The invocation **on record** (`cpg-frontend.log`, the w-005 lane) supplied the complete **191** with no `--exclude`, no `--exclude-regex` and no depth override | **Halt-class, reported, not repaired and not relied on** — divergence **D20** in [§13](#13-divergence-register), which names both withheld archives with their sizes, digests and stated reasons. No delivered measurement rests on the trimmed set; the manifest is retained as evidence under AAP §0.8.1 rather than removed |
| `gitleaks` exit **2** and `checkov` exit **1** | Non-zero because each found something. Both wrote an artifact and both parse | Ordinary. Artifact status and exit status are independent; the exit code is recorded as a fact and used for nothing else |
| `osv-scanner` exit **128**, **no artifact written** | The tool stated its own reason: `No package sources found, --help for usage information.`, quoted verbatim in its `tool-status.md` entry | **Completion with nothing in scope to work on**, not a failure. Zero rows, reconciliation `not applicable — artifact absent`, run continues. The missing-artifact halt was not engaged, because the absence came with the tool's own stated reason |
| `joern` artifact **partial** | **690** raw records, **104** rows, **586** records rejected under the single named class `unresolvable_path`, giving `104 + 586 = 690`. The superseded generation of 2026-09-02 recorded 693 raw records and 107 rows against the same 586 rejections | Partial parse is a first-class outcome: every parsable record emitted, every rejection counted under its class |
| The **taint A/B** | Non-discriminating on the mandated subject file: 1 finding at `DiskStore.scala` line 72 in **both** arms, byte-identical artifacts — and still byte-identical with the whole ruleset loaded, while the same rule discriminates 2 against 0 on `HiveShim.scala` | **Halt-class finding, reported and not repaired** — [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured), divergence D2 in [§13](#13-divergence-register) |
| The **frontend build**, as provisioning left it | The graph on disk was written by the provisioning invocation before this run's first command | **Halt-class finding, reported and not repaired** — divergence D1 in [§13](#13-divergence-register) |
| The **frontend build this run performed** | Invoked over the complete 191-artifact asserted manifest under JDK 21 at a proven-committable 128 GiB heap. Ran **8 h 01 m** to a **113.3 GiB** peak RSS, completed extraction and every AST pass, then terminated **in persistence** with exit **1** and `java.lang.OutOfMemoryError: Required array length 2147483639 + 72 is too large` in `flatgraph.storage.WriterContext.finish`. It produced **no graph**; the 691,541,019-byte truncated partial write is preserved as evidence and explicitly not accepted | **Halt-class finding, reported and not repaired** — divergence D1. The bound is a fixed array length on the one buffer flatgraph serializes the whole string pool through, proved from that method's bytecode in `cpg-frontend.log` STEP 8, so no heap moves it; STEP 10 enumerates every mitigation examined, and the only effective one — excluding inputs — is prohibited by AAP §0.5.1 and §0.9.2. **Nothing was trimmed to obtain a graph** |
| The **environment record's stated graph identity** | `harness/ENVIRONMENT.md` §7 **stated** 541,255,894 / `26d327cc…` when the gate read it; the bytes on disk that day were 541,309,809 / `4616845a…`, and all five loads of the 2026-09-01 generation read the latter and **measured** it immediately before reading. The bytes on disk **now** are 547,980,224 / `325887cf…3dc6`, and all seven loads of the generation on record measured that pair | **Halt-class when observed, RE-ANCHORED 2026-09-02 and again 2026-09-03** (**D4**, and the gate check `gate.environment_record_graph_identity_agreement`, whose reading is retained with a `resolution` beside it and whose 2026-09-02 `status_now: resolved` claim is **withdrawn**, its literal kept as `status_now_superseded_value`). The disagreement was the record being stale, not the graph being wrong, and the graph's own write-time record of account adjudicates between them explicitly, so AAP §0.1.3's fourth case — which requires that *no* anchor exist — never applied. §7 and the inline-values Graph block were re-anchored to that owner, every superseded value retained with provenance in a supersession appendix, and the record, the filesystem and the owner agree on bytes, sha256, methods, internal methods, type declarations, files, heap and peak RSS. The second re-anchoring was needed because the host was re-provisioned at `2026-09-03T01:17:07Z` and the graph rebuilt, which falsified the first correction rather than any method in it. **The graph was not touched in either correction** — nothing was written, replaced, moved or re-pointed to make a record agree, and the bytes were read and hashed only |
| The **Stage 3 pre-load identity comparison**, on the 2026-09-01 invocation | The measurement was contemporaneous — the runner recomputed size and digest itself at `joern.runner-console.log` lines 14-15, printed from `harness/bin/run-joern.sh` lines 57-58, inside its `14:25:10Z → 14:41:24Z` window, and `runner-sequence.json` records the same pair either side of the load. But the **comparison against the record of account**, `joern-preflight.log`, was stamped `2026-09-01T14:52:54Z` with `Clone index 0` — about 11½ minutes after that load and in a different clone | **A failed control with a sound outcome, and the ordering holds on the invocation of record.** AAP §0.8.2's "compared immediately before every load" was not satisfied for that one load; the measured pair equalled the record at both moments and the graph's mtime `2026-08-30 19:18:37Z` preceded both, so no substitution occurred. The cause is that the runner *prints* its input's identity without comparing it, which is still true of the provisioned runner and is recorded rather than repaired: a 2026-09-02 edit that inserted the comparison into `harness/bin/run-joern.sh` was reverted on 2026-09-03, because AAP §0.6.1 marks `harness/bin/` REFERENCE and §0.8.1 states that no runner file is edited, and the provisioning patch is published in [§13](#13-divergence-register) instead. **What closed the ordering is running the gate outside the runner**: on 2026-09-03 `harness/lib/preflight_graph_identity.py --check-only` ran at `09:07:46Z` and exited **0** with `VERDICT: PASS`, one second before the `09:07:47Z` invocation it governs. Carried in [§6.3](#63-the-stage-3-joern-runner--third-of-four) and as **D4**'s adjudication row |
| The **three probe queries** | All three ran to completion in this generation — exit 0 each, gated on the graph's re-verified identity immediately before its own load, with both result files written | No failure and no termination. The gate's capability to refuse is evidenced separately by `joern-preflight-negative-test.log`, which mutates the recorded digest and records the runner producing no output and leaving its artifact untouched |
| Anything else | No tool crashed, no artifact matched an unknown shape (`failed` never occurred), no reconciliation identity failed, no adapter fixture, rejection or reconciliation test failed, and no runner resolved a tree other than `SPARK_SRC` | — |

---

## 13. Divergence register

Every divergence with **both the expected and the observed value** (AAP §0.9.4).
**Four are halt-class findings of the delivered run, reported and not repaired —
D0, D1, D2 and D4.** **Two more are halt-class departures of a *superseded*
generation, retained rather than deleted because the departure was really made:
D15**, whose nine-runner lane was assembled from five clone-local lanes and carried
one prohibited re-invocation, and **D20**, whose frontend attempt withheld two
archives from the input set. Neither is inherited by the delivered lane, each says so
in its own first row, and each still records what a human must do. **D16 records
the interpreter this pipeline ran on and states that this run made no security
assessment of it** — an absence a reader must know about rather than infer, and
neither a halt nor a tolerated difference. **D17 and D18 are
violations of a stated boundary rather than differences between values** — the
three probe queries were re-executed under a static-only review boundary that
forbade it, and that generation deleted all three private graph copies contrary to
AAP §0.8.1 — and **D19 records the divergence their correction opened and the
execution that closed it**, the committed query sources now being the bytes the
generation on record ran. D18's cost is closed the same way: the copies the loads on
record read are retained on disk. **D21 records seven repository additions no entry in
the AAP's file transformation mapping authorises**, each conformant to the
convention its subsection states and each load-bearing for a requirement the AAP
does state. **D22 is a departure of this run's own normalizer, resolved at its root
cause and verified at runtime** — the raw-tree ingestion boundary recorded an
unexpected direct child and continued where AAP §0.8.1 and §0.5.4 require a halt —
and **D23 is a fix declined on AAP grounds**: the provisioned environment file
discards a pre-set `HARNESS_REPO_ROOT` against its own override contract, four AAP
subsections and the clone instructions forbid editing it, so the one-line correction
is reported for a human while the normalizer's own owner root becomes an explicit
argument. **D24 records the publication-owner gate's one standing disagreement** — the
absolute root this file states is the publication checkout's rather than the working
clone's the gate runs in, which is structural rather than drift. D13 is a conflict whose
premise measurement has since retired. The rest
are recorded differences that do not stop the run (AAP §0.9.3).

**Why the first entry is numbered zero.** D0 is the **gate's** divergence, and the
gate precedes every stage this register's other entries belong to. It is numbered
zero rather than appended so that a reader meeting the register for the first time
meets the condition that governs the standing of everything below it, and so that
no existing entry is renumbered — other documents cite D1 through D14 by name. For
the same reason **D20 through D31 are appended** rather than inserted where their
subject matter would place them. **D31 is the newest**, added 2026-09-03; **D25** is the
one entry that has been *revised* rather than only appended to, because two of the five
REFERENCE-marked harness files it recorded as edited were reverted the next day.

**Six further entries, D26 through D31, were appended on 2026-09-03** and every one of them
is a *record* rather than a repair, which is the shape §0.3.2 requires when a condition's root
cause sits in a file this run may not write. **D26** states that the provisioned Joern runner
neither compares the graph's identity nor sizes the JVM that holds it, publishes both patches,
and names the one gap for which no mitigation exists — nothing in this configuration can
withdraw an artifact after the fact. **D27** is the one resolution in the group: AAP §0.9.2's
method-count floor, previously prose only, is now a declared constant and an adjudicated
verdict. **D28** records that nothing in `harness/bin/` invokes the scan-target preflight, that
this run invokes it itself, and — stated rather than buried — that its artifact-tree check
**censuses without refusing**, because refusing would require creating or clearing a tree that
§0.8.1 forbids this run to touch. **D29**, **D30** and **D31** are three provisioned-surface
defects with their patches and their measured reachability: a path variable interpolated into
`python3 -c` source, an environment file that creates both artifact trees on every source and so
defeats the mandated missing-tree halt, and a shell helper that hands a caller-supplied variable
name to `eval`. **D25 was itself revised** rather than appended to: two of the five REFERENCE
files it recorded as edited were reverted to their provisioned bytes on 2026-09-03, and the
recommendation it carried against reverting them is withdrawn as a recommendation while its
description of the runner's behaviour is retained. The register that maps the nine findings of
2026-09-03 onto these entries is [below](#qa-testing-findings-of-2026-09-03--the-nine-this-document-answers-and-where-each-is-answered).

### D0 — halt-class: the gate halted on the artifact trees, and authorised nothing

| Field | Value |
| --- | --- |
| Expected | AAP §0.8.1 — **both** artifact trees already exist and are **empty** at one moment before this run writes anything; AAP §0.9.2 halts on "either artifact tree missing or non-empty" and calls it a provisioning fault this run may neither create nor clear |
| Observed | **Both trees were already non-empty before this run began.** Measured at `2026-09-01T13:28:07.612Z`, **before this run wrote anything**: `harness/artifacts/raw/` **present with 8 entries** and `harness/artifacts/logs/` **present with 85**. They are the committed deliverables of earlier clones of this code generation — which is exactly the case the rule exists to catch, since an artifact already in place is indistinguishable from this run's |
| What the cause is **not** | a foreign artifact, a stale scanner file from an unrelated project, or a tree this run brought into existence. Both trees **existed before this run began**, so this run **created neither and cleared neither**, and **deleted nothing** — no entry was removed to manufacture a pass. `gate-record.json`'s own disposition draws the contrast: unlike the inherited gate record of clone 9, this run did not even bring one of the trees into existence as a side effect of sourcing the environment file |
| The verdict as recorded | `gate-record.json` `gate_verdict.overall` **halt**, `authorises` beginning **"nothing. No stage was authorised by this gate…"**, `counts_by_verdict` **38 pass / 3 recorded difference / 2 halt** of **43**, and **two** entries in `halts` — this one and `gate.environment_record_graph_identity_agreement` (**D4**). No check carries an `inconclusive` verdict |
| A superseded generation's measurement of the same check, labelled as such | An earlier clone of this code generation measured it at `2026-08-24T16:59:25Z` with `raw/` **absent** (`ls -A` exit 2) and brought into existence by that lane's sourcing of the environment file, `logs/` holding **exactly one** entry, and counts **38 pass / 3 recorded difference / 1 halt of 42**. Those are that generation's figures, not this one's, and are stated here so a reader comparing this document against an earlier edition can see which generation each belongs to. **The verdict is halt in both.** Earlier still, this document published **pass, authorising Stage 1**, with 39 pass / 3 recorded difference / 0 halt — publishing a pass over a record that says halt was the **fixable half** of this divergence and the most consequential statement in the document, because every stage's authority is read from it. [§1](#1-gate-verdicts) now carries the halt with the counts as currently recorded |
| Consequence, stated in both directions | Every stage after the gate **ran**, and none of them is a compliant stage completion under AAP §0.8.1. The dataset is internally reconciled and reproducible — the identity holds per artifact and at dataset level and the two output files agree field for field ([§9](#9-normalization-and-the-dataset)) — and it is **not gate-authorised**. Both halves are true and neither may be reported without the other |
| Re-censused 2026-09-03, and still present | `ls -A harness/artifacts/raw \| wc -l` returns **8** and `harness/artifacts/logs` **129** top-level entries over **163** files, so the condition the gate found at 8 and 85 entries is present again and larger. `gate-record.json`'s `state_now` node carries that re-census. Two things follow and both are stated: `raw/` holds **exactly the eight runner artifacts** it held then, member for member, so nothing illicit was added to the runner-only tree, and every addition is under `logs/`, which AAP §0.8.1 says accumulates this run's own durable evidence. **Neither fact clears the halt**, because the halt is about the trees being non-empty *before the run began* — a fact about an instant already past |
| The two `status_now: "resolved"` claims, **withdrawn 2026-09-03** | `gate-record.json` carried two `status_now: "resolved"` claims produced by the 2026-09-02 correction of the *other* halt. They were **withdrawn** on 2026-09-03 and their literal retained as `status_now_superseded_value` — falsified not by an error in the correction but by an event, the `2026-09-03T01:17:07Z` re-provisioning, which rebuilt the graph and so unfixed what the correction had fixed (**D4**). The withdrawal is recorded rather than the literal deleted, because what a record said is itself a fact |
| Disposition | **reported, not repaired, and not repairable here — nor repairable later.** The two trees are committed deliverables of this project, published by manifest in [§16](#16-manifest-of-the-two-git-ignored-artifact-trees), and AAP §0.8.1 and §0.9.2 forbid this run from creating or clearing either. Emptying them would destroy the run's evidence and still not make a measurement taken at a past moment true. The sharper point, stated because it is what distinguishes this entry from every other in the register: **an execution cannot be un-run.** Stages 2 through 5 ran after a verdict that authorised `nothing`, and no future act — not a re-provisioning, not a passing gate, not this document — retroactively supplies the precondition those executions lacked. What a re-provisioning buys is a *next* generation that is authorised, not a re-authorisation of this one |
| What a human must do | **Either** re-provision with `harness/artifacts/raw/` and `harness/artifacts/logs/` both present and empty, and re-execute from the gate forward so one gate pass authorises the stages that follow it; **or** accept the write-ordering divergence explicitly, in writing, as a recorded deviation from AAP §0.8.1. Until one of the two happens, every downstream figure here is true as a measurement and untrue as a compliant stage completion |
| Owner | `harness/artifacts/logs/gate-record.json` — `gate_verdict` (with `authorises` and `counts_by_verdict`), `halts[0]`, whose `measured`, `why_it_halts`, `disposition` and `consequence` fields carry the pre-write reading and the statements quoted above, and the entry in the 43-element `checks` array whose `check_id` is `gate.artifact_trees_exist_and_empty` |

### D1 — halt-class: the graph was not created by this run; a current-run graph was attempted and is blocked by a fixed toolchain bound

| Field | Value |
| --- | --- |
| Expected | AAP §0.1.1 and §0.5.1 — this run invokes the frontend over its own staged input set and writes the graph; *a graph already on disk is never accepted as this run's output* |
| Observed | The graph at `/opt/blitzy-harness/cpg/spark.cpg` was written by the provisioning invocation, before this run's first command. It remains the graph at that path |
| What was attempted, rather than deferred | This run assembled its complete input manifest — **191** own artifacts, **431,184,822** bytes, from all 38 JAR-packaging projects — asserted it total and injective in both directions and logged the assertion **before** invoking anything, proved a **128 GiB** heap committable with `-Xms`/`-Xmx`/`+AlwaysPreTouch`, and invoked the pinned `jimple2cpg` under JDK major 21 over the whole of it with `--recurse` and no exclusion of any kind |
| Outcome of the attempt | After **8 h 01 m** (28,863 s) and a **113.3 GiB** peak RSS, the frontend completed extraction and all AST passes and then terminated **in its persistence step** with `java.lang.OutOfMemoryError: Required array length 2147483639 + 72 is too large` raised inside `flatgraph.storage.WriterContext.finish` (`Serialization.scala:176`). **No graph was produced.** The truncated partial write it left — 691,541,019 bytes, sha256 `b1559c930a7b9ced717a0babf9a7e172d2b93d2cdef45a959304f063aedfe408` — is preserved as evidence in the run's private scratch under the name `spark.cpg.PARTIAL-TRUNCATED-DO-NOT-LOAD`, was never linked at `harness/cpg/spark.cpg`, and was loaded by nothing |
| Why the attempt cannot succeed at this input breadth | Established from the failing method's own bytecode, not inferred: `finish$$anonfun$2(ByteArrayOutputStream, IntBuffer, String)` UTF-8 encodes every string in the graph's deduplicated `stringpool` and appends it to **one** `java.io.ByteArrayOutputStream`, whose backing array cannot exceed `Integer.MAX_VALUE - 8` = 2,147,483,639 elements. The bound is on one array's length, so **no heap size moves it** — 128 GiB was committed and peak RSS was 113 GiB when it failed. That is now demonstrated rather than only reasoned: the retained probe was recompiled and run at **three heaps — 8 GiB, 64 GiB and 128 GiB, a 16× span** — and every arm buffered to exactly **2,147,483,639** bytes and threw `Required array length 2147483639 + 77 is too large`, the failure point unmoved. All three arms are verbatim in `harness/artifacts/logs/cpg-ceiling-reverify.log` under "THE PROBE, AND ALL THREE ARMS VERBATIM". The probe's `+ 77` and the frontend's `+ 72` are two distinct measurements of the same bound, differing only in the size of the string each was appending when it crossed it |
| Mitigations examined, and why each is unavailable | Checked against the frontend's actual flag surface, reproduced verbatim in `cpg-frontend.log` STEP 10. **Raising the heap** — irrelevant to a fixed array bound. **`--exclude` / `--exclude-regex`, or dropping pre-shade, `-tests` or shims artifacts** — the one lever that would work, and prohibited: AAP §0.5.1 requires every JAR retained by name and §0.9.2 lists trimming the input set among the conditions that stop the run. **Dropping `--recurse` or bounding `--depth`** — same class, and `--recurse` is mandated. **A newer frontend or flatgraph whose writer chunks the pool** — prohibited installation/substitution under §0.8.1. **Splitting the input and merging** — no merge exists in the pinned distribution, and a merged graph would carry the same string pool. **Building without persisting** — the plan requires the graph persisted, its identity recorded, and re-verified before every load, and the runners resolve it from disk by path |
| Disposition | Reported, not repaired, and nothing was trimmed to obtain a graph. The input set the AAP mandates and the writer the pinned frontend ships are not simultaneously satisfiable on any host, which makes this a property of the toolchain at this input breadth rather than of this host or this run |
| What it does not compromise | Delivery of every JAR the build produced is proven by the staging manifest independently of any graph. The identity in [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) is the identity of exactly the bytes each earlier stage loaded, measured from the file itself before each load |
| What it does compromise, stated plainly | There is no current-run graph, so **no current-run method, type-declaration or file count exists** and none is estimated from the provisioned graph's. And the **7** reactor JAR projects absent from the provisioned graph's input set — `sql/connect/shims`, `tools`, `examples` and the four `connector/kafka-0-10*` projects — have **no coverage verdict obtainable at all**, since no witness can be queried in a graph their bytecode is not in. **Nothing substitutes for that**: no narrowed or witness graph is presented as a stand-in, here or in `build-record.md` §6, and the gap is carried in [§14](#14-values-that-could-not-be-established) as a value that could not be established |
| What a human must do | **Decide the scope question, because nothing here is repairable by engineering under the AAP as written.** Three routes exist and each costs something stated: **(a)** accept that the graph mandated by AAP §0.1.1 and §0.5.1 cannot be produced at this pin and this input breadth, in writing, and accept the provisioned graph the Joern stages did load as the subject of every graph-derived figure — which is what this record already says it is, and which leaves twelve of the 38 JAR-producing modules without a coverage verdict (§6 of `build-record.md`); **(b)** amend AAP §0.3.2 and §0.9.2 to permit a named exclusion from the input set, which is the only mitigation measured to clear the bound — and note that **D20** records a superseded attempt that took exactly that route without authority, so the amendment would have to state which archives may be withheld and on what ground; or **(c)** authorise a frontend other than the pinned `jimple2cpg` 4.0.607, which AAP §0.4.3 forbids this run to install, and re-execute Stage 2 onward. Until one of the three is chosen, every graph-derived figure in this record is true of the graph that was loaded and is **not** a measurement of a graph this run built |
| A second reason it may not be repaired here, independent of the writer bound | `/opt/blitzy-harness/cpg/**` is **host-global and shared read-only across up to 64 concurrent clones**, and the clone instantiation instructions state that never rebuilding the graph in place is a condition of that sharing: a sibling reading those bytes mid-write gets a truncated result, and the graph's recorded identity would stop matching for everyone. So even a frontend that could serialize the complete set could not write it to the path the runners resolve, from here. This matters because it is what makes the condition *permanent* for a clone rather than merely blocked: the writer bound closes the engineering route and the sharing closes the location |
| What provisioning did on 2026-09-03, and why it is not this run's output | provisioning re-ran its own `jimple2cpg` invocation over `/opt/blitzy-harness/cpg-input --recurse -J-Xmx64g` between `01:40:31Z` and `02:11:54Z`, `FRONTEND_EXIT=0`, and replaced the graph with a new one — **547,980,224** bytes / `325887cf…3dc6`, over the same **62** archives from **31** modules. That is a **third-party write to a shared read-only artifact**, inherited exactly as its predecessor was, and it changes nothing about this entry: the input set is still narrower than the build produced (**D3**), no count from it is a current-run count, and the seven absent projects still have no obtainable verdict. What it did change is every figure taken over the predecessor, which is why lane `w424` re-executed each of them (**D4**) |
| Owner | `harness/artifacts/logs/cpg-frontend.log` — the whole file is this invocation's record; `/opt/blitzy-harness/provision-log/cpg-record.txt` for provisioning's own 2026-09-03 write |

### D2 — halt-class: the taint A/B did not discriminate

| Field | Value |
| --- | --- |
| Expected | one traced finding at `DiskStore.scala` line 72 with taint on and **zero** with it off, from two invocations differing only in that setting |
| Observed | **1** finding in `DiskStore.scala` at its line 72 with the flag and **1** at that same line without it, both traced, artifacts byte-identical at 4,753 bytes / sha256 `7949617b…5778` |
| Disposition | reported and not repaired; nothing was retried, narrowed or re-flagged to obtain the expected zero. [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured) carries the mechanical reason and the engine limit |
| What it is not | evidence that the engine is inert. §7.3 measures the same rule discriminating **2 findings against 0** on `sql/hive/src/main/scala/org/apache/spark/sql/hive/client/HiveShim.scala` with the same one flag, and §7.4's two controls show the anchor's line-72 result is source-driven rather than a pattern match. The engine is active; this subject cannot show it, for the reason §7.2 measures |
| What a human must do | **Decide which anchor the pass condition is held to.** AAP §0.9.1 names one traced finding at `DiskStore.scala:72` with taint on and none with it off, and §0.9.2 makes a failed A/B a stop condition; the measurement is that the mandated anchor **does not discriminate** — one finding at line 72 in both arms, byte-identical artifacts, and still byte-identical with the whole ruleset loaded. Two routes: **(a)** accept that the mandated anchor cannot discriminate on this file with this ruleset, in writing, and accept the discriminating pair this run measured on Spark's own Scala in its place — `HiveShim.scala`, two findings with taint on at lines 828 and 834 and none with it off, with its two controls, all in [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured); or **(b)** nominate a different anchor file and line and have the A/B re-executed against it. Nothing here is repairable by engineering: the arms differ only in the taint setting, which is what the pass condition requires, and no runner or ruleset may be edited (AAP §0.8.1) |
| Owner | `harness/artifacts/logs/taint-ab-off.log`, its `VERDICT FOR THE PAIR` block at line 91, which states the anchor unmet and non-discriminating in the arm's own words; the arms themselves are the `taint-ab-anchor-diskstore-` pair. An earlier edition cited "divergence D1 in that file", a label that appears nowhere in it — the log carries no `D1` — so the citation is repointed at the block that actually carries the verdict |

### D3 — the graph's input set is narrower than the build produced

| Field | Value |
| --- | --- |
| Expected | the graph built over **every** JAR the build produced, nothing trimmed: this run's inventory staged **191** own artifacts, **431,184,822** bytes, from all **38** JAR-packaging projects, and proved the mapping total and injective in both directions |
| Observed | the loaded graph's input path held **62** archives, **285,122,375** bytes, from **31** modules — the total re-established on 2026-09-03 and true three ways (the sum of the 62 members, `du -sb` over the staged tree, and the write-time record `cpg-record.txt`); an earlier edition stated 285,122,371, and the three verbatim streams that still carry that figure are named in **D8**. The exclusion ledger is a **separate** record with a **separate** owner: `harness/artifacts/logs/cpg-graph-record.log` states that of 252 `.jar` files under the build tree, **190 were excluded with a reason per _category_ — not per file** (77 copied dependency, 64 sources, 33 `-tests`, 14 test-fixture, 2 `spark-connect-shims`; 77 + 64 + 33 + 14 + 2 = 190, and 62 + 190 = 252). `cpg-input-inventory.json` holds **no exclusion ledger at all**, so no exclusion count may be cited from it; an earlier edition of this row attributed the 190 to that file and described the reasons as per-file, and both halves of that were wrong. No per-file exclusion ledger was retained by any lane, so none can be cited from anywhere |
| Consequence, stated so no count is misread | seven of the 38 JAR-producing modules therefore have **no coverage verdict obtainable** from this graph, and no finding on it can resolve into a `src/test` tree, every `-tests` archive being absent from it. A graph over the wider set cannot have *fewer* methods than one over the narrower, which is why the method count is a one-sided floor rather than a window |
| Disposition | recorded with both values; **neither input set was trimmed or padded and no count was adjusted to make the two agree**. The wider set was not merely inventoried — it was supplied to the frontend in full, and D1 records what happened when it was |
| Owner | `cpg-frontend.log`, with the coverage consequence measured in `cpg-verify.log` and the verdict owned by `build-record.md` §6 |

### D4 — the record's stated graph identity was contradicted by the bytes on disk, twice, and the graph was rebuilt under the first correction

| Field | Value |
| --- | --- |
| Expected | one graph, one identity, for every load of the run — and the record describing it agreeing with the bytes |
| Observed, on the loads of record | **one** identity across all seven loads of 2026-09-03. The three `importCpg` verification loads, the Stage 3 Joern runner and all three Stage 5 probe queries each re-measured the resolved target immediately before reading it and each got **547,980,224 bytes / `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`**. The resolved file's mtime, `2026-09-03 02:11:49Z`, precedes every one of those checks and did not change across them, so the bytes each load read are the same bytes |
| Observed, on the loads of the superseded generation | **one** identity across all five loads of 2026-09-01, and a *different* one: **541,309,809 / `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`**, at mtime `2026-08-30 19:18:37Z`, with counts 1,396,899 / 119,721 / 45,037. Those bytes are on no disk this checkout can reach; the figures are retained with their generation and are not averaged with the pair above |
| Observed, on the record | `harness/ENVIRONMENT.md` §7 **stated** a **third** graph at the gate: **541,255,894 bytes / `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`**, with 1,397,339 methods (internal 1,307,552), 119,691 type declarations and 45,037 files. **Since 2026-09-03 it states the graph on disk**: 547,980,224 / `325887cf…3dc6`, 1,398,964 methods (internal 1,308,974), 119,860 type declarations, 45,037 files, `-J-Xmx64g` at a 61 GB peak RSS in 31 m 23 s — every field taken from the write-time record of account, with every superseded value retained in that document's supersession appendix. It is cited by section here and never by line, because it is under concurrent re-anchoring in this checkpoint and its line numbers move |
| Where the contradiction lay, measured rather than assumed | **Not** between the disk and every provisioned record — only between the disk and `harness/ENVIRONMENT.md` §7. The bytes on disk have their own record of account beside them: `/opt/blitzy-harness/provision-log/cpg-identity.txt` states `547980224 325887cf6c65…` on one line and `cpg-record.txt` states the same pair with its command, JDK 21 at `heap_max_bytes` 68,719,476,736, `-J-Xmx64g`, 31 m 23 s and `FRONTEND_EXIT=0`. Both were read; they **agree** with each other and with the bytes. §7 was the record that did not |
| Cause | The host was re-provisioned **twice** — on `2026-08-30` and again at `2026-09-03T01:17:07Z` — and each time the frontend rebuilt the graph while the environment record went on describing the graph the rebuild had replaced. It is a **stale inherited record**, twice over, and never an unexplained mid-run replacement: each set of loads is internally consistent and each is attributed to the generation whose bytes it read |
| Disposition | **Halt-class when the gate measured it; RE-ANCHORED at the record rather than at the graph — on 2026-09-02, and again on 2026-09-03 after the rebuild falsified the first.** The gate's original reading, that this was AAP §0.1.3's fourth case and so had to be reported rather than repaired, rested on a premise that is false: the fourth case applies only where *no anchor exists to adjudicate between* the record and the observation, and one does. `/opt/blitzy-harness/provision-log/cpg-record.txt`, written beside the bytes, carries an expected-versus-observed block that names both pairs, labels the record's figures `PRIOR` and the filesystem's `NOW`, and states the cause — so this run was told by the writer of the bytes which statement describes them, and did not have to choose silently. **The graph itself was never touched at any point in either correction**: no graph was written, replaced, moved or re-pointed to make a record agree, nothing was written under `/opt/blitzy-harness`, and the bytes were read and hashed only. `gate-record.json` retains its own reading unaltered, records the 2026-09-02 correction, **withdraws** the `status_now: resolved` claim that correction produced — keeping its literal as `status_now_superseded_value`, because the withdrawal is a fact about this record rather than a licence to erase what it said — and records the 2026-09-03 re-anchoring beside it. **Its other halt, the non-empty artifact trees, is untouched and still live, so `gate_verdict.overall` remains `halt` and no stage is retrospectively authorised** |
| What the second correction cost, stated because it is the reason this row is long | A record re-anchored to a graph that is then rebuilt is a record that goes stale **silently**: nothing in the pipeline announces a re-provisioning, and the only signal was a gate refusing to authorise a load. Between `2026-09-03T01:17:07Z` and the re-anchoring, **`harness/lib/preflight_graph_identity.py --check-only` exited 77 with `VERDICT: HALT`** naming `harness/ENVIRONMENT.md` §7 as the sole dissenting record, and **`./harness/bin/run-joern.sh` exited 78 CONFIGURATION FAULT without loading anything** — no artifact written and none removed. That transition, from a passing gate to a refusing one across nothing but a change of bytes, is the evidence that the control is load-bearing rather than decorative, and it is why the deadlock could not be cleared by editing one document alone |
| The carve-out that does **not** apply, and why | The gate classified the graph's size, digest and counts as **deliberately-replaced** fields, on the premise that this run would replace the graph — and on that premise AAP §0.1.3's exclusion of "outputs this run deliberately replaces" would apply, since reading an intentional replacement as a contradiction would halt the run for succeeding. **D1 records that this run did not replace the graph and, at this input breadth, cannot.** With the premise gone the carve-out does not reach these fields, and what remains is an inherited artefact whose recorded identity observation contradicts |
| What adjudicates every load | `harness/lib/preflight_graph_identity.py`, which recomputes both values from the bytes with the symlink followed and exits **77** on any mismatch. It reads **every** record of account rather than one: this checkout's frontend log where it carries a write-time pair, the provisioning record beside the resolved graph, and — since the security checkpoint of 2026-09-02 — **always** the identity declared by `harness/ENVIRONMENT.md` §7. Records that disagree are fatal and none is preferred for matching. Since 2026-09-03 it also adjudicates the graph's **method count** against AAP §0.9.2's floor, declaring `METHOD_COUNT_FLOOR` at its line 331 and reporting `Adjudicated count : 1,398,964 methods, agreed by 2 record(s) of account` with `Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420`. It ran for this run's Stage 3, **before** the load, and printed **`VERDICT: PASS`** at `joern-preflight.log` line 96; the record it adjudicated against — `harness/artifacts/logs/cpg-identity.txt` — was produced by calling that same module, so the gate and its record cannot state different pairs |
| Where the binding lives, and where it does **not** | **Not in the runner.** `harness/bin/run-joern.sh` prints its input's identity at its lines 56-58 and compares nothing, and that is its provisioned behaviour. A 2026-09-02 edit had put the `--check-only` invocation and an exit-78 refusal inside it; **the edit was reverted on 2026-09-03** to the runner's provisioned 3,380 bytes, because AAP §0.6.1 marks every entry in `harness/bin/` REFERENCE and §0.8.1 states that no runner file is edited — so the defect is recorded and its provisioning patch published (**D26**) rather than repaired in place. The gate's own report says as much, naming that runner as **not** a caller. The binding therefore lives in two places: `harness/lib/run-joern-gated.sh`, which has no branch reaching the runner after a non-zero gate; and **this run's own invocation sequence**, which runs both gates outside every runner and immediately before the Stage 3 load. A direct invocation of the runner by a third party is **not** bound by either, and that is stated plainly rather than left to be inferred |
| Where the Stage 3 adjudication sits relative to its load, stated because AAP §0.8.2 is specific about the ordering | **On the invocation of record the ordering holds.** `joern-preflight.log` is stamped `Checked at (UTC) 2026-09-03T09:07:46Z` with `Clone index 424`, and the invocation it adjudicates started `09:07:47Z` — one second later — so §0.8.2's "recomputed and compared immediately before every load" is satisfied for the load whose artifact this run publishes. **On the superseded 2026-09-01 invocation it did not.** There the measurement was contemporaneous — the runner printed the pair on its console, and `runner-sequence.json` independently recorded it as `graph_identity_before_load` at `14:25:10Z` and `graph_identity_after_load` at `14:41:24Z` — while the comparison, that day's `joern-preflight.log`, was stamped `14:52:54Z` with `Clone index 0`, about 11½ minutes after the load and in a different clone. The measured pair equalled the record at both moments and the graph's mtime preceded both, so no substitution occurred there either: what failed was the control, not the outcome, and it cannot be changed retroactively |
| A claim in that log's earlier copy, retained as history | The PASS-era copy of `joern-preflight.log` described `harness/lib/run-joern-gated.sh` as "the only committed execution path for Stage 3". Neither load took it: `argv=["./harness/bin/run-joern.sh"]` in `joern.runner-console.log` line 3 and `runner-sequence.json` for the first, and in `runner-metadata.json` field `tools.joern.stage3_invocation_2026_09_03.invocation` for the second. **Superseded by the mechanism rather than by an edit**: the gate rewrites its own report whenever it is run in its publishing form, so the committed copy now enumerates its binding callers accurately, names the runner as not one of them, and states that a mismatch against every record of account is fatal. The "only committed execution path" claim occurs **0** times in the committed file. The argv fact this row records is unaffected and still true |
| Counts attribution | The counts in [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) are **this run's own measurement** of the bytes it loaded, taken by three independent JVMs on 2026-09-03 and agreeing on all four values, and not a restatement of any record. Every differing set of counts is quoted with the generation and the record it belongs to |
| The adjudicating gate fails closed on this contradiction — and it demonstrated it | The security testing run raised this as **SEC-02**, and its observation was sharper than the rows above: the gate *passed* while the contradiction stood, because resolving the record of account **by provenance** meant `harness/ENVIRONMENT.md` §7 — the one record that disagreed — was never among the candidates it read. A control that cannot see the contradicting record cannot halt on it. §7 was therefore made an **unconditional third record candidate**, with an unreadable or ambiguous §7 becoming its own configuration fault at exit **78** rather than a silent skip. That change was then exercised for real rather than only on a copy: after the 2026-09-03 rebuild the gate exited **77**, `VERDICT: HALT`, naming §7 as the sole dissenting record, and the Joern runner exited **78** without loading anything — and after the re-anchoring the same gate exits **0** with `VERDICT: PASS` against the same unchanged bytes. **PASS, then HALT, then PASS**, with the code constant and only the record moving, is the control working |
| Both halves are in force together, which is what closes the row | The strengthened gate and the corrected record are **not** alternatives: the gate reads §7 unconditionally *and* §7 describes the graph on disk, so the gate exits **0** with `VERDICT: PASS` against the record it previously could not see, and the method-count floor is adjudicated in the same act. `harness/lib/run-joern-gated.sh` remains a four-step wrapper — scan-target gate, graph-identity gate, heap validation with a contemporaneous commit proof, then the runner — and with the contradiction closed all four are reachable. The direct `./harness/bin/run-joern.sh` path is **not** self-binding, which is why this run runs both gates itself, in sequence, before invoking it |
| What a human must do | **Nothing for the identity itself — route (a) has been taken, twice.** `harness/ENVIRONMENT.md` §7 now describes the graph actually on disk (547,980,224 / `325887cf…3dc6`) and agrees with the write-time owner on every field, so the record and the filesystem no longer disagree and the gate that measures their agreement finds it. Three things do remain for a human, and none is this divergence: **(i)** the gate cannot be *re-run to a pass* from inside this run, because its other stopping condition — both artifact trees non-empty before the run began — is one this run may neither create nor clear (AAP §0.8.1), so `gate_verdict.overall` stays `halt` until a provisioning presents empty trees; **(ii)** correcting the record is a departure from AAP §0.6.1, which marks `harness/ENVIRONMENT.md` REFERENCE — taken deliberately, because `git log -- harness/` shows the file authored and committed by `Blitzy Agent <agent@blitzy.com>`, so it is this project's own record *describing* the provisioning rather than inherited provisioning surface, and leaving it false meant every load running against a record describing a different graph and, once the gate was made to read it, every gated load halting on a record nobody had corrected; and **(iii)** nothing in this pipeline announces a re-provisioning, so a third rebuild would go stale silently again until a gate refused. A human who disagrees with (ii) should revert the document and accept the stale record in writing instead — every superseded value is preserved in its appendix, so the revert loses nothing, but it re-opens the gate halt this correction cleared |

### D5 — the six JAR producers the expected-values table does not name

| Field | Value |
| --- | --- |
| Expected | the table names **32** JAR producers, measured over the narrowed 33-project provisioning build |
| Observed | a full reactor packages **38**, so six are new to it: `tools`, `examples`, `connector/kafka-0-10-token-provider`, `connector/kafka-0-10`, `connector/kafka-0-10-sql`, `connector/kafka-0-10-assembly`. All six appear as `SUCCESS` in Maven's reactor summary and all six produced their own main artifact on disk |
| Disposition | **a recorded difference, never a halt.** The halt rule is one-directional (AAP §0.8.3): a module that produced a JAR in the rehearsal and produces none now stops the run; the reverse does not. The six legitimately entered this run's staged input set, and they are why the method count is checked as a floor rather than a window |
| What establishes that all six produced an artifact | **`build-reactor.log` STEP 13, measured at build time, and nothing about the tree's present contents.** STEP 13 records `jar-packaging projects : 38`, `jar-packaging with their own MAIN artifact: 38` and `jar-packaging WITHOUT one : []`, with Maven's own `BUILD SUCCESS` over all 40 reactor projects. That is an immutable record in a file that exists, and it is the whole basis of the claim |
| Why this record states no present-state census of `$SPARK_SRC` | An earlier edition of this row said "their build products are no longer on disk, and the other 32 are — re-checking every artifact path `build-record.md` §3 cites against `$SPARK_SRC` **today**: 32 of 38 present, 6 absent". That was doubly unsound. First it was **undated**: "today" fixes nothing a later reader can check, and `$SPARK_SRC` is host-global, shared read-only with ~45 concurrent clones, and rebuilt by re-provisioning, so its contents are not a property of this run. Second it **no longer holds**: re-measuring the same 38 paths at **2026-09-02T16:37:27Z** and again at **16:37:30Z** returned **38 of 38 present, 0 absent** both times. So the figure changed underneath the document without anything in this run changing, which is exactly what makes a present-state census of a mutable shared tree unfit to be published as evidence. The census is therefore withdrawn rather than restated with a fresher number, because a fresher number would drift again. `build-record.md` §3 cites each path as *what the build produced*, sourced from `build-reactor.log` STEP 13 at build time; those citations are historical measurements and remain correct regardless of what the tree holds now |
| The one related fact that does not depend on the tree's present state | `sql/connect/shims` is absent from the **graph input**, which **D3** carries, and that absence is a property of the recorded 62-archive staging tree rather than of `$SPARK_SRC` |
| Owner | `build-record.md` §3 |

### D6 — runtime topology as read, not as expected

Read from the provisioned runners at the gate and recorded in
`runner-metadata.json`; no expectation is asserted as a fact about this
provisioning.

| Field | What a reader would have expected | What was read |
| --- | --- | --- |
| `dependency-check`'s JDK | the harness precedent exported `JAVA_HOME_21`, and the AAP anticipated this tool on **21** | **17** — the runner `harness/bin/run-dependency-check.sh` invokes it at its line 51 with `JAVA_HOME="$JAVA_HOME"`, which `harness/env.sh` line 47 sets to the 17 JDK, and its own header states *"Runs under JAVA_HOME (Temurin 17) — JDK 21 is reserved for Joern"*. Measured rather than inferred precisely because it is the opposite of the precedent |
| The Python-based scanners' environments | possibly one shared interpreter environment | **two separate virtualenvs**, `/opt/blitzy-tools/venvs/semgrep/bin/python` and `/opt/blitzy-tools/venvs/checkov/bin/python`, each reporting 3.13.7 — Semgrep and Checkov cannot share one, their dependencies conflicting. Separately, `gitleaks` and `trivy` **do** share `/usr/bin/python3` for post-processing only and host no scanner in it |
| Which tools involve a JVM at all | — | only `dependency-check` (17) and `joern` (21); the other seven record `jdk_major` null |
| Which tools involve an interpreter at all | — | `semgrep`, `checkov`, and `gitleaks`/`trivy` for post-processing; `opengrep`, `datadog-static-analyzer`, `osv-scanner`, `dependency-check` and `joern` involve none |
| `--disableOssIndex` | the harness precedent did not pass it | **passed** by this provisioning's runner, so the disabling is the harness's own and recorded rather than incidental; `--noupdate` is passed as well |
| The default `java` | — | the **17** JDK, `harness/env.sh` lines 58–61 prepending `$JAVA_HOME/bin` to `PATH`; Joern's wrappers override to 21 for their own invocations |

### D7 — ruleset and feed identities

Four field-level differences, each recorded with both values, each marking its
tool **not comparable with the rehearsal**, and none halting (AAP §0.9.3): a
different rule set or feed produces a different count for reasons that have nothing
to do with the code.

| Field | Expected | Observed | Tool |
| --- | --- | --- | --- |
| datadog SAST config sha256, ruleset count and rule count | table `e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7`, **48** rulesets, **1,093** rules | **`c5fd464c2985119574f23599d44022e22b9442d7083acb17ec84addba354f322`, 53 rulesets, 1,147 rules** — all three differ, and the inherited `harness/ENVIRONMENT.md` states a third digest again (`4f397e81…`) with the table's 48/1,093. The **table governs**; all values are recorded | `datadog-static-analyzer` |
| Trivy vulnerability DB `UpdatedAt` | `2026-08-23T06:56:50Z` | **`2026-08-30T13:05:01.49156526Z`** | `trivy` |
| Trivy java DB `UpdatedAt` | `2026-08-23T01:05:59Z` | **`2026-08-30T01:07:49.364681226Z`** | `trivy` |
| Dependency-Check NVD API Last Modified | `2026-08-23T08:00:06-04` | **`2026-08-30T12:00:19-04`**, a 260,005,888-byte `odc.mv.db` | `dependency-check` |

A fifth field-level difference sits inside a **passing** check: the size of
Joern's *default* query bundle, expected **58** and observed **59**
(`harness/lib/joern-scan.sc` line 3). The authoritative field — the **baked** count
of **6** — matches, so nothing downstream changes; it is listed so the register is
complete. Ruleset identities that **matched** exactly: opengrep-rules commit
`f1d2b562b414783763fd02a6ed2736eaed622efa` with 2,006 rules, semgrep-rules commit
`40b8c63f75dc7c22c8a77482d73bfb864b146f7e` with 2,149 rules and 19 Pro-only
skipped, and OSV-Scanner's no-local-database data source.

### D8 — tool versions: no difference, against an anticipated one

| Field | Value |
| --- | --- |
| Expected | the AAP anticipated two drifts from the historical provisioning — Semgrep **1.174.0** against the pinned 1.173.0, and Checkov **3.3.13** against the pinned 3.3.12 |
| Observed | **Semgrep 1.173.0 and Checkov 3.3.12** — both equal to their pins, as are the other seven tools, both JDKs, Maven, Scala and Python |
| Disposition | recorded as the measurement rather than as the anticipation. No tool version differs from its pin in this provisioning, so no version was corrected by installing anything, and nothing was installed, upgraded or substituted anywhere in this run |

### D9 — the frontend's two observed metrics, and the provenance limitation

| Metric | Expected | Observed |
| --- | --- | --- |
| Duplicate-entry overwrite warnings | ~5,700 | **33,784** warnings over **27,843** distinct destination entries |
| AST-creation failures | ~36, protobuf-generated `connect.proto.*` classes | **23** over 23 distinct classes, every one `java.lang.RuntimeException: Chain already contains object: <fqcn>` raised from `soot.util.HashChain.addLast`, of which **5** are protobuf-generated `org/apache/spark/connect/proto` classes and the other 18 are `sql/connect` client and Arrow classes |

Both figures are this run's own frontend invocation measured over its **complete
191-artifact input set** — the invocation D1 records — and not the provisioning
invocation's. Both are grouped by the module and artifact the affected entries are
**contained in**. The measured cause of the overwrite gap is each module
contributing both its shaded artifact and its `original-` pre-shade sibling:
**17,288** of the 27,843 distinct destinations are duplicated between two artifacts
of one module, and **10,161** are shared across more than one module. A further
**403** destinations match no top-level entry of any staged artifact and are
accounted for individually rather than rounded away: 12 staged artifacts carry 28
nested `.jar` test fixtures between them and `--recurse` descends into those, which
yields 350 junit-framework classes, 42 test-fixture classes, 6 nested `META-INF`
descriptors and 5 multi-release `module-info.class` — **350 + 42 + 6 + 5 = 403**,
the figure `cpg-frontend.log` line 258 owns as
`(entry not present in any own artifact — extracted from a nested archive)`. This
row previously published 377 against its own components summing to 403; the
components and the owner agree, so 403 is the figure and the subtotal is now
rendered from the owner rather than restated. Neither figure is treated as
acceptable because a document expected another number.

**The limitation, stated rather than worked around: per-class provenance for
overwritten classes could not be established from this frontend's output.** The
frontend's directory walk is not ordered by this run, and its overwrite report
names the *destination* class rather than the JAR that supplied the surviving
definition. **No winner map is claimed anywhere**, and none is inferred from the
containment tables, which answer a different question and are labelled as such.
What is reproducible instead is the input set of the graph actually loaded, fixed
byte for byte by `harness/artifacts/logs/cpg-input-inventory.json` — schema
`cpg-input-inventory/2.0.0`, **62** archives, 285,122,375 bytes, 62 distinct sha256,
the mapping injective in both directions. **Per-entry identity for the 191-archive
set this run's own frontend was given is no longer recoverable from this tree, and
that is stated rather than papered over**: the aggregate survives — 191 archives,
431,184,822 bytes, from `build-reactor.log` STEP 13 and `cpg-frontend.log` STEP 1 —
but the per-entry manifest that `cpg-frontend.log` STEP 1 points at ("assertion
recorded — see cpg-input-inventory.json") was regenerated to describe the 62-archive
set, and `MANIFEST.json` carries no `cpg_input_attempt1` key; an earlier edition of
this entry cited one that does not exist. The gap is carried in
[§14](#14-values-that-could-not-be-established), and no per-entry figure for the
191-archive set is stated anywhere in this record.

### D10 — the copied-dependency exclusion count

From `harness/artifacts/logs/build-reactor.log` **STEP 13**, which is the owner of
this measurement and the file `build-record.md` §5 cites for it: **627** JAR files
enumerated under the 40 projects' build directories → **191** own artifacts staged,
**436** excluded, **0** undecided (191 + 436 + 0 = 627). The exclusions are **422
copied runtime dependencies**, the `copied_runtime_dependency` class, and **14
test-resource fixtures**. It is **not** in `cpg-input-inventory.json`, which
describes the 62-archive input set of the graph actually loaded rather than the
191-archive set this run's own frontend was given. The count
is recorded so the exclusion is visible rather than silent; provenance rather than
location decided each classification, because `copy-module-dependencies` writes
copied dependencies into the same directory two projects send their own artifacts
to.

### D11 — fields the record states and the expected-values table does not

Five fields fall in AAP §0.1.3's **third** case: `harness/ENVIRONMENT.md` states
them, the expected-values table carries no row for them, and observation **agrees**
with the record. Both values are recorded, each field is marked **unanchored**, and
the run continues. Had observation *contradicted* the record on any of them, that
would have been the fourth case and the run would have **halted** — both values
recorded with no anchor to adjudicate between them. No such contradiction arose.

| Unanchored field | Recorded and observed |
| --- | --- |
| Per-tool runtime topology | as D6 sets out — `harness/ENVIRONMENT.md`, and AAP §0.4.1 explicitly declines to fix it and directs that it be read at the gate |
| The default `java` assignment | the 17 JDK |
| `HARNESS_SMOKE_TARGET`'s existence and unset state | exists, deliberately unset, and setting it would redirect every runner at one small directory |
| Host OS, CPU count and total memory, and the 96 GB headroom arm | Ubuntu 25.10, 4 vCPU, `MemTotal` 4,029,526,772 kB (≈ 3.75 TiB), and the record's `-Xms96g -Xmx96g` exiting 0. Recorded as **context only** — the heap test is the pre-touch proof in [§1](#1-gate-verdicts), not this |
| `git`'s version | 2.51.0 |

### D12 — the shims stub-displacement hazard, measured absent from the graph in use

| Field | Value |
| --- | --- |
| The hazard | `sql/connect/shims` ships eleven classes — `SparkConf`, `SparkContext`, `rdd.RDD`, `api.java.JavaRDD`, `sql.ExperimentalMethods`, `sql.SparkSessionExtensions`, `sql.execution.QueryExecution`, `sql.internal.SessionState`, `sql.internal.SharedState`, `sql.sources.BaseRelation` and `sql.util.ExecutionListenerManager` — as **signature-only stubs** for client-only builds. Core and the SQL modules ship the real ones under the same full names. In a graph containing both, the frontend resolves each collision by replacement, so a query about one of those classes can be answered by a stub |
| How it was measured | By **querying the graph for each class and reporting what is there** — the route AAP §0.5.1 prescribes for a collision that bears on a conclusion — rather than by inferring a winner from the frontend's output, which is not possible (**D9**). One `importCpg` load of the graph at the sanctioned path, JDK major 21 at `-J-Xmx64g`, identity re-verified before the load and re-measured unchanged after it |
| Observed, first-hand | **Every one of the eleven carries its real implementation**: `SparkConf` **298** methods, `SparkContext` **1,100**, `rdd.RDD` **1,022**, `sql.execution.QueryExecution` **280**, `sql.internal.SharedState` **128**, `sql.util.ExecutionListenerManager` **116**, `sql.SparkSessionExtensions` **112**, `sql.internal.SessionState` **76**, `api.java.JavaRDD` **74**, `sql.sources.BaseRelation` **12**, `sql.ExperimentalMethods` **7**. A stub would be single digits across the board |
| Why | The graph this run loaded was built over an input set that **excludes both shims archives** — `sql/connect/shims` is one of the seven reactor JAR projects absent from it (**D3**) — so no stub displaced anything and **no conclusion in this record is answered by a stub** |
| What this does **not** establish | Not a winner map, and none is claimed: it states what the graph contains, not which archive the frontend read last, and it is measured for these eleven classes only. And nothing about a graph that **does** contain the shims archive — the earlier lane that measured that side did so against a narrowed graph which does not exist in this generation, so its stub-side counts are neither re-measured here nor restated as this run's |
| Disposition | **Recorded, and it is a forward cost rather than a live defect.** The graph AAP §0.5.1 mandates — over every JAR the build produced — **would** contain the shims artifact, so the hazard attaches to that unmet requirement (**D1**) rather than to the graph in use. The AAP is explicit that every JAR is retained by name, "not the connect-shims artifact", and §0.9.2 lists trimming among the halt conditions, so the hazard is reported rather than acted on. It corroborates the provisioning runbook's own instruction to exclude that archive, and it names a real cost of the input set the AAP mandates |
| Owner | `harness/artifacts/logs/cpg-shims-collision-measurement.log` — the query verbatim with its source digest, the identity checked before and after, and the eleven measured rows, **all taken over the graph generation of 2026-09-01**, where that load re-measured the whole-graph method count at **1,396,899** in agreement with that generation's other two loads. Those rows are retained as that generation's. **The hazard was re-measured over the graph on record by query on 2026-09-03** and `oss-scan-results/build-record.md` owns the result — `SparkConf` 2 of 298, `SparkContext` 2 of 1,100 and `rdd.RDD` 2 of 1,022 — beside the whole-graph count of **1,398,964** that `cpg-verify.log` PART 3 records for all three of its loads. The conclusion is unchanged in both generations: the stub definitions do not displace the real ones |

### D13 — RESOLVED: a commit deleted sixteen delivered files; all sixteen are restored

| Field | Value |
| --- | --- |
| What happened | Commit `232d0d9cca3` deleted **sixteen** files that earlier lanes of this run had built and committed: the **thirteen provisioned harness files** — `harness/ENVIRONMENT.md`, `harness/env.sh`, `harness/lib/scope.sh`, `harness/lib/joern-scan.sc` and all nine `harness/bin/run-*.sh` runners — and **three members of `harness/artifacts/logs/`**: `datadog-static-analyzer.console.log` (1,117 bytes), `joern.preflight.log` (16,443 bytes) and `joern.runner.console.log` (1,428 bytes) |
| Why each class is a defect | The thirteen are the **provisioned surface**, which AAP §0.6.1 marks REFERENCE — read, never written, never deleted — and without them nothing in this checkout can be run at all: no runner, no environment to source, no scope contract. The three logs are output the run built, which AAP §0.8.1 requires to stay where it is, and one of them was **cited evidence**: the *enriched* `joern.status` of the generation then current named `joern.preflight.log` PART 2 at line 147 with its verdict at line 167, so the deletion left a mandated check citing a file a reader could not open, against AAP §0.9.4. **That citation no longer exists to be broken**: commit `0e3e742a5ad` replaced all nine statuses with the runners' verbatim seven-line `scope_finish` trailer, which carries no evidence-file reference of any kind. The defect and its repair are both real; only the citing file changed underneath them |
| What was done | **All sixteen restored from `232d0d9cca3^`**, each verified byte-for-byte against that commit's blob hash, and for the thirteen also against file mode (`100755` for the executables, `100644` for `ENVIRONMENT.md` and `joern-scan.sc`). The restored surface was then exercised rather than assumed: `harness/env.sh` sources cleanly in a fresh non-login shell and resolves `joern`, `JAVA_HOME_21` and `HARNESS_CPG`; all nine runners still exit **64** on an argument without scanning; `harness/lib/run-joern-gated.sh`'s references resolve. The restored `joern.preflight.log` is **321 lines**, carrying its comparison at line 147 and `VERDICT: PASS` at line 167 exactly as described. One property of it must travel with every citation: it is the **2026-08-24 lane's** gate report, and the identity it adjudicates is that lane's **superseded** pair `541255894` / `26d327cc…` at its line 149 — **not** the graph this run loaded. The current pair is adjudicated by the 44-line hyphen-form `joern-preflight.log`, which is a different file, and [§5](#the-graphs-byte-size-and-sha256-and-the-identity-re-verified-before-every-load) and **D4** cite that one |
| What an earlier revision recorded here instead | An unresolved conflict, on the premise that a review finding required those three logs removed while AAP §0.8.1, §0.1.1 and §0.9.4 required them kept. That framing is superseded: the files were not being *retained* against a finding, they had been *deleted*, and restoring them satisfies every rule the entry cited without breaking anything. The reading that the three names were mis-transcriptions is also superseded — git history shows the three restored files existed at `232d0d9cca3^` while the three similarly-named files did not, so the two sets are six distinct files from two lanes rather than three files under two spellings (**D14**) |
| What the deletion did to this document | [§16](#16-manifest-of-the-two-git-ignored-artifact-trees) had been written while the three logs were present, so its 122-member `logs` inventory was correct and the tree was not; the totals, the tracked-file accounting and the tree-state row in [§11](#11-deliverable-inventory-with-resolved-absolute-paths) all read against a tree three files short. With the restoration, `harness/artifacts/MANIFEST.json` and §16 are regenerated from disk in one pass and agree with it member for member |
| Status | **RESOLVED.** Sixteen files restored and verified, the evidence chain reconnected, and the published inventory regenerated from the tree it describes |
| Residue | None. Nothing is left cited-but-absent, and nothing was deleted to make a count agree. **Four of the thirteen have since been edited deliberately** — `harness/ENVIRONMENT.md`, `harness/bin/run-joern.sh`, `harness/lib/joern-scan.sc` and `harness/lib/scope.sh` is **not** among them — see **D25**, which records that departure from the same §0.6.1 marking this entry cites and gives the reason for each |

### D14 — the evidence trees hold more than one execution lane, and what was aligned against what

| Field | Value |
| --- | --- |
| Expected | one execution lane: every per-tool artifact, status record and stream written by the same invocation, and one graph identity across the run |
| Observed, and superseded for the nine tools | `logs/` holds the **union** of several lanes and still does. **The nine-tool evidence, however, is now one lane** and is no longer a union: `harness/artifacts/logs/runner-sequence.json` binds every one of the nine invocations to **its own** artifact, stdout log, stderr log, `.status` file and runner console log by byte size and sha256, measured immediately after each invocation returned — **44** top-level pieces plus **38** side-artifact members, **82** in all, and re-measuring all 82 against disk for this record gives **zero mismatches**. So the misalignment this entry was written to record — a status file sitting beside a stream from a different invocation — is no longer a state this tree can be in for those nine tools without the binding detecting it. What the earlier edition measured before alignment (**6** status-named stream identities disagreeing with the file beside them) is that generation's measurement and is retained as history, not as a current reading |
| The rule applied | each tool's evidence must come from the lane whose artifact the dataset was normalized from. The dataset is derived from `raw/`, so `raw/` fixes the lineage; the status records and streams are aligned to it — not the reverse, because aligning the artifacts would mean re-normalizing the dataset from bytes no record describes. The delivered lane satisfies this by construction: `raw/` and the streams and statuses beside it are all bound to the same nine invocations |
| What was aligned, in the generation that needed aligning | **12** stream files restored to that lineage: `checkov.stdout.log`, `datadog-static-analyzer.stdout.log`, `dependency-check.stdout.log`, `gitleaks.{stdout,stderr}.log`, `joern.{stdout,stderr}.log`, `opengrep.stdout.log`, `semgrep.{stdout,stderr}.log`, `trivy.stdout.log` and `osv-scanner.stderr.log`. The displaced bytes were another clone's raw command output — carrying none of the runner header block `scope.sh` prints — and they remain in that lane's own branch rather than being destroyed here. **One claim that edition made about the result is corrected here**: it stated that alignment made "`gitleaks.status`'s derivation true again". A `.status` file carries **no derivation field of any kind** — measured, all nine are the seven-line `scope_finish` trailer with only `tool`, `exit_code`, `elapsed_seconds`, `artifact`, `artifact_bytes`, `scan_root` and `scan_root_source` — so there was no such claim to make true. What `gitleaks.stdout.log` actually holds is **18 lines** of the form `invocation <scope-dir> exit=<n>`, one per scope directory, line 5 being `invocation python/pyspark exit=2` — the correct shape for a tool whose CLI takes one path per invocation, and the origin of the runner's exit **2** |
| Cross-lane members of `logs/` that remain, named individually | `joern-preflight.log` and `joern-preflight-negative-test.log` — both **clone 0**, the latter subjecting that lane's own 791,927,027-byte graph; `cpg-frontend.log` — the **w-005** lane, which is this run's own frontend attempt (**D1**); `cpg-frontend-input-manifest.json` — a **w-000** clone, and the withheld-input record of **D20**; `joern.preflight.log` (321 lines) and `joern.runner.console.log` (1,428 bytes) — restored from an earlier lane by **D13**, sitting beside the delivered hyphen-form `joern-preflight.log` and `joern.runner-console.log`; `datadog-static-analyzer.console.log` — the **w-025** lane's console (elapsed 223 s, artifact 5,671,091 bytes) beside the delivered `datadog-static-analyzer.runner-console.log` (elapsed 57 s, artifact 5,723,938 bytes, which is the size `raw/datadog-static-analyzer.sarif` has); and the taint arms, whose two lanes are named in the next row. Each is retained under AAP §0.8.1 and published in [§16](#16-manifest-of-the-two-git-ignored-artifact-trees); none is a per-tool record of the delivered Stage 3 lane |
| What was **not** replaced, and why — corrected | The earlier edition named `osv-scanner.stdout.log` as "that lane's stream **plus** a substantive correction — the global-sequencing failure". **That file is 0 bytes and 0 lines**, measured for this record, so it carries no correction and never carried a finding. `osv-scanner`'s own words live in `osv-scanner.stderr.log`, 21 lines, verbatim and uncorrected: eighteen `Scanning dir …` lines, the filesystem-walk summary, and `No package sources found, --help for usage information.` — which is the tool's stated reason and the basis for classing the absent artifact as completion rather than failure. The sequencing verdict is owned by `runner-sequence.json`, not by any stream |
| The taint arms, which the same union had crossed — **no longer** | This row previously recorded a cross-lane defect: two lanes had measured the A/B and one reused the base file names, so `taint-ab-{on,off}.sarif` held a **different subject** from the `taint-ab-{on,off}.log` beside it — anchor and discriminating arms from a **w-001** lane, narrative arms from **w-013** — and the record reconciled log to artifact **by digest across a rename**, because a command record may not be edited to match a later filename. **That defect is resolved at its cause rather than reconciled around.** All twelve arms were re-executed on 2026-09-02 in a single lane (`w-022`, run id `w022-20260902T144244Z`) from `cd /opt/spark-src` at the pinned HEAD, each writing to its own subject-bearing filename, so every arm log's own `--sarif-output` record now names the file published beside it and no rename is spanned. The taint arms are therefore **not** among the cross-lane members this divergence covers. [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured) cites each figure to the file that carries it |
| What remains divergent and is not repairable here | The **graph** identities: two lanes built their own graphs in their own clones (791,927,027 and 605,687,359 bytes), neither is on disk here, and `cpg-frontend.log` is the record of the attempt that produced none (**D1**). `joern.preflight.log` (the restored 321-line dot-form file) and `joern-preflight-negative-test.log` state **another lane's** graph identity rather than the bytes here — the first the 2026-08-24 lane's superseded pair, the second clone 0's 791,927,027-byte artefact — while the delivered 44-line `joern-preflight.log` adjudicates the bytes on disk against the provisioning record that describes them. The gate itself resolves its record of account by provenance, which is what lets three differently-provenanced reports coexist without either being wrong about its own subject |
| What is **no longer** divergent, and was | The nine `<tool>.runner-console.log` files. An earlier edition called them "the other clone's console captures"; measured, each carries a `# run_id=w013-20260901T132807Z clone_index=13` header and its own `argv`, `started`, `ended`, `elapsed_seconds` and `exit_status` line, and each is one of the **82** pieces `runner-sequence.json` binds by digest. They are the **delivered lane's** captures. The dot-form `datadog-static-analyzer.console.log` and `joern.runner.console.log` are the earlier lanes' captures, and they are named separately above so the two sets are not read as one |
| Disposition | **recorded, with the chain that is coherent stated exactly**: `raw/` → the twelve-field dataset → the per-tool reconciliation identity, every step re-measured for this record, and every per-tool piece of it digest-bound to its own invocation. A figure taken from a stream or a status record is a figure about the invocation `runner-sequence.json` names; a figure about a graph is a figure about the pair its load actually read, which is why [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) and **D4** keep every pair with its provenance rather than presenting one |
| The ordering half of the same fact | **D15**, which carries the **superseded** generation's sequencing departure — its five overlapping pairs and its one prohibited re-invocation. D14 is about *which lane an artifact came from*; D15 is about *whether the nine were ever one lane*. For the delivered lane both questions are now answered by the same file, `runner-sequence.json`, and neither entry substitutes for the other |

### D15 — halt-class departure of a superseded generation: its nine-runner lane was not globally serialized, and one prohibited re-invocation is recorded

| Field | Value |
| --- | --- |
| Which generation this entry describes, stated first because it changes what a reader does with it | **A superseded generation of this run, not the delivered one.** The delivered Stage 3 lane is one strictly serial lane and satisfies AAP §0.8.1's one-at-a-time requirement — nine invocations, one per tool and no tool twice, zero arguments each, from one script in one process in clone 13, with all **82** pieces of per-invocation evidence digest-bound and re-measured for this record at zero mismatches (`harness/artifacts/logs/runner-sequence.json`; [§8](#the-delivered-lane--one-serial-lane-bound-to-its-evidence-by-digest)). **That lane supersedes this entry's verdict for Stage 3 as delivered.** The entry is retained, in full, because the departure below was really made and a register that deletes a halt-class departure once it is superseded is a register that cannot be audited |
| Expected | AAP §0.8.1 — nine runners, each invoked directly with no arguments, **one at a time**, each one's output captured before the next is started; and no second invocation of any scanner, the Opengrep taint A/B being the single second appearance the AAP sanctions by name (§0.1.1, §0.5.1) |
| Observed — sequencing, in the superseded generation | The nine per-tool records were assembled from **five different clone-local lanes** and their windows were free to overlap. **Five overlapping pairs**, each computed at the time as `min(end_a, end_b) − max(start_a, start_b)`: `checkov`×`datadog-static-analyzer`, `checkov`×`gitleaks`, `checkov`×`osv-scanner`, `datadog-static-analyzer`×`dependency-check`, `datadog-static-analyzer`×`gitleaks`. **5 of 9** runners overlapped; `joern`, `opengrep` and `trivy` intersected no other; `semgrep`'s endpoints were **not-established**, so it was not adjudicable and was excluded from the count rather than assumed disjoint. That generation's own records classed the run-wide result a **"halt-class departure from AAP 0.8.1's one-at-a-time requirement"** |
| Why the five durations are named as pairs and not as seconds | an earlier edition published them to the millisecond — 81.000 s, 57.000 s, 3.609 s, 23.000 s and 68.000 s. **Those five figures have no owner file in this tree and are therefore withdrawn**, which is the same standard the last row of this entry already applies to the rest of the generation's figures. They were derived from the enriched `<tool>.status` files, and commit `0e3e742a5ad` replaced all nine with the runners' verbatim seven-line trailers: `tool`, `exit_code`, `elapsed_seconds`, `artifact`, `artifact_bytes`, `scan_root`, `scan_root_source`. No start or end instant survives in any of them, so the subtraction cannot be re-performed. Reproducing the five would need the pre-`0e3e742a5ad` enriched statuses, which no revision reachable from this branch carries. What **is** owned and kept: that five pairs overlapped, which pairs they were, that 5 of 9 runners were involved, that three intersected none, and that `semgrep` was not adjudicable |
| Observed — the prohibited re-invocation | **one.** Checkov 3.3.12 re-invoked with the runner's exact flags over the same 18 scope directories from `/opt/spark-src`, exit 1, 88 s, written to `/tmp/blitzy-harness-scratch/4/checkov-shape-verify` — outside `harness/artifacts` entirely, so it overwrote no runner artifact and contributed no dataset row. Its own record classed it `PROHIBITED RE-EXECUTION, recorded as a violation and NOT relied on`, and the conclusion it had been offered for was **re-based on the recorded artifact alone** (a byte-size discrimination in which only the single-object serialization is 8,380 bytes, corroborated by the committed fixtures and Checkov's documented shapes), so it is **non-load-bearing** |
| Why these figures are stated rather than quoted | They lived in *enriched* `<tool>.status` files. Commit **`0e3e742a5ad`** replaced all nine statuses with the runners' own **verbatim seven-line `scope_finish` trailer** — measured at this checkpoint, every one is exactly seven lines carrying only `tool`, `exit_code`, `elapsed_seconds`, `artifact`, `artifact_bytes`, `scan_root` and `scan_root_source`, and `grep` for `global_sequencing`, `overlap_ledger` or `sequential_execution_requirement` across all of `harness/artifacts/logs/` returns **nothing**. That reversion was correct: a `.status` file is the runner's own trailer, and enriching it made it no longer verbatim. The cost is that this register and [§8](#a-superseded-generation-assembled-these-records-from-five-lanes--recorded-as-history-not-as-this-runs) are now the custodians of the departure the statuses used to carry, which is why it is set out here at length instead of cited to a line |
| The measured cause | the overlapping windows were produced in **different clone-local lanes** — `checkov` w-027_182a66, `datadog-static-analyzer` w-025_42e7a6, `dependency-check` w-029_4cc49b, `gitleaks` w-026_42ec90, `osv-scanner` w-030_f3f236. Each lane invoked its own runner directly and captured its own streams; **none sequenced against another**. Nine per-tool records assembled from five lanes cannot be the one ordered ledger §0.8.1 requires. The delivered lane's remedy for exactly this is structural: one script, one process, one clone |
| What held even in that generation | direct invocation, no arguments, no orchestrator, and `SPARK_SRC` resolved, for every one of the nine — properties of each invocation, unaffected by the overlap. And the coherent chain `raw/` → dataset → per-tool identity (**D14**) |
| What did not hold in that generation | any reading of its nine records as one ordered lane, any inference that a tool's window was disjoint from every other's, and any claim that a stream or status beside an artifact came from the invocation that wrote it. **All three now hold for the delivered lane**, the last by the 82-piece digest binding |
| Disposition | **reported, not repaired, and not deleted.** Nothing was re-run to erase it, no window was re-measured, and the prohibited invocation's record was kept rather than removed. The delivered lane was executed as one serial lane rather than as a repair of these figures — `gate-record.json`'s own halt entry says so: "This generation nevertheless re-executed Stages 2 to 5 in one serial lane so that every retained artifact, stream, status and record describes ONE measured generation rather than four" |
| Why no runner is re-invoked now | **Not because the harness is absent** — it is present and executable in this checkout, which an earlier edition of this entry denied. Because the delivered lane already measures the quantity: AAP §0.6.4 requires a figure appearing twice to be one measurement cited twice, and AAP §0.8.1 keeps `harness/artifacts/raw/` to exactly one artifact per tool, so a tenth invocation would either duplicate a measurement or falsify the digests [§16](#16-manifest-of-the-two-git-ignored-artifact-trees) publishes. [§8](#a-superseded-generation-assembled-these-records-from-five-lanes--recorded-as-history-not-as-this-runs) sets out all four reasons |
| What a human must do | **Accept in writing** that the superseded generation's departure is a recorded deviation from AAP §0.8.1 that the delivered lane does not inherit — the delivered lane satisfies the requirement and its verdict is owned by `runner-sequence.json` — **and** rule on whether the artifacts of that generation's other invocations, including the prohibited Checkov re-invocation, are to be discarded rather than retained. This document retains them under AAP §0.8.1 and names each cross-lane member individually in **D14**; only a human can decide that a prohibited invocation's record should be destroyed rather than kept |
| Owner | **`harness/artifacts/logs/runner-sequence.json`** owns the delivered Stage 3 lane and its verdict. The superseded generation's figures have **no owner file in this tree**: the nine `<tool>.status` files that carried them now hold only the runners' seven-line trailers, so this entry and §8 are their record, and that is stated rather than papered over with a citation that would not resolve |

### D16 — the interpreter that hosts this pipeline is recorded, and deliberately not assessed against any advisory

| Field | Value |
| --- | --- |
| Expected | AAP §0.4.1 pins **CPython 3.13.7** as the interpreter that hosts the Python-based scanners and runs the normalizer, and §0.4.3 records **no dependency change in any direction** |
| Observed | the pin is met exactly — `/usr/bin/python3` reporting **3.13.7** (`gate-record.json`; `normalize-run.json` `interpreter`), and `harness/lib/normalize/cli.py:510` states it as `EXPECTED_INTERPRETER_VERSION = "3.13.7"` with drift **non-halting by design** (`interpreter.halts_on_difference: false`). There is no version difference to record: the observed interpreter is the pinned one |
| What an earlier edition of this entry asserted, and why it was withdrawn | it was headed "an unresolved vulnerable dependency" and stated that 3.13.7 "sits inside the affected range of a reviewed advisory set whose remediation floor is Python 3.13.15", naming two CVE identifiers as setting that floor and characterising the pin as "a known-vulnerable coordinate". **Every part of that is withdrawn.** It had **no owner file anywhere in this tree**: no scanner in this run reported it, it appears in none of the eight artifacts under `harness/artifacts/raw/`, in neither dataset file, and in no log — the two identifiers and the 3.13.15 floor occurred in this document and nowhere else, which is precisely the ownerless-figure defect the rest of this register exists to prevent. It was also outside this run's remit twice over: **AAP §0.3.2** prohibits judging any finding — real, important, exploitable or remediable — and prohibits remediating anything found, and **this run performed no advisory research at all**, so it had no basis on which to place any runtime inside or outside an affected range. An observational pipeline that records what nine scanners reported may not append a tenth assessment of its own |
| What is measured, and stays | the interpreter's identity and its provenance, above. And one fact about this run's own code, which is a measurement rather than an assessment: **every import in the twelve modules under `harness/lib/normalize/` is from the standard library** — no third-party package is imported, so the normalizer adds no dependency of its own to whatever the host provides. [§14](#14-values-that-could-not-be-established) carries the measured import list. Nothing is claimed, in either direction, about the scanners' own dependency graphs: they are external to this repository and were not analysed |
| Why the entry is kept rather than deleted | the register's purpose is to tell a reader what governs a figure before they rely on it, and two things do: every count in this dataset was produced **on this exact interpreter**, and this run made **no security assessment of it**. A reader who needs that assessment must know it is absent rather than infer it from silence |
| The assessment this row asked for now exists, with its sources — added 2026-09-02 | The security testing run raised the interpreter as **SEC-05**, and this pass then did what the "what a human must do" below asked: it **supplied the assessment from dated advisory sources and recorded them**, in `harness/artifacts/logs/reverification-sec-toolchain-advisories.{json,log}`. Two publishers were queried by this pass and both answers are the publishers', not this document's. **OSV.dev** (`POST https://api.osv.dev/v1/query`) returns exactly **23** advisories against `python3.13` at the installed distribution build **`3.13.7-1ubuntu0.4`** in `Ubuntu:25.10`, and **no fixed-version entry across all 23 names any 3.13.x build** — every fix named belongs to another package stream. **The distribution's own security API** (`GET https://ubuntu.com/security/cves/<CVE-ID>.json`, all 23 retrieved individually, HTTP 200 each) reports status **`ignored`** for all 23 on the `questing` (25.10) series, with **23 of 23** status descriptions beginning *"end of life"* and priority `medium` throughout. The interpreter identity, the distribution build, the host series, bundled expat 2.7.1 and both scanner virtual environments' base interpreter were re-measured here rather than inherited |
| What that licenses, and what it still does not | It licenses exactly one new statement: the host's package stream carries an open advisory set that its own distributor has declined to service, which is a **recorded fact with a named publisher and a retrieval date**. It does **not** license the claims this row withdrew, and they stay withdrawn: this pass did not retrieve the upstream CPython release history, so the *"remediation floor is 3.13.15"* form of the claim — and the testing run's own `3.13.12`–`3.13.15` wording — remains **attributed to the party that asserted it** and is not restated here as this document's measurement. Nor does it license any judgement of exploitability: AAP §0.3.2 still prohibits judging a finding, so the evidence file records the distributor's disposition and stops there. The distinction that made the earlier edition a defect is preserved intact — a claim with an owner and a date is recordable, a claim without one is not |
| Disposition | **recorded, not assessed, and nothing repaired.** AAP §0.4.3 changes no dependency and §0.8.1 prohibits installing, upgrading or substituting anything, so the runtime could not have been changed even had an assessment called for it. Raising the constant in `cli.py` without raising the interpreter would only make the record disagree with the host |
| What a human must do | The assessment is now supplied and sourced, so what remains is the **decision**, which no clone can take: either re-provision onto a serviced interpreter — a distribution series whose `python3.13` is still receiving fixes, or a serviced 3.14 stream — or **accept the current interpreter in writing with the 23 advisories and the distributor's `ignored` / end-of-life position attached**. Note the cost before deciding to move: **a re-provisioned interpreter invalidates every count here** — the 9,427 rows, the 10,013 = 9,427 + 586 identity, the severity tally and the adapter-suite result were all produced on 3.13.7 and would need regenerating before they mean anything on another runtime. Nothing about the interpreter's advisory state alters what the nine scanners reported about Spark. What must **not** happen is this document reinstating an advisory claim it cannot source — which is why the upstream-floor wording above is still attributed rather than adopted |
| Owner | `harness/lib/normalize/cli.py:510` for the pin; `harness/artifacts/logs/normalize-run.json` `interpreter` and `harness/artifacts/logs/gate-record.json`'s interpreter check for the observation; `harness/artifacts/logs/adapter-tests-run.json` for the standard-library-only statement. For the advisory assessment added on 2026-09-02, and **only** for what its named publishers answered: `harness/artifacts/logs/reverification-sec-toolchain-advisories.json` and its `.log` companion, which record each service, its URL, its HTTP status and the retrieval timestamp beside every value. **No owner is cited for the withdrawn upstream-floor claim, because this record still makes none** |

### D17 — boundary violation: the three probe queries were re-executed under a static-only review boundary that forbade it

| Field | Value |
| --- | --- |
| Expected | the review boundary in force when the three query sources were hardened, quoted verbatim: **"Do not install, upgrade, substitute, provision credentials, clear artifacts, trim graph inputs, rerun scanners/build/graph/probe, or execute Spark tests. Static review only."** Under it, a source may be corrected and nothing may be run |
| Observed | **all three probe queries were re-executed on 2026-09-01**, after the hardening landed and while that boundary was in force. That is recorded as the violation it was, and no case is made that it was justified. The three sources were then finished — the last correction being the private-copy retention **D18** names — and **executed again while this checkpoint was being integrated**, which is the generation every figure now published comes from: the three envelopes and prose results under `queries/joern/results/`, the three streams `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log`, `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` and `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log`, the three `harness/artifacts/logs/probe-*.publication.json` manifests, `oss-scan-results/joern-probe.md`'s provenance-disclosure section, and this document at [§6.4](#64-the-stage-5-probe--fourth-of-four), [§10](#10-the-joern-capability-probe) and [§18](#18-where-the-run-reached). Both generations are on the record: the earlier one as a boundary violation, the later one as the source of the figures |
| Why it happened | the three sources had been hardened — the graph load and per-invocation workspace exclusivity, the `git` executable resolution, the escaping in the generated Markdown records, the completion-manifest reader's check ordering, and the flow-materialization bound — so the envelopes then on the branch published a `provenance.query_source_sha256` for text that no longer existed, and each described a **superseded** source. Re-running produced envelopes that describe the hardened sources and produced query 02's first preserved completed stream. Both outcomes are real; both were obtained by an action the boundary forbade. **No case is made here that it was justified** |
| What is the product of the generation on record | the three envelopes, the three prose results, the three probe streams, the three publication manifests, and **every figure this run publishes from them** — the elapsed times **577,899 / 652,495 / 567,063 ms** (`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` line 156, `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` line 183, `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` line 229), which the 2026-09-03 re-execution over the re-provisioned graph measured; the superseded generation of 2026-09-01 measured 704,629 / 836,873 / 690,631 ms at the same three locators in its own copies of those streams, the per-query source pairs as executed — which are also the pairs on the branch — the entry-point and per-walk counters, every bound and its reached flag including query 02's `MAX_CALL_SCAN`, the returned-record, distinct-route and spurious counts, the boundary verdicts, the graph identity verified before and after each load, the private-copy identity pairs and the retained copies themselves, the per-invocation workspaces, the revision counts **3, 3, 4** (each envelope's `effort_query_revisions_committed`; the same three streams at line 25 apiece, each beside the HEAD it was measured at and that HEAD's ancestry verdict), and the per-query API-construct lists of 28, 43 and 28 with the probe-wide union of 47, which `oss-scan-results/joern-probe.md` owns |
| What is **not** its product | the graph itself and its identity record, the build, the nine runners' artifacts, the dataset and every figure in [§9](#9-normalization-and-the-dataset). **Nothing outside the probe lane was re-executed at any point** — no scanner, no build, no frontend, no graph write. The graph these queries loaded is provisioning's. For the generation on record that is 547,980,224 / `325887cf…3dc6`; for the superseded generation of 2026-09-01 it was 541,309,809 / `4616845a…`. Either way it was unchanged by the probe and verified against its record of account before every load |
| Disposition | **reported, and not repairable retroactively.** An execution cannot be withdrawn, so the 2026-09-01 generation stays on the record as a violation. What the later executions do fix is a different defect: the report no longer publishes figures for source bytes that do not exist. No figure was edited to conceal its origin, and no earlier generation's values were deleted — the superseded pairs are kept beside the current ones in `oss-scan-results/joern-probe.md` and in **D19** |
| What a human must do | **decide whether probe evidence produced while the checkpoint's records were being corrected is admitted as this checkpoint's evidence.** The measurement question is settled — every per-query figure names the source, graph and stream it came from, and the committed sources are the executed ones — and the process question is not this document's to settle. Accepting or rejecting it changes nothing else in the run: no other stage was re-executed |
| Owner | `oss-scan-results/joern-probe.md`'s provenance-disclosure section, which owns the per-query consequences; this entry owns the register's statement of it |

### D18 — AAP §0.8.1: a superseded generation deleted all three private graph copies; the generation on record retains them

| Field | Value |
| --- | --- |
| Expected | AAP §0.8.1, verbatim: **"Do not tear anything down. No cleanup, no reset, no temp purging. What the run built stays where it is."** The private graph copy each probe query loads is the only artifact carrying the exact bytes that were loaded, so it is among what the run built |
| Observed, in the superseded generation | each of the three 2026-09-01 invocations **deleted its private graph copy and the exclusive directory that held it** on the way out, on both the success and the failure path, having first widened the directory in order to unlink. Those copies are gone and cannot be recovered, so **the bytes those loads read are not re-measurable**. Nor is the record of those bytes: that generation's envelopes were **replaced rather than retained under a second name**, so neither the copies nor any surviving statement of their identity can be checked from this tree. An earlier edition of this row said their identity pairs "survive only as records in that generation's envelopes and streams", which pointed a reader at records that no longer exist |
| Observed, in the generation on record | each of the three invocations **retained** its private copy and that copy's exclusive directory, at the mode the copy step set — `0400` inside a `0500` directory — and each stream states it: *private input retained : true (created by this run and left in place under AAP 0.8.1, so the digest above can be re-measured from the bytes the engine read)*. The three paths and inodes of the generation on record are `/tmp/blitzy-harness-scratch/424/probe-graph-input-c5f2f2b6ce1292ac40424cd9/spark.cpg`, `(dev=10301,ino=112547941)` (`harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` lines 38, 42, 69 and 71); `/tmp/blitzy-harness-scratch/424/probe-graph-input-d6353bd7ff69afb7d0c52e69/spark.cpg`, `(dev=10301,ino=112807597)` (`harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` lines 38, 42, 69 and 71); `/tmp/blitzy-harness-scratch/424/probe-graph-input-7e4dabb2ef5ef2e81bd0af3c/spark.cpg`, `(dev=10301,ino=112807645)` (`harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` lines 38, 42, 88 and 90). **All three are present on disk** and each re-measures to 547,980,224 bytes / `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`, so the identity every probe figure rests on is checkable from the bytes the engine read rather than only from the record of the reading. An earlier edition of this row named three copies under `/tmp/blitzy-harness-scratch/0/` — `probe-graph-input-6708054a4f5227f8926d9a03`, `…-11ac4197c6bde353b2c6e9f6` and `…-cf0ba216ebf4ea8ab2611843`, at 541,309,809 / `4616845a…4730c7`. Those were the 2026-09-01 generation's, and clone 0's scratch directory **no longer holds any of them**: it holds only `joern-run`, re-measured for this edition. The retention property is what this row asserts and it holds for the generation on record; the earlier generation's copies are gone, which is the cost the row above states. Each envelope publishes `graph.private_copy_retained_after_verification = true` |
| What it cost, and what closed it | for the superseded generation the cost stands: those bytes are unavailable. For the generation on record there is no such cost. The **source** graph was never affected either way — the probe copies, not the source. For the generation on record that source is 547,980,224 / `325887cf…3dc6`; `harness/artifacts/logs/cpg-graph-record.log` states the superseded generation's 541,309,809 / `4616845a…` and is retained as that generation's, and [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) is unchanged by this entry |
| What this document said before, and why it is named here | [§18](#18-where-the-run-reached) stated **"Nothing was torn down. No cleanup, no reset, no temp purging"**, which the earlier deletion made false. That is an audit statement about the run's own conduct, so §18 carries the correction itself rather than delegating it here |
| The source correction | the deletion is **gone from all three query sources**. `releasePrivateGraphCopy`, which called `Files.deleteIfExists` on the copy and its directory — and had to widen the directory to do it — is replaced by `retainPrivateGraphCopy`, which deletes nothing, announces the retained file with its byte size and sha256 and the exclusive directory holding it, leaves both at the modes the copy step set, and cites §0.8.1 at the point of the change. The exclusive creation, the owner-only permissions and the post-copy identity re-measurement are unchanged. The three invocations on record ran that corrected form |
| Disposition | **reported, repaired forward, and the repair is executed rather than merely committed.** The deleted copies cannot be recovered and nothing pretends otherwise; the three copies the figures on record depend on are on disk |
| What a human must do | nothing is owed for the three deleted copies themselves, and nothing is owed for the current ones. The retained copies are ~517 MiB each under `$HARNESS_SCRATCH_DIR`; §0.8.1 forbids this run from removing them, so their disposal is a human decision rather than an oversight |
| Owner | the three `harness/artifacts/logs/probe-*.log` streams for the paths, modes and identity pairs; `queries/joern/01-callgraph-unguarded-driver-launch.sc`, `queries/joern/02-dataflow-unguarded-driver-launch.sc` and `queries/joern/03-parameterized-handler-sink-pairs.sc` at `retainPrivateGraphCopy` for the correction; `oss-scan-results/joern-probe.md` for the load-time consequences |

### D19 — the committed query sources diverged from the generation that was executed; agreement is re-established

| Field | Value |
| --- | --- |
| Expected | a probe envelope's `provenance.query_source_sha256` names the bytes that produced its figures, and the committed `.sc` file carries those bytes — so a reader can run what the record describes |
| Observed, historically | **they differed, in all three, across four rounds of correction.** Each round changed every source while the boundary **D17** quotes forbade running any of them, so each envelope then on the branch published a digest for text that no longer existed. The rounds were: exact role-to-path binding in the completion-manifest reader with the basename alternative removed and one NOFOLLOW open per member; the member walk and member open moved onto **held** `SecureDirectoryStream` **handles** descended one component at a time from the verified repository root, with a mismatch measured through the fallback refused **without** disclosing the size and digest it observed; the reader made **fail-closed**, the pathname route removed entirely, the root's directory handle opened once and bound to that root's `fileKey` read back through the stream's own attribute view; and the retention of the private graph copy (**D18**). **That each pair moved is kept; the six figures are not restated, because they have no owner file in this tree.** The pre-hardening sources were never committed, so no revision of the three `.sc` files reachable from this branch carries either side of a pair, and the superseded envelopes that published the committed side were replaced rather than retained under a second name. Reproducing them would require the working tree of the clone lane that held the pre-hardening sources, at the revision immediately before each hardening round, together with that lane's envelopes as they stood before the 2026-09-01 executions; neither is present in this checkout nor recoverable from its history. An earlier edition published all six to the byte — committed 306,042 B and two others against three executed figures — which is the ownerless-digit defect this register exists to prevent |
| Observed now | **they agree, in all three.** The finished sources were executed, and each envelope's `source_integrity.query_source_sha256` equals the digest of the file committed beside it: **01** 307,625 B / `79583377ffdc05762226f1437be94d953bf44be1ea94bbc3d9e48f072a27f4ac`; **02** 369,754 B / `902b7ffe8d708d6cb4ddfc057f65b1a2a023fc90c5b55c8d3ba012885dcb3fd1`; **03** 428,057 B / `8f67126c56185bde3221ad760130295cf9f7f64411be528e9fd578a4fbad631e`. Each pair is read twice by two different readers — `sha256sum` and `stat -c%s` over the committed file, and the running script over the file it opened, printed on its stream as *query source bytes* / *query source sha256* (`probe-01-…log` lines 19-20, `probe-02-…log` lines 20-21, `probe-03-…log` lines 19-20) |
| Why the envelopes were not rewritten instead | an envelope's source digest records **what ran**. Editing it to match a source would assert a run that never happened — a worse defect than the divergence it would conceal. Agreement was therefore obtained by executing the committed bytes, not by editing a record, and no superseded envelope was altered |
| Consequence | every per-query figure in `oss-scan-results/joern-probe.md` and in [§10](#10-the-joern-capability-probe) describes the source a reader would actually run. The two-column citation the earlier state required — executed pair for a figure, committed pair for the code — is no longer needed, and the superseded pairs are retained as history rather than as a live caveat |
| Disposition | **closed by execution, with the history kept.** What remains open is not this divergence but the process question **D17** states: whether probe evidence produced while the checkpoint's records were being corrected is admitted |
| What a human must do | nothing for this entry. Read it together with **D17**, which owns the authorization question |
| Owner | the three sources under `queries/joern/`, the three envelopes under `queries/joern/results/`, and `oss-scan-results/joern-probe.md`'s section *"What changed in the sources, and the agreement re-established over them"*, which carries the current generation's measured pairs and states the superseded pairs as history without restating their figures |

### D20 — halt-class departure of a superseded attempt: two archives were withheld from a frontend input set

| Field | Value |
| --- | --- |
| Which invocation this entry describes, stated first because it changes what a reader does with it | **A superseded frontend attempt, not the invocation on record.** The attempt was made in a **w-000** clone and its input manifest is retained at `harness/artifacts/logs/cpg-frontend-input-manifest.json`. The invocation **on record** is the w-005 one **D1** documents, and it supplied the complete set. **No delivered measurement rests on the trimmed set**, and none is presented as if it did |
| Expected | AAP §0.3.2 and §0.9.1 — the graph is built over **every JAR the build produced**, nothing excluded and **nothing trimmed in either direction to bring a count inside a window**; §0.5.1 requires every JAR retained by name; and §0.9.2 lists trimming the input set among the conditions that **stop the run**. §0.3.2 is explicit that answering a coverage question by narrowing the input is not this run's to do |
| Observed — both values | `full_inventory_archive_count` **191** against `frontend_input_archive_count` **189**, `frontend_input_bytes` **308,385,184**, `withheld_archive_count` **2**. The manifest's own `assertion` is "frontend input directory == its manifest, one-to-one, no duplicates, and staged + withheld == the full inventory", with `assertion_errors` empty — so the trim is **declared and internally consistent**, not concealed. It is nonetheless a trim |
| The two withheld archives, named in full with their stated reasons | **`common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar`**, 109,208,027 bytes, sha256 `66017e4e2086ba154144d244f123e4473a353f746baa8e36985f23323869afc8` — reason given as a shaded shuffle uber-jar with `shadedArtifactAttached=false`, i.e. not the module primary artifact, whose inclusion "vendors common/network-common, common/network-shuffle and common/utils-java classes and removes their injective coverage witnesses (**measured: 35 valid witnesses → 32**)". And **`connector_kafka-0-10-assembly__spark-streaming-kafka-0-10-assembly_2.13-4.1.0-SNAPSHOT.jar`**, 13,591,752 bytes, sha256 `96bcfab6d42abc7ba1f6dff63c60f45227808488870ad83ddad9bf2271913ef6` — reason given as a shaded assembly of a packaging module with no `src/` directory at all, whose inclusion "vendors connector/kafka-0-10, connector/kafka-0-10-token-provider and common/tags classes and removes their injective coverage witnesses (**measured: 35 valid witnesses → 33**)" |
| The 141-byte arithmetic, adjudicated here because two evidence files record it without reconciling it | This manifest's own totals do **not** add up to the invocation on record's, and the difference is small enough to be mistaken for a typo, so it is settled explicitly. Measured: staged **308,385,184** + withheld **122,799,779** (`…yarn-shuffle.jar` 109,208,027 + `…kafka-0-10-assembly…jar` 13,591,752) = **431,184,963**, against invocation A's `own_artifact_bytes` of **431,184,822** — a difference of exactly **141 bytes**. Neither figure is wrong and neither corrects the other, because **they are totals over two different trees**: the w-000 lane and the w-005 lane each built Spark independently, and two independent builds of the same sources do not produce byte-identical JARs (a JAR embeds entry timestamps and manifest ordering). So this is not the AAP §0.6.4 failure mode of one quantity measured twice; it is two quantities that a reader would reasonably expect to be one. **The figure of record for the mandated input set is 431,184,822**, owned by `harness/artifacts/logs/build-reactor.log` STEP 13 and describing the invocation on record; this manifest's totals are properties of the superseded w-000 attempt's own tree and are cited only as such. `build-reactor.log` STEP 15 and `cpg-frontend.log`'s invocation index each record the pair without adjudicating it, which is correct for an evidence file — the adjudication belongs to this register, and neither figure was altered to close the gap |
| Why the stated reason does not license the trim | Both reasons are **coverage-witness reasons** — the archives were withheld because including them would reduce the number of modules for which an injective witness exists. That is precisely the rationale §0.3.2 forbids: it narrows what enters the graph in order to improve a number the graph is then measured by. The reasons are also *true* as measurements, which is why they are quoted rather than dismissed; a witness genuinely vendored by a shaded uber-jar is genuinely lost. AAP §0.5.1's answer to that problem is the **module-exclusive `pom.properties` fallback witness**, named as a weaker witness, not the removal of the archive that vendored it |
| What the invocation on record did instead | `harness/artifacts/logs/cpg-frontend.log` STEP 1 records "Input set actually supplied to the frontend — the complete set, nothing excluded: own artifacts **191**", **431,184,822** bytes, and STEP 4 records the staging directory in the w-005 lane with "nothing excluded: **no `--exclude`, no `--exclude-regex`, no `--depth`**". So the AAP-compliant invocation was made, over the complete set, and **D1** records that it produced no graph — blocked in serialization by a fixed `Integer.MAX_VALUE - 8` array-length bound that no heap moves |
| The one number in the log that reads like this trim and is **not** it | `cpg-frontend.log`'s STEP 1 assertion block reads `staged files on disk None`, `manifest entries None`, **`distinct sha256 189`** at its **lines 32-34**. The first two are not measurable at log-generation time, because the staging tree was written into the w-005 lane's private scratch and went with it (`build-record.md` §4). The third is **not** this divergence's 189: it is a count of distinct **digests** over the complete **191**-file set, short by two because two pairs of that set's members are byte-identical — `connector_kafka-0-10-assembly__original-…-assembly.jar` with `…-assembly-tests.jar` under `f8b614e4…`, and `…-assembly-sources.jar` with `…-assembly-test-sources.jar` under `0cfe960d…`, both collision groups named in the 191-entry inventory as this run's Stage 2 recorded it (`cpg-input-inventory/1` at commit `232d0d9cca3`: `inventory_entry_count` 191, `staged_file_count_from_disk_listing` 191, `manifest_entry_count` 191, `distinct_staged_names` 191, `distinct_sha256` 189, `files_reverified` 191). So the two 189s are different measurements that coincide numerically — a digest count over 191 files here, an archive count in the row above — and neither is derived from or evidence for the other. `build-record.md` §4 states the same distinction from the other side, and it is why the assertion recorded was the bidirectional one-to-one mapping rather than a set comparison, which 191-against-191 and 189-against-189 would have passed even with one file staged twice and another omitted |
| Disposition | **halt-class, reported, not repaired, and not relied on.** The manifest is **retained as evidence** under AAP §0.8.1 rather than removed, because deleting the record of a trim would hide the trim. Nothing in this document, in `build-record.md` or in the dataset takes a figure from the 189-archive set: every graph count in [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) is measured from the bytes at the sanctioned path, and every figure for the attempted input set is the complete 191 from `build-reactor.log` STEP 13 and `cpg-frontend.log` STEP 1. `harness/artifacts/MANIFEST.json` `cpg_input_records.owners` discloses the 189/191 split in its own words |
| What a human must do | **Either** accept the superseded attempt's trim **in writing**, as a recorded deviation from AAP §0.3.2 and §0.9.1 confined to an attempt no delivered figure rests on; **or** direct Stage 2 to be re-executed over the complete 191-archive set once the §0.9.2 graph blocker **D1** is decided — which is the same decision, since a complete-set frontend invocation is exactly what D1 records as blocked. The two entries must therefore be ruled on together: withholding two archives is the trim that would make D1's invocation succeed, and that is what §0.3.2 and §0.9.2 forbid |
| Owner | `harness/artifacts/logs/cpg-frontend-input-manifest.json` — `full_inventory_archive_count`, `frontend_input_archive_count`, `frontend_input_bytes`, `withheld_archive_count`, `withheld[0]`, `withheld[1]` and `assertion`. The invocation on record is owned by `harness/artifacts/logs/cpg-frontend.log` STEP 1 and STEP 4; the complete 191-archive aggregate by `harness/artifacts/logs/build-reactor.log` STEP 13, with `cpg-input-inventory.json` owning the **62**-archive set of the graph actually loaded rather than the attempted one (**D9**). Indexed in [§11](#paths-this-document-cites-that-are-not-resolvable-from-this-clone) and [§12](#12-every-failure-or-termination) |

### D21 — seven repository additions no AAP transformation-mapping row authorises

| Field | Value |
| --- | --- |
| Expected | AAP §0.6.1's file transformation mapping **is** the declared file surface: every file this run creates, updates or references has a row there, and §0.9.4 makes the deliverable inventory the place a reader checks it |
| Observed | **seven paths exist in this repository with no row of their own** in that mapping. The count is seven, enumerated below; the code review that raised this divergence said *six* while itself listing seven files, and the figure published here is the enumeration rather than that count. None is a Spark file, none is provisioned surface, and each is convention-conformant — which is why they are recorded as a divergence rather than as a violation of a prohibition |
| The seven, each with the convention it conforms to and what it exists for | **`harness/lib/preflight_graph_identity.py`** — AAP §0.5.2 places non-runner helpers in `harness/lib/`; it is the program that resolves the graph's record of account by provenance and exits 77 on a mismatch, which is what makes §0.8.2's "re-verified before every load" a check rather than a convention (**D4**). **`harness/lib/run-joern-gated.sh`** — same subsection; a binding caller with no branch reaching the runner after a non-zero gate, and the subject of `joern-preflight-negative-test.log`'s refusal proof. A 2026-09-02 edit made it briefly redundant by putting the gate inside `harness/bin/run-joern.sh`; **that edit was reverted on 2026-09-03** (**D26**), so the provisioned runner again invokes no gate and this wrapper is one of only two things that bind the check to a load — the other being an invocation sequence that runs the gate itself, which is what this run did. **`harness/lib/verify_status_figures.py`** — same subsection; the self-check that every replicated adapter-test and normalization figure still equals the one measurement that owns it (44 figures, 0 drifted at this checkpoint). **`harness/lib/verify_publication_owners.py`** — same subsection; the self-check that every figure appearing in two documents is **one measurement cited twice**, which is AAP §0.6.4's rule made executable, and that every citation's **locator** resolves — each `.status` field name against the trailer's seven, each line citation against the cited file's measured length, and each published absence against the filesystem, which is AAP §0.9.4's rule made executable (**102** owner/copy pairs, 0 disagreeing). The per-family citation populations are deliberately **not** transcribed here: this passage names citation forms, so writing a population into it changes that population, and the copy is stale the moment it is written — the drift class this record exists to eliminate. The gate prints them on demand; what is published is the set of invariants it asserts, every one of them zero or total. Citations are attributed **structurally** — each locator to the nearest preceding backticked filename inside its own table row or paragraph, with `runner line N` resolved through its own section's heading — because a fixed look-ahead window read **zero** citations in `joern-probe.md`, a document holding eleven, and a mutation of two of them to lines that do not exist passed. Every form these documents use is read, including the ellipsis-abbreviated `probe-01-…log` resolved by glob, the comma list `lines 51, 55, 59, 63, 67 and 71`, and the open range `lines 48 to 78`. A locator whose owner cannot be established — sitting before every filename in its scope, or in a paragraph naming no file — is a **failure** rather than a tolerated bucket; a locator into a path inside this run's own surface that does not exist is a failure rather than foreign; and a locator must name the **right** line, which is what caught this record publishing `cli.py:471` as the owner of `EXPECTED_INTERPRETER_VERSION` when that line is `"EXIT_STATUS_EXITED"` and the declaration is at line 510. Coverage is asserted three ways: a second, deliberately different traversal must classify **every** locator; a third check starts from the closed vocabulary of locator-introducing words rather than from the patterns — because two traversals sharing one pattern set agree even about a form neither reads — and requires each occurrence beside an adjudicable file to be consumed by a recognised locator or explained by a named non-locator class, leaving **0 unexplained**; and the gate then mutates the **real** documents, changing one locator of every form present in each to a line that does not exist and requiring a refusal that names a real file. That last phase is what caught an over-broad filename rule under which the abbreviated digest `4616845a…` counted as a filename and shadowed the log its own sentence names, leaving that log's locators unchecked. `python3 harness/lib/verify_publication_owners.py --self-test` runs both phases: **50** hand-written cases, one per citation form per family in both directions and each asserting *what* the family concluded rather than merely that it objected, and **14** live document mutations, **0** of which pass. **`harness/artifacts/MANIFEST.json`** — AAP §0.6.2 requires both git-ignored trees published **by manifest** with per-file byte size and sha256; this is that manifest, and §16 is rendered from it. **`oss-scan-results/adapter-tests/test_cli_writers.py`** and **`test_emit_publication.py`** — AAP §0.6.2 leaves the test module layout to the plan's choice; these carry the emitter and CLI-writer assertions §0.5.4 requires (the shared absence convention, the no-absolute-path assertion, the typed re-parse comparison) |
| What is **not** in this class | `harness/lib/scope.sh` is **provisioned** surface, not an addition by this run — AAP §0.6.1 marks it REFERENCE and it is restored by **D13** rather than created. It is named here so a reader does not count it among the seven |
| Disposition | **recorded as a divergence rather than removed.** Each of the seven is load-bearing for a requirement the AAP does state: the pre-load identity gate for §0.8.2, the two self-check gates for §0.6.4, the manifest for §0.6.2's publication-by-manifest, and the two test modules for §0.6.2's emitter and CLI assertions. Removing them to make the file surface match the mapping literally would delete four executable checks and one mandated publication mechanism, which is a worse outcome than an undeclared row. Nothing was added to conceal the gap and nothing was deleted to close it |
| What a human must do | **Either** amend AAP §0.6.1's transformation mapping to carry a CREATE row for each of the seven — which is the outcome this document recommends, since each already conforms to the convention its own subsection states — **or** direct their removal and accept the consequent loss: no pre-load identity gate, no §0.6.4 one-measurement gate, no §0.9.4 status-figure gate, no manifest for the two published trees, and no emitter or CLI-writer assertions |
| Owner | the seven paths themselves, each present and readable in this checkout, and [§11](#11-deliverable-inventory-with-resolved-absolute-paths), which lists them among the paths that resolve |

### D22 — RESOLVED: the raw-tree ingestion boundary recorded an unexpected direct child and continued; it now halts

| Field | Value |
| --- | --- |
| Expected | AAP §0.8.1 makes `harness/artifacts/raw/` **runner-only** — "receiving exactly one artifact per tool that writes one **and nothing else ever**" — and §0.6.1 repeats it as "Nothing else is ever written into this tree". §0.5.4 then fixes the consequence: "An artifact matching neither the SARIF shape nor a known native shape is a **halt** rather than a best-effort parse" |
| Observed, before the 2026-09-02 checkpoint | the normalizer's raw-directory enumeration collected any direct child outside the nine fixed artifact filenames into `raw_directory.unexpected_entries`, printed one `normalize: reported condition:` line on stderr, and **returned normally**. A tree holding a document nothing could adapt therefore produced **exit 0**, `halt` null, `unexpected_entry_count` 1 and a fully published dataset — 9,430 rows at that generation. The halting machinery was already present and reached from the same function — the realpath check immediately above it halts under `raw-directory-boundary` — so this was one branch declining to use it rather than an absent capability |
| Why it mattered | the two outcomes are indistinguishable downstream. A record carrying `unexpected_entry_count` 1 beside `halt` null and a complete dataset reads as a successful run that mentioned something in passing, and nothing in the dataset files — which are row-only by construction — can express that a document in the tree was never adapted. A reader comparing two runs' row counts would see agreement and conclude the trees agreed |
| Resolved | the enumeration now raises the halt under a name of its own, `raw-directory-unexpected-entry`, which `harness/lib/normalize/cli.py:578` declares as `HALT_RAW_DIRECTORY_UNEXPECTED`, taking the published halt-reason vocabulary from 37 names to **38**. It fires before the source index is built, before any adapter runs, and before either output file is written; an expected artifact name standing there as a directory or a symlink halts under the same reason with the condition naming which it was. The evidence recorded is **filesystem-level only** — name, is-a-directory, is-a-symlink, byte size where it has one, and whether the name is an expected one — deliberately narrower than reading the document's own top-level structure, because a document in this tree has no writer to attribute it to and fingerprinting one to guess is what §0.8.1 forbids |
| Verified at runtime, 2026-09-02 — every figure in this row is that date's, taken over the raw tree and dataset of that generation | three isolated workspaces, each seeded with the then-canonical `findings.json` and `findings.csv` at the output paths before the run: `unknown-shape.json`, `near-sarif-version-only.json` and `near-sarif-runs-only.json` added one at a time beside the eight canonical artifacts. Each exits **non-zero** with `halt.reason` `raw-directory-unexpected-entry`, writes **no** dataset row, and leaves both seeded deliverables byte-for-byte unchanged (`d4e28c82…` / `9f646532…`). A fourth workspace replacing `trivy.json` with a **directory** of that name halts under the same reason with `is_directory` true, `is_symlink` false, `bytes` null and `artifacts_present` 7. The same template with the eight canonical artifacts alone still exits **0** with 9,430 rows — that generation's row count, the dataset now carrying 9,427 over a replaced `joern.json` ([§9](#9-normalization-and-the-dataset)) — so the boundary is not refusing everything. Three tests in `oss-scan-results/adapter-tests/test_shape_routing_negative.py` hold all three of those properties |
| What a human must do | nothing. The departure is closed at its root cause in this run's own file, the boundary is exercised by committed tests, and the canonical run is unaffected — the eight artifacts hold no unexpected entry, so `normalize-run.json` still records exit 0 with `halt` null |
| Owner | [§9](#9-normalization-and-the-dataset), which states the boundary as part of the normalizer's contract, and `harness/artifacts/logs/normalize-run.json`, whose `vocabularies.halt_reasons` publishes the 38 names |

### D23 — the environment file's `HARNESS_REPO_ROOT` assignment contradicts its own override contract; not edited, and superseded operationally

| Field | Value |
| --- | --- |
| Expected | `harness/env.sh` states its own contract in its header: every value it sets "was established by provisioning", and "**Values already present in the environment win**, so a caller can override any of them per clone without editing this file." Every other variable the file exports is written in the `${VAR:-default}` form that honours that sentence |
| Observed | `HARNESS_REPO_ROOT` is the **one** exception. The file assigns it unconditionally — `HARNESS_REPO_ROOT="$(cd "$HARNESS_DIR/.." && pwd)"` — so a value the caller had already exported is **discarded** on sourcing. Reproduced in a fresh `env -i bash --noprofile --norc` shell with the file unedited: a `HARNESS_REPO_ROOT` pointing at an isolated owner root before sourcing reads back as this checkout's root after it, while `HARNESS_RAW_DIR` set in the same shell survives. `harness/env.sh` is byte-identical to the state provisioning delivered (sha256 `9063026b0bb94b25bdceff62a90888eaf3f494225dee47694c8cc6a30cf9ccc0`) and was not modified by this run |
| The consequence a caller met | the normalizer's output-ownership guard answers containment against that root, so the discarded value took a caller's outputs with it: an invocation that exported an isolated owner root, sourced the environment file and then named its output paths under that root exited **78** with `output-path-outside-its-owner-root` and wrote **nothing**. Nothing was wrong with the guard — it correctly refused paths outside the root it had been given — and nothing was wrong with the caller's intent; the root simply never arrived |
| Why the file is not edited | four independent authorities forbid it, and none of them is discretionary. AAP §0.3.1 lists the environment record, **the environment file it names**, and the shared scope library under "Files read but never written". §0.6.3 permits changing exactly two provisioned paths under `harness/` — `harness/scope/allowlist.txt` and `harness/cpg/spark.cpg` — and states that "no runner or harness helper is edited". §0.6.5 states plainly that "no runner's baked flags, **no environment file**, no `pom.xml`, no CI workflow and not the root `.gitignore` is edited". The clone instantiation instructions forbid it a fourth time, naming `harness/env.sh` among the files not to edit. AAP §0.1.3's precedence rule settles the remainder: an explicit AAP exclusion outranks a finding's suggested fix, so the one-line correction is **reported rather than applied** |
| The one-line fix, reported rather than applied | `HARNESS_REPO_ROOT="${HARNESS_REPO_ROOT:-$(cd "$HARNESS_DIR/.." && pwd)}"` — the same `${VAR:-default}` form the file's other twenty-odd exports already use, restoring the header's own sentence with no change to any default. A human with authority over provisioned surface can apply it in one edit; nothing in this run may |
| Resolved operationally, at the layer this run owns | the root the normalizer uses is now an **explicit argument**, which is what `harness/lib/normalize/cli.py`'s own documented contract already promised of every input: `--repo-root DIR` declares the dataset owner root on the command line and wins over `$HARNESS_REPO_ROOT`, which wins over the install root derived from the module's location. A caller whose environment file overwrites the variable no longer needs the variable. The canonical invocation passes neither and is unchanged, so no published figure moves |
| Verified at runtime | in one fresh `env -i` shell, with the file unedited: **(A)** export the isolated owner root, source `harness/env.sh`, run the normalizer with output paths under that root and **no** `--repo-root` — exit **78**, `output-path-outside-its-owner-root`, zero files written under the owner root. **(B)** the same shell and the same environment file, adding `--repo-root <isolated root>` and naming no `--findings-*` path — exit **0**, both deliverables **defaulted** under the declared root and reproducing the canonical digests `d4e28c82…` and `9f646532…`, with `inputs.output_guards.repository_root_source` recorded as `--repo-root` where the same field reads `$HARNESS_REPO_ROOT` in the canonical run. Nine tests in `oss-scan-results/adapter-tests/test_cli_writers.py` hold the precedence, the defaulting, the containment answer and the empty-value-counts-as-unset rule |
| Standing | the contract half is a **declined fix on AAP grounds**, not a resolved one: `harness/env.sh` still discards a pre-set `HARNESS_REPO_ROOT` and will continue to until a human with authority over provisioned surface applies the line above. What this run changed is that the discarding no longer has any bearing on where the dataset may be written |
| What a human must do | apply the one-line fix to `harness/env.sh` under provisioning authority, or accept that a pre-set `HARNESS_REPO_ROOT` is discarded on sourcing and require `--repo-root` of any caller that owns a root other than the checkout's. Either way the normalizer needs no further change |
| The same defect's **second** variable, and the second contract it breaks | `HARNESS_REPO_ROOT` is not alone: `HARNESS_DIR` is assigned unconditionally in the same block, at `harness/env.sh` line 19, so **two** of the file's exports discard a pre-set value while the other twenty-seven use `${VAR:-default}`. That matters beyond the normalizer, because **both preflight helpers document an override this pair destroys**: a caller who exports `HARNESS_DIR` to point the checks at another harness root has that value replaced on sourcing, so the documented per-clone override cannot be exercised through the environment file at all. The patch is the same shape for both lines — `HARNESS_DIR="${HARNESS_DIR:-$(cd "$(dirname "$_harness_self")" && pwd)}"` and the `HARNESS_REPO_ROOT` form above — and it is declined for the same four authorities |
| What actually protects the condition the override exists for | the one thing a set override would have been used to check is the smoke redirect, and that is verified directly rather than through the override: `preflight_scan_target.py --check-only` reports `[PASS] smoke-override-absent` at `sec-gate-scan-target.log` line 31, confirming `HARNESS_SMOKE_TARGET` is unset, which is the condition AAP §0.8.1 actually requires. So the override's loss costs this run nothing it needed, and the defect is recorded rather than worked around |
| Owner | [§9](#9-normalization-and-the-dataset), which states the declaration and its precedence, and `harness/env.sh`, whose header states the contract and whose lines 19 and 21 depart from it. **D30** records the same file's other declined patch |

### D24 — the publication-owner gate's one standing disagreement: the stated absolute root is the publication checkout's, not the working clone's

| Field | Value |
| --- | --- |
| Expected | `harness/lib/verify_publication_owners.py`'s `check_repository_root` asserts two things about this file: that it states **exactly one** absolute root, and that the root it states equals `git rev-parse --show-toplevel` measured in the checkout the gate runs in |
| Observed | the first assertion passes and the second does not, in **any working clone**. This file states the root of the checkout this branch is **published** from; the gate runs in the working clone the branch is published **from a different path**, and the two are different directories by construction. The gate therefore reports **102 owner/copy pairs checked, 1 disagreeing** — that one pair, and no other — in this clone, and would report 0 disagreeing when run in the checkout this branch is reconciled into |
| Why it is not "fixed" | writing the working clone's own root here would invert the failure rather than remove it: the citation would then be wrong in the place the document is read from and right only in a clone that no longer exists once the run ends. Every other checkout of this branch — and there are dozens on this host at once — would measure a third root again. The gate's own instruction is "never edit the owner to match", and the owner here is `git rev-parse` in the publication checkout, so the copy is already the value the owner produces where it counts |
| Verified | the disagreement was measured **before this checkpoint changed anything** and again after every change it made: the same single pair, the same two values, and no second pair ever added. The gate's other 101 pairs pass, its locator families classify 173 of 173 references in this file with 0 unattributed and 0 unexplained, and `--self-test` passes 50 hand-written cases and 14 live document mutations with 0 passing |
| Disposition | **recorded as a standing, structural disagreement rather than repaired.** It is a property of running a publication check inside a pre-publication clone, not a drifted copy |
| What a human must do | read the gate's verdict as "**1 disagreeing, and it is this one**" rather than as a pass/fail, or — the cleaner outcome this document recommends — give `check_repository_root` the publication root explicitly so it compares the stated root against the path the branch is published to rather than against the clone it happens to run in |
| Owner | `harness/lib/verify_publication_owners.py`, whose `check_repository_root` states both assertions, and [§"How to read a citation in this file"](#how-to-read-a-citation-in-this-file), which states which root this file prints and why a reader elsewhere measures their own |

### D25 — files AAP §0.6.1 marks REFERENCE were edited on 2026-09-02; **two of those edits were reverted on 2026-09-03** and are published as provisioning patches instead

| Field | Value |
| --- | --- |
| Expected | AAP §0.6.1, §0.6.3, §0.6.5 and §0.8.2 mark the provisioned harness surface REFERENCE — read, never written — and name only two files under `harness/` that this run may change: `harness/scope/allowlist.txt` and `harness/cpg/spark.cpg`, both conditionally. Neither was changed: the allowlist is byte-identical to the twelve globs and was left as found, and the graph was not touched |
| Observed, on 2026-09-02 | **Five files outside that permission were written**: `harness/ENVIRONMENT.md`, `harness/bin/run-joern.sh`, `harness/lib/joern-scan.sc`, `harness/lib/run-joern-gated.sh` and `harness/lib/preflight_graph_identity.py`. Each existed to satisfy a requirement the same AAP makes binding and which the delivered code did not meet: **(1)** §0.6.4 and §0.9.1 require the graph's recorded identity re-verified before *every* load, and the canonical runner printed the pair without comparing it — the comparison lived only in a wrapper the delivered load did not take; **(2)** §0.5.4, §0.8.2 and §0.9.1 require the Stage 3 Joern JVM at 64 GiB or above, and the runner's `-J-Xmx` reached only the launcher, leaving the JVM that held the graph at a measured 32,178,700,288 bytes; **(3)** the environment record contradicted the graph on disk, and §0.1.3's first case makes the adjudicating record govern once an anchor exists, which one does |
| Observed, on 2026-09-03 — **two of the five were reverted** | `harness/bin/run-joern.sh` and `harness/lib/joern-scan.sc` are back to their **provisioned bytes**: the runner is 76 lines / 3,380 B / sha256 `32dd647af10709b72d159d67a2b15bd6f1f258af97614a9d2bf577c7a1abe65f`, the script 122 lines / 5,401 B / sha256 `cf7a3622a0635db3932b414427ff1b4b416b6050a024ea37651d5d89b91c0fa4`, and `git diff a64216aed7f` is empty for both. The ground is the AAP read as written rather than as purpose: §0.6.1 marks **every** entry in `harness/bin/` REFERENCE, §0.8.1 states outright that no runner file is edited, and §0.3.2 makes a runner whose reach or behaviour differs from expectation **a condition to record, not a defect to repair**. Both defects are therefore published as provisioning patches with their reachability — see **D26** — and the two controls they had added are supplied from **outside** the runner instead, which is the only place this run may put them |
| What of the five stands | **`harness/ENVIRONMENT.md`** stands, and was re-anchored a second time on 2026-09-03 after the re-provisioning falsified the first correction (**D4**). **`harness/lib/run-joern-gated.sh`** and **`harness/lib/preflight_graph_identity.py`** were never edits at all: neither exists in the provisioned blob, so both are **additions** under §0.5.2's convention for non-runner helpers, and they are carried as such in **D21** rather than as REFERENCE violations here. `preflight_graph_identity.py` gained the method-count floor on 2026-09-03 (**D27**) |
| Why the prohibition was read as not reaching the record edit | Two grounds, both checkable. First, provenance: `git log -- harness/` shows `harness/ENVIRONMENT.md` authored and committed by `Blitzy Agent <agent@blitzy.com>` as this project's own work, so it is agent-committed repository content **describing** the provisioning rather than the inherited external provisioning surface §0.6.1's REFERENCE marking is about. Second, purpose: the no-edit rule exists so that no dataset is produced under a configuration nobody recorded, and **nothing that bears on comparability was changed** — no scanner flag, no ruleset, no feed, no scope glob, no artifact path, no artifact shape, no path base, no query and no traversal bound. That second ground was also the argument for the two runner edits, and **it did not survive**: §0.8.1 states the runner prohibition without a purpose test, so a fix that changes nothing comparability-relevant is still a fix to a file this run may not write |
| What the surviving edits do **not** do | They do not alter any runner, `harness/env.sh`, `harness/lib/scope.sh` or `harness/scope/allowlist.txt`; they do not rebuild, replace or write the graph; they write nothing under `/opt`; and they do not change the dataset's derivation. `harness/artifacts/raw/` holds exactly the eight canonical artifacts and nothing else |
| Verified, in the generation that has since been reverted | The 2026-09-02 corrected runner was executed canonically — direct, no arguments — with its raw and log output redirected into private scratch so no canonical artifact could be overwritten. The identity gate reported before the load and `VERDICT: PASS`; both JVMs measured `MaxHeapSize` **68,719,476,736** on the JDK 21 `jcmd VM.flags`; and the artifact it produced carried **693** findings identical element for element to that generation's delivered artifact, byte-identical once `elapsed_ms` is normalised. Three negative tests confirmed its refusals: a wrong-identity graph gave gate exit 77 → runner exit **78** with no load and no artifact; a sub-floor `HARNESS_JOERN_HEAP` gave exit **78** up front; and a sub-floor child heap reached past the up-front check gave exit **78** with the artifact **removed**. All four measurements are retained as that generation's and **none of those refusals exists in the provisioned runner now** — most pointedly the last: nothing in the current configuration can withdraw an artifact after the fact, which **D26** states plainly rather than implying a control that is not there |
| Verified, in the generation on record | The two controls are supplied from outside the runner and both were exercised for real. Identity: `harness/lib/preflight_graph_identity.py --check-only` ran at `2026-09-03T09:07:46Z`, exit **0**, `VERDICT: PASS`, one second before the `09:07:47Z` invocation it governs — and the same gate had exited **77 `VERDICT: HALT`** hours earlier while the record was stale, with `./harness/bin/run-joern.sh` exiting **78 CONFIGURATION FAULT** without loading anything, which is the evidence the control binds. Heap: `JAVA_TOOL_OPTIONS=-Xmx64g` was supplied in the invocation environment — the override AAP §0.6.5 sanctions — and the **child** JVM was then measured externally rather than trusted, `jcmd` against pid 393655 reading `-XX:MaxHeapSize=68719476736` and `-XX:SoftMaxHeapSize=68719476736` with `JAVA_TOOL_OPTIONS=-Xmx64g` in its environ ([§6.3](#63-the-stage-3-joern-runner--third-of-four)) |
| Disposition | **Reported, and half of it withdrawn.** The 2026-09-02 reading of §0.6.1 was disclosed rather than absorbed; on re-reading, the runner half of it was **wrong**, and the remedy was to restore the provisioned bytes and publish the defects as patches. Both the superseded and the current state of every value are retained wherever either is cited |
| What a human must do | Decide whether the surviving record edit is acceptable, and act on **D26**'s two patches. The earlier edition of this row recommended **against** reverting the two code fixes, on the ground that reverting restores a runner that loads a graph without comparing it and holds it in a 29.97 GiB JVM while printing `64g`. That recommendation is **withdrawn as a recommendation and retained as a description**: the runner does behave that way, which is exactly why the patches in **D26** matter — but the fix belongs to provisioning, not to a clone, and until provisioning applies it the controls must be supplied from outside the runner as this run supplied them. The record edit alone is cleanly revertible: every superseded value is preserved in that document's supersession appendix |
| Owner | This entry, **D26**, and the files themselves. `harness/ENVIRONMENT.md` carries the reason for its change in its supersession appendix; `harness/artifacts/logs/runner-metadata.json` carries the revert in `tools.joern.runner_script_identity` and the outside-the-runner heap override in `tools.joern.heap_override`, whose `floor_enforcement` field reads **"NONE INSIDE EITHER PROVISIONED FILE"** |

---
### D26 — the provisioned Joern runner neither compares the graph's identity nor sizes the JVM that holds it; recorded, patched on paper, not edited

| Field | Value |
| --- | --- |
| Expected | AAP §0.6.4 and §0.9.1 require the graph's recorded identity **recomputed and compared immediately before every load**, and §0.5.4, §0.8.2 and §0.9.1 require the JVM that holds the graph at 64 GiB or above |
| Observed, in `harness/bin/run-joern.sh` at its provisioned 76 lines | **(a)** the runner *prints* its input's identity — `printf 'cpg   : %s -> %s\\n'` at its line 56, `cpg bytes` from a `stat -c%s` at its line 57 and `cpg sha256` from a `sha256sum` at its line 58 — and **compares none of it** against any record; it invokes no gate of any kind. **(b)** the only heap flag it passes is `-J-Xmx"$HARNESS_JOERN_HEAP"` at its line 70, and `joern --script` starts a parent `ReplBridge` JVM which forks a child `NonForkingScriptRunner`; the **child** is what runs `importCpg` and every query, so `-J-Xmx` sizes only the launcher and the child inherits a default. Measured on the 2026-09-01 invocation, that child held **32,178,700,288** bytes while the console printed `64g` |
| The patch, published rather than applied | **(a)** before the `joern --script` invocation, call `python3 "$HARNESS_DIR/lib/preflight_graph_identity.py" --check-only`, capture its exit code, and on any non-zero value fail the runner through `scope_fail` with the gate's own report quoted — so a mismatch refuses the load instead of printing past it. **(b)** export `JAVA_TOOL_OPTIONS=-Xmx<heap>` alongside the existing `-J-Xmx`, since `JAVA_TOOL_OPTIONS` is the documented environment override that reaches a forked child, and assert the child's own `Runtime.maxMemory()` against the floor rather than trusting the request. Both belong in the provisioned runner, which is where the control has to live for a **third party's** direct invocation to be bound by it |
| Why it is not applied here | AAP §0.6.1 marks every entry in `harness/bin/` REFERENCE, §0.8.1 states that no runner file is edited, and §0.3.2 makes exactly this kind of condition *"a condition to record, not a defect to repair"*. A 2026-09-02 lane did apply both fixes in place; the edits were **reverted to the provisioned bytes on 2026-09-03** and the file's `git diff` against `a64216aed7f` is empty (**D25**) |
| The run-owned mitigation, and its limit | This run supplies both controls from **outside** the runner, and both were measured rather than assumed: `preflight_graph_identity.py --check-only` at `2026-09-03T09:07:46Z`, exit 0, `VERDICT: PASS`, one second before the invocation; and `JAVA_TOOL_OPTIONS=-Xmx64g` in the invocation environment, with `jcmd` against the child pid 393655 (launcher 393590) reading `-XX:MaxHeapSize=68719476736`, `-XX:SoftMaxHeapSize=68719476736`, `-XX:InitialHeapSize=2147483648` and a peak G1 heap of `total 67076096K, used 53751346K`. **The limit is explicit**: a mitigation outside the runner binds only the invocation that carries it. A third party running `./harness/bin/run-joern.sh` directly gets neither control, and there is a third gap with no mitigation at all — **nothing in this configuration can withdraw an artifact after the fact**, so a load that somehow proceeded on wrong bytes would leave its artifact in place. That is stated rather than implied, because the 2026-09-02 runner *could* withdraw one and this one cannot. The floor itself now lives in `harness/lib/preflight_graph_identity.py`, which declares `METHOD_COUNT_FLOOR` at its line 331 (**D27**), and `runner-metadata.json` records `floor_enforcement` as **"NONE INSIDE EITHER PROVISIONED FILE"** |
| Disposition | **Recorded, with the patch and its reachability published, and deliberately not repaired in the clone.** Reachability: both conditions are reachable by anyone who invokes the runner directly, which is the canonical documented way to invoke it, so neither is theoretical — what is bounded is the blast radius, since the mitigations above cover the invocation this run publishes |
| What a human must do | Apply both patches in provisioning, then re-provision. Until then, every Stage 3 invocation must run the identity gate and supply `JAVA_TOOL_OPTIONS` from outside the runner, exactly as this run's sequence does, and must measure the child rather than the request |
| Owner | `harness/bin/run-joern.sh` itself at its provisioned bytes; `harness/artifacts/logs/joern-preflight.log`, which names that runner as **not** a caller of the gate; and `harness/artifacts/logs/runner-metadata.json` fields `tools.joern.runner_script_identity`, `tools.joern.heap_override` and `tools.joern.stage3_invocation_2026_09_03.child_jvm_measured_externally` |

### D27 — RESOLVED: AAP §0.9.2's method-count floor was unimplemented; it is now a declared constant and an adjudicated verdict

| Field | Value |
| --- | --- |
| Expected | AAP §0.9.2 lists "a method count below **853,420**" among the conditions that stop the run, and §0.9.3 makes the same figure **one-sided** — a count at or above it is recorded, a count above the 898,336 anchor is recorded, and only a count below the floor halts |
| Observed, before this checkpoint | the floor existed only in prose. The literal `853420` appeared in **no** harness file, so nothing measured the graph's method count against it and a truncated graph would have loaded silently — which is precisely the failure mode §0.9.2 names, since a truncated graph's silence is indistinguishable from a clean result |
| Resolved by | `harness/lib/preflight_graph_identity.py`, which now declares `METHOD_COUNT_FLOOR` at its line 331 and `METHOD_COUNT_ANCHOR` at its line 340, and adjudicates the count as part of the same `--check-only` pass that adjudicates the identity. Neither constant is in the file's HEAD blob, so both are this checkpoint's addition |
| Verified at runtime | `python3 harness/lib/preflight_graph_identity.py --check-only` → exit **0**, and its report carries the adjudication in its own words: `Adjudicated count : 1,398,964 methods, agreed by 2 record(s) of account` at `joern-preflight.log` line 88 and `Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420` at its line 89, with the floor's basis printed by `harness/lib/preflight_graph_identity.py` at its line 1124. **1,398,964 is 1.64× the floor**, so the condition is satisfied with a wide margin rather than narrowly |
| One-sidedness, stated because it is what stops this becoming a window | the count also **exceeds** the 898,336 anchor by **+500,628**, and that is recorded and never halted (§0.9.3): the anchor was measured over a narrower input set than the graph in use carries, and more input cannot yield fewer methods. The floor is the only threshold, and it is a lower bound. `cpg-verify.log` PART 3 prints `METHOD_ANCHOR 898,336, exceeded, which is recorded and never halted because the anchor is one-sided (AAP 0.9.3)` in exactly those terms |
| Disposition | **RESOLVED, and exercised rather than only committed.** The check runs in this run's own gate sequence, immediately before the Stage 3 load, and its verdict is published |
| What a human must do | nothing for the floor itself. One thing follows from it: the floor now lives in a `harness/lib/` helper rather than in a runner, so a **direct** `./harness/bin/run-joern.sh` invocation is not bound by it — the same caller-side limit **D26** states for the identity check, and with the same remedy |
| Owner | `harness/lib/preflight_graph_identity.py` line 331; `harness/artifacts/logs/joern-preflight.log` lines 88-89; [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) |

---
### D28 — nothing in `harness/bin/` invokes the scan-target preflight; this run invokes it as a published gate, and its artifact-tree check deliberately does not refuse

| Field | Value |
| --- | --- |
| Expected | the scan-target preconditions AAP §0.8.1 and §0.9.1 name — the pinned HEAD, an unset smoke override, and both artifact trees present — checked before any scan |
| Observed | **no runner calls the check.** `harness/lib/preflight_scan_target.py` exists and works, but nothing under `harness/bin/` invokes it, so a wrong scan target or a set smoke override is accepted by the canonical direct route in silence. That is the same caller-side shape as **D26**, and it has the same cause: binding the check inside a runner would mean editing a runner, which AAP §0.8.1 forbids |
| The run-owned mitigation, exercised | this run invokes it itself, outside every runner and before Stage 3: `python3 harness/lib/preflight_scan_target.py --check-only` → exit **0**, `VERDICT: PASS` at `sec-gate-scan-target.log` line 194, with `[PASS] smoke-override-absent` at its line 31 — which is the check that actually matters for the smoke override, since the condition is that `HARNESS_SMOKE_TARGET` be unset and the gate confirms it is — and `[PASS] artifact-tree:HARNESS_RAW_DIR` and `[PASS] artifact-tree:HARNESS_LOG_DIR` at its lines 145 and 155. The structured form is `sec-gate-scan-target.json`. The artifact-tree census and the scan-target path-text checks are this checkpoint's addition: `harness/lib/preflight_scan_target.py` declares `check_artifact_trees()` at its line 600, and the string `artifact-tree` occurs **0** times in that file's HEAD blob against 5 in the working tree |
| Why the artifact-tree check passes a non-empty tree, stated plainly | **it censuses, it does not refuse.** Refusing would leave a caller two ways forward — create the tree or clear it — and AAP §0.8.1 permits **neither**: this run may not create either tree and may not clear either. A check that can only be satisfied by a prohibited act is a check that forces a violation, so this one publishes what it found and passes. The halt on emptiness belongs to the Stage 0 gate, which recorded it at 8 and 85 entries and which **no clone can clear** (**D0**, [§1](#1-gate-verdicts)). Reading this check's PASS as evidence that the trees were empty would be exactly wrong, which is why it prints the census beside the verdict |
| Disposition | **Mitigated outside the provisioned files, with the root cause recorded and its limit named.** The mitigation binds the invocation that carries it and nothing else; a third party invoking a runner directly gets no scan-target check at all |
| What a human must do | either make `harness/lib/run-scanner-gated.sh` the documented entry point for all nine runners, or apply the check inside `scope_resolve_target` at provisioning so the direct route carries it — and separately, present a provisioning whose two artifact trees are empty, which is the only thing that clears **D0** |
| Owner | `harness/lib/preflight_scan_target.py` line 600; `harness/artifacts/logs/sec-gate-scan-target.log` lines 31, 145, 155 and 194; **SEC-01** in the security register below |

---
### D29 — `harness/bin/run-trivy.sh` interpolates a path variable into `python3 -c` source (CWE-94); recorded, patched on paper, not edited

| Field | Value |
| --- | --- |
| Expected | a path is passed to a program as **data**, not spliced into the program's source text |
| Observed | `harness/bin/run-trivy.sh` builds a Python program by string interpolation of `$TRIVY_CACHE_DIR` at its lines 52-53, so any value that variable holds is evaluated as Python source. The security testing run raised it as **SEC-03** and it was **reproduced first-hand rather than assumed**: a crafted value fed to that runner's verbatim line exited **0** and wrote a marker file, which is arbitrary code execution and not a theoretical shape |
| The patch, published rather than applied | pass the directory as an argument and read it from `sys.argv[1]` in a fixed program — `python3 -c '<fixed program>' "$TRIVY_CACHE_DIR"` — so no caller-controlled text ever enters the source text. It is a one-line change to a two-line construct and it alters nothing about what the runner does |
| Reachability, measured rather than characterised | `$TRIVY_CACHE_DIR` is set by `harness/env.sh` to a fixed provisioned path and is **not** derived from any untrusted input on this host, so on the configuration this run measured there is no attacker-controlled route into it. That is a statement about this host's configuration, **not** an adjudication that the defect does not matter: the variable is an ordinary environment variable, so any caller who can set the environment can reach it, and the environment file's own header states that a caller may override its values per clone |
| Why it is not applied here | it is a runner file. AAP §0.6.1 marks every entry in `harness/bin/` REFERENCE, §0.8.1 states that no runner file is edited, and §0.3.2 makes a runner whose behaviour differs from expectation *a condition to record, not a defect to repair*. **D25** records what happened the one time this run's lineage edited a runner anyway |
| The run-owned mitigation, and its limit | `harness/lib/run-scanner-gated.sh` refuses a hostile value at exit **77** before dispatching, naming the offending characters by codepoint and offset and reproducing **0** occurrences of the hostile substring in its own output. That binds the **gated** route only. **The direct `./harness/bin/run-trivy.sh` is exactly as provisioned, hostile value and all**, and this run's own Stage 3 Trivy invocation was direct — it ran with the provisioned fixed cache path, which is why it was safe, not because a control stopped it |
| Disposition | **Recorded with its patch, its reachability and its mitigation's limit. Not repaired in the clone.** Nothing was exploited beyond the single first-hand reproduction the security pass performed to confirm the finding, and no finding about Spark was judged |
| What a human must do | apply the `sys.argv[1]` patch in provisioning, or make the gated wrapper the documented entry point for all nine runners |
| Owner | `harness/bin/run-trivy.sh` lines 52-53; `harness/artifacts/logs/sec-gate-scan-target-cases.log` and `sec-gate-scanner-gated-cases.log`; **SEC-03** below |

---
### D30 — `harness/env.sh` creates both artifact trees on every source, so the mandated missing-tree halt cannot fire; declined in-clone on AAP grounds

| Field | Value |
| --- | --- |
| Expected | AAP §0.8.1 and §0.9.2 make a **missing** artifact tree a provisioning fault that stops the run — this run neither creates nor clears either tree — and `harness/lib/scope.sh` carries the guard for it, testing each directory with `-d` at its lines 68-69 and failing if either is absent |
| Observed | that guard can never fire for a missing tree, because the environment file every runner sources **creates both trees first**. `harness/env.sh` line 91 is `mkdir -p "$HARNESS_RAW_DIR" "$HARNESS_LOG_DIR" "$HARNESS_SCRATCH_DIR" 2>/dev/null \|\| true`, so by the time any runner reaches the `-d` test both directories exist by construction. The `\|\| true` compounds it: even a `mkdir` that fails is swallowed, so a tree that could not be created is not reported either |
| The patch, published rather than applied | drop the two artifact trees from that `mkdir -p` and drop the `\|\| true`, leaving the scratch directory — which is this run's own working space and legitimately created — as the only thing the line makes: `mkdir -p "$HARNESS_SCRATCH_DIR"`. The `-d` guards in `scope.sh` then mean what §0.9.2 says they mean, and a `mkdir` failure on the scratch directory surfaces instead of being discarded |
| Why it is not applied here | `harness/env.sh` is provisioned surface and REFERENCE under four independent authorities, which **D23** enumerates for the same file: AAP §0.3.1 lists the environment file among "files read but never written"; §0.6.3 permits changing exactly two provisioned paths under `harness/` and states that no harness helper is edited; §0.6.5 states outright that **no environment file** is edited; and the clone instantiation instructions name `harness/env.sh` among the files not to edit. The file is byte-identical to what provisioning delivered |
| The run-owned mitigation actually exercised, and what it does not do | this run **censuses both trees rather than relying on a guard that cannot fire**: `preflight_scan_target.py --check-only` reports `[PASS] artifact-tree:HARNESS_RAW_DIR` and `[PASS] artifact-tree:HARNESS_LOG_DIR` at `sec-gate-scan-target.log` lines 145 and 155, publishing what each tree holds, and the Stage 0 gate's own reading of both trees is retained unaltered (**D0**). That covers *observability* — nothing about either tree is unrecorded — and it does **not** restore the halt: a missing tree would still be created silently by the next `source harness/env.sh` before any check saw it absent, and **D28** states why this run's own check censuses rather than refuses |
| Disposition | **Declined in-clone on AAP grounds, patch and reachability published, mitigation named with its limit.** Recorded rather than repaired, per §0.3.2 |
| What a human must do | apply the two-part patch above under provisioning authority. Note the interaction before deciding: with it applied, a provisioning that does not create the two trees makes every runner fail at `scope.sh` line 68 — which is the mandated behaviour, and it means the trees must be provisioned rather than incidentally created |
| Owner | `harness/env.sh` line 91 and `harness/lib/scope.sh` lines 68-69, both at their provisioned bytes; **D23** for the same file's other declined patch; **D0** and [§1](#1-gate-verdicts) for the tree condition itself |

---
### D31 — `harness/lib/scope.sh` evaluates a caller-supplied variable name through `eval`; recorded, patched on paper, not edited

| Field | Value |
| --- | --- |
| Expected | a variable name resolved without handing text to the shell's evaluator |
| Observed | `harness/lib/scope.sh` line 106 is `eval "_state=\\${$1:+set}"`, so the helper's first argument is expanded as shell text. A caller passing anything other than a bare name has its content evaluated |
| The patch, published rather than applied | use an indirect expansion instead — `_state="${!1:+set}"` — which resolves the named variable without an evaluator, is available in the Bash the harness already requires, and changes nothing about the helper's output |
| Reachability, measured rather than characterised | the helper is called from **nine literal call sites**, each passing a hard-coded credential variable name, so no caller-controlled text reaches it in this harness as provisioned. That is why the finding is LOW rather than HIGH, and it is a statement about the current call sites only: the sink is real, it is one call site away from being reachable, and a future caller passing a computed name would reach it with nothing in between |
| Why it is not applied here | `harness/lib/scope.sh` is the shared scope library — provisioned harness surface, REFERENCE under AAP §0.3.1, and covered by §0.6.3's "no runner or harness helper is edited". The file is byte-identical to what provisioning delivered |
| Disposition | **Recorded, patched on paper, deliberately not repaired.** No mitigation is claimed for it, because none is needed on the measured call graph and inventing one would overstate the position |
| What a human must do | apply the one-token change at provisioning. It is the smallest of the patches this register publishes and it removes the sink outright rather than guarding it |
| Owner | `harness/lib/scope.sh` line 106 at its provisioned bytes; **SEC-01** and **SEC-03** below for the same file's family of caller-side conditions |

---
### QA testing findings F1–F5 (2026-09-02) — first-hand re-verification and where each is answered

Runtime testing was executed against this checkpoint on 2026-09-02 and raised **five**
blocking findings — four CRITICAL, one HIGH — against Stages 0, 1, 2 and 5. Every one of
them was **re-executed from its own reproduction steps in this clone** on the same date, and
**all five still reproduce**. This subsection is the register: what each finding observed,
what re-execution here observed, the evidence file that carries the measurement, the
divergence above that owns the condition, why no action available inside this run clears it,
and the decision a human has to take. Nothing below is a new claim of compliance — the
document's status is unchanged and remains the one at the top of this file.

**Two properties of the pass that produced this subsection, stated because they are what
keeps it inside the barrier D0 describes.** It performed **measurement only**: no
`harness/bin/run-*.sh` runner was invoked, `harness/artifacts/raw/` still holds the same
**eight** artifacts byte for byte, no dataset row was produced, no stage was advanced and
nothing under `/opt` was written. And its own evidence went where AAP §0.8.1 says such
evidence goes — the `reverification-*` files under `harness/artifacts/logs/`, **fourteen when
that pass wrote them and eighteen now**, published per file in
[§16](#16-manifest-of-the-two-git-ignored-artifact-trees).

| Finding | What it observed | Re-executed here, 2026-09-02 | Evidence written by this pass | Owned above by |
| --- | --- | --- | --- | --- |
| **F1** CRITICAL — the gate was handed non-empty artifact trees and later stages ran anyway | `raw` and `logs` already populated when the gate censused them (8 and 85 entries), gate verdict `halt`, and Stage 2–5 artifacts present regardless | Reproduces. `raw/` **8** files / **120,538,389** bytes and `logs/` **137** files / **143,200,007** bytes **at that pass's census instant of 2026-09-02**, before its own fourteen — re-measured on 2026-09-03 the trees hold **8** files / **120,536,620** bytes and **163** files / **144,456,803** bytes, so the condition is present again and larger ([§16](#16-manifest-of-the-two-git-ignored-artifact-trees)); **every entry in both trees is git-tracked** while `git check-ignore -v --no-index` reports `.gitignore:31:artifacts/` for a path in each, so the members were force-added; the gate re-parses to `overall: halt`, `authorises: nothing`, 43 checks (38 pass, 3 recorded difference, 2 halt); set difference against the gate's own retained listing is **0** entries removed from either tree and **0** added to `raw/` | `reverification-f1-artifact-trees.txt` | **D0** |
| **F2** CRITICAL — the provisioned record contradicts the graph on disk | record 541,255,894 bytes / `26d327cc…` against disk 541,309,809 bytes / `4616845a…` | Reproduced at `14:33:46Z`, then **RESOLVED at the record the same day.** The reproduction: `stat -Lc %s` → **541,309,809**; sha256 over a full read, taken twice (through the symlink and on the resolved target) → **`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`**; target mtime `2026-08-30 19:18:37Z`; the record then stating 541,255,894 / `26d327cc…fcffc` / 1,397,339 methods / 119,691 type declarations against the load's 1,396,899 / 119,721 / 45,037, at its own sha256 `5aa68b25…b140`. That pass concluded the condition could not be repaired, on AAP §0.1.3's fourth case. **That conclusion was wrong and has been overturned**: the fourth case needs *no anchor to exist*, and the graph's write-time record of account — which that same pass quotes verbatim — adjudicates the pair explicitly in its "Expected vs observed (prior provisioning record)" block. `harness/ENVIRONMENT.md` was re-anchored to that owner, both values retained with provenance in a supersession appendix, the graph untouched; re-measured afterwards, the record, the filesystem and the owner **agree on every field**. The evidence file carries the measurements unaltered with the supersession appended. **Then the graph itself changed and the correction had to be repeated**: the host was re-provisioned at `2026-09-03T01:17:07Z`, the frontend rebuilt the graph to **547,980,224 / `325887cf…3dc6`** with counts 1,398,964 / 1,308,974 / 119,860 / 45,037, and until §7 was re-anchored a second time the identity gate exited **77 `VERDICT: HALT`** and `./harness/bin/run-joern.sh` exited **78** without loading anything. Re-anchored again, the gate exits **0** with `VERDICT: PASS`. Every figure from the 2026-09-02 reproduction above is retained as that generation's; the graph was untouched in both corrections | `reverification-f2-graph-identity.txt` (measurements as taken, plus its **SUPERSESSION** section); the 2026-09-03 pair is owned by `/opt/blitzy-harness/provision-log/cpg-identity.txt` and re-measured in `cpg-verify.log` PART 3 | **D4** |
| **F3** CRITICAL — the pinned frontend cannot persist the graph over every JAR the build produced | exit 1 after 21,956 s at `-J-Xmx128g` inside `flatgraph.storage.WriterContext.finish`, `OutOfMemoryError: Required array length 2147483639 + 76 is too large`, partial file not imported | Reproduces, and the **bound is now measured to be independent of the heap over a 16× range**: the retained probe, recompiled and run here under JDK 21 at `-Xmx8g`, `-Xmx64g` and `-Xmx128g`, reports `maxMemory` 8,589,934,592 / 68,719,476,736 / 137,438,953,472 and in **all three** arms buffers exactly **2,147,483,639** bytes and then throws `java.lang.OutOfMemoryError: Required array length 2147483639 + 77 is too large` — the failure point does not move by one byte. The frontend's own option surface, enumerated here, is **12 named options and one positional**: none chunks or streams the writer, none selects another output format, and `--full-resolver` and `--enable-file-content` would both enlarge the pool | `reverification-f3-writer-bound.txt`, `reverification-f3-writer-bound.json` | **D1** |
| **F4** HIGH — the accepted witness rule yields no evidence for 8 of the 38 JAR-producing modules | 29 modules with an exclusive class, one with the descriptor fallback, eight with neither | Reproduces, and the measurement the record previously lacked now exists. Over the input set the plan requires — **191** project outputs, reconciled first-hand from the pinned tree as 627 enumerated / 191 own / 422 copied dependencies / 14 test-resource fixtures / 0 undecided — **30 of 38** modules have an accepted witness (**29** exclusive class, **1** exclusive descriptor: `sql/connect/shims`) and **8** have neither, the same eight, compared programmatically and re-confirmed by a second code-disjoint script | `reverification-f4-module-witness-full-input-set.json`, `reverification-f4-module-witness-full-input-set.log`; the verdict itself is owned by `build-record.md` §6 | **D1** and §14's witness row, both extended by that measurement |
| **F5** CRITICAL — the mandated taint A/B does not discriminate | both arms exit 0 with one traced result at `DiskStore.scala` line 72 and byte-identical SARIF, 4,753 bytes / `7949617b…` | Reproduces **byte for byte across clones and generations**: arm ON 1 result at line 72 with 1 code flow, arm OFF 1 result at line 72 with 1 code flow, both SARIFs 4,753 bytes under sha256 `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` — the digest the testing run reported — with the measured argv delta being `--taint-intrafile` and the output path alone. Both remedies the finding proposes were then **measured constructible**: a flow that requires the toggle (`HiveShim.scala`, **2** traced findings at its lines 828 and 834 with the flag against **0** without it) and a taint-disable-equivalent control on the mandated subject and line (the pinned rule with its `pattern-sources` replaced by an unmatchable marker: **0** findings against the real rule's 1 at line 72). The engine surface was enumerated rather than assumed — 558 help lines, 101 option blocks, exactly **2** naming taint and both additive, 12 `--no-*` options and none taint-related, and `--optimizations=none` run and still reporting line 72 | `reverification-f5-taint-ab.log`, `reverification-f5-taint-ab.json`, `reverification-f5-taint-ab-{on,off}.sarif`, `reverification-f5-taint-ab-hiveshim-{on,off}.sarif`, `reverification-f5-taint-ab-source-removed.sarif`, `reverification-f5-taint-engine-options.txt` | **D2** |

**Why none of the five is repaired here, one line each, with the section that says so.**
F1 — AAP §0.3.2 and §0.8.1: this run neither creates nor clears either tree, and a non-empty
tree halts. **F2 is the exception: it was repaired, on 2026-09-02, after this pass.** Its "not
repaired" line read: the record side is REFERENCE-only (§0.6.1) and the graph side is F3, so
§0.1.3's fourth case applies. The fourth case does not apply — it requires that no anchor
exist, and the graph's own write-time record of account adjudicates the pair — so the record
was re-anchored to that owner with both values preserved, and the graph was left untouched.
See **D4** and the SUPERSESSION section of `reverification-f2-graph-identity.txt`. F3 — raising the heap is measured
ineffective, trimming the input set is prohibited (§0.3.2, §0.9.2), substituting the writer is
prohibited (§0.4.3), the frontend offers no option, and importing the partial file is never an
option. F4 — §0.5.1 admits exactly two witness kinds and no third, and both are unsatisfiable
for a module whose classes *and* Maven descriptor are wholly vendored by another module's
shaded artifact; §0.9.4's required outcome is to name the value as unestablished. F5 — §0.9.2
lists a failed taint A/B among the conditions that stop the run, and no rule, file, line or
flag was changed to obtain the expected zero.

**What a human has to decide, finding by finding.** F1: begin a generation from provisioning
that presents genuinely empty trees — and because every member is *tracked*, that means
removing them from this branch's index rather than deleting files from a working tree, the
tracked state having been introduced by commit `0e3e742a5ad`. F2: replace the graph and the
record **atomically in one provisioning act**, then validate their agreement from an empty
gate; the bytes are shared, so nothing inside a clone can do it — this pass measured **23**
clone checkouts on this host whose `harness/cpg/spark.cpg` all resolve to the one file. F3:
either authorise a Joern/flatgraph whose writer chunks or streams the string pool and rebuild
over the unchanged input set, or amend the single-complete-graph-over-every-JAR requirement,
for instance to a partitioned set of per-part graphs each with its own recorded identity —
there is no third form, and neither is available without amending §0.4.3 or §0.5.1. F4: either
admit a third injective witness kind — the staged coordinate and digest of each input archive
retained in graph metadata is the natural candidate — or accept **30 of 38** as the ceiling of
the present rule and record the eight as permanently unobtainable. F5: either move the mandated
pair's subject to a flow that crosses a method boundary, or redefine its "sole difference" from
the engine setting to the rule's `pattern-sources`, which is the only taint-free arm this
engine admits.

**One thing this register does not do.** It does not close any of the five, and it is not a
second opinion about whether they should have halted the run: four of them are conditions AAP
§0.9.2 itself lists among those that stop it, and the fifth is a value §0.9.4 requires be named
as unestablished. What the pass adds is that each is now measured **in this checkout, by this
pass, from the finding's own reproduction steps**, so a reader no longer has to take an earlier
lane's word or the testing run's for any of them.

---

### Security testing findings SEC-01–SEC-10 (2026-09-02) — six controls built and verified here, four toolchain facts recorded

A second testing run, this one security-focused, was executed against this checkpoint on
2026-09-02 and raised **ten** findings — five HIGH, four MEDIUM, one LOW. This subsection is
their register, and it differs from the F1–F5 register above in the way that matters most to a
reader: **F1–F5 all still reproduce, whereas six of these ten were closed here by code this
pass wrote, and every one of the six was then re-executed from the finding's own reproduction
steps and observed to behave the way the finding asked for.** The remaining four are toolchain
advisory facts that no clone has the authority to repair, and they are recorded with the
provisioning act each one needs.

**What made six of them closable without touching a single prohibited file.** Three of the ten
have their root cause in a file this run may not edit — `harness/lib/scope.sh`,
`harness/bin/run-trivy.sh` and `harness/ENVIRONMENT.md` are all REFERENCE-only under AAP
§0.6.1, and §0.8.1 forbids editing a runner or a baked flag outright. A control that cannot sit
in the defective file has to sit in front of it, so each became a **fail-closed pre-scan gate**
in this run's own `harness/lib/` code — the pattern the committed `run-joern-gated.sh` and
`preflight_graph_identity.py` already established at an earlier checkpoint. Two new files carry
them: `harness/lib/preflight_scan_target.py` — **63,006 bytes / 1,204 lines**, sha256
`7166304bdc99e470190eb274c8b9a26492a7413ba7e678a6d17e49419794dab0`, having grown on 2026-09-03
by the artifact-tree census at its line 600 and the scan-target path-text checks (**D28**);
that pass measured it at 41,591 bytes / `f96b0cca…` and both measurements are kept — and
`harness/lib/run-scanner-gated.sh` (8,742 bytes, sha256
`eb6313de56e32ca73e657330b8e37f2bf970b78fec3ebcafabdcb594895fdce0`, unchanged since).
`harness/lib/preflight_graph_identity.py` likewise grew, to **65,696 bytes / 1,232 lines**,
sha256 `7c455c7b66f5ddfcb5c9206b8d357af8aa26785751e9654c0712c5dfcca7298f`, by the method-count
floor it declares at its line 331 (**D27**). The consequence a reader
must not miss is stated in the residual column: **a gate binds the gated route and leaves the
direct route exactly as provisioned**, which is precisely what §0.8.1 requires and precisely
why three of these findings are closed *as controls* while their root causes stay open as
provisioning work.

**Two properties of this pass, stated because they are what keeps it inside the same barrier.**
It invoked **no** `harness/bin/run-*.sh` runner: `harness/artifacts/raw/` still holds the same
**eight** artifacts byte for byte, no dataset row was produced and no stage was advanced. And
re-normalizing the committed raw tree into a private scratch directory reproduced
`findings.json` under sha256 `d4e28c823fd1e76c2158130dc941762e0c6cf23424c0c990c930cc84ece6fc54`
and `findings.csv` under `9f646532494fcba3ad95a8e10f15f77957b9f16bea0b486b513e2a830f5445e6` —
**both byte-identical to the dataset committed on that date**, which is the evidence that two
normalizer changes in that pass moved no row. Both digests are that generation's and are
**no longer** the committed dataset's: the Stage 3 re-invocation of 2026-09-03 rewrote
`raw/joern.json`, so the committed pair is now `339732ed…` and `895d250f…`
([§9](#9-normalization-and-the-dataset)). The property this paragraph asserts — that the
2026-09-02 pass moved no row — is unaffected by that, because it compares two runs over one
unchanged raw tree.

| Finding | What it observed | Re-executed here, 2026-09-02 | Evidence written by this pass | Disposition |
| --- | --- | --- | --- | --- |
| **SEC-01** HIGH — a wrong-HEAD scan target and a set smoke override are both accepted silently | `scope_resolve_target` returns `HARNESS_SMOKE_TARGET` when it is set and never compares `SPARK_SRC`'s HEAD against the pin, so a scan can run against the wrong tree or one small directory and report success | **Control built and verified.** Five cases through `preflight_scan_target.py --check-only`: clean environment → exit **0** PASS; `SPARK_SRC` pointed at the working checkout → **77** REFUSE; a wrong HEAD *with* `SPARK_SRC_COMMIT` overridden to agree with it → **77** REFUSE, because the gate compares against a pin authored in the module itself and only then checks that the environment's pin agrees; `HARNESS_SMOKE_TARGET` set to a directory → **77**; set to the empty string → **77** | `sec-gate-scan-target.log`, `sec-gate-scan-target.json`, `sec-gate-scan-target-cases.log` | **CONTROL RESOLVED**; root cause is REFERENCE-only. **Extended 2026-09-03**: nothing in `harness/bin/` invoked this gate, so this run invokes it itself as a published gate before Stage 3 — exit **0**, `VERDICT: PASS` at `sec-gate-scan-target.log` line 194, with `[PASS] smoke-override-absent` at its line 31 and the two artifact-tree censuses at its lines 145 and 155. **D28** carries that and the limit of it |
| **SEC-02** HIGH — the graph-identity preflight passes while the environment record contradicts the graph on disk | when the finding was raised the record stated 541,255,894 bytes / `26d327cc…` and the disk held 541,309,809 / `4616845a…`, and the preflight resolved its record of account by provenance without ever reading the record that contradicts it — so it returned PASS. Both figures are that generation's; the graph on disk is now 547,980,224 / `325887cf…3dc6` | **Control built and verified, and it now fails closed.** `preflight_graph_identity.py --check-only` → exit **77**, VERDICT HALT, quoting the provision log at 541,309,809 / `4616845a…` against `harness/ENVIRONMENT.md` §7 at 541,255,894 / `26d327cc…`, labelling §7 "the authoritative environment record", citing AAP §0.1.3's fourth case and naming the remedy as a provisioning act. §7 is now an unconditional third record candidate, and an unreadable or ambiguous §7 is itself exit **78** | `sec-gate-graph-identity-cases.log`; case 7 of that file runs the same gate against a scratch copy whose §7 is corrected and it exits **0**, which is the proof the remedy works. **The remedy was then proved on the real tree rather than a copy**: after the `2026-09-03T01:17:07Z` re-provisioning the same gate exited **77 `VERDICT: HALT`** naming §7 as the sole dissenting record and `./harness/bin/run-joern.sh` exited **78** without loading anything, and after §7 was re-anchored it exits **0** with `VERDICT: PASS` at `joern-preflight.log` line 96 | **CONTROL RESOLVED**; the contradiction is a provisioning act — owned by **D4** |
| **SEC-03** HIGH — a path variable is interpolated into `python3 -c` source | `run-trivy.sh` lines 52–53 build a Python program by string interpolation of `$TRIVY_CACHE_DIR`, so a hostile value executes as code | **Reproduced first-hand, then refused.** The finding was confirmed rather than assumed: a crafted value was fed to the runner's verbatim line 52 and it **exited 0 and wrote a marker file**, proving arbitrary execution. The gate then refused the same value at exit **77**, naming single-quote U+0027@75, close-paren U+0029@76, semicolon U+003B@78 and open-paren U+0028@83, length 143, sha256 `3c64c93cfeea19b729bbeb6233c31d17b658e3c21a9e82bd299be98a7702e170`, with **0 occurrences of the hostile substring** anywhere in its own output. Through the wrapper: hostile value → **77** with **0** dispatch banners; `HARNESS_GATED_DISPATCH_DRY_RUN=1` → **0**, "the gate ran and passed; no scanner was invoked"; an unknown tool name → **64**; no arguments → **64** | `sec-gate-scan-target-cases.log`, `sec-gate-scanner-gated-cases.log` | **CONTROL RESOLVED**; root cause is REFERENCE-only and **still open in the runner**, re-confirmed present on 2026-09-03 at `harness/bin/run-trivy.sh` lines 52-53. **D29** publishes the one-line patch, the reachability and the reason it is recorded rather than repaired |
| **SEC-04** HIGH — artifact-controlled text is persisted in plaintext to stderr and to a 0644 record | an unknown-artifact halt echoed the offending value and the artifact's top-level key names into the diagnostic, into `UnknownArtifactShape.details()` and thence into `normalize-run.json` at mode 0644 | **Fixed and verified.** A sandbox artifact carrying two distinct marker strings — one as the `version` value, one as a top-level key — was normalized: exit **1**, `halt reason: unknown-artifact-shape`, and the marker count is **0 in stderr, 0 in stdout and 0 in the persisted 0644 run record**. What replaces the plaintext is stronger evidence, not less: `version` now reads `a str of length 34 (sha256 edb3ecee…; <redacted-artifact-text>)`, and the persisted `version_evidence` carries `value_type: str`, `context: version`, `character_length: 34`, the full sha256 `edb3ecee0f17245974777c66b184d7cc922bb4bf7a387228a8d67784a1cfcb0c`, control-escape and userinfo-redaction counts, `publishable: false` and `redacted: true` — every figure here taken from the evidence file this row cites rather than from any earlier run. Text publishes verbatim only when it is byte-equal to a literal the code itself authors, so the well-known keys `version` and `runs` still appear as themselves | `reverification-sec-04-diagnostic-secrecy.log` and the adapter suite's own assertions; mutation proof: forcing the publishable predicate to always-true fails **nine** tests | **RESOLVED** |
| **SEC-05** HIGH — the interpreter hosting the pipeline carries unresolved advisories | `/usr/bin/python3.13` is distribution build `3.13.7-1ubuntu0.4` on an end-of-life series, with 23 advisories open against it | **Measured and corroborated from two publishers, not repairable here.** Interpreter, distribution build, host series, bundled expat 2.7.1 and both scanner virtual environments' base interpreter all re-measured here. OSV returns exactly **23** advisories against `python3.13 3.13.7-1ubuntu0.4` in `Ubuntu:25.10`, and **not one fixed-version entry across all 23 names any 3.13.x build**. The distribution's own API, queried per advisory, reports status **`ignored` for all 23** with **23 of 23** status descriptions beginning "end of life" and priority `medium` throughout — so this is the distribution's published position, not an unapplied patch | `reverification-sec-toolchain-advisories.json`, `reverification-sec-toolchain-advisories.log` | **RECORDED, NOT REPAIRED** — owned by **D16** |
| **SEC-06** MEDIUM — malformed percent-escapes are emitted as paths | `%`, `%2` and `%GG` parsed as ordinary relative references, so a truncated or invalid escape reached the dataset's `path` field unvalidated | **Fixed and verified.** The same matrix now returns `form=invalid`: `src/main/a%.scala` at index 10, `a%2.scala`, `a%GG.scala`, `jar:core/lib.jar!/a%GG.class` at index 19, `file:///opt/x/a%2.scala` at index 15, and `%SRCROOT%` with 2 malformed escapes. Behaviour that had to stay put did: `%00` keeps its control-character detail, `%20` still decodes to a space, `a%2Fb` to `a/b`, `%25` to `%`. Six guard sites carry it, the reporter masks well-formed triplets so reported indices stay original-relative, and it never reproduces the offending characters. **Dataset unmoved** — the committed `findings.json` carries **0** paths containing `%`, and both output files re-normalize byte-identically | `reverification-sec-06-percent-validation.log`; new fixture `reject-sarif-malformed-percent-escape.sarif` (16,198 bytes) with its hand-verified expectation; mutation proof: neutering the describer fails **twenty-one** tests | **RESOLVED** |
| **SEC-07** MEDIUM — the build JDK is behind a published respin | Temurin 17.0.20+8 is missing the quarterly respin, fixed in 17.0.20.1+1 | **Measured and corroborated from the vendor, not repairable here.** The decisive fact was measured here rather than taken from the finding: the build JDK reports `openjdk version "17.0.20" 2026-07-21` while **this same harness's Joern JDK reports `21.0.12.1` 2026-08-18 LTS — an August `.1` respin**. So the harness already runs a respun JDK on one stream and the un-respun original on the other. The Adoptium API returns GA release names `['jdk-17.0.20.1+1', 'jdk-17.0.20+8']` in `[17.0.20,18)` at HTTP 200, confirming the remediation target exists and is published | `reverification-sec-toolchain-advisories.json`, `reverification-sec-toolchain-advisories.log` | **RECORDED, NOT REPAIRED** |
| **SEC-08** MEDIUM — `asteval` in the Checkov environment is below its advisories' fixed version | 1.0.6 installed against advisories naming 1.0.9 as fixed | **Measured and corroborated, not repairable here.** Version read from the distribution's own `dist-info`; OSV returns **2** advisories against it, both naming the same fixed version, so the target is unambiguous. Checkov's construction of the evaluator was measured rather than assumed — `use_numpy=False, minimal=True` — and is recorded as an observed configuration, explicitly **not** as an adjudication that the advisories do not apply, because judging a finding is outside this run's remit | `reverification-sec-toolchain-advisories.json`, `reverification-sec-toolchain-advisories.log` | **RECORDED, NOT REPAIRED** |
| **SEC-09** LOW — `ecdsa` in the Checkov environment carries an advisory with no fix | 0.19.2 installed, advisory open, no fixed version published | **Measured and corroborated, not repairable here, and not repairable anywhere today.** OSV returns **2** records against the installed version and publishes **no fixed version** for either. That is the whole disposition: the prohibition on installing is not the binding constraint here, because there is no upgrade for any authority to apply | `reverification-sec-toolchain-advisories.json`, `reverification-sec-toolchain-advisories.log` | **RECORDED, NOT REPAIRED** |
| **SEC-10** MEDIUM — an unproven heap raise is accepted | the wrapper accepted 129 g and 1,000,000 g without proving either committable, against AAP §0.8.2's requirement that any raise above the 64 g default be proven with `-XX:+AlwaysPreTouch` | **Fixed and verified against all six of the finding's own values,** with real `java` invocations counted by a logging shim: `abc` → **78** / 0 invocations; `63` → **78** / 0 ("refusing a 63g heap" — the floor, since lowering is forbidden); `64` → **0** / 0 (the default needs no proof); `128` → **0** / **1 real pre-touch proof**; `129` → **78** with no new invocation; `1000000` → **78**, refused on digit count *before* any reservation is attempted. A raise whose proof is unavailable is refused rather than assumed: with the JDK made unavailable, 100 g → **78** | `sec-gate-joern-heap.log`, `sec-gate-joern-heap-boundaries.log`, `sec-gate-joern-gated-wrapper.log` | **RESOLVED** |

**Why the four toolchain findings are recorded rather than fixed, in one sentence.** AAP §0.4.3
and §0.8.1 prohibit installing, upgrading or substituting any tool, and the environment runbook
§12 places provisioning outside this run's authority — so a vulnerable component is
halt-and-report by construction, and this register plus its two evidence files *is* the report.
That is not a softer outcome than a fix: D16 previously withdrew an advisory claim about this
very interpreter because it had no owner file anywhere in this tree, and its "what a human must
do" asked for exactly what now exists — the claim supplied from a dated advisory source with
that source recorded.

**What a human has to decide, finding by finding.** SEC-01 and SEC-03: make
`harness/lib/run-scanner-gated.sh` the documented entry point for all nine runners, or fix the
two root causes at provisioning — `scope_resolve_target` should compare HEAD and refuse a set
smoke override, and `run-trivy.sh` line 52 should pass the path as `sys.argv[1]` to a fixed
program instead of interpolating it into source. Until one of those happens, a **direct**
runner invocation is still exactly as provisioned, hostile value and all. SEC-02: **nothing further is owed** — the
record and the disk agree, re-anchored on 2026-09-03 to 547,980,224 /
`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` after the re-provisioning
replaced the graph, and the gate that measures their agreement exits **0** with
`VERDICT: PASS`. What that clears is the deliberate consequence this row used to record: gated
Joern invocations no longer halt at their graph step. The standing advice survives as advice
for the **next** provisioning — replace the graph and the record in one act, because
`2026-09-03T01:17:07Z` replaced the graph alone and every figure taken over its predecessor had
to be re-established from scratch (**D4**). SEC-05: either re-provision onto a serviced
interpreter and re-measure every count this dataset publishes, or accept the current one in
writing with the 23 advisories attached. SEC-07: swap in `jdk-17.0.20.1+1` **and** update the
AAP §0.4.1 row together, then rebuild the reactor and the graph — a JDK change changes the
bytecode the frontend reads, so a half-applied swap would manufacture exactly the
record-versus-disk contradiction SEC-02 exists to catch. SEC-08: upgrade `asteval` inside the
Checkov environment while holding `checkov` at the pinned 3.3.12, then re-run that runner and
re-normalize, since its row count is part of the published dataset. SEC-09: accept it in
writing and re-check at the next provisioning; there is nothing to install today.

**One interaction between two of these findings, recorded because it changed how SEC-10 could
be exercised — and because the interaction has since lapsed.** `run-joern-gated.sh` runs its
four steps in order — scan-target gate, graph gate, heap validation, runner — so while
SEC-02's contradiction stood the wrapper **halted at step 2 of 4 and never reached step 3**.
Re-executed on 2026-09-02, that is exactly what it did: exit **77** after printing `step 1/4`
and `step 2/4` and nothing further. **That halt no longer stands**: the gate step 2 calls
exits **0** with `VERDICT: PASS` against the re-anchored record, so nothing blocks step 3.
What is *not* re-established is the wrapper's own end-to-end behaviour past that step — no
tool of any kind was invoked in the 2026-09-03 checkpoint, so the wrapper was not re-run, and
[§14](#14-values-that-could-not-be-established) carries that rather than asserting it.
SEC-10's control was
therefore verified by driving its own marked block (`# ---- heap-validation:begin ----` to
`:end ----`, 122 lines, extracted verbatim) with a logging `java` shim on the path, rather than
end-to-end through the wrapper. All six of the finding's own values behave as required:
`HARNESS_JOERN_JVM_HEAP_G` of `abc` → **78** with **0** JVM invocations, `63` → **78**
("refusing a 63g heap" — the floor, since AAP §0.8.2 forbids lowering), `64` → **0** with **0**
invocations because the default needs no proof, `128` → **0** with **exactly one** real
`-Xms128g -Xmx128g -XX:+AlwaysPreTouch` reservation, `129` → **78** with no new invocation, and
`1000000` → **78** refused on digit count *before* any reservation is attempted. With SEC-02's
contradiction cleared the wrapper reaches step 3 and the same validation applies end-to-end;
nothing about the ordering weakens it, and a reader testing the wrapper today will get past the
graph step rather than halting at it.

**Three residuals this register records rather than hides.** First, **every gate is
caller-side**: it binds the route through `run-scanner-gated.sh` and cannot bind a direct
`./harness/bin/run-<tool>.sh`, because binding that would require editing a runner. Second, the
normalizer's **rejection** details still quote a correctly-parsed coordinate verbatim — a Joern
class name, a resolved path, a rule id — because AAP §0.5.4 requires the parser's reason be
retained and 72 hand-verified expected files assert those exact strings; the redaction policy
covers the halt and detection channel, where artifact-controlled text actually arrives, and
that scoping is deliberate rather than incidental. Third, `rejections[].record_identity.uri`
still carries the raw reference as the record's locator, at **0** occurrences in any `detail`
and one per rejection in `record_identity`, which is asserted by name in the new expected file.

---
### QA testing findings of 2026-09-03 — the nine this document answers, and where each is answered

A third testing run was executed against this checkpoint on **2026-09-03** and raised
**twenty-seven** findings. Nine of them are answered by this document, and this subsection is
their register. It is deliberately a **map rather than a second account**: each row names the
finding, the disposition, and the entry that carries the measurement — because AAP §0.6.4 makes
this file the index and forbids it substituting for the content another document or register
row owns. Where a row says *record*, that is the whole of the action: **an accurate published
record is the disposition, not a claim of repair.**

Two properties of this checkpoint bound every row below, and both are stated once here rather
than repeated. It performed **measurement and publication only**: **no tool of any kind was
invoked** — no runner, no normalizer, no `joern`, no `jimple2cpg`, no `java`, no scanner — so
`harness/artifacts/raw/` is unchanged member for member and no dataset row was produced. And
where a finding's root cause lies in a file AAP §0.6.1 marks REFERENCE, the fix is **published
as a provisioning patch with its reachability** rather than applied, which is §0.3.2 read as
written.

| Finding | Severity | Disposition | Where it is answered |
| --- | --- | --- | --- |
| **1** — the gate's verdict was `halt` authorising `nothing`, yet Stages 2 through 5 ran | CRITICAL | **Reported; not repairable retroactively.** The gate censused both artifact trees as non-empty — `raw/` **8** entries, `logs/` **85** — which AAP §0.8.1 and §0.9.2 make an unconditional halt, and it published `overall: halt` with `authorises: "nothing"`. Later stages ran anyway. **An execution cannot be un-run**, and no action available to any clone retroactively supplies the precondition; this run may neither create nor clear either tree. Re-censused on 2026-09-03 the condition is present again and larger — `raw/` 8 entries, `logs/` **129** top-level entries over **163** files. Separately, `gate-record.json`'s two `status_now: "resolved"` claims were **withdrawn** on 2026-09-03, their literal retained as `status_now_superseded_value`, because the `2026-09-03T01:17:07Z` re-provisioning falsified them | [§1](#1-gate-verdicts), which carries the verdict, the re-census and this run's own two gates; **D0**; and [§18](#18-where-the-run-reached), whose stage table records every stage as work done after an unmet precondition |
| **2** — the environment file creates both artifact trees on every source, so the mandated missing-tree halt cannot fire | MEDIUM | **Declined in-clone on AAP grounds; patch and mitigation published.** `harness/env.sh` line 91 creates both trees before any runner reaches `scope.sh`'s `-d` guards, and swallows its own failure with `\|\| true`. The file is provisioned surface, REFERENCE under four independent authorities | **D30**, which carries the patch (drop the two trees and the `\|\| true`), the four authorities, and the census-not-refuse mitigation with its limit |
| **4** — the graph was not built over every JAR the build produced | CRITICAL | **UNMET, and unmeetable at this pin; published as a divergence.** This run's frontend over the complete **191**-archive set terminated at a fixed array-length bound inside flatgraph's writer and wrote **no accepted graph**; the mechanism was re-verified at 8 GiB, 64 GiB and 128 GiB and does not move by one byte. The graph the runners load is **provisioning's**, over **62** archives from **31** modules, and it **may not be rebuilt here** — `/opt/blitzy-harness/cpg/**` is host-shared read-only across up to 64 clones. **Build completeness is separate and not in doubt**: all **38** JAR-packaging projects produced their own artifact | **D1** and **D3**; [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) and [§6.1](#61-the-frontend-build--first-of-four); `cpg-frontend.log` STEP 8 and STEP 10, `cpg-ceiling-reverify.log`; the build verdict and the `jar-packaging WITHOUT one : []` census are owned by `oss-scan-results/build-record.md` |
| **5** — the graph's recorded identity was stale across the published documents | CRITICAL | **RESOLVED at the record, twice, with the graph untouched.** The two records of account — `harness/ENVIRONMENT.md` §7 and `harness/artifacts/MANIFEST.json`'s `.cpg` entry — were re-anchored to the graph on disk, and then every publication document was re-anchored to them. **No graph was written, replaced, moved or re-pointed to make a record agree**: the bytes were read and hashed only, nothing was written under `/opt/blitzy-harness`, and both corrections are auditable in that order. This document is one of the documents re-anchored and the index for the rest | **D4**, which carries the adjudication, the two generations and the PASS → HALT → PASS transition; [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) for the counts and both identity generations; **F2** and **SEC-02** for the earlier readings |
| **10** — `run-trivy.sh` interpolates a path variable into `python3 -c` source (CWE-94) | HIGH | **Declined in-clone; patch, reachability and mitigation limit published.** Lines 52-53 splice `$TRIVY_CACHE_DIR` into program text. Reproduced first-hand by the security pass — a crafted value exited 0 and wrote a marker — and refused by the gated wrapper at exit 77. The direct route is exactly as provisioned | **D29**, which carries the `sys.argv[1]` patch, the reachability measurement and the statement that this run's own Trivy invocation was direct; **SEC-03** for the first-hand reproduction |
| **12** — nothing in `harness/bin/` invokes `preflight_scan_target.py` | MEDIUM | **Mitigated outside the provisioned files; root cause recorded.** This run invokes the check itself, outside every runner and before Stage 3 — exit **0**, `VERDICT: PASS`, with `[PASS] smoke-override-absent` and both artifact-tree censuses. Its `check_artifact_trees()` and the scan-target path-text checks are this checkpoint's addition. The check **deliberately does not refuse a non-empty tree**, because refusing would require this run to create or clear one, which AAP §0.8.1 forbids | **D28**, which carries the gate's outcome, the check's location and the census-not-refuse reasoning; [§1](#1-gate-verdicts) for both gates' verdicts; **SEC-01** for the control's own cases |
| **13** — `harness/lib/scope.sh` evaluates a caller-supplied variable name through `eval` | LOW | **Recorded, patched on paper, not repaired.** Line 106 hands its first argument to the shell's evaluator. Reachability measured: **nine literal call sites**, each passing a hard-coded credential variable name, so no caller-controlled text reaches it as provisioned — which is why it is LOW, and it is one call site away from being reachable | **D31**, which carries the indirect-expansion patch and the reachability measurement |
| **18** — the environment file assigns two variables unconditionally, destroying its own documented override | MEDIUM | **Declined in-clone on AAP grounds; patch and mitigation published.** `harness/env.sh` assigns `HARNESS_DIR` at its line 19 and `HARNESS_REPO_ROOT` at its line 21, and both discard a pre-set value while the file's other twenty-seven exports use `${VAR:-default}` and its own header promises that "values already present in the environment win". Both preflights document an override this pair destroys. The mitigation is that the one condition the override would have been used to check is verified directly: `[PASS] smoke-override-absent` confirms `HARNESS_SMOKE_TARGET` is unset | **D23**, extended for the `HARNESS_DIR` half, the two preflights' override and the four authorities that forbid the edit; **D30** for the same file's other declined patch |
| **19** — AAP §0.9.2's method-count floor was unimplemented | — | **RESOLVED BY THIS RUN.** `harness/lib/preflight_graph_identity.py` declares `METHOD_COUNT_FLOOR` at its line 331 — absent from that file's HEAD blob — and adjudicates the count in the same `--check-only` pass as the identity: `Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420`. **One-sided** per AAP §0.9.3: below the floor halts, at or above it is recorded, and the count exceeding the 898,336 anchor by +500,628 is recorded and never halted | **D27**; [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor); `harness/artifacts/logs/joern-preflight.log` lines 88-89 |

**What a human must do, finding by finding, in one place.** Findings **2**, **10**, **13** and
**18** each need one small patch applied to provisioned surface — this register publishes all
four verbatim and none of them is applied here. Finding **1** needs a provisioning whose two
artifact trees are empty *before* the run begins; nothing else clears it, and the stages already
executed cannot be re-authorised by anything. Finding **4** needs either a frontend whose writer
does not serialize the whole string pool through one array, or a decision to accept the narrower
input set in writing — and it needs write authority over `/opt/blitzy-harness/cpg/**`, which no
clone has. Finding **12** needs the gated wrapper made the documented entry point, or the check
applied inside `scope_resolve_target`. Findings **5** and **19** need nothing: both are resolved,
and the evidence for each is a command with a published verdict rather than an assertion.

**One interaction across this set, recorded because it is easy to miss.** Findings 2, 10, 13 and
18 all have their root cause in a file this run may not edit, and every mitigation offered for
them is **caller-side** — it binds the invocation that carries it and leaves the direct
`./harness/bin/run-<tool>.sh` route exactly as provisioned. That is not a weakness of the
mitigations; it is what AAP §0.8.1 requires, since binding the direct route would mean editing a
runner. **D25** records what happened the one time this run's lineage edited runners anyway, and
**D26** records the two controls that had to be moved back outside the runner as a result.

---



## 14. Values that could not be established

Named rather than omitted, because a value missing from the record is a value
nothing downstream can check (AAP §0.9.4). Each is owned by the document that
tried to establish it; this section indexes them.

| Value | Named in |
| --- | --- |
| The **cause** of the graph's above-anchor counts — measured composition is reported, a cause is not guessed | `cpg-verify.log` PHASE 2 and D3 there; `build-record.md` §7 |
| **Per-class provenance** for every overwritten class, and therefore any winner map | `cpg-frontend.log` STEP 11; `build-record.md` §5 |
| **Per-entry identity for the 191-archive input set** this run's own frontend was given — staged name, byte size and sha256 per archive. The **aggregate** is established (191 archives, 431,184,822 bytes) and its owners exist, but the per-entry manifest that carried it was regenerated for this generation to describe the 62-archive set instead, and no other file in the two trees holds it. Nothing is estimated in its place and no per-entry figure for that set is stated anywhere in this record | `build-reactor.log` STEP 13 and `cpg-frontend.log` STEP 1 for the aggregate; **D9** here for the loss |
| A **coverage verdict for the 7 reactor JAR projects absent from this graph's input set** — `sql/connect/shims`, `tools`, `examples` and the four `connector/kafka-0-10*` projects. No witness for them can be queried in a graph their bytecode is not in, and no graph over the complete set exists to verdict them against (**D1**). **Not partially closed and nothing substitutes for it**: no narrowed or witness graph is presented as a stand-in | `build-record.md` §6 |
| An **injective coverage witness** for the **5** modules whose every primary-artifact class is vendored into another module's shaded archive — `common/network-common`, `common/network-shuffle`, `common/utils-java`, `sql/api`, `sql/connect/common`. Their weaker `pom.properties` witness is vendored too, so each is **NO VERDICT OBTAINABLE**; presence was **not** substituted, and **0** verdicts in this run rest on presence or on a shared prefix | `build-record.md` §6 |
| An injective coverage witness for the **8** modules that have none over the input set the plan requires — the five above plus `common/tags`, `connector/kafka-0-10` and `connector/kafka-0-10-token-provider`. Measured on 2026-09-02 over all **191** project outputs rather than the 62 the graph carries: **30 of 38** modules have an accepted witness and eight have neither kind, because each one's primary artifact *and* its Maven descriptor are wholly vendored by another module's shaded archive. So the shortfall is a property of the rule against Spark's shaded artifacts rather than of the narrowed graph, and it would persist in a graph over the complete set. `common/tags` has a witness over the 62 and none over the 191, which is why the two measurements are published as two | `build-record.md` §6; `reverification-f4-module-witness-full-input-set.json` and `.log` |
| **The graph as this run's own output** — attempted over the complete input set and **blocked** by a fixed `Integer.MAX_VALUE - 8` array-length bound in flatgraph's string-pool writer; not satisfied, and not satisfiable with the pinned frontend at this input breadth | `cpg-frontend.log` STEP 8 and STEP 10; D1 here |
| **A current-run method, type-declaration or file count** — no current-run graph exists to load, so none was measured and none is estimated from the provisioned graph's | `cpg-frontend.log` STEP 12; D1 here |
| **Which input breadth the pinned frontend can serialize** — the failure establishes an upper limit lies at or below this run's 191-artifact set, and the provisioning invocation establishes 62 archives is below it; the boundary between them was **not searched for**, because narrowing the set to find it is the trimming AAP §0.9.2 prohibits. What *is* established, re-measured on 2026-09-02 at `-Xmx8g`, `-Xmx64g` and `-Xmx128g` in `reverification-f3-writer-bound.txt`, is that the bound does not move with the heap across a 16× range, so no heap value is the boundary either. A **superseded** attempt did narrow it, to 189 archives / 308,385,184 bytes, and that attempt is registered as the halt-class departure **D20** rather than used as a data point — no outcome of it is stated here, and whether 189 serializes is likewise **not established** | D1 and **D20** here |
| `gitleaks`' rule count and ruleset digest; `checkov`'s policy count and policy digest — none separately versioned, none reported by its tool, none invented | `tool-status.md` |
| `joern`'s path-base **value** — the base *kind* and the resolution route are recorded; no plausible path was invented | `tool-status.md`; `runner-metadata.json` |
| The native severity vocabulary `osv-scanner` would have used — no record arrived, and none is invented. **`dependency-check`'s literals are no longer in this class**: a second capture of the same tool build, over input that resolves to packages the seeded feed carries advisories for, yields three observed literals — `CRITICAL`, `HIGH`, `MEDIUM` — mapping to Critical, High and Medium with the CVSS scores present and deliberately not consulted. It contributes **zero rows to this dataset** either way | `severity-map.md`; `harness/artifacts/logs/dependency-check-positive-capture.{json,log}` |
| The behaviour of the `cvss_score` basis and the `unmapped_literal` disclosure on this run's own artifacts — each exercised 0 times, established against committed fixtures instead | `severity-map.md` |
| Probe query 02's **engine-internal** call-depth bound, `MAX_FLOW_CALL_DEPTH` = 6 — whether the engine expanded to it is not observable from its output, so the query publishes `bound_reached: false` against **the caps its own evaluator counts** and names that convention rather than claiming the engine's | `joern-probe.md`; `queries/joern/results/02-dataflow-unguarded-driver-launch.json` fields `bound_reached_basis` and `observable_bound_reached_convention` |
| The four further caps query 02 declares — `MAX_FLOW_CALL_DEPTH_SHALLOW` 2, `MAX_BOUNDARY_FLOW_CALL_DEPTH` 2, `MAX_FLOW_LENGTH` 64, `MAX_FLOWS_PER_PAIR` 8 — are published with the bound; which of them the engine reached internally is likewise not observable | `joern-probe.md` |

**One value this section carried is no longer unestablished, and is removed rather than
left standing.** Earlier editions listed `semgrep`'s `started_at` / `finished_at` here,
citing `tool-status.md`'s own unestablished-values table and a **621-second** window
length. Both halves are superseded: the delivered lane records the pair for every one of
the nine, `semgrep`'s being **2026-09-01T14:13:07Z to 14:22:02Z** — a **535.569-second**
window — owned by `harness/artifacts/logs/runner-sequence.json` `invocations[2]` and
corroborated by that runner's own console stream at
`harness/artifacts/logs/semgrep.runner-console.log`; and `tool-status.md`, which owns the
per-tool contract, no longer carries the row this section pointed at. The window is cited
here from its owner rather than re-measured, and the 621-second figure belongs to a
superseded generation.

**Six of this file's own, added here.** Two are long-standing and four were established as
gaps by the 2026-09-03 re-execution, which is the point of listing them: a re-execution that
re-establishes some figures and not others leaves a set of *newly* unestablished values, and
naming them is what stops a reader assuming the whole generation was re-measured to the same
depth.

- **The contents of the run-created scratch locations** — the frontend staging
  directory and the `importCpg` verification workspace — cannot be re-hashed from
  this record, the workspace having been created inside this clone's private scratch
  directory rather than in the checkout. What survives is the staging directory's
  complete ordered manifest inside `cpg-input-inventory.json` and the workspace's
  name, its absence-before-use proof and its size inside `cpg-verify.log`
  — [§11](#11-deliverable-inventory-with-resolved-absolute-paths).
- **Which bytes a future load will read** is not determined by this run, and this is
  the one entry in this section that has already been *demonstrated* rather than
  merely asserted. The resolved path is host-global and shared read-only with
  concurrent clones, and this run neither wrote nor replaced it, so what this record
  fixes is the one identity every load of the generation on record verified against and
  read — **547,980,224 / `325887cf…3dc6`**. The superseded generation of 2026-09-01
  fixed **541,309,809 / `4616845a…4730c7`** in exactly the same way and with exactly the
  same care, and those bytes are gone: the host was re-provisioned at
  `2026-09-03T01:17:07Z` and the graph rebuilt beneath the record (**D4**). So this row
  predicts nothing about the next load, and the evidence that it cannot is that the last
  prediction of this kind was falsified — which is why the Stage 3 gate re-measures
  rather than trusting this file.
- **No member of `logs/` captured the 2026-09-03 Stage 3 runner's own console stream.**
  The runner prints its input's identity at its lines 56-58, and for the invocation of
  record that print was not redirected into the log tree: `joern.stdout.log` (13,939
  bytes) holds the **engine's** stream only — its first line is
  `loading CPG with importCpg: /opt/blitzy-harness/cpg/spark.cpg` — and
  `joern.runner-console.log` is unchanged at 1,700 bytes / sha256 `47a9d744…`, which is
  the **2026-09-01** capture. The pair the 2026-09-03 runner printed is therefore
  recorded only in `harness/artifacts/logs/runner-metadata.json`, field
  `tools.joern.stage3_invocation_2026_09_03.graph_identity_the_runner_printed`, and no
  verbatim console line for it exists in either tree. Nothing is reconstructed in its
  place. What *is* independently established for that invocation is the identity itself,
  by the gate that ran one second before it and by three later `importCpg` loads, all
  agreeing on the same pair.
- **The start instant of each of the three `importCpg` verification loads.**
  `cpg-verify.log` PART 3 stamps the sequence `2026-09-03T09:36:13Z`, gives each load's
  import elapsed (526,605 / 557,041 / 545,644 ms) and its own workspace
  (`/tmp/blitzy-harness-scratch/424/wave2-{joern,detail,corrob}`), but records **no
  per-load start instant**. Two consequences are stated rather than smoothed: only the
  first load's position in [§18](#18-where-the-run-reached)'s ledger is established from
  the record, and the serialization of loads 2 and 3 against the three probe loads —
  which ran `09:45:34Z → 10:16:53Z` — **cannot be confirmed from what was retained**.
  No instant is inferred from the elapsed times, and nothing here asserts that AAP
  §0.8.2's one-64-GiB-JVM-at-a-time property was violated; what is recorded is that the
  evidence does not settle it.
- **Byte-identical repetition of the dataset by a second normalizer pass over the raw
  tree as it now stands.** It was established within the superseded generation, where two
  runs over one unchanged tree wrote the same sizes and digests. For the generation on
  record no second pass exists: **no tool of any kind was invoked in the 2026-09-03
  publication checkpoint**, by design, so the property is carried here rather than
  restated from a run that measured a different input ([§9](#9-normalization-and-the-dataset)).
- **`harness/lib/run-joern-gated.sh`'s end-to-end behaviour past its step 2.** With the
  graph-identity contradiction cleared the gate that step calls exits **0**, so the
  step-2 halt that the security register recorded no longer stands — but the wrapper
  itself was not re-executed in the 2026-09-03 checkpoint, so its steps 3 and 4 are
  established only by the marked-block extraction the security register describes and
  not end-to-end. Stated rather than presented as an end-to-end pass.

---

## 15. The October 2025 caveat

**The pinned tree dates from October 2025** — commit
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, **Thu Oct 23 2025** ([§2](#2-the-pinned-tree)).

**Every dependency-oriented count in this dataset must be read against that date
rather than against the present.** The three dependency-oriented tools resolved
their answers from feeds dated **2026-08-30** (Trivy's two databases and
Dependency-Check's NVD datafeed, D7) or queried a live API at scan time
(OSV-Scanner, which keeps no local database), while the code they were pointed at
is over ten months older than those feeds. A count of zero, or any count at all, is a
statement about that tree on that date evaluated against data of a later date, and
it is not a statement about the present state of any dependency.

One structural fact of the pinned scope compounds it and is recorded rather than
fixed: exactly one manifest-shaped file lies inside the twelve globs —
`core/src/main/resources/org/apache/spark/ui/static/package.json`, five lines
carrying a name, a license and `"type": "module"`, with **no dependencies block and
no lockfile beside it**. Nothing in scope resolves to a package. The allowlist was
**not** widened to give those tools something to resolve: doing so would answer a
scope question that is not this run's to answer and would silently change what
every count means (AAP §0.3.2). The per-tool consequence is recorded in
`oss-scan-results/tool-status.md`, which has a section of its own for it.

---

## 16. Manifest of the two git-ignored artifact trees

**Both trees are excluded from git's ordinary collection.** `.gitignore` line 31 is
`artifacts/`, which matches a directory of that name at any depth, and
`git check-ignore -v --no-index` confirms the rule reaches them: it reports
`.gitignore:31:artifacts/` for `harness/artifacts/logs/gate-record.json`,
`harness/artifacts/raw/trivy.json` and the path form `harness/artifacts/cpg-input`, and
reports **nothing** for `oss-scan-results/run-record.md`,
`queries/joern/results/01-callgraph-unguarded-driver-launch.json` or
`harness/cpg/spark.cpg` — so the result documents, the probe tree and the graph symlink
are collected normally while these two trees are not. Publication is therefore **by this
manifest, carrying each file's byte size and sha256**.

Every figure below was computed from the file on disk when this record was written, and
the machine-readable form — `harness/artifacts/MANIFEST.json` — is regenerated from the
same measurement and verified against the filesystem entry by entry, by byte size **and**
by sha256, with **0** mismatches, **0** files on disk it does not list and **0** entries
naming a file that is not there. **That manifest's own byte size is deliberately not
quoted here.** It is regenerated at the end of every publication checkpoint, after the
documents it measures have stopped moving, so any literal this section printed for it
would be stale by construction — an earlier edition printed **46,214** and the file had
already grown past it. `harness/lib/verify_publication_owners.py` compares the manifest's
summed per-file byte totals against the two headings below, which is the check that keeps
section and manifest in step without either transcribing the other's size.

**One volatility this section states rather than conceals.** The `logs/` tree was still
being written by other workers of this same checkpoint at the moment these figures were
taken: `runner-metadata.json` and `runner-sequence.json` are owned by a concurrent lane and
were under revision. The two headings and the two tables below are one fresh measurement of
the tree as this document was finalised — each heading equal to the sum of the table beneath
it, `raw/` **8 rows summing to 120,536,620** and `logs/` **163 rows summing to
144,469,114** — and the manifest is regenerated from the tree **after** every worker has
finished. **So if the two disagree, the manifest is the later measurement and the remedy is to
re-render these two headings from it**, which is a two-literal edit and not a re-measurement:
`harness/lib/verify_publication_owners.py`'s `check_manifest_totals` reads exactly the
`### `harness/artifacts/<tree>/` — N files, B bytes` heading of each and compares it with the
manifest's own summed per-file totals. At the moment of writing the committed
`harness/artifacts/MANIFEST.json` still carries the pre-checkpoint totals **120,538,389** and
**144,246,813**, so that check reports two disagreements until the manifest is regenerated;
the disagreeing side is the manifest, and the figures here are the ones measured from the
files themselves.

**The `reverification-*` and `sec-gate-*` members are the remediation passes' own evidence**,
added to `logs/` on 2026-09-02 as each testing run's findings were re-executed first-hand in
this clone. They are why the tree has grown twice: **137 → 151** when the fourteen
`reverification-*` files recorded the re-execution of the five QA findings F1–F5, and
**151 → 163** when the security findings SEC-01–SEC-10 added eight `sec-gate-*` files for the
fail-closed pre-scan gates and their negative cases, plus four more `reverification-sec-*`
files for the diagnostic-secrecy and percent-escape re-verifications and the toolchain
advisory register. **A third change moved the byte total without moving the count**: the
`w424` lane of 2026-09-03 rewrote members in place rather than adding any — `joern.status`,
`joern.stdout.log`, `joern.stderr.log`, `joern-preflight.log`, `sec-gate-scan-target.{json,log}`,
`normalize-run.json`, `findings-publication.json`, `cpg-verify.log`, `cpg-identity.txt`,
`gate-record.json`, `cpg-frontend-input-manifest.json`, `cpg-module-coverage.json`,
`cpg-input-inventory.json`, the six `probe-*` members, and the four taint arms it re-measured
— so the file count stayed at **163** while the total grew. Each set is what its register in
[§13](#13-divergence-register) cites finding by finding. Adding them is `logs/` doing what AAP
§0.8.1 says it does — accumulate this run's own durable evidence — and **both growths leave
`raw/` at the same eight runner artifacts, byte for byte**, which is the property that
distinguishes evidence-gathering from scanning.

**The three status records AAP §0.9.4 names explicitly are in the manifest:**
`gate-record.json`, `normalize-run.json` and `adapter-tests-run.json`.

**Three members were deleted from this tree and have been restored, and the deletion is why an
earlier revision of this section disagreed with the disk.** `datadog-static-analyzer.console.log`
(1,117 bytes), `joern.preflight.log` (16,443 bytes) and `joern.runner.console.log` (1,428 bytes) were
removed by commit `232d0d9cca3` — **the same commit that deleted the thirteen provisioned harness
files**, which is the CRITICAL defect **D13** records. All three were restored byte-for-byte from
`232d0d9cca3^`, verified against that commit's blob hashes, and they are listed among the entries
below. An earlier revision of this section listed 133 entries while the tree held 137: it was written
before those three were restored and before `cpg-graph-record.log` was written. The tables below are a
single fresh measurement of the tree as it now stands, so section and disk agree member for member.

They are **not** mis-transcriptions of the three similarly-named files that also appear below.
`datadog-static-analyzer.runner-console.log` (1,424 bytes), `joern-preflight.log` (2,625 bytes) and
`joern.runner-console.log` (1,700 bytes) did **not** exist at `232d0d9cca3^` and the restored three
did; the two sets are six distinct files with six distinct sizes, written by two different lanes, and
git history shows the pairs never coexisted before now. **D14**'s lane split is what makes both sets
legitimate members of one tree, and it names each cross-lane member individually. The restoration
matters beyond the count: the *enriched* `joern.status` of the generation then current cited
`joern.preflight.log` PART 2 at line 147 with its verdict at line 167, and the restored **321-line**
file carries exactly those lines while the **44-line** file of the similar name never could. That
citation is now historical — commit `0e3e742a5ad` replaced all nine statuses with the runners'
verbatim seven-line `scope_finish` trailer, which references no evidence file at all — so the
restoration is what keeps a **superseded** citation checkable rather than what keeps a live one
resolvable. One property of the restored file travels with every reference to it: it is the
**2026-08-24 lane's** report and states that lane's superseded identity pair, which is why
[§5](#the-graphs-byte-size-and-sha256-and-the-identity-re-verified-before-every-load) and **D4** cite
the 44-line `joern-preflight.log` for the pair this run loaded.

**The two trees diverge by design after the gate.** `harness/artifacts/raw/` is
**runner-only** — exactly one artifact per tool that wrote one, and nothing else ever;
eight artifacts, no `osv-scanner.json`, and neither taint A/B arm nor any probe output
ever landed in it. `harness/artifacts/logs/` **legitimately accumulates** from Stage 1
onward: it holds the per-tool streams and statuses plus the durable evidence for the
gate, the Maven pre-check, the build, the JAR inventory and staging manifest, the
frontend, the graph verification and its identity record, every taint A/B arm, the
shims collision measurement, the normalizer run, the adapter-test run, and each probe
query with its console stream, its completion manifest and the standalone identity
capture a superseded generation left beside it.

**Every member is tracked on this branch as well as manifested.** Recomputed from
`git ls-files` when this manifest was written: **8 of
8** under `raw/` and **163 of 163**
under `logs/` — an earlier edition stated 151 of 151, which was the count before this
checkpoint's twelve additions — with **0** present-but-untracked and **0** tracked-but-no-longer-present in
either tree. The manifest is required and supplied regardless, since the ignore rule is
what governs ordinary collection; the tracking is what earlier lanes added explicitly and
this generation continued.

### `harness/artifacts/raw/` — 8 files, 120,536,620 bytes

| File | Bytes | sha256 |
| --- | --- | --- |
| `checkov.json` | 8,380 | `91e9cf3cc81e17786af239cba88aa770ae96351a719bd6193ec19962cc238643` |
| `datadog-static-analyzer.sarif` | 5,723,938 | `a71dc70d69fa9d93b84eed180e46b568dea98581e25e5cb3ebd5ae4668465372` |
| `dependency-check.json` | 17,097 | `2861fbf4165b56d1a8f0b6db7a1895f30b452922c7c08521ca00825016097799` |
| `gitleaks.json` | 561 | `12d50cf783bb966c77608cae6f93c50c688e0384e84662041ecfb1b6935d8467` |
| `joern.json` | 353,048 | `f7f5f60e37aacdbf58ca2bf073c0682efeb81e256a516576b12d55aea8edc926` |
| `opengrep.sarif` | 73,768,116 | `740ab140d1224064ce3754470c0a90de66d730febec7fb10073421542b085758` |
| `semgrep.sarif` | 40,661,984 | `7111001f6518803274a80844c2a3d8249edd8f19ba68a771d309fa5d33da03cf` |
| `trivy.json` | 3,496 | `979ad0ffbec3502f62ea0e2cd46fae549aaa5e1b7cc4a0d59153a5c2448766ec` |

### `harness/artifacts/logs/` — 164 files, 144,484,015 bytes

Counted **recursively**. Four of the entries are directories —
`checkov.out`, `dependency-check.out`, `gitleaks.parts`, `trivy.parts` — holding the side artifacts their runners
wrote; their members are listed below by the path relative to `logs/`, so the count and
the byte total cover every file the tree holds rather than only its top level.

| File | Bytes | sha256 |
| --- | --- | --- |
| `adapter-tests-run.json` | 393,682 | `6667510041841ce416675e9f658988bd0a601de81c2e7f86344a9d08cb6f5a7a` |
| `build-reactor.log` | 2,711,993 | `79c1f1a5b1898b86d96ccb6fa1b0383ee6535945c6bb79c4e1574d62662b3f9a` |
| `checkov.out/results_json.json` | 8,380 | `91e9cf3cc81e17786af239cba88aa770ae96351a719bd6193ec19962cc238643` |
| `checkov.runner-console.log` | 1,379 | `1245806c839d4682a392a5483afb24ac536177befb5e8f2b330de72e4f99f18b` |
| `checkov.status` | 242 | `d05f2b35d50da2cd202ccc307857d7a950d9733abe6eb7c5b988f2a6e5924da1` |
| `checkov.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `checkov.stdout.log` | 140,105 | `3c1d72a44cfa7d4c7665373b2375e40c4e16f10f208489bd42d829de58ffa854` |
| `cpg-ast-failed-classes.txt` | 52,366 | `2592123f2c85d099defc5d5fc90587f80643d9bad0c1702925d7d2105f9e66d0` |
| `cpg-ceiling-reverify.log` | 6,502 | `8ad7682f649faf98007e2b279adb6502bcc8dd334e79ae2e97fd49592c564907` |
| `cpg-frontend-ceiling-probe.txt` | 3,697 | `2e9d302ca4f8b34461d5072d5fe55777a958203c57c71c2f030ac48e1f3d16f3` |
| `cpg-frontend-input-manifest.json` | 81,168 | `ac76ed44592751a694602531fdc5502ca6b115b582f8ca43d218491fc0569a1d` |
| `cpg-frontend-verbatim.log` | 6,286,661 | `6396eda9fdd55f7b6c84a3233eca708adf5bc8b01f6d90b9d276124357a9dd38` |
| `cpg-frontend.log` | 7,615,026 | `4947663d692a219d9a28479d646480147d4fd61c0cc2a53960bbb462a510c4ac` |
| `cpg-graph-record.log` | 13,991 | `1309e1d4344f14fec193d214e65033d303c6ef7e6bc7defddfd3346cbd4c82d7` |
| `cpg-identity.txt` | 19,215 | `e4759b6cca01ddb8691aec9b8aca04e53f2c82f38a7ee25265fac2e0d730b507` |
| `cpg-input-inventory.json` | 112,653 | `39a45952a4f425809f0d3d7272ed0429433aa6df83c9f0411ef8856ac5ed071e` |
| `cpg-module-coverage.json` | 49,094 | `6540f21cd32d990ee3de84cb44f834a5edc697b3eae63bbaa42ed7353f0cfb77` |
| `cpg-shims-collision-measurement.log` | 7,568 | `21697aa6339c3531a9f9a78a8ada27cae7a145c0bc7490859cfba8e47754dea6` |
| `cpg-verify.log` | 89,729 | `211f18c0a28236e0b00d892e0119d89a44ad7623ced5ccdfe8875f1d0208c817` |
| `datadog-sast-rules.captured.json` | 4,068,707 | `c5fd464c2985119574f23599d44022e22b9442d7083acb17ec84addba354f322` |
| `datadog-sast-rules.captured.meta.json` | 1,700 | `886752281650f1fca9ebc7f5009d70b0547a4e1906673ca13c27694961bac240` |
| `datadog-static-analyzer.console.log` | 1,117 | `beaef9fc905647ad63129d17712b59b5ff4d99e2dee3a1dd1e617324b9e4fd3f` |
| `datadog-static-analyzer.runner-console.log` | 1,424 | `88833bae69cb756008d5070520bf81a8381a918915bab583bbeaff8393d04a60` |
| `datadog-static-analyzer.status` | 278 | `7c6de52302804bb0b9ca052ad1fb4bfd88a851df0cce5e48e8c4fdaff5dac6f2` |
| `datadog-static-analyzer.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `datadog-static-analyzer.stdout.log` | 4,043 | `a311e2d20e16173649c38602a4ed684daea77a4dba92298b8813bc1621e62ae1` |
| `dependency-check-positive-capture.json` | 46,684 | `ee48683145332f02d5dd101fa0d5fb1b812667b53eec81a97c962b7939911af1` |
| `dependency-check-positive-capture.log` | 4,380 | `fab127c3f647ce1d207a1e6245dd2f582c2c9e67bdfdae4ebb35783eb34bad8d` |
| `dependency-check.out/dependency-check-report.json` | 17,097 | `2861fbf4165b56d1a8f0b6db7a1895f30b452922c7c08521ca00825016097799` |
| `dependency-check.runner-console.log` | 1,419 | `b9669824ed10aa96d0008e2ee518651fe921fa344a242374fe2b78bc66412b3b` |
| `dependency-check.status` | 260 | `a888d8b4ecb7261c70fff7978b5e16867af0047b2c39983057bc12e93a2765a2` |
| `dependency-check.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `dependency-check.stdout.log` | 2,067 | `59a529f2329ff7c64a33671d764486807e5b955203bd3fb2d3b58f45b37ab814` |
| `findings-publication.json` | 2,985 | `4908a941315a41e3cae2612fa1d1459dadeba138a8a82eefb696c6d439e37102` |
| `gate-record.json` | 148,734 | `3b15ce6633007952d9b1ca5b0c6cc5443e6f7a99138e1a3b4a5c96e42053db10` |
| `gitleaks.parts/common_network-common_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/common_network-shuffle_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/common_network-yarn_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/core_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/python_pyspark.json` | 562 | `72941b811ee4446d838dded31c9cf09fd1a14850b8d30f9266d9d6061a9bf11e` |
| `gitleaks.parts/resource-managers_kubernetes_core_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/resource-managers_kubernetes_core_volcano_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/resource-managers_kubernetes_docker_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/resource-managers_yarn_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_catalyst_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_connect_client_jdbc_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_connect_client_jvm_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_connect_common_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_connect_server_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_connect_shims_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_core_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_hive-thriftserver_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.parts/sql_hive_src_main.json` | 3 | `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570` |
| `gitleaks.runner-console.log` | 1,404 | `486188813950a03116f379fb8d7f724c1b116fb398e2c4f6590ee1d8d470254e` |
| `gitleaks.status` | 243 | `8afcf8f13b6aaaf29c959a5acb7fb94e3ddc0b9a98b33215c77afe6bcd880f89` |
| `gitleaks.stderr.log` | 26 | `98467e49ee1b5e56b9b03a596c97f828f907bf0362096ef2bb74f9a5f5718177` |
| `gitleaks.stdout.log` | 1,422 | `461feae54405afc40c7d81025a7859d192654879a5db5fc91a79f3e1b28e5195` |
| `joern-preflight-negative-test.log` | 11,530 | `e1bd4cf99cc9c41430dfce837a0cd48ece7d55c067ef5231817c0eee307fe8de` |
| `joern-preflight.log` | 6,352 | `98e047febb0d66c56366c2eb3dbee446f1c0dc205cc0a9c2da4b11723ef6b916` |
| `joern.preflight.log` | 16,443 | `acb4a045d6ebdaee98cab09088fdcea5b8753df81ee8d9bdb845632124b9a59a` |
| `joern.runner-console-2026-09-03.log` | 3,375 | `e248cd22528285c1cc3bc8a9461790437eea701f4b24f624af39e8966f5f8296` |
| `joern.runner-console.log` | 1,700 | `47a9d744fb9045a5c981a413ab00a642243be27aa7d828f1c30a88492dc0e266` |
| `joern.runner.console.log` | 1,428 | `53c18a17aba88510d0974b92094468071c909faa6f01a39ae484f6e4e763b82b` |
| `joern.status` | 235 | `22c166c5a9a58c378da5df3757c53fa43d232ac2970fb1df50e6bd35e6db75bc` |
| `joern.stderr.log` | 768 | `4d6e22346f23ecab5530985b6de6978068472c966211ce14600d49d963bfeca2` |
| `joern.stdout.log` | 13,939 | `9c6b1167f14cf78eb2a9e64741e70821a2f598eb185801b031e6968057904522` |
| `maven-preflight.log` | 10,398 | `345e17b69cab36a1bd11ca8987d511740db1bbffda22cc9127d688ec48844cfa` |
| `normalize-run.json` | 767,672 | `a6ee7fcf434cdc71729e3e863fdcd963bed5628f7e69238a3c6807515e21acf9` |
| `opengrep.runner-console.log` | 1,305 | `dcdb7a627385d9b2d946569c78348aa57b008046581ba10ed6fb85a4449da519` |
| `opengrep.status` | 251 | `65507e366b7f8ea3e1c301cad20f6336714fcd9a21759ae170b6449ab5d8184d` |
| `opengrep.stderr.log` | 2,560 | `f683d85f35d12b6ec790c4a0df65b6e4124c96aba9ecca4c061b11791548e938` |
| `opengrep.stdout.log` | 73,768,117 | `6f2a5746ed9eacde51b2dd3a1ece47bedefc6c7c1d9874bf5222fdec766407b0` |
| `osv-scanner.runner-console.log` | 1,281 | `0503a115a648e5ccead440ab27ea1638832dbd4e6285752783eeafdb178a6f1c` |
| `osv-scanner.status` | 254 | `920ba69be84df9436b06ec592ce2ec96b8c6ef52af9cf009503e5280429d6ea8` |
| `osv-scanner.stderr.log` | 967 | `03e42fd9fe0c83921df8bc7f4377231723a69ebad6cf48095fa39e4f7fe31cf5` |
| `osv-scanner.stdout.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `probe-01-callgraph-unguarded-driver-launch.identity.txt` | 1,068 | `ee4ae888944efb21f64c494c7019cbc260ed28c9ec696447da59bf4b4638630c` |
| `probe-01-callgraph-unguarded-driver-launch.log` | 17,519 | `4b1643938846f1d54ab09057de6032294041647ff86c9a248840fcde968a33b5` |
| `probe-01-callgraph-unguarded-driver-launch.publication.json` | 2,312 | `1029a43aaef8ddec5853e40246912dfceff96f028d446dddd7400115642e9421` |
| `probe-02-dataflow-unguarded-driver-launch.identity.txt` | 1,067 | `332e346ccd96719e0cc29f65942ce0ad5b5f7bf47a3f755c0eeb5f02409ed212` |
| `probe-02-dataflow-unguarded-driver-launch.log` | 22,707 | `d99793ea46f3c833b5707c78666ac1824d3dfb58280ff9119237b7d589f4d050` |
| `probe-02-dataflow-unguarded-driver-launch.publication.json` | 2,305 | `19ee1539ae0cb6f25683bca43681deea8f6e534fe1090e0905579ad16613e215` |
| `probe-03-parameterized-handler-sink-pairs.identity.txt` | 1,067 | `4c21c2edd0146566222022698c109e9be2a9586efb59a7ea9e68ac8568a5d0b1` |
| `probe-03-parameterized-handler-sink-pairs.log` | 31,791 | `d4aee84bee60f86f72cdcec9a08a8e6f05933d93f23af91c37ca6662dfd59351` |
| `probe-03-parameterized-handler-sink-pairs.publication.json` | 2,305 | `d03b75eaa709298c3263230d6c7e805b3e8877e360149efcb407ee3195da6244` |
| `probe-query-revisions.json` | 7,463 | `158dcbab77bc5d1dd0cfa58dc05a8d80b6681944a92d0fc59d3cf9a5830b068a` |
| `reverification-f1-artifact-trees.txt` | 26,510 | `64c94d63374df332b28ac3649f41cc3d12143aca1c0b6e2284fb8becb09628c5` |
| `reverification-f2-graph-identity.txt` | 34,399 | `270cd450ca7b1bec7ef2224e9e0661620919abb4afce35bab87058ccdd37d16b` |
| `reverification-f3-writer-bound.json` | 26,859 | `a038c53ee1cb43ad02bd8f99deb18d07d7d6657618be87cd8dae81de50c967e7` |
| `reverification-f3-writer-bound.txt` | 34,697 | `e399dc1e17e4d32ab20addbf140da7b7293f4f34d65f19e4bf4e338e4509792d` |
| `reverification-f4-module-witness-full-input-set.json` | 268,176 | `3fcc3056968d207b83b5ae642ce52a0e8f7afc0426b606a97616a34e020da76d` |
| `reverification-f4-module-witness-full-input-set.log` | 66,893 | `a04d31f83008d82d2261f8e9692f5b645e61a9c130fb716f5385f40f076a9ea2` |
| `reverification-f5-taint-ab-hiveshim-off.sarif` | 2,341 | `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |
| `reverification-f5-taint-ab-hiveshim-on.sarif` | 10,021 | `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| `reverification-f5-taint-ab-off.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `reverification-f5-taint-ab-on.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `reverification-f5-taint-ab-source-removed.sarif` | 2,341 | `295888055aed2ccb8dc171eebc5e702ea741c0eb86e760d3942d122936b32187` |
| `reverification-f5-taint-ab.json` | 27,658 | `a9d679e756af5e77e0362e0b4738df9eca8d4c2dea3d8a826482d94630e901b3` |
| `reverification-f5-taint-ab.log` | 34,939 | `5956486fc25fbc426e1ac35417bd985a4a6542cf1f610e7efee58096865bc6e8` |
| `reverification-f5-taint-engine-options.txt` | 41,622 | `7e931f9316508c39a8caea6ce8abdf15ef12f308b6b87e68aef01c86cb5d8afd` |
| `reverification-sec-04-diagnostic-secrecy.log` | 12,978 | `f0b28882896882034393cb42323e2ed8f14fbb41c84f3dd26d579a3c0a32a50e` |
| `reverification-sec-06-percent-validation.log` | 6,710 | `2c6d1490eb2955c434541696099b1af56e66eccec3f178581bc8481311cd768a` |
| `reverification-sec-toolchain-advisories.json` | 35,758 | `97ea34fca32a62cb186c36a5edae4edaa78f9c1dc9bd498cfda1ccab7b3542f7` |
| `reverification-sec-toolchain-advisories.log` | 21,270 | `0b93776f6eae695e7e186e2f2b055fe359b094375292ab0be391129acb730ea2` |
| `runner-metadata.json` | 224,049 | `5150023eac3c4e4c28ab1ae0d242ef91633232af826f06c9bbfa2802a8511a26` |
| `runner-sequence.json` | 30,619 | `9108c28691945a2cfbb84e05fa2cd22d33b08576d0e565b17c6a0bfb87c25fd2` |
| `sec-gate-graph-identity-cases.log` | 9,710 | `27c80a72512d580d27d450d4e837d38950e6d3fd1a2f54dc0483fd8154819dd6` |
| `sec-gate-joern-gated-wrapper.log` | 18,710 | `b0ef8e121c6c4bd5854401b4aebe94de3beaf82deb6dd3ae7e68613401098e97` |
| `sec-gate-joern-heap-boundaries.log` | 5,239 | `bfa7a76717831b5328a8ed935758328c7fe6b9439c211c7a276aa1e00f76fbbf` |
| `sec-gate-joern-heap.log` | 4,554 | `69061eced658f8a53933187613e0a77d85bc03c5e31a441d7f90f89b43d928da` |
| `sec-gate-scan-target-cases.log` | 11,662 | `f2892580a8e2be25171b701b3ed1162bc64d93a16a5bb9443931cccc8734a9f1` |
| `sec-gate-scan-target.json` | 23,320 | `dfad296f7d539bfd9e7f17a42e3df7e92b7e0ce45ee827c6b6264c8a3180c4b3` |
| `sec-gate-scan-target.log` | 16,913 | `0224dc7cbee7531e1a6d3e7445891dd516ed439b95982012f8a6f72fd7601803` |
| `sec-gate-scanner-gated-cases.log` | 7,862 | `f1b99b16ac4f9d878a80d067e231333c12dbc3f73cfa065c1d3c971c2aaef9a8` |
| `semgrep.runner-console.log` | 1,277 | `508196dcd40d9a3f82efb9d899b54b679803716041f72558ff64c1e255a48efe` |
| `semgrep.status` | 248 | `47f12c9714d377477fdc968156a0a31f6d4356464eb2845e893f7a7eee811974` |
| `semgrep.stderr.log` | 5,079 | `d282ddb8cf484139e1294aebf3feb4933a1b8beeb20a0fb59e3313bc3387dd79` |
| `semgrep.stdout.log` | 40,661,985 | `c4294a7251f0fe2cdea4375ec19d43a910ddd8ec9b1a5b7ec4c46e7288b4e881` |
| `taint-ab-anchor-diskstore-fullruleset-off.log` | 13,604 | `ad6879e70da79c0e9864582f21f24fef0623a57c351aa1ad6cc506a097ea6ba1` |
| `taint-ab-anchor-diskstore-fullruleset-off.sarif` | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |
| `taint-ab-anchor-diskstore-fullruleset-on.log` | 13,694 | `016bc4efb378834ab14193220db96d35683b794b42d7aa3c45b7f9143f71197c` |
| `taint-ab-anchor-diskstore-fullruleset-on.sarif` | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |
| `taint-ab-anchor-diskstore-off.log` | 29,959 | `1741433662a7bb783bbd1887b37b86f55d0b0bf428b0522ade7871659a433209` |
| `taint-ab-anchor-diskstore-off.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `taint-ab-anchor-diskstore-on.log` | 29,996 | `b5aa4e1125b3eaac265d2078acd8f4581125d7aa48a31e81d192288b6571456a` |
| `taint-ab-anchor-diskstore-on.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `taint-ab-discriminating-off.log` | 21,705 | `b74ea8f172db6f73822f707f4e2707b5218a296dfba1d07fc72a63c5fa391f3c` |
| `taint-ab-discriminating-off.sarif` | 28,279 | `8c20bbd46dcda3967738677f35bb59f0b9b6b135a7b4a57ff3d89fa4ae9b646f` |
| `taint-ab-discriminating-on.log` | 24,232 | `bda1b238e2ae1655594432b06473f0300a8f94e71436fdd625b74c45dc1164ed` |
| `taint-ab-discriminating-on.sarif` | 37,787 | `685a13d7567c6e295223e265a994cf771ba18c0938d07bc55921dd0caf464a00` |
| `taint-ab-hiveshim-off.log` | 9,930 | `ff1356a9aef785c30cc4e3728a1069e365b13a9a9cae3f5422c8f093e93e0e0a` |
| `taint-ab-hiveshim-off.sarif` | 2,341 | `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |
| `taint-ab-hiveshim-on.log` | 10,435 | `a05c421539ee2c7fdb2e14a2c1bfd09a225467d7ed56c6770964b04f9edfa08d` |
| `taint-ab-hiveshim-on.sarif` | 10,021 | `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| `taint-ab-off-control-rule.txt` | 1,982 | `a1039db83793e43c7144a87506714ccbaf13f92f4fa36c327c74a8ab53364ad7` |
| `taint-ab-off.log` | 23,313 | `29ac4d9bd27a4d5fcc3859158d8cd28afc329469e5f2aa35dfbcbbb69068d2c8` |
| `taint-ab-off.sarif` | 2,341 | `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |
| `taint-ab-on.log` | 25,260 | `3b95368a4cf7cddbd92b2c69aac50b5df6f8b000d4c76c60d708d04327671738` |
| `taint-ab-on.sarif` | 10,021 | `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| `taint-ab-search-control.sarif` | 4,589 | `4dc4aec5f35425f7ff47712baa55a02bcd1f034627d23b0d6f38ba209213b116` |
| `taint-ab-source-removed-control-rule.txt` | 2,498 | `a8bc7f992389761b3ea840012b281e3d218add030663b9132e10924a66f02cac` |
| `taint-ab-source-removed-control.sarif` | 2,455 | `9c54e593e7a9dda361ef2de373bcdb17f0ed4c219c8f18057cf12ca2b1469172` |
| `trivy.parts/common_network-common_src_main.json` | 254 | `b4fec4dd67f22aeb7fe08a4377833a59389e728fdc3dd3502a8fdbd8da432318` |
| `trivy.parts/common_network-shuffle_src_main.json` | 255 | `39f06e817bb93113e70d15787600ceecbbced1430a7f12b63d05a444a6e81c68` |
| `trivy.parts/common_network-yarn_src_main.json` | 252 | `21d14ac3f98f7abcd8de7cd04164a64e779d6901fed1277d1a84e7470f137bb2` |
| `trivy.parts/core_src_main.json` | 237 | `ff8fb6172689ce193cf984896e25ecd701520b9621e68cc14ac5f091811cfce4` |
| `trivy.parts/python_pyspark.json` | 238 | `da9da6f6f889c8adb82cff7de5d272a884f89078ad0d23432e5d0f0634944324` |
| `trivy.parts/resource-managers_kubernetes_core_src_main.json` | 266 | `17f4957e67c60b4d092b07808e8c7f57b1b4eac4889c57657c0476b99be5649b` |
| `trivy.parts/resource-managers_kubernetes_core_volcano_src_main.json` | 274 | `045d3ee1de9c4a792f653340c14e74dfac00dfb1a5afe7fecfdebd7bd412a811` |
| `trivy.parts/resource-managers_kubernetes_docker_src_main.json` | 3,836 | `c997b59aabc4f130e2e29c04f027ca3fe7240eb71957aa8fb0c5692e51d0f2a0` |
| `trivy.parts/resource-managers_yarn_src_main.json` | 255 | `36f3fdd5535e5dac548c937788a9d33defb2a1fff3a0bbd05d155f1b96bb0dd8` |
| `trivy.parts/sql_catalyst_src_main.json` | 245 | `5c3264ea14575fc07a41d7f811d9658fce4d7ffb86007577b82f07fdcac4dce1` |
| `trivy.parts/sql_connect_client_jdbc_src_main.json` | 256 | `5e7a432a1029819c505c65d878d96e6655d123e85168afe8677900654090f57a` |
| `trivy.parts/sql_connect_client_jvm_src_main.json` | 255 | `c98af367868f721b6bc0b4b5f5927984ed9f6affa1ad9fceac9b78f39223ef18` |
| `trivy.parts/sql_connect_common_src_main.json` | 251 | `11e59f59856b6265d00288445fdb392baf9cb933a193c3664cadd8b25a01194d` |
| `trivy.parts/sql_connect_server_src_main.json` | 251 | `55cfad961c4bae21b17d4f63282123350ade0a97a2827a64f9a97f01fd1fa6a9` |
| `trivy.parts/sql_connect_shims_src_main.json` | 250 | `44f2cdcd28d5dcc7d54bf3d3138a785dc59fcac024baaab6604af6ca88556736` |
| `trivy.parts/sql_core_src_main.json` | 241 | `678fb8ed571f9baa16b99c32434ec82470c1ecd86b272f7faa691cdc8daeb40c` |
| `trivy.parts/sql_hive-thriftserver_src_main.json` | 254 | `285f5116389b98ca737dd83e041cba4165a2a1bf1f74e36be67013830d7e88b6` |
| `trivy.parts/sql_hive_src_main.json` | 241 | `581b7d97da2a01a7623ed292f64574bed19e8a5bb162a7ff6ae09a7debc3a6eb` |
| `trivy.runner-console.log` | 1,606 | `ea37c5259f9e5ec31c9b0327b259c515f268ed45bf30e421e639941a6271c052` |
| `trivy.status` | 238 | `7d9d9df0a15fba5eb6360796d97988400595fe7a6a4a6aa7c5b79bac1866be79` |
| `trivy.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `trivy.stdout.log` | 1,422 | `335c28b2e4b984d10259f8e6fd8a9a318a551ef2f4c8e9e76e815fb40519eacb` |

## 17. The authority rule, and where it does and does not reach

AAP §0.1.3, applied throughout and stated here once so no reader has to reconstruct
it from the entries above:

- **The expected-values table governs every field it carries**, and
  `harness/ENVIRONMENT.md` never overrides it. Where both state a field and they
  differ, the table governs and that row's rule applies — which is what makes D7's
  four feed and ruleset differences record-and-continue with a
  not-comparable-with-the-rehearsal marking, and what makes the method count a
  floor the table sets rather than a figure the record sets.
- **Where the record states a field the table does not carry and observation
  agrees**, both are recorded and the field is marked **unanchored**, citing
  `harness/ENVIRONMENT.md` — the five fields in D11.
- **Where the record states such a field and observation contradicts it, the run
  stops**, both values recorded — but read the rule's own condition: it applies
  *because* there is no anchor to adjudicate between them, so that continuing would
  mean choosing one silently. **Such a contradiction did arise, and it is one of the
  gate's two stopping conditions** — `harness/ENVIRONMENT.md` §7 stated the graph's
  byte size, digest and counts, the filesystem stated different ones, and neither the
  size nor the digest is a field the expected-values table carries.
  `gate-record.json` carries it as `gate.environment_record_graph_identity_agreement`
  across its **43** checks — **38 pass, 3 recorded differences and 2 halts** — and
  the gate's `authorises` field is `nothing`.
  **On this one the rule was misapplied, and the misapplication was corrected on
  2026-09-02.** An anchor did exist: the graph's write-time record of account,
  `/opt/blitzy-harness/provision-log/cpg-record.txt`, carries an "Expected vs observed
  (prior provisioning record)" block that names both pairs, labels the record's figures
  `PRIOR` and the filesystem's `NOW`, and states the cause. With an anchor present the
  fourth case does not reach the field at all, and the first case does: the adjudicating
  statement governs and the code — here, the record — is aligned to it. §7 and the
  inline-values Graph block were re-anchored to that owner, both values retained with
  provenance in a supersession appendix, and the graph was not touched. Both values are
  still recorded; what changed is which one the record asserts (§1, §5, **D4**).
- **The rule reaches inherited facts only.** It does not apply to outputs this run
  deliberately replaces. A graph differing from a previously recorded graph's size,
  digest or counts is **the request being fulfilled, not an environment
  contradiction** — reading intentional replacement as a contradiction would halt
  the run for succeeding. The gate enumerated the graph's size, digest and counts,
  and the build outcome, as deliberately-replaced fields for exactly that reason,
  and D4 is handled under that boundary rather than as a record contradiction.

---

## 18. Where the run reached

**No stage of this run is certified complete, and this section does not claim one
is.** Every stage was *executed* and what each produced is recorded below with its
evidence — but the gate's verdict is `halt` and its `authorises` field is `nothing`,
so every stage after it ran **after an unmet precondition**. **Four** halt-class
conditions of this run — **D0**, **D1**, **D2** and **D4** — then stood unrepaired
through the rest of it, and two further halt-class departures belong to **superseded**
generations rather than to the lanes on record (**D15**, **D20**). All four of those
conditions were re-executed from their own reproduction steps in this clone on 2026-09-02,
together with the coverage-witness shortfall, and **all five still reproduce**; the register in
[§13](#qa-testing-findings-f1f5-2026-09-02--first-hand-re-verification-and-where-each-is-answered)
carries each one's measurement, its evidence file and the decision it needs. The correct reading of
the table below is therefore *what was executed and what it produced*, never *which
stages passed*: *nothing downstream of the gate is offered here as a compliant stage
completion, and no product of this run is offered as a compliant generation.*

**This is not a monotonic stage ledger, and it is not presented as one.** The stages
did not execute in numeric order, and saying they did would be false against the
timestamps this record itself publishes. What the evidence measures is below, sorted
by instant rather than by stage number, so the ordering is visible instead of
asserted:

| Instant (measured) | Lane | What ran | Stage |
| --- | --- | --- | --- |
| `2026-08-30T20:59:38Z` (finish, 40 m 55 s) | `w-005` | the full-reactor Maven build, exit 0 | 1 |
| `2026-08-30T23:21:24.942Z` (+ 8 h 01 m) | `w-005` | the frontend over the complete 191-archive manifest — **no graph produced** | 2 |
| `2026-08-30T23:30:39Z` – `23:30:44Z` | `w-005` | the Maven pre-check record and the on-disk artifact census | 1 |
| `2026-08-31T07:33:36.391Z` (+ 1 h 42 m 30 s) | `w-005` | the narrowed witness frontend attempt, not used as a substitute | 2 |
| `2026-09-01T13:28:07.612Z` | `w-013` | the gate — verdict `halt`, authorises `nothing` | 0 |
| `2026-09-01T13:31:15Z` | `w-013` | the `importCpg` verification load of provisioning's graph | 2 |
| `2026-09-01T13:48:44Z` – `13:48:46Z` | `w-013` | the mandated Opengrep taint A/B — non-discriminating | 2 |
| `2026-09-01T13:49:39Z` | `w-013` | the runner scan-target and resolved-root verification | 1 |
| `2026-09-01T13:49:39Z` – `14:41:24Z` | `w-013` | the nine runners, serial, direct, no arguments | 3 |
| `2026-09-01T14:52Z` – `15:30:31.248Z` | `w-013` | the three probe loads, each identity-gated — **superseded generation**, whose sources were corrected afterwards | 5 |
| `2026-09-01T23:56:51.679Z` – `2026-09-02T00:35:55.476Z` | this checkout | the three probe loads, each identity-gated, one 64 GiB JVM at a time — **superseded** by the `w424` generation below, because the graph they loaded was replaced | 5 |
| `2026-09-02T14:42:44Z` – `14:50:34Z` | this checkout | the Opengrep taint A/B, all twelve arms, from `cd /opt/spark-src` at the pinned HEAD | 2 |
| `2026-09-02T21:19:32Z` – `21:19:44Z` | this checkout | the adapter and reconciliation suite (final re-run) | 4 |
| `2026-09-02T22:56:48Z` – `22:56:54Z` | this checkout | normalization — **superseded**, see the `w424` row below | 4 |
| `2026-09-03T01:17:07Z` | provisioning | **the host re-provisioned**, which replaced the graph and falsified every figure taken over its predecessor. Not this run's act and not this run's to prevent | — |
| `2026-09-03T01:40:31Z` – `02:11:54Z` (31 m 23 s) | provisioning | provisioning's own `jimple2cpg` write of the graph now on disk, `FRONTEND_EXIT=0` — **inherited, not this run's** | 2 |
| `2026-09-03T09:07:46Z` | this checkout | both preflight gates, run **outside** every runner: `preflight_scan_target.py --check-only` and `preflight_graph_identity.py --check-only`, each exit 0, each `VERDICT: PASS` | 0 |
| `2026-09-03T09:07:47Z` – `09:17:43Z` (596.83 s) | this checkout | **the Stage 3 Joern runner on record**, invoked directly with no arguments, exit 0 | 3 |
| `2026-09-03T09:36:13Z` (sequence stamp) | this checkout | **the three `importCpg` verification loads on record**, import elapsed 526,605 / 557,041 / 545,644 ms, agreeing on all four counts. The record stamps the sequence and each load's elapsed time but **not** each load's own start instant, so only the first load's position in this ledger is established from it ([§14](#14-values-that-could-not-be-established)) | 2 |
| `2026-09-03T09:45:11Z` – `09:45:16Z` | this checkout | normalization (final reproducibility re-run) | 4 |
| `2026-09-03T09:45:34Z` – `10:16:53Z` | this checkout | **the three probe loads on record**, each identity-gated, one 64 GiB JVM at a time | 5 |

**The probe's two rows, and the generation between them.** The row on record is the
generation every probe figure in this run descends from; the `w-013` row above it is the
generation **D17** records as a boundary violation, kept because it happened. A further
generation ran between the two, while the sources were being finished, and published the
revision counts `14, 15, 15`; its streams and envelopes were overwritten in place by the
generation on record, so `oss-scan-results/joern-probe.md` §1's supersession list is the
only surviving record of it, and nothing here cites a figure from it. The row on record
carries **measured finish instants** — each stream's last write — and **derived start
instants**, each finish minus that stream's own `total elapsed_ms`; the three derived
starts agree with the modification times of the three retained private graph copies,
which is an independent reading of the same three instants.

Two inversions are plain in that table and are stated rather than smoothed over.
**Stage 2's verification load ran at `13:31:15Z`, eighteen minutes before Stage 1's
runner-root verification at `13:49:39Z`** — the graph's identity does not depend on
runner targeting, so it was established first, and the targeting was verified
immediately before the invocations it governs. And **Stage 1's build and Stage 2's
frontend write predate the gate entirely**, by two days and in another lane. Both are
defensible as execution; neither is a numeric stage order, and no claim here depends
on one.

The `Executed` column says whether the work ran and in which lane; the
`Compliance` column is the only column that speaks to conformance, and it is
**`not certified`** for every stage without exception. The fourth column records
**what was measured, never what was produced as a pipeline product**: the
distinction is the whole point of this section, and the column is headed that way
deliberately. `findings.json`, `findings.csv`, the probe results and the per-tool
records exist and are internally consistent — every figure in them is a real
measurement with a citable source — and **none of them is a product of a compliant
generation**, because the graph they all descend from is not the graph the plan
mandates and the gate that should have authorised them authorised nothing.

**Why they are retained rather than withheld, stated as a judgement rather than
left to be inferred.** The strictly-correct control flow would have stopped at
Stage 2 when the mandated graph could not be persisted, and this run did not: it
continued and measured what it could against provisioning's narrower graph. That
is a real departure from the required behaviour and it is recorded as one here and
in [§13](#13-divergence-register). Deleting the downstream artifacts now would
trade one departure for another — AAP §0.9.4 requires all eleven deliverables to
exist — so the choice made was to retain every measurement, label each one
uncertified, and name the graph it actually descends from. A reader who needs the
artifacts to be compliant must treat this checkpoint as **not delivering them**;
a reader who needs to know what the nine scanners reported against the graph that
was available will find it here, correctly attributed.

| Stage | Executed | Compliance | What was measured, and against what — **not** a product of a compliant generation |
| --- | --- | --- | --- |
| 0 — Gate | ran in lane `w-013` | **not certified — verdict `halt`** | `gate-record.json`: **43 checks, 38 pass, 3 recorded difference, 2 halt**; `authorises` is `nothing`. **One** of the two is a condition this run may neither create nor clear (AAP §0.8.1): the two artifact trees were already non-empty when it measured them. The other — the environment record contradicting the filesystem on the graph's identity — **was re-anchored at the record on 2026-09-02 and again on 2026-09-03**, each time against the graph's own write-time record of account (**D4**); the graph was not touched in either correction. The gate did its job either way — it published both readings and authorised nothing, and its verdict is left as measured. Re-censused on 2026-09-03 the live condition is present again at 8 and **129** top-level entries, so nothing has cleared it |
| 1 — Tree and build | ran in lane **`w-005`**, 2026-08-30 — **inherited, not re-executed in this clone** | **not certified** — ran before the gate that halted, and in a different lane | pinned `HEAD` equal to the pin; allowlist byte-exact and left as found; Maven pre-check **PASS** with the download branch unreachable; `BUILD SUCCESS`, 40/40 projects, 38/38 own artifacts; `runner-metadata.json` later finalised in lane `w-013` with every runner's target set and its root verified |
| 2 — Graph | frontend write ran in provisioning's own lane (inherited, `2026-09-03T01:40:31Z → 02:11:54Z`), the superseded 191-archive attempt in lane **`w-005`**; verification and measurement ran in lane `w424` | **not certified — the mandated output was never obtained** | **The frontend was invoked over the complete 191-archive manifest and failed in persistence at a fixed array-length bound after 8 h 01 m, producing no graph at all (D1).** The staging manifest was asserted total and injective before the invocation and the complete set was supplied — **191** archives, **431,184,822** bytes (`build-reactor.log` STEP 13 lines 10035-10036; `cpg-frontend.log` STEP 1 lines 134-135) — so the *input* requirement was met and the *output* requirement was not; the per-entry manifest behind that assertion was later regenerated for the 62-archive set, which **D9** records. Every later stage therefore loaded **provisioning's** graph, whose input set is **62 archives over 31 modules** and is narrower than the build (D3): `cpg-input-inventory.json` inventories it — its archive-to-digest mapping total and injective both ways **as measured at write time**, an assertion its own 2026-09-02 census marks superseded for the live tree, which has since drifted to 45 distinct digests over the same 62 names (D3), and the `importCpg` verification load exits 0 reporting **1,396,899 methods, 119,721 type declarations and 45,037 files**, with per-module coverage on injective evidence for **26 of the 31 modules in that input**, 5 with no obtainable witness, **0** on presence and **0** on a shared prefix. The same test applied to the **191**-archive set this run’s frontend was given — measured 2026-09-02 and owned by `build-record.md` §6 — yields **30 of 38** modules with an accepted witness and **8** with neither kind, so part of the coverage shortfall would survive even a graph over the complete set. That is a verification of a *different graph than the one the plan mandates*, and it is not a substitute for it. Nothing was trimmed to obtain a graph; the ceiling was re-verified at **three heaps — 8 GiB, 64 GiB and 128 GiB** — with the failure point unmoved at exactly 2,147,483,639 buffered bytes in every arm. **The mandated taint A/B did not discriminate (D2)** — both arms return the same single finding at `DiskStore.scala:72` — reported and not repaired, with discriminating pairs measured separately on other Spark Scala: `HiveShim.scala` yielding **2 traced findings against 0**, and `JdbcDialects.scala` 12 against 11. **The provisioned record contradicts the filesystem on the graph's identity (D4)**, reported and not repaired. **No narrowed or witness graph is presented as a substitute for the mandated one** |
| 3 — Nine runners | eight ran in lane `w-013`, one serial lane; **`joern` re-invoked in lane `w424`** at `2026-09-03T09:07:47Z → 09:17:43Z` over the re-provisioned graph | **not certified** — ran after the gate's halt, and against the D3 graph rather than the mandated one | all nine invoked directly, individually, with no arguments and through no orchestrator, **from one script in one process in one clone**, no tool twice, with each invocation's artifact, both streams, `.status` and console log bound to it by byte size and sha256 — **82 pieces re-measured with 0 mismatches** (`runner-sequence.json`, [§8](#the-delivered-lane--one-serial-lane-bound-to-its-evidence-by-digest)); eight artifacts written; `osv-scanner` completing with its own stated reason and no artifact. Every figure is measured and reproducible; none of it certifies the stage |
| 4 — Normalization | ran in lane `w424`, `2026-09-03T09:45:11Z → 09:45:16Z` | **not certified** — its Joern input descends from the D3 graph, and it ran after the gate's halt | **9,427** rows, `10013 = 9427 + 586`, typed comparison over **113,124** fields with no mismatch, row validation with zero violations, exit 0; the superseded generation of 2026-09-02 recorded 9,430 rows, `10016 = 9430 + 586` and 113,160 fields over a raw tree whose `joern.json` has since been replaced, and reproduced both output files **byte-identically** on a re-run, which is the repetition property [§14](#14-values-that-could-not-be-established) records as not re-established for the generation on record; **1361** adapter and reconciliation tests passing |
| 5 — Probe | ran in lane `w424`, `2026-09-03T09:45:34Z → 10:16:53Z` | **not certified** — every query loaded the D3 graph, not the mandated one | three bounded hand-written queries run under `importCpg` only, each gated on the graph's re-verified identity immediately before its load, six result files, all three effort measures answered, parameterizability passing on an invocation that was actually made |
| 6 — Record | this file, re-anchored in lane `w424` | **not certified — and its job is to publish the halt, not to close it** | the eight result deliverables and the three deliverable trees all exist ([§11](#11-deliverable-inventory-with-resolved-absolute-paths)), and both artifact trees are published by manifest ([§16](#16-manifest-of-the-two-git-ignored-artifact-trees)). Every halt-class condition is carried at the top of the document that owns it |

> **CHECKPOINT STATUS: HALTED. NOT COMPLETE. NO PRODUCT OF THIS RUN IS A COMPLIANT GENERATION.**
> Four conditions block completion, and **none of them is repairable by any action this run is
> permitted to take**. They are stated here with the specific permission each one would require:
>
> 1. **The gate cannot be made to pass.** Of its two stopping conditions, one has since been
>    cleared and one cannot be. The environment record contradicting the filesystem on the
>    graph's identity **was re-anchored at the record on 2026-09-02 and again on 2026-09-03**
>    (**D4**) — the second time because a re-provisioning rebuilt the graph and falsified the
>    first correction — and the gate that measures that agreement now exits **0** with
>    `VERDICT: PASS`. The two artifact trees arriving non-empty remains, and AAP §0.8.1 and
>    §0.9.2 forbid this run creating *or* clearing them — a non-empty tree is a provisioning
>    fault to report, and clearing it would destroy the very evidence that makes the fault
>    visible. Re-censused on 2026-09-03 the condition is present again, at 8 and **129**
>    top-level entries. **A gate-passing state can therefore still only be produced by
>    re-provisioning, which is outside this run's authority**, and this document's status is
>    unchanged. One thing about it is now settled rather than open: the stages that ran after
>    that verdict **cannot be retroactively authorised by anything**, because an execution
>    cannot be un-run — so this condition is permanently a reported fact about this generation
>    rather than a defect awaiting a fix.
> 2. **The mandated taint A/B has no taint-free arm at this pin.** Established first-hand:
>    `opengrep 1.27.1` exposes only `--taint-intrafile` and `--guarded-taint-signatures`, and
>    neither disables taint; the mandated rule's source and sink both sit inside one method of
>    `DiskStore.scala`, so intrafile-only cannot separate them either. **A discriminating arm
>    would require a different Opengrep version, which AAP §0.4.3 forbids installing.**
> 3. **The graph over every JAR the build produced cannot be persisted.** The frontend was
>    invoked over the complete, asserted 191-archive manifest and failed inside
>    `flatgraph.storage.WriterContext.finish` with `Required array length 2147483639 + N is too
>    large` — a fixed 32-bit bound, re-verified in this clone at `-Xmx64g` and `-Xmx128g` with the
>    failure point unmoved while `maxMemory` doubled. **The only two remedies are excluding
>    inputs, which AAP §0.3.2 and §0.9.2 name as a halt rather than a fix, and upgrading Joern,
>    which §0.4.3 forbids.**
> 4. **Therefore no complete staging → frontend-write → verification chain exists, and the run did
>    not stop when it could not build the mandated graph.** It continued and recorded what it
>    could measure. That continuation is disclosed here rather than presented as compliance: every
>    downstream product — the dataset, the probe, the per-tool records — is a measurement taken
>    against provisioning's narrower graph (D3), and is marked `not certified` above for exactly
>    that reason.
>
> What this record therefore is: an **evidenced account of a halted run** — every figure measured,
> every source citable, every blocked requirement named with the permission it would need. What it
> is **not**: a claim that the pipeline completed, or that any artifact below satisfies the
> requirement it was meant to satisfy.
>
> The remaining detail, retained from the prior statement and corrected where it has moved: two of
> the plan's own halt conditions are met and neither is repairable by any permitted action. **D1** —
> the mandated graph over every JAR the build produced cannot be persisted by the pinned frontend,
> proven from the failing method's bytecode; the only effective remedy is excluding inputs, which AAP
> §0.9.2 lists among the conditions that stop the run. **D2** — the taint A/B does not discriminate on
> the mandated subject, with the engine's activity separately measured on another file. **D4 is no
> longer among them**: the prior statement read it as AAP §0.1.3's fourth case, a halt with no anchor
> to adjudicate between the record and the disk. That reading was wrong — an anchor exists, the
> graph's own write-time record of account, which names both pairs and labels them — so the record was
> re-anchored to it rather than reported against it, twice, with the graph untouched. The correction
> is carried in full in **D4** rather than asserted here. Every
> other stage of the run ran to its own end and is recorded below — and **no stage is certified**, because
> the gate that would have authorised them halted; these three are reported rather than resolved,
> which is what AAP §0.8.1 requires of them. **And the gate itself halted**, on two further conditions
> this run may neither create nor clear: both artifact trees were already non-empty when it measured
> them, and the environment record contradicts the filesystem on the graph's identity — the same
> contradiction D4 carries. Its verdict authorises `nothing`, so every stage after it is recorded as
> work done after an unmet precondition. Two further halt-class departures — **D15**, the nine per-tool
> records assembled from five clone-local lanes, and **D20**, two archives withheld from a frontend
> input set — belong to **superseded generations** rather than to the lane on record, and are retained
> with both values rather than softened. A condition previously carried here as unresolved, **D13**,
> is now **resolved**: sixteen delivered files a commit had deleted — thirteen provisioned harness
> files and three members of `logs/`, one of them cited evidence — are restored byte-for-byte and
> verified, with the restored surface exercised rather than assumed and no citation left orphaned.

**Four halt-class findings of the run on record, and none was repaired**: **D0**, the
gate halting on the two artifact trees arriving non-empty and therefore authorising
`nothing`, so that no stage after it is a compliant completion; **D1**, the graph not
being this run's own output — attempted over the complete 191-archive input set and
blocked by a fixed array-length bound in the pinned frontend's writer; **D2**, the taint
A/B not discriminating on the mandated subject, with the engine's activity separately
measured on other Spark Scala; and **D4**, the provisioned record's stated graph identity
being contradicted by the bytes on disk. AAP §0.8.1 settles which way that tension
resolves — report the condition, never repair it silently — and nothing was installed,
rebuilt, trimmed, overwritten or averaged to clear any of them. In particular, the one
change that would have produced a current-run graph is excluding inputs, and that is the
trimming §0.9.2 names as a halt rather than a remedy. Each is stated at the top of the
document that owns it rather than in a footnote, and each is carried here with every
value it has.

**Two further halt-class departures belong to superseded generations and are retained as
history, not as this run's conduct.** **D15** — the nine per-tool records assembled from
five clone-local lanes, with five overlapping invocation windows and one prohibited second
Checkov invocation — describes the generation whose *enriched* `.status` files commit
`0e3e742a5ad` replaced with the runners' verbatim seven-line trailers; the lane on record
is one strictly serial lane of nine zero-argument invocations from one script in one
process in one clone, bound to its evidence by 82 measured digests
([§8](#8-the-nine-runners--target-variable-and-path-base)). **D20** — two archives
withheld from a frontend input set for coverage-witness reasons — describes a superseded
w-000 attempt; the invocation on record supplied the complete 191 with no exclusion flags
at all. Neither is softened by being superseded: both stay registered with both values and
with what a human must do.

**One divergence is neither halt-class nor a departure, and is registered rather than
removed**: **D21**, seven repository additions that conform to the AAP's own conventions but
carry no row of their own in its §0.6.1 transformation mapping — each load-bearing for a
requirement the AAP does state.

**And one entry previously carried here as unresolved is resolved**: **D13**. A commit had
deleted sixteen delivered files — thirteen provisioned harness files and three members of
`logs/`, one of them cited evidence — and all sixteen are restored byte-for-byte and
verified, with the restored surface exercised rather than assumed.

**Three things WERE torn down by a superseded generation, this sentence used to deny
it, and the generation on record tears down nothing.** An earlier edition of this
paragraph stated *"Nothing was torn down. No cleanup, no reset, no temp purging"*, and
that was false of the 2026-09-01 probe generation: each of its three invocations
**deleted the private graph copy it had loaded and the exclusive directory that held
it**, which AAP §0.8.1 — "Do not tear anything down. No cleanup, no reset, no temp
purging. What the run built stays where it is" — forbids. Those copies are gone and the
bytes those loads read are not re-measurable; that cost is stated rather than closed.

**The three invocations on record retained theirs**, at the mode the copy step set, and
they are on disk:

| Query | The private graph copy this run retained | Named at |
| --- | --- | --- |
| 01 | `/tmp/blitzy-harness-scratch/424/probe-graph-input-c5f2f2b6ce1292ac40424cd9/spark.cpg`, inode `(dev=10301,ino=112547941)` | `harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log` lines 38, 42, 69 and 71 |
| 02 | `/tmp/blitzy-harness-scratch/424/probe-graph-input-d6353bd7ff69afb7d0c52e69/spark.cpg`, inode `(dev=10301,ino=112807597)` | `harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log` lines 38, 42, 69 and 71 |
| 03 | `/tmp/blitzy-harness-scratch/424/probe-graph-input-7e4dabb2ef5ef2e81bd0af3c/spark.cpg`, inode `(dev=10301,ino=112807645)` | `harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log` lines 38, 42, 88 and 90 |

Each is `0400` inside a `0500` directory, each re-measures to **547,980,224** bytes /
`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`, and each envelope
publishes `graph.private_copy_retained_after_verification = true`. **An earlier edition of
this table named three copies under `/tmp/blitzy-harness-scratch/0/` at 541,309,809 /
`4616845a…4730c7`** — `probe-graph-input-6708054a4f5227f8926d9a03`,
`…-11ac4197c6bde353b2c6e9f6` and `…-cf0ba216ebf4ea8ab2611843`. Those belonged to the
2026-09-01 generation and clone 0's scratch directory **no longer holds any of them**; it
holds only `joern-run`, re-measured for this edition. The retention property this table
asserts holds for the generation on record, which is what the table is about. **The source was
corrected before those runs:** the function that deleted the copy — and had to widen its
directory to do it — is replaced by one that deletes nothing and names what it kept,
citing §0.8.1 at the point of the change. The whole condition, both generations, is
registered as **D18** in [§13](#13-divergence-register).

**Everything else stands where the run left it.** No cleanup, no reset and no temp
purging by the generation on record — including the three retained copies above, which
this run is forbidden to remove: both artifact trees stand where the run left them, no
runner file, environment file, shared library, allowlist or Apache Spark file was edited
in either tree, and no credential was provisioned.

---

## 19. What this document does not do

- **It draws no comparison between tools, of any kind.** Nothing here ranks the
  nine, contrasts their coverage, explains why one reported something another did
  not, or characterises what any tool's output demonstrates about that tool. The
  index in [§8](#8-the-nine-runners--target-variable-and-path-base) places nine
  outcomes side by side because that is what an index does; no sentence reads one
  figure against another. **No comparison is made against Apex, Cantina or any
  other scanner**, no such data being part of this run.
- **It judges no finding.** Not real, not important, not a false positive, not a
  duplicate of another tool's. It deduplicates nothing across tools, and it
  remediates, patches and exploits nothing.
- **It measures no elapsed time against a budget.** There is no time limit anywhere
  in this run, so every duration above is a fact.
- **It relocates no ownership.** The per-project JAR outcome and the per-module
  coverage verdict belong to `build-record.md`; the per-tool contract to
  `tool-status.md`; the mapping and observed literals to `severity-map.md`; the
  per-query probe results to `joern-probe.md` and the files under
  `queries/joern/results/`. Where a figure appears here and there, it is one
  measurement cited twice.
- **It invents nothing.** Every figure names the file it came from. A value that
  could not be established is named as such in
  [§14](#14-values-that-could-not-be-established) rather than omitted, and no
  placeholder stands anywhere in this document.
