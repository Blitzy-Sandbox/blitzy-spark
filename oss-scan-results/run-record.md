# Run record — the index to every stage

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
[§11](#11-deliverable-inventory-with-resolved-absolute-paths), resolved against
the repository root as it stood when this file was written:

```text
/tmp/blitzy/blitzy-spark/blitzy-f38258d3-f87d-44f5-bedc-af512c69e0ab_a424a0
```

**That root is clone-local, and the evidence files name their own.** This run was
executed across several clones of one branch, each writing its own evidence and
merging it here, so an evidence file records the clone root that was in force when
it was written — `harness/artifacts/logs/gate-record.json` and
`runner-metadata.json` name clone 009, `cpg-input-inventory.json` names clone 020,
several `<tool>.status` files name the clone their runner ran in, and
`cpg-verify.log`, `normalize-run.json` and `adapter-tests-run.json` name the root
above. The repository-relative path is the stable identity across all of them, and
no clone's root is presented here as *the* run's root.

---

## 1. Gate verdicts

Source: `harness/artifacts/logs/gate-record.json` (135,586 bytes; digest in
[§16](#16-manifest-of-the-two-git-ignored-artifact-trees)). Overall verdict
**pass**, authorising Stage 1. Forty-two checks: **39 pass, 3 recorded
difference, 0 halt, 0 inconclusive**; three entries carry sub-verdicts counted
separately so no verdict is double-counted.

| Gate condition | Verdict, as recorded |
| --- | --- |
| The environment record read **first** | `harness/ENVIRONMENT.md`, 923 lines, sha256 `5aa68b255295e26ae129b9159e32ea76b33d1d66f835aa9a3625b040f5ecb140`, read in full before any other gate command ran |
| The environment file **the record names**, never assumed | The record names it twice at its lines 6–13 — the sourcing command `. harness/env.sh` and the sentence naming `harness/env.sh` as the environment file. Present, 4,515 bytes, 91 lines, mode 755 |
| Sourced in a **fresh non-login shell** | `env -i BLITZY_CLONE_INDEX=9 bash --noprofile --norc -c '. harness/env.sh'` → exit 0, stdout empty, stderr empty |
| All nine tools resolve | Eight by bare name under `/opt/blitzy-tools/bin`, plus the `jimple2cpg` wrapper; `dependency-check` resolves through `$DEPENDENCY_CHECK_HOME/bin/dependency-check.sh` rather than by bare name. Zero `NOT-ON-PATH` results |
| Nine versions against their pins | opengrep 1.27.1, semgrep 1.173.0, joern 4.0.607 (read from the startup banner with stdin closed, there being no `--version`), datadog-static-analyzer 0.9.1 revision `f76636e43554f7f9a8e3984a31d03ec8dea5489f`, gitleaks 8.30.1, checkov 3.3.12, trivy 0.74.0, osv-scanner 2.5.1 (osv-scalibr 0.5.2), dependency-check 13.0.0 — **every one equal to its pin** |
| The Python interpreter's absolute path and version | `/usr/bin/python3`, 3.13.7, build string `3.13.7 (main, Mar  3 2026, 12:19:54) [GCC 15.2.0]` — equal to the expected 3.13.7. The two scanner virtualenvs `/opt/blitzy-tools/venvs/semgrep/bin/python` and `/opt/blitzy-tools/venvs/checkov/bin/python` each report 3.13.7 as well |
| Both JDKs present | Temurin-17.0.20+8 (major 17) and Temurin-21.0.12.1+1 (major 21), each reporting its own version |
| The heap **commit** proof | `"$JAVA_HOME/bin/java" -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version` → **exit 0**, and a second arm under the 21 JDK → exit 0 in 9.250 s. `-Xms` equal to `-Xmx` with `+AlwaysPreTouch` touches every page at startup, so a zero exit is **strictly stronger than a reservation** |
| `harness/bin/` enumerated and classified | Nine entries, all mode 755, all `run-<tool>.sh`: **9 runners, 0 helpers, 0 orchestrators**, each mapping to one canonical identifier in the AAP §0.5.4 class table, and **no entry naming a scanner that table does not carry** |
| Each runner's argument guard | Established **by inspection** for all nine: the guard is the first executable statement and exits 64, and it precedes environment sourcing, shared-library sourcing, target resolution and tool invocation in every one. **No rejection probe was run**, and none was permitted, because inspection settled the ordering |
| Both artifact trees exist and are empty | At the emptiness check, `harness/artifacts/raw/` held **0 entries** and `harness/artifacts/logs/` held **exactly one**, `runner-metadata.json`, this run's own gate-stage evidence. `ls -A harness/artifacts` returned exactly `logs` and `raw` — no third sibling |
| The smoke override unset | `HARNESS_SMOKE_TARGET` unset in the fresh non-login shell **and** in the ambient shell, so a value inherited from the invoking environment could not hide behind `env -i`'s stripping |
| Credentials | `SEMGREP_APP_TOKEN`, `DD_API_KEY`, `DD_APP_KEY`, `NVD_API_KEY` and `BC_API_KEY` absent in both arms. `GITHUB_TOKEN` reads set in the ambient shell and is read by no runner. Every runner reports credential state through `scope_cred_state` (`harness/lib/scope.sh` lines 105–109), which expands `${VAR:+set}` only and can print nothing but the fixed tokens `set` and `absent` |
| The allowlist byte-exact | sha256 `0013edf6cdc3a48d69aed5d7db41cc6647cfd461d348f5e1d563ba85664143d1`, 12 lines, byte-identical to the twelve authoritative globs in the AAP §0.3.1 order. Transformation mode **REFERENCE** — read as-is, left exactly as found, **nothing written** |
| Maven identity, and that no download would trigger | `/usr/local/bin/mvn`, Apache Maven 3.9.11 (`3e54c93a704957b63ee3494413a2b544fd3d825b`), home `/opt/blitzy-tools/apache-maven-3.9.11`, running on Temurin 17.0.20 — exactly the version the pinned pom requires |
| Scala, git, pinned HEAD | Scala 2.13.17; git 2.51.0; `git -C /opt/spark-src rev-parse HEAD` equal to the pin |

**The gate record cannot be the thing that made `logs/` non-empty.** The emptiness
check was taken at `2026-08-24T17:01:36.594Z` and the record was written at
`17:06:49.246Z` — **312.652 seconds later**, from a result held in memory. The
record states that ordering as a field of its own, with both timestamps.

**One state worth naming precisely.** The emptiness rule covers `raw/` and
`logs/` only, and `logs/` held one entry at the check: `runner-metadata.json`,
written by this run at the gate as the normalizer's input. Nothing attributable to
an earlier run was present in either tree, and this run neither created nor
cleared either of them.

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
| Tree on disk (`SPARK_SRC`) | `/opt/spark-src` — built by this run, **never edited**, and shared read-only with concurrent clones | `runner-metadata.json` `spark_src`; `gate-record.json` check `gate.pinned_tree_head` |
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
  scope: `Ran 1138 rules on 4095 files: 1322 findings.`
  (`harness/artifacts/logs/opengrep.status`, field `findings_summary_line`).
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
| Wall clock | 1,361 s (18:52:00.024Z → 19:14:41.583Z), Maven's own `Total time: 22:39 min` — a fact, not a figure measured against a budget |
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

**Which graph these counts describe.** Per **D1** there is no current-run graph: this run invoked the
frontend over its complete 191-artifact input set and the invocation failed in serialization at a
fixed array-length bound, producing nothing. Every count in this section therefore describes the graph
at the sanctioned path — the one provisioning wrote and every stage of this run loaded — and **no
count here is a current-run measurement**. That is stated at the top of the section rather than in a
footnote, because a method count read as this run's own would be the single most misleading number in
the record.

The counts come from `harness/artifacts/logs/cpg-verify.log` PHASE 2, which
re-derived them from the artefact itself by loading it with `importCpg`. They were
produced by **two independent loads in two separate JVMs with two separate
workspaces**, and both reported the same three figures — a reproducibility check
rather than a restatement.

| Count | Expected | Observed | Delta | Halt semantics |
| --- | --- | --- | --- | --- |
| methods (anchor) | 898,336 | **1,397,339** | +499,003, +55.55 % | **one-sided: no upper bound** |
| methods (floor) | **853,420** | **1,397,339** | +543,919, +63.73 % | **below the floor HALTS** |
| type declarations | 87,381 | **119,691** | +32,310, +36.98 % | **never halts** |
| files | 38,818 | **45,037** | +6,219, +16.02 % | **never halts** |

`methods > 0` was confirmed explicitly: `NONEMPTY methods>0 true`, and 1,397,339
is not zero — a graph that loads with zero methods is the signature that check
exists to catch. `internal_methods` was 1,307,552.

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
`cpg-verify.log` PHASE 2 states this against its own interest: the AAP's stated
rationale for an above-anchor count is the six extra JAR producers, and PHASE 3
measures those six as **absent** from this graph's input set, so that mechanism
cannot be the cause here. What is measurable was measured instead — 927,093
methods (66.35 %) under `org.apache.spark` and 470,246 (33.65 %) outside it,
vendored by Spark's own shading — and the file stops there rather than reporting a
plausible cause as a finding.

**Per-module coverage is owned by `build-record.md` §6** and is not restated here.
Cited: **31 of 38** JAR-producing modules covered by a witness measured in the
graph at the sanctioned path — 25 on a class exclusive to the module and 6 on
presence evidence labelled as presence — with **7** carrying no verdict obtainable
from that graph, each named individually with the witness tried and the query run,
`sql/connect/shims` among them. Because those seven are an input-set consequence
rather than a property of the modules, `build-record.md` §6 carries a **second,
explicitly labelled column**: a witness measured in a per-module witness graph this
run did build, over one primary artifact per JAR-producing module — 38 artifacts,
130,718,491 bytes, built in 1 h 42 m 30 s at a 71.3 GiB peak RSS under JDK 21,
418,777,229 bytes, sha256 `8d3462b7…`, loaded with `importCpg` reporting **994,192
methods**, 97,292 type declarations and 45,680 files. In it **all 38 witnesses are
present, including all seven**, so the seven's absence from the graph at the
sanctioned path is established to be an input-set consequence and nothing else. That
column is a frontend-capability measurement and is never allowed to stand in for a
verdict against the graph any runner loads; what stays open is a coverage verdict for
the seven against a graph built over every JAR the build produced, which does not
exist. **Zero** verdicts rest on a shared package prefix and **zero** winner maps are
claimed.

### The graph's byte size and sha256, and the identity re-verified before every load

The identity of record for the graph at the sanctioned path, as every stage of this
run measured it before its own load, and re-measured by
`harness/artifacts/logs/cpg-verify.log` after both of its loads. It is **not** owned
by `cpg-frontend.log`: that file is now this run's own frontend invocation and the
only identity it records is the rejected partial write's (D1). Per **D4** the bytes
on disk at this checkpoint are a third pair, **541,309,809** /
`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`, which no load in
this run read:

| Field | Value |
| --- | --- |
| Name the plan gives it | `harness/cpg/spark.cpg` — a **33-byte symlink** |
| Name the environment exports | `$HARNESS_CPG`, which `harness/env.sh` line 28 defaults to that same path |
| Both resolve to | `/opt/blitzy-harness/cpg/spark.cpg` |
| Byte size | **541,255,894** (measured with the symlink **followed**) |
| sha256 | **`26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`** |
| Same-file proof | equal `dev:inode` `1048752:37891488` through **both** names — stronger than equal size or equal digest |

The 33-byte no-follow reading is recorded in that log and explicitly discarded: it
is the length of the target path string, and a record carrying 33 would describe
nothing at all.

**Identity re-verified before every load, each check logged:**

| Load | Where the check is logged | Result |
| --- | --- | --- |
| The `importCpg` verification load | `cpg-verify.log` STEP 4, and again at STEP 11 after the load | match on byte size, sha256 **and** `dev:inode`; the persisted graph byte-identical afterwards, same size, digest, inode and mtime |
| The Stage 3 Joern runner | `joern.status` fields `graph_bytes_observed_before_load` / `graph_sha256_observed_before_load`, with the comparison in `joern.preflight.log` PART 2 (line 147, verdict line 167); checked `2026-08-24T22:38:33Z` and again immediately before the load at `22:41:02Z` | match on both values; `graph_identity_halt_triggered=no` |
| Probe query 01 | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` | match on both values against the pair `cpg-frontend.log` then carried, now carried by `cpg-verify.log` and by this section |
| Probe query 02 | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` | match on both values against the same pair |
| Probe query 03 | `queries/joern/results/03-parameterized-handler-sink-pairs.json` | match on both values **against the record of account for the bytes it read**, which is a different pair — see below |

**The graph was replaced on the host between loads, and that is recorded rather
than reconciled away.** Queries 01 and 02, the verification load and the Stage 3
runner all read 541,255,894 bytes / `26d327cc…`. Query 03 read **548,118,435 bytes
/ `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53`**, reporting
methods 1,399,866, type declarations 119,920 and files 45,037. Measured for this
record at the time, `harness/cpg/spark.cpg` resolved to a file of **548,118,435
bytes, sha256 `f8c71562…`, mtime `2026-08-25 20:19:28Z`** — the second pair. **At
this checkpoint it resolves to a third: 541,309,809 bytes, sha256
`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`**, which no load
in this run read, so no counts of this run's are reported for it — its own
provisioning record of account states them and **D4** carries them with that
provenance. All three pairs are
kept with their provenance under **D4**, which also states why the replacement is a
halt-class record contradiction rather than a tolerated difference. The resolved
path is host-global and shared read-only with concurrent clones, and this run
neither rebuilt nor replaced it, so the replacement happened outside this run.
Every pair is kept with its provenance and none is discarded; **every method count
measured on this path clears the one-sided floor**; and every load was verified
against the record of account for the bytes it was about to read, before reading
them.

**The gate is a program, not a convention, and it is the only committed execution
path for Stage 3.** `harness/lib/preflight_graph_identity.py` resolves the record
of account by **provenance** — the in-checkout `cpg-frontend.log` when it carries a
write-time `bytes:`/`sha256:` pair, and otherwise the provisioning record beside the
resolved graph (`cpg-identity.txt`, corroborated by `cpg-record.txt`) — refuses more
than one distinct pair in any record, refuses two records that disagree with each
other, recomputes both values from the bytes on disk with the symlink **followed**,
and exits **77** before the runner is invoked on any mismatch. Its binding caller
`harness/lib/run-joern-gated.sh` has no branch that reaches the runner after a
non-zero gate. Two records preserve its behaviour: `joern-preflight.log`, the gate's
own report, and `joern-preflight-negative-test.log`, which drives the **wrapper** and
records that the runner produced no output and left its artifact untouched when the
gate refused — both written in the lane that built its own graph, so their subject
identity is that lane's graph (**D14**). Re-run in `--check-only` form at this
checkpoint, which writes nothing, the gate reports **PASS**: it resolves
`/opt/blitzy-harness/provision-log/cpg-identity.txt` as the record of account,
finds `cpg-record.txt` agreeing with it, and measures the bytes on disk equal to
both — so a later Stage 3 invocation would be gated on a pair that describes the
bytes it would read, while this run's own counts stay attributed to the pair each
load actually read.

**The re-verification gate is demonstrably real rather than decorative.** Query
02's reproduction check was **attempted and halted**: it compiled, ran, re-measured
JDK major 21 and its heap, then measured the resolved target as 548,118,435 bytes
/ `f8c71562…`, reported that as **not** matching the identity of record it verifies
against, printed its failure marker and emitted **no result region and no
envelope**. A load against different bytes than the record describes was refused
rather than weakened (`joern-probe.md`, "How the graph was loaded").

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

| Field | Value | Source |
| --- | --- | --- |
| Command | `SL_LOGGING_LEVEL=WARN jimple2cpg /opt/blitzy-harness/cpg-input -o /opt/blitzy-harness/cpg/spark.cpg --recurse -J-Xmx64g < /dev/null` | `harness/ENVIRONMENT.md:292-294` |
| Input | **62** JARs, 273 MB, from **31** modules; 190 of 234 JARs found were excluded with a per-file reason, including **34 `-tests` and 2 `spark-connect-shims` archives excluded by runbook instruction** | `harness/ENVIRONMENT.md:296-302` |
| JDK major / heap / elapsed | **21**, `-J-Xmx64g`, peak sampled RSS 59.0 GB, 53 m 04 s (12:59:23Z → 13:52:27Z) | `harness/ENVIRONMENT.md:289-290` |

**That invocation was not performed by this run**, which is D1; and the difference between its 62-archive
input and this run's 191-artifact manifest is **D3**. The exclusion of `-tests` and shims archives there
is also the direct reason the AAP's complete-input requirement and the frontend's writer cannot both be
satisfied: the runbook's narrower set is producible precisely because it is narrower.

### 6.2 The post-frontend `importCpg` verification load — second of four

| Field | Value | Source |
| --- | --- | --- |
| Command | `JAVA_HOME=$JAVA_HOME_21 SL_LOGGING_LEVEL=WARN HARNESS_VERIFY_CPG=/opt/blitzy-harness/cpg/spark.cpg HARNESS_VERIFY_WITNESS=<witness table> joern --script <verify.sc> -J-Xmx64g < /dev/null` | `cpg-verify.log` STEP 9 |
| JDK major | **21** — Temurin 21.0.12.1+1, `java.version` 21.0.12.1 | `cpg-verify.log` STEP 7 |
| Heap actually used | **64 GiB (`-J-Xmx64g`)** — equal to the recorded minimum and default, so no separate proof for a larger value was owed; the 64 GiB pre-touch proof is recorded regardless | `cpg-verify.log` STEP 8, and the "RECORDED SEPARATELY" block at STEP 9 |
| Exit and elapsed | load 1 exit 0, 704.111 s; load 2 exit 0, 721.462 s | `cpg-verify.log` STATUS |
| Workspaces | `/tmp/blitzy-harness-scratch/0/cpg-verify` and `…/cpg-verify-descriptors` — each **proved absent before use**, outside the checkout, neither reused nor cleared | `cpg-verify.log` STEP 5 |
| Load mechanism | `importCpg`, and the only load call either script makes | `cpg-verify.log` STEP 10 |

Load 2 is a measurement rather than a retry: it enumerates the
`META-INF/maven/**/pom.properties` file nodes the weaker coverage witness depends
on, and re-derives the three counts as a by-product.

### 6.3 The Stage 3 Joern runner — third of four

| Field | Value | Source |
| --- | --- | --- |
| Command | `JAVA_HOME="$JAVA_HOME_21" SL_LOGGING_LEVEL="${SL_LOGGING_LEVEL:-WARN}" HARNESS_SCAN_CPG="$CPG_REAL" HARNESS_SCAN_OUT="$ART" HARNESS_SCAN_BOUND="$BOUND" joern --script "$SCRIPT" -J-Xmx"$HARNESS_JOERN_HEAP" < /dev/null` | `joern.status` field `command` |
| JDK major | **21** — parsed from `java.specification.version` rather than read off the banner; `/opt/blitzy-tools/jdk/jdk-21.0.12.1+1`, VM `21.0.12.1+1-LTS` | `joern.status` fields `jdk_major`, `jdk_major_source` |
| Heap actually used | **64g = 68,719,476,736 bytes**, resolved flag `-J-Xmx64g`, through `HARNESS_JOERN_HEAP` — the runner's own documented environment override applied at `harness/bin/run-joern.sh` line 70. **No raise required and none made**; no runner file and no baked flag was changed | `joern.status` fields `heap_used`, `heap_mechanism`, `heap_raise_made` |
| Exit and elapsed | exit **0**, 1,074 s | `joern.status` |
| Working directory | `$HARNESS_SCRATCH_DIR/joern-run` — outside the checkout, because Joern writes a large `./workspace` into whatever directory it runs from | `runner-metadata.json`, tool `joern` |

### 6.4 The Stage 5 probe — fourth of four

| Field | Value | Source |
| --- | --- | --- |
| Command, per query | `JAVA_HOME="$JAVA_HOME_21" JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script queries/joern/<nn>-<slug>.sc -J-Xmx64g < /dev/null`, invoked from a scratch directory outside the checkout | each query's envelope, `runtime.command` |
| JDK major | **21** for all three — `21.0.12.1+1-LTS`, and each envelope publishes `jdk_major_required` 21 beside it | `queries/joern/results/*.json`, `runtime.jdk_major` |
| Heap actually used | **68,719,476,736 bytes = 64 GiB** for all three, **measured from inside the child JVM** rather than taken from the flag: `joern --script` forks a child to which `-J-Xmx` does not propagate, so each query measures its own heap and halts below the floor rather than trusting the flag it was given | `queries/joern/results/*.json`, `runtime.heap_actually_used_bytes`, `runtime.heap_override_mechanism` |
| Relative to the floor | **at** the floor, not above it, so no additional pre-touch proof was owed beyond the gate's | `runtime.heap_at_or_above_floor`, `runtime.heap_pre_touch_proof` |
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
| 1 | the mandated A/B: one pinned rule, the anchor file, taint on vs off | `taint-ab-anchor-diskstore-on.sarif`, `taint-ab-anchor-diskstore-off.sarif` | `taint-ab-anchor-diskstore-on.log`, `taint-ab-anchor-diskstore-off.log`, and the analysis in `taint-ab-on.log` / `taint-ab-off.log` |
| 2 | the same A/B with the **entire** ruleset loaded, so the outcome cannot be an artefact of a one-rule invocation | `taint-ab-anchor-diskstore-fullruleset-on.sarif`, `…-off.sarif` | `taint-ab-anchor-diskstore-fullruleset-on.log`, `…-off.log` |
| 3 | is the taint engine active on Spark's own Scala at all — same rule, one variable, a different subject | `taint-ab-hiveshim-on.sarif`, `taint-ab-hiveshim-off.sarif` | `taint-ab-hiveshim-on.log`, `taint-ab-hiveshim-off.log` |
| 4 | two controls on the anchor: the same patterns without taint mode, and the taint rule with its source removed | `taint-ab-search-control.sarif`, `taint-ab-source-removed-control.sarif` | rule texts verbatim in `taint-ab-off-control-rule.txt`, `taint-ab-source-removed-control-rule.txt` |

### 7.1 The mandated A/B — the pass condition, and it failed

| | Expected | Observed |
| --- | --- | --- |
| Taint **on** (`--taint-intrafile`) | 1 traced finding at `core/src/main/scala/org/apache/spark/storage/DiskStore.scala` line 72 | **1** finding at line 72 with a 2-step dataflow trace — exit 0, 3 s |
| Taint **off** (the control) | **0** findings | **1** finding at line 72 with the same 2-step trace — exit 0, 3 s |
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
| Taint **on** | **1** finding at `DiskStore.scala` line 72, traced — exit 0, 72 s |
| Taint **off** | **1** finding at the same line, traced — exit 0 |
| The two arms' artifacts | **byte-identical**: 2,939,276 bytes each, sha256 `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |

A one-rule invocation could in principle miss a taint-only finding another rule
would have produced. With every rule directory loaded the two arms are still byte
for byte the same file, so the non-discrimination is a property of this subject
rather than of the rule selection.

**The mechanical reason, measured rather than speculated.** The rule's source is a
method parameter and its sink is the interpolated string, and on this file both sit
inside **one method**: the parameter is declared at line 64 (`def put(blockId:
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
| Taint **on** | **2** findings, lines **828** and **834**, each carrying a **5-step** dataflow trace — exit 0, 4 s, 10,021 bytes, sha256 `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| Taint **off** | **0** findings — exit 0, 3 s, 2,341 bytes, sha256 `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |
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

### 7.4 Two controls on the anchor file, and what each excludes

| Control | Rule change | Observed | What it excludes |
| --- | --- | --- | --- |
| Search-mode | the same patterns with `mode: taint` **removed** (`taint-ab-off-control-rule.txt`) | **2** findings, `DiskStore.scala` lines **72** and **215**, **no** `codeFlows` — 4,424 bytes, sha256 `272a530fea4ef95417cd539b5964a70f6805e5def72ab58264cf73dbbbdb8ceb` | that the taint rule's line-72 result is just a pattern match: the pattern alone matches a **second** site the taint rule never reports |
| Source-removed | `mode: taint` kept, `pattern-sources` replaced with an unmatchable marker (`taint-ab-source-removed-control-rule.txt`) | **0** findings — 2,347 bytes, sha256 `e98c1e1fb37c66cbf7dac92838485314b57a4561a41a6d15d9043eebbaac745f` | that the line-72 result is source-independent: remove the source and it disappears, so it is genuinely source-driven |

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

**One naming note, because a reader pairs a log with the artifact beside it.** Two
lanes measured this A/B and both wrote a `--sarif-output taint-ab-on.sarif`, so the
base names carried two different subjects. The discriminating arms' artifacts are
therefore published as `taint-ab-hiveshim-{on,off}.sarif` — the name states the
subject it measured — and each carries its own arm log under the matching name.
`taint-ab-on.log` and `taint-ab-off.log` are the narrative analysis of the
**mandated** A/B in §7.1.

**Every arm log states the `--sarif-output` filename as it ran in its own clone, and
those names are left exactly as written** — editing a command record to match a
later filename would falsify the command. The reconciliation is by digest instead,
which each log carries on its own `sarif sha256` line, and every one of them resolves
to a file published here:

| A log says it wrote | with sha256 | published here as |
| --- | --- | --- |
| `taint-ab-anchor-diskstore-{on,off}.sarif` | `7949617b…` | the same names — 4,753 bytes each |
| `taint-ab-{on,off}.sarif` (the `-hiveshim-` arm logs) | `1a6c9a57…` / `6669ca2c…` | `taint-ab-hiveshim-{on,off}.sarif`, byte-identical across the rename |
| `taint-ab-{on,off}.sarif` (the §7.1 narrative logs) | `7949617b…` | `taint-ab-anchor-diskstore-{on,off}.sarif`, which carries that digest |

Two directory listings captured inside other lanes' logs — in `normalize-run.json`
and `osv-scanner.stdout.log` — name `taint-ab-on.sarif` because that is what those
trees held when the listing was taken. They are captures rather than citations, and
they are left verbatim for the same reason.

---

## 8. The nine runners — target variable and path base

Every one of the nine was invoked **directly, individually, with no arguments**,
and **no orchestrator was used**; `harness/bin/` contains no orchestrator to have
used (§1). Source for this table: `harness/artifacts/logs/runner-metadata.json`,
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
| `joern` | `SPARK_SRC` | `/opt/spark-src` | verified | **bytecode class** — base *kind* recorded, base *value* deliberately **not invented**, since no filesystem base exists for a bytecode coordinate | `joern.json` | 0 |

**No runner has "none" for its target variable**: all nine read `SPARK_SRC`, all
nine resolved `/opt/spark-src`, and `resolved_scan_root_verified` is true for every
one — so the targeting halt condition (a runner resolving a tree other than
`SPARK_SRC`) was not engaged anywhere.

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
`2026-08-25T04:10:15Z → 04:10:18Z`, **exit 0**, `halt` null. The normalizer uses
the standard library only, so it runs on the base interpreter independently of any
scanner's virtualenv.

| Indexed value | Figure |
| --- | --- |
| Artifacts routed | **9** — 8 present, 1 absent; every one routed by **detected shape**, never by filename |
| Dataset rows | **9,433** |
| Raw finding records traversed | **10,018**, by a traversal that walks the count units and **builds no rows** |
| Rejected records | **585**, all under the single named class `unresolvable_path` |
| Dataset-level reconciliation | `10018 = 9433 + 585` — **pass**, and every per-artifact identity held individually |
| Parsed `findings.json` rows against the dataset | 9,433 against 9,433 — pass |
| Parsed `findings.csv` rows against the dataset | 9,433 against 9,433 — pass, asserted **separately** rather than inferred from the JSON |
| Parsed JSON rows against parsed CSV rows | 9,433 against 9,433 — pass, as a third assertion |
| Typed field-for-field comparison | **9,433 rows / 113,196 fields**, `first_mismatch` null |
| Row validation | all 9,433 rows carry exactly the twelve fields in order; `path` absent **0**, `severity_norm` absent **0**, absolute paths **0**; absence appears only in `cve` (9,433), `cwe` (8,674), `package_coordinate` (9,433), `severity_native` (7) and `start_line` (3) |
| Parse status | `clean` ×7, `partial` ×1 (`joern`, 692 raw records → 107 rows, 585 rejected), `absent` ×1 (`osv-scanner`) |
| `osv-scanner`'s reconciliation | the literal **`not applicable — artifact absent`**, not a zero-equals-zero pass |
| Output files | `findings.json` 4,411,501 bytes, sha256 `a1ef544c45faad1ca0592b4c77cb4b43028308c84342e79445f5d0b7e0c3f358`; `findings.csv` 2,083,744 bytes, sha256 `895cf6a887dcbc16f565cef88e4882ff4cba5ed2ffcedfff94dde2c6a81088e7` — both re-measured for this record, and both agreeing with `harness/artifacts/logs/findings-publication.json`, the manifest the normalizer wrote beside them |

**Row counts are parsed, never counted as physical lines.** Both files were parsed
to obtain every figure above; a message field carrying an embedded newline makes a
line count over-report, which is the method AAP §0.5.4 prohibits.

### The non-filesystem path count and proportion

From `normalize-run.json` `totals.path_kinds`:

| Path kind | Rows |
| --- | --- |
| `tree_file` | 9,326 |
| `bytecode_source` | 107 |
| `outside_root` | 0 |
| `archive_member` | 0 |
| **Non-filesystem total** | **0 of 9,433 — proportion 0.0** |

No row in this dataset names an archive member, a container outside the root or any
other non-filesystem coordinate, so the serialization those forms would have taken
was not exercised. `in_scope` is false on **29** rows, all of them `joern`'s, and
those rows are **kept** and counted; every other tool's rows are in scope.

### The adapter and reconciliation tests

Source: `harness/artifacts/logs/adapter-tests-run.json`. Command
`/usr/bin/python3 -m unittest discover -s oss-scan-results/adapter-tests -p 'test_*.py' -v`
from the repository root, on the standard library's `unittest` — no third-party
runner, no plugin and no install step. **1134 tests and 27,087 subTests, 0
failures, 0 errors, 0 skipped, 0 expected failures, 0 unexpected successes, result
`OK`, exit 0**, 7.026 s as `unittest` reported it and 7,400 ms wall. The zero
skip, expected-failure and unexpected-success counters are reported rather than
omitted, so a green result cannot have been obtained by excusing a test.

The committed tree holds `README.md`, **10 test modules, 103 fixtures and 103
expected-row files**, of which **72 are negative fixtures** cross-checked against
the nine rejection conditions AAP §0.5.4 enumerates, so that **every rejection
condition each exercised adapter can produce is fixture-backed** — for all six
exercised adapters, with the constructed-record assertions kept beside the fixtures
rather than in place of them. The corpus also carries the documents that reach an
already-covered class by a different route: a wrong `file_line_range` container
against a wrong first element, an empty message against an absent one, a zero line
against a boolean one. Which fixture came from which artifact, and what each module
asserts, is owned by `oss-scan-results/adapter-tests/README.md`.

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
| Query revisions committed | 1, 1, 1 on the counting convention the report states |
| Distinct Joern API constructs | 28, 42 and 28, each computed from a published deduplicated list, with a probe-wide union of **46** |
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

---

## 11. Deliverable inventory with resolved absolute paths

Resolved against the repository root as it stood when this file was written:
`/tmp/blitzy/blitzy-spark/blitzy-f38258d3-f87d-44f5-bedc-af512c69e0ab_a424a0`
(written `<repo>` below for the absolute column's readability; every path is that
root followed by the repository-relative path in the first column).

### The eight result-deliverable categories under `oss-scan-results/`

| Deliverable | Absolute path | State |
| --- | --- | --- |
| `oss-scan-results/findings.json` | `<repo>/oss-scan-results/findings.json` | present, 4,411,501 bytes |
| `oss-scan-results/findings.csv` | `<repo>/oss-scan-results/findings.csv` | present, 2,083,744 bytes |
| `oss-scan-results/severity-map.md` | `<repo>/oss-scan-results/severity-map.md` | present |
| `oss-scan-results/tool-status.md` | `<repo>/oss-scan-results/tool-status.md` | present |
| `oss-scan-results/build-record.md` | `<repo>/oss-scan-results/build-record.md` | present |
| `oss-scan-results/joern-probe.md` | `<repo>/oss-scan-results/joern-probe.md` | present |
| `oss-scan-results/run-record.md` | `<repo>/oss-scan-results/run-record.md` | **this file** |
| `oss-scan-results/adapter-tests/` | `<repo>/oss-scan-results/adapter-tests/` | present — `README.md`, 8 `test_*.py`, `fixtures/` (52), `expected/` (52) |

### The three deliverable trees

| Tree | Absolute path | State |
| --- | --- | --- |
| `queries/joern/` | `<repo>/queries/joern/` | present — 3 `.sc`, `results/` with 6 files, `.workspace/.gitignore` |
| `harness/artifacts/raw/` | `<repo>/harness/artifacts/raw/` | present — **8** artifacts, one per tool that wrote one, and nothing else |
| `harness/artifacts/logs/` | `<repo>/harness/artifacts/logs/` | present — **46** files |

### Scope, staging, graph and normalizer

| Path | Absolute path | State |
| --- | --- | --- |
| `harness/scope/allowlist.txt` | `<repo>/harness/scope/allowlist.txt` | present, 343 bytes, 12 globs, sha256 `0013edf6…4143d1`, left exactly as found |
| `harness/cpg/spark.cpg` | `<repo>/harness/cpg/spark.cpg` | present — a 33-byte symlink resolving to `/opt/blitzy-harness/cpg/spark.cpg` |
| `harness/lib/normalize/` | `<repo>/harness/lib/normalize/` | present — 6 modules plus `adapters/` |
| The frontend staging directory | `<repo>/harness/artifacts/cpg-input` | **present**, 191 staged artifacts, 431,184,822 bytes. Proved absent before use, created by this run's inventory lane, never cleared, and supplied to this run's frontend invocation in full. Its complete ordered manifest — 191 entries with names, sizes and digests, and the bidirectional assertion — is published in `harness/artifacts/logs/cpg-input-inventory.json`. Excluded from git collection by `.gitignore:31`, which is why the manifest rather than the tree is the deliverable |
| This run's frontend output path | `<scratch>/cpg/spark.cpg.PARTIAL-TRUNCATED-DO-NOT-LOAD` | **present as evidence and explicitly not accepted**: 691,541,019 bytes, sha256 `b1559c930a7b9ced717a0babf9a7e172d2b93d2cdef45a959304f063aedfe408`, the truncated write left by the serialization failure in D1. Renamed to make loading it impossible by accident, never linked at `harness/cpg/spark.cpg`, and loaded by nothing |
| The per-module witness graph and its input | `<scratch>/cpg/witness.cpg` and `<scratch>/witness-input` | built by this run over **one primary artifact per JAR-producing module** — 38 artifacts, 130,718,491 bytes — to establish what per-module coverage is measurable given D1. **A labelled frontend-capability measurement**: not the graph the AAP mandates, not at the sanctioned path, loaded by no runner, contributing no dataset row. Recorded in `cpg-frontend.log` PART 2 and `cpg-verify.log` PART 2, with the verdicts owned by `build-record.md` §6 |
| The `importCpg` verification workspaces | `/tmp/blitzy-harness-scratch/0/cpg-verify` and `/tmp/blitzy-harness-scratch/0/cpg-verify-descriptors` | outside the checkout, each proved absent before use, neither reused nor cleared by this run — and **neither on disk now**, stated rather than implied. `cpg-verify.log` STEP 5 and STEP 11 preserve their names, their absence-before-use proof and the 2.9 GB working copy one of them held |
| The provisioning frontend's input path | `/opt/blitzy-harness/cpg-input` | present on the host, 62 archives — the input set of the graph actually loaded (divergence D3) |

Nothing was torn down: no cleanup, no reset and no temp purging (AAP §0.8.1). The
two verification workspaces that are gone were not removed by this run, and the
truncated partial write was renamed rather than deleted so that the failure it
evidences stays checkable.

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

| Event | What it was | Disposition |
| --- | --- | --- |
| `gitleaks` exit **2** and `checkov` exit **1** | Non-zero because each found something. Both wrote an artifact and both parse | Ordinary. Artifact status and exit status are independent; the exit code is recorded as a fact and used for nothing else |
| `osv-scanner` exit **128**, **no artifact written** | The tool stated its own reason: `No package sources found, --help for usage information.`, quoted verbatim in its `tool-status.md` entry | **Completion with nothing in scope to work on**, not a failure. Zero rows, reconciliation `not applicable — artifact absent`, run continues. The missing-artifact halt was not engaged, because the absence came with the tool's own stated reason |
| `joern` artifact **partial** | 692 raw records, 107 rows, **585** records rejected under the single named class `unresolvable_path` | Partial parse is a first-class outcome: every parsable record emitted, every rejection counted under its class |
| The **taint A/B** | Non-discriminating on the mandated subject file: 1 finding at line 72 in **both** arms, byte-identical artifacts — and still byte-identical with the whole ruleset loaded, while the same rule discriminates 2 against 0 on `HiveShim.scala` | **Halt-class finding, reported and not repaired** — [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured), divergence D2 in [§13](#13-divergence-register) |
| The **frontend build**, as provisioning left it | The graph on disk was written by the provisioning invocation before this run's first command | **Halt-class finding, reported and not repaired** — divergence D1 in [§13](#13-divergence-register) |
| The **frontend build this run performed** | Invoked over the complete 191-artifact asserted manifest under JDK 21 at a proven-committable 128 GiB heap. Ran **8 h 01 m** to a **113.3 GiB** peak RSS, completed extraction and every AST pass, then terminated **in persistence** with exit **1** and `java.lang.OutOfMemoryError: Required array length 2147483639 + 72 is too large` in `flatgraph.storage.WriterContext.finish`. It produced **no graph**; the 691,541,019-byte truncated partial write is preserved as evidence and explicitly not accepted | **Halt-class finding, reported and not repaired** — divergence D1. The bound is a fixed array length on the one buffer flatgraph serializes the whole string pool through, proved from that method's bytecode in `cpg-frontend.log` STEP 8, so no heap moves it; STEP 10 enumerates every mitigation examined, and the only effective one — excluding inputs — is prohibited by AAP §0.5.1 and §0.9.2. **Nothing was trimmed to obtain a graph** |
| The graph at the sanctioned path **replaced again**, after this run's last load | The path resolves to a third identity, 541,309,809 / `4616845a…`, which no load in this run read, and which contradicts the identity `harness/ENVIRONMENT.md:284-285` states | **Halt-class record contradiction, reported and not repaired** — divergence D4. No load read mismatched bytes; the contradiction is between the provisioned record and the disk |
| Probe query 02's **reproduction check** | Attempted, and **halted** on a graph-identity mismatch, emitting no result region and no envelope | Working as designed: a load against different bytes than the record describes was refused rather than weakened |
| Anything else | No tool crashed, no artifact matched an unknown shape (`failed` never occurred), no reconciliation identity failed, no adapter fixture, rejection or reconciliation test failed, and no runner resolved a tree other than `SPARK_SRC` | — |

---

## 13. Divergence register

Every divergence with **both the expected and the observed value** (AAP §0.9.4).
Two are halt-class findings reported and not repaired; the rest are recorded
differences that do not stop the run (AAP §0.9.3).

### D1 — halt-class: the graph was not created by this run; a current-run graph was attempted and is blocked by a fixed toolchain bound

| Field | Value |
| --- | --- |
| Expected | AAP §0.1.1 and §0.5.1 — this run invokes the frontend over its own staged input set and writes the graph; *a graph already on disk is never accepted as this run's output* |
| Observed | The graph at `/opt/blitzy-harness/cpg/spark.cpg` was written by the provisioning invocation, before this run's first command. It remains the graph at that path |
| What was attempted, rather than deferred | This run assembled its complete input manifest — **191** own artifacts, **431,184,822** bytes, from all 38 JAR-packaging projects — asserted it total and injective in both directions and logged the assertion **before** invoking anything, proved a **128 GiB** heap committable with `-Xms`/`-Xmx`/`+AlwaysPreTouch`, and invoked the pinned `jimple2cpg` under JDK major 21 over the whole of it with `--recurse` and no exclusion of any kind |
| Outcome of the attempt | After **8 h 01 m** (28,863 s) and a **113.3 GiB** peak RSS, the frontend completed extraction and all AST passes and then terminated **in its persistence step** with `java.lang.OutOfMemoryError: Required array length 2147483639 + 72 is too large` raised inside `flatgraph.storage.WriterContext.finish` (`Serialization.scala:176`). **No graph was produced.** The truncated partial write it left — 691,541,019 bytes, sha256 `b1559c930a7b9ced717a0babf9a7e172d2b93d2cdef45a959304f063aedfe408` — is preserved as evidence in the run's private scratch under the name `spark.cpg.PARTIAL-TRUNCATED-DO-NOT-LOAD`, was never linked at `harness/cpg/spark.cpg`, and was loaded by nothing |
| Why the attempt cannot succeed at this input breadth | Established from the failing method's own bytecode, not inferred: `finish$$anonfun$2(ByteArrayOutputStream, IntBuffer, String)` UTF-8 encodes every string in the graph's deduplicated `stringpool` and appends it to **one** `java.io.ByteArrayOutputStream`, whose backing array cannot exceed `Integer.MAX_VALUE - 8` = 2,147,483,639 elements. The bound is on one array's length, so **no heap size moves it** — 128 GiB was committed and peak RSS was 113 GiB when it failed |
| Mitigations examined, and why each is unavailable | Checked against the frontend's actual flag surface, reproduced verbatim in `cpg-frontend.log` STEP 10. **Raising the heap** — irrelevant to a fixed array bound. **`--exclude` / `--exclude-regex`, or dropping pre-shade, `-tests` or shims artifacts** — the one lever that would work, and prohibited: AAP §0.5.1 requires every JAR retained by name and §0.9.2 lists trimming the input set among the conditions that stop the run. **Dropping `--recurse` or bounding `--depth`** — same class, and `--recurse` is mandated. **A newer frontend or flatgraph whose writer chunks the pool** — prohibited installation/substitution under §0.8.1. **Splitting the input and merging** — no merge exists in the pinned distribution, and a merged graph would carry the same string pool. **Building without persisting** — the plan requires the graph persisted, its identity recorded, and re-verified before every load, and the runners resolve it from disk by path |
| Disposition | Reported, not repaired, and nothing was trimmed to obtain a graph. The input set the AAP mandates and the writer the pinned frontend ships are not simultaneously satisfiable on any host, which makes this a property of the toolchain at this input breadth rather than of this host or this run |
| What it does not compromise | Delivery of every JAR the build produced is proven by the staging manifest independently of any graph. The identity in [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) is the identity of exactly the bytes each earlier stage loaded, measured from the file itself before each load |
| What it does compromise, stated plainly | There is no current-run graph, so no current-run method, type-declaration or file count exists, and seven of the 38 JAR-producing modules have no coverage verdict obtainable from the graph at the sanctioned path. `build-record.md` §6 carries a second, explicitly labelled column measured in a per-module witness graph this run did build, and never lets it stand in for the first |
| Owner | `harness/artifacts/logs/cpg-frontend.log` — the whole file is this invocation's record |

### D2 — halt-class: the taint A/B did not discriminate

| Field | Value |
| --- | --- |
| Expected | one traced finding at `DiskStore.scala` line 72 with taint on and **zero** with it off, from two invocations differing only in that setting |
| Observed | **1** at line 72 with the flag and **1** at line 72 without it, both traced, artifacts byte-identical at 4,753 bytes / sha256 `7949617b…5778` |
| Disposition | reported and not repaired; nothing was retried, narrowed or re-flagged to obtain the expected zero. [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured) carries the mechanical reason and the engine limit |
| What it is not | evidence that the engine is inert. §7.3 measures the same rule discriminating **2 findings against 0** on `sql/hive/src/main/scala/org/apache/spark/sql/hive/client/HiveShim.scala` with the same one flag, and §7.4's two controls show the anchor's line-72 result is source-driven rather than a pattern match. The engine is active; this subject cannot show it, for the reason §7.2 measures |
| Owner | `harness/artifacts/logs/taint-ab-off.log`, divergence D1 in that file; the arms themselves are the `taint-ab-anchor-diskstore-` pair |

### D3 — the graph's input set is narrower than the build produced

| Field | Value |
| --- | --- |
| Expected | the graph built over **every** JAR the build produced, nothing trimmed: this run's inventory staged **191** own artifacts, **431,184,822** bytes, from all **38** JAR-packaging projects, and proved the mapping total and injective in both directions |
| Observed | the loaded graph's input path held **62** archives, 285,122,375 bytes, from **31** modules, with its own manifest recording 190 files excluded and a per-file reason for each |
| Consequence, stated so no count is misread | seven of the 38 JAR-producing modules therefore have **no coverage verdict obtainable** from this graph, and no finding on it can resolve into a `src/test` tree, every `-tests` archive being absent from it. A graph over the wider set cannot have *fewer* methods than one over the narrower, which is why the method count is a one-sided floor rather than a window |
| Disposition | recorded with both values; **neither input set was trimmed or padded and no count was adjusted to make the two agree**. The wider set was not merely inventoried — it was supplied to the frontend in full, and D1 records what happened when it was |
| Owner | `cpg-frontend.log`, with the coverage consequence measured in `cpg-verify.log` and the verdict owned by `build-record.md` §6 |

### D4 — the graph on the host was replaced between this run's loads, and again after them

| Field | Value |
| --- | --- |
| Expected | one graph, one identity, for every load of the run |
| Observed | **three** identities across one path. The verification load, the Stage 3 runner and probe queries 01 and 02 read **541,255,894 bytes / `26d327cc…`** (methods 1,397,339, type declarations 119,691, files 45,037); probe query 03 read **548,118,435 bytes / `f8c71562…`** (methods 1,399,866, type declarations 119,920, files 45,037); and at this checkpoint the path resolves to a **third** pair, **541,309,809 bytes / `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`**, which no load in this run read |
| Disposition | all **three** pairs kept with their provenance, none discarded. Every load was verified against the record of account for the bytes it read, before reading them, and each comparison matched; query 02's reproduction check **halted** on exactly this mismatch rather than loading mismatched bytes. Both loaded method counts clear the one-sided floor of 853,420; the third pair was never loaded, so it has no counts and none is invented for it. The file is host-shared and read-only to this run, which neither rebuilt nor replaced it |
| The carve-out that was relied on, and why it no longer holds | The gate classified the graph's size, digest and counts as **deliberately-replaced** fields, on the premise that this run would replace the graph — and on that premise AAP §0.1.3's exclusion of "outputs this run deliberately replaces" applies, and reading a replacement as a contradiction would halt the run for succeeding. **D1 records that this run did not replace the graph and, at this input breadth, cannot.** With the premise gone the carve-out does not reach these fields, and what remains is an inherited artifact whose recorded identity observation contradicts |
| Disposition under the rule as it actually applies | AAP §0.1.3's fourth case — the record states a field the expected-values table does not carry and observation contradicts it — and §0.9.2, which names both that case and "a graph whose byte size or sha256 differs from the values recorded at write time at any later load", make this **halt-class**. It is therefore reported with every value kept and none chosen. `harness/ENVIRONMENT.md:284-285` states 541,255,894 / `26d327cc…`; the disk holds 541,309,809 / `4616845a…`. Repair is not available: the file is host-global, shared with concurrent readers, and was not written by this run |
| Where the contradiction actually lies, measured for this record | **not** between the disk and every provisioned record — only between the disk and `harness/ENVIRONMENT.md` §7. The third pair has its own record of account beside the graph: `/opt/blitzy-harness/provision-log/cpg-identity.txt` states `541309809 4616845ab2b0…` on one line and `cpg-record.txt` (written 2026-08-30T19:33:42Z) states the same pair with its command, JDK 21, `-J-Xmx64g`, 50 m 42 s and `FRONTEND_EXIT=0`. The two agree with each other and with the bytes on disk; `harness/ENVIRONMENT.md` §7 is the record that does not, and it describes the earlier graph (12:59:23Z → 13:52:27Z). So this is a **stale inherited record**, not an unexplained replacement |
| The third pair's counts, from that record and labelled as its measurement rather than this run's | methods **1,396,899** (internal 1,307,112), type declarations **119,721**, files **45,037** — the provisioner's own `importCpg` verification load, with its deltas against the prior record tabulated there (−440 methods, +30 type declarations, files exact). They clear the one-sided floor of 853,420 as well. **No load in this run read these bytes**, so these counts are not restated as this run's in [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor); they are recorded here because a value that exists and is omitted is a value nothing downstream can check |
| What now adjudicates a later load of these bytes | `harness/lib/preflight_graph_identity.py`, which resolves the record of account by provenance — the in-checkout frontend log when it carries a write-time pair, and otherwise the provisioning record beside the resolved graph — reads exactly one pair, refuses two that disagree, and exits 77 before the Stage 3 runner on any mismatch. Re-run at this checkpoint it reports **PASS**: the bytes on disk are the bytes their own record of account describes |

### D5 — the six JAR producers the expected-values table does not name

| Field | Value |
| --- | --- |
| Expected | the table names **32** JAR producers, measured over the narrowed 33-project provisioning build |
| Observed | a full reactor packages **38**, so six are new to it: `tools`, `examples`, `connector/kafka-0-10-token-provider`, `connector/kafka-0-10`, `connector/kafka-0-10-sql`, `connector/kafka-0-10-assembly`. All six appear as `SUCCESS` in Maven's reactor summary and all six produced their own main artifact on disk |
| Disposition | **a recorded difference, never a halt.** The halt rule is one-directional (AAP §0.8.3): a module that produced a JAR in the rehearsal and produces none now stops the run; the reverse does not. The six legitimately entered this run's staged input set, and they are why the method count is checked as a floor rather than a window |
| Owner | `build-record.md` §3 |

### D6 — runtime topology as read, not as expected

Read from the provisioned runners at the gate and recorded in
`runner-metadata.json`; no expectation is asserted as a fact about this
provisioning.

| Field | What a reader would have expected | What was read |
| --- | --- | --- |
| `dependency-check`'s JDK | the harness precedent exported `JAVA_HOME_21`, and the AAP anticipated this tool on **21** | **17** — the runner's line 51 invokes it with `JAVA_HOME="$JAVA_HOME"`, which `harness/env.sh` line 47 sets to the 17 JDK, and its own header states *"Runs under JAVA_HOME (Temurin 17) — JDK 21 is reserved for Joern"*. Measured rather than inferred precisely because it is the opposite of the precedent |
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
| datadog SAST config sha256 | `e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7` | `4f397e81414f8e9469d20abc18c80c85c722e72b9f85b8bcf69dbe34b8fef6f1` (48 rulesets and 1,093 rules both **match**) | `datadog-static-analyzer` |
| Trivy vulnerability DB `UpdatedAt` | `2026-08-23T06:56:50Z` | `2026-08-24T06:55:32.451220873Z` | `trivy` |
| Trivy java DB `UpdatedAt` | `2026-08-23T01:05:59Z` | `2026-08-24T01:07:04.599776272Z` | `trivy` |
| Dependency-Check NVD API Last Modified | `2026-08-23T08:00:06-04` | `2026-08-24T08:00:04-04` | `dependency-check` |

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
**377** destinations match no top-level entry of any staged artifact and are
accounted for individually rather than rounded away: 12 staged artifacts carry 28
nested `.jar` test fixtures between them and `--recurse` descends into those, which
yields 350 junit-framework classes, 42 test-fixture classes, 6 nested `META-INF`
descriptors and 5 multi-release `module-info.class`. Neither figure is treated as
acceptable because a document expected another number.

**The limitation, stated rather than worked around: per-class provenance for
overwritten classes could not be established from this frontend's output.** The
frontend's directory walk is not ordered by this run, and its overwrite report
names the *destination* class rather than the JAR that supplied the surviving
definition. **No winner map is claimed anywhere**, and none is inferred from the
containment tables, which answer a different question and are labelled as such.
What is reproducible instead is the input set itself, fixed byte for byte by the
ordered staging manifest in `cpg-input-inventory.json`.

### D10 — the copied-dependency exclusion count

From `harness/artifacts/logs/cpg-input-inventory.json` `totals`: **627** JAR files
enumerated across the 40 projects → **191** own artifacts staged, **436** excluded,
**0** undecided (191 + 436 + 0 = 627). The exclusions are **422 copied runtime
dependencies** — the figure the inventory publishes as
`copied_dependency_exclusion_count` — and **14 test-resource fixtures**. The count
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

### D12 — the shims stub definitions displace the real ones in any graph containing the shims artifact

| Field | Value |
| --- | --- |
| How it was measured | By querying the graph for each class and reporting what is there — the route AAP §0.5.1 prescribes for a collision that bears on a conclusion — rather than by inferring a winner from the frontend's output, which is not possible (D9) |
| Observed | In the per-module witness graph, whose input **includes** the `spark-connect-shims` primary artifact, all eleven classes that artifact ships as client-only stubs carry stub-sized method counts: `SparkConf` 8, `SparkContext` 2, `rdd.RDD` 8, `api.java.JavaRDD` 2, and 2 to 4 each for `sql.ExperimentalMethods`, `sql.SparkSessionExtensions`, `sql.execution.QueryExecution`, `sql.internal.SessionState`, `sql.internal.SharedState`, `sql.sources.BaseRelation` and `sql.util.ExecutionListenerManager`. In the graph at the sanctioned path, whose input **excluded** both shims archives, the same classes carry 298, 1,100, 1,022 and 74 for the first four |
| What follows | In a graph that contains the shims artifact, the stub definitions win those eleven collisions and the real implementations are absent from the graph. Any query about those eleven classes against such a graph is answered by a stub |
| What does not follow | This is **not** a winner map and none is claimed. It states what the graph contains, not which archive the frontend read last, and it is measured for these eleven classes only |
| Disposition | **Recorded; the run does not stop and nothing is excluded on the strength of it.** AAP §0.5.1 requires every JAR retained by name — "not the connect-shims artifact" — and §0.9.2 lists trimming among the halt conditions, so the observation is reported rather than acted on. It corroborates the provisioning runbook's own instruction to exclude that archive, and it is a real, named cost of the input set the AAP mandates |
| Owner | `harness/artifacts/logs/cpg-verify.log` PART 2 STEP P5 for the counts; `build-record.md` §5 and §6 for the statement |

### D13 — UNRESOLVED CONFLICT: three runner-written logs are required to stay and required to go

| Field | Value |
| --- | --- |
| What the conflict is between | A review finding that names `harness/artifacts/logs/{datadog-static-analyzer.console.log, joern.preflight.log, joern.runner.console.log}` as outside this checkpoint's frozen file boundary and **requires their removal from the delivered artifact tree**, against three AAP rules that require the opposite |
| The AAP side, cited | **§0.8.1** — "Do not tear anything down. No cleanup, no reset, no temp purging. What the run built stays where it is." These are output the run built. **§0.1.1** — `logs/` "legitimately accumulates from Stage 1 onward", an explicit statement that its membership is not a fixed list; only `raw/` is runner-only-and-nothing-else-ever. **§0.9.4** — every number must name the file it came from and that file must exist |
| The specific evidence chain removal would break | `joern.preflight.log` is the cited evidence for the **Stage 3 graph-identity re-verification**, which is a mandated check: [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor)'s identity table cites it at PART 2 line 147 with the verdict at line 167, and `tool-status.md`'s `joern` entry rests on the same file. Deleting it, or moving it outside the deliverable tree, would leave a mandated check citing a file that is not there — trading a boundary defect for an evidence defect |
| What was done | The narrower half of the fix, which does not require breaking anything: all three are **out of the implementation change set** — `git ls-files` reports none of them, each having been removed from the index with `git rm --cached`, which leaves every byte on disk. [§16](#16-manifest-of-the-two-git-ignored-artifact-trees) then partitions `logs/` structurally, listing them under **"Runner-written accumulation, NOT planned deliverables"** rather than among the members the AAP's file map enumerates, with these citations repeated at the point of use |
| What was **not** done, and why it is recorded rather than decided | They were **not deleted from `harness/artifacts/logs/` and not relocated**, and they remain in §16's manifest. Removing them from the manifest while they sit in a git-ignored tree would make them invisible to every downstream check, which is strictly worse than a labelled boundary exception |
| Status | **UNRESOLVED CONFLICT — the finding stays open.** Listing these files is explicitly *not* authorisation for them, and this entry is not a case for keeping them; it records that the two instructions cannot both be honoured and that the run declined to break the evidence chain to satisfy the boundary. AAP §0.1.3's precedence puts the frozen plan above a finding's suggested resolution, which is why the plan's side was taken |
| The suggested workaround was tested and is closed | The obvious route — re-point the citation at a sanctioned file, then delete — does not open the path, because **AAP §0.8.1 prohibits both halves of the removal in one sentence**: "What the run built stays where it is, **and the manifest is how the two git-ignored trees are published**." Clause one forbids deleting these three; clause two forbids de-manifesting them. Re-pointing a citation changes neither clause |
| One correction that cuts against this entry's own §0.9.4 argument, recorded because it is true | The Stage 3 identity **fact** does not depend solely on `joern.preflight.log`. It is also carried by `joern.status`, which the AAP file map enumerates as `<tool>.status`: `graph_bytes_observed_before_load=541255894` at line 383, `graph_sha256_observed_before_load=26d327cc…` at line 388, and `graph_identity_halt_triggered=no` at line 375. What only `joern.preflight.log` carries is the **comparison verdict** — PART 2 line 147 with the verdict at line 167. So deleting it would orphan the verdict rather than the measurement, which is a weaker objection than this entry first made, and the §0.9.4 ground is correspondingly narrower than §0.8.1's. **§0.8.1 is the decisive ground and it stands on its own** |
| What a human must decide | Whether the frozen file boundary or AAP §0.8.1 governs. Nothing this run can do satisfies both. If the boundary is held to govern, the action is: delete the three, drop them from §16, and re-point §5's identity table and `tool-status.md`'s `joern` entry from the comparison verdict onto `joern.status`'s three fields above — accepting that the verdict line itself is then gone, and that the deletion contradicts §0.8.1 in terms |

---

### D14 — the evidence trees hold more than one execution lane, and what was aligned against what

| Field | Value |
| --- | --- |
| Expected | one execution lane: every per-tool artifact, status record and stream written by the same invocation, and one graph identity across the run |
| Observed | the two trees hold the **union** of several lanes. `raw/` is one coherent set — all **8** artifacts from a single lane, each digest re-measured for this record. `logs/` accumulated from more than one: the nine `<tool>.status` records describe that same lane's streams, while the streams themselves had been replaced by another clone's re-invocations of the same runners, whose own artifacts are **not** the eight in `raw/`. Measured before alignment: **6** status-named stream identities disagreed with the file beside them, and `gitleaks.status`'s own derivation — "gitleaks.stdout.log lines 3, 4 and 5 show the raw directory, log directory and allowlist" — was false of the stream on disk, whose lines 3 to 5 were per-directory invocation results |
| The rule applied | each tool's evidence must come from the lane whose artifact the dataset was normalized from. The dataset is derived from `raw/`, so `raw/` fixes the lineage and the status records and streams are aligned to it — not the reverse, because aligning the artifacts would mean re-normalizing the dataset from bytes no record describes |
| What was aligned | **12** stream files restored to that lineage: `checkov.stdout.log`, `datadog-static-analyzer.stdout.log`, `dependency-check.stdout.log`, `gitleaks.{stdout,stderr}.log`, `joern.{stdout,stderr}.log`, `opengrep.stdout.log`, `semgrep.{stdout,stderr}.log`, `trivy.stdout.log` and `osv-scanner.stderr.log`. The displaced bytes were the other clone's raw command output — they carry none of the runner header block `scope.sh` prints and no correction of their own — and they remain in that lane's own branch rather than being destroyed here. After alignment: **8** status-named stream identities equal, **6** status records independently re-stating their own artifact's digest and all 6 equal, and `gitleaks.status`'s derivation true again |
| What was **not** replaced, and why | `osv-scanner.stdout.log`, which is that lane's stream **plus** a substantive correction — the global-sequencing failure, recorded there in the tool's own record and cross-referenced from `tool-status.md` and [§8](#8-the-nine-runners--target-variable-and-path-base). Restoring the unqualified text would have deleted a finding to gain byte-level tidiness |
| The taint arms, which the same union had crossed | Two lanes measured the A/B and one reused the base file names, so `taint-ab-{on,off}.sarif` held a **different subject** from the `taint-ab-{on,off}.log` beside them. The artifacts are renamed to `taint-ab-hiveshim-{on,off}.sarif` — byte-identical, digests re-verified across the rename — and that measurement's own arm logs are restored beside them, so every name states the subject it measured. [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured) cites each figure to the file that carries it |
| What remains divergent and is not repairable here | The **graph** identities: two lanes built their own graphs in their own clones (791,927,027 and 605,687,359 bytes), neither is on disk here, and `cpg-frontend.log` is the record of the attempt that produced none (**D1**). `joern-preflight.log` and `joern-preflight-negative-test.log` are the gate's records **from the lane that built its own graph**, so their subject identity is that lane's graph rather than the bytes here; the gate itself resolves its record of account by provenance and, re-run at this checkpoint, adjudicates the bytes on disk against the provisioning record that describes them. The nine `<tool>.runner-console.log` files are the other clone's console captures, retained under §0.8.1 and listed in §16 |
| Disposition | **recorded, with the chain that is coherent stated exactly**: `raw/` → the twelve-field dataset → the per-tool reconciliation identity, every step re-measured for this record. A figure taken from a stream or a status record is a figure about the lane `raw/` came from; a figure about a graph is a figure about the pair its load actually read, which is why [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) and **D4** keep all three pairs with their provenance rather than presenting one |

---

## 14. Values that could not be established

Named rather than omitted, because a value missing from the record is a value
nothing downstream can check (AAP §0.9.4). Each is owned by the document that
tried to establish it; this section indexes them.

| Value | Named in |
| --- | --- |
| The **cause** of the graph's above-anchor counts — measured composition is reported, a cause is not guessed | `cpg-verify.log` PHASE 2 and D3 there; `build-record.md` §7 |
| **Per-class provenance** for every overwritten class, and therefore any winner map | `cpg-frontend.log` STEP 11; `build-record.md` §5 |
| A **coverage verdict for seven modules against a graph built over every JAR the build produced** — each named with the witness tried and the query run. Partially addressed and explicitly not closed: all seven witnesses ARE present in the per-module witness graph this run built, which establishes the absence as an input-set consequence, but no graph over the complete set exists to verdict them against (D1) | `build-record.md` §6, both columns |
| An **injective coverage witness** for eight modules against this complete input set — presence evidence is reported and labelled as presence | `build-record.md` §6 |
| **The graph as this run's own output** — attempted over the complete input set and **blocked** by a fixed `Integer.MAX_VALUE - 8` array-length bound in flatgraph's string-pool writer; not satisfied, and not satisfiable with the pinned frontend at this input breadth | `cpg-frontend.log` STEP 8 and STEP 10; D1 here |
| **A current-run method, type-declaration or file count** — no current-run graph exists to load, so none was measured and none is estimated from the provisioned graph's | `cpg-frontend.log` STEP 12; D1 here |
| **Which input breadth the pinned frontend can serialize** — the failure establishes an upper limit lies at or below this run's 191-artifact set, and the provisioning invocation establishes 62 archives is below it; the boundary between them was not searched for, because narrowing the set to find it is the trimming AAP §0.9.2 prohibits | D1 here |
| `semgrep`'s `started_at` / `finished_at` — the 621-second window length **is** established | `tool-status.md`, "Values that could not be established" |
| `gitleaks`' rule count and ruleset digest; `checkov`'s policy count and policy digest — none separately versioned, none reported by its tool, none invented | `tool-status.md` |
| `joern`'s path-base **value** — the base *kind* and the resolution route are recorded; no plausible path was invented | `tool-status.md`; `runner-metadata.json` |
| The native severity vocabulary `osv-scanner` would have used, and the literals `dependency-check` emits — no record arrived to exercise either | `severity-map.md`, "Values that could not be established" |
| The behaviour of the `cvss_score` basis and the `unmapped_literal` disclosure on this run's own artifacts — each exercised 0 times, established against committed fixtures instead | `severity-map.md` |
| Probe query 02's `MAX_CALL_SCAN` reached-flag — published as `null` with its reason, the console stream that carried it not being preserved on this branch | `joern-probe.md`, "Values that could not be established" |
| Query 02's engine-internal call-depth bound — not observable from the engine's output, so the query reports the caps its own evaluator counts | `joern-probe.md` |

Two of this file's own, added here:

- **The contents of the run-created scratch locations** — the frontend staging
  directory and both `importCpg` verification workspaces — are no longer on disk,
  so their bytes cannot be re-hashed from this record. What survives is the staging
  directory's complete ordered manifest inside `cpg-input-inventory.json` and the
  workspaces' names, absence-before-use proofs and sizes inside `cpg-verify.log`
  — [§11](#11-deliverable-inventory-with-resolved-absolute-paths).
- **Which of the three graph identities a future load will read** is not determined
  by this run. The path is host-shared and was replaced outside it more than once
  (D4), so this record fixes all three pairs and the identity each load actually
  verified against, and predicts nothing about the next one.

---

## 15. The October 2025 caveat

**The pinned tree dates from October 2025** — commit
`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, **Thu Oct 23 2025** ([§2](#2-the-pinned-tree)).

**Every dependency-oriented count in this dataset must be read against that date
rather than against the present.** The three dependency-oriented tools resolved
their answers from feeds dated 2026-08-24 (Trivy's two databases and
Dependency-Check's NVD datafeed, D7) or queried a live API at scan time
(OSV-Scanner, which keeps no local database), while the code they were pointed at
is ten months older than those feeds. A count of zero, or any count at all, is a
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
`harness/artifacts/raw/trivy.json` and the path form
`harness/artifacts/cpg-input`, and reports
**nothing** for `oss-scan-results/run-record.md`,
`queries/joern/results/01-callgraph-unguarded-driver-launch.json` or
`harness/cpg/spark.cpg` — so the result documents, the probe tree and the graph
symlink are collected normally while these two trees are not. Publication is
therefore **by this manifest, carrying each file's byte size and sha256**.

Every figure below was computed from the file on disk when this record was
written. Where a `.status` file independently recorded its own artifact's digest,
the two agree — one measurement, two witnesses — for `opengrep.sarif`
(`302b515b…`), `semgrep.sarif` (`da983951…`),
`datadog-static-analyzer.sarif` (`2020ad1b…`) and `joern.json` (`deb0cd76…`).

**The three status records AAP §0.9.4 names explicitly are in the manifest:**
`gate-record.json`, `normalize-run.json` and `adapter-tests-run.json`.

**3 further files a runner wrote into `logs/` fall outside the AAP's file map, and they are
partitioned out rather than presented as deliverables** — `datadog-static-analyzer.console.log`,
`joern.preflight.log` and `joern.runner.console.log`. They are held out of the implementation change
set (`git ls-files` reports none of them), they are listed in their own clearly-labelled subsection
below rather than among the enumerated members, and the AAP sections that require their bytes to stay
on disk are cited there. Listing them is not authorisation: an unmanifested member of a git-ignored
tree would be invisible to every downstream check, which is the worse outcome of the two.

**The two trees diverge by design after the gate.** `harness/artifacts/raw/` is
**runner-only** — exactly one artifact per tool that wrote one, and nothing else
ever; eight artifacts, no `osv-scanner.json`, and neither taint A/B arm nor any
probe output ever landed in it. `harness/artifacts/logs/` **legitimately
accumulates** from Stage 1 onward: it holds the per-tool streams and statuses plus
the durable evidence for the gate, the Maven pre-check, the build, the JAR
inventory and staging manifest, the frontend, the graph verification, both taint
A/B arms, the normalizer run, the adapter-test run and each probe query.

One observed fact about collection, recorded because it does not follow from the ignore
rule alone: **127 of the 130 files are nevertheless tracked** on this branch —
`git ls-files` reports 8 under `raw/` and 119 under `logs/` — earlier lanes of this run having
added them explicitly. The manifest is required and supplied regardless, since the ignore rule is
what governs ordinary collection. The 3 that are **not** tracked are the runner-written
accumulation partitioned out below, and the reason each is held out of the change set is given there.

### `harness/artifacts/raw/` — 8 files, 120,557,145 bytes

| File | Bytes | sha256 |
| --- | --- | --- |
| `checkov.json` | 8,380 | `d5e4492ac799875f6cf14c187f9130bc2ad3f8060c320e757c127dfd1fda98fc` |
| `datadog-static-analyzer.sarif` | 5,671,091 | `2020ad1b3c10ce58cc30cbc01c8b31ae484ad4a92e254a64163c8462d79e37f6` |
| `dependency-check.json` | 17,097 | `ebe98aed11973718591f8c7490eedde86f97bf4fb2047a059e499be50e02c3b9` |
| `gitleaks.json` | 561 | `12d50cf783bb966c77608cae6f93c50c688e0384e84662041ecfb1b6935d8467` |
| `joern.json` | 354,343 | `deb0cd765602cc0be2bf4ffa03cc8a39cccfb5e17fb0631d094d24af55204a4a` |
| `opengrep.sarif` | 73,840,948 | `302b515b77a052dc4217a03cc9d1e0bc7bc0259beed928ca36cbcf48a503102a` |
| `semgrep.sarif` | 40,661,229 | `da98395187f5eb141e1c52cf1d700f94ea7a645d0ff488c506c9fac514857d84` |
| `trivy.json` | 3,496 | `4551d6ca435ad71aa2306bd03aa45ce92ba95d5f9ee03f304767f9f791878eba` |

### `harness/artifacts/logs/` — 122 files, 70,613,131 bytes

**This tree is partitioned, and the partition is structural rather than a note.** AAP §0.1.1 states
that `logs/` "legitimately accumulates from Stage 1 onward", so it holds two different kinds of file
and a reader must not take the second kind for a planned deliverable.

#### Members the AAP §0.6.1 file map enumerates — 119 files, 70,594,143 bytes

These are this run's planned log deliverables: the per-tool streams and statuses, and the durable
evidence for the gate, the Maven pre-check, the build, the JAR inventory and staging manifest, the
frontend, the graph verification, both taint A/B arms, the normalizer run, the adapter-test run and
each probe query.

Four of the names below carry a `<directory>/` prefix: a runner wrote its per-target output into a
subdirectory of `logs/`, and each member is published with its path relative to `logs/` rather than
left out — an unmanifested file in a git-ignored tree is invisible to every downstream check.

| File | Bytes | sha256 |
| --- | --- | --- |
| `adapter-tests-run.json` | 327,768 | `f0fb589cc68476b6b286f177b7142068e0bd5459c855b6c1acda10a5dfde4153` |
| `build-reactor.log` | 2,708,999 | `1ba2c5583f8796d28477491d8408ec805aa1f7e60bad04a5e04c6ba9b844c8e7` |
| `checkov.out/results_json.json` | 8,728 | `fa7d0258f48fc558a726a8fda5f1d05c7755cc621d51544a22342643a0ed0206` |
| `checkov.runner-console.log` | 1,161 | `c2878b4053e049927ef927be4f0857cbdf2697df0a12d80671b6e15e1568dca0` |
| `checkov.status` | 21,866 | `1f37a8ab75f51df4fc814dc43db29b94e554b09ebd701a866aca0e1a06c04d04` |
| `checkov.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `checkov.stdout.log` | 141,208 | `b7df3d6d72670dec11131fe7975e16ff7b002d8d0974f9abd66b7e57d1a2118b` |
| `cpg-ast-failed-classes.txt` | 52,366 | `2592123f2c85d099defc5d5fc90587f80643d9bad0c1702925d7d2105f9e66d0` |
| `cpg-frontend-ceiling-probe.txt` | 2,632 | `e7d82064047c1cfee06dfe22ba2398f5fe805160408289e354596ae2df97ab79` |
| `cpg-frontend-input-manifest.json` | 42,799 | `1edcbc502086126edaad023302fad4a4e56553fb3048f31f94bc3c23cafb781b` |
| `cpg-frontend-verbatim.log` | 6,286,661 | `6396eda9fdd55f7b6c84a3233eca708adf5bc8b01f6d90b9d276124357a9dd38` |
| `cpg-frontend.log` | 7,605,980 | `dd98cd028fd7aef0862c85c5950c786a5070646621837719fe14a24fb1733290` |
| `cpg-input-inventory.json` | 732,875 | `75eb43283b6404db511959975b33366d1604367c53bda015dc63605ce60889d8` |
| `cpg-module-coverage.json` | 89,627 | `0be2759e68fead303df655173853fa395f1b0aaecffb2ee2fcbc90f31bb8c964` |
| `cpg-verify.log` | 163,590 | `cb61ede915b6b1449b2d79b7c6a567c95082f60072b89a51c3f1c112f6a90e07` |
| `datadog-sast-rules.captured.json` | 4,068,707 | `c5fd464c2985119574f23599d44022e22b9442d7083acb17ec84addba354f322` |
| `datadog-sast-rules.captured.meta.json` | 1,700 | `886752281650f1fca9ebc7f5009d70b0547a4e1906673ca13c27694961bac240` |
| `datadog-static-analyzer.runner-console.log` | 1,174 | `6d69741f1da035c04d35835e01cc09b39a61ff3800b63b91dbd6874e0ded83ca` |
| `datadog-static-analyzer.status` | 12,938 | `4611bb0cd28e2c70e828bce41b3e51d7fb1410b86ff48456996e0a9b943cbf5a` |
| `datadog-static-analyzer.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `datadog-static-analyzer.stdout.log` | 4,033 | `eb2b84ff6863e7fc61736d3d69c6c556e0f9c17f6b45126714b92511737bb817` |
| `dependency-check.out/dependency-check-report.json` | 18,953 | `e78839911a970d683f13e55972a15f06cbbf3521d0b342ed9948cffaafb0122c` |
| `dependency-check.runner-console.log` | 1,184 | `c00e85df6a62a69e70983f6da7c16a8521593746e56624d70a70bf0955e79c6b` |
| `dependency-check.status` | 24,373 | `8749004fec6fbedde8d46588980924fe3c9110e0300bdf75ad8bd27906755c9d` |
| `dependency-check.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `dependency-check.stdout.log` | 57,061 | `598d4969628d5264e24258785725fadfd2e5f16a4031b5a644155bdfcf4bebd4` |
| `findings-publication.json` | 2,985 | `f5fac3fb586395707d8732ec441a7b860798775b3c05404406cf8e0dfd322fbd` |
| `gate-record.json` | 161,817 | `3a0ccd4c973ab9165b954d1e34962c15e44122bf58af99f680ee8c8e39c170b5` |
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
| `gitleaks.runner-console.log` | 1,241 | `17d5abb2e0efe865c14d5ca6c031b8491f804c21d0a57deb67a0efc805f2c48f` |
| `gitleaks.status` | 13,358 | `6a71991b834298401fddc7d1377520f111a31fde2aee9b547a38cffaf69d3299` |
| `gitleaks.stderr.log` | 27 | `0a726625c9ba4a8f2b0eefe36d323364dfe24ec01fa7227e50d05c6daadce00c` |
| `gitleaks.stdout.log` | 2,547 | `ce00b09c132d1805f6b8a249de1c0e8f74a614040da9c8ec80d6407bcf59432e` |
| `joern-preflight-negative-test.log` | 11,530 | `e1bd4cf99cc9c41430dfce837a0cd48ece7d55c067ef5231817c0eee307fe8de` |
| `joern-preflight.log` | 2,501 | `238d693fbf8b12f243c2344c1dfcafe67c222c7d03eb49e9c93c60bbc5811dce` |
| `joern.runner-console.log` | 1,566 | `21b86b63ace1b644a888b1fc0cdd61fcc651cf998028c2b0b4de894e9e7c24a1` |
| `joern.status` | 31,913 | `cf55f7f359dc25be4bb78a6eaf67b41299519cab7bdec0d7429f74974b467fc0` |
| `joern.stderr.log` | 699 | `17fb236e3b11d0489f158f27b5b632cbb10e1cebb670e262dfa8b3e42ea197fc` |
| `joern.stdout.log` | 16,444 | `4962e35b76e8c6d6070eb6a2a0a99470523af56819c75aba1373687af8fad83d` |
| `maven-preflight.log` | 10,398 | `345e17b69cab36a1bd11ca8987d511740db1bbffda22cc9127d688ec48844cfa` |
| `normalize-run.json` | 890,002 | `538e49111bdd12f3883cb570d4b301e38ce3fe8a485300b3f4e696fc9cb482b1` |
| `opengrep.runner-console.log` | 1,082 | `61314b3c175de286aaa4c12ae0af894c56c6c5f7aeb0a4577946d4a39ec4920c` |
| `opengrep.status` | 17,996 | `a7dec00fb25fd31233672ff107a4a5303d7af03abb9246a340deebfed193333f` |
| `opengrep.stderr.log` | 2,560 | `975f9e32be664d7bd4f5009cbe5d17236bfd19c8669138a3aeb5c14165854e0a` |
| `opengrep.stdout.log` | 963 | `447a2b8a980e80b678b273060069a0f1cb6eefaaf36158f8b038b03585737fdf` |
| `osv-scanner.runner-console.log` | 1,053 | `7f024806288727155729cf2406152986bb90b3da55056e11eac01ceda3b5dd53` |
| `osv-scanner.status` | 26,012 | `7790c4146ae552bba5a9f1ced0a905d208a3291b48209a86111d7369b823fc85` |
| `osv-scanner.stderr.log` | 969 | `021347c72dcd98e06b26c579164cded04c26b0eacc203aff07d5eb0487f2c401` |
| `osv-scanner.stdout.log` | 45,872 | `5819350d2f5b82461b8de8a708ddd2ca8c5389a21d18ae38fbee62bc777c2847` |
| `probe-01-callgraph-unguarded-driver-launch.log` | 12,737 | `7dabdc739c45b33dc3e439c55fd92bea1daf7fd75aadbd0dc3be160d27badf9b` |
| `probe-01-callgraph-unguarded-driver-launch.publication.json` | 2,312 | `4d16c3c9d0378cedc0459051f1a58f2b477ea8d655d8313fb99fa982daedcf70` |
| `probe-02-dataflow-unguarded-driver-launch.log` | 18,085 | `2d53abcbb7a2060233634c5825929f51a2c5711217d1a682e0fd399dd0cf8264` |
| `probe-02-dataflow-unguarded-driver-launch.publication.json` | 2,304 | `048fe5f48e27afd195994ef4adf415d10b179be7ffaa24d7bb6ae019cc7fd3f0` |
| `probe-03-parameterized-handler-sink-pairs.log` | 24,567 | `ffa1f3ddebeaf4b964d61376400b2daf89d7d1e7c4d82e5e5794884e77eda052` |
| `probe-03-parameterized-handler-sink-pairs.publication.json` | 2,305 | `5c9240a976d68980f9319840495cd1de45fe67236daa0ea8c30cfd01232ae66a` |
| `probe-query-revisions.json` | 2,326 | `7fd8668d9c0e8184fffb5cd19fa2a0d448c1845024c3a45520e398d88873bc2f` |
| `runner-metadata.json` | 117,676 | `6314fbc9607de647cd449a73baa4156d1cba3378791ff24729897ad6d130cd60` |
| `runner-sequence.json` | 6,113 | `26458f2586350f76b57cadc52746abc0e3eb1beb256adc461aaebae658436340` |
| `semgrep.runner-console.log` | 1,057 | `40553675b6e56144c9fc6102263c5fe5eec20d17aa16ab6a5ddc1df8abd5fea3` |
| `semgrep.status` | 21,322 | `5d9a65188846d3c23d0d06fcdc4610eb78c537299fb9dcf345c79fcce67838d7` |
| `semgrep.stderr.log` | 5,079 | `ba65027215fa502812b57f0e42d02f8a160af187b932ea87aa9738970dbc0159` |
| `semgrep.stdout.log` | 40,661,230 | `96c7eb268cbc17eea688bbd3836b477a3846095f0dcaade7ff45451ac63b523d` |
| `taint-ab-anchor-diskstore-fullruleset-off.log` | 4,361 | `d440fa546e31e75c839bf8aae3f5eaac5b8db0efcd9a2ee6c9ffe1cd5b65f047` |
| `taint-ab-anchor-diskstore-fullruleset-off.sarif` | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |
| `taint-ab-anchor-diskstore-fullruleset-on.log` | 4,377 | `512bda7e81c6cedb4d70bd80d67faa7e3ea33e816d7f6642c2c739e870f87415` |
| `taint-ab-anchor-diskstore-fullruleset-on.sarif` | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |
| `taint-ab-anchor-diskstore-off.log` | 2,216 | `a72ea02ad345259abdfbf6dc4faf6b82c10f46f521d5bcc03a27a9059661a94c` |
| `taint-ab-anchor-diskstore-off.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `taint-ab-anchor-diskstore-on.log` | 2,235 | `663afe9c8aeba1b79c6cad4a609346d22289654f96a83d0c0b72f2787fd940f7` |
| `taint-ab-anchor-diskstore-on.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `taint-ab-hiveshim-off.log` | 1,997 | `a442acdce88cfc53b5b4b3b63435dbb26d72720a741a59af65b9c31652b098fc` |
| `taint-ab-hiveshim-off.sarif` | 2,341 | `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |
| `taint-ab-hiveshim-on.log` | 2,457 | `ae387d47d16253e301c2e5f65478235ab1fbbc4911f54486c24e339e85a56950` |
| `taint-ab-hiveshim-on.sarif` | 10,021 | `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| `taint-ab-off-control-rule.txt` | 1,982 | `a1039db83793e43c7144a87506714ccbaf13f92f4fa36c327c74a8ab53364ad7` |
| `taint-ab-off.log` | 68,033 | `42fc3b8e81b85debd01f5fa7bc541724868286acf52558fdfbe814b52e5b02ca` |
| `taint-ab-on.log` | 55,934 | `1175d16a818fb9dbf86d237efd654470f65e9c7e1ea6d23336bfdb746792c564` |
| `taint-ab-search-control.sarif` | 4,424 | `272a530fea4ef95417cd539b5964a70f6805e5def72ab58264cf73dbbbdb8ceb` |
| `taint-ab-source-removed-control-rule.txt` | 2,498 | `a8bc7f992389761b3ea840012b281e3d218add030663b9132e10924a66f02cac` |
| `taint-ab-source-removed-control.sarif` | 2,347 | `e98c1e1fb37c66cbf7dac92838485314b57a4561a41a6d15d9043eebbaac745f` |
| `trivy.parts/common_network-common_src_main.json` | 254 | `f849fc7761f05ccab97a5f3e4c998fe9c136b1b93bf78a4459fdd8634c40dfea` |
| `trivy.parts/common_network-shuffle_src_main.json` | 255 | `8577a169ff8b6a73f06fec0398cf7eef9da3b17ded897f5159b9afa7632d62ee` |
| `trivy.parts/common_network-yarn_src_main.json` | 252 | `632d6829cfb7d23805ea26da6809b591af3c34b8e2eb6b0ed61a085bd2680a0b` |
| `trivy.parts/core_src_main.json` | 237 | `4816274c985cf999abdde2ad395efd4effa1d682948dc71e15ba41b5e0a26cb6` |
| `trivy.parts/python_pyspark.json` | 238 | `767c401550c386dec3b65f10dbba9057ae0a9e34f40d4a5937e7c288cd3ec1a5` |
| `trivy.parts/resource-managers_kubernetes_core_src_main.json` | 266 | `af2983f390323a70476bca5a2cbde03e170f3643e97c3f9da4127815945d1ff0` |
| `trivy.parts/resource-managers_kubernetes_core_volcano_src_main.json` | 273 | `7e7ff6044b0dd47f1e81a1b36f727b40f5383dbf67ee9bba14bc3f93ec0867cc` |
| `trivy.parts/resource-managers_kubernetes_docker_src_main.json` | 3,836 | `52e3bcfb25afc8dc09485abfbe98b15d5defd5ce3dc0e977bbab0f34488339bb` |
| `trivy.parts/resource-managers_yarn_src_main.json` | 255 | `e3e6ba78d71129b824e8f82fd951dc26168596c9a45238f621356c3c4abce6d2` |
| `trivy.parts/sql_catalyst_src_main.json` | 245 | `0513c5803c42bed973a82c8aa76bbf997bf5a9349d634086ef7a40639fa8d7a7` |
| `trivy.parts/sql_connect_client_jdbc_src_main.json` | 256 | `d5b60a891ccaa74c263e011c20b47353302207c6c337aa87d3bb013de2d808e7` |
| `trivy.parts/sql_connect_client_jvm_src_main.json` | 255 | `20e543bbb9c9e99df8056436bbee8c213c23cad07497e70d48b9cffa59a6b2ce` |
| `trivy.parts/sql_connect_common_src_main.json` | 251 | `dded47b2987aa04f2efd499d10c6afd1e786106f1d15ceb6a491de94ee3e4bd7` |
| `trivy.parts/sql_connect_server_src_main.json` | 251 | `dd692d42487c1d4340b371877a214648921c766ca4c324bdb815afa810cd5d87` |
| `trivy.parts/sql_connect_shims_src_main.json` | 250 | `d009b892e950ebd41a344bb1f891e10651f3af3da767c6370ac27dd959a979da` |
| `trivy.parts/sql_core_src_main.json` | 241 | `8784ac23cb4524425e73422a949cea50a15f116d8addbd965eb17790244f586f` |
| `trivy.parts/sql_hive-thriftserver_src_main.json` | 254 | `f61309becbcc730769fa98f5bcf0be9f8a764730f94d255cf1d1a04c293125b0` |
| `trivy.parts/sql_hive_src_main.json` | 241 | `f5b6223daa5b8b1d70eb02a9a3eb09ac88c95129a8ea3d812162a1fb176fa41f` |
| `trivy.runner-console.log` | 1,391 | `17bbcf39a79e32be06cdb8bfefc0f1e1d9062789c977d15fd5457c459a61b049` |
| `trivy.status` | 30,961 | `800170b2fb04d9d8e0ac6f77988f07bd7af6a2c96d059e874944b505caf36d66` |
| `trivy.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `trivy.stdout.log` | 2,756 | `d5371915e604aef6c3087f10f4c6ea01f81a194cc6448f40c8d553538cf88428` |

#### Runner-written accumulation, NOT planned deliverables — 3 files, 18,988 bytes

**These match no entry in the AAP's file map.** Each was written by a runner during Stage 3
rather than planned as a deliverable, and each is therefore held out of the implementation change
set: `git ls-files` reports none of them, and they were removed from the index with
`git rm --cached`, which leaves every byte on disk. They are listed here because a file present in a
published tree and absent from its manifest would be the worse defect — an unmanifested member of a
git-ignored tree is invisible to every downstream check.

**Why they are retained on disk rather than deleted or relocated, with the sections that require it.**
Three AAP rules bear on this and all three point the same way:

- **§0.8.1** — "Do not tear anything down. No cleanup, no reset, no temp purging. What the run built
  stays where it is." These are output the run built.
- **§0.1.1** — `logs/` "legitimately accumulates", which is an explicit statement that its membership
  is not a fixed list; only `raw/` is runner-only-and-nothing-else-ever.
- **§0.9.4** — every number must name the file it came from and that file must exist. `joern.preflight.log`
  is the cited evidence for the Stage 3 graph-identity re-verification ([§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor)
  cites it at PART 2 line 147 with the verdict at line 167), so deleting or moving it would leave a
  mandated check citing a file that is not there.

So they are **classified rather than removed**: outside the planned boundary, outside the
implementation change set, inside the tree the run wrote, and named as such at the point of use.

**This is recorded as an unresolved conflict rather than as a resolution, and the finding that names
these three stays open.** Classifying them here is not authorisation for them. The two instructions —
remove them from the delivered tree, and do not tear down what the run built or orphan a cited
evidence file — cannot both be honoured, so the run took the frozen plan's side per AAP §0.1.3 and
declined to break the evidence chain. **D13** in [§13](#13-divergence-register) states the conflict in
full, including what a human must decide and what would have to be rebuilt first if the boundary is
held to govern.

| File | Bytes | sha256 |
| --- | --- | --- |
| `datadog-static-analyzer.console.log` | 1,117 | `beaef9fc905647ad63129d17712b59b5ff4d99e2dee3a1dd1e617324b9e4fd3f` |
| `joern.preflight.log` | 16,443 | `acb4a045d6ebdaee98cab09088fdcea5b8753df81ee8d9bdb845632124b9a59a` |
| `joern.runner.console.log` | 1,428 | `53c18a17aba88510d0974b92094468071c909faa6f01a39ae484f6e4e763b82b` |


**Totals: 130 files, 191,170,276 bytes** — 8 in `raw/`, 119 enumerated in `logs/` and 3 runner-written. The machine-readable form of this manifest is `harness/artifacts/MANIFEST.json` (93,231 bytes), which carries the same per-file sizes and digests plus the graph's identity and its record of account; it states no digest of its own, because a file cannot carry its own. Two pairs of `taint-ab-*.sarif` entries share a digest within the pair — the `-anchor-diskstore-` arms at 4,753 bytes each and the `-fullruleset-` arms at 2,939,276 bytes each — because in each pair the two arms' outputs are byte-identical, which is the measurement divergence D2 rests on rather than a manifest error; the `taint-ab-hiveshim-` arms differ from each other because that pair is the discriminating one ([§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured)). The **4** zero-byte entries — `checkov.stderr.log`, `datadog-static-analyzer.stderr.log`, `dependency-check.stderr.log`, `trivy.stderr.log` — carry the sha256 of the empty string, as they must.

---

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
  halts**, both values recorded, because there is no anchor to adjudicate between
  them and continuing would mean choosing one silently. **No such contradiction
  arose**; `gate-record.json` records zero halts across its 42 checks.
- **The rule reaches inherited facts only.** It does not apply to outputs this run
  deliberately replaces. A graph differing from a previously recorded graph's size,
  digest or counts is **the request being fulfilled, not an environment
  contradiction** — reading intentional replacement as a contradiction would halt
  the run for succeeding. The gate enumerated the graph's size, digest and counts,
  and the build outcome, as deliberately-replaced fields for exactly that reason,
  and D4 is handled under that boundary rather than as a record contradiction.

---

## 18. Where the run reached

**All seven stages ran, in order, and the record stage completed.** Nothing was
skipped, and nothing below is a projection.

| Stage | Reached | The evidence that says so |
| --- | --- | --- |
| 0 — Gate | complete, verdict **pass** | `gate-record.json`: 42 checks, 39 pass, 3 recorded difference, 0 halt, 0 inconclusive |
| 1 — Tree and build | complete | pinned `HEAD` equal to the pin; allowlist byte-exact and left as found; Maven pre-check **PASS** with the download branch unreachable; `BUILD SUCCESS`, 40/40 projects, 38/38 own artifacts; `runner-metadata.json` finalised with every runner's target set and its root verified |
| 2 — Graph | complete, with three conditions carried | inventory reconciled and staged 191/191 with the mapping total and injective both ways; `importCpg` verification load exit 0 reporting 1,397,339 methods; per-module coverage measured on two agreeing axes. **The frontend was invoked by this run over the complete manifest and failed in persistence at a fixed array-length bound after 8 h 01 m, producing no graph (D1)**, so the graph at the sanctioned path remains provisioning's and its input set is narrower than the build (D3) — reported, and nothing trimmed to obtain a graph. **The taint A/B did not discriminate (D2)**, reported and not repaired. **The graph at the path was replaced again after this run's last load (D4)**, reported and not repaired. Additionally this run **built and verified a per-module witness graph** — 38 primary artifacts, exit 0, 418,777,229 bytes, 994,192 methods, all 38 module witnesses present — as a labelled frontend-capability measurement that establishes the seven missing verdicts to be an input-set consequence without standing in for them |
| 3 — Nine runners | complete | all nine invoked directly, individually, with no arguments and through no orchestrator; eight artifacts written; `osv-scanner` completing with its own stated reason and no artifact |
| 4 — Normalization | complete | 9,433 rows, `10018 = 9433 + 585`, typed comparison over 113,196 fields with no mismatch, row validation with zero violations, exit 0; **1134** adapter and reconciliation tests passing |
| 5 — Probe | complete | three bounded hand-written queries run under `importCpg` only, six result files, all three effort measures answered, parameterizability passing on an invocation that was actually made |
| 6 — Record | complete with this file | the eight result deliverables and the three deliverable trees all exist ([§11](#11-deliverable-inventory-with-resolved-absolute-paths)), and both artifact trees are published by manifest ([§16](#16-manifest-of-the-two-git-ignored-artifact-trees)) |

> **CHECKPOINT STATUS: HALTED, NOT COMPLETE.** Two of the plan's own halt conditions are met and
> neither is repairable by any permitted action. **D1** — the mandated graph over every JAR the build
> produced cannot be persisted by the pinned frontend, proven from the failing method's bytecode; the
> only effective remedy is excluding inputs, which AAP §0.9.2 lists among the conditions that stop the
> run. **D4** — the provisioned record's stated graph identity is contradicted by the bytes on disk, on
> a field the expected-values table does not carry, which AAP §0.1.3's fourth case makes a halt with no
> anchor to adjudicate between the values. A third, **D2**, is the taint A/B not discriminating on the mandated subject — with the engine's activity separately measured on another file. Every
> other stage of the run completed and is recorded below; these three are reported rather than resolved,
> which is what AAP §0.8.1 requires of them. A fourth condition, **D13**, is an unresolved conflict
> rather than a halt: a review finding requires three runner-written logs removed from the delivered
> tree, and §0.8.1, §0.1.1 and §0.9.4 require them kept — the run took the frozen plan's side, recorded
> the conflict in full, and leaves the decision to a human.

**Three halt-class findings are on the record and none was repaired**: **D1**, the
graph not being this run's own output — attempted over the complete input set and
blocked by a fixed array-length bound in the pinned frontend's writer; **D2**, the
taint A/B not discriminating on the mandated subject; and **D4**, the provisioned
record's stated graph identity being contradicted by the bytes on disk. AAP §0.8.1
settles which way that tension resolves — report the condition, never repair it
silently — and nothing was installed, rebuilt, trimmed, overwritten or averaged to
clear any of them. In particular, the one change that would have produced a
current-run graph is excluding inputs, and that is the trimming §0.9.2 names as a
halt rather than a remedy. Each is stated at the top of the document that owns it
rather than in a footnote, and each is carried here with every value it has.

**Nothing was torn down.** No cleanup, no reset, no temp purging. Both artifact
trees stand where the run left them, no runner file, environment file, shared
library, allowlist or Apache Spark file was edited in either tree, and no
credential was provisioned.

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
