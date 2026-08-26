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
graph — 25 on a class exclusive to the module and 6 on presence evidence labelled
as presence — with **7** carrying no verdict obtainable from this graph, each named
individually with the witness tried and the query run, `sql/connect/shims` among
them. **Zero** verdicts rest on a shared package prefix and **zero** winner maps
are claimed.

### The graph's byte size and sha256, and the identity re-verified before every load

The identity of record, written at the time the graph was written and owned by
`harness/artifacts/logs/cpg-frontend.log` PHASE 3:

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
| Probe query 01 | `queries/joern/results/01-callgraph-unguarded-driver-launch.json` | match on both values against `cpg-frontend.log`'s pair |
| Probe query 02 | `queries/joern/results/02-dataflow-unguarded-driver-launch.json` | match on both values against the same pair |
| Probe query 03 | `queries/joern/results/03-parameterized-handler-sink-pairs.json` | match on both values **against the record of account for the bytes it read**, which is a different pair — see below |

**The graph was replaced on the host between loads, and that is recorded rather
than reconciled away.** Queries 01 and 02, the verification load and the Stage 3
runner all read 541,255,894 bytes / `26d327cc…`. Query 03 read **548,118,435 bytes
/ `f8c715624b1b91c9cbb1a88931c11e2d2f18ec3f56d908af57415651f5d22c53`**, reporting
methods 1,399,866, type declarations 119,920 and files 45,037. Measured for this
record, `harness/cpg/spark.cpg` now resolves to a file of **548,118,435 bytes,
sha256 `f8c71562…`, mtime `2026-08-25 20:19:28Z`** — the second pair. The resolved
path is host-global and shared read-only with concurrent clones, and this run
neither rebuilt nor replaced it, so the replacement happened outside this run.
Both pairs are kept with their provenance and neither is discarded; **both method
counts clear the one-sided floor**; and every load was verified against the record
of account for the bytes it was about to read, before reading them.

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

| Field | Value | Source |
| --- | --- | --- |
| Command | `SL_LOGGING_LEVEL=WARN jimple2cpg /opt/blitzy-harness/cpg-input -o /opt/blitzy-harness/cpg/spark.cpg --recurse -J-Xmx64g < /dev/null` | `cpg-frontend.log` PHASE 2, "THE COMMAND LINE" |
| JDK major | **21** — Temurin-21.0.12.1+1, reported by that JDK | `cpg-frontend.log` STEP 14 |
| Heap actually used | **`-J-Xmx64g` = 64 GiB.** Neither raised nor lowered; peak sampled RSS 59.0 GB | `cpg-frontend.log` "THE THREE VALUES, RECORDED SEPARATELY" |
| Commit proof at that value | `"$JAVA_HOME_21/bin/java" -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version` → exit 0 | `cpg-frontend.log` STEP 16 |
| Elapsed | 53 m 04 s (12:59:23Z → 13:52:27Z) | the invocation record quoted at `cpg-frontend.log` STEP 13 |
| Exclusions on the command line | **none** — no `--exclude`, no `--exclude-regex`, no depth override | `cpg-frontend.log` PHASE 2 |

**This invocation was not performed by this run**, which is the halt-class finding
D1 in [§13](#13-divergence-register).

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

Sources: `harness/artifacts/logs/taint-ab-on.sarif`,
`harness/artifacts/logs/taint-ab-off.sarif`, and both arms' logs
`taint-ab-on.log` and `taint-ab-off.log`. **Both files are under `logs/` and
neither is under `raw/`**, so neither could overwrite the Stage 3 runner's
`opengrep.sarif` and neither contributes a dataset row.

| | Expected | Observed |
| --- | --- | --- |
| Taint **on** (`--taint-intrafile`) | 1 traced finding at `core/src/main/scala/org/apache/spark/storage/DiskStore.scala` line 72 | **1** finding at line 72, carrying a dataflow trace — exit 0, 1.890 s |
| Taint **off** (the control) | **0** findings | **1** finding at line 72, carrying a dataflow trace — exit 0, 1.877 s |
| The two arms' artifacts | different | **byte-identical**: 4,753 bytes each, sha256 `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` — re-measured for this record and equal |
| Verdict | a discriminating pair | **the A/B pair FAILED: non-discriminating on the mandated subject file** |

**This is a halt-class finding (AAP §0.9.2 lists a failed taint A/B among the
conditions that stop the run). It is reported and not repaired.** The ON arm was
additionally re-run in the same shell seconds after the control and again returned
1 at line 72, so the contrast does not rest on a figure quoted from another
clone's file. Nothing was adjusted to obtain the expected zero: no rule, no file,
no line and no flag set was changed, and the arm was not retried with a narrower
rule.

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

**A taint-free arm is not constructible at this pin**, which the OFF arm
establishes from the engine's own option list rather than assuming a flag name:
the only taint options are `--taint-intrafile` and `--guarded-taint-signatures`
(the latter requiring `--experimental`); `--optimizations=none` toggles
optimizations rather than taint; and the `--pro` family requires the proprietary
engine, which is unlicensed and deliberately unused. So "taint off" here means
*intraprocedural taint* rather than *no taint*, and this arm must not be read as a
pattern-matching-only control.

**Inherited evidence that agrees, recorded with its provenance and not re-run.**
`harness/ENVIRONMENT.md` section 11, Test 5 states the same non-discriminating
outcome for this subject file in its own words, and separately reports a
**discriminating** pair on a different file — `JdbcDialects.scala`, 12 findings
with taint on against 11 with it off, with lines 659, 666, 670 and 676 reachable
only with taint on and every ON-arm finding carrying `codeFlows`. That measurement
is **inherited and unanchored**: the expected-values table names no such subject,
and this run did **not** re-run it, because retrying on a different file to obtain
the expected answer is precisely what the reject-rather-than-infer principle
forbids. It is recorded because a fact this run could not establish must be named
rather than omitted — **not** as a substitute for the mandated A/B, and **not** as
a pass. Ruleset and engine identity for both arms match their pins exactly
(opengrep-rules commit `f1d2b562b414783763fd02a6ed2736eaed622efa`, engine 1.27.1,
one rule loaded), so neither arm is marked non-comparable, and that absence is for
a measured reason rather than by omission.

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
| Output files | `findings.json` 4,423,605 bytes, sha256 `b719fe739417f53f3bf501d00a10d380855894751eda4efef02d2b94d6a9cab3`; `findings.csv` 2,100,816 bytes, sha256 `6c7bea599e176b73113a6651f937b9aa109c452bc9fd26cdd046a7f1f7f49fe4` |

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
runner, no plugin and no install step. **577 tests and 12,955 subTests, 0
failures, 0 errors, 0 skipped, 0 expected failures, 0 unexpected successes, result
`OK`, exit 0**, 1.534 s as `unittest` reported it and 1,746 ms wall. The zero
skip, expected-failure and unexpected-success counters are reported rather than
omitted, so a green result cannot have been obtained by excusing a test.

The committed tree holds `README.md`, **8 test modules, 52 fixtures and 52
expected-row files**, of which **38 are negative fixtures** cross-checked against
the nine rejection conditions AAP §0.5.4 enumerates. Which fixture came from which
artifact, and what each module asserts, is owned by
`oss-scan-results/adapter-tests/README.md`.

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
| `oss-scan-results/findings.json` | `<repo>/oss-scan-results/findings.json` | present, 4,423,605 bytes |
| `oss-scan-results/findings.csv` | `<repo>/oss-scan-results/findings.csv` | present, 2,100,816 bytes |
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
| The frontend staging directory | `/tmp/blitzy/blitzy-spark/blitzy-f38258d3-f87d-44f5-bedc-af512c69e0ab-w-020_fe4cf4/harness/artifacts/cpg-input` | **no longer on disk**, and named as such rather than omitted. It was proved absent before use, created by this run's inventory lane, and never cleared by it; its complete ordered manifest — 191 staged files with names, sizes and digests — is preserved inside `harness/artifacts/logs/cpg-input-inventory.json`, which is the deliverable that publishes it |
| The `importCpg` verification workspaces | `/tmp/blitzy-harness-scratch/0/cpg-verify` and `/tmp/blitzy-harness-scratch/0/cpg-verify-descriptors` | outside the checkout, each proved absent before use, neither reused nor cleared by this run — and **neither on disk now**, stated rather than implied. `cpg-verify.log` STEP 5 and STEP 11 preserve their names, their absence-before-use proof and the 2.9 GB working copy one of them held |
| The provisioning frontend's input path | `/opt/blitzy-harness/cpg-input` | present on the host, 62 archives — the input set of the graph actually loaded (divergence D2) |

Nothing was torn down: no cleanup, no reset and no temp purging (AAP §0.8.1). The
two scratch locations that are gone were not removed by this run.

---

## 12. Every failure or termination

**No invocation anywhere in this run terminated without an exit code, so
`exit_status: timeout` appears nowhere and no entry carries that status.** Every
one of the nine runners ended with its own exit code
(`oss-scan-results/tool-status.md`, "Artifact status and exit status are
independent"), and **exit 78 — the harness's configuration-fault status — was
never observed**.

| Event | What it was | Disposition |
| --- | --- | --- |
| `gitleaks` exit **2** and `checkov` exit **1** | Non-zero because each found something. Both wrote an artifact and both parse | Ordinary. Artifact status and exit status are independent; the exit code is recorded as a fact and used for nothing else |
| `osv-scanner` exit **128**, **no artifact written** | The tool stated its own reason: `No package sources found, --help for usage information.`, quoted verbatim in its `tool-status.md` entry | **Completion with nothing in scope to work on**, not a failure. Zero rows, reconciliation `not applicable — artifact absent`, run continues. The missing-artifact halt was not engaged, because the absence came with the tool's own stated reason |
| `joern` artifact **partial** | 692 raw records, 107 rows, **585** records rejected under the single named class `unresolvable_path` | Partial parse is a first-class outcome: every parsable record emitted, every rejection counted under its class |
| The **taint A/B** | Non-discriminating on the mandated subject file: 1 finding at line 72 in **both** arms, byte-identical artifacts | **Halt-class finding, reported and not repaired** — [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured), divergence D2 in [§13](#13-divergence-register) |
| The **frontend build** | Not performed by this run; the graph on disk was written by the provisioning invocation before this run's first command | **Halt-class finding, reported and not repaired** — divergence D1 in [§13](#13-divergence-register) |
| Probe query 02's **reproduction check** | Attempted, and **halted** on a graph-identity mismatch, emitting no result region and no envelope | Working as designed: a load against different bytes than the record describes was refused rather than weakened |
| Anything else | No tool crashed, no artifact matched an unknown shape (`failed` never occurred), no reconciliation identity failed, no adapter fixture, rejection or reconciliation test failed, and no runner resolved a tree other than `SPARK_SRC` | — |

---

## 13. Divergence register

Every divergence with **both the expected and the observed value** (AAP §0.9.4).
Two are halt-class findings reported and not repaired; the rest are recorded
differences that do not stop the run (AAP §0.9.3).

### D1 — halt-class: the graph was not created by this run

| Field | Value |
| --- | --- |
| Expected | AAP §0.1.1 and §0.5.1 — this run invokes the frontend over its own staged input set and writes the graph; *a graph already on disk is never accepted as this run's output* |
| Observed | The graph at `/opt/blitzy-harness/cpg/spark.cpg` was written **2026-08-24 at 13:52:22Z**, before this run's first command, by the provisioning invocation reproduced at [§6.1](#61-the-frontend-build--first-of-four). This run did not invoke the frontend and did not write, replace or write through the graph path |
| Why not repaired | The resolved path is host-global and shared read-only with up to 45 concurrent clones, whose instructions forbid rebuilding it; replacing those bytes while a sibling is part-way through a load would corrupt that sibling's scan irreversibly. Writing a fresh graph anywhere else would leave `$HARNESS_CPG` — which `harness/bin/run-joern.sh` actually resolves and loads — on the provisioning bytes, so the record would then describe a graph no later stage opens |
| What it does not compromise | The identity in [§5](#5-the-graph--its-counts-its-bytes-and-the-one-sided-floor) is the identity of exactly the bytes each later stage loaded, measured from the file itself |
| Owner | `harness/artifacts/logs/cpg-frontend.log`, divergence D1 |

### D2 — halt-class: the taint A/B did not discriminate

| Field | Value |
| --- | --- |
| Expected | one traced finding at `DiskStore.scala` line 72 with taint on and **zero** with it off, from two invocations differing only in that setting |
| Observed | **1** at line 72 with the flag and **1** at line 72 without it, both traced, artifacts byte-identical at 4,753 bytes / sha256 `7949617b…5778` |
| Disposition | reported and not repaired; nothing was retried, narrowed or re-flagged to obtain the expected zero. [§7](#7-the-taint-ab--the-graph-stage-pass-condition-as-measured) carries the mechanical reason and the engine limit |
| Owner | `harness/artifacts/logs/taint-ab-off.log`, divergence D1 in that file |

### D3 — the graph's input set is narrower than the build produced

| Field | Value |
| --- | --- |
| Expected | the graph built over **every** JAR the build produced, nothing trimmed: this run's inventory staged **191** own artifacts, 431,184,900 bytes, from all **38** JAR-packaging projects, and proved the mapping total and injective in both directions |
| Observed | the loaded graph's input path held **62** archives, 285,122,375 bytes, from **31** modules, with its own manifest recording 190 files excluded and a per-file reason for each |
| Consequence, stated so no count is misread | seven of the 38 JAR-producing modules therefore have **no coverage verdict obtainable** from this graph, and no finding on it can resolve into a `src/test` tree, every `-tests` archive being absent from it. A graph over the wider set cannot have *fewer* methods than one over the narrower, which is why the method count is a one-sided floor rather than a window |
| Disposition | recorded with both values; **neither input set was trimmed or padded and no count was adjusted to make the two agree** |
| Owner | `cpg-frontend.log` divergence D2, with the coverage consequence measured in `cpg-verify.log` and the verdict owned by `build-record.md` §6 |

### D4 — the graph on the host was replaced between loads

| Field | Value |
| --- | --- |
| Expected | one graph, one identity, for every load of the run |
| Observed | the verification load, the Stage 3 runner and probe queries 01 and 02 read **541,255,894 bytes / `26d327cc…`** (methods 1,397,339, type declarations 119,691, files 45,037); probe query 03 read **548,118,435 bytes / `f8c71562…`** (methods 1,399,866, type declarations 119,920, files 45,037), and the path resolves to that second pair now, mtime `2026-08-25 20:19:28Z` |
| Disposition | both pairs kept with their provenance, neither discarded. Every load was verified against the record of account for the bytes it read, before reading them, and each comparison matched; query 02's reproduction check **halted** on exactly this mismatch rather than loading mismatched bytes. Both method counts clear the one-sided floor of 853,420. The file is host-shared and read-only to this run, which neither rebuilt nor replaced it |
| Not an authority-rule contradiction | AAP §0.1.3 confines the authority rule to **inherited** facts and excludes outputs this run deliberately replaces; the gate additionally classified the graph's size, digest and counts as deliberately-replaced fields for exactly this reason. Reading a graph replacement as a record contradiction would halt the run for succeeding |

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
| Duplicate-class overwrite warnings | ~5,700 | **31,598** warnings over **26,221** distinct destination class files |
| AST-creation failures | ~36, protobuf-generated `connect.proto.*` classes | **173** over 173 distinct classes, every one `java.lang.RuntimeException: Chain already contains object: <fqcn>`, and **none protobuf-generated**: 104 `org/sparkproject/io` netty-vendored and 69 `org.apache.spark.sql.*` |

Both are grouped by the module and artifact the affected classes are **contained
in**, and the measured cause of the overwrite gap is each module contributing both
its shaded artifact and its `original-` pre-shade sibling: 16,150 of the 26,221
destinations are duplicated between two artifacts of one module. Neither figure is
treated as acceptable because a document expected another number.

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

---

## 14. Values that could not be established

Named rather than omitted, because a value missing from the record is a value
nothing downstream can check (AAP §0.9.4). Each is owned by the document that
tried to establish it; this section indexes them.

| Value | Named in |
| --- | --- |
| The **cause** of the graph's above-anchor counts — measured composition is reported, a cause is not guessed | `cpg-verify.log` PHASE 2 and D3 there; `build-record.md` §7 |
| **Per-class provenance** for every overwritten class, and therefore any winner map | `cpg-frontend.log`, "THE LIMITATION"; `build-record.md` §5 |
| A **coverage verdict for seven modules**, each named with the witness tried and the query run | `build-record.md` §6 |
| An **injective coverage witness** for eight modules against this complete input set — presence evidence is reported and labelled as presence | `build-record.md` §6 |
| **The graph as this run's own output** — not satisfied | `cpg-frontend.log` D1, indexed here as D1 |
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
- **Which of the two graph identities a future load will read** is not determined
  by this run. The path is host-shared and was replaced outside it (D4), so this
  record fixes both pairs and the identity each load actually verified against,
  and predicts nothing about the next one.

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
`gate-record.json`, `normalize-run.json` and `adapter-tests-run.json`. **So are the
files a runner wrote into `logs/` beyond the AAP's enumeration**, which is a real
case rather than a hypothetical: `datadog-static-analyzer.console.log`,
`joern.preflight.log` and `joern.runner.console.log`.

**The two trees diverge by design after the gate.** `harness/artifacts/raw/` is
**runner-only** — exactly one artifact per tool that wrote one, and nothing else
ever; eight artifacts, no `osv-scanner.json`, and neither taint A/B arm nor any
probe output ever landed in it. `harness/artifacts/logs/` **legitimately
accumulates** from Stage 1 onward: it holds the per-tool streams and statuses plus
the durable evidence for the gate, the Maven pre-check, the build, the JAR
inventory and staging manifest, the frontend, the graph verification, both taint
A/B arms, the normalizer run, the adapter-test run and each probe query.

One observed fact about collection, recorded because it differs from what the
ignore rule alone would suggest: all **54** files in these two trees are
nevertheless **tracked** on this branch — `git ls-files` reports 8 under `raw/` and
46 under `logs/` — earlier lanes of this run having added them explicitly. The
manifest is required and supplied regardless, since the ignore rule is what governs
ordinary collection.

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

### `harness/artifacts/logs/` — 46 files, 53,259,957 bytes

| File | Bytes | sha256 |
| --- | --- | --- |
| `adapter-tests-run.json` | 108,278 | `6b60ad530ac784d4b8feb71379ef5e45dbfe56b6c47404ac8ad9c192e1a49df5` |
| `build-reactor.log` | 2,538,978 | `18c1cd7e45e7e320100e513a5303f4f6e386e9384ad756346a25964e5536397d` |
| `checkov.status` | 11,901 | `06bade92f990005d2ae77a3d740d1ca9ffbe281f9862a0db06eb33f4722220da` |
| `checkov.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `checkov.stdout.log` | 141,208 | `b7df3d6d72670dec11131fe7975e16ff7b002d8d0974f9abd66b7e57d1a2118b` |
| `cpg-frontend.log` | 6,402,153 | `a93eef2d24fdc83c64101095b6bc1871f66a8fcb9d1750bc0243591b251046e3` |
| `cpg-input-inventory.json` | 1,331,360 | `3cd4de2e7ebc61d7913e33b530d85d15748858ce2efb60f4732b671b5e5b5d6f` |
| `cpg-verify.log` | 134,061 | `f5b3039e3499de9f2ed931c7c9fcec5d54ac18b2e376d01cdb571df5091678e9` |
| `datadog-static-analyzer.console.log` | 1,117 | `beaef9fc905647ad63129d17712b59b5ff4d99e2dee3a1dd1e617324b9e4fd3f` |
| `datadog-static-analyzer.status` | 11,456 | `db6508059bbb9b3d276e4e361a4b29994daa841ea34d4334b4caac03d19ed644` |
| `datadog-static-analyzer.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `datadog-static-analyzer.stdout.log` | 4,033 | `eb2b84ff6863e7fc61736d3d69c6c556e0f9c17f6b45126714b92511737bb817` |
| `dependency-check.status` | 22,479 | `b19bf3964e89efeefa078f08a825f3d59431586e806771b84e24148ad850fa4c` |
| `dependency-check.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `dependency-check.stdout.log` | 57,061 | `598d4969628d5264e24258785725fadfd2e5f16a4031b5a644155bdfcf4bebd4` |
| `gate-record.json` | 135,586 | `74d34b1c4a0080881344ed2f40fe19cb837c39a55eeba6ab16fb4dd51a113a1a` |
| `gitleaks.status` | 11,922 | `a0ab3a8681638e34adc2e8f49bba51964654d38675849385619403322afbd7cd` |
| `gitleaks.stderr.log` | 27 | `0a726625c9ba4a8f2b0eefe36d323364dfe24ec01fa7227e50d05c6daadce00c` |
| `gitleaks.stdout.log` | 2,547 | `ce00b09c132d1805f6b8a249de1c0e8f74a614040da9c8ec80d6407bcf59432e` |
| `joern.preflight.log` | 16,443 | `acb4a045d6ebdaee98cab09088fdcea5b8753df81ee8d9bdb845632124b9a59a` |
| `joern.runner.console.log` | 1,428 | `53c18a17aba88510d0974b92094468071c909faa6f01a39ae484f6e4e763b82b` |
| `joern.status` | 29,999 | `a9d6ea84387f023964ea085b10764df90db41242b68277401f04864b22d62e29` |
| `joern.stderr.log` | 699 | `17fb236e3b11d0489f158f27b5b632cbb10e1cebb670e262dfa8b3e42ea197fc` |
| `joern.stdout.log` | 16,444 | `4962e35b76e8c6d6070eb6a2a0a99470523af56819c75aba1373687af8fad83d` |
| `maven-preflight.log` | 30,606 | `5466852a11b2393d213985a90d0e76241209dec08ae5ce44e6c97f7ddce7fe0e` |
| `normalize-run.json` | 813,452 | `2528cddd35f295622ce68319dcf81005224a404db874b3f28686d71eda5bd432` |
| `opengrep.status` | 16,169 | `a6884d8a4592de32616fe27a20dbc4c6081c8f2205b27d472aa746b67537e097` |
| `opengrep.stderr.log` | 2,560 | `975f9e32be664d7bd4f5009cbe5d17236bfd19c8669138a3aeb5c14165854e0a` |
| `opengrep.stdout.log` | 963 | `447a2b8a980e80b678b273060069a0f1cb6eefaaf36158f8b038b03585737fdf` |
| `osv-scanner.status` | 21,647 | `503da2b35e245b520286a16fb1c9b34e10bd1d8b54a3b124c231cf804840094e` |
| `osv-scanner.stderr.log` | 969 | `021347c72dcd98e06b26c579164cded04c26b0eacc203aff07d5eb0487f2c401` |
| `osv-scanner.stdout.log` | 39,545 | `16888631f684d1f9183a3a2028a419557acfb30d75a1f6761e7459ea4977a783` |
| `probe-01-callgraph-unguarded-driver-launch.log` | 114,024 | `630bd02a6d6b53523c4bb1bcf0d2c063d29db5fffbd4f852c2949e0526121a5e` |
| `probe-02-dataflow-unguarded-driver-launch.log` | 112,190 | `6de3598c1085629a98a3132f4d897e87e582b248d861c2f840f9d4dae6d06f7c` |
| `probe-03-parameterized-handler-sink-pairs.log` | 192,829 | `9200c34eb89aaab20bc3f480468bd4f470e634cf8fee4ffae7ce28412d7c1ffb` |
| `runner-metadata.json` | 108,542 | `d7ac82dbb7c990c3e5821eb43c9200319459c9556f81d63d89bd2962a8187dc4` |
| `semgrep.status` | 19,831 | `aba87af3b0d4d034237615112ffeeda65f494a26b075c7f701c871593df3f2b3` |
| `semgrep.stderr.log` | 5,079 | `ba65027215fa502812b57f0e42d02f8a160af187b932ea87aa9738970dbc0159` |
| `semgrep.stdout.log` | 40,661,230 | `96c7eb268cbc17eea688bbd3836b477a3846095f0dcaade7ff45451ac63b523d` |
| `taint-ab-off.log` | 56,226 | `ad090ef483beffc2fe377d79ab5f8a16af9ac7bbd847bd79f130bfdf5f90b86f` |
| `taint-ab-off.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `taint-ab-on.log` | 45,639 | `f5b89c1f10bcc983d7bd389b43b9b0669041989421267ac4a4d541f3525de699` |
| `taint-ab-on.sarif` | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` |
| `trivy.status` | 27,014 | `6a8fc4f4c327076d31197f9993b4355a27359bb3b02e1219f38b35fde472ea2d` |
| `trivy.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `trivy.stdout.log` | 2,756 | `d5371915e604aef6c3087f10f4c6ea01f81a194cc6448f40c8d553538cf88428` |

**Totals: 54 files, 173,817,102 bytes.** The two `taint-ab-*.sarif` entries share
one digest because the two arms' outputs are byte-identical, which is the
measurement divergence D2 rests on rather than a manifest error. The **four**
zero-byte entries — `checkov.stderr.log`,
`datadog-static-analyzer.stderr.log`, `dependency-check.stderr.log` and
`trivy.stderr.log` — carry the sha256 of the empty string, as they must.

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
| 2 — Graph | complete, with two conditions carried | inventory reconciled and staged 191/191 with the mapping total and injective both ways; `importCpg` verification load exit 0 reporting 1,397,339 methods; per-module coverage measured on two agreeing axes. **The graph was not written by this run (D1) and its input set is narrower than the build (D3)**, both reported and not repaired. **The taint A/B did not discriminate (D2)**, reported and not repaired |
| 3 — Nine runners | complete | all nine invoked directly, individually, with no arguments and through no orchestrator; eight artifacts written; `osv-scanner` completing with its own stated reason and no artifact |
| 4 — Normalization | complete | 9,433 rows, `10018 = 9433 + 585`, typed comparison over 113,196 fields with no mismatch, row validation with zero violations, exit 0; 577 adapter and reconciliation tests passing |
| 5 — Probe | complete | three bounded hand-written queries run under `importCpg` only, six result files, all three effort measures answered, parameterizability passing on an invocation that was actually made |
| 6 — Record | complete with this file | the eight result deliverables and the three deliverable trees all exist ([§11](#11-deliverable-inventory-with-resolved-absolute-paths)), and both artifact trees are published by manifest ([§16](#16-manifest-of-the-two-git-ignored-artifact-trees)) |

**Two halt-class findings are on the record and neither was repaired**: D1, the
graph not being this run's own output, and D2, the taint A/B not discriminating on
the mandated subject. AAP §0.8.1 settles which way that tension resolves — report
the condition, never repair it silently — and nothing was installed, rebuilt,
trimmed, overwritten or averaged to clear either. Both are stated at the top of the
documents that own them rather than in a footnote, and both are carried here with
both values.

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
