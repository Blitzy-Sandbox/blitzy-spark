# Build record — the pinned Apache Spark tree, its full-reactor build, and the graph over its bytecode

**Subject.** Apache Spark at the pinned commit `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` of
`https://github.com/blitzy-public-samples/blitzy-spark` — Spark `4.1.0-SNAPSHOT`, Scala `2.13.17`,
`java.version` 17, `maven.version` 3.9.11, read from that tree's own root pom at `pom.xml:29`,
`pom.xml:178`, `pom.xml:120` and `pom.xml:123`.

## What this file is, and what it owns

This is the build-and-graph provenance record for the pinned tree. It is the **owner** of exactly three
measurements:

1. **the build's own identity and wall clock** — section 2 and section 3,
2. **the per-project JAR outcome for all 40 reactor projects** — section 3, and
3. **the per-module graph coverage verdict with its evidence** — section 6.

`oss-scan-results/run-record.md` indexes all three and must not substitute for any of them. In the
other direction, this file states no per-tool finding count, no severity mapping and no scanner
outcome: those belong to `tool-status.md`, `severity-map.md` and the dataset, and nothing here bears on
them.

**The build measurement, in the exact form any other document must cite it.** One build, one
measurement, from `harness/artifacts/logs/build-reactor.log` STEP 11:

| Field | Value |
| --- | --- |
| Result and exit code | `BUILD SUCCESS`, exit **0** |
| Maven's own duration | `Total time:  40:55 min` |
| Maven's own finish time | `Finished at: 2026-08-30T20:59:38Z` |
| The runner's independent measurement of the same build | elapsed **2460 s** |
| Selector | none — `MODULE_SELECTOR_PRESENT=0`, the full 40-project reactor |

`40:55 min` and `2460 s` are the same build measured two ways: Maven's own summary line reports
**2,455 s** of Maven time, and the runner's wall clock brackets the whole `./build/mvn` invocation
including the wrapper and JVM startup, so it reads five seconds longer. **No other duration for this
build exists in this run's evidence**, and any figure elsewhere that differs from these two is not a
second measurement of this build. STEP 11 prints exactly the three values above and no start timestamp,
so none is stated anywhere in this file.

**One further measurement is recorded here without being owned here.** The graph stage's second pass
condition, the Opengrep taint A/B, is measured arm by arm in **section 8**, because it is a graph-stage
measurement and this is the graph-stage record. Its run-level divergence entry is **D2** in
`oss-scan-results/run-record.md` §13 and its run-level narrative is that file's §7; section 8 states the
verdict and the evidence and defers the register entry, so the two are one account cited twice rather
than two accounts.

**Every figure below is one measurement, cited from the producer log that made it, never a second
measurement of the same thing.** Nothing here was re-derived by running anything again, and no number
was computed by this document from figures in another one. Where a value could not be established, it
is named as not established rather than omitted.

## Governing constraints

**No user-specified rules were provided for this project.** `review_rules` returns exactly the single
line *"No user rules provided."*, and that is the complete document rather than a truncated read; the
Agent Action Plan corroborates the absence in §0.7 and §0.10.2. Enterprise-standard documentation
practice is substituted, and the absence is not read as licence to lower the bar. Everything cited in
this file as a constraint is therefore an AAP **requirement**, cited by section — not a rule, and no
rule is invented anywhere. Where the phrases *the authority rule*, *the halt rule* and *the
record-and-continue rule* appear below, they are the Agent Action Plan's own names for three of those
requirements, and nothing in this file is a user-specified rule.

## The producer records — the only sources of fact in this file

| Producer record | What this file takes from it |
| --- | --- |
| `harness/artifacts/logs/maven-preflight.log` | the Maven pre-check verdict (section 1) |
| `harness/artifacts/logs/build-reactor.log` | the build command, the JVM major and Maven version used, the reactor's project count and build order, the per-project `SUCCESS`/`FAILURE`, the build's own wall clock and exit status, and the on-disk per-project artifact outcome (sections 2 and 3) |
| `harness/artifacts/logs/cpg-frontend.log` | this run's frontend invocation over the complete 191-archive staged input set: its serialization failure with the bytecode-level diagnosis, the partial write it refused, the mitigations examined, and the observed overwrite and AST-creation-failure metrics measured over that complete set (sections 4 and 5) |
| `harness/artifacts/logs/cpg-ceiling-reverify.log` | this generation's own first-hand re-verification of that serialization ceiling, at two heaps (section 5) |
| `harness/artifacts/logs/cpg-input-inventory.json` | the input set of the graph the Joern stages actually load — its 62 archives with their digests, the 31 reactor projects present in it, the 7 absent, and the per-module witness computation (sections 4 and 6) |
| `harness/artifacts/logs/cpg-identity.txt` | the one record of account for the graph's identity and its provenance (STATUS, sections 5 and 6) |
| `harness/artifacts/logs/cpg-verify.log` | the `importCpg` verification load of exactly those bytes: the three counts against their expected values, and the per-module coverage witness queries (sections 5 and 6) |
| `harness/artifacts/logs/joern-preflight.log` | the Stage 3 identity gate's comparison of the graph against its record of account — its verdict, and the time and clone it ran in (section 5) |
| `harness/artifacts/logs/joern.runner-console.log` | the Stage 3 Joern runner's own recompute of the graph's byte size and digest at load time, with the invocation header that brackets it (sections 5 and 7) |
| `harness/artifacts/logs/runner-sequence.json` | cited once, for one value this file does not own: which invocation the Stage 3 console log, artifact, streams and status file belong to (section 7) |
| `harness/artifacts/logs/gate-record.json` | cited twice, for two values this file does not own: the gate verdict, and the environment-record graph-identity contradiction (STATUS, sections 5 and 7) |
| `harness/cpg/spark.cpg` | the graph at the path the AAP names — a 33-byte provisioned symlink whose resolved target is host-global and was written by provisioning, not by this run (STATUS, section 5) |

Two absences are stated rather than left to be noticed. No
`harness/artifacts/logs/build-<module-path>.log` exists, and none is cited: section 3 records why none
was needed. And `harness/artifacts/logs/cpg-module-coverage.json` is present in the tree but is not an
**independent** source of fact here: it is a machine-readable rendering of the same two measurements
section 6 reads — `cpg-input-inventory.json`'s per-module witness exclusivity joined against
`cpg-verify.log` PHASE 2's witness queries — so every figure it carries is one of those measurements
cited a second time rather than a second measurement of the same thing. It agrees with section 6 row
for row: 31 modules in the graph input, 26 COVERED on injective evidence, 5 NO VERDICT OBTAINABLE, and
**0** verdicts resting on presence or on a shared package prefix, against the graph identity section 5
states. Its own `schema_version` is `2` and its `supersedes` field names what it replaced: an edition
written in clone `w-001` on `2026-08-31T16:11:52Z` describing a graph of 605,687,359 bytes and sha256
`ceefe60e58308ffcfc1d93f8ed6226bf25bac85678f1a54caf826340a25542a6` over 39 producing modules — a graph
that is not the file on disk, and a module count that is not this input set's.

## STATUS — read this before any graph number below

> **UNMET REQUIREMENT, ATTEMPTED AND BLOCKED. The graph AAP §0.5.1 mandates — one graph created by
> this run over every JAR the build produced — does not exist and cannot be produced by the pinned
> frontend.** This run assembled the complete 191-archive input set, asserted it, and invoked the
> frontend over all of it; the frontend built the graph in memory and then could not write it, because
> flatgraph serializes the entire deduplicated string pool through a single `ByteArrayOutputStream`
> whose backing array the JVM caps at `Integer.MAX_VALUE - 8` = 2,147,483,639 elements. The only
> lever that would clear that bound is excluding inputs, which AAP §0.3.2 forbids and §0.9.2 names as
> a condition that **stops the run rather than gets repaired**; upgrading the frontend is forbidden by
> §0.4.3. **The requirement is therefore stated here as unmet — not as satisfied, and not as an
> exception.** Two consequences follow and neither is softened anywhere in this file: **no graph
> written by this run exists**, and **every count and every coverage verdict below describes the graph
> provisioning wrote**, which carries the bytecode of 31 of the 38 JAR-packaging projects and of no
> others.

Five facts bound what the numbers in this file describe, and reading a coverage figure without them
would misread it.

- **The full-reactor build was performed by this run.** `build-reactor.log` records it end to end:
  `BUILD SUCCESS`, Maven exit code 0, 40 of 40 reactor projects `SUCCESS`, no `-pl` and no module
  selector of any kind, and all 38 JAR-packaging projects confirmed on disk to have produced their own
  main artifact. Section 3 owns that outcome.
- **The graph every stage of this run loaded was written by PROVISIONING, on 2026-08-30T19:18:37Z, not
  by this run.** Its identity is one pair — **541,309,809 bytes**, sha256
  `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` — and
  `harness/artifacts/logs/cpg-identity.txt` is the single record of account for it. That record was
  produced by calling `harness/lib/preflight_graph_identity.py`'s own `record_of_account()`, the same
  function the Stage 3 identity gate calls, so the record and the gate cannot state different pairs.
  That function prefers this checkout's own frontend write-time pair and falls back to the record
  written beside the graph at write time (`/opt/blitzy-harness/provision-log/cpg-identity.txt`,
  corroborated by `cpg-record.txt`, both read and in agreement) — and the fallback applied here for
  one reason only: this run's frontend produced no graph, so there was no write-time pair of its own
  to prefer. The pair was re-measured from the bytes on disk for every one of the five loads and every
  check is logged, and **four of the five comparisons ran immediately before the load they gate**: the
  Stage 2 verification load (`cpg-verify.log:47-50`, "GRAPH IDENTITY, RE-VERIFIED IMMEDIATELY BEFORE
  THE LOAD") and each of the three Stage 5 probe queries
  (`harness/artifacts/logs/probe-*.identity.txt`).
- **For the Stage 3 Joern runner the measurement was contemporaneous and the comparison was not, and
  both halves are stated wherever either is.** The runner recomputed the size and digest itself and
  printed `cpg bytes : 541309809` / `cpg sha256 : 4616845a…4730c7` inside its own
  `2026-09-01T14:25:10Z → 14:41:24Z` invocation (`joern.runner-console.log:14-15`, from
  `harness/bin/run-joern.sh:57-58`). The comparison of that pair against the record of account —
  `joern-preflight.log`, **VERDICT: PASS** — is stamped `2026-09-01T14:52:54Z` with `Clone index 0`,
  about **11.5 minutes after that load ended and from a different clone**. So AAP §0.8.2's
  "immediately before every load" is **not** satisfied for that one load, and this file does not claim
  it is. The resolved file's mtime precedes every check and all five checks state the one pair above, so
  no substitution occurred: the control ran late, the outcome is sound, and section 5 states both.
- **The requirement that this run create that graph is unmet and unmeetable at this pin**, for the
  measured reason the blockquote states and section 5 evidences from the failing method's own
  bytecode and from a two-heap re-verification. It is published as a divergence, carried in the run's
  divergence register in `oss-scan-results/run-record.md` §13 under the label **D1**, which the
  producer records themselves use. Nothing in this file repairs it and nothing substitutes for it:

  - **What was attempted.** This run assembled the complete 191-archive input set its own full-reactor
    build produced, proved a 128 GiB heap committable at the value used, and invoked the pinned
    frontend over the whole of it under JDK major 21 with `--recurse` and no exclusion flag of any
    kind. After **8 h 01 m** (28,863 s) and a **113.3 GiB** peak RSS against a 128 GiB heap, the
    frontend terminated in its persistence step with
    `java.lang.OutOfMemoryError: Required array length 2147483639 + 72 is too large`, raised inside
    `flatgraph.storage.WriterContext.finish` (`Serialization.scala:174`, appending at `:176`).
    **No graph was produced.** The truncated partial write it left behind — `691,541,019` bytes,
    sha256 `b1559c930a7b9ced717a0babf9a7e172d2b93d2cdef45a959304f063aedfe408`, named
    `spark.cpg.PARTIAL-TRUNCATED-DO-NOT-LOAD` — was recorded as evidence and **explicitly not
    accepted**; it was never linked at `harness/cpg/spark.cpg` and no stage loaded it.
    (`cpg-frontend.log` STEPS 2, 4, 5, 8 and 9.)
  - **Why no heap clears it, established by measurement rather than by argument.**
    `cpg-ceiling-reverify.log` re-ran the ceiling probe in this clone at **`-Xmx64g`** and at
    **`-Xmx128g`**: both threw `java.lang.OutOfMemoryError: Required array length 2147483639 + 77 is
    too large` with 2,147,483,639 bytes already buffered, while the JVM's reported `maxMemory` doubled
    from 68,719,476,736 to 137,438,953,472 bytes. **The failure point did not move by one byte.** The
    bound is on one array's length, not on the heap, and it scales with the total UTF-8 size of the
    graph's distinct strings — that is, with the breadth of the input set.
  - **Why it is not repaired.** `cpg-frontend.log` STEP 10 enumerates every mitigation against the
    frontend's actual flag surface. The only lever that would work is excluding inputs
    (`--exclude`, `--exclude-regex`, dropping pre-shade / `-tests` / shims artifacts, or bounding
    `--depth`), and AAP §0.3.2 forbids trimming the input set while §0.9.2 lists it among the
    conditions that stop the run rather than get repaired. A frontend or flatgraph build whose writer
    chunks the string pool would clear it, and AAP §0.4.3 forbids installing, upgrading or
    substituting any tool. So the input set the AAP mandates and the writer the pinned frontend ships
    are not simultaneously satisfiable on any host at this pin.
  - **Why nothing was written to the AAP's path.** `/opt/blitzy-harness/cpg/spark.cpg` is host-global
    and read by concurrent clones while they scan. Writing there would corrupt siblings' in-flight
    loads; and with no valid current-run graph to install, there was in any case nothing to install.
- **The gate that precedes all of this is not this file's verdict, and this file declares none.**
  `harness/artifacts/logs/gate-record.json` records 43 checks — 38 `pass`, 3 `recorded_difference`,
  2 `halt` — with `gate_verdict.overall` = `"halt"`, authorising nothing. One of those two halts is
  the environment record's graph identity contradicting the filesystem, which section 5 states with
  both values. That record and `oss-scan-results/run-record.md` own the gate; this file records
  measurements and cites the verdict rather than restating or softening it.

**The consequence lands on section 6, and it is stated there as a limit rather than worked around.**
Because the graph that loads was built over a narrower input set than this run's build produced, seven
of the 38 JAR-packaging projects have **no bytecode in it at all**, and five of the 31 projects that do
own **no injective witness of either kind the AAP permits**. All twelve are reported as **NO VERDICT
OBTAINABLE**, each with what was tried and why it is unobtainable. No third kind of evidence is
admitted, and no narrower graph is presented as a substitute for the one the AAP mandates.

**A third coverage statement exists, belongs to the graph on disk, and is attributed to its own
record.** `cpg-graph-record.log` carries the coverage verdict the provisioning invocation measured over
its own 62-JAR input set — **31 of 31 contributing modules covered, 0 missing**, 26 of them by a class
unique to the module and 5 by a named weaker witness. That denominator is *contributing modules*, not
*JAR-producing modules*, so it is not the same measurement as section 6's first column and is never
totalled with it. Section 6 names it and its denominator before its own verdict table, and section 7
adjudicates the environment record's copy of it against that verdict.

---

## 1. The Maven pre-check verdict — no download occurred, and the branch that would have caused one was unreachable

Every value in this section is `harness/artifacts/logs/maven-preflight.log`'s measurement.

| What was established | Value | Where in the log |
| --- | --- | --- |
| `<maven.version>` required by the pinned root pom, extracted with the wrapper's own pipeline | `3.9.11` — `pom.xml:123` | STEP 2 |
| Tree the pom was read from | the build tree, `git rev-parse HEAD` = `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, working tree clean | STEP 1 |
| Wrapper's early-return candidate `build/apache-maven-3.9.11/bin/mvn` | **does not exist** — `test -f` reported absent; and `ls -d build/apache-maven*` found no distribution of **any** version | STEP 4 |
| Resolvable `mvn` | `/usr/local/bin/mvn` | STEP 5 |
| Detected version, by the wrapper's own extraction — the third whitespace-separated field of the first line of `mvn --version`, taken with the wrapper's own two-stage pipeline | `3.9.11` — the full banner reads `Apache Maven 3.9.11 (3e54c93a704957b63ee3494413a2b544fd3d825b)`, Maven home `/opt/blitzy-tools/apache-maven-3.9.11` | STEPS 5 and 6 |
| Both version tokens present and well-formed before use | yes — both normalize to a nine-digit token, which an empty or malformed value could not | STEP 7 |
| Normalized comparison, with `version()` defined exactly as the wrapper defines it and run as the wrapper runs it | required `003009011`, detected `003009011`; `[ 003009011 -ne 003009011 ]` exited **1**, so `DOWNLOAD_BRANCH_TAKEN=no` | STEP 7 |
| JDK the build would run under | `java.specification.version` = **17**, `/opt/blitzy-tools/jdk/jdk-17.0.20+8`, Temurin-17.0.20+8, and Maven reports the same runtime | STEP 8 |
| Verdict | **PASS — no download would be triggered** | STEP 9 |

**Why no download occurred.** `build/mvn`'s `install_mvn()` has exactly two ways not to download.
`build/mvn:117` reads `<maven.version>` from the root pom; `build/mvn:118` builds the candidate path
`build/apache-maven-<version>/bin/mvn` and `build/mvn:119`–`121` return early if that file exists;
otherwise `build/mvn:122` resolves `mvn` from `PATH`, `build/mvn:124` extracts its version, and
`build/mvn:126` downloads the `apache-maven-<version>-bin.tar.gz` tarball named at `build/mvn:127`
when the two normalized versions differ. The early return was **not** taken — no distribution exists
under `build/`, of the pinned version or any other — so the conditional at `build/mvn:126` is what
decided the outcome, and it evaluated false because the detected and required versions are equal
under the wrapper's own normalization. The download branch was therefore **unreachable**, established
before the build ran rather than observed afterwards not to have fired. Nothing was installed,
upgraded or substituted.

**This is the one version difference in the whole run that would not have been recorded and tolerated.**
AAP §0.4.3 and §0.8.3 settle it: continuing past a Maven mismatch would require the wrapper to install
a distribution, which the run is forbidden to do, so a detected version other than `3.9.11` with no
pinned distribution already present **halts and reports both versions** rather than proceeding. AAP
§0.9.2 states the halting condition in those terms, and AAP §0.9.3 carves Maven out of the
record-and-continue rule for exactly this reason. The condition did not arise.

For completeness, `maven-preflight.log`'s closing block also read the working checkout's pom once as a
labelled contrast — `3.9.12` at the branch tip against `3.9.11` at the pin — and used it for nothing.
The working checkout is neither built nor scanned (AAP §0.3.2).

---

## 2. The build command, and why five flags add four modules

**The invocation, quoted verbatim from the script that ran it** — `build-reactor.log` STEP 10 prints
the exact lines it `sed`-extracted from that lane's own `work/01-build.sh`, in scratch outside every
repository checkout:

```bash
./build/mvn --no-transfer-progress -DskipTests \
  -Pyarn -Pkubernetes -Phive -Phive-thriftserver -Pvolcano \
  -Dmaven.repo.local="$B/m2" \
  package > "$B/build-reactor-verbatim.log" 2>&1
```

`$B` is that lane's build directory: STEP 9 resolves `$B/m2` to
`/tmp/blitzy/scratch/f38258d3-f87d-44f5-bedc-af512c69e0ab/w-005/build/m2` when it inventories the
private repository, and STEP 11 reads Maven's own summary out of `$B/build-reactor-verbatim.log`.

**The reactor was not narrowed.** `build-reactor.log` STEP 10 quotes the invocation verbatim from the
script that ran it and then greps that script for a module selector, reporting
`MODULE_SELECTOR_PRESENT=0` — no `-pl`, no `-am`, no selector of any kind. The same step lists the
profile flags it found — `-Phive -Phive-thriftserver -Pkubernetes -Pvolcano -Pyarn` — which is exactly
the five mandated flags and nothing else, and reports `SKIPTESTS_PRESENT=1` with `TEST_GOAL_PRESENT=0`:
**no Spark test suite was executed, in any language.**

Two additions to the mandated form are recorded rather than left to be noticed.
`--no-transfer-progress` suppresses per-artifact transfer chatter and changes no goal, profile or
module. `-Dmaven.repo.local=…` points the build at a private byte-exact copy of the primed local
repository, because a full reactor resolves coordinates the narrowed provisioning build never needed
and building against the shared `/root/.m2/repository` would have written into a path concurrent
clones read. `build-reactor.log` STEP 9 establishes that the shared repository was **not** written by
this build: the private copy was taken before the build and now holds **6,014 files and 901 jars**,
while the shared tree still holds **5,823 files and 867 jars** — every coordinate the full reactor
resolved beyond the primed set landed in the private copy.

**Where the build ran.** In a private clone of the pinned commit at
`/tmp/blitzy/scratch/f38258d3-f87d-44f5-bedc-af512c69e0ab/w-005/build/spark-src`,
checked out **by SHA** and proved equal to the shared pinned clone: `git rev-parse HEAD` equals the
pin, the working tree is clean, and the sha256 of the sha256sums of every tracked file matches
`/opt/spark-src`'s, reported as `BUILD_TREE_MATCHES_SPARK_SRC=yes` (`build-reactor.log` STEP 4). The
shared clone was left untouched because it is read concurrently and carries only a **narrowed**
build — STEP 3 records **zero** archives under `tools`, `examples`, `connector/kafka-0-10` and
`connector/kafka-0-10-sql` there, which is why a full-reactor question cannot be answered from it.
Every artifact path in this file is therefore relative to that build tree, not to `SPARK_SRC`.

**Runtime versions actually used** (`build-reactor.log` STEP 7 and STEP 8, and cited from
`maven-preflight.log` for Maven): JVM **major 17** — `java.specification.version` = `17`,
Temurin-17.0.20+8 at `/opt/blitzy-tools/jdk/jdk-17.0.20+8`, read as the machine property rather than
parsed from a banner — and Maven **3.9.11** from `/usr/local/bin/mvn`, the same binary the pre-check
cleared.

### Five flags, four modules — the arithmetic, verified against the pinned root pom

Read without this arithmetic, "five flags, four modules" looks like an error. It is not: three flags
expand the reactor and two do not.

**Three root profiles that add child modules — four modules from three flags:**

- `-Pyarn` — `pom.xml:3384` — adds **two** modules: `resource-managers/yarn` (`pom.xml:3386`) and
  `common/network-yarn` (`pom.xml:3387`). One flag, two modules: that is the whole of the apparent
  oddity.
- `-Pkubernetes` — `pom.xml:3392` — adds `resource-managers/kubernetes/core` (`pom.xml:3394`).
- `-Phive-thriftserver` — `pom.xml:3407` — adds `sql/hive-thriftserver` (`pom.xml:3409`).

**Two module-local profiles that add nothing to the reactor and change what gets compiled:**

- `-Phive`. There is **no root profile with the id `hive`** — grepping the root pom for
  `<id>hive</id>` returns nothing, and the root's hive-adjacent profile is `hive-provided` at
  `pom.xml:3520`. `sql/hive` is listed **unconditionally** at `pom.xml:98`, so it needs no profile to
  enter the reactor. `-Phive` resolves inside `sql/hive/pom.xml`, at line 209, and dropping it changes
  what `package` compiles there. Removing it as a "correction" would be introducing a change.
- `-Pvolcano`. There is no root `volcano` profile either. It resolves in the Kubernetes module: the
  profile is declared at `resource-managers/kubernetes/core/pom.xml:36`, its build-helper execution
  `add-volcano-source` at line 56, and the source root it adds —
  `<source>volcano/src/main/scala</source>` — at line 63. Those are the **pinned** tree's line
  numbers; the same three constructs sit at `:36`, `:59` and `:66` in the working checkout's tip, and
  `build-reactor.log` records that one-line pin-versus-tip drift explicitly. Both are true of their
  own tree and neither corrects the other.

**Why omitting any flag matters.** Omit one of the three expanding flags and there is no bytecode for
the YARN shuffle service, the Kubernetes resource manager or the Thrift server. Omit `-Pvolcano` and
the in-scope `VolcanoFeatureStep` is never compiled and never reaches the graph — `build-reactor.log`
STEP 6 confirms the source root exists in the built tree and that `VolcanoFeatureStep.scala` is inside
it. In every one of those cases a later graph query returns nothing for the module, **which reads
exactly like a clean result**. That is why no flag is dropped even where the reactor would still build
without it.

---

## 3. The 40-project outcome

### The reactor arithmetic, verified against the pinned root pom

```text
  35   unconditional child modules            pom.xml:79-113, inside <modules> at pom.xml:78-115
+  4   profile-added child modules            the three root profiles in section 2
-----
  39   child modules
+  1   the root parent project itself         pom.xml:30 <packaging>pom</packaging>
-----
  40   Maven projects in the reactor
```

Walking every child pom for `<packaging>`, **only `assembly` is `pom`**; the root parent is `pom` as
well, at `pom.xml:30`. So the reactor is **40 Maven projects: 38 packaging a JAR and 2 packaging
none.** `build-reactor.log` STEP 12 checks that pom-derived count against Maven's own Reactor Build
Order and finds 40 entries — 38 marked `[jar]`, 2 marked `[pom]`, the two being
`Spark Project Parent POM` and `Spark Project Assembly`.

### The build's own outcome

| Measurement | Value | Where |
| --- | --- | --- |
| Maven result and exit code | `BUILD SUCCESS`, exit **0** | `build-reactor.log` STEP 11 |
| Wall clock | elapsed **2460 s** as the runner measured it, alongside Maven's own `Total time:  40:55 min` and `Finished at: 2026-08-30T20:59:38Z`. STEP 11 records exactly these three values and no start timestamp, so none is stated here | STEP 11 |
| Reactor summary lines | `SUCCESS [` **40**, `FAILURE [` **0**, `SKIPPED` **0** | STEP 12 |
| Projects enumerated from the poms, independently on disk | **40** — jar-packaging 38, pom-packaging 2 | STEP 13 |
| JAR-packaging projects with their own main artifact on disk | **38 of 38** | STEP 13 |
| JAR-packaging projects with no own main artifact | **(none)** | STEP 13 |

No time limit was imposed anywhere and the elapsed figure is recorded as a fact rather than measured
against a budget (AAP §0.8.1).

Because the reactor holds 40 projects and the summary carries 40 `SUCCESS` lines with 0 `FAILURE` and
0 `SKIPPED`, **every project in the table below is `SUCCESS` in Maven's own summary.** The per-project
column in that table is the stronger, independent statement: the artifact outcome established a second
time from the filesystem by `build-reactor.log` STEP 13, by provenance rather than by location. The
ordering is the enumeration order STEP 13 and the inventory both use — the root parent, then the 35
unconditional modules in the order `pom.xml` lists them, then the four profile-added modules.

| # | Maven project | artifactId | packaging | JAR outcome | primary artifact, relative to the build tree | own artifacts inventoried |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `(root parent)` | `spark-parent_2.13` | pom | **produced none — EXPECTED, `packaging=pom`** | — none | 1 |
| 2 | `common/sketch` | `spark-sketch_2.13` | jar | produced its own main artifact | `common/sketch/target/spark-sketch_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 3 | `common/kvstore` | `spark-kvstore_2.13` | jar | produced its own main artifact | `common/kvstore/target/spark-kvstore_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 4 | `common/network-common` | `spark-network-common_2.13` | jar | produced its own main artifact | `common/network-common/target/spark-network-common_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 5 | `common/network-shuffle` | `spark-network-shuffle_2.13` | jar | produced its own main artifact | `common/network-shuffle/target/spark-network-shuffle_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 6 | `common/unsafe` | `spark-unsafe_2.13` | jar | produced its own main artifact | `common/unsafe/target/spark-unsafe_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 7 | `common/utils` | `spark-common-utils_2.13` | jar | produced its own main artifact | `common/utils/target/spark-common-utils_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 8 | `common/utils-java` | `spark-common-utils-java_2.13` | jar | produced its own main artifact | `common/utils-java/target/spark-common-utils-java_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 9 | `common/variant` | `spark-variant_2.13` | jar | produced its own main artifact | `common/variant/target/spark-variant_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 10 | `common/tags` | `spark-tags_2.13` | jar | produced its own main artifact | `common/tags/target/spark-tags_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 11 | `sql/connect/shims` | `spark-connect-shims_2.13` | jar | produced its own main artifact | `sql/connect/shims/target/spark-connect-shims_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 12 | `core` | `spark-core_2.13` | jar | produced its own main artifact | `core/target/spark-core_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 13 | `graphx` | `spark-graphx_2.13` | jar | produced its own main artifact | `graphx/target/spark-graphx_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 14 | `mllib` | `spark-mllib_2.13` | jar | produced its own main artifact | `mllib/target/spark-mllib_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 15 | `mllib-local` | `spark-mllib-local_2.13` | jar | produced its own main artifact | `mllib-local/target/spark-mllib-local_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 16 | `tools` | `spark-tools_2.13` | jar | produced its own main artifact | `tools/target/spark-tools_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 17 | `streaming` | `spark-streaming_2.13` | jar | produced its own main artifact | `streaming/target/spark-streaming_2.13-4.1.0-SNAPSHOT.jar` | 6 |
| 18 | `sql/api` | `spark-sql-api_2.13` | jar | produced its own main artifact | `sql/api/target/spark-sql-api_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 19 | `sql/catalyst` | `spark-catalyst_2.13` | jar | produced its own main artifact | `sql/catalyst/target/spark-catalyst_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 20 | `sql/core` | `spark-sql_2.13` | jar | produced its own main artifact | `sql/core/target/spark-sql_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 21 | `sql/hive` | `spark-hive_2.13` | jar | produced its own main artifact | `sql/hive/target/spark-hive_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 22 | `sql/pipelines` | `spark-pipelines_2.13` | jar | produced its own main artifact | `sql/pipelines/target/spark-pipelines_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 23 | `sql/connect/server` | `spark-connect_2.13` | jar | produced its own main artifact | `sql/connect/server/target/spark-connect_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 24 | `sql/connect/common` | `spark-connect-common_2.13` | jar | produced its own main artifact | `sql/connect/common/target/spark-connect-common_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 25 | `sql/connect/client/jdbc` | `spark-connect-client-jdbc_2.13` | jar | produced its own main artifact | `sql/connect/client/jdbc/target/spark-connect-client-jdbc_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 26 | `sql/connect/client/jvm` | `spark-connect-client-jvm_2.13` | jar | produced its own main artifact | `sql/connect/client/jvm/target/spark-connect-client-jvm_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 27 | `assembly` | `spark-assembly_2.13` | pom | **produced none — EXPECTED, `packaging=pom`** | — none | 0 |
| 28 | `examples` | `spark-examples_2.13` | jar | produced its own main artifact | `examples/target/scala-2.13/jars/spark-examples_2.13-4.1.0-SNAPSHOT.jar` | 4 |
| 29 | `repl` | `spark-repl_2.13` | jar | produced its own main artifact | `repl/target/spark-repl_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 30 | `launcher` | `spark-launcher_2.13` | jar | produced its own main artifact | `launcher/target/spark-launcher_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 31 | `connector/kafka-0-10-token-provider` | `spark-token-provider-kafka-0-10_2.13` | jar | produced its own main artifact | `connector/kafka-0-10-token-provider/target/spark-token-provider-kafka-0-10_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 32 | `connector/kafka-0-10` | `spark-streaming-kafka-0-10_2.13` | jar | produced its own main artifact | `connector/kafka-0-10/target/spark-streaming-kafka-0-10_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 33 | `connector/kafka-0-10-assembly` | `spark-streaming-kafka-0-10-assembly_2.13` | jar | produced its own main artifact | `connector/kafka-0-10-assembly/target/spark-streaming-kafka-0-10-assembly_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 34 | `connector/kafka-0-10-sql` | `spark-sql-kafka-0-10_2.13` | jar | produced its own main artifact | `connector/kafka-0-10-sql/target/spark-sql-kafka-0-10_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 35 | `connector/avro` | `spark-avro_2.13` | jar | produced its own main artifact | `connector/avro/target/spark-avro_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 36 | `connector/protobuf` | `spark-protobuf_2.13` | jar | produced its own main artifact | `connector/protobuf/target/spark-protobuf_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 37 | `resource-managers/yarn` | `spark-yarn_2.13` | jar | produced its own main artifact | `resource-managers/yarn/target/spark-yarn_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 38 | `common/network-yarn` | `spark-network-yarn_2.13` | jar | produced its own main artifact | `common/network-yarn/target/spark-network-yarn_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 39 | `resource-managers/kubernetes/core` | `spark-kubernetes_2.13` | jar | produced its own main artifact | `resource-managers/kubernetes/core/target/spark-kubernetes_2.13-4.1.0-SNAPSHOT.jar` | 5 |
| 40 | `sql/hive-thriftserver` | `spark-hive-thriftserver_2.13` | jar | produced its own main artifact | `sql/hive-thriftserver/target/spark-hive-thriftserver_2.13-4.1.0-SNAPSHOT.jar` | 5 |

Every column in that table is `build-reactor.log` STEP 13's own on-disk measurement and nothing else
is cited for it: the `PKG`, `OWN`, `MAIN` and `PRIMARY ARTIFACT` columns of its per-project listing,
plus the totals its verification block prints. The `own artifacts inventoried` column is STEP 13's
`OWN` count of the archives each project itself emitted, and it sums to STEP 13's own total of **191**
— 36 modules at 5, `streaming` at 6, `examples` at 4, the root parent at 1 and `assembly` at 0. The
`primary artifact` column is STEP 13's `PRIMARY ARTIFACT` value, each project's main, unclassified,
non-`original-`, non-`-tests` JAR, which is the artifact the coverage test in section 6 is applied to.

**Two cautions about how this table may be cross-read.** First, every path here is relative to the
build tree named in section 2, not to `SPARK_SRC`. Second, this table is **not** cross-checked against
`cpg-input-inventory.json`, and must not be: that file measures the graph's own **62-archive** input
set from a different tree (section 4), so a project-by-project comparison against it would compare two
different sets. Where the 191 figure is checked a second time, it is checked against `cpg-frontend.log`
STEP 1's independent count of the set handed to the frontend, which agrees. And no digest comparison is
made across trees at all: a JAR entry carries a build timestamp, so identical source yields a different
digest in each tree, which section 4 states with the yarn-shuffle artifact as the worked case.

### The two projects that produced no JAR, and why that is the expected outcome

- **`(root parent)` — `spark-parent_2.13`, `packaging=pom` at `pom.xml:30`.** `build-reactor.log`
  STEP 13 records it as `pom` with `OWN 1`, `MAIN n/a` and the reason in place of a path:
  `no JAR expected: packaging=pom`. Its one own archive is an attached test-jar —
  `[INFO] Building jar: …/target/spark-parent_2.13-4.1.0-SNAPSHOT-tests.jar` in the same log's verbatim
  Maven output — not a main artifact. No size is quoted for it, because STEP 13 records the count and
  not the bytes. The parent produces no main artifact, which is the expected outcome and not a failure.
- **`assembly` — `spark-assembly_2.13`, `packaging=pom` in its own pom**, confirmed both by STEP 6's
  per-module packaging listing and by Maven's own `[pom]` marker at STEP 12. STEP 13 records it as
  `pom` with `OWN 0`, `MAIN n/a` and the same reason, `no JAR expected: packaging=pom`: **not one
  archive under its build directory is its own**. What is there was copied in by
  `copy-module-dependencies` (`pom.xml:3095`, output directory at `pom.xml:3102`), and every such file
  falls into STEP 13's `copied_runtime_dependency` exclusion class, counted in the 422 total in
  section 4 rather than attributed to this project. No per-project figure is quoted, because STEP 13
  publishes that class as a total and not per project.

Neither is a failure, and neither is left to be inferred from an absence.

### All 38 JAR-packaging projects produced their own artifact

`build-reactor.log` STEP 13's independent on-disk pass prints, from its own verification block:
`jar-packaging projects : 38`, `jar-packaging with their own MAIN artifact: 38`,
`jar-packaging WITHOUT one : []`, and then in terms — *"VERDICT: every one of the 38 JAR-packaging
projects produced its own main artifact. The two pom-packaging projects produced none, which is the
expected outcome for packaging=pom and is recorded as expected rather than as a failure (AAP 0.9.1)."*
AAP §0.9.2 makes any of the 38 failing to produce its own artifact a halt, *"including a project the
expected-values list does not name"* — so this file would otherwise have to name each project that did
not and quote its log. **There is no such project, so there is no such entry.**

### The six JAR producers the expected-values table does not name — a recorded difference, never a halt

The expected-values table names 32 JAR producers, and it was measured over a **narrowed** build that
this run did not perform and does not reuse. `harness/ENVIRONMENT.md:205-213` quotes that build's
invocation, and it carries `-pl core,common/network-common,…,resource-managers/yarn -am`; `:217` records
its outcome as *"Reactor = **33 projects: 32 producing a JAR + the parent POM**"*, and its section 6
table at `:231-272` lists all 33 rows — the parent plus exactly those 32. AAP §0.5.1 mandates the full
reactor and §0.3.2 forbids narrowing it, which is why this run built 40 projects with
`MODULE_SELECTOR_PRESENT=0` (section 2) and why the narrowed build is cited here only to explain where
the 32 came from. A full reactor packages 38, so six are new to it — the set difference between STEP
13's 38 and that record's 32:

- `tools`
- `examples`
- `connector/kafka-0-10-token-provider`
- `connector/kafka-0-10`
- `connector/kafka-0-10-sql`
- `connector/kafka-0-10-assembly`

All six are `SUCCESS` in Maven's own summary and each produced its own main artifact on disk: STEP 12
counts **40** `Building … [n/40]` lines, **40** ` SUCCESS [` lines and **0** `FAILURE [`-or-`SKIPPED`
lines, so no project in the reactor is anything but `SUCCESS`; and STEP 13's per-project listing carries
all six with `MAIN yes` and their primary artifact's path, which the table above reproduces at rows 16,
28 and 31 to 34. **The halt rule is one-directional** (AAP §0.8.3): a module that produced a JAR in the
rehearsal and produces none now is a halt; the reverse is not. So the six are a **recorded
difference**, and because the expected-values anchors were measured over 32 JAR producers while this
build packages 38, the graph's method count is checked as a **floor rather than a window** (section 5).
Nothing was trimmed in either direction to make a number fit.

**Where those six actually went, stated exactly, because it is easy to assume.** All six entered the
191-archive set this run's frontend was given (section 4, staged input set 1). **None of them is in the
62-archive input set of the graph that loads**, so none of their bytecode is in that graph — which is
why section 5 records that the six cannot be the explanation for its above-anchor counts, and why
section 6 gives four of them, plus `sql/connect/shims`, `examples` and `tools`, no coverage verdict.

### Seven of the 38 have no bytecode in the graph — a coverage fact, not a build outcome

Kept as its own statement because the two are independent. On the build side all 38 produced their own
main artifact, `tools`, `examples`, `sql/connect/shims`, `connector/kafka-0-10`,
`connector/kafka-0-10-assembly`, `connector/kafka-0-10-sql` and `connector/kafka-0-10-token-provider`
included, each with its path in the table above. On the graph side those same seven have **zero**
archives in the 62-archive input set the loaded graph was built over (`cpg-input-inventory.json`,
`reactor_projects_absent_from_this_input`), so the graph carries none of their compiled code. Section 6
records their coverage verdict as NO VERDICT OBTAINABLE on that ground alone, and this file never reads
their absence from the graph as a build failure or their build success as graph coverage.

### `python/pyspark` — an expected non-JAR outcome, and not one of the 40

`python/pyspark` is one of the twelve authoritative scope roots and is scanned, but it is **no Maven
module and appears in no reactor**: `build-reactor.log` STEP 14 shows the directory present and
`ls python/pyspark/pom.xml` failing with *"No such file or directory"*, and reads it as an expected
non-JAR outcome rather than a build failure. It therefore has no row in the table above and none is
invented for it. STEP 14 records the same for `resource-managers/kubernetes/docker`, whose
`src/main` the file-based tools reach through the mid-path `**` of the Kubernetes glob and whose
`pom.xml` is likewise absent.

### The diagnostic pass — not needed, and the protocol it would have followed

The reactor did not fail, so **no per-project diagnostic pass was run and no
`harness/artifacts/logs/build-<module-path>.log` exists** — the log directory contains no file of that
form, and `build-reactor.log` STEP 12 (40 `SUCCESS`, 0 `FAILURE`/`SKIPPED`) with STEP 13's verdict
(38 of 38 with their own main artifact) is why none was needed. The protocol is recorded
because its absence is a measurement rather than an omission, and AAP §0.5.1 fixes it: Maven's own
printed build order walked front to back;
`./build/mvn -DskipTests -Pyarn -Pkubernetes -Phive -Phive-thriftserver -Pvolcano -pl <module-path> -am package`
per project, with `-am` required because the reactor used `package` rather than `install`, so a
project's SNAPSHOT dependencies are not resolvable from the local repository and would otherwise fail
for a reason unrelated to the project under test; **the failure attributed to the project Maven names
in its reactor summary, not to the project selected by `-pl`**, which is what separates a project's own
compilation failure from an upstream dependency's; a project already attributed an outcome not
re-attributed by a later `-am` rebuild, so each project contributes exactly one entry; and one retained
log per invocation at
`harness/artifacts/logs/build-<module-path-with-slashes-as-underscores>.log`.

**The pass is diagnostic only and confers no licence to reduced coverage.** Had it run, the run would
still halt unless every one of the 38 JAR-packaging projects produced its own artifact.

---

## 4. The JAR inventory, and the two staged input sets kept apart

This section has two subjects and they are never mixed, because at this checkpoint they are different
artefacts measured by different records.

- **What this run's build produced, and what its frontend was given** — the 191 own artifacts of the
  40-project reactor. The per-project outcome and the totals are `build-reactor.log` STEP 13's
  measurement; what was actually handed to the frontend, and the fact that nothing was excluded, is
  `cpg-frontend.log` STEPS 1 and 4.
- **What the graph the Joern stages load was built over** — the 62-archive staging tree
  `/opt/blitzy-harness/cpg-input`, measured member by member in
  `harness/artifacts/logs/cpg-input-inventory.json`. That tree is provisioning's, not this run's, and
  it is the input set every count and every coverage verdict in sections 5 and 6 belongs to.

Nothing here claims the first set reached the graph. It did not: the frontend invocation over it wrote
no graph at all (STATUS).

### The inventory is built by provenance, not by location — and both directions were needed

A project's JAR is not reliably at `target/<artifactId>-<version>.jar`, and the directory that holds
one project's own output also holds other projects' dependencies. Three facts at this pin make
location alone insufficient:

- **`examples` sends its main artifact to `${jars.target.dir}`.** `examples/pom.xml:136` sets the
  `maven-jar-plugin` `<outputDirectory>` to that property, and `pom.xml:265` resolves it to
  `target/scala-2.13/jars`. So the artifact is not in the build directory's root.
- **`common/network-yarn` writes its shaded shuffle JAR outside the root and does not attach it.**
  `common/network-yarn/pom.xml:97` sets the shade plugin's `<outputFile>` to `${shuffle.jar}`, defined
  at `common/network-yarn/pom.xml:37` as
  `${project.build.directory}/scala-${scala.binary.version}/spark-${project.version}-yarn-shuffle.jar`,
  with `<shadedArtifactAttached>false</shadedArtifactAttached>` at
  `common/network-yarn/pom.xml:96`. So it is neither in the build directory's root nor attached as a
  classified artifact.
- **The same directory receives copied runtime dependencies.** `pom.xml`'s
  `copy-module-dependencies` execution (`pom.xml:3095`) writes into `${jars.target.dir}`
  (`pom.xml:3102`) — the very directory `examples` publishes its main artifact to. Location therefore
  cannot separate a project's own output from a dependency's, in either direction.

The inventory is consequently reconciled from two directions: each project's own coordinates and
declared output paths, read from its pom with the first `artifactId` outside the `<parent>` block so an
inherited parent coordinate cannot be mistaken for the project's own; and every `*.jar` under that
project's build directory, enumerated recursively with its size, sha256 and archive contents read from
the file. A file counts as the project's own when its filename stem — after an optional `original-`
prefix — is exactly `<artifactId>-<version>` or begins with `<artifactId>-<version>-`, **or** when its
path is exactly an output path that project's own pom declares. No Maven goal was invoked to obtain the
effective model, because even an offline goal can write into the shared local repository.

### Both non-root output cases, asserted present

| Case | Resolved path | Evidence | Staged as |
| --- | --- | --- | --- |
| `examples` main artifact under `${jars.target.dir}` | `examples/target/scala-2.13/jars/spark-examples_2.13-4.1.0-SNAPSHOT.jar` | declared at `examples/pom.xml:136`, property at `pom.xml:265`; **asserted present** | `examples__spark-examples_2.13-4.1.0-SNAPSHOT.jar` |
| `common/network-yarn` unattached shaded shuffle JAR | `common/network-yarn/target/scala-2.13/spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` — `build-reactor.log` records the build writing it at that exact path (its `[jar] Building jar:` line) | declared at `common/network-yarn/pom.xml:97` with `:96` and `:37`; **asserted present**; classified as the project's own **by its declared output path**, because its filename carries no artifactId and a filename rule alone would have excluded it | `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` |

**No digest is quoted for either artifact here, deliberately.** A JAR embeds entry timestamps, so every
build of the same source yields the same byte size and a different sha256 — and the yarn-shuffle JAR is
the case that proves it: `109,208,027` bytes in every tree on record, under a different digest in each.
A digest is therefore only meaningful with the tree it was measured in, and the only tree whose
per-archive digests bear on any conclusion in this file is the graph's own input set, where
`cpg-input-inventory.json` records this artifact at `109,208,027` bytes, sha256
`ab5f23f67b2131fc852b8122a956610e6c023605041545232c063ff8347c394c`, 11,910 entries of which 11,070 are
class entries.

### The totals of this run's build, and the exclusions counted rather than silent

Every figure here is `build-reactor.log` STEP 13's own measurement, except the two marked as
`cpg-frontend.log` STEP 1's, which measured the set at the moment it was handed to the frontend.

| Measurement | Value | Record |
| --- | --- | --- |
| JAR files enumerated under the 40 projects' build directories | **627** | `build-reactor.log` STEP 13 |
| Classified as a project's own | **191**, totalling **431,184,822 bytes** | `build-reactor.log` STEP 13; the same two figures in `cpg-frontend.log` STEP 1 |
| Of those, carrying bytecode | **110** | `cpg-frontend.log` STEP 1 |
| Class entries across the own artifacts | **99,723** | `cpg-frontend.log` STEP 1 |
| **Excluded copied dependency JARs** | **422** (`copied_runtime_dependency`) | `build-reactor.log` STEP 13, and recorded again as "copied dependencies excluded 422 (recorded, never supplied)" in `cpg-frontend.log` STEP 1 |
| Also excluded: test-resource fixtures inside a module's compiled-output tree | **14** (`test_resource_fixture`) | `build-reactor.log` STEP 13 |
| Undecided provenance | **0** | `build-reactor.log` STEP 13 |
| Arithmetic | own 191 + excluded (422 + 14) + undecided 0 = **627** enumerated | — |

**Nothing the project itself emitted was sampled or dropped.** Main artifacts, `original-` pre-shade
siblings, shaded siblings, classifier artifacts, `-tests` artifacts, `-sources` and `-test-sources`
artifacts, and the unattached shaded shuffle JAR are all retained. The two exclusion classes are named
and counted rather than left silent, and neither removes anything a project itself produced: a copied
runtime dependency carries another project's coordinate, and a test-resource fixture is a JAR checked
into a module's resources rather than emitted by its build.

No per-project breakdown of those exclusions is stated, because no record in this tree carries one.
STEP 13 publishes the four totals above and the per-project **own-artifact** counts, and this file
states exactly what it can cite.

### Staged input set 1 — the 191 archives this run's frontend was given

The bundled `jimple2cpg` accepts **one** input path, so "every JAR the build produced" and "one input
path" are reconciled by staging the inventory into a single directory. Everything below is
`cpg-frontend.log`'s record of that invocation, and the honest state of the evidence is stated with it.

| Property | Value | Record |
| --- | --- | --- |
| Own artifacts supplied | **191**, totalling **431,184,822 bytes** — the complete set, nothing excluded | STEP 1 |
| Carrying bytecode / class entries across them | 110 / 99,723 | STEP 1 |
| Distinct sha256 across the 191 | **189** | STEP 1 |
| Copied dependencies excluded and recorded, never supplied | 422 | STEP 1 |
| Bidirectional staging assertion | recorded result **True**, before the invocation | STEP 1 |
| Staged-name form | `<module-path-with-slashes-as-underscores>__<original-filename>` | STEP 1's staged names, e.g. `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` |
| Exclusion flags used | **none** — "no `--exclude`, no `--exclude-regex`, no `--depth`", `--recurse` as the AAP mandates, stdin closed | STEP 4 |
| Invoked | 2026-08-30T23:21:24.942Z, working directory outside every repository checkout | STEPS 1 and 4 |

**What is no longer measurable, stated rather than implied.** The staging tree itself was written into
the private scratch of the clone that ran the frontend and was removed with it, so `cpg-frontend.log`
STEP 1 records the staged-file count and the manifest-entry count as **not measurable at
log-generation time** (`None`) rather than restating them. **This checkout contains no staging tree**:
`harness/artifacts/` holds `MANIFEST.json`, `logs/` and `raw/` and nothing else, and no
`harness/artifacts/cpg-input*` path is tracked. `harness/artifacts/MANIFEST.json` records the two
staging trees rather than publishing them, under `cpg_input_records`: it names both as
`not_present_in_this_checkout`, names the artifact that owns each, and — stated in its own
`why_no_per_file_entries` — deliberately restates **no per-file number**, because a previous revision's
per-file copies disagreed with their owners. So the record of the 191-archive set is the aggregate its
owners state, and no tree and no per-archive entry is cited as though a reader could walk it.

**Per-archive identity for this set is consequently not retained anywhere in the two trees, and is
named rather than estimated.** The aggregate is established twice over — 191 archives and 431,184,822
bytes in `build-reactor.log` STEP 13 and again in `cpg-frontend.log` STEP 1 — but no name/size/sha256
entry for an individual member of it survives in `harness/artifacts/`: `MANIFEST.json`'s
`regenerated.corrections` records that the 191 per-file entries it once carried were withdrawn together
with their **431,184,903** total, which disagreed with the owners' 431,184,822 by 81 bytes, and
`cpg-input-inventory.json` was regenerated in this generation to describe the 62-archive set instead.
This file therefore states the aggregate and nothing per-archive for the 191, and `run-record.md` §14
carries the loss as a value that could not be established.

**Why the bidirectional form of the assertion is the one that matters.** A set discards multiplicity,
so two different multisets can share both a count and a hash set — and this input set is a live example:
191 files carry only **189** distinct digests. A set-based check would have compared 191 to 191 and 189
to 189 and passed even if one file had been staged twice and another omitted. The assertion recorded is
therefore the one-to-one mapping in both directions, and it was recorded **before** the frontend ran,
so it cannot have been shaped to fit what a frontend happened to ingest.

**That 189 is a count of digests over 191 files, and it is not the other 189 in this run's record.**
`cpg-frontend.log` STEP 1 prints it as `distinct sha256 189` in the assertion block beside the
191-archive supplied set: 191 staged files carrying 189 distinct digests, a shortfall of two, which is
what makes the multiset argument above concrete rather than hypothetical. A different 189 appears in the
withheld-input divergence cross-referenced two paragraphs below — the **number of archives a superseded
attempt supplied**. The two are different measurements that happen to coincide numerically, and neither
is derived from or evidence for the other.

**What this establishes, and what it cannot — and it is a statement about the invocation on record,
not about every attempt this run's lanes made.** For the invocation `cpg-frontend.log` records, it
establishes **delivery**: `cpg-frontend.log` STEP 1 states "Input set actually supplied to the frontend
— the complete set, nothing excluded: own artifacts **191**", **431,184,822** bytes, and STEP 4 records
the staging directory in the **w-005** lane with "nothing excluded: no `--exclude`, no
`--exclude-regex`, no `--depth`". So every JAR this run's build produced was assembled into the set
**that** frontend invocation was given, and no archive was withheld from **it**. It establishes nothing
at all about the graph that exists, because that invocation produced no graph (STATUS). Coverage —
whether a module's own code reached the graph the Joern stages load — is a different question against a
different input set, and it is section 6's.

**A superseded attempt did withhold two archives, and that is a halt-class departure registered
elsewhere.** `harness/artifacts/logs/cpg-frontend-input-manifest.json`, written in a **w-000** clone and
retained in the logs tree as evidence, records `full_inventory_archive_count` **191** against
`frontend_input_archive_count` **189**, `frontend_input_bytes` **308,385,184** and
`withheld_archive_count` **2** — its own `assertion` holding for the reduced set with
`assertion_errors` empty, so the trim is declared rather than concealed. The two withheld archives, with
the byte size, digest and stated reason the manifest itself gives:

| Withheld archive | Bytes | sha256 | The manifest's own stated reason |
| --- | --- | --- | --- |
| `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` | 109,208,027 | `66017e4e2086ba154144d244f123e4473a353f746baa8e36985f23323869afc8` | a shaded shuffle uber-jar with `shadedArtifactAttached=false`, i.e. not the module primary artifact; including it "vendors common/network-common, common/network-shuffle and common/utils-java classes and removes their injective coverage witnesses (measured: 35 valid witnesses -> 32)" |
| `connector_kafka-0-10-assembly__spark-streaming-kafka-0-10-assembly_2.13-4.1.0-SNAPSHOT.jar` | 13,591,752 | `96bcfab6d42abc7ba1f6dff63c60f45227808488870ad83ddad9bf2271913ef6` | a shaded assembly of a packaging module "that has no src/ directory at all"; including it "vendors connector/kafka-0-10, connector/kafka-0-10-token-provider and common/tags classes and removes their injective coverage witnesses (measured: 35 valid witnesses -> 33)" |

Both reasons are coverage-witness reasons: the archives were withheld because including them would
reduce the number of modules for which an injective witness exists. **That is precisely the rationale
AAP §0.3.2 forbids** — it narrows what enters the graph in order to improve a number the graph is then
measured by — and §0.9.2 names trimming the input set among the conditions that stop the run rather than
get repaired. §0.5.1's answer to a vendored witness is the module-exclusive `pom.properties` fallback,
not the removal of the archive that vendored it, and section 6 uses only the two witness kinds §0.5.1
names.

**No delivered measurement in this file rests on that attempt.** Every 191-archive figure above is
`build-reactor.log` STEP 13's and `cpg-frontend.log` STEP 1's, both of the complete set; every graph
count and every coverage verdict in sections 5 and 6 is measured over the **62**-archive input set of
the graph that loads (`cpg-input-inventory.json`); and no figure anywhere in this file is taken from the
189-archive set or from its manifest beyond the disclosure above. The manifest is neither deleted nor
re-registered here: it is retained as evidence under AAP §0.8.1, and its run-level divergence entry is
**D20** in `oss-scan-results/run-record.md` §13, which owns the run's single divergence register and
carries the disposition and the decision a human must take.

### Staged input set 2 — the 62 archives the graph that loads was built over

This is the input set every figure in sections 5 and 6 belongs to. It was measured member by member
from the tree on disk by `harness/artifacts/logs/cpg-input-inventory.json`, which states its own
provenance in the same file: the graph is `/opt/blitzy-harness/cpg/spark.cpg` and "**It was written by
PROVISIONING over this staging tree, not by this run.**"

| Property | Value |
| --- | --- |
| Staging tree | `/opt/blitzy-harness/cpg-input` — host-global, provisioning's, read-only to this run |
| Archives | **62** |
| Total bytes | **285,122,371** |
| Distinct sha256 | **62** — the archive-to-digest mapping is injective in both directions, so no two members are the same bytes under two names and no member is missing a digest |
| Class entries across them | **76,151** |
| Reactor projects represented | **31** of the 38 JAR-packaging projects |
| Reactor projects absent entirely | **7**, each named in the record with the same reason: no archive of that project is in the tree |
| Archives marked that module's primary artifact | 32 — `common/network-yarn` has two, its main artifact and its unattached shaded shuffle JAR |

The 7 absent projects are `connector/kafka-0-10`, `connector/kafka-0-10-assembly`,
`connector/kafka-0-10-sql`, `connector/kafka-0-10-token-provider`, `examples`, `sql/connect/shims` and
`tools`. Section 3 records that all seven produced their own main artifact in this run's build, and
section 6 records what their absence from this input set does to their coverage verdict. The two facts
are separate and both are stated.

---

## 5. The graph: one identity, its counts against the expected values, and what is not measurable

### The graph's identity and provenance — one pair, one record of account

**Which file.** `harness/cpg/spark.cpg` is a 33-byte **symlink** to `/opt/blitzy-harness/cpg/spark.cpg`,
one hop with no intermediate indirection, so the AAP's named path and the environment's `HARNESS_CPG`
are the same repository path resolving to one file — `joern-preflight.log` enumerates every subject and
reports "All 1 subject(s) resolve to one file: yes". The size is always taken through the link: the
33-byte no-follow reading is the length of the target path string and would describe nothing.

**Which bytes, and who wrote them.** One pair, and one record of account for it:

| | |
| --- | --- |
| Bytes | **541,309,809** |
| sha256 | **`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`** |
| Written by | **PROVISIONING**, 2026-08-30T19:18:37Z — **not by this run** |
| Record of account | `harness/artifacts/logs/cpg-identity.txt` |
| How that record was resolved | `harness/lib/preflight_graph_identity.py` `record_of_account()`, the same function the Stage 3 gate calls; it prefers this checkout's own frontend write-time pair and fell back to the record written beside the graph because this run's frontend wrote no graph |
| The record it fell back to | `/opt/blitzy-harness/provision-log/cpg-identity.txt`, written 2026-08-30T19:19:09Z, corroborated by `/opt/blitzy-harness/provision-log/cpg-record.txt`; both were read and agree, and a disagreement between them would have prevented the record being written at all |
| Re-measured while that record was written | 2026-09-01T14:54:56.741Z — byte size **MATCH**, sha256 **MATCH** |

**Re-measured for every one of the five loads, every check logged — and for one of the five the
comparison ran after the load rather than before it.** The graph is inherited, so nothing about it is
assumed between stages: the bytes are re-read and re-hashed for each load and a mismatch halts instead
of proceeding. Four of the five checks ran **immediately before** the load they gate, which is what
AAP §0.8.2 requires. The fifth — the Stage 3 Joern runner — is split, and both halves are in the table:
the runner recomputed the pair itself at load time and printed it, while the comparison of that pair
against the record of account was performed **about 11.5 minutes after the load and from a different
clone**. That is stated as the ordering defect it is, not as a pass, in the subsection below.

| Load | When the check ran | Record | Result |
| --- | --- | --- | --- |
| Stage 2 `importCpg` verification load | 2026-09-01T13:31:15.334Z, before the load | `cpg-verify.log:47-50`, "GRAPH IDENTITY, RE-VERIFIED IMMEDIATELY BEFORE THE LOAD" | MATCH on both fields |
| Stage 3 Joern runner — recompute, contemporaneous | inside the invocation, 2026-09-01T14:25:10Z → 14:41:24Z | `harness/bin/run-joern.sh:57-58` computing `stat -c%s` and `sha256sum` over the resolved target, printed at `joern.runner-console.log:14-15` | `cpg bytes : 541309809`, `cpg sha256 : 4616845a…4730c7` |
| Stage 3 Joern runner — comparison against the record of account, **after the fact** | 2026-09-01T14:52:54Z, `Clone index 0` (`joern-preflight.log:17-18`) — about 11.5 min after the load ended, in another clone | `joern-preflight.log:27-28` recorded pair, `:36-37` re-measured `MATCH`/`MATCH`, `:43` | **VERDICT: PASS**, but not a pre-load check for this load |
| Stage 5 probe query 01 | 2026-09-01T14:56:12.096Z, before the load | `probe-01-callgraph-unguarded-driver-launch.identity.txt` | `bytes=541309809`, same sha256 |
| Stage 5 probe query 02 | 2026-09-01T15:08:05.774Z, before the load | `probe-02-dataflow-unguarded-driver-launch.identity.txt` | `bytes=541309809`, same sha256 |
| Stage 5 probe query 03 | 2026-09-01T15:30:31.248Z, before the load | `probe-03-parameterized-handler-sink-pairs.identity.txt` | `bytes=541309809`, same sha256 |

**The Stage 3 ordering, and what it does and does not put in doubt.** The two halves must travel
together. What was contemporaneous is the **measurement**: the runner reads the graph's size and digest
from the resolved target itself at `harness/bin/run-joern.sh:57-58` and prints them before it hands the
path to `harness/lib/joern-scan.sc`, and `joern.runner-console.log:14-15` carries them inside the
invocation its own header brackets (`argv=["./harness/bin/run-joern.sh"]`,
`started=2026-09-01T14:25:10Z ended=2026-09-01T14:41:24Z`, `clone_index=13`). What ran late is the
**comparison** against the record of account: `joern-preflight.log` is stamped
`Checked at (UTC) : 2026-09-01T14:52:54Z` with `Clone index : 0`. So for this one load AAP §0.8.2's
"immediately before every load" is **not** satisfied by that gate log, and no sentence in this file says
it is.

What the late comparison does not put in doubt is *which bytes were read*. The pair the runner
recomputed equals the pair the record of account states and the pair every other check got; the resolved
file's mtime, **2026-08-30T19:18:37Z**, precedes all five checks and did not move between them; and the
gate re-measured the same values at 14:52:54Z. **No substitution occurred — the control ran late and the
outcome is sound**, and those are two findings rather than one.

**One further statement in that same log overstates, and is corrected here rather than in it.**
`joern-preflight.log` lines 11-12 describe `harness/lib/run-joern-gated.sh` as "the only committed
execution path for Stage 3", and lines 12-13 add that it "has no branch that reaches the runner after a
non-zero gate". The load on record did not take that path: `argv=["./harness/bin/run-joern.sh"]` in the
console log's own header, so the wrapper's structural gate-binding was **not** what bound this load and
is not offered here as though it had been. What bound it is the pair in the table — the runner's own
recompute at load time, and the after-the-fact comparison. The log is published verbatim under AAP
§0.8.1 and is not edited, which is why the correction is stated in this file. The run-level register
entry for both this and the ordering above is **D4** in `oss-scan-results/run-record.md` §13.

**One live contradiction, recorded with both values and repaired by nothing.**
`harness/ENVIRONMENT.md` §7 states this graph's identity explicitly, and the filesystem contradicts it:

| Source | Bytes | sha256 | Methods |
| --- | --- | --- | --- |
| `harness/ENVIRONMENT.md:284-286`, the provisioned record in this clone | 541,255,894 | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | 1,397,339 |
| The bytes on disk, measured through the symlink and loaded by this run | **541,309,809** | **`4616845a…4730c7`** | **1,396,899** |

Neither the byte size nor the digest is a field the request's expected-values table carries, so on those
two fields the record is the only statement and observation contradicts it. That is AAP §0.1.3's fourth
case, and `harness/artifacts/logs/gate-record.json` carries it as the gate halt
`gate.environment_record_graph_identity_agreement` — one of the two halts in a gate whose overall
verdict is `halt`. The cause is inherited rather than produced: the host was re-provisioned on
2026-08-30 and the shared graph was replaced, and this run built no graph of its own. Repair is not
available in any case — the file is host-global and read by concurrent clones — so both values are
recorded wherever either is cited, and neither is chosen.

### The three counts, against their expected values

These are `cpg-verify.log`'s measurement, PHASE 1, taken by the `importCpg` load of exactly the bytes
above — one load, one measurement, cited here and by section 6 rather than measured twice. They describe
**provisioning's graph over its 62-archive input set**, never the complete-input graph the AAP mandates,
which does not exist.

| Count | Expected | Observed | Delta | Rule, and how the difference is classified |
| --- | --- | --- | --- | --- |
| Methods | 898,336 | **1,396,899** | +498,563 | **One-sided**: floor 853,420, no upper bound. The observation is above the floor and above the anchor, which AAP §0.9.3 **records** rather than halts |
| Type declarations | 87,381 | **119,721** | +32,340 | Anchor, reported; no threshold applies — a **recorded difference** under AAP §0.9.3, never a halt |
| Files | 38,818 | **45,037** | +6,219 | Anchor, reported; no threshold applies — a **recorded difference** under AAP §0.9.3, never a halt |

The load's own supporting figures, from the same PHASE 1: internal methods **1,307,112**, external
methods **89,787**, methods under `org.apache.spark.*` **925,445** (66.25 % of all methods), and
`pom.properties` file nodes **102**. The load ran under Temurin **21.0.12.1+1** (major 21) at
**`-J-Xmx64g`**, proven committable at the gate with `-Xms64g -Xmx64g -XX:+AlwaysPreTouch`, into a
workspace outside the repository, and took **885,009 ms**.

**The two checks the floor exists for.** `methods > 0` — 1,396,899 is not zero, and a graph that loads
with zero methods is indistinguishable from a clean scan. And 1,396,899 is at or above the one-sided
floor of 853,420, so the truncation signature the floor exists to catch is absent. **No input was
trimmed or added to move any of these three numbers**, in either direction.

**What is not established, and is not guessed.** The *cause* of the excess over the anchors. The AAP's
stated rationale is the six extra JAR producers a full reactor packages, and those six are measured in
section 6 as **absent from this graph's input set**, so that mechanism cannot be the explanation here.
`cpg-verify.log` records the cause as not established and this file does the same rather than inventing
one.

### The two observed metrics

Both metrics below are `harness/artifacts/logs/cpg-frontend.log`'s recount from the frontend's own
preserved output stream, and both are **observed facts of this run's own invocation — the one that
failed and produced no graph — rather than pre-approved expectations**. They describe processing of the
complete 191-artifact set, not the graph on disk, whose own figures are the subsection above. Neither is
treated as acceptable because a document expected some other number.

| Observed metric | Value |
| --- | --- |
| Duplicate-entry overwrite warnings | **33,784** lines |
| Distinct destination entries overwritten | **27,843** |
| Of those, `.class` files | **32,990** warnings |
| Of those, `META-INF/maven` descriptors | **456** warnings |
| Of those, other `META-INF` entries | **225** warnings |
| Of those, other resources | **113** warnings |
| AST-creation failures | **23** lines over **23** distinct classes |
| The exception behind every one of them | `java.lang.RuntimeException: Chain already contains object: <fqcn>`, raised from `soot.util.HashChain.addLast` via `SootClass.setApplicationClass` under `AstCreationPass.runOnPart` — one failure class, not an assortment |

**Overwrites attributed to the modules whose own artifacts carry the affected entry.** STEP 6 states
the caveat before the numbers, and it is repeated here because these figures are not additive:
*"a warning is attributed to every module whose own artifacts carry that entry, so these figures
overlap by construction and do not sum to the total."* The largest attributions are
`sql/connect/client/jvm` **13,056**, `sql/connect/server` **8,240**, `sql/connect/common` **5,934**,
`sql/catalyst` **5,685**, `common/network-yarn` **5,614**, `sql/core` **4,320** and `core` **3,294**;
the smallest are `sql/connect/client/jdbc` **253** and `(root parent)` **218**; and every one of the 38
JAR-packaging projects appears in the listing alongside the root parent. A further **403** warnings are
attributed to no own artifact at all, recorded verbatim as *"(entry not present in any own artifact —
extracted from a nested archive)"* — `--recurse`, which AAP §0.5.1 mandates and STEP 4 confirms was
passed, descends into archives nested inside the staged ones, so those destinations have no top-level
staged entry to match. **No finer grouping is stated**, because STEP 6 publishes the entry-kind split
and this module attribution and nothing else: there is no destination-package breakdown and no
containment analysis in it to cite, so none appears here.

**AST-creation failures grouped the same way.** All 23 are `sql/connect` classes. By package: **10**
under `org/apache/spark/sql/connect/client/arrow`, **6** under `org/apache/spark/connect/proto`, **6**
under `org/apache/spark/sql/connect/client`, and **1** `org/apache/spark/sql/connect/StreamingQueryListenerBus`.
That sums to 23, and STEP 7 lists all 23 class names in full.

**Both metrics differ from the provisioned record's, both values are on the record, and they are not
the same measurement.** `harness/ENVIRONMENT.md:311-317` records **31,598** overwrite warnings over
**26,221** distinct class files and **173** AST-creation failures, against a runbook expectation of
roughly 5,700 and roughly 36. Those belong to **provisioning's own frontend run over its 62-archive
input set** (`harness/ENVIRONMENT.md:849`), a different invocation over a different input from this
run's 191-archive attempt, so the difference is not a discrepancy to reconcile: this run's figures are
higher because its input carries every pre-shade sibling and every `-tests` artifact that the
62-archive set leaves out. `cpg-frontend.log` is the sole owner of this run's two figures, and no
figure of this run's is restated from any other record.

### The limitation, stated rather than worked around

**Per-class provenance for overwritten classes could not be established with this frontend's output.**
The frontend extracts every input into one layout with replacement, so some definitions win and others
are discarded — but its directory walk is not ordered by this run, and its overwrite warning names the
**destination** class file rather than the archive whose definition survived. Which input won a given
collision is therefore not measurable from what the frontend emitted.

**No winner map is claimed anywhere in this file, and none is presented.** The groupings above are
*containment* — which archives hold an entry of that name — which is a different question, and it is
labelled as containment at every use. What *is* reproducible is the input set itself: the ordered
staging manifest in section 4 fixes it byte for byte, so the input to any future frontend run is
determined even where the outcome of a given collision is not.

### The `sql/connect/shims` collision, and what can and cannot be said about it

This is the collision most likely to be misread as a coverage problem, because `sql/connect/shims`
ships stub `SparkConf`, `SparkContext` and `RDD` classes that `core` and the SQL modules also ship. Two
measured facts bound what may be stated about it, and nothing beyond them is claimed.

- **In this run's 191-archive input set the collision is real, and it is measured only as far as the
  log measures it.** `cpg-frontend.log` STEP 6 attributes **361** overwrite warnings to
  `sql/connect/shims`, under the same overlapping-attribution caveat as every other module's figure.
  Which definition survived any one of them is **not** measurable from the frontend's output, for the
  reason stated immediately above, so no winner is named and no per-class outcome is inferred.
- **In the graph that exists the question does not arise.** Both `spark-connect-shims` archives are
  **absent** from its 62-archive input set (section 4), so no shims stub is in that graph and no
  collision with one occurred in it. That absence is also why section 6 records `sql/connect/shims`
  as NO VERDICT OBTAINABLE — an input-set fact, not a graph defect and not a build failure, since
  section 3 records the project producing its own main artifact.

**No method counts are quoted for those classes, deliberately.** The only load this file cites,
`cpg-verify.log`, queried the 26 module coverage witnesses (PHASE 2) and the four probe-surface classes
(PHASE 3); it did not query `org.apache.spark.SparkConf`, `SparkContext`, `rdd.RDD` or
`api.java.JavaRDD`. No record in this tree carries their counts for this graph, so none is stated here,
and no comparison is made against a narrower graph because none was retained (STATUS).

---

## 6. The per-module graph coverage verdict

This file owns this verdict. It has exactly two inputs and neither is a document:
`harness/artifacts/logs/cpg-input-inventory.json`, which computed witness exclusivity by walking the
62 archives of the graph's input set, and `harness/artifacts/logs/cpg-verify.log` PHASE 2, which
queried each surviving witness in the graph under the single `importCpg` load whose identity section 5
states. `harness/artifacts/logs/cpg-module-coverage.json` is **not a third input**: it is a rendering of
those same two files, regenerated in this generation against the graph identity section 5 states, and it
agrees with the table below row for row — 31 modules, 26 COVERED, 5 NO VERDICT OBTAINABLE, 0 presence
verdicts, 0 prefix verdicts. Nothing below is derived from it, and its `supersedes` field records the
clone `w-001` edition it replaced.

**Which graph generation the first verdict column measures, stated before the table rather than after
it.** It is the one graph this run loaded and the one identity section 5 states: **541,309,809 bytes,
sha256 `4616845a…4730c7`**. `cpg-verify.log` records a **single** `importCpg` load, performed by this
generation in clone 13 (`cpg-verify.log:27-28`), against exactly those bytes — its SUBJECT block states
them at `:33-34` and its pre-load identity check re-measures them at `:47-50`. Every "graph result" and
every verdict in the first column below, and the type-declaration cross-reference at the end of this
section, are that load's PHASE 2 measurements (`cpg-verify.log:105-228`) of those bytes. No verdict here
is a measurement of the superseded **541,255,894 / `26d327cc…`** generation: that pair is the inherited
environment record's (`harness/ENVIRONMENT.md:284-288`), it is stated in section 5's and section 7's
contradiction tables as the contradiction, and `cpg-verify.log` names it only at its own `:76-80`, and
only to identify the record the filesystem contradicts.

A **third** coverage statement exists and belongs to a different denominator: `cpg-graph-record.log:48`
records the provisioning invocation's own verdict over its 62-JAR input set as **31 of 31 contributing
modules covered, 0 missing**. It is stated in full in this file's STATUS block, and section 7's
authority-rule subsection adjudicates the environment record's copy of it against the verdict below.
*Contributing modules* is not *JAR-producing modules*, so it is never totalled with this section's first
column and neither is substituted for the other.

### The two questions, kept apart

**Delivery** — was every JAR the build produced in the input set this run assembled? — is already
settled, and not by a class search: section 4's staging manifest settles it one file at a time, total
and injective in both directions over 191 files, recorded before any frontend invocation. What that
does **not** establish is that those 191 archives are in the graph that exists. They are not: the
frontend invocation over them wrote no graph at all (STATUS, D1), and the graph every Joern stage
loaded was written by provisioning over **62 archives from 31 modules** (D3).

**Coverage** — did a module's own code reach the graph the runners actually read? — is this section's
question. It is asked of that 62-archive graph, because that is the only graph there is, and the
answer is bounded by its input set in two different ways that are never merged below.

### The one admissible test, and the one fallback the AAP names

> **A class present in that module's primary artifact — its main, unclassified, non-`original-`,
> non-`-tests` JAR — and absent from every other module's artifacts.**

Three qualifications, all load-bearing:

- **A shared package prefix is never admissible evidence.** Every Spark module ships under
  `org.apache.spark`, so a prefix test lets one module vouch for a dozen absent ones. No prefix test
  appears in this verdict or in either producer record it rests on.
- **A class shared with a same-module sibling still qualifies**; a class shared with **another module**
  does not. That is what makes the test satisfiable for a shaded artifact and its pre-shade sibling,
  which share every class: "unique to that JAR" is unsatisfiable, "unique to that module" is not.
- **Where no such class exists, the module-exclusive `META-INF/maven/**/pom.properties` node is the
  weaker witness AAP §0.5.1 names, and it is the only fallback.** There is no third kind. In
  particular, **presence of a class that another module's archive also ships is not a coverage
  verdict** and is not recorded as one anywhere in this section: it would let a shaded archive vouch
  for a module whose own artifact might be absent from the input entirely, which is the single failure
  mode the injectivity requirement exists to prevent.

### How the witnesses were computed, and then queried

`cpg-input-inventory.json` walked all 62 archives of the input set and took, per module, the classes of
that module's primary artifact minus the union of every other module's classes. **26 of the 31 modules
in the input own such a class; 5 own none.** Each of the 26 surviving candidates was then queried in the
graph by exact type-declaration full name, and each of the 5 was checked for the weaker witness as
well. Both steps are one measurement each, cited here and nowhere re-derived.

### The verdict — the 26 modules covered on injective evidence

Every row's witness is a class **in that module's primary artifact and in no other module's artifact**
across the 62-archive input set, queried in the graph by exact type-declaration full name
(`cpg.typeDecl.fullNameExact(<witness>)`). The two count columns are
`cpg-input-inventory.json`'s archive-level measurement; the graph column is `cpg-verify.log`
PHASE 2's query result from the single `importCpg` load of the identity in section 5.

| Module | Witness class, as queried | Classes in its primary artifact | Of those, exclusive to the module | Graph result | Verdict |
| --- | --- | --- | --- | --- | --- |
| `common/kvstore` | `org.apache.spark.util.kvstore.ArrayWrappers` | 39 | 38 | PRESENT — 2 type declarations, 4 methods, 1 file node | **COVERED on injective evidence** |
| `common/network-yarn` | `org.apache.spark.network.yarn.YarnShuffleService` | 11,070 | 6,175 | PRESENT — 2 type declarations, 36 methods, 1 file node | **COVERED on injective evidence** |
| `common/sketch` | `org.apache.spark.util.sketch.BitArray` | 16 | 15 | PRESENT — 2 type declarations, 28 methods, 1 file node | **COVERED on injective evidence** |
| `common/tags` | `org.apache.spark.annotation.AlphaComponent` | 12 | 11 | PRESENT — 2 type declarations, 0 methods, 1 file node | **COVERED on injective evidence** |
| `common/unsafe` | `org.apache.spark.sql.catalyst.expressions.HiveHasher` | 65 | 64 | PRESENT — 2 type declarations, 12 methods, 1 file node | **COVERED on injective evidence** |
| `common/utils` | `org.apache.spark.BreakingChangeInfo` | 164 | 163 | PRESENT — 2 type declarations, 16 methods, 1 file node | **COVERED on injective evidence** |
| `common/variant` | `org.apache.spark.types.variant.ShreddingUtils` | 34 | 33 | PRESENT — 2 type declarations, 8 methods, 1 file node | **COVERED on injective evidence** |
| `connector/avro` | `org.apache.spark.sql.avro.AvroDataToCatalyst` | 24 | 23 | PRESENT — 2 type declarations, 106 methods, 1 file node | **COVERED on injective evidence** |
| `connector/protobuf` | `org.apache.spark.sql.protobuf.CatalystDataToProtobuf` | 825 | 825 | PRESENT — 2 type declarations, 82 methods, 1 file node | **COVERED on injective evidence** |
| `core` | `org.apache.spark.Aggregator` | 5,097 | 5,096 | PRESENT — 2 type declarations, 54 methods, 1 file node | **COVERED on injective evidence** |
| `graphx` | `org.apache.spark.graphx.Edge` | 132 | 131 | PRESENT — 2 type declarations, 120 methods, 1 file node | **COVERED on injective evidence** |
| `launcher` | `org.apache.spark.launcher.AbstractAppHandle` | 33 | 32 | PRESENT — 2 type declarations, 30 methods, 1 file node | **COVERED on injective evidence** |
| `mllib` | `org.apache.spark.ml.Estimator` | 2,646 | 2,645 | PRESENT — 2 type declarations, 20 methods, 1 file node | **COVERED on injective evidence** |
| `mllib-local` | `org.apache.spark.ml.impl.Utils` | 23 | 22 | PRESENT — 2 type declarations, 12 methods, 1 file node | **COVERED on injective evidence** |
| `repl` | `org.apache.spark.repl.Main` | 9 | 8 | PRESENT — 2 type declarations, 24 methods, 1 file node | **COVERED on injective evidence** |
| `resource-managers/kubernetes/core` | `org.apache.spark.deploy.k8s.Config` | 145 | 144 | PRESENT — 2 type declarations, 254 methods, 1 file node | **COVERED on injective evidence** |
| `resource-managers/yarn` | `org.apache.spark.deploy.yarn.AmIpFilter` | 79 | 78 | PRESENT — 2 type declarations, 18 methods, 1 file node | **COVERED on injective evidence** |
| `sql/catalyst` | `org.apache.spark.sql.catalyst.AliasIdentifier` | 5,333 | 5,332 | PRESENT — 2 type declarations, 40 methods, 1 file node | **COVERED on injective evidence** |
| `sql/connect/client/jdbc` | `org.apache.spark.sql.connect.client.jdbc.NonRegisteringSparkConnectDriver` | 2 | 2 | PRESENT — 2 type declarations, 16 methods, 1 file node | **COVERED on injective evidence** |
| `sql/connect/client/jvm` | `org.apache.spark.sql.application.ConnectRepl` | 12,652 | 4,970 | PRESENT — 2 type declarations, 2 methods, 1 file node | **COVERED on injective evidence** |
| `sql/connect/server` | `org.apache.spark.sql.connect.SimpleSparkConnectService` | 8,050 | 4,153 | PRESENT — 2 type declarations, 2 methods, 1 file node | **COVERED on injective evidence** |
| `sql/core` | `org.apache.spark.sql.DataSourceRegistration` | 3,879 | 3,878 | PRESENT — 2 type declarations, 72 methods, 1 file node | **COVERED on injective evidence** |
| `sql/hive` | `org.apache.spark.sql.hive.DeferredObjectAdapter` | 149 | 148 | PRESENT — 2 type declarations, 36 methods, 1 file node | **COVERED on injective evidence** |
| `sql/hive-thriftserver` | `org.apache.spark.sql.hive.thriftserver.ArrayFetchIterator` | 211 | 210 | PRESENT — 2 type declarations, 258 methods, 1 file node | **COVERED on injective evidence** |
| `sql/pipelines` | `org.apache.spark.sql.pipelines.AnalysisWarning` | 285 | 284 | PRESENT — 2 type declarations, 0 methods, 1 file node | **COVERED on injective evidence** |
| `streaming` | `org.apache.spark.status.api.v1.streaming.ApiStreamingApp` | 359 | 358 | PRESENT — 2 type declarations, 16 methods, 1 file node | **COVERED on injective evidence** |

Two rows carry a figure that would read oddly without its reason, and both are the shaded-artifact
case the test was written to survive. `common/network-yarn` counts **11,070** classes in its primary
artifacts because it has *two* — its main JAR and its unattached shaded shuffle uber-JAR — of which
**6,175** are exclusive to the module; a class shared with a same-module sibling still qualifies,
which is exactly why the test is satisfiable here. `sql/connect/client/jvm` counts **12,652** with
**4,970** exclusive, for the same reason.

**The weaker witness was available for all 26 and needed by none of them.** Every one of the 26 also
owns at least one module-exclusive `META-INF/maven/**/pom.properties` entry in
`cpg-input-inventory.json`'s `exclusive_pom_properties` — `core` owns 11, `common/network-yarn` 32,
`sql/connect/client/jvm` 7, and the rest one apiece — so the fallback AAP §0.5.1 names existed and
was simply not reached, the class witness being the stronger of the two.

### The five modules in the input set for which no witness of either kind exists

These five own **no** class exclusive to them and **no** exclusive Maven descriptor node, because
another module's shaded archive vendors both. AAP §0.5.1 supplies exactly two witness kinds and no
third, so each is recorded as **NO VERDICT OBTAINABLE**. Presence is not substituted for
exclusivity anywhere below: a class the vendoring archive also ships would let that archive vouch
for a module whose own artifact might be absent, which is the failure mode the injectivity
requirement exists to prevent.

| Module | Classes in its primary artifact | Exclusive to it | Exclusive descriptor nodes | The archive that vendors both | Verdict |
| --- | --- | --- | --- | --- | --- |
| `common/network-common` | 2,170 — **every one of them** also in the archive named right | 0 | 0 | `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` | **NO VERDICT OBTAINABLE** |
| `common/network-shuffle` | 92 — **every one of them** also in the archive named right | 0 | 0 | `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` | **NO VERDICT OBTAINABLE** |
| `common/utils-java` | 40 — **every one of them** also in the archive named right | 0 | 0 | `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` | **NO VERDICT OBTAINABLE** |
| `sql/api` | 1,203 — **every one of them** also in the archive named right | 0 | 0 | `sql_connect_client_jvm__spark-connect-client-jvm_2.13-4.1.0-SNAPSHOT.jar` | **NO VERDICT OBTAINABLE** |
| `sql/connect/common` | 1,879 — **every one of them** also in the archive named right | 0 | 0 | `sql_connect_server__spark-connect_2.13-4.1.0-SNAPSHOT.jar` | **NO VERDICT OBTAINABLE** |

### The seven JAR-packaging projects absent from the graph's input set entirely

A different reason, kept separate from the five above: these seven have **no archive at all** in the
62-archive input set, so the graph carries none of their bytecode and no witness of any kind can be
in it. `cpg-input-inventory.json` records each with the same reason verbatim, and `cpg-verify.log`
PHASE 2 repeats it as the coverage cost of the unmet all-JAR requirement.

| Project (JAR-packaging, built successfully by this run) | Archives in the graph's input set | Verdict |
| --- | --- | --- |
| `connector/kafka-0-10` | **0** | **NO VERDICT OBTAINABLE** — no archive of this project is in the input set |
| `connector/kafka-0-10-assembly` | **0** | **NO VERDICT OBTAINABLE** — no archive of this project is in the input set |
| `connector/kafka-0-10-sql` | **0** | **NO VERDICT OBTAINABLE** — no archive of this project is in the input set |
| `connector/kafka-0-10-token-provider` | **0** | **NO VERDICT OBTAINABLE** — no archive of this project is in the input set |
| `examples` | **0** | **NO VERDICT OBTAINABLE** — no archive of this project is in the input set |
| `sql/connect/shims` | **0** | **NO VERDICT OBTAINABLE** — no archive of this project is in the input set |
| `tools` | **0** | **NO VERDICT OBTAINABLE** — no archive of this project is in the input set |

### What that adds up to

| Outcome | Count |
| --- | --- |
| Reactor projects | **40** |
| — `packaging=pom`, no primary artifact, outside this test by construction | 2 |
| **JAR-producing modules under the test** | **38** |
| **COVERED on injective evidence** — a class exclusive to the module, measured present in the graph | **26** |
| **NO VERDICT OBTAINABLE** | **12** |
| — because no witness of either kind exists across the input set | 5 |
| — because the module has no archive in the input set at all | 7 |
| Verdicts resting on presence of a class another module also ships | **0** |
| Verdicts resting on a shared package prefix | **0** |
| Witness kinds admitted beyond the two AAP §0.5.1 names | **0** |
| Winner maps claimed | **0** |

**26 of 38 is the coverage this graph supports, and the 12 are stated as unobtainable rather than
softened.** Nothing below the line is folded into a pass, dropped from the count, or answered with a
narrower graph — section 4's second input account and STATUS record why no such substitute is offered.
The two reasons behind the 12 are different and stay separate: 5 are a property of a *complete-enough*
input set, where fat artifacts vendor everything smaller modules ship, and 7 are a property of this
input set being *narrower than the build*, which is the coverage cost of the unmet all-JAR requirement.

### Cross-reference against the counts the verification load reports

One `importCpg` load produced both this verdict and section 5's counts, so they are one measurement
read two ways rather than two measurements: `cpg-verify.log` PHASE 1 reports **1,396,899 methods,
119,721 type declarations and 45,037 files** with `methods > 0` explicitly confirmed, and PHASE 2's 26
present witnesses are type declarations counted inside that 119,721. Every PRESENT row's module is in
the graph's input set, and no witness was found for any module outside it — which is a check on both
axes at once, because a module in the input whose witness was missing would be a coverage failure, and
a witness found for a module outside the input would mean the witness was never exclusive.

**The weaker witness kind is functional on this graph, which is why the five and the seven are archive
facts rather than graph facts.** The same load counts **102** `META-INF/maven/**/pom.properties` file
nodes, so descriptor nodes are represented and queryable here. For the five, the node exists in the
graph's input but is not *exclusive* to the module, and for the seven no archive of the module is in the
input at all. Neither outcome is a limitation of the query or of the node type.

---

## 7. Prohibitions, the authority rule, and what is recorded without stopping the run

### What this record does not do

- **No cross-tool interpretation of any kind.** No scanner's output is named, ranked, contrasted or
  characterised anywhere in this file, and there is no comparison against Apex, Cantina or any other
  scanner — no such data exists in this run (AAP §0.1.3, §0.3.2). The Maven wrapper, the Joern bytecode
  frontend and the `importCpg` load appear only as steps that built and verified the artefacts this
  record describes, never as subjects of comparison.
- **No finding is judged.** No finding of any kind appears in this file, so none is called real,
  important, a false positive or a duplicate of another tool's.
- **No Apache Spark file was modified**, in either tree, and no runner, baked flag, environment file or
  allowlist was edited. The only writes into a Spark tree were the compiler's own output under
  `*/target/` of the private build clone.
- **No Spark test suite was executed, in any language.** The build carries `-DskipTests` and no test
  goal. Test-classified artifacts the packaging phase produced are recorded as artifacts; nothing in
  them was run.
- **Nothing was installed, upgraded or substituted**, and no input set was trimmed in either direction
  to bring a count inside a window.

### The authority rule, and where it does and does not reach

The request's expected-values table governs every field it carries, and the harness's environment
record never overrides it (AAP §0.1.3). Applied to this file's subject matter, that rule reaches
**inherited** facts — what the run observed about the provisioning as it found it — and not outputs this
run deliberately replaces. Two consequences, both material here:

- The **Maven identity** and the **JDK assignment** are inherited facts, and both agree with the table:
  required 3.9.11, detected 3.9.11, build JVM major 17, all three from `maven-preflight.log`.
- **The graph is an inherited fact in its entirety.** No part of it is an output this run replaced,
  because this run's frontend produced no graph at all (D1). So every statement the environment record
  makes about the graph is adjudicated under the authority rule rather than excused as intentional
  replacement — and the graph's three counts are separately compared against the expected-values table
  under its own rules in section 5. The one figure this run genuinely did replace is the **JAR
  inventory** of its own build, which is wider than the provisioning's narrowed one; a difference there
  is the requirement being fulfilled rather than an environment contradiction.

**One environment-record contradiction is of the halting kind, and it concerns the graph's identity.**
`harness/ENVIRONMENT.md` states that identity explicitly, and it does not match the file on disk that
every load in this run read:

| Source | Bytes | sha256 | Methods / typeDecls / files |
| --- | --- | --- | --- |
| `harness/ENVIRONMENT.md:284-288`, the provisioned record in this clone | 541,255,894 | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | 1,397,339 / 119,691 / 45,037 |
| The file on disk, measured through the symlink and recorded in `harness/artifacts/logs/cpg-identity.txt`; loaded and counted by `cpg-verify.log` | 541,309,809 | `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` | 1,396,899 / 119,721 / 45,037 |

Neither field is carried by the request's expected-values table, and observation contradicts the
record, so AAP §0.1.3's fourth case applies exactly. `gate-record.json` records this as one of the
run's two halting gate checks — `gate.environment_record_graph_identity_agreement`, carrying both value
sets — and `gate_verdict.overall` is `halt`, with the gate record stating in terms that it authorises
nothing. Both values are on the record and neither is chosen: there is no anchor to adjudicate between
them, and repair is not available in any case, because the file is host-global, shared with concurrent
readers, and was not written by this run.

What is **not** claimed here is that the bytes moved underneath the run. All five loads had the identity
re-measured from the bytes and all five of those records state the same pair, and the resolved file's
mtime of 2026-08-30T19:18:37Z precedes every one of those measurements — so this run is internally
consistent on exactly one graph. What is **not** claimed either is that all five comparisons ran before
their load: for the Stage 3 Joern runner the recompute was contemporaneous and the comparison against
the record of account ran about 11.5 minutes afterwards from a different clone, which section 5 states
in full and which does not move any value in the table above. The disagreement here is between that one
graph and a record of an earlier graph at the same shared path.

**Two further environment-record statements are contradicted by observation and are carried with both
values without stopping the run**, because neither is a field of the expected-values table and neither
was raised at the gate — `gate-record.json` records exactly one halting environment-record
contradiction, the identity above:

- The record states that no Spark artifact at this pin contains a
  `META-INF/maven/**/pom.properties` node (`harness/ENVIRONMENT.md:373-374`, repeated at `:774-775`), so
  the AAP's named weaker witness was unavailable for every module. Observation is the opposite:
  `cpg-input-inventory.json` records a module-exclusive `pom.properties` entry for 26 of the 31 modules
  in the graph's input set — `META-INF/maven/org.apache.spark/spark-core_2.13/pom.properties` for
  `core`, and one apiece for the others — and `cpg-verify.log` counts **102** such file nodes in the
  graph. The correction does not change any verdict in section 6: the 26 modules that own an exclusive
  `pom.properties` node are the same 26 that already own an exclusive class, and the 5 that own no
  exclusive class own no exclusive `pom.properties` node either.
- The record states **"31 of 31 contributing modules covered (26 unique-class witnesses, 5 named
  weaker witnesses)"** (`harness/ENVIRONMENT.md:605`, repeated at `:323`). This run measures 26 covered
  and 5 with no verdict obtainable (section 6). The 26 agree module for module. The 5 do not, and the
  record's own table at `:360-371` says why: it describes those five witnesses as "presence rather than
  exclusivity", and a class another module's shaded archive also ships is not injective evidence. The
  AAP's named weaker witness is a module-exclusive `pom.properties` node, and
  `cpg-input-inventory.json` records `exclusive_pom_properties` as empty for exactly those five
  modules. Section 6 records them as NO VERDICT OBTAINABLE and admits no third witness kind.

**What has changed since that contradiction was first recorded, and what has not.** The bytes on disk
now have a write-time record of their own — `cpg-graph-record.log`, byte-identical to
`/opt/blitzy-harness/provision-log/cpg-record.txt` — which states exactly one identity pair and equals
them, and all three probe queries verified that pair against that record before their load and
re-verified it after, each having loaded a private copy of the verified bytes (section 5). So a
**current** load is anchored. What has not changed is the inherited-record contradiction above, which is
about an identity stated for a file that was replaced; it is carried as halt-class finding **D4** in
`oss-scan-results/run-record.md` §13, which owns the register entry and keeps both generations with
their provenance.

**The Stage 3 lineage is that same one graph, and the superseded pair is no part of it.** The delivered
Joern runner read **541,309,809 / `4616845a…4730c7`**. `harness/bin/run-joern.sh` lines 57-58 recompute
the byte size and the digest from the resolved target and print them, and they appear as
`cpg bytes       : 541309809` and `cpg sha256      : 4616845a…4730c7` at
`harness/artifacts/logs/joern.runner-console.log` lines 14-15, inside the invocation that log's own
header brackets — `run_id=w013-20260901T132807Z clone_index=13`, `argv=["./harness/bin/run-joern.sh"]`,
`started=2026-09-01T14:25:10Z ended=2026-09-01T14:41:24Z elapsed_seconds=974.22 exit_status=0`. And
`harness/artifacts/logs/runner-sequence.json` binds that console log, the artifact
`harness/artifacts/raw/joern.json`, both of the runner's streams and its 241-byte status file to that one
invocation by byte size and sha256. So the dataset's `joern` rows come from the load that read the pair
section 5 states, and **541,255,894 / `26d327cc…` is the inherited environment record's identity**
(`harness/ENVIRONMENT.md:284-288`, section 5's contradiction table) rather than a lineage of this run.

**What `joern.status` is, so that nothing is looked for in it that it does not carry.** All nine
`<tool>.status` files are the runner's verbatim seven-line `scope_finish` trailer, and `joern.status` is
7 lines and 241 bytes carrying exactly `tool`, `exit_code`, `elapsed_seconds`, `artifact`,
`artifact_bytes`, `scan_root` and `scan_root_source`. It records **no graph identity of any kind** and no
command line, and no figure in this file is cited from it.

Neither the runner nor its load is re-run to settle any of this, and the reason is not that anything is
missing: `harness/bin/run-joern.sh`, `harness/env.sh`, `harness/lib/scope.sh` and
`harness/lib/joern-scan.sc` are all present and readable in this clone. AAP §0.8.1 forbids re-running a
scanner, and §0.6.4 makes the measurement already taken the one to cite rather than a second measurement
of the same thing. Both the inherited-record contradiction and the unmet all-JAR requirement remain
reported and unrepaired.

### Values named as not established

Named rather than omitted, because a value missing from the record is a value nothing downstream can
check (AAP §0.9.4):

- **Per-class provenance for every overwritten class** — not measurable from this frontend's output
  (section 5). No winner map is claimed.
- **A coverage verdict for twelve of the 38 JAR-producing modules** — not obtainable from this graph,
  for two separate reasons section 6 keeps apart: **seven** have no archive at all in its input set, and
  **five** own neither a class exclusive to them nor an exclusive Maven descriptor node, because another
  module's shaded archive vendors both. All twelve are named individually with what was tried.
- **An injective coverage witness for those five modules** — none of either kind AAP §0.5.1 permits
  exists across this input set. Presence of a class the vendoring archive also ships is **not** offered
  in its place, and no third witness kind is admitted anywhere in this file.
- **The cause of the graph's above-anchor counts** — recorded by `cpg-verify.log` as not established,
  deliberately not guessed.
- **The graph as this run's own output** — **attempted and blocked**, not deferred. This run invoked
  the frontend over its complete 191-artifact manifest under JDK 21 at a proven-committable 128 GiB
  heap; after 8 h 01 m, at a 113.3 GiB peak RSS, it failed in persistence at a fixed `Integer.MAX_VALUE - 8` array-length bound
  in flatgraph's string-pool writer, producing no graph. Carried as halt-class finding **D1** in
  `run-record.md` §13 and evidenced end to end by `cpg-frontend.log`; reported rather than repaired,
  and nothing was trimmed to obtain a graph.
- **A current-run method, type-declaration or file count** — none exists, because no current-run graph
  exists to load. None is estimated from the provisioned graph's counts.

### Recorded without stopping the run

Each of these is a recorded difference under AAP §0.9.3, with both values on the record:

- **The six JAR producers the expected-values table does not name** — `tools`, `examples`,
  `connector/kafka-0-10`, `connector/kafka-0-10-sql`, `connector/kafka-0-10-assembly` and
  `connector/kafka-0-10-token-provider` — all six `SUCCESS` and all six with their own artifact on
  disk. The rule is one-directional: a module that produced a JAR in the rehearsal and produces none
  now stops the run; the reverse does not.
- **The copied-dependency exclusion count** — 422 copied runtime dependencies excluded from the graph
  input set, plus 14 test-resource fixtures, out of 627 JARs enumerated.
- **The observed overwrite and AST-creation-failure counts** — **33,784** warnings over **27,843**
  distinct entry paths and **23** failures over 23 distinct classes, measured over this run's own
  complete 191-artifact input set, split by entry kind, attributed by contributing module under the
  log's own overlap caveat, with the **403** nested-archive entries recorded as such, the provenance
  limitation stated and no winner map presented. The provisioned record's own frontend figures over its
  62-archive input (31,598 / 26,221 / 173) are recorded beside them in section 5 as a different
  invocation over a different input set rather than as a conflict.
- **A reactor that failed and was then resolved project by project** — this did not occur: the reactor
  succeeded, all 40 projects have an outcome, all 38 JAR-packaging projects produced their artifact,
  and no diagnostic log was needed or written.
- **The graph's three counts against their expected values** — methods 1,396,899 above the 898,336
  anchor and the one-sided 853,420 floor, type declarations 119,721 against 87,381, files 45,037
  against 38,818 — together with the input-set difference **D3** that bounds what they describe: they
  are provisioning's graph over 62 archives from 31 modules, never the complete-input graph the AAP
  mandates.
- **Seven of the 38 JAR-packaging projects have no bytecode in the graph, and five more own no
  injective witness** — twelve NO VERDICT OBTAINABLE outcomes in section 6, each named with what was
  tried, none folded into a pass and none answered with a narrower graph.

---

## 8. The taint A/B — the graph-stage pass condition, as measured

The graph stage carries a second pass condition beside the graph itself, and it is stated in the same
terms as every other figure in this file: as a measurement, from the file that made it. AAP §0.5.1
requires that Opengrep's taint engine be proven active on Spark's own Scala **by an A/B result rather
than by a configuration reading**, and §0.9.1 restates the condition exactly, over the mandated subject
`core/src/main/scala/org/apache/spark/storage/DiskStore.scala` — *one traced finding at line 72 with
taint on and zero with it off, from two invocations differing only in that setting*.

> **THAT PASS CONDITION FAILED.** The taint-off arm returned the **same** traced finding at
> `DiskStore.scala` line 72 as
> the taint-on arm, and its SARIF is **byte-identical** to the on arm's. The off arm's own log states the
> verdict and its class:
>
> `THE A/B PAIR THEREFORE FAILED: NON-DISCRIMINATING ON THE MANDATED SUBJECT FILE. A contrast of zero is
> not a contrast. AAP 0.9.2 lists 'a failed taint A/B' among the conditions that STOP the run, so this
> is a HALT-CLASS finding, reported here and NOT repaired.`
> — `harness/artifacts/logs/taint-ab-off.log`, STATUS
>
> Nothing was adjusted to obtain the expected zero: the off arm's log records that no rule, no file, no
> line and no flag set was changed and that the arm was not retried with a narrower rule.

### The mandated pair, arm by arm

Both arms ran the same pinned rule from the same pinned ruleset against the same subject file, with
taint as the **sole** difference. Per-arm figures are each arm's own log and its own SARIF.

| Property | Value |
| --- | --- |
| Subject | `core/src/main/scala/org/apache/spark/storage/DiskStore.scala` — 380 lines, 12,045 bytes, sha256 `bc5491ac8a6bd9a8822ef5b4a55ac32c47ecb9ed25e2ca1770a1c9040739d02e`, and re-measured unchanged after both arms |
| Rule | `/opt/blitzy-harness/rules/opengrep-rules/scala/lang/security/audit/tainted-sql-string.yaml`, rule id `tainted-sql-string`, `mode: taint` — 90 lines, 2,824 bytes, sha256 `24fb1dcb0eb6e38efb6afe21426f113be5019c94764015c4e5fb030666d7079d` |
| Ruleset and engine | commit `f1d2b562b414783763fd02a6ed2736eaed622efa`, Opengrep **1.27.1** — both arms identical, no divergence, nothing marked not comparable |
| The one variable | `--taint-intrafile`, present in the on arm and absent in the off arm; it is the only taint discriminator Opengrep 1.27.1 exposes |

| Arm | Exit | Elapsed | Findings | SARIF bytes | SARIF sha256 | The two files each row is measured from |
| --- | --- | --- | --- | --- | --- | --- |
| **on** | 0 | 3 s | **1**, `DiskStore.scala` line **72**, `codeFlows=1`, 2 dataflow steps | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` | `harness/artifacts/logs/taint-ab-anchor-diskstore-on.log` and `harness/artifacts/logs/taint-ab-anchor-diskstore-on.sarif` |
| **off** | 0 | 3 s | **1**, `DiskStore.scala` line **72**, `codeFlows=1`, 2 dataflow steps | 4,753 | `7949617b3c88edba9faec24b79c7256667c59cf00885aadb8bd12da099845778` | `harness/artifacts/logs/taint-ab-anchor-diskstore-off.log` and `harness/artifacts/logs/taint-ab-anchor-diskstore-off.sarif` |

The two digests are **the same value**, which is the whole of the result: the arms did not differ in one
byte. The off arm additionally re-ran the on arm inside itself — exit 0, 1.849 s, 1 finding at
`DiskStore.scala` line 72,
recorded in its own STATUS block — so the identity is not an artefact of comparing runs taken hours
apart. The two arms' full narrative records, each with its stdout and stderr appended verbatim, are
`harness/artifacts/logs/taint-ab-on.log` and `harness/artifacts/logs/taint-ab-off.log`; the per-arm
tables above are measured from the four smaller per-arm files named in them.

### The same pair with the whole ruleset loaded — not a one-rule artefact

| Arm | Configs | Exit | Elapsed | Findings | SARIF bytes | SARIF sha256 |
| --- | --- | --- | --- | --- | --- | --- |
| **on** | 29 rule-bearing directories, 58 argv elements, 2,006 rules of which 241 multilang and 25 Scala applied to this file | 0 | 72 s | 1 at line 72, traced | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |
| **off** | the same 29 directories, the same rule count | 0 | 77 s | 1 at line 72, traced | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |

Measured from `harness/artifacts/logs/taint-ab-anchor-diskstore-fullruleset-on.log` with
`harness/artifacts/logs/taint-ab-anchor-diskstore-fullruleset-on.sarif`, and from
`harness/artifacts/logs/taint-ab-anchor-diskstore-fullruleset-off.log` with
`harness/artifacts/logs/taint-ab-anchor-diskstore-fullruleset-off.sarif`. **Byte-identical again**, so
the non-discrimination is not a consequence of invoking a single rule file.

### Why the arms cannot differ on this file, and why that is not an excuse

The mechanical reason is measured rather than speculated, from the trace the arms themselves attached:
the rule's source is a method parameter declared at `DiskStore.scala:64` (`def put(blockId: BlockId)`)
and its sink is the interpolated string at line 72 — step 0 of the trace is `$blockId` at line 72
column 21 and step 1 is the sink at line 72 column 13. **The flow never crosses a method boundary**, and
intra-file *inter-procedural* taint is precisely and only what `--taint-intrafile` adds, so it has
nothing to contribute on this file; the default intraprocedural taint analysis already reaches the sink,
in both arms.

**A taint-free arm is not constructible at this pin**, established from the engine's own option list:
the only taint options are `--taint-intrafile` and `--guarded-taint-signatures` (the latter requiring
`--experimental`), `--optimizations=none` toggles optimizations rather than taint, and the `--pro`
family requires the proprietary engine, which is unlicensed and deliberately unused (AAP §0.3.2). So
"taint off" here means *intraprocedural taint*, not *no taint*.

Both facts explain the observation. **Neither converts it into a pass**, and neither was used to. The
expectation anchored to the mandated file remains unmet.

### The discriminating pair on another file — a separate measurement, never a substitute

| Arm | Subject | Exit | Elapsed | Findings | SARIF bytes | SARIF sha256 |
| --- | --- | --- | --- | --- | --- | --- |
| **on** | `sql/hive/src/main/scala/org/apache/spark/sql/hive/client/HiveShim.scala` | 0 | 4 s | **2**, lines **828** and **834**, each `codeFlows=1` with 5 dataflow steps | 10,021 | `1a6c9a57986062ef4cc8683acbbf00335badedadadcea461d5ecced6f62c0d24` |
| **off** | the same file | 0 | 3 s | **0** | 2,341 | `6669ca2c5fcb0666efe3591a1c33b55d2f478fbb6a26febc753c6fc171977ced` |

Measured from `harness/artifacts/logs/taint-ab-hiveshim-on.log` with
`harness/artifacts/logs/taint-ab-hiveshim-on.sarif`, and from
`harness/artifacts/logs/taint-ab-hiveshim-off.log` with
`harness/artifacts/logs/taint-ab-hiveshim-off.sarif`. **A naming caveat that a later reader will
otherwise trip over:** those two logs record their output path under the **pre-rename** names
`taint-ab-on.sarif` and `taint-ab-off.sarif`. The files were renamed to their subject-bearing names
afterwards, the digests above are the digests of the renamed files on disk, and no `taint-ab-on.sarif`
or `taint-ab-off.sarif` exists on disk — so the log's own output-path field is stale where its digest
is not.

**This pair does not satisfy the AAP requirement and is not offered as satisfying it.** It is a
discriminating result — 2 against 0 from one flag — on a file the AAP does not name, and the AAP names
one subject. Reporting a different file's pair as though it met the mandated one is exactly the
substitution AAP §0.1.3 forbids. It is recorded here as its own measurement, with its own subject, and
the mandated pair's verdict above stands unchanged.

### Two controls on the mandated file, and what each excludes

| Control | Rule change | Observed | What it excludes |
| --- | --- | --- | --- |
| Search-mode | the same patterns with `mode: taint` **removed**, the rule preserved verbatim at `harness/artifacts/logs/taint-ab-off-control-rule.txt` | **2** findings, `DiskStore.scala` lines **72** and **215**, **no** `codeFlows` on either — `harness/artifacts/logs/taint-ab-search-control.sarif`, 4,424 bytes, sha256 `272a530fea4ef95417cd539b5964a70f6805e5def72ab58264cf73dbbbdb8ceb` | that the taint rule's line-72 result is merely a pattern match: the pattern alone matches a **second** site the taint rule never reports |
| Source-removed | `mode: taint` kept, `pattern-sources` replaced with an unmatchable marker, the rule preserved verbatim at `harness/artifacts/logs/taint-ab-source-removed-control-rule.txt` | **0** findings — `harness/artifacts/logs/taint-ab-source-removed-control.sarif`, 2,347 bytes, sha256 `e98c1e1fb37c66cbf7dac92838485314b57a4561a41a6d15d9043eebbaac745f` | that the line-72 result is source-independent: remove the source and it disappears, so it is genuinely source-driven |

Both controls are on the mandated file and neither is offered as an A/B arm. `oss-scan-results/run-record.md`
§7.4 owns the run-level statement of both, and carries in addition an **inherited and unanchored**
taint result from the provisioned environment record. That record is **present** in this clone —
`harness/ENVIRONMENT.md` §11 "Test 5 — the taint A/B, in full", at its lines 609-634 — and this file
does not restate it for a reason that has nothing to do with availability: AAP §0.6.4 puts the
run-level statement in one document and that document is `run-record.md` §7.4. What the record holds is
worth being exact about, because both halves of it bear on this section. Its A/B **proper** is over a
**different subject file**, `sql/core/src/main/scala/org/apache/spark/sql/jdbc/JdbcDialects.scala`
(`:614`), not over the subject AAP §0.9.1 mandates, so it is no measurement of the pass condition this
section reports. And its own second honest note (`:629-632`) records that on the mandated
`core/src/main/scala/org/apache/spark/storage/DiskStore.scala` **both** arms report one finding at line
72 and the pair is **non-discriminating** — which is the same outcome this section measured first-hand,
inherited and unanchored corroboration of it rather than a substitute for it. Neither half makes the
mandated A/B discriminate, and **D2** stands exactly as stated.

### It is blocked at root cause, and a human has to clear it

**Blocker.** The failure is a property of the subject/rule combination and of the engine at this pin,
not of how the arms were run. On `DiskStore.scala` the rule's source and sink sit in one method, so the
only flag that changes taint behaviour cannot change the result; and no option at this pin disables
taint, so no genuinely taint-free arm exists to contrast against. Manufacturing one would mean reaching
for `--experimental` or the unlicensed Pro engine, which AAP §0.1.3 and §0.3.2 forbid, and changing the
subject or the rule to obtain the expected zero is the same prohibited move in another form.

**Human action.** A human must either (a) supply a subject-and-rule combination that genuinely
discriminates **on the mandated file** `core/src/main/scala/org/apache/spark/storage/DiskStore.scala`,
or (b) amend the AAP explicitly — either to name a different subject for this pass condition, or to
accept an inter-file taint contrast as satisfying it. Either is a decision about the requirement, which
is why it is a human's and not this run's.

**What is untrue until then.** It is **not** true that Opengrep's taint engine was proven active on the
AAP's mandated subject by an A/B result, and no sentence in this file says so. What is true, and stated
above with its evidence, is that the mandated A/B did not discriminate; that the same non-discrimination
holds with the whole ruleset loaded; that a discriminating pair exists on `HiveShim.scala` and is not a
substitute; and that the two controls establish the line-72 result is source-driven rather than a bare
pattern match. The run-level divergence entry for this failure is **D2** in `oss-scan-results/run-record.md`
§13, which owns the run's single divergence register, and its §7 carries the full arm-by-arm narrative;
neither is substituted for by anything here and nothing here softens either.

---

## Self-check against this file's validation contract

1. **Every figure names a producer record, and that record is one of the twelve listed at the top.**
   PASS — section 1 cites `maven-preflight.log`; sections 2 and 3 cite `build-reactor.log` by step
   (STEPS 3, 4, 6 to 15), and `harness/ENVIRONMENT.md:205-272` only to say where the expected-values
   table's 32 producers came from; section 4 cites `build-reactor.log` STEP 13, `cpg-frontend.log`
   STEPS 1 and 4, `harness/artifacts/MANIFEST.json` and `cpg-input-inventory.json`; section 5 cites
   `cpg-identity.txt`, `cpg-verify.log`, `joern-preflight.log`, `joern.runner-console.log`, the three
   `probe-*.identity.txt` files, `cpg-frontend.log` STEPS 6, 7 and 11, `cpg-ceiling-reverify.log` and
   `gate-record.json`; section 6 cites `cpg-input-inventory.json` and `cpg-verify.log` PHASES 1 and 2;
   section 7 cites `joern.runner-console.log`, `runner-sequence.json`, `joern.status` for what it does
   **not** carry, and `harness/ENVIRONMENT.md` as the contradicted record. Checked mechanically as well as
   by reading: every multi-digit figure in this file was extracted and matched against those records,
   with none unmatched, and every `pom.xml`/`build/mvn`/module-pom line citation was resolved in the
   pinned tree. The absence of any `build-<module-path>.log` is itself recorded, and
   `cpg-module-coverage.json` is named as superseded with nothing taken from it.
2. **All 40 projects appear exactly once; the two `pom`-packaging projects are marked expected; all 38
   JAR producers are accounted for.** PASS — the section 3 table has 40 numbered rows, the root parent
   and `assembly` are marked *produced none — EXPECTED, `packaging=pom`*, and the remaining 38 each
   carry their own main artifact with its path, from `build-reactor.log` STEP 13's per-project listing.
3. **Every coverage verdict rests on injective evidence or is recorded as unobtainable; no verdict
   rests on a package prefix or on presence.** PASS — section 6 admits only the two witness kinds
   AAP §0.5.1 names. **26** of the 38 JAR-producing modules are COVERED on a class exclusive to the
   module, measured present in the graph; **12** are NO VERDICT OBTAINABLE — 7 with no archive in the
   graph's input set and 5 with neither an exclusive class nor an exclusive `pom.properties` node — each
   named individually with what was tried. **0** verdicts rest on presence of a class another module
   ships, **0** on a package prefix, and **0** additional witness kinds are admitted. No second verdict
   column exists, and no narrowed or witness graph is presented as a substitute for the mandated one.
4. **The staged input sets are described accurately, and the assertion is described as recorded before
   the frontend ran.** PASS — section 4 keeps the two apart. For the 191-archive set this run's frontend
   was given it reports `cpg-frontend.log` STEP 1's own values, including `assertion result True` taken
   before the invocation, the 191-versus-189-distinct-digest multiset argument for why a bidirectional
   mapping is the only sufficient form, and — stated rather than implied — that STEP 1 records the
   staged-file and manifest-entry counts as not measurable at log-generation time and that no staging
   tree exists in this checkout. For the 62-archive set the graph was built over it reports
   `cpg-input-inventory.json`'s member-by-member measurement.
5. **No winner map is claimed anywhere; the provenance limitation is stated.** PASS — section 5 states
   the limitation in terms, quotes `cpg-frontend.log` STEP 6's own caveat that module attribution
   overlaps by construction, and publishes no destination-package or containment grouping at all. The
   `sql/connect/shims` collision is reported to exactly the depth the records support: 361 warnings
   attributed to that module, and both its archives absent from the graph that loads.
6. **No sentence compares one tool against another or judges any finding.** PASS — no scanner's output
   and no finding of any kind appears anywhere in this file; the only tools named are the Maven wrapper,
   the Joern bytecode frontend and the `importCpg` load, each as a step in the build-and-graph pipeline
   rather than as a subject of comparison.
7. **Markdown renders cleanly; tables are well-formed; no placeholder text and no invented numbers.**
   PASS — the section 6 verdict tables were generated directly from `cpg-input-inventory.json` and
   `cpg-verify.log` rather than transcribed, every other figure carries its citation, and every value
   that could not be established is named as such in section 7 instead of being estimated.
8. **The graph's status is stated before any graph number, and the attempt to satisfy the requirement
   is recorded with its evidence rather than deferred or softened.** PASS — the STATUS block states the
   all-JAR requirement **UNMET, ATTEMPTED AND BLOCKED** before any count appears, with the 8 h 01 m
   invocation over the complete input set, the commit proof at the heap used, the bytecode-level
   diagnosis of the fixed array-length bound, the two-heap re-verification, the refused partial write
   with its size and digest, and the six mitigations examined against the frontend's own flag surface.
9. **One graph identity, stated wherever the graph is cited, with its provenance and its
   re-verification.** PASS — one pair, 541,309,809 bytes and sha256 `4616845a…4730c7`, with
   `cpg-identity.txt` named as the record of account and `record_of_account()` named as how it was
   resolved; the graph is stated as **written by provisioning on 2026-08-30, not by this run**, at every
   place it is cited. Section 5 lists all five loads with their timestamps and results, and it
   distinguishes the four whose comparison against the record of account ran immediately before the load
   from the one — Stage 3 — whose recompute was contemporaneous with the load while its comparison ran
   2026-09-01T14:52:54Z in clone 0, after the 14:25:10Z→14:41:24Z load; no sentence in this file presents
   that late comparison as satisfying AAP §0.8.2's *immediately before every load*. Every appearance of a
   superseded identity is an appearance **as** something superseded, and there are five: the environment
   record's 541,255,894 / `26d327cc…` quoted as the contradiction in section 5's and section 7's
   contradiction tables and named as that record's — never as a lineage of this run — in section 6 before
   its verdict table and in section 7's Stage 3 lineage paragraph; and the 605,687,359 /
   `ceefe60e…` pair named in the producer-records note above as the graph the superseded
   `cpg-module-coverage.json` edition described. No figure in this file is taken from any of them.
