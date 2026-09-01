# Build record — the pinned Apache Spark tree, its full-reactor build, and the graph over its bytecode

**Subject.** Apache Spark at the pinned commit `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` of
`https://github.com/blitzy-public-samples/blitzy-spark` — Spark `4.1.0-SNAPSHOT`, Scala `2.13.17`,
`java.version` 17, `maven.version` 3.9.11, read from that tree's own root pom at `pom.xml:29`,
`pom.xml:178`, `pom.xml:120` and `pom.xml:123`.

## What this file is, and what it owns

This is the build-and-graph provenance record for the pinned tree. It is the **owner** of exactly two
verdicts:

1. **the per-project JAR outcome for all 40 reactor projects** — section 3, and
2. **the per-module graph coverage verdict with its evidence** — section 6.

`oss-scan-results/run-record.md` indexes both and must not substitute for either. In the other
direction, this file states no per-tool finding count, no severity mapping and no scanner outcome:
those belong to `tool-status.md`, `severity-map.md` and the dataset, and nothing here bears on them.

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

## The six producer logs — the only sources of fact in this file

| Producer log | What this file takes from it |
| --- | --- |
| `harness/artifacts/logs/maven-preflight.log` | the Maven pre-check verdict (section 1) |
| `harness/artifacts/logs/build-reactor.log` | the build command, the JVM major and Maven version used, the reactor's project count and build order, the per-project SUCCESS/FAILURE, and the on-disk per-project artifact outcome (sections 2 and 3) |
| `harness/artifacts/logs/cpg-input-inventory.json` | the JAR inventory, the exclusions and the staging manifest (section 4), and the coverage-witness inputs (section 6) |
| `harness/artifacts/logs/cpg-frontend.log` | this run's frontend invocation over the complete input manifest, its serialization failure with the bytecode-level diagnosis, the mitigations examined, and the observed overwrite and AST-creation-failure metrics measured over that complete set (sections 4 and 5); PART 2 records the per-module witness graph's own invocation |
| `harness/artifacts/logs/cpg-verify.log` | the `importCpg` verification counts and per-module coverage witnesses for the graph at the sanctioned path (section 6, first verdict column); PART 2 records the witness graph's load and its 38 witness queries (section 6, second verdict column) |
| `harness/cpg/spark.cpg` | the graph at the path the AAP names — a provisioned symlink whose resolved target is host-global and was written by provisioning, not by this run (STATUS, D1 and D4) |

No `harness/artifacts/logs/build-<module-path>.log` exists, and none is cited: section 3 records why
none was needed.

## STATUS — read this before any graph number below

> **HALT DECLARED. The graph AAP §0.5.1 mandates — one graph over every JAR the build produced — does
> not exist and cannot be produced by the pinned frontend.** This run assembled the complete
> 191-artifact input, asserted it, and invoked the frontend over all of it; the frontend built the
> graph in memory and then could not write it, because flatgraph serializes the entire string pool
> through a single `ByteArrayOutputStream` bounded at `Integer.MAX_VALUE - 8`. The only change that
> would clear that bound is excluding inputs, which AAP §0.5.1 requires against and §0.9.2 names as a
> condition that **stops the run rather than gets repaired**. So this is a halt: reported with its
> evidence, not worked around. Two consequences follow and neither is softened anywhere in this file —
> **no current-run complete-input graph exists**, and **seven of the 38 JAR-producing modules have no
> coverage verdict against such a graph**. Section 6's second column measures what is measurable
> without it and carries four explicit disclaimers making it no substitute for it.

Three facts bound what the numbers in this file describe, and reading a coverage figure without them
would misread it.

- **The full-reactor build was performed by this run.** `build-reactor.log` records it end to end:
  `BUILD SUCCESS`, Maven exit code 0, 40 of 40 reactor projects `SUCCESS`, and all 38 JAR-packaging
  projects confirmed on disk to have produced their own main artifact.
- **The JAR inventory and its staging manifest were produced by this run.** `cpg-input-inventory.json`
  inventories 191 own artifacts from those 40 projects, stages them into one fresh directory, and
  proves the mapping total and injective in both directions before any frontend invocation.
- **The graph at the AAP's path was not created by this run. This run attempted to create one over
  its complete input set, and the attempt failed in serialization at a fixed toolchain bound.**
  Three entries of the run's divergence register carry this — **D1**, **D3** and **D4**, owned by
  `oss-scan-results/run-record.md` §13, which is the run's single register and the only place these
  labels are defined. None of them is repaired by anything in this file:

  - **D1 — halt-class, attempted and blocked.** AAP §0.1.1 requires the graph to be created by this
    run. The graph at the resolved path was written by the provisioning invocation before this run's
    first command. This run therefore assembled its complete 191-artifact input manifest, asserted it
    total and injective in both directions, proved a 128 GiB heap committable, and invoked the pinned
    frontend over the whole of it under JDK 21. After **8 h 01 m** and a **113.3 GiB** peak RSS the
    frontend terminated in its persistence step with
    `java.lang.OutOfMemoryError: Required array length 2147483639 + 72 is too large`, raised inside
    `flatgraph.storage.WriterContext.finish`. **No graph was produced**, and the truncated partial
    write it left behind (691,541,019 bytes, sha256
    `b1559c930a7b9ced717a0babf9a7e172d2b93d2cdef45a959304f063aedfe408`) was recorded as evidence and
    explicitly not accepted. `cpg-frontend.log` STEP 8 establishes from the failing method's own
    bytecode that this is a fixed `Integer.MAX_VALUE - 8` array-length bound on the single
    `ByteArrayOutputStream` that flatgraph serializes the graph's entire string pool through — not a
    heap shortage, and not movable by any heap size. STEP 10 enumerates every mitigation examined and
    why each is unavailable: the only lever that would work is excluding inputs, which AAP §0.3.2 and
    §0.9.2 place among the conditions that stop the run rather than get repaired.
  - **D3 — recorded difference.** The graph at the path was built from 62 archives, 285,122,375
    bytes, from 31 modules, against this run's 191 own artifacts, 431,184,822 bytes, from all 38
    JAR-packaging projects.
  - **D4 — extended at this checkpoint.** The bytes at the resolved path today (**541,309,809**, sha256
    `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`) differ from the identity every
    earlier stage of this run recorded and re-verified before each of its loads (**541,255,894**,
    sha256 `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`), and from the third
    pair D4 already records for probe query 03 (**548,118,435**, sha256 `f8c71562…`). Provisioning has
    re-run against this host more than once, replacing the shared file each time. The earlier records
    remain accurate as of the loads they describe — every load verified the identity immediately before
    reading and every comparison matched — and the inherited identity chain simply cannot be
    re-verified today. Recorded rather than reconciled.
  - **Why nothing was written to the resolved path.** `/opt/blitzy-harness/cpg/spark.cpg` is
    host-global and read by concurrent clones while they scan. Writing there would corrupt siblings'
    in-flight loads; and with no valid current-run graph to install, there was in any case nothing to
    install.

**The consequence lands on section 6, and section 6 now carries two verdicts per module rather than
one.** Because the graph at the path was built from the narrower input set, seven of the 38
JAR-producing modules have no coverage verdict obtainable *from that graph*. To establish what is
measurable in spite of that, this run built a **per-module witness graph** over one primary artifact
per JAR-producing module — 38 artifacts, 130,718,491 bytes — and queried every module's witness in it.
That graph is a labelled capability measurement: it is not the graph the AAP mandates, it is not at
the sanctioned path, no runner loads it, and it contributes no dataset row. Section 6 reports both
columns side by side and never lets the second stand in for the first.

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

**The invocation, quoted from the script that ran it** (`build-reactor.log` STEP 10), with
`MAVEN_OPTS="-Xss64m -Xmx6g -Xms2g -XX:ReservedCodeCacheSize=512m"`:

```bash
./build/mvn --no-transfer-progress -DskipTests \
  -Pyarn -Pkubernetes -Phive -Phive-thriftserver -Pvolcano \
  -Dmaven.repo.local="/tmp/blitzy/scratch/f38258d3-f87d-44f5-bedc-af512c69e0ab/w-005/build/m2" \
  package
```

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
| Wall clock | started 2026-08-30T20:18:38Z, elapsed **2460 s** as the runner measured it; Maven's own `Total time: 40:55 min` and `Finished at: 2026-08-30T20:59:38Z` | STEP 11 |
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

The "own artifacts inventoried" column is `cpg-input-inventory.json`'s per-project count of the
artifacts that project itself emitted, and the column sums to that file's own total of **191**
(section 4). Every one of the 40 rows — the outcome, the primary artifact's path and the own-artifact
count — was re-checked against this run's inventory measurement, project by project, with **zero
disagreements**, so the table and `cpg-input-inventory.json` are one measurement cited twice. The "primary artifact" column is each project's main, unclassified, non-`original-`,
non-`-tests` JAR — the artifact the coverage test in section 6 is applied to.

**Two cautions so this table and `cpg-input-inventory.json` cannot be read as disagreeing**, both
stated by `build-reactor.log`'s own cross-reference section. First, every path here is relative to the
build tree named in section 2, not to `SPARK_SRC`. Second, the cross-check between the two documents
is project → outcome and artifact → relative path, never a digest comparison: a JAR entry carries a
build timestamp, so a second build of identical source yields a different digest, and a digest
difference between two builds is not a discrepancy while a missing project or a differently located
artifact would be.

### The two projects that produced no JAR, and why that is the expected outcome

- **`(root parent)` — `spark-parent_2.13`, `packaging=pom` at `pom.xml:30`.** `build-reactor.log`
  STEP 13 records `own artifact: NONE - EXPECTED, packaging=pom`. Its build directory does contain
  `target/spark-parent_2.13-4.1.0-SNAPSHOT-tests.jar`, 20,371 bytes — an attached test-jar artifact,
  listed as `also` rather than `MAIN`. The parent still produces no main artifact, which is the
  expected outcome and not a failure.
- **`assembly` — `spark-assembly_2.13`, `packaging=pom` in its own pom**, confirmed both by STEP 6's
  per-module packaging listing and by Maven's own `[pom]` marker at STEP 12. Its build directory holds
  340 JARs, every one a copied runtime dependency written there by `copy-module-dependencies`
  (`pom.xml:3095` with its output directory at `pom.xml:3102`) and not one of them `assembly`'s own;
  STEP 13 counts and excludes them.

Neither is a failure, and neither is left to be inferred from an absence.

### All 38 JAR-packaging projects produced their own artifact

`build-reactor.log` STEP 13's independent on-disk pass reports `38 of 38` and
`JAR-PACKAGING PROJECTS WITH NO OWN MAIN ARTIFACT: (none)`. AAP §0.9.2 makes any of the 38 failing to
produce its own artifact a halt, *"including a project the expected-values list does not name"* — so
this file would otherwise have to name each project that did not and quote its log. **There is no such
project, so there is no such entry.**

### The six JAR producers the expected-values table does not name — a recorded difference, never a halt

The expected-values table names 32 JAR producers, and it was measured over the **narrowed**
provisioning build, whose 33-project reactor is what `harness/ENVIRONMENT.md` section 5 records. A full
reactor packages 38, so six are new to it (`build-reactor.log`, "THE RECORDED DIFFERENCE", derived by
set difference against that record's own list of 32):

- `tools`
- `examples`
- `connector/kafka-0-10-token-provider`
- `connector/kafka-0-10`
- `connector/kafka-0-10-sql`
- `connector/kafka-0-10-assembly`

All six appear in Maven's reactor summary as `SUCCESS` — STEP 12 isolates their six summary lines
verbatim — and STEP 13 confirms each produced its own main artifact on disk. **The halt rule is
one-directional** (AAP §0.8.3): a module that produced a JAR in the rehearsal and produces none now is
a halt; the reverse is not. So the six are a recorded difference, they legitimately entered this run's
graph input set (section 4), and they are the reason the graph's method count is checked as a **floor
rather than a window** — an anchor measured over 32 JAR producers cannot bound a graph built from 38.
Nothing was trimmed in either direction to make a number fit.

### `python/pyspark` — an expected non-JAR outcome, and not one of the 40

`python/pyspark` is one of the twelve authoritative scope roots and is scanned, but it is **no Maven
module and appears in no reactor**: `build-reactor.log` STEP 14 records `grep -c '<module>python'` = 0
against the root pom and `ls` failing on `python/pyspark/pom.xml`. It therefore has no row in the table
above and none is invented for it. The same holds for `resource-managers/kubernetes/docker`, which the
file-based tools reach through the mid-path `**` of the Kubernetes glob.

### The diagnostic pass — not needed, and the protocol it would have followed

The reactor did not fail, so **no per-project diagnostic pass was run and no
`harness/artifacts/logs/build-<module-path>.log` exists**. `build-reactor.log` states the same at its
end: *"0 projects UNESTABLISHED, 0 diagnostic-pass logs needed or written."* The protocol is recorded
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

## 4. The JAR inventory and the staging manifest

Every figure in this section is `harness/artifacts/logs/cpg-input-inventory.json`'s measurement, taken
over the build tree named in section 2.

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
| `common/network-yarn` unattached shaded shuffle JAR | `common/network-yarn/target/scala-2.13/spark-4.1.0-SNAPSHOT-yarn-shuffle.jar`, 109,208,027 bytes, sha256 `296b8013d55bbf38e80206a3446e8fa38eb0a3fa7a64a84df53e0470914eaeda` | declared at `common/network-yarn/pom.xml:97` with `:96` and `:37`; **asserted present**; classified as the project's own **by its declared output path**, because its filename carries no artifactId and a filename rule alone would have excluded it | `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` |

### The totals, and the exclusions counted rather than silent

| Measurement | Value |
| --- | --- |
| JAR files enumerated under the 40 projects' build directories | **627** |
| Classified as a project's own | **191**, totalling **431,184,822 bytes** |
| Of those, carrying bytecode | 110 — recorded per file as a class-entry count, never used as a reason to drop one |
| Class entries across the own artifacts | 99,723 |
| **Excluded copied dependency JARs** | **422** (`copied_runtime_dependency`) |
| Also excluded: test-resource fixtures inside a module's compiled-output tree | **14** (`test_resource_fixture`) |
| Total excluded | **436** |
| Undecided provenance | **0** |
| Arithmetic | own 191 + excluded 436 + undecided 0 = 627 enumerated |

**Every figure in that table except the byte total is identical to the figure the run's earlier build
produced**, measured independently over a different build of the same commit: 627 enumerated, 191 own,
110 carrying bytecode, 99,723 class entries, 422 copied dependencies, 14 fixtures, 0 undecided, and the
same 287/149 split of the excluded files' coordinate sources. The byte total differs by 78 bytes and
every per-file digest differs, because a JAR embeds entry timestamps — which is exactly why the graph's
input set is measured from the tree that produced it rather than carried over.

Each excluded file's coordinate is recorded: 287 from the archive's own Maven descriptor and 149 from
the filename alone. The per-project exclusions are `core` 12 copied plus 3 fixtures,
`sql/connect/client/jvm` 46 plus 2, `assembly` 340, `examples` 23, `mllib` 1, `sql/core` 2 fixtures,
`sql/hive` 4 fixtures, `sql/connect/common` 2 fixtures and `sql/hive-thriftserver` 1 fixture.

**Nothing the project itself emitted was sampled or dropped.** Main artifacts, `original-` pre-shade
siblings, shaded siblings, classifier artifacts, `-tests` artifacts, `-sources` and `-test-sources`
artifacts, and the unattached shaded shuffle JAR are all retained.

### The staging manifest

The bundled `jimple2cpg` accepts **one** input path, so "every JAR the build produced" and "one input
path" are reconciled by staging the inventory into a single directory.

| Property | Value |
| --- | --- |
| Staging directory | `harness/artifacts/cpg-input` (in this clone's checkout; `.gitignore:31` `artifacts/` keeps it out of git's ordinary collection) |
| **Absent before use** | **yes** — the staging script halts and exits non-zero if the directory exists, so a run that reached the copy step is a run that found it absent; the directory's own birth time, read with `stat`, is **23:19:24.607Z** and its last file was copied at **23:19:28.541Z** |
| Never cleared, never reused | a pre-existing staging tree is a halt (AAP §0.9.2): a stale archive left in it would enter the graph silently, and a graph's silence about a module is indistinguishable from a clean result |
| Staged-name form | `<module-path-with-slashes-as-underscores>__<original-filename>`, with the reactor root project's slug the literal `root` since it has no module path |
| Staging method | file copy, chosen over a hard link so the staged bytes are independent of the build tree and can be re-hashed as an independent measurement |
| Ordering | by module path, then by the artifact's path relative to the build tree — recorded so the input set is reproducible byte for byte |
| Staged files / bytes | **191** / **431,184,822** |

**The assertion, and why it is not a count plus a hash set.** The mapping asserted is: every inventory
entry maps to exactly one staged name and one sha256, and every file found in the staging directory
maps back to exactly one inventory entry — **total and injective in both directions**. Result `true`,
computed at **23:19:29.813Z** — before the frontend's **23:21:24.937Z** invocation, which is what
`computed_before_the_frontend_ran = true` records. Inventory → staged: 191
mapped, 0 unmapped, 0 violations. Staged → inventory: 191 mapped, 0 unmapped, 0 names missing on disk,
0 violations. All 191 staged digests were re-hashed from the disk listing rather than from the staging
loop's own list, with 0 mismatches.

A set discards multiplicity, so two different multisets can share both a count and a hash set — and
**this input set is a live example of why that matters**: 191 files carry only 189 distinct digests,
over two collision groups, both inside `connector/kafka-0-10-assembly` (its `original-` artifact equals
its `-tests` artifact byte for byte, and its `-sources` equals its `-test-sources`). A set-based check
would have compared 191 to 191 and 189 to 189 and passed even if one file had been staged twice and
another omitted.

**This logged manifest, not a per-module class search, is what establishes that every JAR the build
produced was in the input set this run assembled** — one file at a time and in both directions, rather
than by a count and a hash set. Two precisions follow, and both matter. It was logged **before** any
frontend invocation, so it cannot have been shaped to fit whatever a frontend happened to ingest. And
it is a statement about the input set, not about the graph that exists: per **D3** the frontend that
wrote that graph was given 62 archives from 31 modules, so these 191 archives were not its input, and
no claim that they reached the graph is made here or in section 6. Coverage — whether a module's own
code reached the graph — is a separate question with a separate proof, and it is section 6's.

---

## 5. Determinism: what is reproducible, and what is not measurable

### Which input set these metrics belong to, and which graph is at the AAP's path

These are two different things at this checkpoint, and conflating them would misattribute every number
in this section.

**The metrics below belong to this run's own frontend invocation over its complete 191-artifact input
set** — the invocation D1 records, which built the graph in memory and then failed in serialization.
They are measurements of that run's processing of the complete set, and they are valid as such: the
extraction and AST passes completed over every staged artifact, and the failure came afterwards, in
persistence. `cpg-frontend.log` is that invocation's log and is the sole source for them.

**The graph at the AAP's path is a different artifact, written by provisioning.**
`harness/cpg/spark.cpg` is a 33-byte **symlink** to `/opt/blitzy-harness/cpg/spark.cpg`, one hop with
no intermediate indirection, and both names resolve to one file. The size is always taken through the
link: the 33-byte no-follow reading is the length of the target path string and would describe nothing.
**Three** identities are on record for that one path, which is **D4**:

| When measured | Bytes | sha256 | Methods |
| --- | --- | --- | --- |
| The verification load, the Stage 3 Joern runner, and probe queries 01 and 02 — each re-verified immediately before its own load | 541,255,894 | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | 1,397,339 |
| Probe query 03, likewise re-verified immediately before loading | 548,118,435 | `f8c71562…` | 1,399,866 |
| At this checkpoint, from the bytes on disk | 541,309,809 | `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` | not loaded — no load was performed against these bytes |

Provisioning has re-run against this host more than once and replaced the shared file each time. Two
things follow, and they are different:

- **No load in this run ever read bytes other than the ones it had just recorded.** Every load
  re-verified size and digest immediately beforehand and every comparison matched; probe query 02's
  reproduction check **halted** on a mismatch rather than loading mismatched bytes. That is the
  protection the plan's re-verification rule exists to give, and it held.
- **The identity `harness/ENVIRONMENT.md` states is no longer the identity on disk.** That is a record
  contradiction on an inherited artifact, it is of the halting kind, and §7 states it in full with both
  values. Nothing repairs it: the file is host-global, shared with concurrent readers, and was not
  written by this run.

Per **D1**, no graph at that path was written by this run. This run attempted to write one, over the
complete input set, and the attempt failed for the reason STATUS states and `cpg-frontend.log` STEP 8
proves from the failing method's bytecode.

### The two observed metrics

Both metrics below are `harness/artifacts/logs/cpg-frontend.log`'s recount from the frontend's own
preserved output stream, and both are **observed facts of that run rather than pre-approved
expectations**. Neither is treated as acceptable because a document expected some other number.

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

**Overwrites grouped by the module and artifact the affected entries are contained in.** Of the 27,843
distinct destinations, **17,305** are contained in exactly one module's artifacts — and **17,288** of
those in more than one artifact *of that same module*, which is the shaded artifact and its
`original-` pre-shade sibling duplicating each other. **10,161** are contained in more than one
module's artifacts, which is cross-module vendoring: `org/sparkproject/io` netty classes, and
`org/sparkproject/guava` and `org/sparkproject/connect` likewise. By destination package the largest
groups are `org/apache/spark/**` at 20,454 distinct destinations over 25,788 warnings, then
`org/sparkproject/io/**` 2,593, `org/sparkproject/connect/**` 2,017, `org/sparkproject/guava/**`
2,017, `org/apache/hive/**` 126, `org/junit/internal/**` 98, and `META-INF/maven/org.apache.spark/**`
80 over 326.

**The 377 destinations that match no entry name in any staged archive are accounted for individually,
not rounded away.** They come from *nested* archives: 12 of the 191 staged artifacts carry 28 nested
`.jar` entries between them — test fixtures such as `artifact-tests/junitLargeJar.jar`,
`TestHelloV2_2.13.jar`, `TestUDTF.jar`, `SPARK-33084.jar` and `data/files/TestSerDe.jar`, all inside
`-tests` and `-test-sources` artifacts of `core`, `sql/core`, `sql/hive`, `sql/hive-thriftserver`,
`sql/connect/common` and `sql/connect/client/jvm` — and `--recurse`, which the plan mandates, descends
into them. By family the 377 are **350** junit-framework classes, **42** test-fixture classes
(`HelloWorld/Main`, `MyCoolClass`, `com/example/Hello`, the Hive UDTF fixtures), **6** `META-INF`
descriptors of those nested fixture projects, and **5** `module-info.class` from multi-release entries.
Every one is a nested-archive entry rather than a top-level entry of a staged artifact, which is why a
containment lookup against the staged archives' own entry names does not find them.

**AST-creation failures grouped the same way.** All 23 are `sql/connect` classes: **13** under
`org/apache/spark/sql/connect/client/arrow`, **4** under `org/apache/spark/sql/connect/client`, **5**
under `org/apache/spark/connect/proto`, and **1** `org/apache/spark/sql/connect/StreamingQueryListenerBus`.
Total accounted for: 23.

Both figures are compared against the provisioning runbook's expectations and both differ, with both
values recorded: roughly 5,700 overwrite warnings expected against **33,784** observed, and roughly 36
protobuf-generated AST failures expected against **23** observed of which **5** are protobuf-generated.
The overwrite figure is higher for a reason this run can name rather than guess at — its input set is
191 artifacts including every pre-shade sibling and every `-tests` artifact, against the 62 archives
the runbook's own guidance produces, and pre-shade/shaded duplication alone accounts for 17,288 of the
distinct destinations.

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

### The `sql/connect/shims` collision, resolved by querying the graph

This is the collision most likely to be misread as a coverage problem, because `sql/connect/shims`
ships stub `SparkConf`, `SparkContext` and `RDD` classes that `core` and the SQL modules also ship. It
is settled the only way permitted — by querying the graph and reporting what is there, not by inferring
a winner. `cpg-verify.log` STEP 18 ran the queries:

| Class queried | Type declarations | Methods |
| --- | --- | --- |
| `org.apache.spark.SparkConf` | 2 | 298 |
| `org.apache.spark.SparkContext` | 2 | 1,100 |
| `org.apache.spark.rdd.RDD` | 2 | 1,022 |
| `org.apache.spark.api.java.JavaRDD` | 2 | 74 |
| `org.apache.spark.unused.UnusedStubClass` | 29 | 29 |

All four classes the shims artifact ships as client-only stubs are present, each carrying a full
implementation's worth of methods rather than the near-zero a stub would show. **Which archive supplied
those definitions is not claimed**, and in that graph the question does not arise: its input set
excluded both `spark-connect-shims` archives, so no collision with the shims stubs occurred in it at
all, and the absence of a shims coverage verdict in section 6's first column is an input-set fact
rather than a graph defect. `org.apache.spark.unused.UnusedStubClass` is included for scale: it comes
from Spark's `org.spark-project.spark:unused` stub dependency and is shipped by 35 modules, which is
also why several modules' shared-class counts read as 1 against nearly every other module.

**In this run's own complete input set the collision is real, and it is measured rather than assumed.**
Both `spark-connect-shims` archives are present, so each of the eleven classes the shims artifact ships
as a stub is contained in **four** archives across **two** modules — the shims shaded and pre-shade
pair together with `core`'s pair for `SparkConf`, `SparkContext`, `rdd/RDD` and `api/java/JavaRDD`, and
with `sql/core`'s pair for `ExperimentalMethods`, `SparkSessionExtensions`, `execution/QueryExecution`,
`internal/SessionState`, `internal/SharedState`, `sources/BaseRelation` and
`util/ExecutionListenerManager`. Each was overwritten exactly **3** times, consistent with four
containing archives and one surviving definition. `org.apache.spark.unused.UnusedStubClass` sits in 35
archives across 35 modules and was overwritten 34 times. Which definition survived is not measurable
from the frontend's output, for the reason stated immediately above, and no winner is claimed. What is
in the graph is settled by querying it — section 6's second column does that in the per-module witness
graph, where the shims primary artifact is present, and the answer is substantive: **there, all eleven
classes carry stub-sized method counts** (`SparkConf` 8, `SparkContext` 2, `rdd.RDD` 8,
`api.java.JavaRDD` 2, and 2 to 4 for the other seven) against the 298, 1,100, 1,022 and 74 the same
classes carry in the graph at the sanctioned path, whose input excluded the shims archives. So in a
graph containing the shims artifact the stub definitions displace the real ones for those eleven
classes. That is what the graph contains, measured by querying it; it is not a claim about which
archive the frontend read last.

---

## 6. The per-module graph coverage verdict

This file owns this verdict. Its inputs are `harness/artifacts/logs/cpg-input-inventory.json`, which
measured witness exclusivity from the archives, and `harness/artifacts/logs/cpg-verify.log`, which
queried the graph for each witness under two `importCpg` loads this run performed.

### The two questions, kept apart

**Delivery** — was every JAR the build produced in the input set this run assembled? — is already
proved, and not by a class search: section 4's staging manifest proves it one file at a time, total and
injective in both directions over 191 files, logged before any frontend invocation. What that does
**not** establish, and what nothing can establish for this graph, is that those 191 archives entered
it: per **D3** the frontend that wrote this graph was given 62 archives from 31 modules. That
difference is the whole reason seven modules below have no verdict obtainable, and it is why delivery
and coverage cannot be run together here.

**Coverage** — did a module's own code reach the graph? — is this section's question, and it has exactly
one test, stated once and applied uniformly:

> **A class present in that module's primary artifact — its main, unclassified, non-`original-`,
> non-`-tests` JAR — and absent from every other module's artifacts.**

Three qualifications, all load-bearing:

- **A shared package prefix is never admissible evidence.** Every Spark module ships under
  `org.apache.spark`, so a prefix test lets one module vouch for a dozen absent ones. No prefix test
  appears anywhere in this verdict or in either producer log it rests on.
- **A class shared with a same-module sibling still qualifies**; a class shared with **another module**
  does not. That is precisely what makes the test satisfiable for a shaded artifact and its pre-shade
  sibling: they share every class, so "unique to that JAR" is unsatisfiable while "unique to that
  module" is not.
- **Where no such class exists, the module-exclusive `META-INF/maven/**/pom.properties` file node is
  accepted and named as the weaker witness.** `sql/connect/shims` is the module AAP §0.5.1 anticipates
  needing that fallback, and it is: 0 classes are exclusive to it, because it ships stub `SparkConf`,
  `SparkContext` and `RDD` classes that `core` and the SQL modules also ship. Its weaker witness is
  exactly `META-INF/maven/org.apache.spark/spark-connect-shims_2.13/pom.properties`, confirmed
  module-exclusive at the archive level.

**Exclusivity was re-derived independently rather than taken on trust.** `cpg-verify.log` STEP 14
recomputed the class-to-module index by reading all 191 own artifacts directly — 67,135 distinct class
names across them, 45,501 across the primary artifacts alone, the same two figures the inventory
states as its own measurement basis — and **not one of the 29 declared class witnesses fails
independent exclusivity**. The shims descriptor node is confirmed module-exclusive by that recount too.

### The verdict, one row per JAR-producing module

The two `packaging=pom` projects — the root parent and `assembly` — have no primary artifact and are
outside this test by construction, so they carry no coverage verdict. That leaves the 38
JAR-producing modules below. Every row carries a **named witness**: a class exclusive to the module, the
module-exclusive `pom.properties` node, or a named least-shared class explicitly labelled presence
evidence with the other module's artifact that also ships it named. No row rests on a package prefix.
The "graph result" column is the query `cpg-verify.log` ran — `cpg.typeDecl.fullNameExact(<witness>)`
for a class witness, `cpg.file.name.count(_.endsWith(<node>))` for the descriptor node.

| Module (JAR-producing) | Witness kind | Witness, as queried | Archive-level exclusivity | Graph result | Verdict |
| --- | --- | --- | --- | --- | --- |
| `common/sketch` | class exclusive to the module | `org.apache.spark.util.sketch.BitArray` | exclusive; 15 classes exclusive to this module | PRESENT — 2 type declarations, 28 methods | **COVERED** |
| `common/kvstore` | class exclusive to the module | `org.apache.spark.util.kvstore.ArrayWrappers` | exclusive; 38 classes exclusive to this module | PRESENT — 2 type declarations, 4 methods | **COVERED** |
| `common/network-common` | least-shared class — presence evidence only | `org.apache.spark.network.TransportContext` | NOT exclusive — also shipped by `common/network-yarn` | PRESENT — 3 type declarations, 57 methods | **COVERED** on presence evidence, not exclusivity |
| `common/network-shuffle` | least-shared class — presence evidence only | `org.apache.spark.network.sasl.ShuffleSecretManager` | NOT exclusive — also shipped by `common/network-yarn` | PRESENT — 3 type declarations, 21 methods | **COVERED** on presence evidence, not exclusivity |
| `common/unsafe` | class exclusive to the module | `org.apache.spark.sql.catalyst.expressions.HiveHasher` | exclusive; 64 classes exclusive to this module | PRESENT — 2 type declarations, 12 methods | **COVERED** |
| `common/utils` | class exclusive to the module | `org.apache.spark.BreakingChangeInfo` | exclusive; 163 classes exclusive to this module | PRESENT — 2 type declarations, 16 methods | **COVERED** |
| `common/utils-java` | least-shared class — presence evidence only | `org.apache.spark.QueryContext` | NOT exclusive — also shipped by `common/network-yarn` | PRESENT — 3 type declarations, 24 methods | **COVERED** on presence evidence, not exclusivity |
| `common/variant` | class exclusive to the module | `org.apache.spark.types.variant.ShreddingUtils` | exclusive; 33 classes exclusive to this module | PRESENT — 2 type declarations, 8 methods | **COVERED** |
| `common/tags` | least-shared class — presence evidence only | `org.apache.spark.annotation.AlphaComponent` | NOT exclusive — also shipped by `connector/kafka-0-10-assembly` | PRESENT — 2 type declarations, 0 methods | **COVERED** on presence evidence, not exclusivity |
| `sql/connect/shims` | module-exclusive `pom.properties` node — the weaker witness | `META-INF/maven/org.apache.spark/spark-connect-shims_2.13/pom.properties` | descriptor node exclusive to this module; 0 classes exclusive | ABSENT — 0 type declarations / 0 matching file nodes | **NO VERDICT OBTAINABLE** — artifacts not in the graph's input set |
| `core` | class exclusive to the module | `org.apache.spark.Aggregator` | exclusive; 5092 classes exclusive to this module | PRESENT — 2 type declarations, 54 methods | **COVERED** |
| `graphx` | class exclusive to the module | `org.apache.spark.graphx.Edge` | exclusive; 131 classes exclusive to this module | PRESENT — 2 type declarations, 120 methods | **COVERED** |
| `mllib` | class exclusive to the module | `org.apache.spark.ml.Estimator` | exclusive; 2645 classes exclusive to this module | PRESENT — 2 type declarations, 20 methods | **COVERED** |
| `mllib-local` | class exclusive to the module | `org.apache.spark.ml.impl.Utils` | exclusive; 22 classes exclusive to this module | PRESENT — 2 type declarations, 12 methods | **COVERED** |
| `tools` | class exclusive to the module | `org.apache.spark.tools.GenerateMIMAIgnore` | exclusive; 2 classes exclusive to this module | ABSENT — 0 type declarations / 0 matching file nodes | **NO VERDICT OBTAINABLE** — artifacts not in the graph's input set |
| `streaming` | class exclusive to the module | `org.apache.spark.status.api.v1.streaming.ApiStreamingApp` | exclusive; 358 classes exclusive to this module | PRESENT — 2 type declarations, 16 methods | **COVERED** |
| `sql/api` | least-shared class — presence evidence only | `org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction` | NOT exclusive — also shipped by `sql/connect/client/jvm` | PRESENT — 3 type declarations, 3 methods | **COVERED** on presence evidence, not exclusivity |
| `sql/catalyst` | class exclusive to the module | `org.apache.spark.sql.catalyst.AliasIdentifier` | exclusive; 5332 classes exclusive to this module | PRESENT — 2 type declarations, 40 methods | **COVERED** |
| `sql/core` | class exclusive to the module | `org.apache.parquet.filter2.predicate.SparkFilterApi` | exclusive; 3871 classes exclusive to this module | PRESENT — 2 type declarations, 14 methods | **COVERED** |
| `sql/hive` | class exclusive to the module | `org.apache.hadoop.hive.ql.exec.HiveFunctionRegistryUtils` | exclusive; 148 classes exclusive to this module | PRESENT — 2 type declarations, 14 methods | **COVERED** |
| `sql/pipelines` | class exclusive to the module | `org.apache.spark.sql.pipelines.AnalysisWarning` | exclusive; 284 classes exclusive to this module | PRESENT — 2 type declarations, 0 methods | **COVERED** |
| `sql/connect/server` | class exclusive to the module | `com.google.apps.card.v1.Action` | exclusive; 4153 classes exclusive to this module | PRESENT — 1 type declaration, 63 methods | **COVERED** |
| `sql/connect/common` | least-shared class — presence evidence only | `org.apache.spark.connect.proto.AddArtifactsRequest` | NOT exclusive — also shipped by `sql/connect/client/jvm`, `sql/connect/server` | PRESENT — 4 type declarations, 252 methods | **COVERED** on presence evidence, not exclusivity |
| `sql/connect/client/jdbc` | class exclusive to the module | `org.apache.spark.sql.connect.client.jdbc.NonRegisteringSparkConnectDriver` | exclusive; 2 classes exclusive to this module | PRESENT — 2 type declarations, 16 methods | **COVERED** |
| `sql/connect/client/jvm` | class exclusive to the module | `org.apache.spark.sql.application.ConnectRepl` | exclusive; 4970 classes exclusive to this module | PRESENT — 2 type declarations, 2 methods | **COVERED** |
| `examples` | class exclusive to the module | `org.apache.spark.examples.AccumulatorMetricsTest` | exclusive; 791 classes exclusive to this module | ABSENT — 0 type declarations / 0 matching file nodes | **NO VERDICT OBTAINABLE** — artifacts not in the graph's input set |
| `repl` | class exclusive to the module | `org.apache.spark.repl.Main` | exclusive; 8 classes exclusive to this module | PRESENT — 2 type declarations, 24 methods | **COVERED** |
| `launcher` | class exclusive to the module | `org.apache.spark.launcher.AbstractAppHandle` | exclusive; 32 classes exclusive to this module | PRESENT — 2 type declarations, 30 methods | **COVERED** |
| `connector/kafka-0-10-token-provider` | least-shared class — presence evidence only | `org.apache.spark.kafka010.ExceptionsHelper` | NOT exclusive — also shipped by `connector/kafka-0-10-assembly` | ABSENT — 0 type declarations / 0 matching file nodes | **NO VERDICT OBTAINABLE** — artifacts not in the graph's input set |
| `connector/kafka-0-10` | least-shared class — presence evidence only | `org.apache.spark.streaming.kafka010.Assign` | NOT exclusive — also shipped by `connector/kafka-0-10-assembly` | ABSENT — 0 type declarations / 0 matching file nodes | **NO VERDICT OBTAINABLE** — artifacts not in the graph's input set |
| `connector/kafka-0-10-assembly` | class exclusive to the module | `com.fasterxml.jackson.annotation.JacksonAnnotation` | exclusive; 5855 classes exclusive to this module | ABSENT — 0 type declarations / 0 matching file nodes | **NO VERDICT OBTAINABLE** — artifacts not in the graph's input set |
| `connector/kafka-0-10-sql` | class exclusive to the module | `org.apache.spark.sql.kafka010.AssignStrategy` | exclusive; 123 classes exclusive to this module | ABSENT — 0 type declarations / 0 matching file nodes | **NO VERDICT OBTAINABLE** — artifacts not in the graph's input set |
| `connector/avro` | class exclusive to the module | `org.apache.spark.sql.avro.AvroDataToCatalyst` | exclusive; 23 classes exclusive to this module | PRESENT — 2 type declarations, 106 methods | **COVERED** |
| `connector/protobuf` | class exclusive to the module | `org.apache.spark.sql.protobuf.CatalystDataToProtobuf` | exclusive; 825 classes exclusive to this module | PRESENT — 2 type declarations, 82 methods | **COVERED** |
| `resource-managers/yarn` | class exclusive to the module | `org.apache.spark.deploy.yarn.AmIpFilter` | exclusive; 78 classes exclusive to this module | PRESENT — 2 type declarations, 18 methods | **COVERED** |
| `common/network-yarn` | class exclusive to the module | `org.apache.spark.network.yarn.YarnShuffleService` | exclusive; 6 classes exclusive to this module | PRESENT — 2 type declarations, 36 methods | **COVERED** |
| `resource-managers/kubernetes/core` | class exclusive to the module | `org.apache.spark.deploy.k8s.Config` | exclusive; 144 classes exclusive to this module | PRESENT — 2 type declarations, 254 methods | **COVERED** |
| `sql/hive-thriftserver` | class exclusive to the module | `org.apache.hive.service.AbstractService` | exclusive; 210 classes exclusive to this module | PRESENT — 2 type declarations, 26 methods | **COVERED** |

### What that table adds up to

| Outcome | Count |
| --- | --- |
| JAR-producing modules under the test | **38** |
| **Covered** — witness measured in the graph | **31** |
| — of those, on a class exclusive to the module | 25 |
| — of those, on presence evidence, labelled as presence and never as exclusivity | 6 |
| **No verdict obtainable from this graph** | **7** |
| Verdicts resting on a shared package prefix | **0** |
| Winner maps claimed | **0** |
| **Second column** — witness present in the per-module witness graph this run built | **38 of 38**, including all **7** above |

**Witness kinds as declared across all 38**, from the inventory's own measurement: 29 modules have a
class exclusive to the module, 1 has only the module-exclusive Maven descriptor node
(`sql/connect/shims`), and 8 have neither and carry presence evidence instead.

**The eight with no exclusive witness of either kind are named, with the reason.** A shaded artifact of
*another* module vendors their classes, and their descriptor node too, so nothing in their primary
artifact is theirs alone across this input set: `common/network-common`, `common/network-shuffle` and
`common/utils-java` into `common/network-yarn`'s shuffle JAR; `sql/api` and `sql/connect/common` into
`sql/connect/client/jvm`'s shaded artifact; `common/tags`, `connector/kafka-0-10` and
`connector/kafka-0-10-token-provider` into `connector/kafka-0-10-assembly`. This is a consequence of the
input set being **complete** — a narrower set contains fewer fat artifacts, so more classes are
exclusive. For those modules **coverage cannot be established injectively, and that is named here
rather than presented as a pass**; their membership in this run's assembled input set is nevertheless
established for every one of them by section 4's bidirectional manifest, which does not depend on class
exclusivity at all.

**The descriptor fallback was tested for the two Kafka modules and is genuinely unavailable, which is
measured here rather than asserted.** AAP §0.5.1 offers exactly two witness kinds — a class exclusive
to the module, or failing that the module-exclusive `META-INF/maven/**/pom.properties` node — and no
third. For `connector/kafka-0-10-token-provider` and `connector/kafka-0-10` the second kind was checked
directly against all 191 staged artifacts:

| Module | Descriptor node | Archives containing it | Disqualifying holder |
| --- | --- | --- | --- |
| `connector/kafka-0-10-token-provider` | `META-INF/maven/org.apache.spark/spark-token-provider-kafka-0-10_2.13/pom.properties` | **6** | `connector/kafka-0-10-assembly`'s fat JAR — the other five are that module's own siblings, which §0.5.1 permits |
| `connector/kafka-0-10` | `META-INF/maven/org.apache.spark/spark-streaming-kafka-0-10_2.13/pom.properties` | **6** | the same assembly, on the same reading |

A node shared with a same-module sibling still qualifies; one shared with **another** module does not,
and the assembly is another module. So neither witness kind exists for either module across the
complete input set, the AAP supplies no third, and presence evidence explicitly labelled as presence
with the other module's artifact named is the strongest statement the input set admits. It is reported
as that and never as an injective witness.

### The seven modules with no coverage verdict obtainable, each named with what was tried

All seven are the same condition, and it is the input-set fact carried as **D3**: their artifacts were
not in the input the frontend was given, so no witness of either kind can be in the graph. This is
neither a coverage failure nor a pass, and none of the seven is folded into one or dropped from the
count. `cpg-verify.log` STEP 19 records each with its witness, its query and its reason:

| Module | Witness tried | Witness kind | Query run | Result |
| --- | --- | --- | --- | --- |
| `sql/connect/shims` | `META-INF/maven/org.apache.spark/spark-connect-shims_2.13/pom.properties` | module-exclusive `pom.properties` node — the weaker witness | `cpg.file.name.count(_.endsWith(node))`, leaf-contains 102 | ABSENT — 0 matching file nodes |
| `tools` | `org.apache.spark.tools.GenerateMIMAIgnore` | class exclusive to the module | `cpg.typeDecl.fullNameExact(...)` | ABSENT — 0 type declarations |
| `examples` | `org.apache.spark.examples.AccumulatorMetricsTest` | class exclusive to the module | `cpg.typeDecl.fullNameExact(...)` | ABSENT — 0 type declarations |
| `connector/kafka-0-10-token-provider` | `org.apache.spark.kafka010.ExceptionsHelper` | least-shared class — presence evidence | `cpg.typeDecl.fullNameExact(...)` | ABSENT — 0 type declarations |
| `connector/kafka-0-10` | `org.apache.spark.streaming.kafka010.Assign` | least-shared class — presence evidence | `cpg.typeDecl.fullNameExact(...)` | ABSENT — 0 type declarations |
| `connector/kafka-0-10-assembly` | `com.fasterxml.jackson.annotation.JacksonAnnotation` | class exclusive to the module | `cpg.typeDecl.fullNameExact(...)` | ABSENT — 0 type declarations |
| `connector/kafka-0-10-sql` | `org.apache.spark.sql.kafka010.AssignStrategy` | class exclusive to the module | `cpg.typeDecl.fullNameExact(...)` | ABSENT — 0 type declarations |

Six of the seven are exactly the six JAR producers the expected-values table does not name (section 3);
the seventh is `sql/connect/shims`, whose two archives were excluded from that graph's input by the
provisioning runbook's own instruction. **The weaker witness kind is not what failed:** the graph holds
102 `pom.properties` file nodes, 31 of them Spark module descriptors, so the node type is representable
and functional on this graph — the shims node is absent for one reason only, which is that the archive
carrying it was never in the input.

**That explanation is now measured rather than argued.** The subsection below runs each of these seven
witnesses against a graph built from these modules' own primary artifacts, and **all seven are
present** — including the shims `pom.properties` descriptor node, at 1 matching file node. So for every
one of the seven, absence from the graph at the sanctioned path is established to be a consequence of
that graph's input set and of nothing else. What that does **not** establish, and what remains open, is
a coverage verdict for the seven against a graph built over every JAR the build produced: no such graph
exists, and **D1** records why.

### The second verdict column — the per-module witness graph this run built

The seven verdicts above are unobtainable for one reason and one reason only: those modules'
artifacts were not in the input the frontend that wrote that graph was given. That is a fact about
an input set, not about the modules — and it leaves a different question open and answerable: does
this frontend, given a module's own primary artifact, put that module's witness into a graph at all?

This run measured that directly. It built a **per-module witness graph** over exactly one artifact
per JAR-producing module — that module's primary artifact — and ran the same witness queries against
it.

**What this column is, and the four things it is not.** Stated before any number, because a reader
who takes it for the first column would draw exactly the wrong conclusion.

- It **is** a frontend-capability measurement: whether the pinned frontend produces a module's own
  classes into a loadable graph from that module's own artifact.
- It is **not** the graph AAP §0.5.1 mandates, and it does not satisfy **D1**. The mandated
  complete-input graph remains unbuildable with this frontend, for the reason `cpg-frontend.log`
  STEP 8 proves.
- It is **not** at `harness/cpg/spark.cpg`, which is unchanged and still resolves to the
  provisioned graph.
- It is **not** loaded by any runner, and it contributes **no row** to `findings.json` or
  `findings.csv`.
- It does **not** replace or upgrade any verdict in the first column. The first column answers
  "did this module's code reach the graph the runners read?" and for seven modules the answer
  remains *not obtainable*.

**The input set, chosen by a rule rather than by which modules were missing.** Exactly one artifact
per JAR-producing module: its primary artifact, the main unclassified, non-`original-`, non-`-tests`
JAR. That is the minimal set in which every module's own witness can be sought, and it is the same
definition of *primary artifact* the coverage test itself uses.

| Property | Value |
| --- | --- |
| Input artifacts | **38** — one per JAR-producing module |
| Input bytes | 130,718,491 |
| Input class entries | 52,584, against 99,723 in the complete set |
| Frontend | pinned `jimple2cpg`, JDK major **21**, `-J-Xmx128g` proven committable, `--recurse`, no exclusions |
| Elapsed | 6,150 s (1 h 42 m 30 s) |
| Peak RSS | 74,748,328 kB (71.3 GiB) |
| Exit code | 0 |
| Graph bytes | 418,777,229 |
| Graph sha256 | `8d3462b78d3c4b009c994d1ae838b6266aa2af3e68b3c0fbdcbd3b3f630ad41d` |
| Loaded with | `importCpg` as a statement, in a workspace proved absent before use; **methods 994,192**, type declarations 97,292, files 45,680 |
| Evidence | `harness/artifacts/logs/cpg-frontend.log` PART 2 (build) and `harness/artifacts/logs/cpg-verify.log` PART 2 (load, identity re-verification, and every query below) |

**That this graph serialized while the complete-input graph could not is the diagnosis behaving as
diagnosed**, not a workaround. The bound is on the total UTF-8 size of the graph's distinct strings,
and this input carries 52,584 class entries against 99,723 in the complete
set — 53 % of them. Narrowing the input is exactly what AAP §0.9.2 prohibits for the mandated
graph, which is why this is reported as a capability measurement beside D1 rather than as a
resolution of it.

**One cost of the complete input set is visible here, and it is recorded rather than acted on.** In
this graph all eleven classes `sql/connect/shims` ships as client-only stubs carry stub-sized method
counts — `SparkConf` 8, `SparkContext` 2, `rdd.RDD` 8, `api.java.JavaRDD` 2, and 2 to 4 for the
other seven — where the same classes in the graph at the sanctioned path, whose input set **excluded**
the shims archives, carry a full implementation's worth: `SparkConf` 298, `SparkContext` 1,100,
`rdd.RDD` 1,022, `api.java.JavaRDD` 74. So in any graph containing the shims artifact, the stub
definitions displace the real ones for those eleven classes. This is what the graph contains,
measured by querying it — the route AAP §0.5.1 sanctions — and it is **not** a winner map: it says
what is there, not which archive the frontend read last. It corroborates the provisioning runbook's
own instruction to exclude that archive, and it is a real consequence of the complete input set the
AAP mandates. It is reported and nothing is excluded on the strength of it, because AAP §0.9.2
forbids the exclusion.

**The queries, one row per module, same test and same witnesses as the first column.** Exclusivity
was established against the **complete 191-artifact inventory**, not against this narrower set — the
stronger reading, and the one that keeps these witnesses comparable with the first column's.

| Module (JAR-producing) | Witness, as queried | Witness kind | Result in the witness graph |
| --- | --- | --- | --- |
| `common/kvstore` | `org.apache.spark.util.kvstore.ArrayWrappers` | class exclusive to the module | PRESENT — 1 type declaration, 2 methods |
| `common/network-common` | `org.apache.spark.network.TransportContext` | least-shared class -- presence evidence only | PRESENT — 1 type declaration, 19 methods |
| `common/network-shuffle` | `org.apache.spark.network.sasl.ShuffleSecretManager` | least-shared class -- presence evidence only | PRESENT — 1 type declaration, 7 methods |
| `common/network-yarn` | `org.apache.spark.network.yarn.YarnShuffleService` | class exclusive to the module | PRESENT — 1 type declaration, 18 methods |
| `common/sketch` | `org.apache.spark.util.sketch.BitArray` | class exclusive to the module | PRESENT — 1 type declaration, 14 methods |
| `common/tags` | `org.apache.spark.annotation.AlphaComponent` | least-shared class -- presence evidence only | PRESENT — 2 type declarations, 0 methods |
| `common/unsafe` | `org.apache.spark.sql.catalyst.expressions.HiveHasher` | class exclusive to the module | PRESENT — 1 type declaration, 6 methods |
| `common/utils` | `org.apache.spark.BreakingChangeInfo` | class exclusive to the module | PRESENT — 1 type declaration, 8 methods |
| `common/utils-java` | `org.apache.spark.QueryContext` | least-shared class -- presence evidence only | PRESENT — 1 type declaration, 8 methods |
| `common/variant` | `org.apache.spark.types.variant.ShreddingUtils` | class exclusive to the module | PRESENT — 1 type declaration, 4 methods |
| `connector/avro` | `org.apache.spark.sql.avro.AvroDataToCatalyst` | class exclusive to the module | PRESENT — 1 type declaration, 53 methods |
| `connector/kafka-0-10` ⚑ | `org.apache.spark.streaming.kafka010.Assign` | least-shared class -- presence evidence only | PRESENT — 2 type declarations, 50 methods |
| `connector/kafka-0-10-assembly` ⚑ | `com.fasterxml.jackson.annotation.JacksonAnnotation` | class exclusive to the module | PRESENT — 1 type declaration, 0 methods |
| `connector/kafka-0-10-sql` ⚑ | `org.apache.spark.sql.kafka010.AssignStrategy` | class exclusive to the module | PRESENT — 1 type declaration, 61 methods |
| `connector/kafka-0-10-token-provider` ⚑ | `org.apache.spark.kafka010.ExceptionsHelper` | least-shared class -- presence evidence only | PRESENT — 2 type declarations, 2 methods |
| `connector/protobuf` | `org.apache.spark.sql.protobuf.CatalystDataToProtobuf` | class exclusive to the module | PRESENT — 1 type declaration, 41 methods |
| `core` | `org.apache.spark.Aggregator` | class exclusive to the module | PRESENT — 1 type declaration, 27 methods |
| `examples` ⚑ | `org.apache.spark.examples.AccumulatorMetricsTest` | class exclusive to the module | PRESENT — 1 type declaration, 1 method |
| `graphx` | `org.apache.spark.graphx.Edge` | class exclusive to the module | PRESENT — 1 type declaration, 60 methods |
| `launcher` | `org.apache.spark.launcher.AbstractAppHandle` | class exclusive to the module | PRESENT — 1 type declaration, 15 methods |
| `mllib` | `org.apache.spark.ml.Estimator` | class exclusive to the module | PRESENT — 1 type declaration, 10 methods |
| `mllib-local` | `org.apache.spark.ml.impl.Utils` | class exclusive to the module | PRESENT — 1 type declaration, 6 methods |
| `repl` | `org.apache.spark.repl.Main` | class exclusive to the module | PRESENT — 1 type declaration, 12 methods |
| `resource-managers/kubernetes/core` | `org.apache.spark.deploy.k8s.Config` | class exclusive to the module | PRESENT — 1 type declaration, 127 methods |
| `resource-managers/yarn` | `org.apache.spark.deploy.yarn.AmIpFilter` | class exclusive to the module | PRESENT — 1 type declaration, 9 methods |
| `sql/api` | `org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction` | least-shared class -- presence evidence only | PRESENT — 2 type declarations, 2 methods |
| `sql/catalyst` | `org.apache.spark.sql.catalyst.AliasIdentifier` | class exclusive to the module | PRESENT — 1 type declaration, 20 methods |
| `sql/connect/client/jdbc` | `org.apache.spark.sql.connect.client.jdbc.NonRegisteringSparkConnectDriver` | class exclusive to the module | PRESENT — 1 type declaration, 8 methods |
| `sql/connect/client/jvm` | `org.apache.spark.sql.application.ConnectRepl` | class exclusive to the module | PRESENT — 1 type declaration, 1 method |
| `sql/connect/common` | `javax.annotation.Generated` | least-shared class -- presence evidence only | PRESENT — 2 type declarations, 6 methods |
| `sql/connect/server` | `com.google.apps.card.v1.Action` | class exclusive to the module | PRESENT — 1 type declaration, 63 methods |
| `sql/connect/shims` ⚑ | `META-INF/maven/org.apache.spark/spark-connect-shims_2.13/pom.properties` | module-exclusive pom.properties node -- the weaker witness | PRESENT — 1 matching file node |
|   ↳ same module | `org.apache.spark.SparkConf` | presence of a class the module ships -- provenance not measurable | PRESENT — 2 type declarations, 8 methods |
|   ↳ same module | `org.apache.spark.rdd.RDD` | presence of a class the module ships -- provenance not measurable | PRESENT — 2 type declarations, 8 methods |
|   ↳ same module | `org.apache.spark.sql.internal.SharedState` | presence of a class the module ships -- provenance not measurable | PRESENT — 2 type declarations, 2 methods |
| `sql/core` | `org.apache.parquet.filter2.predicate.SparkFilterApi` | class exclusive to the module | PRESENT — 1 type declaration, 7 methods |
| `sql/hive` | `org.apache.hadoop.hive.ql.exec.HiveFunctionRegistryUtils` | class exclusive to the module | PRESENT — 1 type declaration, 7 methods |
| `sql/hive-thriftserver` | `org.apache.hive.service.AbstractService` | class exclusive to the module | PRESENT — 1 type declaration, 13 methods |
| `sql/pipelines` | `org.apache.spark.sql.pipelines.AnalysisWarning` | class exclusive to the module | PRESENT — 1 type declaration, 0 methods |
| `streaming` | `org.apache.spark.status.api.v1.streaming.ApiStreamingApp` | class exclusive to the module | PRESENT — 1 type declaration, 8 methods |
| `tools` ⚑ | `org.apache.spark.tools.GenerateMIMAIgnore` | class exclusive to the module | PRESENT — 1 type declaration, 2 methods |

⚑ marks the seven modules that have no verdict obtainable from the graph at the sanctioned path.

**What the column establishes.**

| Outcome | Count |
| --- | --- |
| JAR-producing modules queried | **38** |
| Witness PRESENT in the witness graph | **38** |
| Witness ABSENT in the witness graph | **0** |
| Of the seven with no first-column verdict, witness PRESENT here | **7** of 7 |

**Every one of the 38 modules' witnesses is present in the witness graph**, including all seven
that have no first-column verdict. So for those seven the first column's *not obtainable* is
established to be an input-set consequence and nothing else: given the module's own artifact,
this frontend does produce that module's witness into a graph.

**The conclusion this column supports, stated no more strongly than the measurement allows.** For
each of the seven, their absence from the graph at the sanctioned path is explained by that graph's
input set, and is not evidence that the module's bytecode is unrepresentable or that the build did
not produce it — delivery is separately proved for all 38 by section 4's bidirectional manifest.
What remains unestablished, and is **not** closed by this column, is a coverage verdict for those
seven **against a graph built over every JAR the build produced**. That graph does not exist, D1
says why, and no measurement here substitutes for it.
### Cross-reference against the type declarations the verification load reports

`cpg-verify.log`'s `importCpg` loads — two loads, in two separate JVMs, both under JDK major 21 at a
64 GiB heap, both reporting identically — measured the graph as **1,397,339 methods, 119,691 type
declarations and 45,037 files**, with `methods > 0` explicitly confirmed. Each covered module's witness
above is a type declaration counted inside that 119,691, and each ABSENT witness is absent from it, so
the coverage verdict and the type-declaration count are one measurement read two ways rather than two
measurements. A second, independent axis agrees module for module with zero disagreements: the module's
own Maven descriptor node is present in the graph for **31 of the 38** JAR-producing modules — the same
31 — measured on a different node type with a different query. Every PRESENT row's module is in the
graph's input set and every ABSENT row's module is not, with 0 disagreements either way, which is itself
a check on both axes: a module in the input whose witness was missing would be a coverage failure, and a
witness found for a module outside the input would mean the witness was not exclusive after all.

The three counts are reported against their expected values by `cpg-verify.log`, and are cited here only
where they bear on this verdict: methods 1,397,339 against an anchor of 898,336 and a one-sided floor of
853,420, which the observation passes with no upper bound applying; type declarations 119,691 against
87,381; files 45,037 against 38,818. The last two never halt. The **cause** of the excess is recorded
there as **not established** rather than guessed — the AAP's stated rationale is the six extra JAR
producers, and those six are measured above as absent from this graph's input set, so that mechanism
cannot be it.

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
record never overrides it. Applied to this file's subject matter, that rule reaches **inherited** facts
— what the run observed about the provisioning as it found it — and not outputs this run deliberately
replaces. Two consequences, both material here:

- The **Maven identity** and the **JDK assignment** are inherited facts, and both agree with the table:
  required 3.9.11, detected 3.9.11, build JVM major 17.
- The **JAR inventory and the graph's counts are not** governed by it in the same way. This run's input
  set is deliberately wider than the provisioning's, so a difference there is the requirement being
  fulfilled rather than an environment contradiction; and the graph's method, type-declaration and file
  counts are compared against the **expected-values table** under its own rules, which
  `cpg-verify.log` does. Reading intentional replacement as a contradiction would stop the run for
  succeeding.

One environment-record statement is contradicted by observation and is carried as a recorded
difference by the documents that measured it, not resolved here: the record states that no Spark
artifact at this pin contains a `META-INF/maven/**/pom.properties` node, and both
`cpg-input-inventory.json` and `cpg-verify.log` measured the opposite — the `spark-connect-shims`
artifact contains one and it is module-exclusive, and the graph holds 102 such file nodes. Both values
are recorded; the statement is not a field of the expected-values table, so nothing stops on it.

**A second contradiction is of the halting kind, and is reported rather than repaired — this is D4.**
`harness/ENVIRONMENT.md` states the graph's identity explicitly:

| Source | Bytes | sha256 |
| --- | --- | --- |
| `harness/ENVIRONMENT.md:284-285`, the provisioned record in this clone | 541,255,894 | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` |
| The file on disk at this checkpoint, measured through the symlink | 541,309,809 | `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` |

Neither field is carried by the request's expected-values table, and observation contradicts the
record. AAP §0.1.3's fourth case applies exactly — the record states a field the table does not carry
and observation contradicts it — and §0.9.2 names both this and "a graph whose byte size or sha256
differs from the values recorded at write time at any later load" among the conditions that stop the
run. So both values are recorded and neither is chosen: there is no anchor to adjudicate between them,
and repairing it is not available in any case, because the file is host-global, shared with concurrent
readers, and was not written by this run.

The cause is established rather than guessed: provisioning re-ran against this host between the stages
of this run that loaded the graph and this checkpoint, and replaced the shared file. This is why the
run's earlier per-load identity records — each taken immediately before its load, as the plan requires,
and consistent with each other across every load — describe a file that is no longer at the path. It is
also, independently of D1, why a current-run graph was attempted: an inherited artifact that the
environment can replace underneath a run cannot anchor a reproducible dataset.

### Values named as not established

Named rather than omitted, because a value missing from the record is a value nothing downstream can
check (AAP §0.9.4):

- **Per-class provenance for every overwritten class** — not measurable from this frontend's output
  (section 5). No winner map is claimed.
- **A coverage verdict for the seven modules in section 6** — no verdict obtainable from this graph,
  each named with the witness tried and the query run.
- **An injective coverage witness for the eight modules named in section 6** — none exists against
  this complete input set; presence evidence is reported and labelled as presence.
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
  distinct destinations and **23** failures over 23 classes, measured over this run's own complete
  191-artifact input set, grouped by containing module and artifact, with the 377 nested-archive
  destinations accounted for individually, the provenance limitation stated and no winner map
  presented.
- **A reactor that failed and was then resolved project by project** — this did not occur: the reactor
  succeeded, all 40 projects have an outcome, all 38 JAR-packaging projects produced their artifact,
  and no diagnostic log was needed or written.
- **The graph's three counts against their expected values**, and the input-set difference D3 that
  bounds what they describe.

---

## Self-check against this file's validation contract

1. **Every figure names a producer log, and that path is one of the six listed above.** PASS — sections
   1 and 2 cite `maven-preflight.log` and `build-reactor.log` by step; section 3 cites
   `build-reactor.log` STEPS 11 to 14 and `cpg-input-inventory.json` for the per-project artifact
   counts; section 4 cites `cpg-input-inventory.json` and `cpg-frontend.log`; section 5 cites
   `cpg-frontend.log` STEPS 5 to 11 and `cpg-verify.log` STEP 18; section 6 cites
   `cpg-input-inventory.json`, `cpg-verify.log` STEPS 14 to 19 for the first verdict column, and
   `cpg-frontend.log` PART 2 with `cpg-verify.log` PART 2 for the second. The absence of any
   `build-<module-path>.log` is itself recorded.
2. **All 40 projects appear exactly once; the two `pom`-packaging projects are marked expected; all 38
   JAR producers are accounted for.** PASS — the section 3 table has 40 numbered rows, the root parent
   and `assembly` are marked *produced none — EXPECTED, `packaging=pom`*, and the remaining 38 each
   carry their own main artifact with its path.
3. **Every JAR-producing module has a coverage verdict carrying either a unique class or a named
   `pom.properties` witness; no verdict rests on a package prefix.** PASS on the witness requirement,
   with the graph outcome reported honestly: all 38 rows carry a named witness — 29 a class exclusive to
   the module, 1 the module-exclusive `pom.properties` node, 8 a named least-shared class explicitly
   labelled presence evidence with the other module's artifact named — and 0 rows rest on a package
   prefix. Of those witnesses, 31 are present in the graph at the sanctioned path and 7 are absent
   because their modules' artifacts were not in that graph's input set; those 7 are named individually
   with the witness tried and the query run, and are neither folded into a pass nor dropped. Because
   that absence is an input-set fact rather than a property of the modules, section 6 additionally
   carries a **second, separately labelled verdict column** measured in a per-module witness graph this
   run built over one primary artifact per JAR-producing module — a frontend-capability measurement,
   carrying four explicit disclaimers — not the mandated graph, not at the sanctioned path, loaded by no runner, and no
   substitute for the first column.
4. **The staging manifest is described as total and injective in both directions and as logged before
   the frontend ran.** PASS — section 4, with 191 entries mapped in each direction, 0 unmapped, 0
   violations, all 191 digests re-verified, `computed_before_the_frontend_ran = true`, and the
   multiset argument for why a count plus a hash set would not do.
5. **No winner map is claimed anywhere; the provenance limitation is stated.** PASS — section 5 states
   the limitation in terms and labels the groupings as containment at every use, and section 6's second
   column repeats it for the witness graph; the one collision that bears on a conclusion is settled by
   querying the graph rather than by inferring a winner.
6. **No sentence compares one tool against another or judges any finding.** PASS — no scanner's output
   and no finding of any kind appears anywhere in this file; the only tools named are the Maven wrapper,
   the Joern bytecode frontend and the `importCpg` load, each as a step in the build-and-graph pipeline
   rather than as a subject of comparison.
7. **Markdown renders cleanly; tables are well-formed; no placeholder text and no invented numbers.**
   PASS — the two large tables were generated directly from the producer logs rather than transcribed,
   every other figure carries its citation, and each value that could not be established is named as
   such in section 7.
8. **The graph's status is stated before any graph number, and the attempt to satisfy it is recorded
   with its evidence rather than deferred.** PASS — the STATUS block carries D1, D3 and D4 with the
   8 h 01 m invocation, the 128 GiB commit proof, the bytecode-level diagnosis of the serialization
   bound, the rejected partial write with its size and digest, and the six mitigations examined. No
   number in sections 5 or 6 is presented as a current-run graph measurement, and section 7 names the
   current-run counts as not established.
