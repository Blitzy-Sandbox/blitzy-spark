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
| `harness/artifacts/logs/cpg-frontend.log` | the frontend invocation, the graph's identity pair, and the observed overwrite and AST-creation-failure metrics (sections 4 and 5) |
| `harness/artifacts/logs/cpg-verify.log` | the `importCpg` verification counts and the per-module coverage witnesses as measured in the graph (section 6) |
| `harness/cpg/spark.cpg` | the graph itself, at the path the AAP names — a provisioned symlink whose resolved target carries the recorded bytes |

No `harness/artifacts/logs/build-<module-path>.log` exists, and none is cited: section 3 records why
none was needed.

## STATUS — read this before any graph number below

Three facts bound what the numbers in this file describe, and reading a coverage figure without them
would misread it.

- **The full-reactor build was performed by this run.** `build-reactor.log` records it end to end:
  `BUILD SUCCESS`, Maven exit code 0, 40 of 40 reactor projects `SUCCESS`, and all 38 JAR-packaging
  projects confirmed on disk to have produced their own main artifact.
- **The JAR inventory and its staging manifest were produced by this run.** `cpg-input-inventory.json`
  inventories 191 own artifacts from those 40 projects, stages them into one fresh directory, and
  proves the mapping total and injective in both directions before any frontend invocation.
- **The graph was NOT created by this run, and its input set is narrower than the build.**
  `cpg-frontend.log` carries this as two entries and owns both: **D1**, a halt-class finding — the
  graph at the resolved path was written at 13:52:22Z by the provisioning invocation, before this
  run's first command, and AAP §0.1.1's requirement that the graph be created by this run is
  therefore not satisfied; and **D2**, a recorded difference — the input that graph was built from
  held 62 archives, 285,122,375 bytes, from 31 modules, against this run's 191 own artifacts,
  431,184,900 bytes, from all 38 JAR-packaging projects. Both are reported and not repaired: the
  resolved path `/opt/blitzy-harness/cpg/spark.cpg` is host-global and shared with concurrent clones
  that read it while they scan, and writing a graph anywhere else would leave the name the Stage 3
  Joern runner actually loads pointing at the provisioning bytes.

**The consequence lands on section 6 and is stated there rather than smoothed over.** Because the
graph's input set is the narrower one, seven of the 38 JAR-producing modules have no coverage verdict
obtainable from this graph, and they are named individually with the witness tried and the query run.
This file neither repairs that nor re-derives it: D1 and D2 belong to `cpg-frontend.log`, and
`cpg-verify.log` measured the coverage consequence.

---

## 1. The Maven pre-check verdict — no download occurred, and the branch that would have caused one was unreachable

Every value in this section is `harness/artifacts/logs/maven-preflight.log`'s measurement.

| What was established | Value | Where in the log |
| --- | --- | --- |
| `<maven.version>` required by the pinned root pom, extracted with the wrapper's own pipeline | `3.9.11` — `pom.xml:123` | STEP 6 |
| Tree the pom was read from | the pinned clone, `git rev-parse HEAD` = `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` | STEP 3 |
| Wrapper's early-return candidate `build/apache-maven-3.9.11/bin/mvn` | **does not exist** — `test -f` exited 1; and `ls -d build/apache-maven*` found no distribution of **any** version | STEP 9 |
| Resolvable `mvn` | `/usr/local/bin/mvn` | STEP 10 |
| Detected version, by the wrapper's own extraction — the third whitespace-separated field of the first line of `mvn --version`, taken with the wrapper's own two-stage pipeline | `3.9.11` — the full banner reads `Apache Maven 3.9.11 (3e54c93a704957b63ee3494413a2b544fd3d825b)`, Maven home `/opt/blitzy-tools/apache-maven-3.9.11` | STEP 11 |
| Both version tokens validated non-empty and well-formed before use | yes | STEPS 8 and 12 |
| Normalized comparison, with `version()` defined exactly as the wrapper defines it | required `003009011`, detected `003009011`; `[ 003009011 -ne 003009011 ]` exited **1**, so `DOWNLOAD_BRANCH_TAKEN=no` | STEP 13 |
| JDK the build would run under | `java.specification.version` = **17**, `/opt/blitzy-tools/jdk/jdk-17.0.20+8`, Temurin-17.0.20+8, and Maven reports the same runtime | STEP 14 |
| Verdict | **PASS — no download would be triggered** | VERDICT |

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

For completeness, `maven-preflight.log` STEP 7 also read the working checkout's pom once as a labelled
contrast — `3.9.12` at the branch tip against `3.9.11` at the pin — and used it for nothing. The
working checkout is neither built nor scanned (AAP §0.3.2).

---

## 2. The build command, and why five flags add four modules

**The invocation, quoted from the script that ran it** (`build-reactor.log` STEP 10), with
`MAVEN_OPTS="-Xss64m -Xmx6g -Xms2g -XX:ReservedCodeCacheSize=512m"`:

```bash
./build/mvn --no-transfer-progress -DskipTests \
  -Dmaven.repo.local="/tmp/blitzy-harness-scratch/17/build-reactor/m2-repo" \
  -Pyarn -Pkubernetes -Phive -Phive-thriftserver -Pvolcano package
```

**The reactor was not narrowed.** `build-reactor.log` STEP 10 greps the script for `-pl` and reports
`0` (`MODULE_SELECTOR_PRESENT=no`); there is no `-am` either, and no module selector of any kind. The
same step lists the profile flags it found — `-Phive`, `-Phive-thriftserver`, `-Pkubernetes`,
`-Pvolcano`, `-Pyarn` — which is exactly the five mandated flags and nothing else. `-DskipTests` is
present and no test goal appears: **no Spark test suite was executed, in any language.**

Two additions to the mandated form are recorded rather than left to be noticed.
`--no-transfer-progress` suppresses per-artifact transfer chatter and changes no goal, profile or
module. `-Dmaven.repo.local=…` points the build at a private byte-exact copy of the primed local
repository, because a full reactor resolves coordinates the narrowed provisioning build never needed
and building against the shared `/root/.m2/repository` would have written into a path concurrent
clones read; `build-reactor.log` STEP 9 and STEP 15 record the shared repository unchanged at 867 jars
and 5,832 files before and after.

**Where the build ran.** In a private clone of the pinned commit at
`/tmp/blitzy-harness-scratch/17/build-reactor/spark-src`, checked out **by SHA** and proved equal to
the shared pinned clone: `git rev-parse HEAD` equals the pin, and the sha256 of the sha256sums of every
tracked file matches `/opt/spark-src`'s, reported as `BUILD_TREE_MATCHES_SPARK_SRC=yes`
(`build-reactor.log` STEP 4). The shared clone was left untouched because it is read concurrently and
carries only a **narrowed** build — STEP 3 records 31 build directories there, with `tools`,
`examples`, `assembly` and all four `connector/kafka-0-10*` build directories absent, which is why a
full-reactor question cannot be answered from it. Every artifact path in this file is therefore
relative to that build tree, not to `SPARK_SRC`.

**Runtime versions actually used** (`build-reactor.log` STEP 7 and STEP 8, and cited from
`maven-preflight.log` for Maven): JVM **major 17** — `BUILD_JVM_MAJOR_IS_17=yes`, Temurin-17.0.20+8 at
`/opt/blitzy-tools/jdk/jdk-17.0.20+8`, read as the machine value
`java.specification.version` rather than parsed from a banner — and Maven **3.9.11** from
`/usr/local/bin/mvn`, the same binary the pre-check cleared.

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
| Wall clock | started 2026-08-24T18:52:00.024Z, ended 19:14:41.583Z, elapsed **1361 s**; Maven's own `Total time: 22:39 min` and `Finished at: 2026-08-24T19:14:41Z` | STEP 11 |
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
(section 4). The "primary artifact" column is each project's main, unclassified, non-`original-`,
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
| `common/network-yarn` unattached shaded shuffle JAR | `common/network-yarn/target/scala-2.13/spark-4.1.0-SNAPSHOT-yarn-shuffle.jar`, 109,208,027 bytes, sha256 `976b112a41653fb53c317827b094531a4542c107f5aaece3d86900dabe08600e` | declared at `common/network-yarn/pom.xml:97` with `:96` and `:37`; **asserted present**; classified as the project's own **by its declared output path**, because its filename carries no artifactId and a filename rule alone would have excluded it | `common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` |

### The totals, and the exclusions counted rather than silent

| Measurement | Value |
| --- | --- |
| JAR files enumerated under the 40 projects' build directories | **627** |
| Classified as a project's own | **191**, totalling **431,184,900 bytes** |
| Of those, carrying bytecode | 110 — recorded per file as a class-entry count, never used as a reason to drop one |
| Class entries across the own artifacts | 99,723 |
| **Excluded copied dependency JARs** | **422** (`copied_runtime_dependency`) |
| Also excluded: test-resource fixtures inside a module's compiled-output tree | **14** (`test_resource_fixture`) |
| Total excluded | **436** |
| Undecided provenance | **0** |
| Arithmetic | own 191 + excluded 436 + undecided 0 = 627 enumerated |

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
| Staging directory | `harness/artifacts/cpg-input` (in the checkout of the clone that measured the inventory) |
| **Absent before use** | **yes** — `ls -d` on it exited 2 with *"No such file or directory"* at 19:58:41.394Z, and the parent listing at 19:58:41.398Z shows only `logs` and `raw`; the directory was created at 19:58:41.398Z |
| Never cleared, never reused | a pre-existing staging tree is a halt (AAP §0.9.2): a stale archive left in it would enter the graph silently, and a graph's silence about a module is indistinguishable from a clean result |
| Staged-name form | `<module-path-with-slashes-as-underscores>__<original-filename>`, with the reactor root project's slug the literal `root` since it has no module path |
| Staging method | file copy, chosen over a hard link so the staged bytes are independent of the build tree and can be re-hashed as an independent measurement |
| Ordering | by module path, then by the artifact's path relative to the build tree — recorded so the input set is reproducible byte for byte |
| Staged files / bytes | **191** / **431,184,900** |

**The assertion, and why it is not a count plus a hash set.** The mapping asserted is: every inventory
entry maps to exactly one staged name and one sha256, and every file found in the staging directory
maps back to exactly one inventory entry — **total and injective in both directions**. Result `true`,
computed at 19:58:42.533Z, with `computed_before_the_frontend_ran = true`. Inventory → staged: 191
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
it is a statement about the input set, not about the graph that exists: per **D2** the frontend that
wrote that graph was given 62 archives from 31 modules, so these 191 archives were not its input, and
no claim that they reached the graph is made here or in section 6. Coverage — whether a module's own
code reached the graph — is a separate question with a separate proof, and it is section 6's.

---

## 5. Determinism: what is reproducible, and what is not measurable

### The graph these metrics belong to, identified exactly

`harness/cpg/spark.cpg` — the path the AAP names — is a 33-byte **symlink** to
`/opt/blitzy-harness/cpg/spark.cpg`, one hop with no intermediate indirection. Measured with
symlink-following semantics by `cpg-frontend.log`, the graph is **541,255,894 bytes**, sha256
**`26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`**, created 13:52:00.598Z and last
written 13:52:22.145Z. Both names resolve to one file, proved by an equal `dev:inode` of
`1048752:37891488` through the AAP's name and through the name the environment exports — which is
stronger than equal size or equal digest, so the mismatch between the two names that would stop the run
cannot arise while the link stands. The size was taken through the link deliberately: the 33-byte
no-follow reading is the length of the target path string, and a record carrying 33 would describe
nothing at all. `cpg-verify.log` re-measured all three fields after its loads and found them unchanged.

Per **D1**, that file was not written by this run — the pre-existing path held a symlink with a valid
target, and this run created, replaced and wrote through nothing.

### The two observed metrics

Both metrics below are `harness/artifacts/logs/cpg-frontend.log`'s recount from the frontend's own
preserved output stream, and both are **observed facts of that run rather than pre-approved
expectations**. Neither is treated as acceptable because a document expected some other number.

The stream partitions into exactly two message kinds, which is established before any grouping:
**31,598** `WARN ProgramHandlingUtil$` lines and **173** `WARN AstCreationPass` lines, with a
counter-grep confirming nothing else is in the stream at all.

| Observed metric | Value |
| --- | --- |
| Duplicate-class overwrite warnings | **31,598** lines |
| Distinct destination class files overwritten | **26,221** |
| AST-creation failures | **173** lines over **173** distinct classes |
| The exception behind every one of them | `java.lang.RuntimeException: Chain already contains object: <fqcn>` — one failure class, not an assortment |

**Overwrites grouped by the module and artifact the affected classes are contained in.** Of the 26,221
distinct destinations, **16,164** are contained in exactly one module's artifacts — and 16,150 of those
in more than one artifact *of that same module*, which is the shaded artifact and its `original-`
pre-shade sibling duplicating each other. The remaining **10,056** are contained in more than one
module's artifacts, which is cross-module vendoring: `org/sparkproject/io` netty classes in both
`common/network-yarn` and `sql/connect/client/jvm`, and `org/sparkproject/guava` and
`org/sparkproject/connect` likewise. By destination package the largest groups are
`org/apache/spark/**` at 19,301 distinct destinations over 24,525 warnings, then
`org/sparkproject/io/**` 2,593, `org/sparkproject/connect/**` 2,017, `org/sparkproject/guava/**`
2,017, `META-INF/**` 116 over 252, and `org/apache/hive/**` 126. Exactly one destination —
`module-info.class`, overwritten 4 times — matches no entry name in any staged archive, explained by
five `META-INF/versions/9/module-info.class` and `META-INF/versions/11/module-info.class` entries
across three modules' archives landing on one destination whose name matches none of them.

**AST-creation failures grouped the same way.** 104 of the 173 affected classes are contained in
`common/network-yarn`'s shuffle JAR and `sql/connect/client/jvm`'s shaded artifact — all of them
`org/sparkproject/io` netty-vendored classes — and the remaining 69 in `sql/core`'s own shaded and
pre-shade pair, all `org.apache.spark.sql.*`. Total accounted for: 173.

Both figures differ from the provisioning runbook's expectations of roughly 5,700 overwrite warnings
and roughly 36 protobuf-generated AST failures; both differences are recorded with both values, and
none of the 173 failures is protobuf-generated.

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
those definitions is not claimed.** It does not need to be: `cpg-frontend.log`'s own containment table
measures that in this graph's input each of the three shims-shared classes was overwritten exactly once
and is contained in exactly two archives, both of them module `core`'s shaded and pre-shade pair —
because the two `spark-connect-shims` archives were excluded from that input before the frontend ran.
So no three-way collision with the shims stubs occurred at all, and the absence of a shims coverage
verdict in section 6 is an input-set fact rather than a graph defect.
`org.apache.spark.unused.UnusedStubClass` is included for scale: it comes from Spark's
`org.spark-project.spark:unused` stub dependency and is shipped by 35 modules, which is also why
several modules' shared-class counts read as 1 against nearly every other module.

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
it: per **D2** the frontend that wrote this graph was given 62 archives from 31 modules. That
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

### The seven modules with no coverage verdict obtainable, each named with what was tried

All seven are the same condition, and it is the input-set fact carried as **D2**: their artifacts were
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
- **The graph as this run's own output** — not satisfied, carried by `cpg-frontend.log` as its
  halt-class finding D1 and reported rather than repaired.

### Recorded without stopping the run

Each of these is a recorded difference under AAP §0.9.3, with both values on the record:

- **The six JAR producers the expected-values table does not name** — `tools`, `examples`,
  `connector/kafka-0-10`, `connector/kafka-0-10-sql`, `connector/kafka-0-10-assembly` and
  `connector/kafka-0-10-token-provider` — all six `SUCCESS` and all six with their own artifact on
  disk. The rule is one-directional: a module that produced a JAR in the rehearsal and produces none
  now stops the run; the reverse does not.
- **The copied-dependency exclusion count** — 422 copied runtime dependencies excluded from the graph
  input set, plus 14 test-resource fixtures, out of 627 JARs enumerated.
- **The observed overwrite and AST-creation-failure counts** — 31,598 warnings over 26,221 distinct
  destinations and 173 failures over 173 classes, grouped by containing module and artifact, with the
  provenance limitation stated and no winner map presented.
- **A reactor that failed and was then resolved project by project** — this did not occur: the reactor
  succeeded, all 40 projects have an outcome, all 38 JAR-packaging projects produced their artifact,
  and no diagnostic log was needed or written.
- **The graph's three counts against their expected values**, and the input-set difference D2 that
  bounds what they describe.

---

## Self-check against this file's validation contract

1. **Every figure names a producer log, and that path is one of the six listed above.** PASS — sections
   1 and 2 cite `maven-preflight.log` and `build-reactor.log` by step; section 3 cites
   `build-reactor.log` STEPS 11 to 14 and `cpg-input-inventory.json` for the per-project artifact
   counts; section 4 cites `cpg-input-inventory.json` and `cpg-frontend.log`; section 5 cites
   `cpg-frontend.log` and `cpg-verify.log` STEP 18; section 6 cites `cpg-input-inventory.json` and
   `cpg-verify.log` STEPS 14 to 19. The absence of any `build-<module-path>.log` is itself recorded.
2. **All 40 projects appear exactly once; the two `pom`-packaging projects are marked expected; all 38
   JAR producers are accounted for.** PASS — the section 3 table has 40 numbered rows, the root parent
   and `assembly` are marked *produced none — EXPECTED, `packaging=pom`*, and the remaining 38 each
   carry their own main artifact with its path.
3. **Every JAR-producing module has a coverage verdict carrying either a unique class or a named
   `pom.properties` witness; no verdict rests on a package prefix.** PASS on the witness requirement,
   with the graph outcome reported honestly: all 38 rows carry a named witness — 29 a class exclusive to
   the module, 1 the module-exclusive `pom.properties` node, 8 a named least-shared class explicitly
   labelled presence evidence with the other module's artifact named — and 0 rows rest on a package
   prefix. Of those witnesses, 31 are present in the graph and 7 are absent because their modules'
   artifacts were not in the graph's input set; those 7 are named individually with the witness tried
   and the query run, and are neither folded into a pass nor dropped.
4. **The staging manifest is described as total and injective in both directions and as logged before
   the frontend ran.** PASS — section 4, with 191 entries mapped in each direction, 0 unmapped, 0
   violations, all 191 digests re-verified, `computed_before_the_frontend_ran = true`, and the
   multiset argument for why a count plus a hash set would not do.
5. **No winner map is claimed anywhere; the provenance limitation is stated.** PASS — section 5 states
   the limitation in terms and labels the groupings as containment at every use; the one collision that
   bears on a conclusion is settled by querying the graph.
6. **No sentence compares one tool against another or judges any finding.** PASS — no scanner's output
   and no finding of any kind appears anywhere in this file; the only tools named are the Maven wrapper,
   the Joern bytecode frontend and the `importCpg` load, each as a step in the build-and-graph pipeline
   rather than as a subject of comparison.
7. **Markdown renders cleanly; tables are well-formed; no placeholder text and no invented numbers.**
   PASS — the two large tables were generated directly from the producer logs rather than transcribed,
   every other figure carries its citation, and each value that could not be established is named as
   such in section 7.
