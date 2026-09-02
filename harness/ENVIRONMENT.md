# Environment record — OSS security scanner harness for Apache Spark

Written by the provisioning run on **2026-08-24**. This is the record the scanning
run reads before it does anything else.

**Source this file first, in a fresh non-login shell:**

```bash
. harness/env.sh
```

`harness/env.sh` is the environment file. Nothing else needs sourcing, and every
variable it exports uses `${VAR:-default}` so a caller can override any of them
without editing it.

**Nothing here is installed by the scanning run.** Provisioning installed the
toolchain, the nine scanners, the pinned rulesets and feeds, the pinned Spark clone
and the code-property graph. The scanning run verifies them and stops if one is
missing.

Every number below traces to a file under `/opt/blitzy-harness/provision-log/`,
named at the end of each section. Where a value could not be established it is
named as such rather than omitted.

---

## 1. Host

| | Observed |
|---|---|
| OS | **Ubuntu 25.10**, x86-64 (the runbook targets 22.04 — recorded divergence, nothing failed because of it) |
| CPU | 4 vCPU |
| RAM | `MemTotal` 4,029,526,772 kB ≈ **3.75 TiB** |
| Disk | 24 TB free on `/` and `/tmp` |
| Heap commit proof | `java -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version` exits **0** on both JDKs; `-Xms96g -Xmx96g` also exits 0 |
| Minimum memory this pipeline actually needed | **64 GB heap; peak sampled RSS 59.0 GB** during the graph build (2026-08-24; **66.6 GB** for the graph on disk — appendix S.3). 48 GB would not have held it |
| Locale | image carries only `C`/`C.utf8`/`POSIX` with `LANG` unset, so `env.sh` exports `LANG=LC_ALL=C.utf8` and `PYTHONUTF8=1` (without this Opengrep aborts at 3 s on a `UnicodeDecodeError` reading its own rule files) |

Image gaps worth knowing: **`/usr/bin/time`, `unzip` and `shellcheck` are absent**;
`python3 -m venv` is broken (`ensurepip` exit 1) so virtualenvs were made with
`virtualenv 21.7.4`; `tail -2` is rejected by this coreutils (use `tail -n 2`); the
Adoptium API rejects `urllib` with HTTP 403 but accepts `curl`.

---

## 2. Toolchain — requested vs installed

Every pin was honoured exactly. No substitutions.

| Component | Requested | Installed | Location |
|---|---|---|---|
| JDK (build + eight scanners) | Temurin 17.0.20+8 | **`openjdk 17.0.20 2026-07-21`, Temurin-17.0.20+8** | `/opt/blitzy-tools/jdk/jdk-17.0.20+8` = `$JAVA_HOME`, and the default `java` via `update-alternatives` priority 1700 |
| JDK (Joern only) | Temurin 21.0.12.1+1 | **`openjdk 21.0.12.1 2026-08-18 LTS`** | `/opt/blitzy-tools/jdk/jdk-21.0.12.1+1` = `$JAVA_HOME_21` |
| Maven | 3.9.11 | **`Apache Maven 3.9.11 (3e54c93a704957b63ee3494413a2b544fd3d825b)`** | `/opt/blitzy-tools/apache-maven-3.9.11`, also `/usr/local/bin/mvn` |
| Scala | 2.13.17 | **`Scala code runner version 2.13.17`** | `/opt/blitzy-tools/scala-2.13.17` (from the `scala/scala` GitHub release — `downloads.lightbend.com` returns 403) |
| Python | 3.13.7 | **`Python 3.13.7`** | system interpreter, plus one virtualenv per Python scanner |
| git | ≥ 2.51 | **2.51.0** | system |

JDK archives were sha256-verified: 17 = `be7668bc030d578b83d6d5ef9221d6d6729bbbca8cf94a7d52e16ac68b5a5a35`,
21 = `ce79869e1307ed8ee1e2baa86a412b1eb5b75d10a01006d788a6f968bcfaee94`. Maven was
verified against its official sha512; Scala against sha256
`ada6b8deb341875838cced8d32070c63f96f77a833033f4ca5e30fe2ee6a171b`.

Evidence: `provision-log/toolchain-versions.txt`, `provision-log/test1-toolchain.log`.

---

## 3. The nine scanners — requested vs installed

All nine at the exact pin. Every binary is on `PATH` via `/opt/blitzy-tools/bin`.

| Tool | Requested | Installed (as reported by the tool) | Source |
|---|---|---|---|
| Opengrep | 1.27.1 | `1.27.1` | `opengrep/opengrep` release `v1.27.1`, `opengrep_manylinux_x86` |
| Semgrep CE | 1.173.0 | `1.173.0` | `pip semgrep==1.173.0` in **its own venv** `/opt/blitzy-tools/venvs/semgrep` (Python 3.13.7) |
| Joern | 4.0.607 | `Version: 4.0.607` (read from the banner with stdin closed — Joern has no `--version`) | `joernio/joern` release `v4.0.607`, `joern-cli-linux-x86_64.zip`, sha512 verified |
| datadog-static-analyzer | 0.9.1 | `Version: 0.9.1, revision: f76636e43554f7f9a8e3984a31d03ec8dea5489f` | `DataDog/datadog-static-analyzer` tag **`0.9.1`** (no leading `v`; `tags/v0.9.1` is a 404) |
| Gitleaks | 8.30.1 | `8.30.1` | `gitleaks/gitleaks` release `v8.30.1` |
| Checkov | 3.3.12 | `3.3.12` | `pip checkov==3.3.12` in **its own venv** `/opt/blitzy-tools/venvs/checkov` (Python 3.13.7) |
| Trivy | 0.74.0 | `Version: 0.74.0` | `aquasecurity/trivy` release `v0.74.0` |
| OSV-Scanner | 2.5.1 | `osv-scanner version: 2.5.1` (osv-scalibr 0.5.2) | `google/osv-scanner` release `v2.5.1` |
| OWASP Dependency-Check | 13.0.0 | `dependency-check-cli version 13.0.0` | **`dependency-check/DependencyCheck`** release `v13.0.0`, sha256 `44d920d1ec03e948df862a253f0912782a31b9beee8a7c8895b9cb95760176ed` |

**Two things a re-provisioning will trip over.**
`jeremylong/DependencyCheck` returns **404** for tag `v13.0.0` — the repository moved
to `dependency-check/DependencyCheck`. And the joern-cli zip extracts **without
executable bits** when unpacked with Python's `zipfile` (the image has no `unzip`),
which surfaces as `bin/repl-bridge: Permission denied`; provisioning ran
`chmod +x` over `joern-cli/bin/*` and `joern-cli/frontends/*/bin/*`.

**Joern is wrapped to pin JDK 21.** Joern's own launchers use whatever `java` is on
`PATH`, which here is 17. `/opt/blitzy-tools/bin/{joern,jimple2cpg,joern-scan,joern-export,joern-parse,joern-flow}`
are wrappers that export `JAVA_HOME=${JAVA_HOME_21:-/opt/blitzy-tools/jdk/jdk-21.0.12.1+1}`
before delegating. Call the wrappers, not `$JOERN_HOME/joern-cli/*` directly.

**CodeQL is excluded deliberately: it does not support Scala at all** — Scala is
absent from GitHub's supported-languages list for CodeQL
(<https://codeql.github.com/docs/codeql-overview/supported-languages-and-frameworks/>).
Its absence is a decision, not an oversight.

Evidence: `provision-log/tool-versions.txt`, `provision-log/test1-toolchain.log`.

---

## 4. Rulesets and vulnerability feeds

| Source | Pin | Count | Where |
|---|---|---|---|
| Opengrep rules | commit **`f1d2b562b414783763fd02a6ed2736eaed622efa`** | **2,006 Code rules** over 58 `--config` directories | `$OPENGREP_RULES_DIR` = `/opt/blitzy-harness/rules/opengrep-rules` |
| Semgrep rules | commit **`40b8c63f75dc7c22c8a77482d73bfb864b146f7e`** | **2,149 Code rules, 19 Pro-only skipped** over 60 `--config` directories | `$SEMGREP_RULES_DIR` = `/opt/blitzy-harness/rules/semgrep-rules` |
| datadog SAST ruleset | sha256 **`4f397e81414f8e9469d20abc18c80c85c722e72b9f85b8bcf69dbe34b8fef6f1`** | **48 rulesets / 1,093 rules** | `$DD_SAST_RULES_FILE` = `/opt/blitzy-harness/rules/datadog/datadog-sast-rules.json` |
| Trivy vulnerability DB | v2, `UpdatedAt` **2026-08-24T06:55:32.451220873Z** | 108.98 MiB | `$TRIVY_CACHE_DIR` = `/opt/blitzy-harness/trivy-cache` |
| Trivy java DB | v1, `UpdatedAt` **2026-08-24T01:07:04.599776272Z** | 910.94 MiB | same cache |
| Dependency-Check NVD | keyless NIST JSON 2.0 datafeed, **NVD API Last Modified 2026-08-24T08:00:04-04** | 239 MB `odc.mv.db` | `$HARNESS_DC_DATA_DIR` = `/opt/blitzy-harness/dc-data` |
| OSV-Scanner | **no local database** — queries `https://api.osv.dev` live at scan time | — | — |
| Gitleaks | default rule set built into 8.30.1, not separately versioned | — | — |
| Checkov | policies bundled with 3.3.12, not separately versioned | — | — |

**The datadog ruleset is captured locally, which closes the largest reproducibility
gap in the dataset.** Left alone this tool fetches ~1,093 rules from its API
mid-scan with no recorded digest. Provisioning read the pinned source at tag `0.9.1`
(`crates/bins/src/bin/datadog-export-rulesets.rs`,
`crates/cli/src/model/datadog_api.rs`,
`crates/static-analysis-kernel/src/model/{rule,ruleset}.rs`) for the serde shape,
enumerated the default rulesets for all 12 languages from
`/api/v2/static-analysis/default-rulesets/<LANG>`, fetched each from
`/api/v2/static-analysis/rulesets/<name>?include_tests=false&include_testing_rules=true`,
and wrote one local config. It is **proven offline**: with `-r $DD_SAST_RULES_FILE`
the tool reports `#static analysis rules : 1093` and makes no API call.

**Recorded digest divergence.** The scanning run's plan expects
`e70ede308813b6d8c4087b0995609cdafdb9ab48159a313fe58ac343ff6c44f7` for this file.
That was a differently-serialized capture of the same content; a byte digest cannot
survive a different serializer. The comparable measures all match exactly: **48
rulesets, 1,093 rules, anonymous source, analyzer revision `f76636e4`**. Compare on
those, not on the digest.

Two feed facts a re-provisioning needs: Dependency-Check's `--nvdDatafeed` default
pattern is `nvdcve-{0}.json.gz` while NVD publishes `nvdcve-2.0-{0}.json.gz`, so the
explicit pattern
`https://nvd.nist.gov/feeds/json/cve/2.0/nvdcve-2.0-{0}.json.gz` is required; and a
transient 404 on the trailing `modified` feed leaves `scanInfo.dataSource` **empty**
while still exiting 0, so seed with `--nvdMaxRetryCount 10 --nvdApiDelay 2000` and
verify the timestamps afterwards. The successful seed took 896 s.

Evidence: `provision-log/rulesets-feeds.txt`,
`opengrep-ruleset-validate.log`, `semgrep-ruleset-validate.log`,
`dc-nvd-seed.log`, `dc-nvd-seed2.log`, and the `.meta.json` sidecar beside the
datadog ruleset.

---

## 5. The pinned Spark tree

| | |
|---|---|
| `SPARK_SRC` | **`/opt/spark-src`** — outside the working checkout, which is never built and never scanned |
| `SPARK_SRC_COMMIT` | **`59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`**, verified with `git rev-parse HEAD` |
| Commit date | `2025-10-23T15:31:06-04:00` = **2025-10-23T19:31:06Z** |
| Subject | `[SPARK-54001][SQL] Optimize memory usage in session cloning with ref-counted cached local relations` |
| Divergence from `apache/spark` | **`identical`** — recorded as given; no `apache` remote was added to re-derive it |
| Version rows in the pinned pom | `4.1.0-SNAPSHOT` (L29), `java.version` 17 (L120), `maven.version` 3.9.11 (L123), `scala.version` 2.13.17 (L178), `scala.binary.version` 2.13 (L179) |
| Tree size | 297 MB |

Cloned **by SHA**, shallow, anonymously:
`GIT_LFS_SKIP_SMUDGE=1 git fetch -q --depth 1 origin <SHA>`. Note that
`git -c http.extraHeader="Authorization: Bearer $GITHUB_TOKEN"` **fails** here with
`fatal: could not read Username for 'https://github.com'`; the repository is public,
so anonymous fetch is the working path.

### Scope — twelve authoritative globs

`$HARNESS_SCOPE_FILE` = `harness/scope/allowlist.txt`, 12 lines, LF, sha256
**`0013edf6cdc3a48d69aed5d7db41cc6647cfd461d348f5e1d563ba85664143d1`**. Byte-exact
as given; not derived, not extended, not narrowed.

They expand on the pinned tree to **exactly 18 directories / 4,095 files** (paths
containing `src/test` excluded). Eighteen and twelve are the same scope: expanding a
glob is arithmetic, not a widening.

| Directory | Files | | Directory | Files |
|---|---|---|---|---|
| core/src/main | 751 | | sql/connect/server/src/main | 65 |
| common/network-common/src/main | 103 | | sql/connect/shims/src/main | 1 |
| common/network-shuffle/src/main | 51 | | sql/hive/src/main | 42 |
| common/network-yarn/src/main | 3 | | sql/hive-thriftserver/src/main | 114 |
| sql/catalyst/src/main | 799 | | resource-managers/kubernetes/core/src/main | 53 |
| sql/core/src/main | 790 | | resource-managers/kubernetes/core/volcano/src/main | 1 |
| sql/connect/client/jdbc/src/main | 3 | | resource-managers/kubernetes/docker/src/main | 5 |
| sql/connect/client/jvm/src/main | 2 | | resource-managers/yarn/src/main | 31 |
| sql/connect/common/src/main | 78 | | python/pyspark | 1,203 |

`python/pyspark` contributes 1,203 files of which **832 are test modules** (under a
`tests/` directory or named `test_*.py`). **None contains a `src/test` path
segment**, so they are in scope and are scanned. No Spark test suite is executed, in
any language.

### Build

Maven preflight before the build: the pom requires 3.9.11, `PATH` `mvn` **is**
3.9.11 and no `build/apache-maven*` exists, so `./build/mvn`'s `install_mvn()`
early-returns and **downloads nothing**. That check matters — a wrapper-triggered
download is an install, and `.gitignore:39` would have hidden it.

```bash
cd "$SPARK_SRC"
export MAVEN_OPTS="-Xss64m -Xmx6g -Xms2g -XX:ReservedCodeCacheSize=512m"
./build/mvn --no-transfer-progress -DskipTests \
  -Pyarn -Pkubernetes -Phive -Phive-thriftserver -Pvolcano \
  -pl core,common/network-common,common/network-shuffle,common/network-yarn,\
sql/catalyst,sql/core,sql/connect/shims,sql/connect/common,sql/connect/server,\
sql/connect/client/jvm,sql/connect/client/jdbc,sql/hive,sql/hive-thriftserver,\
resource-managers/kubernetes/core,resource-managers/yarn -am package
```

**`BUILD SUCCESS`, total time 25:43 min, finished 2026-08-24T12:37:07Z.** JVM major
17. Reactor = **33 projects: 32 producing a JAR + the parent POM**. Slowest: SQL
3:47, Core 2:26, ML Library 2:06, Catalyst 1:59.

`-Pvolcano` is confirmed load-bearing: `VolcanoFeatureStep` appears in the graph as
7 type declarations. Two projects write outside their build directory root, both
confirmed present: `common/network-yarn/target/scala-2.13/spark-4.1.0-SNAPSHOT-yarn-shuffle.jar`
(109 MB shaded) and copied runtime dependencies under `core/target/jars`,
`mllib/target/jars`, `sql/connect/client/jvm/target/connect-repl`.

Evidence: `provision-log/build-reactor.log` (1.6 MB, verbatim),
`provision-log/build-modules.txt`.

---

## 6. Per-module JAR outcome — all 33 reactor projects

Every project succeeded; **32 of 32 JAR-packaging projects produced their own
artifact**. `Spark Project Parent POM` is `<packaging>pom</packaging>` and produces
none, which is expected.

| Project | Module path | JAR | Build time |
|---|---|---|---|
| Spark Project Parent POM | *(root)* | **none — expected, packaging=pom** | 33.958 s |
| Spark Project Tags | common/tags | yes | 12.880 s |
| Spark Project Sketch | common/sketch | yes | 7.633 s |
| Spark Project Common Java Utils | common/utils-java | yes | 7.109 s |
| Spark Project Common Utils | common/utils | yes | 16.519 s |
| Spark Project Local DB | common/kvstore | yes | 10.572 s |
| Spark Project Networking | common/network-common | yes | 15.545 s |
| Spark Project Shuffle Streaming Service | common/network-shuffle | yes | 10.150 s |
| Spark Project Variant | common/variant | yes | 3.650 s |
| Spark Project Unsafe | common/unsafe | yes | 12.579 s |
| Spark Project Connect Shims | sql/connect/shims | yes — **excluded from the graph by instruction** | 3.518 s |
| Spark Project Launcher | launcher | yes | 8.524 s |
| Spark Project Core | core | yes | 02:26 min |
| Spark Project ML Local Library | mllib-local | yes | 01:18 min |
| Spark Project GraphX | graphx | yes | 01:21 min |
| Spark Project Streaming | streaming | yes | 01:35 min |
| Spark Project SQL API | sql/api | yes | 31.836 s |
| Spark Project Catalyst | sql/catalyst | yes | 01:59 min |
| Spark Project SQL | sql/core | yes | 03:47 min |
| Spark Project ML Library | mllib | yes | 02:06 min |
| Spark Project Declarative Pipelines Library | sql/pipelines | yes | 28.088 s |
| Spark Project Hive | sql/hive | yes | 01:00 min |
| Spark Project Connect Common | sql/connect/common | yes | 44.062 s |
| Spark Avro | connector/avro | yes | 24.901 s |
| Spark Protobuf | connector/protobuf | yes | 29.931 s |
| Spark Project REPL | repl | yes | 16.381 s |
| Spark Project Connect Server | sql/connect/server | yes | 52.066 s |
| Spark Project Connect JDBC Driver | sql/connect/client/jdbc | yes | 9.171 s |
| Spark Project Connect Client | sql/connect/client/jvm | yes | 54.598 s |
| Spark Project YARN Shuffle Service | common/network-yarn | yes | 49.568 s |
| Spark Project YARN | resource-managers/yarn | yes | 31.340 s |
| Spark Project Kubernetes | resource-managers/kubernetes/core | yes | 36.498 s |
| Spark Project Hive Thrift Server | sql/hive-thriftserver | yes | 33.786 s |

**Two in-scope roots are not Maven modules and appear in no reactor** —
`python/pyspark` and `resource-managers/kubernetes/docker`. Both are scanned by the
file-based tools. Their absence from this table is expected, not a gap.

---

## 7. The code-property graph

| | |
|---|---|
| Path | The bytes live at `/opt/blitzy-harness/cpg/spark.cpg`. `$HARNESS_CPG` is set to `<repo>/harness/cpg/spark.cpg`, a 33-byte **symlink** to those bytes — so the runbook's named path and the exported variable are the same file, and both resolve to the sha256 below. The six identity/count rows and the two write-fact rows below were **re-anchored on 2026-09-02** to this graph's write-time record of account; the superseded 2026-08-24 figures are kept in the supersession appendix at the end of this file |
| **Bytes** | **541,309,809** |
| **sha256** | **`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`** |
| **Methods** | **1,396,899** (internal 1,307,112) |
| **Type declarations** | **119,721** |
| **Files** | **45,037** |
| Heap used | **`-J-Xmx64g`** under JDK 21.0.12.1+1, peak sampled RSS **66.6 GB** |
| Elapsed | **50 m 42 s** (18:28:00Z → 19:18:42Z) |

```bash
SL_LOGGING_LEVEL=WARN jimple2cpg /opt/blitzy-harness/cpg-input \
  -o /opt/blitzy-harness/cpg/spark.cpg --recurse -J-Xmx64g < /dev/null
```

Input: **62 JARs, 273 MB, from 31 modules**, staged as hard links into one directory
with collision-safe names `<module_with_underscores>__<filename>`, verified 1:1 (62
files, 62 distinct sha256). Of 234 JARs found under the build tree, 190 were
excluded with a per-file reason: 64 `sources` jars (no bytecode), 59 copied runtime
dependencies (other coordinates), **34 `-tests` jars and 2 `spark-connect-shims`
jars — both excluded by runbook instruction**, 14 test-fixture jars under
`*/test-classes/`, 17 not build outputs. Each module contributed its main artifact
and its `original-` pre-shade sibling.

**Verified three times by `importCpg`** (post-build verification, an independent
second load, and Test 4), and a fourth time by the Joern runner: all four report
**1,397,339 / 119,691 / 45,037 identically** — the SUPERSEDED graph's counts, see
appendix S.3. Verification load: ~12.5 min, RSS 30.2 GB.

**Frontend metrics, observed rather than expected.** **31,598 `Overwriting class
file` warnings over 26,221 distinct class files** (org/apache/spark 24,525;
org/sparkproject/io 2,593; org/sparkproject/guava 2,017; org/sparkproject/connect
2,017; org/apache/hive 126) — far above the runbook's ~5,700 because each module
contributes both its shaded and its pre-shade artifact, which duplicate every class.
**173 AST-creation exceptions** (104 `org/sparkproject/io` netty-vendored, 69
`org/apache/spark`), against the runbook's expected ~36 protobuf failures.
**Limitation: per-class provenance for an overwritten class is not measurable from
this frontend's output** — the warning names the destination class, never the JAR
whose definition survived. The ordered staging manifest makes the input set
reproducible; the winner map does not exist.

### Per-module graph coverage — 31 of 31 contributing modules covered

The test is a class present in that module's primary artifact and **absent from every
other module's**. A shared package prefix is not evidence. Every witness below was
confirmed present as a type declaration in the graph.

**26 modules covered by a class unique to that module's artifact:**

| Module | Witness class | typeDecls | methods |
|---|---|---|---|
| core | `org.apache.spark.Aggregator` | 2 | 54 |
| sql/catalyst | `org.apache.spark.sql.catalyst.AliasIdentifier` | 2 | 40 |
| sql/core | `org.apache.spark.sql.DataSourceRegistration` | 2 | 72 |
| sql/hive | `org.apache.spark.sql.hive.DeferredObjectAdapter` | 2 | 36 |
| sql/hive-thriftserver | `org.apache.spark.sql.hive.thriftserver.ArrayFetchIterator` | 2 | 258 |
| sql/connect/server | `org.apache.spark.sql.connect.SimpleSparkConnectService` | 2 | 2 |
| sql/connect/client/jvm | `org.apache.spark.sql.application.ConnectRepl` | 2 | 2 |
| sql/connect/client/jdbc | `org.apache.spark.sql.connect.client.jdbc.NonRegisteringSparkConnectDriver` | 2 | 16 |
| sql/pipelines | `org.apache.spark.sql.pipelines.AnalysisWarning` | 2 | 0 |
| resource-managers/kubernetes/core | `org.apache.spark.deploy.k8s.Config` | 2 | 254 |
| resource-managers/yarn | `org.apache.spark.deploy.yarn.AmIpFilter` | 2 | 18 |
| common/network-yarn | `org.apache.spark.network.yarn.YarnShuffleService` | 2 | 36 |
| common/kvstore | `org.apache.spark.util.kvstore.ArrayWrappers` | 2 | 4 |
| common/sketch | `org.apache.spark.util.sketch.BitArray` | 2 | 28 |
| common/tags | `org.apache.spark.annotation.AlphaComponent` | 2 | 0 |
| common/unsafe | `org.apache.spark.sql.catalyst.expressions.HiveHasher` | 2 | 12 |
| common/utils | `org.apache.spark.BreakingChangeInfo` | 2 | 16 |
| common/variant | `org.apache.spark.types.variant.ShreddingUtils` | 2 | 8 |
| connector/avro | `org.apache.spark.sql.avro.AvroDataToCatalyst` | 2 | 106 |
| connector/protobuf | `org.apache.spark.sql.protobuf.CatalystDataToProtobuf` | 2 | 82 |
| graphx | `org.apache.spark.graphx.Edge` | 2 | 120 |
| launcher | `org.apache.spark.launcher.AbstractAppHandle` | 2 | 30 |
| mllib | `org.apache.spark.ml.Estimator` | 2 | 20 |
| mllib-local | `org.apache.spark.ml.impl.Utils` | 2 | 12 |
| repl | `org.apache.spark.repl.Main` | 2 | 24 |
| streaming | `org.apache.spark.status.api.v1.streaming.ApiStreamingApp` | 2 | 16 |

**5 modules where no class is exclusively theirs, with the weaker witness accepted
named:** in each case the class **is** in that module's primary artifact but is also
vendored into another module's shaded artifact, so the evidence is presence rather
than exclusivity.

| Module | Witness accepted | Also vendored by |
|---|---|---|
| common/network-common | `org.apache.spark.network.TransportContext` | common/network-yarn |
| common/network-shuffle | `org.apache.spark.network.sasl.ShuffleSecretManager` | common/network-yarn |
| common/utils-java | `org.apache.spark.QueryContext` | common/network-yarn |
| sql/api | `org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction` | sql/connect/client/jvm |
| sql/connect/common | `org.apache.spark.connect.proto.AddArtifactsRequest` | sql/connect/client/jvm, sql/connect/server |

**The `META-INF/maven/**/pom.properties` fallback was unavailable** — no Spark
artifact at this pin contains one, so it could not be used for any module.
`sql/connect/shims` has **no coverage verdict because it is excluded from the graph
input by instruction**, not because it is missing.

Evidence: `provision-log/cpg-record.txt`, `cpg-input-inventory.json`,
`cpg-frontend.log` (6.3 MB verbatim), `cpg-verify.log`, `cpg-coverage.log`,
`module-witness-candidates.json`.

### One graph fact that will otherwise look like a defect

`cpg.method.nameExact("createDriver")` returns **0**, while `Master` (2 typeDecls,
624 methods), `DriverRunner`, `StandaloneRestServer` and 54 `receiveAndReply`
methods are all present. Cause established with `javap`: **Scala 2.13 name-mangles
`private def createDriver` to `org$apache$spark$deploy$master$Master$$createDriver`**.
A probe keyed on the literal source name finds nothing on a bytecode graph. This is
a property of Scala's private-method encoding, not a gap in the graph. Related: file
nodes appear as `<unknown>` or as `/tmp/jimple2cpg-<id>/...` extraction paths.

---

## 8. Harness layout and contract

```
harness/
  ENVIRONMENT.md          this file
  env.sh                  the environment file to source
  lib/scope.sh            shared scope + logging + credential contract
  lib/joern-scan.sc       the bounded Joern query set
  bin/run-<tool>.sh       nine runners, one per tool — the ONLY entries in bin/
  scope/allowlist.txt     the twelve globs, byte-exact
  cpg/spark.cpg           symlink to the shared graph
  artifacts/raw/          EMPTY — one artifact per tool goes here at scan time
  artifacts/logs/         EMPTY — per-tool streams and status go here
```

**`harness/bin/` contains exactly the nine runners and nothing else — no
orchestrator, no helper.** There is deliberately no `run-all.sh`: it would destroy
per-tool attribution and carry a run past a condition that must stop it.

`env.sh` exports: `HARNESS_DIR`, `HARNESS_REPO_ROOT`, `HARNESS_RAW_DIR`,
`HARNESS_LOG_DIR`, `HARNESS_SCOPE_FILE`, `HARNESS_CPG`, `HARNESS_LIB_DIR`,
`HARNESS_SHARED_DIR`, `HARNESS_TOOLS_DIR`, `HARNESS_SCRATCH_DIR`, `SPARK_SRC`,
`SPARK_SRC_COMMIT`, `JAVA_HOME`, `JAVA_HOME_21`, `JOERN_HOME`,
`DEPENDENCY_CHECK_HOME`, `MAVEN_HOME`, `SCALA_HOME`, `PATH`, `OPENGREP_RULES_DIR`,
`SEMGREP_RULES_DIR`, `DD_SAST_RULES_FILE`, `TRIVY_CACHE_DIR`, `HARNESS_DC_DATA_DIR`,
`LANG`, `LC_ALL`, `PYTHONUTF8`, `SL_LOGGING_LEVEL`, `HARNESS_JOERN_HEAP`.

`lib/scope.sh` provides `scope_fail` (diagnostic to stderr then **exit 78**,
`EX_CONFIG`), `scope_resolve_target`, `scope_dirs` (allowlist expansion under
`globstar`/`nullglob`/`dotglob`, trailing `/**` stripped, any path containing
`src/test` skipped, `sort -u`), `scope_begin`, `scope_finish` (exit code, elapsed
seconds, artifact bytes or `MISSING`, plus a machine-readable
`$HARNESS_LOG_DIR/<tool>.status`) and `scope_cred_state`.

**Every runner rejects an argument before it scans.** The guard is the first
executable statement in all nine, ahead of sourcing `env.sh`, and exits **64**:
`run-<tool>.sh: takes no arguments (configuration is baked in); refusing to scan`.

**Credential reporting prints a fixed token only.** `scope_cred_state` uses
`${VAR:+set}` and emits exactly `set` or `absent`. The `${VAR:+set}${VAR:-absent}`
form is **not** used anywhere: with a value present its `:-` arm yields the
variable's own value, which would write a live credential into a log this pipeline
preserves verbatim.

**`HARNESS_SMOKE_TARGET` exists and is deliberately unset.** Setting it redirects
every runner at one small directory and makes `scope_dirs` return `.`; it is for
setup-time verification only. Leaving it set silently scans the wrong thing.

**Runtime topology per runner** — read this rather than assuming it:

| Runner | JVM | Interpreter | Notes |
|---|---|---|---|
| run-joern.sh | **`$JAVA_HOME_21`** at `$HARNESS_JOERN_HEAP` (64g) | — | stdin closed; runs from `$HARNESS_SCRATCH_DIR/joern-run` so Joern's ~800 MB `./workspace` never lands in the repo; exits 78 if the graph is missing |
| run-dependency-check.sh | **`$JAVA_HOME` (17)** | — | `--noupdate --disableOssIndex --data $HARNESS_DC_DATA_DIR` |
| run-semgrep.sh | — | venv Python **3.13.7** (`/opt/blitzy-tools/venvs/semgrep`) | |
| run-checkov.sh | — | venv Python **3.13.7** (`/opt/blitzy-tools/venvs/checkov`) | |
| run-opengrep.sh | — | standalone binary | |
| run-gitleaks.sh, run-trivy.sh | — | system Python 3.13.7 **only to merge per-directory JSON** | |
| run-datadog-static-analyzer.sh, run-osv-scanner.sh | — | standalone binary | |

Dependency-Check `--version` succeeds under both JDKs; 17 is the assignment.

---

## 9. Per-tool results over the full twelve-root scope

All nine invoked **directly, one at a time, no arguments, no time limit**. Verified
scan root `/opt/spark-src` from `SPARK_SRC` for every one. Output was written to
`/opt/blitzy-harness/verify-run/{raw,logs}` so the deliverable
`harness/artifacts/{raw,logs}` stay empty.

| Tool | Exit | Elapsed | Findings | Format | Parsed | Artifact bytes |
|---|---|---|---|---|---|---|
| opengrep | 0 | 929 s | **1,322** | SARIF 2.1.0 | yes | 73,840,948 |
| semgrep | 0 | 449 s | **1,162** | SARIF 2.1.0 | yes | 40,660,951 |
| datadog-static-analyzer | 0 | 57 s | **6,832** | SARIF 2.1.0 | yes | 5,671,090 |
| gitleaks | 2 | 15 s | **1** | native JSON array | yes | 561 |
| checkov | 1 | 88 s | **6** | native JSON, object form | yes | 8,380 |
| trivy | 0 | 17 s | **3** | native JSON SchemaVersion 2 | yes | 3,496 |
| dependency-check | 0 | 6 s | **0** | native JSON | yes | 17,097 |
| osv-scanner | **128** | 0 s | **0** | **no artifact written** | n/a | — |
| joern | 0 | 734 s | **692** | native JSON | yes | 354,343 |

Total 10,018 findings. **Non-zero exit with findings is ordinary** — gitleaks (2)
and checkov (1) both exit non-zero *because* they found something; both artifacts
parse.

### Path base per tool — what a normalizer must anchor on

| Tool | Base |
|---|---|
| opengrep, semgrep | `uri` is **root-relative to `$SPARK_SRC`**. Both set `uriBaseId: "%SRCROOT%"` on every result but **neither emits `run.originalUriBaseIds`**, so SARIF §3.4.4 resolution cannot complete and the runner-recorded scan root is the only base. Every `uri` was verified to exist under `$SPARK_SRC`; zero absolute URIs |
| datadog-static-analyzer | plain relative `uri`, **no `uriBaseId`, no `originalUriBaseIds`**; relative to `$SPARK_SRC` |
| gitleaks | `File` root-relative to `$SPARK_SRC` (the runner cds to the root and passes one root-relative directory per invocation) |
| checkov | **`file_path` is relative to the matching `-d` directory, with a leading slash — NOT to the scan root**, because this invocation passes 18 `-d` roots. `repo_file_path` is root-relative with a leading slash; `file_abs_path` is absolute. **Anchor on `repo_file_path` or `file_abs_path`** |
| trivy | `Results[].Target` **root-relative to `$SPARK_SRC`** and `ArtifactName` `"."`, because the merge prefixes each part's Target with that part's own `ArtifactName`. The 18 unmerged per-directory reports in `logs/trivy.parts/` state Target relative to their single path argument |
| dependency-check | `dependencies[].filePath` **filesystem-absolute** under `$SPARK_SRC` |
| osv-scanner | n/a — no artifact |
| joern | the `file` field is the frontend's **ephemeral** `/tmp/jimple2cpg-<id>/<pkg>/<Class>.class` path for **692 of 692** findings and is **not** a path in the Spark tree. Resolve through the `class` field instead |

**Joern class→source resolvability of its 692 findings**, measured: **89** resolve
uniquely to a `src/main` source file, **0** ambiguous, **0** into `src/test`, **18**
are `org.apache.spark` classes whose source filename does not match the class name
(Scala permits it — e.g. `ProcessBuilderLike$$anon$3` lives in `CommandUtils.scala`),
and **585** are third-party classes shaded into Spark's JARs with no source in the
tree at all (`org.sparkproject` 527, non-Spark `org.apache` 48, `com.google` 12,
`org.fusesource` 1, `org.rocksdb` 1).

### What each tool reported about its own reach, in its own words

- **opengrep** — `Ran 1138 rules on 4095 files: 1322 findings.` / `Scan was limited
  to files tracked by git.` / `Partially scanned: 46 files only partially analyzed
  due to parsing or internal Opengrep errors`
- **semgrep** — `Targets scanned: 4094` / `Parsed lines: ~99.9%` / `Scan was limited
  to files tracked by git`. The single in-scope file not selected is a `.png` the
  engine drops as binary.
- **datadog-static-analyzer** — `#static analysis rules : 1093` over 18
  subdirectories. **Its pinned ruleset carries no Scala rules at all**: 1,093 rules
  across 12 languages (PYTHON, RUBY, JAVA, GO, PHP, BASH, DART, KOTLIN, CSHARP,
  RUST, JAVASCRIPT, TYPESCRIPT), so **none of the 2,206 in-scope `.scala` files can
  produce a finding**. Its 6,832 results come from `.py` (355 files), `.java` (192),
  `.js` (19) and `.sh` (2). This is the single most important reach fact about this
  tool and it is not visible from the finding count.
- **gitleaks** — `WRN leaks found: 1`. Invoked **once per scope directory** because
  `gitleaks dir` takes exactly one path and silently falls back to the working
  directory when handed more. `--redact=100`, so `Secret` and `Match` are `REDACTED`
  in the artifact.
- **checkov** — `passed=201 failed=6 skipped=0 parsing_errors=0 resource_count=3`.
  The only IaC content in scope is the 3 Kubernetes Dockerfiles. Emitted the
  **object** form (`check_type: dockerfile`) because a single framework reported; the
  multi-framework array form appears when more than one does. Only `failed_checks`
  are findings.
- **trivy** — 3 `Results` sections, all `Class=config Type=dockerfile`: 3
  misconfigurations (`DS-0026 No HEALTHCHECK defined`, LOW). **Zero
  Vulnerabilities, zero Secrets.** `Licenses` and `ExperimentalModifiedFindings`
  verified empty. Flags: `--skip-db-update --skip-java-db-update --skip-check-update
  --offline-scan` (an earlier run without `--offline-scan` took
  `429 Too Many Requests, Retry-After: 1800` from Maven Central and wrote **no
  artifact at all**).
- **dependency-check** — 32 dependencies analysed, **0 with a vulnerability, 0 with
  a resolved package coordinate**. All 32 are vendored web assets (31 `.js` plus the
  80-byte `package.json`) under `core/src/main/resources/.../ui/static` and
  `sql/core/src/main/resources/.../ui/static`. Verbatim: `Analyzing
  .../ui/static/package.json - however, the node_modules directory does not exist.
  Please run 'npm install' prior to running dependency-check` and `No lock file
  exists - this will result in false negatives; please run 'npm install
  --package-lock'`. 6 s rather than the runbook's ~29 min **because there is nothing
  in scope to resolve**, not because anything failed.
- **osv-scanner** — stdout **empty**; stderr ends `End status: 640 dirs visited,
  4735 inodes visited, 0 Extract calls, 295.514705ms elapsed` then **`No package
  sources found, --help for usage information.`**
- **joern** — 6 bounded structural queries over the whole graph, bound **2,000**,
  `bound_reached=false` on all six: reflection-forname 412, unsafe-deserialization
  178, process-exec 55, message-digest 23, xml-factory 13, cipher-getinstance 11.
  Graph counts at load identical to the build.

### The one tool that wrote no artifact

**osv-scanner, exit 128 → the tool found nothing in scope to work on and said so in
its own output. It did not fail.** Its own words are quoted above: zero `Extract`
calls means it resolved no manifest or lockfile to extract packages from, which is
this tool's long-standing documented behaviour for a zero-package input
(<https://github.com/google/osv-scanner/issues/348>). It walked the real scope — 640
directories and 4,735 inodes ≈ the 4,095 in-scope files plus their directories.
**The scanning run continues past this rather than halting.**

Evidence: `provision-log/test6-record.json`, `test6-per-tool.txt`,
`runner-<tool>.console.log` (nine files), and
`/opt/blitzy-harness/verify-run/logs/<tool>.{stdout,stderr}.log` verbatim.

---

## 10. Zero resolvable dependency manifests in scope

Searched all 18 scope directories for 25 manifest and archive patterns: `pom.xml`,
`build.gradle*`, `build.sbt`, `requirements*.txt`, `package.json`,
`package-lock.json`, `yarn.lock`, `pnpm-lock.yaml`, `setup.py`, `setup.cfg`,
`pyproject.toml`, `Pipfile*`, `poetry.lock`, `Gemfile*`, `go.mod`, `go.sum`,
`Cargo.toml`, `Cargo.lock`, `composer.json`, `*.jar`, `*.war`, `*.whl`,
`*.egg-info`, `conda*.yml`, `environment.yml`.

**Every pattern returned 0 except `package.json`, which returned 1.**

**Honest correction to the runbook.** The runbook states the roots contain "no
`package.json`". One does exist:
`core/src/main/resources/org/apache/spark/ui/static/package.json`, **80 bytes**,
contents `{"name": "spark-ui", "license": "Apache License 2.0", "type": "module"}` —
**no dependencies block, no lockfile beside it and no `node_modules`**. So the
accurate statement is **zero *resolvable* dependency manifests**: one
manifest-shaped file exists and declares no dependencies, so nothing in scope
resolves to a package.

Every per-tool consequence the runbook predicted holds unchanged:

- **osv-scanner** writes no artifact — exit 128, `0 Extract calls`, `No package sources found`.
- **dependency-check** sees only vendored web assets — 32 files, 0 vulnerabilities, 0 resolved coordinates.
- **trivy's vulnerability scanner** resolves nothing — 0 `Vulnerabilities`, 0 `Secrets`, only 3 dockerfile misconfigurations.

**This is a property of the scope, not of the installation.** Each of the three is
proven functional in isolation. **The allowlist was not widened** and its sha256 is
unchanged. Do not report these three as broken.

---

## 11. Test results — runbook §8

| Test | Result |
|---|---|
| 1. toolchain resolves in a fresh non-login shell | **PASS.** `env -i bash --noprofile --norc`, then `. harness/env.sh` (exit 0), then all nine tools plus both JDKs, Maven, Scala and Python reported their versions. `LANG=LC_ALL=C.utf8` |
| 2. every runner rejects an argument without scanning | **PASS.** Guard confirmed by inspection to be the first executable statement in all nine; all nine exit **64**; `harness/artifacts/{raw,logs}` verified 0/0 entries before **and** after |
| 3. the pinned tree is the pinned tree | **PASS.** `git -C /opt/spark-src rev-parse HEAD` = `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` |
| 4. the graph loads and is covered per module | **PASS.** `importCpg` reports 1,397,339 / 119,691 / 45,037 (the SUPERSEDED graph's counts — appendix S.3); 31 of 31 contributing modules covered (26 unique-class witnesses, 5 named weaker witnesses) |
| 5. taint is active on Spark's own Scala | **PASS — see below** |
| 6. all nine scanners across the full scope, one at a time | **PASS — §9 above** |

### Test 5 — the taint A/B, in full

Same pinned rule file
`/opt/blitzy-harness/rules/opengrep-rules/scala/lang/security/audit/tainted-sql-string.yaml`
(`mode: taint`, line 6), same target file
`sql/core/src/main/scala/org/apache/spark/sql/jdbc/JdbcDialects.scala`,
`--dataflow-traces` on both arms, and **`--taint-intrafile` as the only difference**:

| Arm | Findings | Lines |
|---|---|---|
| **taint ON** (`--taint-intrafile`) | **12** | 268, 286, 296, 309, 603, 615, **659, 666, 670, 676**, 683, 714 |
| **taint OFF** (intraprocedural) | **11** | 268, 286, 296, 309, 544, 603, 615, 683, 700, 702, 714 |

**Lines reachable only with taint on: 659, 666, 670, 676.** All 12 ON-arm findings
carry `codeFlows` — a dataflow trace a pattern match cannot produce. **Taint is
active on Spark's own Scala.**

Two honest notes. **Opengrep has no engine-level flag to disable taint entirely**
(`--optimizations=none` only toggles optimizations; there is no `--no-taint`), so
`--taint-intrafile` is the available discriminator and the OFF arm is
intraprocedural rather than taint-free. And on
`core/src/main/scala/org/apache/spark/storage/DiskStore.scala` **both** arms report 1
finding at line 72 — that matches the expected line-72 result but is
**non-discriminating**, which is why the A/B moved to a file where the arms differ.

Artifacts: `/opt/blitzy-harness/verify-run/taint/{ab-on,ab-off,on,off,core-on,core-off}.{sarif,log}`.

### The Joern runner's bound, proven

`harness/lib/joern-scan.sc` bakes **6 bounded structural queries**, each keyed on an
indexed call name, filtered by callee full-name prefix, and cut with
`.take(bound+1)` so truncation is observable. Proven over the full graph: **exit 0,
elapsed 735 s (12 m 15 s), artifact 354,344 bytes, 692 findings, `bound_reached=false`
on all six**. Per-query time totals ≈1.2 s — the 12 minutes are `importCpg` plus
Joern's default overlays, not the queries.

| Query id | Callee prefixes | Returned | ms |
|---|---|---|---|
| joern-process-exec | `java.lang.Runtime.exec`, `java.lang.ProcessBuilder.start` | 55 | 1052 |
| joern-unsafe-deserialization | `java.io.ObjectInputStream.readObject` | 178 | 6 |
| joern-reflection-forname | `java.lang.Class.forName` | 412 | 10 |
| joern-message-digest | `java.security.MessageDigest.getInstance` | 23 | 1 |
| joern-cipher-getinstance | `javax.crypto.Cipher.getInstance` | 11 | 0 |
| joern-xml-factory | `DocumentBuilderFactory`/`SAXParserFactory`/`XMLInputFactory`/`TransformerFactory.newInstance` | 13 | 1 |

For contrast, the runbook records Joern's default 59-query bundle running **2 h 26 m
wall / 9 h 35 m CPU without finishing** on this class of host. The bound governs
which queries run, never which files or modules are in scope.

**Joern's launcher does not forward `-D` properties to a script** — only `-J` flags
reach the JVM, so a first attempt died in 7 s with `harness.cpg not set`. The runner
therefore passes `HARNESS_SCAN_CPG`, `HARNESS_SCAN_OUT` and `HARNESS_SCAN_BOUND` as
environment variables, with `-D` names kept as a fallback.

Evidence: `provision-log/joern-runner-bound-proof.log`, `test1-toolchain.log`,
`graph-diagnostic.log`.

---

## 12. Two runner behaviours that were corrected during provisioning

Both are configuration decisions this run owns, since the scanning run may not alter
a runner. Both are recorded because they change what a count means.

**1. Trivy's merged artifact was not anchorable.** `trivy fs` takes one path, so the
runner invokes it once per scope directory; each per-directory report states
`Results[].Target` relative to its own single path argument and names that path in
its own `ArtifactName`. Concatenating the sections lost that attribution and left
Targets like `dockerfiles/spark/Dockerfile` unresolvable. The merge now prefixes
every Target with its own part's `ArtifactName` and sets the merged `ArtifactName` to
`"."`, so **every Target is root-relative to `$SPARK_SRC`**; all three were verified
to be real files. The 18 per-directory reports are retained **verbatim** under
`$HARNESS_LOG_DIR/trivy.parts/`.

**2. opengrep and semgrep were silently dropping 846 of the 4,095 in-scope files.**
Both engines apply bundled `.semgrepignore` patterns that skip `tests/` directories.
Measured exactly with `semgrep scan --x-ls`: **3,249 of 4,095 selected**, the 846
skipped being **834 files under `tests/` directories** (python/pyspark/pandas/tests
510, sql/tests 180, ml/tests 68, tests 22, and smaller) plus 12 vendored assets under
`core/src/main/resources/.../ui/static`. Both tools said so themselves — semgrep
`Files matching .semgrepignore patterns: 845`, opengrep `11 files and 14 directories
matching .semgrepignore patterns`.

`python/pyspark/**` is squarely inside the authoritative scope, and **an in-scope
file never analyzed reads exactly like a file with nothing to report** — the same
failure mode the runbook flags for `-Pvolcano`. `--include` cannot restore them: it
is applied *after* semgrepignore filtering and only narrows. Both runners therefore
pass **`--x-ignore-semgrepignore-files`**, which raises selection to **4,094 of
4,095** (the one exclusion being a `.png` dropped as binary). The flag is marked
internal by the tools, which is acceptable at a pinned version.

**Effect on the counts, which is why both numbers are published:**

| Tool | Default reach | Configured reach | Findings before | Findings after | Elapsed before → after |
|---|---|---|---|---|---|
| opengrep | 3,250 files | **4,095** | 500 | **1,322** | 220 s → 929 s |
| semgrep | 3,249 files | **4,094** | 503 | **1,162** | 183 s → 449 s |

The default configuration was hiding **~62% of opengrep's and ~57% of semgrep's**
findings over the authoritative scope. **Neither tool's count is comparable with any
figure measured under the default.**

---

## 13. Credentials and secrets

All four scanner-relevant credentials are **absent, deliberately**, and each runner
reports its state with the fixed-token form only.

| Name | State | Consequence |
|---|---|---|
| `SEMGREP_APP_TOKEN` | **absent — do not attach** | Semgrep runs as Community Edition; Pro and interfile analysis unavailable. That unlicensed capability is the measurement |
| `DD_API_KEY`, `DD_APP_KEY` | absent | datadog's AI and secrets paths disabled; rules were fetched anonymously and then **pinned locally**, which is what closes the reproducibility gap |
| `NVD_API_KEY` | absent | keyless datafeed used; seeded in 896 s. Dependency-Check aborts on an *empty* key (`Invalid API Key, length of 0 too short`), so leave it unset rather than set to `""` |
| Sonatype OSS Index | absent | analyzer disabled, and `--disableOssIndex` is passed explicitly so the disabling is ours and recorded |
| `GITHUB_TOKEN` | **present** in the provisioning environment | used only for GitHub release/API downloads. Not read by any runner, and never printed |

---

## 14. Running many clones at once

The pinned clone, the graph, the rulesets, the feeds and the tool installs are all
**shared, read-only** and safe for concurrent readers. Two things were verified
empirically rather than assumed: two simultaneous `trivy fs` runs against the shared
`TRIVY_CACHE_DIR` both exit 0, and two simultaneous Dependency-Check runs against
the shared `--data` directory both exit 0 with full reports. No lock contention.

**Do not restart, re-seed, re-clone or delete any of these** — a sibling clone is
probably mid-scan against them.

**What must be per-clone.** `env.sh` derives
`HARNESS_SCRATCH_DIR=/tmp/blitzy-harness-scratch/${BLITZY_CLONE_INDEX:-0}` and the
Joern runner runs from `$HARNESS_SCRATCH_DIR/joern-run`, because Joern writes an
~800 MB `./workspace` into its working directory and two Joern processes sharing one
workspace corrupt each other. Pass the index on the command line
(`BLITZY_CLONE_INDEX=3 ./harness/bin/run-joern.sh`) rather than exporting it, since
each command runs in its own subshell. `HARNESS_RAW_DIR` and `HARNESS_LOG_DIR`
default inside each clone's own checkout and so are already private.

**No TCP port is bound by anything in this harness**, so there is no port-block
scheme to derive: no scanner, no runner and no Joern invocation listens. The one
resource that genuinely cannot be shared is the Joern workspace, and the scratch
directory above partitions it.

**Memory is the real constraint, not naming.** Each Joern invocation asks for a
64 GB heap and peaks near 59 GB. This host has ~3.75 TiB, so a handful of
concurrent Joern runs fit — but do not launch dozens, and do not run two in one
clone at once.

---

## 15. Values that could not be established

Named rather than omitted, since an unrecorded value is one the gate cannot check.

1. **Per-class provenance for the 26,221 overwritten class files.** The frontend's
   warning names the destination class, never the surviving JAR. Not measurable from
   this frontend's output. The ordered staging manifest makes the input set
   reproducible; the winner map does not exist.
2. **The datadog ruleset's byte digest against the scanning run's expected value.**
   Ours is `4f397e81…`, that table says `e70ede30…`. Same content, different
   serializer; compare on 48 rulesets / 1,093 rules / revision `f76636e4` instead.
3. **A taint-free Opengrep arm.** No flag disables taint entirely, so the OFF arm is
   intraprocedural. The A/B is still decisive (four lines and all `codeFlows` exist
   only in the ON arm).
4. **`META-INF/maven/**/pom.properties` as a coverage witness.** No Spark artifact at
   this pin contains one, so the fallback was unavailable for all 31 modules.
5. **A coverage verdict for `sql/connect/shims`.** Excluded from the graph input by
   instruction, so it has none — this is by design, not a gap.
6. **Wall-clock CPU accounting per stage.** `/usr/bin/time` is absent from this
   image; elapsed times come from `date` arithmetic and RSS from sampling.

---

# Values to inline into the scanning prompt

Bare values, copy as-is.

**Tools**
```
opengrep                 1.27.1
semgrep                  1.173.0
joern                    4.0.607
datadog-static-analyzer  0.9.1 (revision f76636e43554f7f9a8e3984a31d03ec8dea5489f)
gitleaks                 8.30.1
checkov                  3.3.12
trivy                    0.74.0
osv-scanner              2.5.1 (osv-scalibr 0.5.2)
dependency-check         13.0.0
```

**Runtimes**
```
JDK build/eight scanners  Temurin 17.0.20+8   (openjdk 17.0.20 2026-07-21)
JDK Joern only           Temurin 21.0.12.1+1 (openjdk 21.0.12.1 2026-08-18 LTS)
Maven                    3.9.11 (3e54c93a704957b63ee3494413a2b544fd3d825b)
Scala                    2.13.17
Python                   3.13.7
git                      2.51.0
```

**Rulesets**
```
opengrep-rules commit    f1d2b562b414783763fd02a6ed2736eaed622efa    2006 rules (58 configs)
semgrep-rules commit     40b8c63f75dc7c22c8a77482d73bfb864b146f7e    2149 rules, 19 Pro-only skipped (60 configs)
datadog SAST ruleset     sha256 4f397e81414f8e9469d20abc18c80c85c722e72b9f85b8bcf69dbe34b8fef6f1    48 rulesets / 1093 rules
gitleaks rules           built into 8.30.1, not separately versioned
checkov policies         bundled with 3.3.12, not separately versioned
```

**Vulnerability data**
```
trivy vuln DB            v2, UpdatedAt 2026-08-24T06:55:32.451220873Z
trivy java DB            v1, UpdatedAt 2026-08-24T01:07:04.599776272Z
dependency-check NVD     keyless NIST JSON 2.0 datafeed, NVD API Last Modified 2026-08-24T08:00:04-04
osv-scanner              no local database, queries https://api.osv.dev at scan time
```

**Pinned tree**
```
SPARK_SRC                /opt/spark-src
SPARK_SRC_COMMIT         59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d
commit date              2025-10-23T19:31:06Z
version at that commit   Spark 4.1.0-SNAPSHOT, Scala 2.13.17
divergence from upstream identical
allowlist sha256         0013edf6cdc3a48d69aed5d7db41cc6647cfd461d348f5e1d563ba85664143d1
scope expansion          18 directories / 4095 files (832 of them python/pyspark test modules, in scope)
```

**Graph**
```
path                     /opt/blitzy-harness/cpg/spark.cpg  (harness/cpg/spark.cpg is a symlink to it)
methods                  1396899
type declarations        119721
files                    45037
internal methods         1307112
bytes                    541309809
sha256                   4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7
heap needed              -J-Xmx64g under JDK 21, peak sampled RSS 66.6 GB
build elapsed            50m42s
input set                62 JARs from 31 modules (main + original- pre-shade sibling each); -tests and connect-shims JARs excluded by instruction
overwrite warnings       31598 over 26221 distinct class files
AST-creation failures    173
```

**Build outcome**
```
reactor                  33 projects: 32 JAR-producing, 1 parent POM
JAR outcome              32 of 32 produced their own artifact; parent POM produces none (expected)
non-module in-scope roots python/pyspark and resource-managers/kubernetes/docker (no Maven module, scanned by file-based tools)
build result             BUILD SUCCESS, 25:43 min, JVM major 17
```

**Graph coverage**
```
modules covered          31 of 31 contributing modules
unique-class witnesses   26
weaker witnesses         5 — common/network-common (org.apache.spark.network.TransportContext),
                             common/network-shuffle (org.apache.spark.network.sasl.ShuffleSecretManager),
                             common/utils-java (org.apache.spark.QueryContext),
                             sql/api (org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction),
                             sql/connect/common (org.apache.spark.connect.proto.AddArtifactsRequest)
pom.properties fallback  unavailable — no Spark artifact at this pin contains one
sql/connect/shims        no verdict: excluded from the graph input by instruction
```

**Taint A/B on Spark Scala**
```
rule                     opengrep-rules/scala/lang/security/audit/tainted-sql-string.yaml (mode: taint)
file                     sql/core/src/main/scala/org/apache/spark/sql/jdbc/JdbcDialects.scala
taint ON                 12 findings, all carrying codeFlows
taint OFF                11 findings
only-with-taint lines    659, 666, 670, 676
verdict                  PASS — taint is active on Spark's own Scala
```

**Per tool, full twelve-root scope**
```
tool                     exit  elapsed  findings  path base
opengrep                    0     929s      1322  root-relative uri; uriBaseId %SRCROOT% but NO originalUriBaseIds
semgrep                     0     449s      1162  root-relative uri; uriBaseId %SRCROOT% but NO originalUriBaseIds
datadog-static-analyzer     0      57s      6832  plain relative uri, no uriBaseId at all
gitleaks                    2      15s         1  File root-relative
checkov                     1      88s         6  file_path is -d-relative; anchor on repo_file_path or file_abs_path
trivy                       0      17s         3  Results[].Target root-relative, ArtifactName "."
dependency-check            0       6s         0  filePath absolute
osv-scanner               128       0s         0  n/a — no artifact
joern                       0     734s       692  file field is the frontend temp path; resolve via the class field
```

**Wrote nothing, and why**
```
osv-scanner   exit 128, stdout empty, stderr "No package sources found, --help for usage information."
              preceded by "0 Extract calls" over 640 dirs / 4735 inodes.
              FOUND NOTHING IN SCOPE, in its own words. NOT a failure. The scanning run continues.
```

**Host**
```
minimum memory this pipeline actually needed   64 GB heap, peak RSS 59.0 GB (2026-08-24; 66.6 GB for the graph on disk — appendix S.3)
host memory available                          ~3.75 TiB, 4 vCPU
heap commit proof                              java -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version exits 0
```

**Reach caveats that change what a count means**
```
opengrep/semgrep   runners pass --x-ignore-semgrepignore-files; the tool default would skip 846 of 4095
                   in-scope files (834 under tests/). Counts are NOT comparable with any default-config figure.
datadog            pinned ruleset has NO Scala rules (12 languages, 1093 rules), so 0 of 2206 in-scope
                   .scala files can produce a finding.
opengrep           46 files only partially analyzed due to parsing or internal errors.
both SAST engines  scan limited to files tracked by git (all in-scope files are tracked).
scope              zero RESOLVABLE dependency manifests; one 80-byte package.json exists with no
                   dependencies block, no lockfile and no node_modules.
```

---

# Appendix S — supersession record for §7, the code-property graph

**Added 2026-09-02. Everything above this appendix was written by the provisioning run
of 2026-08-24; this appendix is the only part of the file that was not.** It exists
because the graph §7 describes was replaced on the host, and it records three things a
reader needs and cannot get from the corrected table alone: which figures were
corrected, what they said before, and which figures were deliberately left as the
2026-08-24 run wrote them.

Nothing about the graph itself was changed to produce this appendix. The bytes at
`/opt/blitzy-harness/cpg/spark.cpg` were read and hashed, never written.

## S.1 What was re-anchored, from which owner, and why

This file's own header dates it **2026-08-24** (line 3) and its preamble undertakes that
"every number below traces to a file under `/opt/blitzy-harness/provision-log/`"
(lines 21-23). The host was **re-provisioned on 2026-08-30** and the graph was rebuilt:
the graph file's own mtime is **2026-08-30T19:18:37Z** and the owner records the
frontend's write window as closing at **19:18:42Z**, and provisioning recorded that
graph's identity beside it in the same directory this file names as its evidence —

| Owner file | Written | What it owns |
|---|---|---|
| `/opt/blitzy-harness/provision-log/cpg-identity.txt` | 2026-08-30T19:19:09Z | the one write-time `<bytes> <sha256>` pair for the graph on disk |
| `/opt/blitzy-harness/provision-log/cpg-record.txt` | 2026-08-30T19:33:42Z | the same pair, plus the `importCpg` verification counts, the heap and peak RSS, and the write window |

So §7's graph block was a faithful snapshot of a graph that has since been **superseded**,
and it no longer traced to the provision-log it cites. **That is the defect this appendix
closes: a stale record, not a wrong graph.** The write-time record that sits beside a
graph is the owner of that graph's identity, and §7 has been re-anchored to it.

Re-measured independently at re-anchor time, with the symlink followed, before anything
was edited: `stat -Lc %s "$HARNESS_CPG"` printed **541309809** and
`sha256sum "$(readlink -f "$HARNESS_CPG")"` printed
**`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`** — both equal to
what the owner files state, so the corrected table is a projection of the owner and of
the disk at once.

**Exactly what was rewritten, and how.** The six identity/count rows and the two
write-fact rows of the §7 table (lines 284-290), and the corresponding eight values in
the inline `**Graph**` block (lines 841-848). Line 283 gained a pointer to this
appendix. **No line was inserted into or deleted from lines 1-923**, because other
documents in this tree cite this file by line number; the corrections are in place and
this appendix is appended after the original last line.

## S.2 The superseded figures, in full, as the 2026-08-24 provisioning wrote them

Both values are kept, with their provenance, because that is what the authority rule
requires of an inherited field the expected-values table does not adjudicate: record the
inherited value **and** the newly measured one rather than choosing between them
silently. Correcting §7 therefore destroyed no evidence — the previous generation's
figures are here.

| Field | Superseded — 2026-08-24 provisioning | Current — 2026-08-30 write-time owner, now in §7 |
|---|---|---|
| Bytes | 541,255,894 | **541,309,809** |
| sha256 | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | **`4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7`** |
| Methods | 1,397,339 | **1,396,899** (−440, 0.03%) |
| Internal methods | 1,307,552 | **1,307,112** (−440) |
| Type declarations | 119,691 | **119,721** (+30) |
| Files | 45,037 | **45,037** — unchanged, the one figure the two generations share |
| Heap used / peak RSS | `-J-Xmx64g`, peak sampled RSS 59.0 GB | **`-J-Xmx64g`, peak sampled RSS 66.6 GB** |
| Elapsed / write window | 53 m 04 s (12:59:23Z → 13:52:27Z) | **50 m 42 s (18:28:00Z → 19:18:42Z)** |

A reader meeting **541,255,894** or **`26d327cc…`** anywhere in this tree is looking at
the 2026-08-24 generation, which is not on disk. The AAP method floor is unaffected:
1,396,899 is far above the 853,420 lower bound, and the direction of the change is
recorded rather than interpreted.

## S.3 §7 content retained as the 2026-08-24 provisioning's own observations

These are **not** re-anchored. They are that run's observations of its own frontend
invocation and its own verification load, and rewriting them would replace one run's
observations with another's inside a record that identifies itself by date. They are
named here instead, each with the current owner's superseding value, and the counts at
line 308 additionally carry an inline SUPERSEDED label pointing at this section.

| Retained text | As this file states it (2026-08-24) | Current owner's value (`provision-log/cpg-record.txt`) |
|---|---|---|
| Input set and JAR-exclusion breakdown, lines 297-304 | 62 JARs, 273 MB, 31 modules, staged 1:1; of **234** JARs found, 190 excluded — 64 `sources`, 59 copied runtime dependencies, 34 `-tests`, 2 `spark-connect-shims`, 14 test-fixture, 17 not build outputs | 62 JARs / 273 MB / 31 modules and the 1:1 staging verdict are **unchanged**; of **252** `.jar` files found, 190 excluded — **77** copied dependency / not a build output, **64** sources, **33** `-tests`, **14** test-fixture under `*/test-classes/`, **2** `spark-connect-shims` |
| "Verified three times by `importCpg`… all four report **1,397,339 / 119,691 / 45,037** identically. Verification load: ~12.5 min, RSS 30.2 GB", lines 306-309 | those three counts, from four loads of the superseded graph | the verification load of the graph on disk reports **1,396,899 / 119,721 / 45,037**, elapsed ~11 min, `VERIFY_EXIT=0`. The four-load agreement is a property of the 2026-08-24 run and is not restated for this graph |
| Overwrite-warning totals and their per-package split, lines 311-315 | 31,598 `Overwriting class file` warnings over 26,221 distinct class files (org/apache/spark 24,525; org/sparkproject/io 2,593; org/sparkproject/guava 2,017; org/sparkproject/connect 2,017; org/apache/hive 126) | **31,598 over 26,221 — exact match**, so this row is corroborated rather than superseded. The per-package split is this file's own; the owner does not restate it |
| "**173 AST-creation exceptions** (104 `org/sparkproject/io` netty-vendored, 69 `org/apache/spark`)", lines 316-317 | 173 | **`AstCreationPass` warnings 429**, with 0 ERROR-level lines. 173 is the 2026-08-24 figure and is the one `harness/artifacts/logs/cpg-graph-record.log` cites from line 316 |
| The provenance limitation, lines 318-321 | per-class provenance for an overwritten class is not measurable from this frontend's output; the ordered staging manifest makes the input set reproducible; no winner map exists | the owner states the same limitation in the same terms — **not superseded** |
| Per-module coverage, §7's subsection at lines 323-376 | 31 of 31 contributing modules covered, 26 unique-class witnesses, 5 named weaker witnesses, `pom.properties` fallback unavailable, `sql/connect/shims` without a verdict by instruction | the owner reaches the **same verdict** — 31 of 31, 26 + 5, fallback unavailable, `shims` excluded by instruction. Three of the five weaker witnesses are named differently by the owner: `common/network-shuffle` → `org.apache.spark.network.shuffle.AppsWithRecoveryDisabled`, `sql/api` → `org.apache.spark.sql.AnalysisException`, `sql/connect/common` → `org.apache.spark.sql.connect.Catalog`, where lines 368, 370 and 371 name `ShuffleSecretManager`, `FlatMapGroupsWithStateFunction` and `AddArtifactsRequest`. The per-witness `typeDecls`/`methods` columns are the 2026-08-24 load's measurements |
| Inline `**Graph**` block, lines 849-851 | input set "62 JARs from 31 modules… `-tests` and connect-shims JARs excluded by instruction"; overwrite warnings 31,598 over 26,221; **AST-creation failures 173** | input set and overwrite warnings as in the two rows above; AST-creation failures **429** by the current owner |

**Two figures outside §7 are LABELLED in place and named here rather than rewritten.**
Each attributes a measurement to a specific 2026-08-24 load or test, so replacing the
value would falsify the attribution rather than correct it; each therefore carries an
inline pointer to this section and its current owner's value below:

| Retained text | As this file states it | Current owner's value |
|---|---|---|
| §12 gate row 4, line 605 | "`importCpg` reports **1,397,339 / 119,691 / 45,037**; 31 of 31 contributing modules covered (26 unique-class witnesses, 5 named weaker witnesses)" | the counts are the superseded graph's; the graph on disk reports **1,396,899 / 119,721 / 45,037**. The coverage verdict is unchanged. `oss-scan-results/build-record.md` cites this line for the coverage verdict, not for the counts |
| Inline **Host** block, line 908, and the §1 Host table row at line 36 | "minimum memory this pipeline actually needed 64 GB heap, **peak RSS 59.0 GB**" | the heap is unchanged at 64 GB; the graph build's peak sampled RSS is **66.6 GB**, so the memory this pipeline needed is the higher figure |

## S.4 What this appendix does not re-anchor

Everything else in this record — the host, the toolchain and scanner versions, the
ruleset commits and rule counts, the vulnerability-feed timestamps, the pinned Spark
tree and its allowlist and scope expansion, the build outcome, the taint A/B, and the
per-tool exit/elapsed/finding figures — **remains the 2026-08-24 provisioning's and is
deliberately left as written.** Those fields are anchored by the request's own
expected-values table: where an observation differs from them, the correct outcome is to
record both values as a recorded difference, not to rewrite the record. Several of them
*do* differ after the 2026-08-30 re-provisioning — `provision-log/rulesets-feeds.txt`
records the Trivy vulnerability and java DB timestamps and the Dependency-Check NVD
`Last Modified` as **NEWER** than this file's, and the Datadog SAST ruleset as **53
rulesets / 1,147 rules**, sha256 `c5fd464c…`, against this file's 48 / 1,093 — and every
one of those differences belongs to that recorded-difference class and to the run records
that carry it, not to this finding.

The graph block was different in exactly one respect that made this correction the right
outcome rather than the wrong one: **its fields are unanchored by the expected-values
table**, so the only owner available for them is the write-time record beside the graph,
and a record that cites that owner while contradicting it can only mislead every reader
and every gate downstream of it.
