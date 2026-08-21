# harness/ENVIRONMENT.md — the environment record

This file records what the environment-setup run built, at what versions, with what feed state.
It is the **authority** for tool versions, ruleset identity, module JAR outcomes, the persisted
code-property graph, vulnerability-feed state and network reachability. It is written once, by the
setup run, and is **not to be edited** — a disagreement between this record and observed reality is
itself a reportable environment failure.

Recorded: `2026-08-21T04:45Z` · host: Ubuntu 25.10 x86_64, 4 CPU · setup log directory:
`/opt/blitzy-harness/logs/`

## 1. Entering the environment

A non-login shell does not read any profile, so nothing here is on `PATH` until the environment
file is sourced. **The environment file is `harness/env.sh`**, which exports the per-clone paths and
then sources the shared `/opt/blitzy-harness/env.sh`. Both are idempotent and install nothing.

```bash
. harness/env.sh          # from the repository root; then trivy, joern, semgrep … all resolve
```

Two layers, deliberately:

| Layer | Path | Shared? | Holds |
|---|---|---|---|
| shared root | `/opt/blitzy-harness` | shared by every clone on this host | the nine scanners, both JDKs, Maven, Scala, the harness venv, the pinned Spark clone, the persisted CPG, the pinned rulesets, the vulnerability feeds, `env.sh` |
| clone-local | `<repo>/harness` | one per clone | `env.sh`, `scope/allowlist.txt`, `bin/run-*.sh`, `lib/`, `cpg/spark.cpg` (symlink into the shared root), `artifacts/` |

`harness/artifacts/` is matched by the repository's own `.gitignore` (`artifacts/`), so it is never
committed and **a fresh clone does not have it**. Before invoking any runner in a new clone:

```bash
mkdir -p harness/artifacts/raw harness/artifacts/logs      # raw/ must exist and be EMPTY
```

Key exports (see `/opt/blitzy-harness/env.sh` for the full list): `SPARK_SRC`,
`SPARK_SRC_COMMIT`, `SPARK_SRC_COMMIT_DATE`, `JAVA_HOME` (17), `JAVA_HOME_21`, `MAVEN_HOME`,
`SCALA_HOME`, `JOERN_HOME`, `DEPENDENCY_CHECK_HOME`, `HARNESS_VENV`, `HARNESS_CPG`,
`OPENGREP_RULES_DIR`, `SEMGREP_RULES_DIR`, `TRIVY_CACHE_DIR`, `OSV_SCANNER_CACHE_DIR`,
`HARNESS_DC_DATA_DIR`, `HARNESS_RAW_DIR`, `HARNESS_LOG_DIR`, `HARNESS_SCOPE_FILE`,
`HARNESS_SMOKE_TARGET` (empty in normal operation).

## 2. The tree that is scanned

| | |
|---|---|
| `$SPARK_SRC` | `/opt/blitzy-harness/spark-src` |
| repository | `https://github.com/blitzy-public-samples/blitzy-spark` (only remote: `origin`) |
| commit | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` — checked out **by SHA**, detached HEAD, `git fetch --depth 1 origin <sha>` |
| `git rev-parse HEAD` | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` (verified equal to the pin) |
| `git log -1 --format=%cI` | `2025-10-23T15:31:06-04:00` = **`2025-10-23T19:31:06Z`** |
| Spark version | `4.1.0-SNAPSHOT` (`pom.xml` `project/version`) |
| pom agreement | `java.version 17`, `maven.version 3.9.11`, `scala.version 2.13.17` — all agree with the pinned toolchain, so no disagreement to report |
| divergence from `apache/spark` | **`identical`** (0 ahead, 0 behind, 0 files changed). Re-confirmed read-only via `GET https://api.github.com/repos/apache/spark/commits/59b8a448…` → HTTP 200, same SHA, same committer date, message `[SPARK-54001][SQL] Optimize memory usage in session cloning with ref-c…`. **No `apache` remote was added** and none is to be added. |

The tree also contains Maven build output under `*/target/` from §6. That output is part of the tree
as it now stands; the dependency scanners see those jars, and the allowlist (§3) decides scope.

## 3. Path scope

`harness/scope/allowlist.txt` holds the twelve authoritative globs verbatim, one per line, in the
order given by the setup instructions. They are used exactly as found and are never edited.

```
core/src/main/**
common/network-common/src/main/**
common/network-shuffle/src/main/**
common/network-yarn/src/main/**
sql/catalyst/src/main/**
sql/core/src/main/**
sql/connect/**/src/main/**
sql/hive/src/main/**
sql/hive-thriftserver/src/main/**
resource-managers/kubernetes/**/src/main/**
resource-managers/yarn/src/main/**
python/pyspark/**
```

Three of these carry a **mid-path `**`** (`sql/connect/**/src/main/**`,
`resource-managers/kubernetes/**/src/main/**`) or match a whole subtree (`python/pyspark/**`). A
consumer matching them must give `/**/` zero-or-more-directories semantics; Python's `fnmatch` and
`PurePath.match` do not, and getting it wrong drops whole modules silently.

Under the pinned tree they expand to **18 existing directories** (helper: `harness/lib/scope.sh`,
`harness_scope_dirs`): `common/network-common/src/main`, `common/network-shuffle/src/main`,
`common/network-yarn/src/main`, `core/src/main`, `python/pyspark`,
`resource-managers/kubernetes/core/src/main`, `resource-managers/kubernetes/core/volcano/src/main`,
`resource-managers/kubernetes/docker/src/main`, `resource-managers/yarn/src/main`,
`sql/catalyst/src/main`, `sql/connect/client/jdbc/src/main`, `sql/connect/client/jvm/src/main`,
`sql/connect/common/src/main`, `sql/connect/server/src/main`, `sql/connect/shims/src/main`,
`sql/core/src/main`, `sql/hive-thriftserver/src/main`, `sql/hive/src/main`.

Scale, measured (not a scan): 4,095 files in scope, of which 3,952 are `.scala`/`.java`/`.py`;
89,273 files in the whole tree including build output.

`src/test/**` is out of scope: the expansion never returns a `src/test` directory and every runner
additionally passes its own tool-level test exclusion (§8).

## 4. Runtimes and the nine tools

Runtimes (tarball installs under `/opt/blitzy-harness/tools`, no apt packages replaced):

| Runtime | Version | Notes |
|---|---|---|
| Temurin JDK 17 | `17.0.20+8` | `JAVA_HOME`; the Spark build toolchain the pin requires |
| Temurin JDK 21 | `21.0.12.1+1` | `JAVA_HOME_21`; **required by Joern 4.x**, whose README states "JDK 21". Additive, not a substitution — Spark still builds on 17 |
| Apache Maven | `3.9.11` | matches `<maven.version>`, so `./build/mvn` uses it and downloads nothing |
| Scala | `2.13.17` | `scala`/`scalac` both report 2.13.17 |
| Python venv | `3.13.7`, pip `26.2.1` | `/opt/blitzy-harness/venv`; created with `--without-pip` + `get-pip.py` because the base image has no `ensurepip` |

The nine tools. Every one resolved on `PATH` and printed its version from a fresh non-login shell
after sourcing the environment file (evidence: `harness/artifacts/smoke/versions.txt`).

| Tool | Resolved version | Install method |
|---|---|---|
| Opengrep | `1.27.1` | GitHub release binary `opengrep_manylinux_x86` → `tools/opengrep-1.27.1`, shim in `tools/bin` |
| Semgrep CE | `1.174.0` | `pip install semgrep==1.174.0` into the harness venv, shim in `tools/bin` |
| Joern | `4.0.607` | GitHub release `joern-cli-linux-x86_64.zip` → `tools/joern-cli` (runs on JDK 21) |
| datadog-static-analyzer | `0.9.1` (rev `f76636e43554f7f9a8e3984a31d03ec8dea5489f`) | GitHub release `datadog-static-analyzer-x86_64-unknown-linux-gnu.zip` |
| Gitleaks | `8.30.1` | GitHub release `gitleaks_8.30.1_linux_x64.tar.gz` |
| Checkov | `3.3.13` | `pip install checkov==3.3.13` into the harness venv |
| Trivy | `0.74.0` | GitHub release `trivy_0.74.0_Linux-64bit.tar.gz` |
| OSV-Scanner | `2.5.1` (osv-scalibr `0.5.2`) | GitHub release binary `osv-scanner_linux_amd64` |
| OWASP Dependency-Check | `13.0.0` | Maven Central `org/owasp/dependency-check-cli/13.0.0/dependency-check-cli-13.0.0-release.zip`. The project's GitHub "latest" release is still `12.1.0`; Maven Central carries `13.0.0`, which is what is installed |

`joern` has no `--version` flag: probe it as `printf '' | joern` and read the `Version:` banner line.
Without a closed stdin it drops into an interactive REPL and appears to hang.

### CodeQL is excluded deliberately

CodeQL is **not** installed and is not one of the nine, because it does not support Scala at all.
GitHub's own documentation states that CodeQL does not support languages outside its published list
and that this "includes, but is not limited to, PHP, Scala", warning that using it on them may
produce no alerts and incomplete analysis —
<https://docs.github.com/en/code-security/concepts/code-scanning/codeql/codeql-code-scanning>
(canonical language list: <https://codeql.github.com/docs/codeql-overview/supported-languages-and-frameworks/>),
retrieved 2026-08-21. Scala appears nowhere in that list. Its absence here is a decision, not an oversight.

## 5. Rulesets, taint configuration and credential-gated paths

### Opengrep taint: ENABLED

Baked into `harness/bin/run-opengrep.sh`: **`--taint-intrafile --dataflow-traces`**.

`opengrep scan --help` (1.27.1) documents `--taint-intrafile` as "Enable intra-file
inter-procedural taint analysis" with supported languages "Apex, C, Clojure, C#, C++, Dart, Elixir,
Go, Java, JavaScript, Julia, Kotlin, Lua, Python, Ruby, Rust, **Scala**, Swift, TypeScript, Visual
Basic". Scala is therefore covered by the engine that is enabled — recorded from the installed
tool's own help output rather than from external documentation.

Not used, with reasons: `--pro` and `--pro-path-sensitive` ("Requires Semgrep Pro Engine" — not
open source); `--guarded-taint-signatures` (requires `--experimental`). Opengrep 1.27.1 has no
`--metrics` flag and no telemetry to disable.

Semgrep CE runs with **no taint flag** on purpose: it is the control arm, and its lack of a
cross-function taint engine is part of what the dataset is meant to show.

### Ruleset identity

| Consumer | Ruleset | Identity | Sets selected |
|---|---|---|---|
| Opengrep | `/opt/blitzy-harness/rules/opengrep-rules` (`github.com/opengrep/opengrep-rules`) | commit `f1d2b562b414783763fd02a6ed2736eaed622efa`, 2031 rule files (scala 27, java 123, python 334) | `scala`, `java`, `python`, `generic/secrets` |
| Semgrep CE | `/opt/blitzy-harness/rules/semgrep-rules` (`github.com/semgrep/semgrep-rules`) | commit `40b8c63f75dc7c22c8a77482d73bfb864b146f7e` (scala 27) | `scala`, `java`, `python`, `generic/secrets` |
| datadog-static-analyzer | its own bundled rules | 1093 static-analysis rules, languages `dart, javascript, bash, c#, php, ruby, rust, java, kotlin, go, typescript, python` — **no Scala rules** | all bundled |
| Checkov | bundled policies, `--skip-download` | frameworks `kubernetes, dockerfile, yaml, json, helm, kustomize` | `secrets` deliberately not enabled |
| Gitleaks | its default rule set | — | — |
| Joern | `harness/lib/joern-baked-queries.sc` | five baked queries, named with their smoke counts in §12 | — |

Both SAST runners `cd` into their ruleset root and pass **relative** `--config` paths, so rule ids
come out canonical (`scala.lang.security.audit.…`, `java.…`, `python.…`) and the two tools' ids are
directly comparable. Local pinned clones are used rather than registry packs so that two runs cannot
differ because a registry changed; measured effect on the smoke target: opengrep ran 337 rules,
semgrep 338 rules, both over the same 51 files.

### datadog-static-analyzer AI path: UNAVAILABLE

The Datadog-backed path (`--enable-secrets`, documented as "Limited Availability feature. Requires
using Datadog API keys") is **not available in this environment**. Credential source: the
environment variables **`DD_API_KEY`** and **`DD_APP_KEY`** — both absent here; no value is recorded
anywhere in this harness, by name only. `https://api.datadoghq.com/api/v1/validate` returns 403
unauthenticated. The runner therefore passes `--enable-secrets false` and uses the bundled rules; it
switches on only if `HARNESS_DD_SECRETS=1` **and** both variables are genuinely set.

## 6. The Maven build and per-module JAR outcomes

```bash
cd "$SPARK_SRC"
JAVA_HOME=$JAVA_HOME_17 MAVEN_OPTS="-Xss128m -Xmx24g -XX:ReservedCodeCacheSize=2g" \
./build/mvn -B -DskipTests -Pyarn -Pkubernetes -Phive -Phive-thriftserver \
  -pl core,common/network-common,common/network-shuffle,common/network-yarn,sql/catalyst,sql/core,\
sql/hive,sql/hive-thriftserver,sql/connect/shims,sql/connect/common,sql/connect/server,\
sql/connect/client/jvm,sql/connect/client/jdbc,resource-managers/yarn,resource-managers/kubernetes/core -am package
```

**Result: BUILD SUCCESS** — 33 reactor modules SUCCESS, 0 FAILURE, 0 SKIPPED, 22 min 35 s, finished
`2026-08-21T03:46:45Z`. Full log: `/opt/blitzy-harness/logs/spark-build.log`. No source file was
modified and there were no compilation errors.

**Every in-scope module produced a JAR. No module is missing.**

| Scope root | Maven module | Profile needed | JAR |
|---|---|---|---|
| `core/src/main/**` | `core` | — | yes |
| `common/network-common/src/main/**` | `common/network-common` | — | yes |
| `common/network-shuffle/src/main/**` | `common/network-shuffle` | — | yes |
| `common/network-yarn/src/main/**` | `common/network-yarn` | `-Pyarn` | yes |
| `sql/catalyst/src/main/**` | `sql/catalyst` | — | yes |
| `sql/core/src/main/**` | `sql/core` | — | yes |
| `sql/connect/**/src/main/**` | `sql/connect/{shims,common,server,client/jvm,client/jdbc}` | — | yes (all five) |
| `sql/hive/src/main/**` | `sql/hive` | in the default reactor of this pin; `-Phive` passed anyway | yes |
| `sql/hive-thriftserver/src/main/**` | `sql/hive-thriftserver` | `-Phive-thriftserver` | yes |
| `resource-managers/kubernetes/**/src/main/**` | `resource-managers/kubernetes/core` | `-Pkubernetes` | yes |
| `resource-managers/yarn/src/main/**` | `resource-managers/yarn` | `-Pyarn` | yes |
| `python/pyspark/**` | — (Python) | — | n/a — produces no JAR by nature |

Seventeen `-am` dependency modules also produced JARs, and their bytecode is in the graph too:
`common/kvstore`, `common/sketch`, `common/tags`, `common/unsafe`, `common/utils`,
`common/utils-java`, `common/variant`, `connector/avro`, `connector/protobuf`, `graphx`, `launcher`,
`mllib`, `mllib-local`, `repl`, `sql/api`, `sql/pipelines`, `streaming`. Fifteen in-scope Maven
modules plus these seventeen is the 32-jar CPG input of §7.

## 7. The persisted code-property graph

| | |
|---|---|
| canonical graph | `/opt/blitzy-harness/cpg/spark.cpg` — also `harness/cpg/spark.cpg` (symlink) |
| size / sha256 | 509,171,114 B · `16c40508128a148e20894aab3a1e5f082aa8ce05fec4f07869445bd5fbd931e7` |
| **methods** | **445,567** (> 0, verified by loading it) |
| type declarations | 57,863 (54,053 under `org.apache.spark`) · calls 3,693,199 · files 19,500 |
| raw frontend output | `/opt/blitzy-harness/cpg/spark.raw.cpg`, 206,816,106 B, sha256 `889515f618644c35d569c92ec215e63d624fb7562bf979146ab0ec2cc313faf5` |
| build command | `jimple2cpg --recurse --depth 1 -o …/spark.cpg /opt/blitzy-harness/cpg/jars` on JDK 21, `JAVA_OPTS=-Xmx128g -Xss64m`, ~28 min. Log `/opt/blitzy-harness/logs/cpg-build.log` |
| inputs | **32 module jars, 19,443 classes**, staged in `/opt/blitzy-harness/cpg/jars`; inventory `/opt/blitzy-harness/cpg/jar-inventory.json` |

Joern's JVM-bytecode frontend ingests Scala-compiled jars without difficulty, so the capability the
setup instructions asked about is present.

One jar per module. For the 31 modules that emit both a shaded artifact and a pre-shade
`original-*.jar`, the `original-*.jar` was used: it carries only that module's own classes, so no
relocated third-party bytecode enters the graph and no class is analysed twice.
`common/network-yarn` emits no `original-` jar, so its shaded 6-class jar was used.

The canonical graph is the **overlay-applied** graph: `importCpg` on the raw output applies Joern's
default overlays (including `ReachingDefPass`, ~125 s) and that result was promoted. Re-importing the
canonical graph takes about 20 s and does **not** re-run those passes. `importCpg` does not modify the
file it loads — sha256 identical before and after — so concurrent clones may import it simultaneously.

**Per-module bytecode coverage** (`/opt/blitzy-harness/logs/cpg-coverage2.log`, witnesses in
`/opt/blitzy-harness/cpg/module-witness-classes.json`): for each module, up to three classes owned
**only** by that module were checked as `TYPE_DECL` full names — 31 of 31 modules present, 0 absent.

> `sql/connect/shims` owns **no class exclusively**: its eleven classes (`SparkConf`, `SparkContext`,
> `RDD`, `JavaRDD`, `QueryExecution`, `SessionState`, `SharedState`, `BaseRelation`,
> `ExperimentalMethods`, `SparkSessionExtensions`, `ExecutionListenerManager`) are also shipped by
> `core` and the SQL modules. A strictly injective per-module test cannot evaluate it. Its bytecode
> **is** in the graph — three sampled classes present, 3/3 — via those other jars. Treat shims as
> covered on this evidence rather than as "not evaluable".

Frontend limitation: 2 of 19,443 classes failed AST creation (a `ConcurrentModificationException` in
jimple2cpg's `AstForTypeDeclsCreator`, logged as WARN) — 0.01% of classes, non-fatal:

```
org/apache/spark/sql/execution/streaming/operators/stateful/flatmapgroupswithstate/FlatMapGroupsWithStateExecBase.class
org/apache/spark/sql/execution/InputRDDCodegen.class
```

Facts about the graph that a query author needs (capability facts, no judgement about Spark):

* deploy-package RPC entry points: `receive` 8 methods, `receiveAndReply` 6 methods.
  `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
  is present at line 408.
* Sinks: `org.apache.spark.deploy.worker.DriverRunner.<init>` present with its full signature;
  `DriverRunner` `TYPE_DECL` present; `java.lang.ProcessBuilder.start` call sites: 19.
* **Scala name mangling matters.** There is no method named `createDriver`; `Master`'s
  `private def createDriver` (source `Master.scala:1356`, called at 417 and 1130) appears as
  `org$apache$spark$deploy$master$Master$$createDriver`. `Master` has 312 methods in the graph.
* `org.apache.spark.SecurityManager` exposes `aclsEnabled`, `checkAdminPermissions`,
  `checkModifyPermissions`, `checkUIViewPermissions`, `isAuthenticationEnabled` — and also
  `isEncryptionEnabled`, `isSslRpcEnabled`.
* **Paths.** `cpg.method.file.name` is the CPG-build-time class path, e.g.
  `/tmp/jimple2cpg-<id>/org/apache/spark/deploy/master/Master.class` — *not* a `$SPARK_SRC`-relative
  source path. `harness/bin/run-joern.sh` maps it back to source with `harness/lib/joern_collect.py`
  (67/67 rows resolved on the smoke run).

## 8. The nine runners

`harness/bin/` holds exactly the nine `run-<tool>.sh` runners plus `run-all.sh`. Non-runner helpers
live in `harness/lib/` (`scope.sh`, `joern-baked-queries.sc`, `joern_collect.py`, `smoke_verify.py`),
so nothing in `bin/` is anything other than a runner or `run-all.sh`.

Uniform contract, identical across all nine:

* **Takes no arguments.** Any argument is a usage error, `exit 64`.
* Resolves its own directory, sources `harness/env.sh` and `harness/lib/scope.sh` — no reliance on
  the caller's environment beyond `BLITZY_HARNESS_ROOT` if it is overridden.
* Prints a banner (tool, scan_root, raw_dir, allowlist, started_at) and a trailer (exit_code,
  elapsed_seconds, artifact bytes, finished_at) on stdout.
* Scans `$SPARK_SRC`, excludes `src/test`, writes exactly one artifact into
  `$HARNESS_RAW_DIR` (`harness/artifacts/raw/`), and **exits with the tool's own exit code** — a
  non-zero exit is a tool condition to record, not a harness failure.

| Runner | Artifact | Baked invocation (abridged; see the script for the full text) |
|---|---|---|
| `run-trivy.sh` | `raw/trivy.json` | `trivy fs --cache-dir "$TRIVY_CACHE_DIR" --scanners vuln,secret,misconfig --skip-db-update --skip-java-db-update --skip-dirs '**/src/test' --skip-dirs '**/src/test/**' --format json --output … --no-progress <whole tree>`. `HARNESS_TRIVY_UPDATE=1` re-enables DB refresh |
| `run-osv-scanner.sh` | `raw/osv-scanner.json` | `osv-scanner scan source --recursive --allow-no-lockfiles --format json --output … --verbosity info <whole tree>` |
| `run-dependency-check.sh` | `raw/dependency-check.json` | `dependency-check.sh --project spark-pinned-<sha12> --scan <tree> --exclude '**/src/test/**' --data "$HARNESS_DC_DATA_DIR" --noupdate --format JSON --prettyPrint --out <tmpdir>`, then renames `dependency-check-report.json` into place. Forces `JAVA_HOME=$JAVA_HOME_21`. `HARNESS_DC_UPDATE=1` switches to the NVD datafeed route of §9 |
| `run-gitleaks.sh` | `raw/gitleaks.json` | `gitleaks dir --no-banner --redact --report-format json --report-path … --log-level info <18 scope dirs>` |
| `run-checkov.sh` | `raw/checkov.json` | `checkov -d <each scope dir> --framework kubernetes,dockerfile,yaml,json,helm,kustomize --skip-path '.*/src/test/.*' --skip-download --compact --quiet --output json --output-file-path <tmpdir>`, then renames `results_json.json` |
| `run-opengrep.sh` | `raw/opengrep.sarif` | `cd "$OPENGREP_RULES_DIR"` then `opengrep scan --taint-intrafile --dataflow-traces --config scala --config java --config python --config generic/secrets --exclude 'src/test' --timeout 0 --sarif-output=… <18 scope dirs>` |
| `run-semgrep.sh` | `raw/semgrep.sarif` | `cd "$SEMGREP_RULES_DIR"` then `semgrep scan --metrics=off --disable-version-check --config scala --config java --config python --config generic/secrets --exclude 'src/test' --timeout 0 --sarif --output … <18 scope dirs>` |
| `run-datadog-static-analyzer.sh` | `raw/datadog-static-analyzer.sarif` | `datadog-static-analyzer -i <tree> -u <each scope dir> -p '**/src/test/**' --enable-static-analysis true --enable-secrets false -f sarif -o …` |
| `run-joern.sh` | `raw/joern.json` | `JAVA_HOME=$JAVA_HOME_21`, `JAVA_OPTS=${JAVA_OPTS:--Xmx48g}`, private `mktemp -d` workspace, `joern --script lib/joern-baked-queries.sc --param cpgPath=$HARNESS_CPG`, then `joern_collect.py <log> <artifact> $SPARK_SRC`. Loads the graph with **`importCpg`**; `importCode` appears nowhere |

`--timeout 0` in both SAST runners disables the default 5-second per-rule-per-file limit. That is a
determinism choice: with the default, a slow rule can time out on one run and not the next, and the
finding count would differ between two runs of the same configuration. Measured cost: 29 s for 51
files, so the full 4,095-file scope is roughly 40 minutes worst case.

Every runner is executable, and all twelve shell scripts pass `bash -n`; `joern_collect.py` and
`smoke_verify.py` pass `py_compile`.

`harness/bin/run-all.sh` exists for a human convenience only. **The scanning run must not invoke
it** — it invokes the nine in sequence, which makes exit code and elapsed time non-attributable.

`HARNESS_SMOKE_TARGET` is **empty in normal operation** and must stay that way for a real scan. Set
to a path relative to `$SPARK_SRC` it collapses the scan root to that one directory, which is how
§11's smoke evidence was produced.

## 9. Vulnerability-feed state, as found

Three tools consume a vulnerability feed. All three were populated at setup time and **all three
runners run offline by default**, so a feed cannot silently change between runs.

| Tool | Feed | State | Update outcome |
|---|---|---|---|
| Trivy | `trivy.db` (1.31 GB) from `mirror.gcr.io/aquasec/trivy-db:2`, cache `/opt/blitzy-harness/caches/trivy` | `{"Version":2,"UpdatedAt":"2026-08-21T01:31:14Z","NextUpdate":"2026-08-22T01:31:14Z","DownloadedAt":"2026-08-21T03:07:51Z"}` | **succeeded** at setup; the runner then passes `--skip-db-update` |
| Trivy Java DB | `trivy-java-db:1` (909 MB) | `{"Version":1,"UpdatedAt":"2026-08-21T01:03:49Z","NextUpdate":"2026-08-24T01:03:49Z","DownloadedAt":"2026-08-21T03:08:08Z"}` | **succeeded**; runner passes `--skip-java-db-update` |
| OSV-Scanner | live OSV API (`api.osv.dev`) plus `api.deps.dev`; local cache `$OSV_SCANNER_CACHE_DIR` | no offline database — queries at scan time | **not applicable** (online lookup, both endpoints 200) |
| OWASP Dependency-Check | H2 database `odc.mv.db`, 237 MB, in `$HARNESS_DC_DATA_DIR` | in-report `dataSources`: NVD API Last Checked `2026-08-21T03:07:24Z`, NVD API Last Modified `2026-08-20T20:00:06-04`, NVD Cache Last Checked `2026-08-21T03:07:24Z`, NVD Cache Last Modified `2026-08-20T20:00:06-04`. Plus CISA KEV, retireJS `jsrepository.json`, `publishedSuppressions.xml` | **succeeded**; runner then passes `--noupdate` |

**How the NVD data was obtained without an API key** — worth recording, because the obvious route
fails. Dependency-Check 13.0.0 refuses keyless use of the NVD *API*:
`NvdApiException: Invalid API Key, length of 0 too short to provided a masked partial key`. The
`jeremylong.github.io/vulnz` hosted mirror does not exist (every candidate URL 404s; that project
documents building your own cache). The route that works is the official NVD JSON 2.0 **datafeeds**,
via an option visible only under `--advancedHelp`:

```bash
dependency-check.sh --updateonly --data "$HARNESS_DC_DATA_DIR" \
  --nvdDatafeed "https://nvd.nist.gov/feeds/json/cve/2.0/nvdcve-2.0-{0}.json.gz"
```

That ran for 858,979 ms (~14.3 min) over the yearly feeds 2002–2026 plus `modified`, updated the CPE
ecosystem on 153,787 records, removed it on 5,251, cleaned 44 orphaned records and defragmented.
Log: `/opt/blitzy-harness/logs/dc-nvd-update.log`. **`NVD_API_KEY` is absent** and was not needed;
supply it only if a future run wants API-based incremental updates.

Each feed was proved through its consumer rather than by probing a URL. On a synthetic Maven target
(`/opt/blitzy-harness/feed-probe` — log4j-core 2.14.1, commons-collections 3.2.1, guava 24.1.1-jre,
deliberately outside the allowlist, so not a scan of scope): Trivy returned 12 vulnerabilities
including CVE-2021-44228; OSV-Scanner returned 12 (exit 1 = vulnerabilities found);
Dependency-Check returned 9 on the jar including CVE-2021-44228 and CVE-2021-45046.

**Dependency-Check does not expand `pom.xml` declarations** — on that probe it found exactly one
dependency, the jar on disk. That is why its runner scans the whole tree including `*/target/`,
where §6's build output supplies real jars.

**Commit-date caveat.** The pinned tree is dated `2025-10-23T19:31:06Z` while the feeds are current
to 2026-08-21. A dependency tree of that vintage will show CVEs upstream has since moved past.
Counts are what they are; nothing here corrects, adjusts or annotates them.

## 10. Network egress, per endpoint

Measured 2026-08-21T04:41:27Z; HTTP status after redirects. Full file, with the consumer of each
endpoint: `harness/artifacts/smoke/network-endpoints.txt`.

| Status | Endpoint | Needed by |
|---|---|---|
| 200 | `https://github.com` | release downloads: opengrep, joern, gitleaks, trivy, osv-scanner, datadog |
| 200 | `https://pypi.org/simple/semgrep/` | semgrep CE, checkov |
| 200 | `https://repo1.maven.org/maven2/` | Spark Maven build; dependency-check-cli distribution |
| 200 | `https://nvd.nist.gov/feeds/json/cve/2.0/nvdcve-2.0-2026.meta` | dependency-check NVD datafeed — **the route used**, no API key |
| 200 | `https://services.nvd.nist.gov/rest/json/cves/2.0` | dependency-check NVD API — reachable but **API-key gated** in DC 13.0.0 |
| 200 | `https://www.cisa.gov/…/known_exploited_vulnerabilities.json` | dependency-check CISA KEV |
| 200 | `https://api.osv.dev/v1/vulns/GHSA-jfh8-c2jp-5v3q` | osv-scanner |
| 200 | `https://api.deps.dev/v3alpha/…/org.apache.spark%3Aspark-core_2.13` | osv-scanner default data source |
| **401** | `https://mirror.gcr.io/v2/` | trivy DB mirror — 401 anonymous is **expected**; trivy's own token flow works and the DB downloaded |
| **401** | `https://ghcr.io/v2/` | trivy DB origin — 401 anonymous is **expected** |
| 200 | `https://raw.githubusercontent.com/opengrep/opengrep-rules/main/README.md` | ruleset clones |
| 200 | `https://semgrep.dev/api/registry/rules` | semgrep registry — reachable but **deliberately unused** (pinned local clones) |
| **403** | `https://api.datadoghq.com/api/v1/validate` | datadog AI/secrets path — 403 unauthenticated, `DD_API_KEY`/`DD_APP_KEY` absent |
| 200 | `https://api.adoptium.net/v3/info/available_releases` | JDK 17, JDK 21 |
| 200 | `https://archive.apache.org/dist/maven/maven-3/3.9.11/…tar.gz` | Maven 3.9.11 |

Only the datadog 403 corresponds to a capability that is unavailable (§5). The two 401s are the
normal anonymous response of a container registry and did not impede the download they gate.

## 11. Proof that the harness works

Evidence tree: `harness/artifacts/smoke/` (also copied to `/opt/blitzy-harness/artifacts/smoke`,
7.0 MB, so it survives independently of any clone) — `versions.txt`, `network-endpoints.txt`,
`cpg-load.txt`, `verify.txt`, and `pass1/`, `pass2/` each holding `raw/` and `logs/`
(`<tool>.stdout.log`, `<tool>.stderr.log`, `<tool>.meta.json` with exit code, elapsed seconds and
timestamps). Smoke target: `common/network-shuffle/src/main`, 51 files, 464 KB.

1. **Every tool resolves on `PATH` and prints a version** from a fresh `bash --noprofile --norc`
   after sourcing the environment file — §4, `versions.txt`.
2. **Every runner completes and emits a parseable artifact.** All nine exited 0 on both passes.
3. **Two consecutive runs produce identical finding counts** (`harness/lib/smoke_verify.py`, using
   each tool's documented record locator):

   | tool | format | pass 1 | pass 2 | identical |
   |---|---|---|---|---|
   | trivy | JSON | 0 | 0 | yes |
   | osv-scanner | JSON | 0 | 0 | yes |
   | dependency-check | JSON | 0 | 0 | yes |
   | gitleaks | JSON array | 0 | 0 | yes |
   | checkov | JSON | 0 | 0 | yes |
   | opengrep | SARIF 2.1.0 | 0 | 0 | yes |
   | semgrep | SARIF 2.1.0 | 0 | 0 | yes |
   | joern | JSON | 67 | 67 | yes |
   | datadog-static-analyzer | SARIF 2.1.0 | 83 | 83 | yes |

   `SMOKE_VERIFY: PASS`. The zeros are genuine analysis of the target, not skipped work: opengrep
   reported "Ran 337 rules on 51 files: 0 findings"; semgrep "Rules run: 338 / Targets scanned: 51 /
   Parsed lines: ~100.0%"; datadog returned 83 results on the same files; joern loaded 445,567
   methods. Engine drivers in the SARIF: `Opengrep OSS 1.27.1`, `Semgrep OSS 1.174.0`.
4. **Network egress per endpoint** — §10.
5. **The persisted CPG loads and reports > 0 methods** — 445,567, both through `run-joern.sh` and
   through an independent coverage script (`COV_METHODS=445567`,
   `COV_SUMMARY modules_with_bytecode=31 modules_without=0`, `COV_SHIMS_present=3/3`). Details and
   sha256s: `harness/artifacts/smoke/cpg-load.txt`.

`harness/artifacts/raw/` is left **created and empty** (0 entries).

A third directory, `pass3/`, holds a post-lint re-verification of the two runners whose files were
touched by shell/Python linting after pass 2 (`dependency-check`, `joern`). Both reproduced their
pass-1 and pass-2 results exactly — dependency-check exit 0, joern 67 findings / 445,567 methods /
67 of 67 paths resolved — so the lint fixes are behaviour-neutral. Determinism is still the pass-1
versus pass-2 comparison; pass 3 is corroboration, not a third data point.

## 12. Artifact shapes and quirks a consumer needs to know

Recorded because each one silently corrupts a naive parse.

* **Checkov's top level changes shape with content.** With findings across several frameworks it is
  a JSON **array** of `{check_type, results:{failed_checks:[…]}}`; with nothing to report it is a
  single JSON **object**. Handle both. `file_path` is scan-root-relative **with a leading slash**
  (not filesystem-absolute — reading it as absolute yields a wrong path and a false out-of-scope
  verdict); `file_abs_path` is the absolute one; `severity` is `null` per row in the OSS build.
* **datadog SARIF `uri`s are relative to its `-i` root**, e.g.
  `java/org/apache/spark/network/shuffle/OneForOneBlockPusher.java`.
* **Gitleaks runs with `--redact`**, so matched secret material is redacted in its own artifact. Its
  `Description` is the rule description; `Secret`/`Match` are not the description.
* **Opengrep and semgrep both report "Scan was limited to files tracked by git."** Untracked files
  under a scope directory are skipped by default by both engines.
* **`run-joern.sh`'s artifact is this harness's own shape**, not a tool-native format:
  `{tool, cpg_path, generated_at, cpg_methods, cpg_typedecls, source_index_size,
  declaration_index_size, queries[], findings[]}`, each finding carrying `rule_id, message, path,
  start_line, method_full_name, class_file, path_resolution`. `path_resolution` is one of
  `source-index-filename`, `source-index-declaration`, `unresolved-bytecode-only`. Its five baked
  queries and their smoke counts: `process-launch-site=19`, `java-deserialization-site=8`,
  `reflective-class-load=40`, `weak-hash-algorithm=0`,
  `rpc-handler-reaches-process-launch=0`. It emits no severity.
* **Trivy** distinguishes its three classes structurally, by which of `Results[].Vulnerabilities[]`,
  `Results[].Secrets[]`, `Results[].Misconfigurations[]` a record came from.
* Artifact extensions follow the format each runner emits: `.sarif` for opengrep, semgrep and
  datadog; `.json` for the other six.

## 13. Running with more than one clone on this host

The shared root is read-mostly and safe to share; four things are not, and each has a per-clone
value. `<i>` is the clone index, a 0-based integer.

| Resource | Why it cannot be shared | Per-clone value |
|---|---|---|
| Dependency-Check H2 database | H2 takes no concurrent writers | `HARNESS_DC_DATA_DIR=/opt/blitzy-harness/dependency-check/data-<i>` — **already created** for `<i>` in 0,1,2 (237 MB each), plus `…/data` as the pristine original |
| Joern workspace | a workspace holds one project; a second import corrupts a shared one | already private: `run-joern.sh` makes a `mktemp -d` per invocation. Never point two clones at one workspace |
| Trivy cache | concurrent DB writers | `TRIVY_CACHE_DIR=/tmp/blitzy-trivy-<i>` if a clone sets `HARNESS_TRIVY_UPDATE=1`; the default read-only shared cache is fine with `--skip-db-update` |
| Artifact trees | two clones would overwrite one another's findings | clone-local by construction: `harness/artifacts/{raw,logs}` inside each checkout. `mkdir -p` them per clone |

Safe to share, unchanged: `/opt/blitzy-harness/tools`, `rules`, `venv`, `spark-src`,
`cpg/spark.cpg` (`importCpg` is read-only on the file — sha256 identical after a load), `env.sh`,
`caches/trivy` read-only, `artifacts/smoke`.

**Nothing in `/opt/blitzy-harness` is to be deleted, reset, re-downloaded or rebuilt**, and no
service needs starting or stopping — this harness binds no port. Removing the CPG, the DC database
or a ruleset clone costs another run 14–30 minutes each and changes the ruleset commit the dataset
was produced under.

## 14. What this environment deliberately does not contain

* **No findings, report or normalized dataset.** Producing those is the scanning run's job. The only
  tool output here is `harness/artifacts/smoke/`, which is proof-of-life on a 51-file target and is
  **never a fallback** for a runner that fails on the real scope.
* **No CodeQL** — §4.
* **No CI integration.** Nothing was added to `.github/workflows/`, and no scheduled job re-runs any
  of this.
* **No modification to the pinned tree, to any scanner's configuration, or to any repository source
  file.** The Spark checkout carries §6's build output under `*/target/` and nothing else added.
* **No credential value.** No file or log written by the setup run contains a secret value; the two
  credential-gated capabilities (§5, §9) are recorded by variable **name** only — `DD_API_KEY`,
  `DD_APP_KEY`, `NVD_API_KEY`.
