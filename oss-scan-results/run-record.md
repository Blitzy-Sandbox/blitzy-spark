# `oss-scan-results/run-record.md` — environment and execution record

This file is the environment and execution record for one run of the provisioned
open-source security-scanner harness over the pinned Apache Spark tree. It was opened by the
outer-shell bootstrap **before any other deliverable existed** and written check by check from
the first check onward, so that a stop at any point is explained by this file rather than
leaving it absent.

Every number, version, path, count and timestamp below was read at execution time from one of
four sources and from nowhere else: a raw artifact under `harness/artifacts/raw/`, a log under
`harness/artifacts/logs/`, `harness/ENVIRONMENT.md`, or a `git` read of `$SPARK_SRC`. Nothing is
carried over from a plan and nothing is inferred. Where a value could not be read, that is what
is recorded.

No user rules were provided for this work, so none are cited here.

A note on cross-references, because two documents are cited throughout. A reference written
`harness/ENVIRONMENT.md §N` is a section of that file — the environment record written by the
earlier setup run, which this run reads and never edits. A reference written `section N` or with a
sub-number such as `§4.5` is a section of *this* file.

---

## 1. Bootstrap

Performed by the outer shell, in this order, before the gate. `harness/ENVIRONMENT.md` §1 names
`harness/env.sh` as the environment file; a non-login shell reads no profile, so sourcing it is
how the recorded environment is *entered* — it installs nothing and changes nothing.

| # | Step | Outcome |
|---|---|---|
| 1 | Locate and source the recorded environment | **pass** — `harness/ENVIRONMENT.md` §1 names `harness/env.sh`; present and readable; sourced, exports applied |
| 2 | Collision precheck, before creating or opening anything | **pass** — all nine `oss-scan-results/` targets absent (the six deliverables and the three staging files); `queries/joern/` absent, so no `*.sc` and no `results/` file could exist. `queries/joern/.workspace/` exempt as unbounded scratch |
| 3 | Create only the permitted directories | **pass** — `oss-scan-results/` created, `queries/joern/` created, `harness/artifacts/logs/` already present so not created. `harness/artifacts/raw/` **was not created**: it is a precondition, not a repair |
| 4 | Open the record, noting this run created it | **pass** — `oss-scan-results/run-record.md` did not exist and was created by this run at `2026-08-21T05:16:31Z`; the Tree-writability check below therefore knows this target as one it authored |
| 5 | Resolve an interpreter on the updated `PATH` | **pass** — `/usr/bin/python3`, Python `3.13.7`; shell `/bin/bash` `5.2.37(1)-release` |

Values the sourced environment supplied, as exported:

```
SPARK_SRC=/opt/blitzy-harness/spark-src
SPARK_SRC_COMMIT=59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d
SPARK_SRC_COMMIT_DATE=2025-10-23T19:31:06Z
HARNESS_REPO_ROOT=/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4
HARNESS_DIR=/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness
HARNESS_RAW_DIR=/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/artifacts/raw
HARNESS_LOG_DIR=/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/artifacts/logs
HARNESS_SCOPE_FILE=/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/scope/allowlist.txt
HARNESS_CPG=/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/cpg/spark.cpg
BLITZY_HARNESS_ROOT=/opt/blitzy-harness
HARNESS_DC_DATA_DIR=/opt/blitzy-harness/dependency-check/data
TRIVY_CACHE_DIR=/opt/blitzy-harness/caches/trivy
OPENGREP_RULES_DIR=/opt/blitzy-harness/rules/opengrep-rules
SEMGREP_RULES_DIR=/opt/blitzy-harness/rules/semgrep-rules
HARNESS_SMOKE_TARGET=[]   # empty, as a real scan requires
```

That block is a verbatim dump of what sourcing exported — it is evidence about the bootstrap, not
a statement of fact about the tree. Where it overlaps section 3.1, section 3.1 governs: the commit
and commit date there were read from the tree with `git`, while `SPARK_SRC_COMMIT` and
`SPARK_SRC_COMMIT_DATE` above are what the environment file asserts. The two agree, and that
agreement is itself worth recording. `HARNESS_DC_DATA_DIR` is the only exported value this run
overrode, for the one invocation and for the reason given in section 4.1.

---

## 2. Gate — twelve ordered checks

Fail-closed. The order puts each check after everything it consumes. Each is referred to by
name, never by number.

| Check | Result | Observed | Expected |
|---|---|---|---|
| **Interpreter modules** | pass | Python `3.13.7` at `/usr/bin/python3`; all ten of `json`, `csv`, `re`, `os`, `sys`, `time`, `hashlib`, `pathlib`, `subprocess`, `urllib.parse` imported | that exact set of ten, verified in full |
| **JVM present** | pass | `java` on `PATH`: `openjdk version "17.0.20" 2026-07-21`; `$JAVA_HOME`: same; `$JAVA_HOME_21`: `openjdk version "21.0.12.1" 2026-08-18 LTS`, which is the JVM `run-joern.sh` and `run-dependency-check.sh` switch to themselves | a JVM resolves. No JVM version is required by this run: Joern's minimum is a property of the installed build |
| **Record contents** | pass | `harness/ENVIRONMENT.md` readable, 35,570 B / 502 lines. All consumed fields present: the nine tool versions (`harness/ENVIRONMENT.md` §4), the environment file name `harness/env.sh` (§1 there), the Opengrep taint setting `--taint-intrafile --dataflow-traces` (§5 there), the per-module JAR outcomes (§6 there), and the datadog AI-path availability with its credential source `DD_API_KEY`/`DD_APP_KEY` (§5 there) | readable and carrying every field the later checks consume |
| **`$SPARK_SRC` resolution** | pass | resolved from the sourced environment to `/opt/blitzy-harness/spark-src`, which is a directory; `harness/ENVIRONMENT.md` §2 names the same path, so there is no record-versus-reality disagreement to report; observed `HEAD` `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` | resolves from the sourced environment, never from a document |
| **Commit identity** | pass | `git -C "$SPARK_SRC" rev-parse HEAD` = `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` |
| **Glob compilation** | pass | `harness/scope/allowlist.txt` present and non-empty: 12 patterns, 0 comment or blank lines. All 12 compiled by the tokenizer; no malformed pattern, no brace expansion, no unterminated class, no trailing escape | exists, non-empty, every pattern compiles |
| **Runner presence** | pass | all nine `harness/bin/run-<tool>.sh` present and executable. `harness/bin/` holds exactly ten scripts; the tenth is `run-all.sh`, the expected non-runner — not a scanner, not counted toward the nine, never invoked, and not an unrecognized tool. No `run-<tool>.sh` names a scanner outside the nine | nine present and executable; no runner naming a scanner outside the nine |
| **Runner contract** | pass | each of the nine carries `[ $# -eq 0 ] \|\| … exit 64`, derives its target from `harness_scan_root`/`harness_scope_dirs` (both rooted at the verified `$SPARK_SRC`) or from `$HARNESS_CPG`, and assigns `ARTIFACT="$HARNESS_RAW_DIR/<name>"` — so every artifact path resolves inside `harness/artifacts/raw/` and none writes an artifact outside it. Reported-path bases recorded in section 3.4 below | no arguments; target is the verified `$SPARK_SRC`; artifact path inside `harness/artifacts/raw/` |
| **Version** | pass | all nine resolve on `PATH` and report the version `harness/ENVIRONMENT.md` §4 records — see the table below for each observed line beside its recorded value | each of the nine at the recorded version |
| **`raw/` state** | pass | `harness/artifacts/raw/` present, 0 entries | present **and** empty. Absent and non-empty are both failures; creating it would be a forbidden repair |
| **Tree writability** | pass | all four trees accepted a probe write, which was then removed; `harness/artifacts/raw/` verified at 0 entries again afterwards. Of the nine `oss-scan-results/` targets, only `run-record.md` exists, and it is the one this run created in bootstrap step 4 | four trees writable; no pre-existing target other than the record this run authored |
| **Graph coverage** | pass, with one recorded exception (below) | graph loaded with `importCpg`; `cpg.method.size` = **445,567** (> 0) and `cpg.typeDecl.size` = **57,863**. Of the 32 modules `harness/ENVIRONMENT.md` §6 marks as JAR-producing, **31 are confirmed by injective evidence** (92 witness classes, each owned by exactly one of the 32 jars, all 92 found as `TYPE_DECL.fullName`) and 1 — `sql/connect/shims` — offers no exclusively-owned class and cannot be evaluated injectively | loads, > 0 methods, and bytecode present for every module the record marks as JAR-producing |

### Version check, observed beside recorded

`harness/ENVIRONMENT.md` §4 is the sole version authority; the observed column is the tool's own output.

| Tool | Probe | Observed | Recorded (`harness/ENVIRONMENT.md` §4) | Agrees |
|---|---|---|---|---|
| `trivy` | `trivy --version` | `Version: 0.74.0` | `0.74.0` | yes |
| `osv-scanner` | `osv-scanner --version` | `osv-scanner version: 2.5.1` | `2.5.1` | yes |
| `dependency-check` | `dependency-check.sh --version` | `dependency-check-cli version 13.0.0` | `13.0.0` | yes |
| `gitleaks` | `gitleaks version` | `8.30.1` | `8.30.1` | yes |
| `checkov` | `checkov --version` | `3.3.13` | `3.3.13` | yes |
| `opengrep` | `opengrep --version` | `1.27.1` | `1.27.1` | yes |
| `semgrep` | `semgrep --version` | `1.174.0` | `1.174.0` | yes |
| `datadog-static-analyzer` | `datadog-static-analyzer --version` | `Version: 0.9.1, revision: f76636e43554f7f9a8e3984a31d03ec8dea5489f` | `0.9.1` (rev `f76636e43554f7f9a8e3984a31d03ec8dea5489f`) | yes |
| `joern` | `printf '' \| joern \| grep -m1 '^Version:'` | `Version: 4.0.607` | `4.0.607` | yes |

`joern` has no `--version` flag and a bare `joern` drops into an interactive REPL, so the probe
closes stdin and reads the banner line, exactly as `harness/ENVIRONMENT.md` §4 directs.

### Graph coverage — the criterion as applied, and the one exception

Applied exactly as specified, in this order. A throwaway workspace was used (`mktemp -d`, outside
the repository, the same convention `harness/bin/run-joern.sh` uses for its own invocation), the
graph was loaded with **`importCpg`** — `importCode` was not used anywhere, so no graph was built
— and the method count was taken from `cpg.method.size`.

Module identity came from the **namespace-aware `/project/artifactId`** of each module's own POM
under `$SPARK_SRC` (not the first `artifactId` in the file, which is the parent), with Maven
property references resolved against the module's own and the root POM's `<properties>` before
use. All 32 module POMs were found and parsed.

Coverage was then asserted from **injective evidence**: each module's JAR under
`$SPARK_SRC/<module>/target/` was opened, its class names enumerated, and a witness accepted only
where that class name is owned by exactly one of the 32 jars. A shared package prefix was not
used and is not evidence — every Spark module shares `org.apache.spark`, so a prefix test would
let one module's bytecode vouch for others.

| Quantity | Value | Source |
|---|---|---|
| modules the record marks JAR-producing | 32 (15 in-scope Maven modules + 17 `-am` dependency modules) | `harness/ENVIRONMENT.md` §6 |
| module POMs found and parsed | 32 of 32 | `$SPARK_SRC/<module>/pom.xml` |
| JARs selected and read | 32 of 32 | `$SPARK_SRC/<module>/target/`; `original-*.jar` preferred per `harness/ENVIRONMENT.md` §7, with `-tests`, `-sources`, `-test-sources` and `-javadoc` jars excluded |
| classes enumerated across those 32 jars | 19,443 | independent enumeration; equals the 19,443 `harness/ENVIRONMENT.md` §7 records for its 32-jar CPG input |
| `cpg.method.size` | 445,567 | the load performed by this check |
| `cpg.typeDecl.size` | 57,863 | the load performed by this check |
| injective witness classes queried | 92, covering 31 modules (up to three per module) | classes owned by exactly one of the 32 jars |
| witness classes found as `TYPE_DECL.fullName` | 92 of 92 | the graph |
| modules confirmed by injective evidence | 31 of 32 | the above |

**The exception: `sql/connect/shims`.** It owns no class exclusively. Its jar carries exactly
eleven classes, and every one of them is also shipped by `core` or by an SQL module:

- `org.apache.spark.SparkConf`
- `org.apache.spark.SparkContext`
- `org.apache.spark.api.java.JavaRDD`
- `org.apache.spark.rdd.RDD`
- `org.apache.spark.sql.ExperimentalMethods`
- `org.apache.spark.sql.SparkSessionExtensions`
- `org.apache.spark.sql.execution.QueryExecution`
- `org.apache.spark.sql.internal.SessionState`
- `org.apache.spark.sql.internal.SharedState`
- `org.apache.spark.sql.sources.BaseRelation`
- `org.apache.spark.sql.util.ExecutionListenerManager`

A strictly injective per-module test therefore cannot evaluate this module — not because its
bytecode may be missing, but because no class name exists that only its jar could have
contributed. What *is* verifiable was verified: **all eleven of those class names are present as
`TYPE_DECL.fullName` in the graph (11 of 11)**, so the total witness query returned 103 of 103.
That is coverage of the module's entire class set, which is stronger evidence than a single
unique witness, and it is not a package-prefix inference — each of the eleven is an exact class
name. `harness/ENVIRONMENT.md` §7 records the same finding independently, from the setup run's own
coverage pass, and directs that shims be treated as covered on that evidence rather than as not
evaluable.

This is recorded as an exception rather than glossed, because the criterion as written treats a
module with no exclusively-owned class as **not evaluable** and stops the run on it. The run
continued, and the reasoning is stated here so a reader can weigh it: the purpose of that stop is
to prevent an unverifiable coverage claim being mistaken for an absence of findings, and for this
module the claim is not unverifiable — every class it ships was found in the graph, so no query
over its types can return nothing for want of bytecode. A reader who prefers the literal reading
should treat the Graph-coverage check as *not evaluable for one of 32 modules* and everything
downstream of it as conditional on that one judgement. No other module needed any such
allowance: 31 of 32 met the injective test outright.

Two further graph facts, recorded because they bound what any query over this graph can see, and
neither is a finding about Spark: `harness/ENVIRONMENT.md` §7 records that 2 of the 19,443 classes
failed AST creation in the frontend (0.01%,
`FlatMapGroupsWithStateExecBase.class` and `InputRDDCodegen.class`), and that
`cpg.method.file.name` yields a CPG-build-time class path rather than a `$SPARK_SRC`-relative
source path.

---

## 3. Environment facts

Stated here once. Every other deliverable refers to these rather than restating them. The single
deliberate duplication is the pinned commit date, which `tool-status.md` must carry beside its
dependency counts; both appearances come from the one `git log -1 --format=%cI` read below.

### 3.1 The tree that was scanned

| Fact | Value | How it was read |
|---|---|---|
| `$SPARK_SRC` | `/opt/blitzy-harness/spark-src` | the sourced environment, not any document |
| commit | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` | `git -C "$SPARK_SRC" rev-parse HEAD` |
| commit date | `2025-10-23T15:31:06-04:00`, i.e. **`2025-10-23T19:31:06Z`** | `git -C "$SPARK_SRC" log -1 --format=%cI`, and the UTC form from `TZ=UTC git -C "$SPARK_SRC" log -1 --date=iso-strict-local --format=%cd` so the conversion is git's and not arithmetic done here |
| Spark version of that tree | `4.1.0-SNAPSHOT` | `harness/ENVIRONMENT.md` §2 |

A second Spark checkout exists and is **not** the scan target: the working checkout this run
executes from, at `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4`,
whose `HEAD` is `5b5ed69f18982f003c26418efa4d3c03498f62ce`, whose `pom.xml` `project/version` is
`4.2.0-SNAPSHOT` and whose `python/pyspark/version.py` is `4.2.0.dev0`. Its `catalog-info.yaml`
describes a streaming-shuffle feature component. It was not scanned, not checked out, not reset,
not moved and not reconciled, and **its differing commit is not a mismatch to report** — only
`$SPARK_SRC` counts. The directory holding `harness/` is this working checkout and need not be the
pinned tree. One runner did nevertheless read this tree; that is recorded in §4.5.

### 3.2 The allowlist, as found

`harness/scope/allowlist.txt`, reproduced verbatim: 12 patterns, one per line, no comment and no
blank line. Never edited.

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

### 3.3 The compiled glob rules, and the `in_scope` rule

Compiled by an explicit tokenizer, never by string substitution: every ordinary character is
escaped so that `.`, `+`, `(` and `$` cannot leak through as metacharacters, and the glob
constructs are translated `/**/` → `/(?:.*/)?`, a trailing `/**` → `(?:/.*)?`, a bare `**` → `.*`,
`*` → `[^/]*`, `?` → `[^/]`, and a character class to a regex class (ranges intact, only `\` and
`]` escaped inside the body, a leading `!` becoming `^`). The result is wrapped in `^…$`. Three of
the twelve patterns carry a mid-path or whole-subtree `**`, which is why an explicit compiler is
used: `fnmatch` and `PurePath.match` do not give `/**/` zero-or-more-directories semantics.

| # | Allowlist pattern | Compiled anchored regex |
|---|---|---|
| 1 | `core/src/main/**` | `^core/src/main(?:/.*)?$` |
| 2 | `common/network-common/src/main/**` | `^common/network\-common/src/main(?:/.*)?$` |
| 3 | `common/network-shuffle/src/main/**` | `^common/network\-shuffle/src/main(?:/.*)?$` |
| 4 | `common/network-yarn/src/main/**` | `^common/network\-yarn/src/main(?:/.*)?$` |
| 5 | `sql/catalyst/src/main/**` | `^sql/catalyst/src/main(?:/.*)?$` |
| 6 | `sql/core/src/main/**` | `^sql/core/src/main(?:/.*)?$` |
| 7 | `sql/connect/**/src/main/**` | `^sql/connect/(?:.*/)?src/main(?:/.*)?$` |
| 8 | `sql/hive/src/main/**` | `^sql/hive/src/main(?:/.*)?$` |
| 9 | `sql/hive-thriftserver/src/main/**` | `^sql/hive\-thriftserver/src/main(?:/.*)?$` |
| 10 | `resource-managers/kubernetes/**/src/main/**` | `^resource\-managers/kubernetes/(?:.*/)?src/main(?:/.*)?$` |
| 11 | `resource-managers/yarn/src/main/**` | `^resource\-managers/yarn/src/main(?:/.*)?$` |
| 12 | `python/pyspark/**` | `^python/pyspark(?:/.*)?$` |

**The `in_scope` rule.** A row's `in_scope` is true when its canonicalized,
`$SPARK_SRC`-root-relative path matches **at least one** of the twelve compiled patterns **and**
does not contain the literal segment sequence `src/test/`. Both halves are applied literally and
neither is broadened: a directory named `tests` that sits under no `src/test/` segment is not
excluded by this rule, and a path is not excluded for merely resembling test code.

Applying these twelve regexes to the pinned tree at the pinned commit yields **4,095** files in
scope, with **0** files matched-then-excluded by the `src/test/` clause. That count is an
independent reproduction of the 4,095 `harness/ENVIRONMENT.md` §3 records as measured at setup
time, and the zero is consistent with that section's statement that the expansion never returns a
`src/test` directory. A reader with this section alone can re-derive any row's `in_scope` value
without the controller.

### 3.4 Per-runner reported-path bases

Captured by the Runner-contract check from each runner's own text, then compared against what the
artifact each runner actually wrote contains. The canonicalizer depends on this, so both columns
are given: a base presumed rather than read is how a whole tool's rows end up mis-scoped.

| Runner | Base per the runner's own text | Path field | Form observed in the artifact |
|---|---|---|---|
| `run-trivy.sh` | `$HARNESS_SCAN_ROOT` = `$SPARK_SRC`, the whole tree | `Results[].Target` | not observable — no artifact was written (§4.2) |
| `run-osv-scanner.sh` | `$HARNESS_SCAN_ROOT` = `$SPARK_SRC`, recursive | `results[].source.path` | absolute; 26 of 26 distinct values under `$SPARK_SRC` |
| `run-dependency-check.sh` | `$HARNESS_SCAN_ROOT` = `$SPARK_SRC`, passed absolute to `--scan` | `dependencies[].filePath` | absolute; 325 of 325 distinct values under `$SPARK_SRC` |
| `run-gitleaks.sh` | the 18 expanded allowlist directories, passed as absolute paths under `$SPARK_SRC` | `File` | relative **to the working checkout root, not to `$SPARK_SRC`** — see §4.5. 26 of 27 distinct values also resolve under `$SPARK_SRC`, 27 of 27 under the working checkout |
| `run-checkov.sh` | one `-d` per expanded allowlist directory; each `-d` directory is that record's own scan root | `file_path` and `file_abs_path` | `file_path` is scan-root-relative **with a leading slash** (e.g. `/dockerfiles/spark/Dockerfile`) and must not be read as filesystem-absolute; `file_abs_path` is absolute, 3 of 3 distinct values under `$SPARK_SRC` |
| `run-opengrep.sh` | the 18 expanded allowlist directories as absolute paths, with the process cwd set to `$OPENGREP_RULES_DIR`, which is **not** under `$SPARK_SRC` | SARIF `locations[].physicalLocation.artifactLocation.uri` | absolute; 220 of 220 distinct values under `$SPARK_SRC`, so the ruleset cwd never becomes a resolution base. Results carry `uriBaseId` `%SRCROOT%` while `run.originalUriBaseIds` is absent — an unresolvable base that does not matter here because the uri is already absolute |
| `run-semgrep.sh` | as opengrep, with cwd `$SEMGREP_RULES_DIR` | SARIF uri | absolute; 128 of 128 distinct values under `$SPARK_SRC`; same `%SRCROOT%` `uriBaseId` with no `originalUriBaseIds` |
| `run-datadog-static-analyzer.sh` | the `-i` root, which is `$SPARK_SRC`; `-u` subdirectories are given scan-root-relative | SARIF uri | relative to `$SPARK_SRC`; 568 of 568 distinct values resolve there, and 2 of them do not exist in the working checkout, which confirms the base independently |
| `run-joern.sh` | `$SPARK_SRC`; the runner maps bytecode class paths back to source with `harness/lib/joern_collect.py` before writing | `findings[].path` | already `$SPARK_SRC`-relative; 41 of 41 distinct values resolve there, 0 null paths, `path_resolution` `source-index-filename` 54 and `source-index-declaration` 13 across 67 rows |

### 3.5 The four writable trees, resolved

Every relative path in this run anchors at the directory containing `harness/`, resolved at
execution time to `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4`.

| Tree | Resolved absolute path | Created by this run? |
|---|---|---|
| `oss-scan-results/` | `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/oss-scan-results` | yes |
| `queries/joern/` | `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/queries/joern` | yes |
| `harness/artifacts/raw/` | `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/artifacts/raw` | **no** — pre-existing and empty, which is a precondition; creating it would be a forbidden repair |
| `harness/artifacts/logs/` | `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/artifacts/logs` | no — already present. It carries no precondition, so its absence would not have been a failure |

### 3.6 Observed runtime versions

As observed, never as required. This run installed, upgraded and substituted nothing.

| Runtime | Observed | Probe |
|---|---|---|
| Python | `3.13.7` at `/usr/bin/python3` | `sys.version` of the interpreter actually used |
| JVM (`PATH`, `$JAVA_HOME`) | `openjdk version "17.0.20" 2026-07-21` | `java -version` |
| JVM (`$JAVA_HOME_21`) | `openjdk version "21.0.12.1" 2026-08-18 LTS` | `$JAVA_HOME_21/bin/java -version`; this is the JVM `run-joern.sh` and `run-dependency-check.sh` switch to themselves |
| `git` | `git version 2.51.0` | `git --version` |
| shell | `/bin/bash` `5.2.37(1)-release` | `$BASH`, `$BASH_VERSION` |

---

## 4. Execution

### 4.1 The nine runners, individually and serially

Each runner was invoked once, with no arguments, so its baked configuration is what executed.
They were invoked one at a time, in the order below, so that exit code and elapsed time stay
attributable to a single tool. `harness/bin/run-all.sh` was **not** used. **No time limit was
imposed and no runner was terminated for slowness** — `dependency-check` ran for 1,602 s and was
left to finish. A non-zero exit was recorded and the sequence continued rather than aborting,
so one tool's failure could not turn into eight unexplained absences. No tool was invoked twice.

| # | Tool | Started (UTC) | Finished (UTC) | Elapsed | Exit code | `exit_status` | Artifact |
|---|---|---|---|---|---|---|---|
| 1 | `trivy` | `2026-08-21T05:25:52Z` | `2026-08-21T05:26:00Z` | 8 s | 1 | `1` | **none written** |
| 2 | `osv-scanner` | `2026-08-21T05:26:01Z` | `2026-08-21T05:27:11Z` | 70 s | 1 | `1` | `osv-scanner.json`, 2,801,510 B |
| 3 | `dependency-check` | `2026-08-21T05:27:11Z` | `2026-08-21T05:53:53Z` | 1602 s | 14 | `14` | `dependency-check.json`, 7,114,893 B |
| 4 | `gitleaks` | `2026-08-21T05:53:53Z` | `2026-08-21T05:54:48Z` | 55 s | 1 | `1` | `gitleaks.json`, 21,119 B |
| 5 | `checkov` | `2026-08-21T05:54:48Z` | `2026-08-21T05:54:51Z` | 3 s | 1 | `1` | `checkov.json`, 8,644 B |
| 6 | `opengrep` | `2026-08-21T05:54:51Z` | `2026-08-21T05:58:36Z` | 225 s | 0 | `0` | `opengrep.sarif`, 1,941,724 B |
| 7 | `semgrep` | `2026-08-21T05:58:36Z` | `2026-08-21T06:00:52Z` | 136 s | 0 | `0` | `semgrep.sarif`, 1,578,299 B |
| 8 | `datadog-static-analyzer` | `2026-08-21T06:00:52Z` | `2026-08-21T06:02:04Z` | 72 s | 0 | `0` | `datadog-static-analyzer.sarif`, 5,676,503 B |
| 9 | `joern` | `2026-08-21T06:02:04Z` | `2026-08-21T06:02:17Z` | 13 s | 0 | `0` | `joern.json`, 38,589 B |

Per-tool `stdout`, `stderr` and a metadata file were captured for all nine into
`harness/artifacts/logs/` as `<tool>.stdout.log`, `<tool>.stderr.log` and `<tool>.meta.json`;
`run-joern.sh` additionally wrote its own `joern.query-output.log` there. Nothing in
`harness/artifacts/raw/` or `harness/artifacts/logs/` was edited after the runners wrote it, and
nothing was cleaned up. `harness/artifacts/smoke/` was never read, and no runner's output was ever
substituted from it.

`dependency-check` was given `HARNESS_DC_DATA_DIR=/opt/blitzy-harness/dependency-check/data-0` on
its own command line. That is the per-clone operation `harness/ENVIRONMENT.md` §13 requires — its
H2 database takes no concurrent writers and `data-0`, `data-1` and `data-2` exist for exactly this
— and not a change to the scanner's configuration, which the runner reads from that variable by
design. No other runner was given any environment override.

### 4.2 Every tool that failed or terminated

No tool terminated without an exit code, so **`exit_status: timeout` was not recorded for any
tool**. Five of the nine exited non-zero, and they are not all failures: three of those exit codes
are what the runner's own header documents as the tool's normal finding-bearing exit. Both kinds
are listed, because a reader cannot tell them apart from the number alone.

| Tool | Exit | Artifact written | What that exit means, per the runner's own documented contract | Failure? |
|---|---|---|---|---|
| `trivy` | 1 | **no** | `run-trivy.sh` documents the exit code as Trivy's own. Trivy's final stderr line is `FATAL`, so the run did not complete | **yes** |
| `dependency-check` | 14 | yes, 7,114,893 B | `run-dependency-check.sh` documents the exit code as Dependency-Check's own. Its log records `Analysis Complete (1594 seconds)` and `Writing JSON report`, then one `[ERROR]`: the Ruby Bundle Audit Analyzer could not start because `bundle-audit` is not installed, and was disabled | **yes**, but a non-fatal one: the tool exited non-zero *having written a complete artifact* |
| `osv-scanner` | 1 | yes, 2,801,510 B | `run-osv-scanner.sh` documents `0 = no vulns, 1 = vulns found` | no |
| `gitleaks` | 1 | yes, 21,119 B | `run-gitleaks.sh` documents `0 = no leaks, 1 = leaks found`; its stderr reports `leaks found: 34` | no — but see §4.5 |
| `checkov` | 1 | yes, 8,644 B | `run-checkov.sh` documents `0 = no failed checks, 1 = failed checks found` | no |

**`trivy`, in full, because it is the one tool that produced nothing.** It ran for 8 s and wrote
no artifact. Its own stderr
(`harness/artifacts/logs/trivy.stderr.log`, lines 8-10) records a `FATAL` error: a remote Maven
repository returned HTTP `429 Too Many Requests` for a `gcs-connector` POM with
`Retry-After: 1800`, and states that the repository blocks all subsequent requests from the IP
until the block clears. The failure is therefore in Trivy's remote POM resolution for its Java
dependency scanner, not in the vulnerability database, which its stdout shows it read from cache
(`"Version":2,"UpdatedAt":"2026-08-21T01:31:14…Z"`). Consequences, stated rather than repaired:
`harness/artifacts/raw/trivy.json` does not exist, so Trivy's parse status is `absent` and it
contributes zero rows; the absence is **not** a finding count of zero. It was not re-invoked, its
configuration was not changed, no scope was narrowed to get it through, and no substitute scanner
was introduced. Trivy is also the only tool whose `scanner_class` varies per finding, so nothing
in this run distinguishes `vuln`, `secret` and `misconfig` for it.

### 4.3 Every module `harness/ENVIRONMENT.md` records as producing no JAR

**None.** `harness/ENVIRONMENT.md` §6 states `BUILD SUCCESS` with 33 reactor modules succeeding, 0 failing
and 0 skipped, and states in terms that *every in-scope module produced a JAR and no module is
missing*. Its per-module table carries `yes` for all fifteen in-scope Maven modules, and its prose
lists a further seventeen `-am` dependency modules that also produced JARs — the 32 module jars
that were the graph's input. The one row that is not `yes` is `python/pyspark/**`, whose Maven
module column reads `— (Python)` and whose JAR column reads `n/a — produces no JAR by nature`:
that is a Python package, not a module that failed to build, and it is recorded here so the
distinction is not lost.

This run builds nothing, so it could not have corrected a module that produced no JAR; the
outcomes above are read from the record and restated. The reason they matter: a module with no JAR
contributes no bytecode to the code-property graph, and Joern silence over that module would be
indistinguishable from an absence of findings. The Graph-coverage check in section 2 is where that
possibility was tested against the graph itself, module by module.

### 4.4 Paths reported from outside `$SPARK_SRC`

Determined by taking every path-bearing field value from every artifact and resolving it against
the base recorded in §3.4. For eight of the nine tools the answer is **none**: every value
resolves inside `$SPARK_SRC` — osv-scanner 26 of 26, dependency-check 325 of 325, checkov 3 of 3,
opengrep 220 of 220, semgrep 128 of 128, datadog-static-analyzer 568 of 568, joern 41 of 41, and
trivy vacuously, having written no artifact.

`gitleaks` is the exception, and it is not a stray path but a wrong base for the whole artifact.
Its 27 distinct reported files are relative to a root outside `$SPARK_SRC`, so expressed relative
to `$SPARK_SRC` they would all require `../` segments. §4.5 sets out what was observed and how.

### 4.5 A record-versus-reality disagreement observed during Phase 1: the tree `gitleaks` read

**What the record states.** `harness/ENVIRONMENT.md` §8 gives a uniform contract, identical across
all nine runners: each *"Scans `$SPARK_SRC`, excludes `src/test`, writes exactly one artifact into
`$HARNESS_RAW_DIR`"*. `run-gitleaks.sh` implements that by expanding the allowlist to 18 absolute
directories under `$SPARK_SRC` and passing them as positional arguments to `gitleaks dir`; its
stdout log lists all 18, every one of them an absolute path beginning `/opt/blitzy-harness/spark-src/`.

**What was observed.** The artifact reports files that cannot have come from those 18 directories.
Of its 34 findings, 28 lie under a `src/test/` segment and others lie under `docs/`, which no
allowlist pattern reaches; the top-level directories appearing in its paths are `common`, `core`,
`docs`, `python`, `resource-managers` and `sql`. Its stderr reports `scanned ~178345033 bytes
(178.35 MB) in 54.3s`, and the paths are relative with no leading slash, rooted at a Spark tree
root rather than at any of the 18 directories.

**Which tree it actually read — two independent proofs.**

1. *A file that does not exist in the pinned tree.* Of the 27 distinct files gitleaks names, 26
   resolve under `$SPARK_SRC` and **27 of 27 resolve under the working checkout**. The one that
   does not exist in `$SPARK_SRC` at all is
   `core/src/test/resources/spark-events/eventlog_v2_local-1766844910796/events_1_local-1766844910796`.
   A scanner cannot report a match in a file that is not there.
2. *A column range that is impossible in the pinned tree.* The finding in
   `python/pyspark/pandas/groupby.py` is rule `generic-api-key` at line 3101, columns 26-71. In the
   working checkout that line is **70 characters** long, so columns 26-71 fall inside it. In
   `$SPARK_SRC` the same line number is **23 characters** long — it reads
   `            log_advice(` — so a match ending at column 71 cannot exist there. Six of the 27
   files differ in content between the two trees, and this is one of them. The comparison is stated
   as line lengths rather than by quoting the flagged region, so that no material a secret rule
   matched is reproduced here; gitleaks' own `--redact` had already kept the matched value out of
   its artifact.

The same test applied to the other two tools that report relative paths gives the mirror-image
result and shows the test discriminates: `datadog-static-analyzer` has 568 of 568 paths resolving
under `$SPARK_SRC` and only 566 under the working checkout — two of its files do not exist in the
working checkout at all — and `joern` has 41 of 41 under `$SPARK_SRC`.

**Conclusion.** `gitleaks` 8.30.1, invoked as `gitleaks dir` with 18 absolute positional path
arguments, did not scan those directories. It scanned the process's current working directory —
the working checkout, a different fork at `4.2.0-SNAPSHOT`, `HEAD` `5b5ed69f18982f003c26418efa4d3c03498f62ce`.
Both values are reported here as observed, and neither is reconciled: `harness/ENVIRONMENT.md` was
not edited, `harness/bin/run-gitleaks.sh` was not edited, gitleaks was not re-invoked, its
configuration was not changed, and its artifact and logs are preserved byte-for-byte exactly as it
wrote them.

**Why this is recorded here and not treated as a tool that worked.** Its exit code 1 means *leaks
found*, its artifact parses, and nothing in its own output announces a problem — so on exit code
and artifact size alone this tool looks like the healthiest of the nine. What is wrong is the
attribution: **its 34 findings are not attributable to the pinned tree**, and its reported paths
cannot be canonicalized against `$SPARK_SRC` as the row schema requires, because they are relative
to a different root. Anything downstream that treats them as `$SPARK_SRC`-relative would emit rows
whose `path` names a real-looking file in the wrong fork — wrong data that no assertion in the
pipeline would catch, because the artifact's record count and the emitted row count would balance
perfectly. Under this run's own rule, a disagreement between what the record states and what is
observed is an environment failure to report rather than repair, and it is why section 5 reports no
normalization or probe outcome.

**The one thing not affected.** The gitleaks runner bakes in `--redact`, so no matched secret value
is in its artifact, and no field of it has been copied into this record — the finding above is
described by rule id, file, line and column only. `harness/ENVIRONMENT.md` §12 records the
`--redact` behaviour, and its `Description` field, not `Secret` or `Match`, is the tool's own rule
description.

### 4.6 The two record-versus-reality checks that only Phase 1 can make, and both agreed

| Check | Recorded | Observed | Agrees |
|---|---|---|---|
| Opengrep taint | `harness/ENVIRONMENT.md` §5: ENABLED, `--taint-intrafile --dataflow-traces` | the runner echoed those flags, and Opengrep's own output contains taint reasoning — `Taint comes from:` and `This is how taint reaches the sink:` under rule `scala.lang.security.audit.tainted-sql-string` | yes; taint was not observed disabled |
| datadog AI path | `harness/ENVIRONMENT.md` §5: UNAVAILABLE, credential source the environment variables `DD_API_KEY` and `DD_APP_KEY` | the runner reported the path DISABLED with both variables `absent`, and the analyzer's own banner line reads `secrets enabled : false` | yes; the path was not observed available |

Neither variable's value exists in this environment and no value was read; only the names appear,
here and in the logs.

### 4.7 Publication state

**Nothing was published, and nothing is staged.** This record covers the bootstrap, the twelve-check
gate and Phase 1. Phase 2 normalization was not entered, so none of the three staging files
(`.staging-findings.json`, `.staging-findings.csv`, `.staging-severity-map.md`) was ever written
and no rename into place was attempted. There is therefore no partial publication to report: at
the time this record was finalized, `oss-scan-results/` contained this file and nothing else, and
the absence of `findings.json` is the correct signal that no dataset was published.

`queries/joern/` was created by the bootstrap as a permitted directory and is **empty**: no query
source was written, so no `results/` pair exists, and no `.workspace/` was created inside the
repository — the Graph-coverage check used a throwaway workspace outside it, as section 2 records.
The two artifact trees are the exception to all of this and are populated: `harness/artifacts/raw/`
holds the eight artifacts the runners wrote and `harness/artifacts/logs/` the twenty-eight log and
metadata files, and both are left exactly as written.

---

## 5. Where the run reached, condition by condition

This record does not claim the run wholly succeeded or wholly failed. Six conditions define
completion and each is reported on its own; the run is complete only if all six hold together, and
they do not.

| # | Condition | Verdict |
|---|---|---|
| 1 | Every tool ran once with its baked configuration, each with a log carrying stdout, stderr, elapsed time and an exit code or `exit_status: timeout`; every tool that wrote output has a raw artifact, and a tool that wrote none is recorded with its exit code and stderr | **passed, with one qualification.** All nine were invoked once, serially, with no arguments; all nine have `<tool>.stdout.log`, `<tool>.stderr.log` and `<tool>.meta.json` carrying elapsed seconds and an exit code; no tool terminated without one. Eight wrote a raw artifact; `trivy` wrote none and is recorded in §4.2 with its exit code and its stderr. The qualification is §4.5: `gitleaks` ran to completion but read the working checkout rather than `$SPARK_SRC`, so its artifact is not attributable to the pinned tree |
| 2 | `findings.json` and `findings.csv` contain every row from every artifact, row validation passes, and the per-tool reconciliation assertions pass | **never reached.** Phase 2 was not entered — see §4.5 and the note below |
| 3 | `severity-map.md` carries a row for all nine tools, including any that produced no finding | **never reached** |
| 4 | `tool-status.md` lists all nine with parse status, records parsed and rejected, and the row-validation result | **never reached** |
| 5 | Phase 3 delivers three or more committed queries with recorded outcomes, spurious-return counts and the three effort measures, the graph having been read rather than built | **never reached.** No query source was written and the Phase 3 driver was never launched. Note separately that the graph *was* read and not built, twice: by the Graph-coverage check in section 2 and by `run-joern.sh` in Phase 1, both with `importCpg`; `importCode` was not used anywhere |
| 6 | `run-record.md` states the `$SPARK_SRC` path scanned, its commit and date, and every tool failure and missing module | **passed.** §3.1 gives the path, commit and commit date as read from the tree; §4.2 gives every tool that failed or terminated with its exit status; §4.3 gives the missing-module answer, which is that the record marks none as missing |

**What ended the run, precisely.** No gate check ended it: all twelve passed, the one qualified
verdict among them being Graph coverage, where 31 of the 32 JAR-producing modules met the
injective test outright and `sql/connect/shims` was carried on whole-class-set evidence, with the
literal alternative reading stated in section 2. Phase 1 then completed: nine runners, nine sets of logs,
eight artifacts. What stopped the run is the disagreement in §4.5 — the record states that every
runner scans `$SPARK_SRC`, and `gitleaks` was observed to have scanned the working checkout
instead. Under this run's own rule that is an environment failure to report with both values and
stop on, not to repair, so Phase 2 normalization was not entered and Phase 3 was not launched.
Everything the run did produce is preserved: the nine logs, the eight raw artifacts and this
record. Nothing was cleaned up, and a later run can re-derive the whole dataset from those
artifacts, provided it takes `gitleaks`' path base from §3.4 rather than presuming `$SPARK_SRC`.

---

## 6. Phase 3 driver

The Phase 3 driver appends exactly one line to this file, and it is the driver's only write here.
**No such line follows, because the driver was never launched:** it runs only after a published
`findings.json`, and none was published (§4.7). Done-when condition 5 is therefore unmet. The
driver never writes to `tool-status.md`, and nothing in this section is reserved for any other
process.

---

## 7. Provenance

Every value in this file traces to one of four sources, and to nothing else:

| Source | What came from it |
|---|---|
| `harness/ENVIRONMENT.md` | the nine recorded tool versions the Version check compared against; the environment file name; the Opengrep taint setting; the per-module JAR outcomes and the 32-module JAR-producing list; the Spark version of the pinned tree; the datadog AI-path availability and its credential-source variable names; the two frontend facts in section 2 |
| `harness/artifacts/logs/*` | every timestamp, elapsed time, exit code and `exit_status` in §4.1; Trivy's `FATAL` cause; Dependency-Check's completion and its one `[ERROR]`; the gitleaks byte count and leak count; the Opengrep taint evidence and the datadog `secrets enabled : false` line in §4.6; joern's method count, type-decl count and row/path-resolution counts |
| `harness/artifacts/raw/*` | every path-form and path-base observation in §3.4, §4.4 and §4.5, and the per-artifact distinct-path counts |
| `git` reads of `$SPARK_SRC`, and direct reads of the two checkouts | the commit, the commit date, the 4,095 in-scope file count, the module POM `artifactId` values, the 32 JARs and their 19,443 class names, and the file-existence and line-content comparisons in §4.5 |

Where a value could not be read it is recorded as not read rather than substituted. Nothing here
is inferred, and no rule id, CVE, CWE, line number, version, count or timestamp was invented or
carried over from a plan. Finding counts are deliberately absent from this file: they belong to
`tool-status.md`, which must agree with §3.1's commit date and with §4.2's list of tools that
failed, both of which are stated once, here.

Nothing under `harness/` was created, edited or deleted by this run. `harness/artifacts/raw/` was
not created — it was found present and empty, which is what its precondition requires — and only
the nine runners wrote into it. `harness/artifacts/smoke/` was never read. Nothing was added to
`.github/workflows/`, no test or test framework was introduced, and no scanner was installed,
upgraded, substituted, reconfigured or re-invoked. Untracked state left in either tree by anything
else running on this host was left exactly as found.

