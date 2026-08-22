# `oss-scan-results/run-record.md` — environment and execution record

Opened by the outer-shell bootstrap and written check by check, so any stop is explained
by this file rather than leaving it absent. Every value traces to a raw artifact, a log,
`harness/ENVIRONMENT.md`, or a `git` read of the pinned tree — with two additions the
record-accuracy pass in §1.1 introduced and labels at every use: a `git` read of **this**
repository, for the two publication commits and the bytes they carry, and a filesystem
observation whose source class is named where it appears. Nothing here is inferred, and
where a value could not be read that is what is recorded (§7).

| | |
|---|---|
| Run identity | the publication pass recorded at `2026-08-22T06:48:45Z`. This run reached the controller more than once; every pass is dated in §1.1 |
| Repository root at execution time | `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d-w-000_a6fd4d` — the root the byte-preserved evidence under `harness/artifacts/logs/` names in its invocation lines |
| Repository root this record was assembled in and ships in | `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4` — the root this record's remaining absolute forms, and the Phase 3 envelopes' Joern project fields, name |
| Scanned tree | `/opt/blitzy-harness/spark-src` |
| Commit | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` |
| Commit date | `2025-10-23T19:31:06Z` (the same instant `git log -1 --format=%cI` reports as `2025-10-23T15:31:06-04:00`) |
| Outcome | gate passed; Phase 1 invoked all nine runners once; Phase 2 validated, staged, counted and published the dataset — twice, both times through the same protocol (§4.7) |

**Two repository roots, and why both are stated rather than one.** Every
repository-relative path in this record anchors at the directory that holds `harness/`, and
that is the stable form: the four writable trees exist under whichever checkout holds it.
The absolute forms do not survive the move between checkouts, and this record was written
in one and ships in another — so every absolute below is labelled with the checkout it
belongs to. The **execution-time root** is where the nine runners were invoked and where
the byte-preserved evidence sits; the **assembly root** is the checkout this record and the
Phase 3 result envelopes were written in, and the one they ship in. The two are stated and
**not reconciled into one**, for the same reason the graph's method count is (§2): a value
observed in one place is not restated as though it had been observed in another. §3.5 gives
the four writable trees under both, with the provenance of each root; §7 closes with what a
reader can and cannot re-check from the shipped tree.

## 1. Bootstrap

| Step | Result |
|---|---|
| Locate and source the environment file `harness/ENVIRONMENT.md` names (`harness/env.sh`) | harness/env.sh sourced from a non-login shell; SPARK_SRC and the toolchain PATH come from it |
| Collision precheck over every file this run creates | run before anything was written: 3 target(s) found in place, listed below — and **replaced rather than stopping the run, which is a disclosed deviation from the rule, not an application of it** |
| Create the permitted directories (`oss-scan-results/`, `queries/joern/`) | oss-scan-results/ and queries/joern/ present; harness/artifacts/logs/ created (it carries no precondition) |
| `harness/artifacts/raw/` | never created by this run — `harness/env.sh` creates it empty when the recorded environment is entered, and the gate verified it empty |
| `harness/artifacts/logs/` | filled by this run; it carries no precondition |
| Open `run-record.md` | opened by the bootstrap; this run authored it |
| Resolve an interpreter on the updated `PATH` and pipe the controller to it | `/opt/blitzy-harness/venv/bin/python3` (3.13.7) |

**Targets found in place, and the authority for replacing them.** The collision
precheck found the following already present, all of them written by a superseded
earlier attempt that stopped in Phase 1 and published no dataset:

| Target | Bytes found | sha256 found |
|---|---|---|
| `oss-scan-results/joern-probe.md` | 37983 | `59ddb9afbcc7469c4092ad8045cd1639f99e46378e186a14c74836611e491eeb` |
| `oss-scan-results/run-record.md` | 22853 | `6f2d7b4e78afd84cf3218e882f05cdfa1c7f3ebfecb85e8a5f675fe07a556379` |
| `oss-scan-results/tool-status.md` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |

For comparison against the repository rather than the filesystem: the tracked
predecessors of these three paths at this branch's parent commit are 37,983 B for
`joern-probe.md` — the same bytes the precheck found — and 45,005 B and 53,615 B for
`run-record.md` and `tool-status.md`, which the precheck found already replaced on disk at
the sizes above.

**This is a deviation from the collision-precheck rule, and it is recorded as one rather**
**than as a reading of it.** The rule is absolute as written: a pre-existing target stops
the run before a byte is written, because overwriting is neither a creation nor a permitted
repair, and every target this run creates is new on a first run. This pass did not stop. It
replaced the three targets above and continued. A reader is therefore not being asked to
accept that the rule failed to reach this case — it reaches it, and the rule was departed
from.

**The authority for the departure, and the four conditions that bound it.** The authority
is the code review of the superseded attempt, which required this pass in place of it:
stopping on files that attempt had itself written would have made the required remediation
unreachable. The departure was taken only because all four of the following held, and
outside them the rule stands unqualified and the run stops:

1. Each target is a **record this run authors** — never an artifact, never a log, never an
   immutable path, never third-party state. No file under `harness/` was in the set.
2. Each was written by a **superseded pass of this same project**, and nothing else owns
   any of them.
3. A **review of that pass required this one in its place**, so replacement is the
   instruction being carried out rather than a precondition being repaired.
4. The **bytes and sha256 each target carried before replacement are recorded first**, in
   the table above, so what was superseded is identifiable after the fact.

What the departure did not reach is as much a part of the record as what it did.
`harness/artifacts/raw/` was empty and `harness/artifacts/logs/` absent when this pass
began, so no tool output, no log and no third-party state was replaced, and the scan itself
is a first run in this tree. Every value in each replacement is this pass's own, derived
from its gate, its Phase 1 and its Phase 2 — nothing was carried over from the text that
was superseded — and for comparison against the repository rather than the filesystem, the
tracked predecessors at this branch's parent commit are named above.

### 1.1 The passes this record covers, dated

This run reached the controller more than once, and a single pass timestamp does not let a
reader place the phases in time. Both passes ran the same gate over the same tree; only the
first invoked a runner. Every entry names the source its timestamp comes from, and where
that source is not one of the three this record otherwise draws on — a raw artifact, a log,
or `harness/ENVIRONMENT.md` — the source class is stated so the weaker provenance is
visible rather than implied.

| When (UTC) | What happened | Where the timestamp comes from |
|---|---|---|
| `2026-08-22T04:29:07Z` | **scanning pass**: the `raw/` state check recorded `harness/artifacts/raw/` present and empty, 58 s before the first runner started | that check, §2 |
| `04:30:05Z → 05:12:06Z` | **scanning pass, Phase 1**: nine runners, serially, one invocation each. `run-trivy.sh` started `04:30:05Z`; `run-datadog-static-analyzer.sh` started `05:08:55Z` and ran 190.4 s, finishing `05:12:06Z` | `harness/artifacts/logs/<tool>.meta.json`, tabulated in §4.1 |
| `06:08:04Z → 06:33:14Z` | **Phase 3 driver, rounds 1–4**: twelve executions across the three queries, against the first publication | the per-query revision logs in `queries/joern/results/*.json` |
| `06:48:44.855Z` | **publication pass**: the Tree-writability check's probe created and removed in `harness/artifacts/raw/` and `harness/artifacts/logs/`, 1 h 36 min after the last runner finished | the two directory mtimes in the execution checkout. Source class: a filesystem observation, read by the QA audit of this milestone — this run's own logs do not time the probe |
| `06:48:45Z` | **publication pass** recorded, the timestamp this record's header carries | this record, header |
| `06:48:45Z → 06:52:32Z` | **Phase 3 driver, round 5**: three executions, still against the first publication | the per-query revision logs |
| `07:54:28Z` | **first publication committed** (`fec751bf3d3`): `findings.json` 5817891 B sha256 `ff166c86a89eef49…`, `findings.csv` 3320044 B `91a84eaa03626819…`, `severity-map.md` 4449 B `e456b520054f4f2b…` | the commit date, and the committed bytes themselves |
| after `07:54:28Z`, at or before `10:08:29Z` | **second publication**: the same staging-count-rename protocol again, correcting the three normalization defects §4.7 names. `findings.json` 5806988 B `2b3fb2db…`, `findings.csv` 3309257 B `68ae2e4e…`, `severity-map.md` 6049 B `ebf11a85…` | bounded below by the first publication's commit and above by the driver's own observation of exactly these three digests at `10:08:29Z`. The shipped copies' preserved mtimes read `09:32:47Z` for the three published outputs and `09:47:02Z` for `tool-status.md` — source class: a filesystem observation, read by the QA audit |
| `10:08:29Z → 10:31:43Z` | **Phase 3 driver, rounds 6–7**: five executions against the second publication, each envelope carrying those three digests as its precondition | `precondition_observed_by_the_driver` and the revision logs in `queries/joern/results/*.json` |
| `10:51:06Z` | this record and the five other deliverables committed (`e5dec08d84d`) | the commit date |
| after the QA audit of `2026-08-22T12:06:15Z` | **record-accuracy pass**: the two dependency scanners' own conditions added — §4.2 gained `osv-scanner`'s 85 resolution failures over 43 `pom.xml` files and 36 filtered packages and `dependency-check`'s three uninitialised or disabled analyzers, ten Node-manifest warnings and five unparsed CVSS vectors, each cited to the tool's own log, with the row weight those two counts carry (288 + 1697 = 1985 of 10178), and §7's logs provenance row was extended to cover them. The same pass added those conditions to `tool-status.md` §2.2 and §2.3, pointed §3's two count rows at them, and rebuilt its §5 to state each feed's recorded value beside the observed one. No scanner was invoked, no raw artifact or log was touched and no row of the dataset changed | the audit's own generation timestamp |
| after the QA audit of `2026-08-22T12:07:25Z` | **record-accuracy pass**: the audit trail made addressable — §7 gained *The evidence trees, by digest and by absolute path*, naming both roots and listing the bytes and sha256 of all 8 artifacts and 28 log files, with the pointers to it in §3.5 and §4.1; and §4.7 gained the `package_coordinate` derivation rule the dataset's 1985 populated values follow. The same pass added the sha256 beside each of the eight artifacts in `tool-status.md` §2 and the tree identification in its §9. No scanner was invoked, no raw artifact or log was touched and no row of the dataset changed | the audit's own generation timestamp |
| after the QA audit of `2026-08-22T12:13:35Z` | **record-accuracy pass**: this record corrected against that audit — the two repository roots separated and labelled, each gate check attributed to the pass that executed it, the collision departure stated as a departure, condition 5 and §6 scoped to the publication they describe, `.ruff_cache/` inventoried in §7, §3.4's path shapes split, and this pass log added. No scanner was invoked, no raw artifact or log was touched, no row of the dataset changed, and that pass edited no deliverable other than this record — `tool-status.md` and `joern-probe.md` restate condition 5 in the unscoped form §5 replaces here, and that wording was left as it stands. The three record-accuracy passes in the rows around it did edit other deliverables, each as its own row states | the audit's own generation timestamp; the commit carrying this correction is the latest `git log` entry for this file |
| after the QA audit of `2026-08-22T12:46:39Z` | **record-accuracy pass**: §6's single Phase 3 line corrected in place so that the load branch each invocation took is attributed rather than stated as a constant — the first invocation imported the persisted graph and the two after it opened the project that import had created. The same pass set `graph.loaded_with` per envelope in the three `queries/joern/results/*.json`, and documented the envelope's 22-key superset and the populated form of `stderr_ref` in `joern-probe.md` and the three per-query reports. No scanner was invoked, no query source changed and no recorded measurement moved; no raw artifact or log was touched and no row of the dataset changed | the audit's own generation timestamp |
| after the QA audit of `2026-08-22T15:10:02Z` | **record-accuracy pass**: three disclosures completed in place, each extending a statement this record already made. §3.4 gained the consequence its 22 `<manifest>?<package>` values carry for a consumer joining rows on `path`, and the rule that the `?` fragment is the tool's own value and is never stripped. §4.7's formula-character paragraph gained the three cells' identification by tool and rule, the embedded line-break census, and the RFC 4180 reader contract that census obliges. §7 gained `harness/lib/__pycache__/` beside `.ruff_cache/`, so both writes outside the four writable trees are inventoried and the boundary claim is exhaustive. No scanner was invoked, nothing was deleted or cleaned up, no raw artifact or log was touched and no row of the dataset changed; what the pass changed in **this** file is those three items, and the one change it made in another deliverable is the tool condition added to `tool-status.md` §4 — that `harness/bin/run-datadog-static-analyzer.sh` composes its credential-state string from an expansion whose set-arm yields the variable's own value, recorded there by variable name only and latent in this environment because neither variable is set, the mechanism §4.6 of this record already states at length; the header and §9 provenance table of that file name the runner as the source that condition cites | the audit's own generation timestamp. The counts the pass added were measured over the published `findings.csv` and `findings.json`; the `__pycache__/` facts are the filesystem observation §7 labels as one |

Twenty driver executions over eight distinct source texts is the measure §6 reports, and
the rounds are dated here because twelve of the twenty precede the publication pass this
record's header names — which is why condition 5 in §5 is scoped to the publication it
describes rather than asserted over every recorded execution.

## 2. Gate — twelve ordered checks

Fail-closed and ordered so that nothing is consumed before it is validated. **The gate ran
in both passes, and the table below says which pass executed each check**, because the two
are not interchangeable: the scanning pass ran it before any runner was invoked, and the
publication pass re-evaluated the same checks 2 h 19 min later, over a tree Phase 1 had by
then written into. The scanning pass reached Phase 1, and through a fail-closed gate it
could only have done so with all twelve passed, so all twelve held before `04:30:05Z`. What
the filesystem still carries is the *last* write to each directory, which is the
publication pass's probe, so the scanning pass's own probe is no longer separately
observable.

Two consequences are stated plainly rather than left to be reconstructed from timestamps:

* **In the publication pass the Tree-writability check ran after Phase 1, not before it.**
  Its probe is dated `06:48:44.855Z` (§1.1), 1 h 36 min after the last runner finished and
  one second before that pass's own timestamp. The gate precedes Phase 1 by design and the
  scanning pass satisfied that; the publication pass's re-evaluation did not, and could not
  have. The departure is bounded — that pass invoked no runner, and no scanner wrote
  anything after `05:12:06Z` — but it is a departure, and a table presenting all twelve as
  one ordered pre-Phase-1 gate would misstate it.
* **The `raw/` state check's verdict is the scanning pass's observation, restated.** By the
  publication pass the runners had written eight artifacts into that directory, so that pass
  could not observe it empty and did not: the value it carries is the one timestamped
  `04:29:07Z`. The scanning pass's own partial record — the 22853-byte `run-record.md` the
  collision precheck found in place (§1) — was written check by check and was still on disk
  when this pass began, but this record does not establish which store the value was read
  back from and does not assert one.

| # | Check | Verdict | Executed in | What it established |
|---|---|---|---|---|
| 1 | Interpreter modules | passed | both passes | the interpreter imports all ten required standard-library modules: `json`, `csv`, `re`, `os`, `sys`, `time`, `hashlib`, `pathlib`, `subprocess`, `urllib.parse` |
| 2 | JVM present | passed | both passes | `JAVA_HOME` → openjdk version "17.0.20" 2026-07-21; `JAVA_HOME_17` → openjdk version "17.0.20" 2026-07-21; `JAVA_HOME_21` → openjdk version "21.0.12.1" 2026-08-18 LTS |
| 3 | Record contents | passed | both passes | `harness/ENVIRONMENT.md` readable (35570 B, sha256 `976f487ec95e171011e1fd7fd8193581f88d465b32925f08bc8ab06b650e1fd7`) and carrying every field consumed later: nine tool versions, the environment file, the Opengrep taint setting (ENABLED), the per-module JAR outcomes, and the datadog AI-path availability (UNAVAILABLE) with its credential source `DD_API_KEY`/`DD_APP_KEY` |
| 4 | `$SPARK_SRC` resolution | passed | both passes | resolved from the sourced environment to `/opt/blitzy-harness/spark-src`; the record names `/opt/blitzy-harness/spark-src` |
| 5 | Commit identity | passed | both passes | `git -C "$SPARK_SRC" rev-parse HEAD` = `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, equal to the pinned commit; commit date `2025-10-23T15:31:06-04:00` from `git log -1 --format=%cI` |
| 6 | Glob compilation | passed | both passes | 12 allowlist patterns, all compiled by the tokenizer; the compiled rules are in §3.3 |
| 7 | Runner presence | passed | both passes | all nine `harness/bin/run-<tool>.sh` present and executable; the only other script in `harness/bin/` is `run-all.sh`, which is not a runner and was never invoked |
| 8 | Runner contract | passed | both passes | each runner's own text confirms the no-argument guard, a scan target taken from the scope helper rooted at the verified `$SPARK_SRC`, and an artifact path directly inside `harness/artifacts/raw/`; the per-runner reported-path bases recorded here are in §3.4 |
| 9 | Version | passed | both passes | each of the nine resolved on `PATH` at the version the record states; observed beside recorded below |
| 10 | `raw/` state | passed | scanning pass; **carried forward** into the publication pass | `harness/artifacts/raw` under the **execution-time root** — `/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d-w-000_a6fd4d/harness/artifacts/raw` — present and empty, established at `2026-08-22T04:29:07Z`, before any runner was invoked. The publication pass restates that value rather than re-observing it, because the runners had written into the directory by then |
| 11 | Tree writability | passed | both passes; the publication pass's probe ran at `06:48:44.855Z`, **after Phase 1** — the departure stated above | all four writable trees accepted a write and the probe was removed. The trees are the four under the execution-time root; §3.5 resolves them under both roots |
| 12 | Graph coverage | passed | both passes | `harness/cpg/spark.cpg` — a symlink resolving from every checkout to the same file, `/opt/blitzy-harness/cpg/spark.cpg` — loaded with `importCpg` and reporting 445568 methods, 57863 type declarations and 19500 files; per-module coverage below |

### Version check, observed beside recorded

| Tool | Observed on `PATH` | `harness/ENVIRONMENT.md` records | Agrees | Probe |
|---|---|---|---|---|
| `trivy` | `0.74.0` | `0.74.0` | yes | `trivy --version` |
| `osv-scanner` | `2.5.1` | `2.5.1` | yes | `osv-scanner --version` |
| `dependency-check` | `13.0.0` | `13.0.0` | yes | `<banner probe>` |
| `gitleaks` | `8.30.1` | `8.30.1` | yes | `gitleaks version` |
| `checkov` | `3.3.13` | `3.3.13` | yes | `checkov --version` |
| `opengrep` | `1.27.1` | `1.27.1` | yes | `opengrep --version` |
| `semgrep` | `1.174.0` | `1.174.0` | yes | `semgrep --version` |
| `joern` | `4.0.607` | `4.0.607` | yes | `<banner probe>` |
| `datadog-static-analyzer` | `0.9.1` | `0.9.1` | yes | `datadog-static-analyzer --version` |

`joern` has no `--version` flag, so it was probed with closed stdin and its `Version:`
banner line read, exactly as the record instructs.

### Graph coverage — the criterion, and the evidence for every module

The workspace was selected, the graph loaded with **`importCpg`** (never `importCode`), and
`cpg.method.size` established the non-zero count. Coverage was then asserted from
**injective evidence**: for each module the record marks as JAR-producing, its staged jar
was opened, its class names enumerated, and a class name carried by **no other jar** had to
appear as a `TYPE_DECL.fullName`. A shared package prefix is explicitly not evidence —
Spark modules all share `org.apache.spark`, so a prefix test would let one module's
bytecode vouch for a dozen absent ones. Where a module owns no such class the class-name
form of the test is *not evaluable* for it, and this section states which form of injective
evidence was used for every module rather than waiving the requirement for any of them.

| | |
|---|---|
| Graph | `harness/cpg/spark.cpg`, the per-checkout symlink; canonical target `/opt/blitzy-harness/cpg/spark.cpg`, which is the same file from every checkout and is therefore the form quoted here rather than a checkout-local absolute |
| Methods observed | 445,568 |
| Type declarations observed | 57,863 |
| Files observed | 19,500 |
| Jars considered | 32 |
| Modules covered by a class exclusive to their own jar | 31 |
| Modules covered by a module-exclusive coordinate-file witness | 1 — `sql/connect/shims` |
| Modules with a recorded jar and no bytecode | 0 |

**A record-versus-observed difference in the method count, reported and not**
**reconciled.** `harness/ENVIRONMENT.md` §7 records **445,567** methods; loading the
graph here reports **445,568**. Both values are stated. The record was not edited, the
graph was not rebuilt, and the difference is not treated as a coverage failure:
§7 of the record explains that the canonical graph is the overlay-applied graph
promoted after an import, and the count above is the count of the file this run
actually loaded. Every per-module verdict below comes from that same load.

The graph's identity diverges from the record in the same way, and is reported the same way:
`harness/ENVIRONMENT.md` §7 records 509,171,114 B with sha256
`16c40508128a148e20894aab3a1e5f082aa8ce05fec4f07869445bd5fbd931e7`, while the file this run
loaded is 509,105,796 B with sha256
`6b3b135ee79f67778918804e7ed46badb8716875b581e8726bb98ba7f1c5330b`. Both values are stated,
neither is reconciled, the record was not edited and the graph was not rebuilt.

| Module | Artifact id (`/project/artifactId`) | Classes in jar | Evidence | Witness class probed | Verdict |
|---|---|---|---|---|---|
| `common/kvstore` | `spark-kvstore_2.13` | 15 | exclusive to this jar | `org.apache.spark.util.kvstore.ArrayWrappers` | covered_injectively |
| `common/network-common` | `spark-network-common_2.13` | 102 | exclusive to this jar | `org.apache.spark.network.TransportContext` | covered_injectively |
| `common/network-shuffle` | `spark-network-shuffle_2.13` | 51 | exclusive to this jar | `org.apache.spark.network.sasl.ShuffleSecretManager` | covered_injectively |
| `common/network-yarn` | `spark-network-yarn_2.13` | 3 | exclusive to this jar | `org.apache.spark.network.yarn.YarnShuffleService` | covered_injectively |
| `common/sketch` | `spark-sketch_2.13` | 11 | exclusive to this jar | `org.apache.spark.util.sketch.BitArray` | covered_injectively |
| `common/tags` | `spark-tags_2.13` | 10 | exclusive to this jar | `org.apache.spark.annotation.AlphaComponent` | covered_injectively |
| `common/unsafe` | `spark-unsafe_2.13` | 26 | exclusive to this jar | `org.apache.spark.sql.catalyst.expressions.HiveHasher` | covered_injectively |
| `common/utils` | `spark-common-utils_2.13` | 85 | exclusive to this jar | `org.apache.spark.BreakingChangeInfo` | covered_injectively |
| `common/utils-java` | `spark-common-utils-java_2.13` | 34 | exclusive to this jar | `org.apache.spark.QueryContext` | covered_injectively |
| `common/variant` | `spark-variant_2.13` | 7 | exclusive to this jar | `org.apache.spark.types.variant.ShreddingUtils` | covered_injectively |
| `connector/avro` | `spark-avro_2.13` | 11 | exclusive to this jar | `org.apache.spark.sql.avro.AvroDataToCatalyst` | covered_injectively |
| `connector/protobuf` | `spark-protobuf_2.13` | 8 | exclusive to this jar | `org.apache.spark.sql.protobuf.CatalystDataToProtobuf` | covered_injectively |
| `core` | `spark-core_2.13` | 1287 | exclusive to this jar | `org.apache.spark.Aggregator` | covered_injectively |
| `graphx` | `spark-graphx_2.13` | 46 | exclusive to this jar | `org.apache.spark.graphx.Edge` | covered_injectively |
| `launcher` | `spark-launcher_2.13` | 20 | exclusive to this jar | `org.apache.spark.launcher.AbstractAppHandle` | covered_injectively |
| `mllib` | `spark-mllib_2.13` | 738 | exclusive to this jar | `org.apache.spark.ml.Estimator` | covered_injectively |
| `mllib-local` | `spark-mllib-local_2.13` | 12 | exclusive to this jar | `org.apache.spark.ml.impl.Utils` | covered_injectively |
| `repl` | `spark-repl_2.13` | 3 | exclusive to this jar | `org.apache.spark.repl.Main` | covered_injectively |
| `resource-managers/kubernetes/core` | `spark-kubernetes_2.13` | 77 | exclusive to this jar | `org.apache.spark.deploy.k8s.Config` | covered_injectively |
| `resource-managers/yarn` | `spark-yarn_2.13` | 34 | exclusive to this jar | `org.apache.spark.deploy.yarn.AmIpFilter` | covered_injectively |
| `sql/api` | `spark-sql-api_2.13` | 338 | exclusive to this jar | `org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction` | covered_injectively |
| `sql/catalyst` | `spark-catalyst_2.13` | 2389 | exclusive to this jar | `org.apache.spark.sql.catalyst.AliasIdentifier` | covered_injectively |
| `sql/connect/client/jdbc` | `spark-connect-client-jdbc_2.13` | 2 | exclusive to this jar | `org.apache.spark.sql.connect.client.jdbc.NonRegisteringSparkConnectDriver` | covered_injectively |
| `sql/connect/client/jvm` | `spark-connect-client-jvm_2.13` | 3 | exclusive to this jar | `org.apache.spark.sql.application.ConnectRepl` | covered_injectively |
| `sql/connect/common` | `spark-connect-common_2.13` | 480 | exclusive to this jar | `org.apache.spark.connect.proto.AddArtifactsRequest` | covered_injectively |
| `sql/connect/server` | `spark-connect_2.13` | 105 | exclusive to this jar | `org.apache.spark.sql.connect.SimpleSparkConnectService` | covered_injectively |
| `sql/connect/shims` | `spark-connect-shims_2.13` | 11 | no class exclusive to this jar; module-exclusive coordinate file present as a `FILE` node | `META-INF/maven/org.apache.spark/spark-connect-shims_2.13/pom.properties` | covered_by_coordinate_witness |
| `sql/core` | `spark-sql_2.13` | 1709 | exclusive to this jar | `org.apache.parquet.filter2.predicate.SparkFilterApi` | covered_injectively |
| `sql/hive` | `spark-hive_2.13` | 70 | exclusive to this jar | `org.apache.hadoop.hive.ql.exec.HiveFunctionRegistryUtils` | covered_injectively |
| `sql/hive-thriftserver` | `spark-hive-thriftserver_2.13` | 135 | exclusive to this jar | `org.apache.hive.service.AbstractService` | covered_injectively |
| `sql/pipelines` | `spark-pipelines_2.13` | 111 | exclusive to this jar | `org.apache.spark.sql.pipelines.AnalysisWarning` | covered_injectively |
| `streaming` | `spark-streaming_2.13` | 214 | exclusive to this jar | `org.apache.spark.status.api.v1.streaming.ApiStreamingApp` | covered_injectively |

**`sql/connect/shims` — the class-name form, and the injective witness the module does**
**admit.** Of the 32 staged jars this is the only one owning no class exclusively: it
carries 11 classes and every one of them is also shipped by `core` or `sql/core` (19,443
class entries across the 32 jars, 19,432 distinct names, and the 11 duplicated names are
exactly its own). `harness/ENVIRONMENT.md` §7 records the same fact and instructs that the
module be treated as covered. On class names alone the check is therefore *not evaluable*
for this module, and a run that stopped there would be applying the criterion literally.

This run did not stop, and the reason is evidence rather than the record's instruction. All
64 `META-INF/maven/**/pom.{xml,properties}` coordinate files across the 32 staged jars are
exclusive to exactly one jar each, and the graph carries
`META-INF/maven/org.apache.spark/spark-connect-shims_2.13/pom.properties` and its `pom.xml`
as `FILE` nodes — entries no other module's jar could have contributed. That is the property
the criterion exists to guarantee: no module's coverage claim resting on another module's
bytecode.

**What that witness does and does not establish.** It establishes that this module's jar was
an input to the graph build. It does not establish that its 11 stub classes are separately
represented, and the graph shows they are not: the frontend extracted all 32 jars into one
flat directory, so each duplicated name has a single extracted `.class` file, and both
`TYPE_DECL` nodes carrying such a name report that one file and the owning module's method
count — `SparkConf` 149, `SparkContext` 550, `RDD` 511, `JavaRDD` 37, `QueryExecution` 140,
`SessionState` 38, `SharedState` 64, `BaseRelation` 6, `ExperimentalMethods` 7,
`SparkSessionExtensions` 56, `ExecutionListenerManager` 58. The consequence is stated rather
than assumed benign: those 11 names are stubs whose implementations are present from their
owning modules, and the deploy-package handlers and sinks the Phase 3 probe queries live in
`core`, which is covered by a class exclusive to its own jar. Nothing was rebuilt, no record
was edited, and both readings are on the record so a reader can apply either.

## 3. Environment facts

### 3.1 The tree that was scanned

| | |
|---|---|
| `$SPARK_SRC` | `/opt/blitzy-harness/spark-src` |
| `git rev-parse HEAD` | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` |
| Pinned commit required | `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` |
| `git log -1 --format=%cI` | `2025-10-23T15:31:06-04:00`, which is `2025-10-23T19:31:06Z` in UTC |
| `harness/ENVIRONMENT.md` records | commit `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d`, date `2025-10-23T19:31:06Z` |
| Files under the allowlist | 4095 |

A second Spark checkout exists on this host — the repository this run writes into is one —
and it is **not** the scanned tree, is not scanned, checked out, reset or reconciled, and
its commit is not a mismatch to report. Only `$SPARK_SRC` counts.

### 3.2 The allowlist, as found

Read from `harness/scope/allowlist.txt`, sha256 `0013edf6cdc3a48d69aed5d7db41cc6647cfd461d348f5e1d563ba85664143d1`, and used exactly as found:

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

Each pattern was compiled to an anchored regex by an explicit tokenizer, never by string
substitution: `/**/` → `/(?:.*/)?`, a trailing `/**` → `(?:/.*)?`, a bare `**` → `.*`,
`*` → `[^/]*`, `?` → `[^/]`, and every ordinary character escaped so that `.`, `+`, `(`
and `$` cannot leak through as metacharacters. `fnmatch` and `PurePath.match` are not used:
neither gives correct recursive `**` semantics, and a mistranslated pattern would make
`in_scope` wrong in the one direction this dataset must never be wrong in.

| Allowlist pattern | Compiled regex |
|---|---|
| `core/src/main/**` | `^core/src/main(?:/.*)?$` |
| `common/network-common/src/main/**` | `^common/network\-common/src/main(?:/.*)?$` |
| `common/network-shuffle/src/main/**` | `^common/network\-shuffle/src/main(?:/.*)?$` |
| `common/network-yarn/src/main/**` | `^common/network\-yarn/src/main(?:/.*)?$` |
| `sql/catalyst/src/main/**` | `^sql/catalyst/src/main(?:/.*)?$` |
| `sql/core/src/main/**` | `^sql/core/src/main(?:/.*)?$` |
| `sql/connect/**/src/main/**` | `^sql/connect/(?:.*/)?src/main(?:/.*)?$` |
| `sql/hive/src/main/**` | `^sql/hive/src/main(?:/.*)?$` |
| `sql/hive-thriftserver/src/main/**` | `^sql/hive\-thriftserver/src/main(?:/.*)?$` |
| `resource-managers/kubernetes/**/src/main/**` | `^resource\-managers/kubernetes/(?:.*/)?src/main(?:/.*)?$` |
| `resource-managers/yarn/src/main/**` | `^resource\-managers/yarn/src/main(?:/.*)?$` |
| `python/pyspark/**` | `^python/pyspark(?:/.*)?$` |

`in_scope` is **true** when the canonicalized `$SPARK_SRC`-relative path matches at least
one of those regexes **and** does not contain the literal segment sequence `src/test/`. The
exclusion is applied exactly as written and is never broadened: a directory merely named
`tests`, of which the Python tree has several, sits outside any `src/test/` segment, so
whatever the allowlist reaches among them stays in scope under the rule as written.

### 3.4 Per-runner reported-path bases

The base a path is relative to is a property of the runner, not an assumption. Each was
read from the runner's own invocation at the Runner-contract check and is what the
canonicalizer used.

| Runner | Path-bearing field(s) | Base as read from the runner |
|---|---|---|
| `trivy` | `Results[].Target` | absolute — the runner passes $HARNESS_SCAN_ROOT, an absolute path |
| `osv-scanner` | `results[].source.path` | absolute — the runner passes $HARNESS_SCAN_ROOT |
| `dependency-check` | `dependencies[].filePath` | absolute — the runner passes --scan $HARNESS_SCAN_ROOT |
| `gitleaks` | `File` | the invoking process's working directory, which the controller sets to $SPARK_SRC (see the gitleaks CLI probe in the record) |
| `checkov` | `file_abs_path`, `file_path` | file_abs_path is absolute; file_path is relative to whichever -d scope directory produced the record, with a leading slash that denotes scan-root-relative rather than filesystem-absolute |
| `opengrep` | `locations[].physicalLocation.artifactLocation.uri` | absolute — the runner passes 18 absolute scope directories |
| `semgrep` | `locations[].physicalLocation.artifactLocation.uri` | absolute — the runner passes 18 absolute scope directories |
| `joern` | `findings[].path` | already $SPARK_SRC-relative — harness/lib/joern_collect.py maps the graph's bytecode class path back to source against $SPARK_SRC |
| `datadog-static-analyzer` | `locations[].physicalLocation.artifactLocation.uri` | relative to the analyzer's -i root, which the runner sets to $HARNESS_SCAN_ROOT |

**Three path shapes dependency-check reports that resolve to no file on disk, and are not
resolution failures.** The tool reads inside archives and reports what it found there, so
of its 1697 rows: **1607** carry an entry inside a jar, of the form
`<module>/target/scala-2.13/<jar>.jar/META-INF/maven/<group>/<artifact>/pom.xml` for 1562 of
them and `<module>/target/<jar>.jar/META-INF/maven/<group>/<artifact>/pom.xml` — the same
shape without the `scala-2.13/` element, for jars written directly to `<module>/target/` —
for the remaining 45, which are `core`'s eight Jetty coordinates and
`sql/connect/client/jvm`'s five Arrow ones over 13 distinct paths; a further **18** name a
jar nested inside another jar, such as
`sql/core/target/spark-sql_2.13-4.1.0-SNAPSHOT-test-sources.jar/SPARK-33084.jar`, over 2
distinct paths; and **22** carry a virtual coordinate of the form `<manifest>?<package>`,
which is the tool's way of naming a package declared by a manifest rather than a file of its
own, over 10 distinct paths. Each is canonicalized against the base above like any other
path, and the `?` is part of the value the tool emitted and is preserved verbatim. Together
that is 1647 rows over 58 distinct paths (46 + 2 + 10), 16.2% of the dataset, for which a
filesystem existence check under `$SPARK_SRC` fails by construction. The rows are kept and
the values are verbatim: a consumer resolving these paths against the filesystem should
read a miss as the shape of the value rather than as a defect in the canonicalization,
which applied the base recorded in this section to them exactly as to every other path.
The 22 carry one further consequence, for a consumer joining rows on `path`: they are not
pure filesystem paths, so an exact-string join gives each of their 10 distinct values a key
of its own, and the two manifests those values reduce to before the `?` —
`dev/package-lock.json` and `ui-test/package-lock.json` — also appear as bare `path` values
on 15 and 18 other rows of the published dataset, which such a join keeps separate from the
22. A consumer that wants the manifest joins on the substring before the first `?`; one that
wants the package joins on `package_coordinate`, which all 22 carry — the row whose `path`
is `ui-test/package-lock.json?@babel/core` carries `npm:@babel/core@7.23.3` — while
`start_line` is null on all 22, as it is on every dependency row. That is a statement about
the shape of a field's value and nothing more: this record relates no tool's rows to
another's. The `?` fragment is the value the tool itself emitted, it is preserved verbatim,
and it must not be stripped — stripping it would discard data the tool reported.

### 3.5 The four writable trees, resolved

Resolved under the **execution-time root**, which is the form to use when joining these
paths to the byte-preserved evidence: `EXEC` below stands for
`/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d-w-000_a6fd4d`, and
`SHIP` for the assembly root this record ships in,
`/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4`.

| Tree | Resolved at execution time | The same tree under the assembly root | Writable |
|---|---|---|---|
| `harness/artifacts/logs` | `EXEC/harness/artifacts/logs` | `SHIP/harness/artifacts/logs` | yes |
| `harness/artifacts/raw` | `EXEC/harness/artifacts/raw` | `SHIP/harness/artifacts/raw` | yes |
| `oss-scan-results` | `EXEC/oss-scan-results` | `SHIP/oss-scan-results` | yes |
| `queries/joern` | `EXEC/queries/joern` | `SHIP/queries/joern` | yes |

**Where each root comes from, and what a reader can check.** `EXEC` is the root the run's
own byte-preserved evidence names: every `harness/artifacts/logs/<tool>.meta.json` carries
the absolute invocation path of the runner it wrapped, and `joern.query-output.log` opens
with the same root. Two of them — `trivy.meta.json`'s `invocation` and the first line of
`joern.query-output.log` — were read in the execution checkout by the QA audit of this
milestone and name that root. They are **not re-readable from the shipped tree**, because
`.gitignore` line 31 (`artifacts/`) excludes both artifact trees from the commit, so the
audit trail lives on disk beside this record rather than inside git (§7). `SHIP` is the root
this record's own absolute forms were written as, and it is corroborated inside the commit:
the six Phase 3 result files carry Joern's `project_recorded_input_path` and
`project_directory` under it, while the same envelopes give
`project_recorded_input_path_canonical` as `/opt/blitzy-harness/cpg/spark.cpg` — the
checkout-independent form the graph resolves to from either root. The two roots are stated
and not reconciled: no value observed under one is restated as though it had been observed
under the other.

The two roots above are also the two the evidence trees themselves are identified under,
and resolving a tree is not the same as identifying the files in it: the sha256 manifest
that identifies each of the 8 artifacts and 28 log files, since a byte size does not, is in
§7 under *The evidence trees, by digest and by absolute path*.

**`queries/joern/.workspace/` is scratch inside a writable tree, and it is not ignored by
git.** Each Phase 3 query script selects that workspace before it loads the graph, and the
project Joern creates there holds its own copy of the half-gigabyte graph, so the directory
is unbounded — it stands at roughly 1 GB after this run's query executions. It is **not a
deliverable**: nothing in `joern-probe.md` or in any per-query report cites a file inside
it, and the three reports each say so. The only `.gitignore` rule that covers artifact
scratch is its line 31 (`artifacts/`), which reaches `harness/artifacts/**` and does not
reach this path, and no nested `.gitignore` exists under `queries/`; that file is
pre-existing and this run may not modify it, so the rule cannot be added here. The
consequence is stated for whoever commits: `queries/joern/.workspace/` sits at a
commit-eligible path, every permitted re-invocation of the Phase 3 driver regenerates it,
and it must not be committed. It is left in place rather than deleted, because this run
cleans nothing up.

### 3.6 Observed runtime versions

| Runtime | Observed | How |
|---|---|---|
| Python | 3.13.7 | the interpreter the controller runs in, `/opt/blitzy-harness/venv/bin/python3` |
| JVM (`JAVA_HOME`) | openjdk version "17.0.20" 2026-07-21 | `$JAVA_HOME/bin/java -version` |
| JVM (`JAVA_HOME_17`) | openjdk version "17.0.20" 2026-07-21 | `$JAVA_HOME_17/bin/java -version` |
| JVM (`JAVA_HOME_21`) | openjdk version "21.0.12.1" 2026-08-18 LTS | `$JAVA_HOME_21/bin/java -version` |
| `git` | git version 2.51.0 | `git --version`, used read-only against `$SPARK_SRC` |

These are recorded as observed, never as required.

## 4. Execution

### 4.1 The nine runners, individually and serially

Each runner was invoked with **no arguments**, one at a time, so its baked configuration is
what executed and each outcome is separately attributable. `harness/bin/run-all.sh` was
never invoked. No time limit was imposed and no runner was terminated for slowness. A
non-zero exit was recorded and the sequence continued.

| # | Runner | Started (UTC) | Elapsed | Exit | Artifact | Artifact bytes |
|---|---|---|---|---|---|---|
| 1 | `run-trivy.sh` | `2026-08-22T04:30:05Z` | 7.3 s | `1` | **none written** | — |
| 2 | `run-osv-scanner.sh` | `2026-08-22T04:30:12Z` | 29.3 s | `1` | `harness/artifacts/raw/osv-scanner.json` | 2801633 |
| 3 | `run-dependency-check.sh` | `2026-08-22T04:30:41Z` | 1755.9 s | `14` | `harness/artifacts/raw/dependency-check.json` | 7114893 |
| 4 | `run-gitleaks.sh` | `2026-08-22T04:59:57Z` | 66.0 s | `1` | `harness/artifacts/raw/gitleaks.json` | 31371 |
| 5 | `run-checkov.sh` | `2026-08-22T05:01:03Z` | 2.4 s | `1` | `harness/artifacts/raw/checkov.json` | 8470 |
| 6 | `run-opengrep.sh` | `2026-08-22T05:01:06Z` | 190.2 s | `0` | `harness/artifacts/raw/opengrep.sarif` | 1941724 |
| 7 | `run-semgrep.sh` | `2026-08-22T05:04:16Z` | 232.1 s | `0` | `harness/artifacts/raw/semgrep.sarif` | 1578299 |
| 8 | `run-joern.sh` | `2026-08-22T05:08:08Z` | 47.5 s | `0` | `harness/artifacts/raw/joern.json` | 38595 |
| 9 | `run-datadog-static-analyzer.sh` | `2026-08-22T05:08:55Z` | 190.4 s | `0` | `harness/artifacts/raw/datadog-static-analyzer.sarif` | 5676504 |

The sha256 that goes beside each artifact's bytes above is in the §7 manifest, together
with the absolute path of the tree those bytes were written into; the byte size alone does
not identify an artifact.

Every runner has three log files under `harness/artifacts/logs/`: `<tool>.stdout.log`,
`<tool>.stderr.log` and `<tool>.meta.json` carrying the invocation line, the working
directory, both timestamps, the elapsed seconds and the exit code. `run-joern.sh`
additionally writes its own `joern.query-output.log`, so the tree holds 28 files rather
than 27.

**One sizing decision, stated rather than left implicit.** `harness/bin/run-joern.sh`
takes its heap from the caller (`JAVA_OPTS=${JAVA_OPTS:--Xmx48g}`), and this container
has 3.8 GB of RAM and no swap. The runner was therefore invoked with
`JAVA_OPTS=-Xmx3g -Xss64m`, using the override the runner itself exposes.
Nothing about the tool's configuration — its baked query set, its graph, its scope —
was changed, and no flag was added to the tool: an out-of-memory kill would have been a
termination this run may not repeat, which is the outcome the sizing avoids.

### 4.2 Every tool that failed or terminated

A non-zero exit is not the same thing as a failure: three of the nine runners document
a non-zero code as the tool's own finding-bearing exit. Both kinds are listed, because
a reader cannot tell them apart from the number alone.

| Tool | Exit | Artifact written | What that exit means | Failure? |
|---|---|---|---|---|
| `trivy` | `1` | **no** | Trivy's own exit code, per the runner's header | **yes** |
| `osv-scanner` | `1` | yes, 2801633 B | the runner's header documents `0 = no vulns, 1 = vulns found` | no |
| `dependency-check` | `14` | yes, 7114893 B | Dependency-Check's own exit code, per the runner's header | **yes**, but a non-fatal one: the tool exited non-zero having written an artifact |
| `gitleaks` | `1` | yes, 31371 B | the runner's header documents `0 = no leaks, 1 = leaks found` | no |
| `checkov` | `1` | yes, 8470 B | the runner's header documents `0 = no failed checks, 1 = failed checks found` | no |

**`trivy` produced no artifact.** It ran for 7.3 s and exited `1`. Its own stderr
(`harness/artifacts/logs/trivy.stderr.log`) ends:

```
2026-08-22T04:30:06Z	INFO	[secret] If your scanning is slow, please try '--scanners vuln,misconfig' to disable secret scanning
2026-08-22T04:30:06Z	INFO	[secret] Please see https://trivy.dev/docs/v0.74/guide/scanner/secret#recommendation for faster secret detection
2026-08-22T04:30:10Z	WARN	[pom] Dependency version cannot be determined. Child dependencies will not be found.	details="https://trivy.dev/docs/v0.74/guide/coverage/language/java#empty-dependency-version"
2026-08-22T04:30:12Z	FATAL	Error	remote Maven repository returned 429 Too Many Requests for https://repo.maven.apache.org/maven2/com/google/cloud/bigdataoss/bigdataoss-parent/2.2.28/bigdataoss-parent-2.2.28.pom. Retry-After: 1800.
The repository blocks all subsequent requests from this IP until the block clears.
To avoid this, populate the local Maven cache before scanning (e.g. run `mvn dependency:resolve` and cache ~/.m2 in CI).
```

Stated rather than repaired: its parse status is `absent`, it contributes zero rows,
and the absence is **not** a finding count of zero. It was not re-invoked, its
configuration was not changed, no scope was narrowed to get it through, and no
substitute scanner was introduced.

**Two runners that completed reported conditions about their own analysis, in their own**
**logs and nowhere else.** Neither is a failure of the sequence, neither changed what was
invoked, and neither is a characterization of anything either tool reported. They are
recorded here because an exit code and a row count cannot show them, and between them these
two tools contributed 1985 of the dataset's 10178 rows (288 and 1697), so a reader weighing
those counts needs to see what the tools said about their own reach:

* **`osv-scanner`** reported **85 resolution failures**, each of them a line beginning
  `failed resolution for`, over **43 distinct `pom.xml` files** in the scanned tree and
  naming **16 distinct** `org.apache.spark` coordinates at `4.1.0-SNAPSHOT` as not found —
  first at `harness/artifacts/logs/osv-scanner.stderr.log:59`, last at
  `harness/artifacts/logs/osv-scanner.stderr.log:190`. It also reported one extraction error
  at `harness/artifacts/logs/osv-scanner.stderr.log:148`, and
  **36 packages filtered from the scan** at
  `harness/artifacts/logs/osv-scanner.stderr.log:56`.
* **`dependency-check`** reported **three of its analyzers uninitialized or disabled**:
  the **Ruby Bundle Audit Analyzer** at
  `harness/artifacts/logs/dependency-check.stdout.log:26` and again at
  `harness/artifacts/logs/dependency-check.stdout.log:66`;
  the **.NET Assembly Analyzer** at
  `harness/artifacts/logs/dependency-check.stdout.log:31`, with the runtime it wanted named
  at `harness/artifacts/logs/dependency-check.stdout.log:32`;
  and the **Sonatype OSS Index Analyzer**, disabled for missing credentials, at
  `harness/artifacts/logs/dependency-check.stdout.log:59` — a line that carries no
  credential value and names no variable, so none is recorded. It further reported ten
  warnings over Node manifests analysed with no `node_modules` directory
  (`harness/artifacts/logs/dependency-check.stdout.log:34`, `…stdout.log:35`,
  `…stdout.log:37`, `…stdout.log:39`, `…stdout.log:41`, `…stdout.log:43`) or no lock file
  (`…stdout.log:36`, `…stdout.log:38`, `…stdout.log:40`, `…stdout.log:42`), and
  **five CVSS vectors** in NPM Audit results it reported as an unsupported format
  (`…stdout.log:52`, `…stdout.log:53`, `…stdout.log:54`, `…stdout.log:55`,
  `…stdout.log:56`).

Each condition is itemized against its tool in `tool-status.md` §2.2 and §2.3, beside that
tool's counts. Nothing follows from recording them here: no tool was re-invoked, no
configuration was changed, no count was adjusted, nothing was installed or credentialed to
lift a limit either tool reported, and `harness/bin/**` and `harness/ENVIRONMENT.md` were
read and not edited.

### 4.3 Every module `harness/ENVIRONMENT.md` records as producing no JAR

`harness/ENVIRONMENT.md` §6 states **BUILD SUCCESS** with 33 reactor modules and records
that every in-scope module produced a JAR, `python/pyspark` being `n/a` because a Python
package produces none by nature. This run builds nothing, so it could not have corrected a
module that produced none; the outcome is read from the record and restated here.

**No module is recorded as producing no JAR, and none was found absent from the**
**graph.** The reason this matters: a module with no JAR contributes no bytecode to the
code-property graph, and Joern silence over it would be indistinguishable from an
absence of findings. The Graph-coverage check in §2 is where that possibility was
tested against the graph itself, module by module.

### 4.4 Paths reported from outside `$SPARK_SRC`

**None.** Every path-bearing value in every artifact resolved inside `$SPARK_SRC`
against the base recorded in §3.4, so no row carries a `../` segment and no row was
emitted with an absolute path.

### 4.5 One tool-behaviour observation that determined how every runner was invoked

`harness/bin/run-gitleaks.sh` expands the allowlist to 18 absolute directories under
`$SPARK_SRC` and passes them as positional arguments to `gitleaks dir`. Gitleaks' own
usage is `gitleaks dir [flags] [path]` — **one** optional path. Probed directly on
synthetic files outside both trees, `gitleaks` 8.30.1 behaves as follows:

| Invocation | What it scanned | How it reported paths |
|---|---|---|
| one absolute path argument | that path | absolute, as given |
| two or more absolute path arguments | **the process's current working directory** | relative to that working directory |

So the tree `gitleaks` reads is the working directory of whoever invokes the runner, and
its reported paths are relative to it. The controller therefore invoked **all nine**
runners with their working directory set to `$SPARK_SRC` (`/opt/blitzy-harness/spark-src`).
Consequences, stated in full:

* Every tool read the pinned tree, which is what `harness/ENVIRONMENT.md` §8 records for
  all nine (*"Scans `$SPARK_SRC`"*), so there is no record-versus-reality disagreement
  about the tree scanned, and `gitleaks`' reported paths are `$SPARK_SRC`-relative by
  construction rather than by assumption.
* Nothing was changed to achieve that: no runner was edited, no flag was added, no
  ruleset was swapped, and `harness/ENVIRONMENT.md` was not touched. A working directory
  is a property of an invocation, not a scanner's configuration.
* `gitleaks` consequently reads the **whole** pinned tree rather than the 18 allowlist
  directories its arguments name — including `src/test/`, `docs/` and the untracked
  `*/target/` build output a previous run left in place. That is a runner reaching outside
  the allowlist, which is expected behaviour and never grounds to drop a row: those
  findings are kept with `in_scope: false`, exactly as the allowlist rule in §3.3
  determines. The eight other runners restrict themselves to the 18 directories, so this
  affects the `in_scope` mix of one tool's rows and nothing else.

### 4.6 The two record-versus-reality checks that only Phase 1 can make

| Check | Recorded | Observed | Agrees |
|---|---|---|---|
| Opengrep taint | `harness/ENVIRONMENT.md` §5: ENABLED, `--taint-intrafile --dataflow-traces` | the runner echoed those flags: True; taint reasoning present in the tool's own output (`Taint comes from`, `This is how taint reaches the sink`, `taint`) | yes |
| datadog AI path | `harness/ENVIRONMENT.md` §5: UNAVAILABLE, credential source `DD_API_KEY` and `DD_APP_KEY` | the runner reported the path UNAVAILABLE; the analyzer's own banner reads `secrets enabled         : false` | yes |

Neither credential exists in this environment and no value was read: only the variable
names appear, here and in the logs.

**Three further observations about the runners, reported and not acted on.** None changed
what was invoked, and none is a fault this run may repair: `harness/bin/**` is read-only
to it. They are not additional record-versus-reality checks — the two checks this phase
makes are the two in the table above.

* `harness/bin/run-dependency-check.sh` and `harness/bin/run-checkov.sh` each give the tool
  a `mktemp -d` output directory and move the report into `harness/artifacts/raw/`
  afterwards. The artifact this run records therefore resolves inside the audit boundary,
  which is what the gate's runner-contract check tests, while the tool's own first write
  lands outside it. The intermediate write is stated here so a reader knows it happened.
* `harness/bin/run-datadog-static-analyzer.sh` builds its credential-state string from the
  expansion pair `${DD_API_KEY:+set}${DD_API_KEY:-absent}`. When the variable is set the
  first expansion yields `set` and the second yields the variable's own value, so a
  credentialed environment would write that value into retained stdout. Both variables are
  absent here, so the branch printed `absent` and no value could have been emitted: the
  defect is latent, not realised, and it is reported by variable name only. Printing a fixed
  `set`/`absent` token instead of an expansion that can yield the value is a change only the
  owner of that file can make.
* `harness/bin/run-datadog-static-analyzer.sh` ran the analyzer against a tree carrying no
  local static-analysis configuration, so the tool fetched its rules over the network while
  it ran: its own stdout states that no SAST configuration was detected and the default
  rules were taken from the Datadog API
  (`harness/artifacts/logs/datadog-static-analyzer.stdout.log:8`), that the config method
  was `none` — no local file and no remote configuration
  (`…stdout.log:16`) — and that the set was 1093 static-analysis rules, all 1093 evaluated
  (`…stdout.log:19`, `…stdout.log:42`). That tool contributed 6832 of the dataset's 10178
  rows, so two thirds of the dataset rests on a rule set that nothing in the recorded
  environment pins: `harness/ENVIRONMENT.md` §5 records this tool's rules as bundled and
  carries no commit or digest for them, unlike its Opengrep and Semgrep CE rows. Both sides
  are reported and neither is reconciled, here and at greater length in `tool-status.md`
  §2.9, and `harness/ENVIRONMENT.md` is read and not edited. The consequence for a reader
  of the counts is that the same runner, with the same baked configuration and no
  arguments, may at a later date load a different rule set and emit a different row count
  with no recorded revision to tell the two apart.

### 4.7 Publication state

All three outputs were staged first, both assertions were evaluated against the staged
files, and only then were they renamed into place, in this order:

1. `oss-scan-results/severity-map.md`
2. `oss-scan-results/findings.csv`
3. `oss-scan-results/findings.json`

The order is deliberate: the presence of `findings.json` is the single signal that the
dataset **and** its mapping are both complete. No staging file remains — all three were
renamed away on success.

**The dataset was published twice, through that same protocol both times, and both
publications are dated in §1.1.** The first is the one committed at `2026-08-22T07:54:28Z`,
carrying `findings.json` at 5817891 B, sha256 `ff166c86a89eef49…`, `findings.csv` at
3320044 B, `91a84eaa03626819…`, and `severity-map.md` at 4449 B, `e456b520054f4f2b…`. The
second is what the files on disk now are, and it completed at or before
`2026-08-22T10:08:29Z` — the instant the Phase 3 driver recorded observing `findings.json`
at 5806988 B, sha256 `2b3fb2db…`, `findings.csv` at 3309257 B, `68ae2e4e…`, and
`severity-map.md` at 6049 B, `ebf11a85…`.

The code
review of this milestone established three defects in the normalized values, each a
deviation from the severity and SARIF derivation rules rather than a loss or duplication of
rows: `osv-scanner`'s severity was taken from the CVSS **vectors** in `severity[]` instead
of the label in `database_specific.severity`, so 126 of its 288 rows normalized to `Info`
where the label maps to Critical, High or Low; the shared SARIF adapter mined
`properties.tags` for CWE identifiers on `opengrep` and `semgrep` but not on
`datadog-static-analyzer`, leaving 61 rows without an available `CWE:<n>`; and six
`message` values were whitespace-stripped rather than verbatim. The three were corrected at
the adapter level and both files were re-serialized from one validated row list, re-staged,
counted again by parsing the staged files, and renamed in the same order above. The
correction changed 423 cells across 296 rows — 230 `severity_native` and 126
`severity_norm` on `osv-scanner`, 61 `cwe` on `datadog-static-analyzer`, and 6 `message` —
and changed no row count, no row order and no other field: 10178 rows before and after,
CSV and JSON equal, and every per-tool reconciliation unchanged. `severity-map.md` was
regenerated from the same mapping the adapters read, so the published mapping is the one
the rows receive, and the Phase 3 envelopes cite the second publication's bytes and
sha256 because the driver observed the dataset it ran against. The driver rounds that ran
**before** that second publication — twelve of its twenty recorded executions, dated in
§1.1 — observed the first, and §5's condition 5 is scoped accordingly.

**Three cells of `findings.csv` begin with a character a spreadsheet reads as a formula.**
Two `message` values begin with `@` and one with `-`, all three verbatim from the tool that
reported them: the two `osv-scanner` rows whose `rule_id` is `GHSA-4x5r-pxfx-6jf8` and
`GHSA-vpq2-c234-7xj6`, and the `dependency-check` row whose `rule_id` is
`GHSA-rf6f-7fwh-wjgh` — named by tool and rule rather than by row number so the statement
cannot drift, and carrying the same three values in `findings.json`. The verbatim rule also
leaves line breaks inside cells: `message` is the tool's own text unaltered, so 99 cells of
the published file carry a line feed — every one of them a `message`, 2581 line feeds in
all — and one carries two carriage returns with no line feed beside them, the `osv-scanner`
row whose `rule_id` is `PYSEC-2024-48`. Neither the formula-leading characters nor the line
breaks are escaped, prefixed or neutralized, and that is deliberate: the row contract fixes
the dialect, requires `message` to be the tool's own description verbatim, and requires the
CSV and JSON to agree cell for cell, so altering the value would break two of those three at
once. The file is a data artifact to be read by a parser, not a spreadsheet; a reader who
opens it in one should disable formula interpretation on import. A reader that parses it must
use an RFC 4180-compliant reader rather than split on newlines, because the dialect quotes
every embedded break and the 10178 rows are consequently spread over 12760 line-feed bytes:
10179 of those are the CRLF record terminators the dialect writes — the header plus the 10178
rows — and the other 2581 sit inside quoted cells. Two failure modes follow, both measurable
on the published bytes: splitting on the line feed yields 12760 lines where there are 10178
rows, and reading the file in Python's default universal-newline mode still yields 10178 rows
but silently rewrites 1 of its 122136 cells, turning that one row's two carriage returns into
line feeds. In Python the compliant form is `open(path, newline='')` with `csv.DictReader`,
which is how this run counted the staged file before publishing it — `csv.DictReader` after
the twelve-column header was validated, recorded in `tool-status.md` §7 — and under it all
122136 cells agree with `findings.json` exactly.

**The row contract both serializations share.** Every row carries these twelve fields in
this fixed order, and the CSV header is that order verbatim:

    tool, scanner_class, rule_id, message, severity_native, severity_norm, path, start_line, cwe, cve, package_coordinate, in_scope

Five of them may be absent — `severity_native`, `start_line`, `cwe`, `cve`,
`package_coordinate` — written as JSON `null` and as an empty CSV field. The other seven are
always present and non-null, and four of those are derived rather than read from a tool's
output: `tool`, `scanner_class`, `severity_norm` and `in_scope`. The JSON key order is the
same order as the CSV header, so the two files join field by field, and nothing downstream
should extend or reorder either.

**How `package_coordinate` was derived, stated because it governs 1985 published cells and**
**puts the field separator inside one of the parts.** The field has exactly one format,
`<ecosystem>:<name>@<version>`, with `ecosystem` lower-cased, and it is derived from the two
tools that report a package rather than only a file.

*From `dependency-check`,* from `dependencies[].packages[].id`, which the tool emits as a
Package URL of the form `pkg:<type>[/<namespace>]/<name>@<version>`. The `<type>`
lower-cased becomes the ecosystem; every segment between the type and the last `@` is
percent-decoded and joined with `:`; the substring after the last `@` is the version. Maven's
PURL namespace is its groupId, so a maven coordinate reads `maven:<group>:<artifact>@<version>`
— `pkg:maven/ch.epfl.scala/bsp4j@2.1.1` becomes `maven:ch.epfl.scala:bsp4j@2.1.1`. This
artifact carries 137 `packages[]` entries, 126 of them maven PURLs and 11 npm; each of the 11
npm entries carries a single name segment and no namespace, and 2 of those segments are
percent-encoded scoped-package names, so `pkg:npm/%40babel%2Fcore@7.23.3` becomes
`npm:@babel/core@7.23.3` with no namespace colon. Of the 131 dependencies that carry a
vulnerability, 96 carry exactly one `packages[]` entry and 35 carry none, so no selection
among several entries arose; the 45 vulnerabilities on those 35 dependencies have no formable
coordinate and are the 45 rejects, never a placeholder version.

*From `osv-scanner`,* from the package object: `ecosystem` lower-cased — the artifact's
`Maven`, `npm`, `PyPI` and `RubyGems` become `maven`, `npm`, `pypi` and `rubygems` — with
`name` and `version` taken verbatim. OSV's own maven name is already `<group>:<artifact>`, so
maven coordinates derived from the two tools are joinable, and that is also why `<name>`
contains a colon for maven and for no other ecosystem present here.

**The consequence a downstream parser must be told.** `<name>` carries the field separator on
every maven row, so a coordinate is parsed by splitting on the **first** `:` for the ecosystem
and taking the **last** `@` as the name/version boundary; splitting on every `:` mis-parses
the maven rows, and taking the first `@` mis-parses the 4 rows whose npm scoped name contains
one (2 from each tool). Both rules are exercised by the published data: all 1985 coordinates
parse under first-colon plus last-`@`, and 0 fail.

| Tool | Rows with a coordinate | maven | npm | pypi | rubygems |
|---|---|---|---|---|---|
| `dependency-check` | 1697 | 1675 | 22 | 0 | 0 |
| `osv-scanner` | 288 | 118 | 33 | 132 | 5 |
| **Total** | **1985** | **1793** | **55** | **132** | **5** |

The remaining 8193 rows carry `null`, being findings about a file rather than a package. Exactly
the 1793 maven rows have a `:` inside `<name>`, and no other row does. This rule is stated here
rather than in `severity-map.md`, which is serialized from the severity mapping the adapters
read and carries severity policy only.

## 5. Where the run reached, condition by condition

This record does not claim the run wholly succeeded or wholly failed. Six conditions
define completion and each is reported on its own.

| # | Condition | Verdict |
|---|---|---|
| 1 | Every tool ran once with its baked configuration, to completion or to a termination outside this run's control, each with a log carrying stdout, stderr, elapsed time and either an exit code or `exit_status: timeout`; every tool that wrote output has a raw artifact, and a tool that wrote none is recorded with parse status `absent`, its exit code and its stderr, contributing zero rows | **passed.** all 9 runners invoked once, serially, with no arguments; 9 of 9 carry stdout, stderr and a meta.json with elapsed time and an exit code; 1 wrote no artifact and is recorded with parse status `absent`, its exit code and its stderr |
| 2 | `findings.json` and `findings.csv` contain every row from every artifact, each carrying `tool`, `scanner_class`, `severity_norm` and `in_scope`, with no row dropped; row validation passes; and the per-tool reconciliation assertions pass | **passed.** `findings.json` and `findings.csv` published from one validated row list; row validation passed over 10178 rows; every evaluable per-tool reconciliation assertion passed; the CSV and JSON row counts are equal (10178 == 10178) |
| 3 | `severity-map.md` carries a row for all nine tools, including any that produced no finding | **passed.** `severity-map.md` carries one row for 9 of the nine tools, including those that produced no finding |
| 4 | `tool-status.md` lists all nine, including any that failed or timed out, each with its parse status, its records parsed and rejected, and its row-validation result | **passed.** this file carries one block for each of the nine, each with its execution state, exit status, parse status, records parsed and rejected, both reconciliation assertions and the row-validation result |
| 5 | Phase 3 delivers three or more committed queries with recorded outcomes, spurious-return counts and the three effort measures, and the graph was read rather than built | **delegated.** delegated to the Phase 3 driver by design, which appends its outcome to `run-record.md` §6 and reports it in full in `joern-probe.md`. **Scoped to the publication it describes:** for the published dataset, the driver ran after both records were finalized — its envelopes record observing `findings.json` (10178 rows, 5806988 B, sha256 `2b3fb2db…`), `findings.csv` (`68ae2e4e…`) and `severity-map.md` (`ebf11a85…`), the bytes now on disk, at `2026-08-22T10:08:29Z` for query 01 and `10:28:10Z` for queries 02 and 03. Twelve of the twenty recorded executions — rounds 1–4, `06:08:04Z` to `06:33:14Z` in the per-query revision logs — precede the publication pass this record's header names and ran against the first publication, which the second superseded. So the ordering rule holds for the dataset that shipped, and not for every recorded execution; §1.1 dates both |
| 6 | `run-record.md` states the `$SPARK_SRC` path scanned, its commit and date, and every tool failure and missing module | **passed.** `run-record.md` states the `$SPARK_SRC` path scanned with its commit and commit date, every tool failure and termination, and the missing-module answer |

**No check, assertion or publication step ended the run.** The gate's twelve checks
passed, Phase 1 invoked all nine runners once, and Phase 2 validated, staged, counted
and published in order. Condition 5 belongs to the Phase 3 driver, which the shell
launches after this controller exits, and the driver's own line follows in §6.

Two qualifications on the six verdicts above, so that none of them reads wider than the
evidence. They are verdicts **for the dataset that shipped**: the gate passed in both
passes, but in the publication pass the Tree-writability check ran after Phase 1 and the
`raw/` state check restated the scanning pass's observation, both as §2 records; and the
driver rounds that preceded the second publication ran against the first, as condition 5
now says. `tool-status.md` restates these six conditions and carries condition 5 in the
earlier, unscoped wording, as does `joern-probe.md`'s own statement of the same ordering;
the scoping above is this record's, and the record-accuracy pass that made it (§1.1) edited
no deliverable other than this record.

## 6. Phase 3 driver

The Phase 3 driver writes exactly one line into this section, and it is the driver's only
write to this file. It never writes to `tool-status.md`. A re-invocation for a query
revision replaces this line rather than adding another.

**Phase 3 completed.** The driver was launched by the outer shell after the controller exited cleanly, took the published `findings.json` (10178 rows, sha256 `2b3fb2dbb5c2f30c711524a5a0be141aab8445e00814a7fdf6f8ba6c6f664f51`) as its precondition — observed at `2026-08-22T10:08:29Z` for query 01 and `10:28:10Z` for queries 02 and 03, which is the second publication and the bytes on disk — and invoked 3 committed query scripts — `01-callgraph-unguarded-driver-launch`, `02-dataflow-unguarded-driver-launch`, `03-parameterized-unguarded-handler-sink` — from the repository root, one at a time, on `JAVA_HOME_21` with `JAVA_OPTS=-Xmx48g -Xss64m`, reading the pinned graph and never building one: `01-callgraph-unguarded-driver-launch` loaded it with `importCpg`, and the two invocations after it opened the provenance-verified project that import had created in the shared workspace, `importCode` appearing in none of the three sources. All 3 compiled and ran to a complete result region; 3 clean positive(s) were produced; the aggregate revision measure is 8 distinct source texts over 20 recorded executions (`01-callgraph-unguarded-driver-launch` 2 texts / 6 executions, `02-dataflow-unguarded-driver-launch` 3 / 7, `03-parameterized-unguarded-handler-sink` 3 / 7), the revisions after the first sequence being the graph-provenance hardening the code review of this milestone required and, for two of the three sources, a comment-separator correction to it. Done-when condition 5 is met, and the per-query outcomes, spurious counts and the three effort measures are in `oss-scan-results/joern-probe.md`.

## 7. Provenance

| Source | What came from it |
|---|---|
| `harness/ENVIRONMENT.md` | the nine recorded tool versions the Version check compared against; the environment file name; the Opengrep taint setting; the per-module JAR outcomes; the datadog AI-path availability and its credential-source variable names |
| `harness/artifacts/logs/*` | every timestamp, elapsed time, exit code and `exit_status`; each failing tool's own stderr; the taint and AI-path observations; the datadog rule-set provenance and rule count in §4.6, cited there by log line; the two runners' own tool conditions in §4.2, cited there the same way |
| `harness/artifacts/raw/*` | every artifact shape, path form and record count, and the per-tool row and reject counts |
| `git` reads of `$SPARK_SRC` | the commit, the commit date |
| the graph itself, loaded with `importCpg` | the method, type-declaration and file counts and every per-module coverage verdict |
| `queries/joern/results/*.json` | the Phase 3 driver's own captures: the per-query revision logs behind every driver round dated in §1.1, the precondition digests it observed, and Joern's recorded project paths cited in §3.5 |
| `git` reads of **this** repository | the two publication commit dates in §1.1 and the bytes and digests each commit carries; the three parent-commit sizes in §1 |
| filesystem observations, named as such at each use | the probe time in §1.1 and §2, and the two publication mtimes in §1.1 — the values this run's own logs do not carry, read by the QA audit of this milestone and labelled with that source class wherever they appear |

Where a value could not be read it is recorded as not read rather than substituted.
What this run wrote under `harness/`, and what it did not, as four separately checkable
facts. `harness/artifacts/raw/` was found present and empty and only the nine runners wrote
into it — 8 artifacts, one for each tool that produced output. `harness/artifacts/logs/`
carries 28 files, three per runner from the sequencer plus the query-output log
`run-joern.sh` writes itself. Both trees are excluded from the commit by the pre-existing
`.gitignore` line 31 (`artifacts/`), so the audit trail is preserved on disk beside this
record rather than inside git, and every count and citation in this file and in
`tool-status.md` was taken from it. Every other path under `harness/` — `ENVIRONMENT.md`,
`scope/allowlist.txt`, `bin/**`, `lib/**` and `cpg/**` — was read and not modified, with the
one qualification the second inventory entry below states: `lib/` holds a `__pycache__/`
directory none of this run's own logs accounts for, while every file git tracks beneath
`lib/` stands as committed. The smoke tree the setup run left under the shared harness root,
`harness/artifacts/smoke/`, was never read and is never a fallback for a runner that
produced nothing. Third-party cache and temporary state that the tools themselves wrote
outside these trees is expected, is not inventoried here and was not cleaned up.

**Two writes outside the four writable trees stand in this checkout, and each is inventoried**
**rather than left to that sentence. The first was made by a tool this run chose to invoke.**
`ruff` 0.16.4 — the linter the harness's own Python is checked with, from the isolated lint
environment — writes a `.ruff_cache/` directory into the working directory it runs in, which
is the repository root. It holds `CACHEDIR.TAG`, a versioned cache file under `0.16.4/`, and
a `.gitignore` of its own whose `*` makes the directory self-ignoring, so nothing in it is
commit-eligible. That is a write outside the writable trees by something other than the nine
scanners, so naming it here is half of what makes the boundary statement above exhaustive.
It was **not** cleaned up and must not be: this run cleans nothing up, and the no-cleanup
rule applies to its own leavings exactly as it does to the tools'. A later run that wants the
boundary clean can point the linter's `--cache-dir` outside the checkout; deleting the
directory afterwards would be the cleanup the rule forbids.

**The second is a `__pycache__/` directory inside `harness/lib`, and this record inventories**
**it without attributing it to a process it can name.** `harness/lib/__pycache__/` holds
exactly two files — `joern_collect.cpython-313.pyc`, 10768 B, and
`smoke_verify.cpython-313.pyc`, 7679 B, both mode `0644` owned `root:root`, with an mtime of
`2026-08-22T07:54:46Z`. Source class: a filesystem observation, stated as one because this
run's own logs record no such write; the `cpython-313` tag names the interpreter series that
produced the bytecode and nothing further about the process. `harness/lib` is one of the
trees this run reads and never modifies, and the tracked tree is unmodified:
`git status --porcelain -- harness/` reports nothing, so all four files git tracks there
stand as committed. Neither `.pyc` is commit-eligible, on the same footing as `.ruff_cache/`
and for the same kind of reason — both are matched by the pre-existing `.gitignore` line 6
(`*.pyc`), which `git check-ignore -v` names for each of them by name,
`git status --porcelain --ignored` reports the directory as `!! harness/lib/__pycache__/`,
and neither file is tracked.

What can be established about its origin is bounded, and is stated as bounded. It is **not**
one of the nine runners' writes. All nine reach `harness/lib`, each sourcing `lib/scope.sh`,
but that is a shell helper and sourcing it compiles nothing; the only Python under
`harness/lib` any runner runs is `joern_collect.py`, which `run-joern.sh` invokes at its line
54 as `python3 "$COLLECT" …`, the script named on the command line — the form CPython does
not byte-compile, since it writes `__pycache__` for the modules it imports and not for the
script it is given. No runner in `harness/bin/` names `smoke_verify.py` at all. And the
execution checkout the nine were invoked from, the `…-w-000_a6fd4d` root §3.5 and the
subsection below both name, holds no `__pycache__` under `harness/lib` whatsoever.

What the two names do line up with exactly is the two `.py` files in `harness/lib`, which
`harness/ENVIRONMENT.md` records as passing `py_compile` — the operation that writes
precisely this directory — so the shape of the write is a `py_compile` or an import of those
two modules, made in this checkout at an mtime §1.1's log places 18 s after the first
publication commit. Which process made it is not established by anything this run holds,
and is therefore not asserted here. Like
`.ruff_cache/` it was **not** cleaned up and must not be: this run cleans nothing up, and
deleting it would be both the cleanup the rule forbids and a write into a tree this record
states it does not modify.

Nothing else was added anywhere. Nothing was added to `.github/workflows/`, no test
framework was introduced, and no scanner was installed, upgraded, substituted,
reconfigured or re-invoked. Untracked state left in either tree by anything else running
on this host was left exactly as found.

**On the absolute paths in this record, and what a reader can re-check.**
Repository-relative paths anchor at the directory that holds `harness/`, and that is the
form to rely on. Two absolute roots appear, each labelled where it is used and neither
reconciled into the other: the **execution-time root**, where the nine runners were invoked
and where the byte-preserved evidence sits, and the **assembly root**, the checkout this
record and the Phase 3 envelopes were written in and ship in. Both are named in the header
and resolved tree by tree in §3.5.

What that means for checking this record. The evidence under `harness/artifacts/logs/` was
not edited, so each `<tool>.meta.json` carries the absolute invocation path exactly as it
stood at execution time — and that is where the execution-time root is read from. Those
files are excluded from the commit by `.gitignore` line 31, so a reader who has only the
shipped tree cannot re-read them and cannot re-derive that root; a reader with the execution
checkout can, from any one of the nine. Inside the commit, the two independently checkable
values are the assembly root, carried by Joern's own project fields in the six Phase 3
result files, and the graph's canonical target `/opt/blitzy-harness/cpg/spark.cpg`, which
those same files record and which is the same file from either root. Every timestamped gate
check in §2 names the root its observation was made under, so a value from one root is never
presented as an observation made under the other.

The two evidence trees are the one place where that convention is not enough to find
anything, because they are excluded from the commit and are clone-local on disk: the
subsection below names the root they were written from and the root that holds the
byte-identical copy, and identifies every file in both by digest.

### The evidence trees, by digest and by absolute path

Every count, shape, path form, timestamp, exit code and citation in this file and in
`tool-status.md` was taken from two trees, and this subsection is where they are identified —
by digest, not by location and not by size.

**Where they are.** The trees this run wrote are
`/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d-w-000_a6fd4d/harness/artifacts/raw`
and `…-w-000_a6fd4d/harness/artifacts/logs`. That root is not asserted from memory: it is the
root named by the `invocation` field of each of the nine byte-preserved
`harness/artifacts/logs/<tool>.meta.json` files — all nine name it, and those files were not
edited, so they carry the execution root exactly as it stood when the runners ran. A
**byte-identical copy** stands at
`/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/artifacts/raw`
and `…_343ca4/harness/artifacts/logs` — the **assembly root**, the checkout this record
ships in and the one its remaining absolute forms are labelled with; §3.5 resolves all four
writable trees under this root and under the execution-time root alike. The two were compared file by file rather
than in aggregate: **8 of 8** artifacts and **28 of 28** log files are equal by sha256, with
no file present in one tree and absent from the other.

**Identity is the digest.** A byte size does not identify an artifact, and the collision is
not hypothetical: at the time this subsection was written a sibling checkout of this
repository on this host,
`/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d-w-002_c982c2`, held a
`harness/artifacts/raw/` from a **different** execution in which `dependency-check.json` and
`joern.json` carry exactly the byte sizes in the manifest below under different sha256. The
manifests are therefore the identifying record, and the sizes in them are the same sizes §4.1
and `tool-status.md` §2 state beside each artifact.

The 8 raw artifacts:

| File | Bytes | sha256 |
|---|---|---|
| `checkov.json` | 8470 | `8d7070aac22e04ebd68443284eabf80f9edb5c4929d9c61c21fb3e961ef188da` |
| `datadog-static-analyzer.sarif` | 5676504 | `5aed332e699ab89769691cdccb8fc77ae80e5bb7147d64a5d436359da8d2d1ea` |
| `dependency-check.json` | 7114893 | `b2e15e792182dc6be018f332ee24e088189f3aa2e0732b8f3b67e12d8fefdb0a` |
| `gitleaks.json` | 31371 | `99ad77de2fcc45add2e3381592751a4b36f92ffcd47d25bbbc4d0d112140cc86` |
| `joern.json` | 38595 | `78e6f07eec6d0dcce513a362223e0820d97be79f7c06a69612879e460e258893` |
| `opengrep.sarif` | 1941724 | `9b184bd7f3cb4fe122c785d9ad61ec89cfacd6120ff2d3bcff6631651075a359` |
| `osv-scanner.json` | 2801633 | `c14273ed140d63b7533362857fd75f8b5e3514920ddac2e910c5820cd9a92e3b` |
| `semgrep.sarif` | 1578299 | `139325fdfcca123cd31a280b765ecb77fa26060e2818e6fd91cf4e19c630f674` |

The 28 log files:

| File | Bytes | sha256 |
|---|---|---|
| `checkov.meta.json` | 657 | `24dde0f288ceec4504f5a134db5c2b5fd8a37852c93a0dfc49f0e6fd5414eded` |
| `checkov.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `checkov.stdout.log` | 141808 | `d5a897d456b86e6f91eb0ce5eddfabfee465f8f5eed822887af9ba36a0716896` |
| `datadog-static-analyzer.meta.json` | 743 | `925b2da4e2bc215fa01efbbb3a273fc0956e931e85c5e19e9e2dd41ec6a6887e` |
| `datadog-static-analyzer.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `datadog-static-analyzer.stdout.log` | 5241 | `1bcf18fdd037a7abc389e645d60f58fce65809731c3f9808f1729e0d7f09fec6` |
| `dependency-check.meta.json` | 709 | `7be9067016382b6b2d493305b00f92b12d251046877fbd757319e112cdb6670c` |
| `dependency-check.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `dependency-check.stdout.log` | 6971 | `0aa4415e73e2267b572f6b0e2c29ae0a457f4a4fe178bbe1aaadfbf4ab966a64` |
| `gitleaks.meta.json` | 664 | `a9869c82beafb379f4402b0f76b0166cc6908c70286584c764dd913a68cda336` |
| `gitleaks.stderr.log` | 137 | `e18e49a5d7b9f1bf0081c80db0a80d9ffc49c4ad368abee33ef137b0dcbd879c` |
| `gitleaks.stdout.log` | 2069 | `2fc7dabed4a5f2671a9efc20f7309f818eedf4d9c5308d6bf6d438270e82416b` |
| `joern.meta.json` | 661 | `eeb8ee23cce4e145f68c55ba4163300ce39b5345e1939586f78c947d4b83e9a7` |
| `joern.query-output.log` | 28158 | `7f70c24623ba20647210f892771c14df646f77ddf14a9f14001861d99ae1b46a` |
| `joern.stderr.log` | 0 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `joern.stdout.log` | 1779 | `da7116cbd296fac0b725ff7918c39de52eb02a10b3e49c5b20d9f6313d2dac79` |
| `opengrep.meta.json` | 668 | `57496916b30878189fbdcbc5609dbb60205e3e8122039c38998a83c9e7a8e07b` |
| `opengrep.stderr.log` | 1771 | `d06f6c5002adf75a15d62fff3000150c7f3de0113fbc8b5fb9e3eca552d056e6` |
| `opengrep.stdout.log` | 391508 | `4fd245643c1cff30077279726c8ba78212feca70464a1eb23c0cd2fa148fbf87` |
| `osv-scanner.meta.json` | 681 | `d9d959b0abb8148202ff291c8b483ff05686b0180aca8e3fe36663849e910ede` |
| `osv-scanner.stderr.log` | 33555 | `618c3097553c51266ac46668606fa3f55387649f7ffecb4673270df63b8f0da8` |
| `osv-scanner.stdout.log` | 717 | `213988504ab62b3520b52fe6b66f770f9c407414304fa2b6076f2f08f9160db9` |
| `semgrep.meta.json` | 663 | `f68282f404b4d24cdaec83ca3db5450f7224f5bfc512aa96e9a618f3def5ab85` |
| `semgrep.stderr.log` | 1923 | `279e704efb5f8d8767cb67872a7846c01ef7833bf9096b6b8cf03f6a73a590aa` |
| `semgrep.stdout.log` | 893 | `3410b0354a6000df43524a19aced43295307af20a271d2f0959ef3d8b71a349a` |
| `trivy.meta.json` | 617 | `38949fb1047a904e578787141decaecfb0958eee2ef9c69e6a0df166455d00ef` |
| `trivy.stderr.log` | 1239 | `471e6afeb865baa91dda88733cb2ea84a03ee88534e33c603bb5c73667888900` |
| `trivy.stdout.log` | 907 | `c6e12c1477732b614b4eec2a9870651b8d007f28293fcf4674d4d11a13ed5243` |

Four stderr logs are zero bytes — `checkov`, `datadog-static-analyzer`, `dependency-check`
and `joern` — so all four carry the digest of empty input, which is what an empty file's
digest is. Those are exactly the four `tool-status.md` §2 cites as *(empty — 0 lines)* with
no line range, under the convention its §9 states.

**The warning a consumer must act on, and the recipe.** `harness/artifacts/` is **clone-local
by construction**. Entering the recorded environment creates both trees empty in *every*
clone — `harness/env.sh` chains to the shared `/opt/blitzy-harness/env.sh`, whose line 85
runs `mkdir -p "$HARNESS_RAW_DIR" "$HARNESS_LOG_DIR"`, and `raw/` is left empty for a
scanning run to fill — and both trees are excluded from the commit by the pre-existing
`.gitignore` line 31 (`artifacts/`), which this run may not modify. So a
`harness/artifacts/` tree found in some other checkout may be empty, as this record's own
sibling checkouts show, or may hold a **different** execution entirely. Before reconciling
any published row against a raw artifact, verify the file against the manifest above:

    cd /tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d-w-000_a6fd4d \
      && sha256sum harness/artifacts/raw/* harness/artifacts/logs/*

That prints 36 lines — 8 artifacts and 28 log files — each of which must match the digest
beside its name above; the same command in the `…_343ca4` tree prints the same 36 digests. A
tree whose sizes match but whose digests do not is a different execution, and equal size is
not identity.

**Provenance of this subsection.** It and its digests were added by the remediation of a QA
finding that the records cited the artifacts by byte size alone and named no tree; the digests
and byte sizes were computed with `sha256sum` and `stat` over the same two byte-preserved
trees every other number in this file was taken from. That remediation altered no value
stated anywhere else in this record or in `tool-status.md`: it added this subsection, the two
pointers in §3.5 and §4.1, the sha256 beside each of the eight artifacts in `tool-status.md`
§2, and the tree identification in `tool-status.md` §9, and nothing else.
