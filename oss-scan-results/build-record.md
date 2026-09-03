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
| `harness/artifacts/logs/cpg-ceiling-reverify.log` | this generation's own first-hand re-verification of that serialization ceiling, at **three** heaps — 8 GiB, 64 GiB and 128 GiB (section 5) |
| `harness/artifacts/logs/cpg-input-inventory.json` | the input set of the graph the Joern stages actually load — its 62 archives with their digests, the 31 reactor projects present in it, the 7 absent, and the per-module witness computation (sections 4 and 6) |
| `harness/artifacts/logs/cpg-frontend-input-manifest.json` | two things kept apart inside one file: its original body, the ordered 189-entry staging manifest of **invocation C** and of no other; and the top-level node `delivery_evidence_for_the_graph_the_runners_load_2026_09_03`, added on 2026-09-03, which is the delivery evidence for the graph that loads — provisioning invocation **P**'s command, JDK, heap, elapsed time and written identity, the 62-entry ordered manifest with per-entry size, sha256, `st_nlink`, `st_ino` and mtime, both one-to-one assertions, the cross-check against `cpg-input-inventory.json`, the named mutability hazard, and what it does **not** establish (sections 4 and 5) |
| `/opt/blitzy-harness/provision-log/cpg-identity.txt`, corroborated by `cpg-record.txt` in the same directory | the **write-time record of account** for the graph on disk, written beside it by the provisioning that wrote it: the identity pair, the frontend command, the JDK and heap, the elapsed time and exit code, the input set, the write-time `importCpg` counts, that record's own coverage claim, its frontend metrics, and the provenance limitation it states (SUPERSESSION, STATUS, sections 4, 5, 6 and 7) |
| `harness/artifacts/logs/cpg-identity.txt` | this checkout's in-tree transcription of that same record of account, resolved by `preflight_graph_identity.py`'s `record_of_account()`; cited for that standing rather than for a figure of its own (STATUS, section 5) |
| `harness/artifacts/logs/cpg-verify.log` | the `importCpg` verification loads of exactly those bytes: PHASES 1 to 3 re-anchored on 2026-09-03; **PART 3** the three loads of 2026-09-03 with their elapsed times, JDK, measured child heap and identity checks; **PART 3.1** the per-witness detail for all 31 modules; **PART 3.2** the method and file-node breakdown; **PART 3.3** the probe surface and the connect-shims collision resolved by query; **PART 3.4** the environment-record contradiction resolved. Its **APPENDIX A** and **PART 2** are deliberately verbatim history of other lanes and no figure in this file is taken from either (sections 5 and 6) |
| `harness/artifacts/logs/cpg-graph-record.log` | the write-time record of the **2026-08-30** graph, which the 2026-09-03 re-provisioning has since replaced — a **verbatim preserved stream**, quoted here as that generation's and never re-anchored: its invocation's command, JDK, heap and elapsed time, its own frontend metrics over that 62-archive input, the five exclusion categories and the reproducibility limitation it states, and the withdrawal of its former summary coverage figure (STATUS, sections 4, 5, 6 and 7) |
| `harness/artifacts/logs/cpg-module-coverage.json` | the owner of record of the per-module coverage verdict — the file `cpg-graph-record.log:70-74` and `cpg-verify.log:258-260` both name as owning it and both cite rather than remeasure: its 31-module and 38-project views of that one verdict, its `count_check` arithmetic, and its own `written_by` and `supersedes` fields (section 6) |
| `harness/artifacts/logs/cpg-shims-collision-measurement.log` | the per-class method counts of the eleven `sql/connect/shims` stub classes as the graph on disk holds them, measured first-hand against that graph's re-verified identity (section 5) |
| `harness/artifacts/logs/joern-preflight.log` | the Stage 3 identity gate's comparison of the graph against every record of account — the recorded pair, the per-subject `MATCH`, the adjudicated method count against the one-sided floor, its verdict, and the time and clone it ran in; and its own statement that `harness/bin/run-joern.sh` is **not** a caller of it (SUPERSESSION, STATUS, section 5) |
| `harness/artifacts/logs/sec-gate-scan-target.log` | the other gate this run runs outside the runner: its verdict and the three checks this file names — `smoke-override-absent`, `artifact-tree:HARNESS_RAW_DIR` and `artifact-tree:HARNESS_LOG_DIR` (STATUS, section 5) |
| `harness/artifacts/logs/runner-metadata.json` | the Stage 3 invocation of record under `.tools.joern.stage3_invocation_2026_09_03` — the gates run first and their verdicts, the command and its no-argument form, the timestamps and elapsed time, the exit code, the child JVM's externally measured heap, the artifact with its size, digest and finding count, and the per-query returns; and under `.tools.joern.runner_script_identity` the measured revert of both provisioned files (SUPERSESSION, STATUS, sections 5 and 7) |
| `harness/artifacts/logs/joern.runner-console.log` | a **verbatim preserved stream** — the **2026-09-01** Stage 3 console, quoted here as that generation's: its recompute of the graph's byte size and digest at load time, and the invocation header that brackets it (SUPERSESSION, sections 5 and 7) |
| `harness/artifacts/logs/runner-sequence.json` | cited once, for one value this file does not own: which invocation the Stage 3 console log, artifact, streams and status file belong to (section 7) |
| `harness/artifacts/logs/joern.status` | cited for two things only: the Stage 3 artifact size and elapsed time of the invocation of record, and — in section 7 — what the trailer does **not** carry (SUPERSESSION, section 7) |
| `harness/artifacts/logs/gate-record.json` | cited twice, for two values this file does not own: the gate verdict, and the environment-record graph-identity contradiction (STATUS, sections 5 and 7) |
| `harness/cpg/spark.cpg` | the graph at the path the AAP names — a 33-byte provisioned symlink whose resolved target is host-global and was written by provisioning, not by this run (STATUS, section 5) |
| `harness/artifacts/logs/reverification-f3-writer-bound.txt` with its `.json` | the 2026-09-02 re-measurement of the serialization ceiling at three heaps in this clone, the frontend's re-enumerated option surface, and the permitted-action matrix (STATUS) |
| `harness/artifacts/logs/reverification-f4-module-witness-full-input-set.json` with its `.log` | the 2026-09-02 measurement of the same two witness kinds over the 191-archive set this run's frontend was given: the inventory reconciliation, the per-module rows for both input sets, and the eight modules with no accepted witness (section 6) |

One absence is stated rather than left to be noticed: no
`harness/artifacts/logs/build-<module-path>.log` exists, and none is cited — section 3 records why none
was needed.

**And one record's standing is stated rather than left to be inferred.**
`harness/artifacts/logs/cpg-module-coverage.json` is the **owner of record** of the per-module coverage
verdict. `cpg-graph-record.log:70-74` names it as that owner, "established by the load recorded in
harness/artifacts/logs/cpg-verify.log PHASE 2", and cites its figures rather than remeasuring them;
`cpg-verify.log:258-260` says the same of the per-project map, "owned by
harness/artifacts/logs/cpg-module-coverage.json; the figures here are that file's, cited rather than
re-derived". Being the owner does not make it a **second measurement**: it renders the same two
measurements section 6 reads — `cpg-input-inventory.json`'s per-module witness exclusivity joined
against `cpg-verify.log` PHASE 2's witness queries — so every figure it carries is one of those
measurements cited again rather than taken again. It agrees with section 6 at **both** denominators,
which it also keeps apart in its own `denominator_note`: over the **31** modules contributing an
archive to the graph's input, **26** COVERED on injective evidence and **5** NO VERDICT OBTAINABLE;
over the **38** JAR-packaging reactor projects AAP §0.5.1 sets as the denominator, **26** covered and
**12** without a covered verdict, split **5** that own no admissible witness and **7** with no archive
in the input at all, under its own `count_check` — `5 + 7 = 12; 12 + 26 = 38`. It carries **0** verdicts
resting on presence and **0** on a shared package prefix, against the graph identity section 5 states,
and its `written_by` field records that graph as provisioning's own write of
`2026-09-03T01:40:31Z to 02:11:54Z (31 m 23 s)`, naming this run's frontend termination at the
flatgraph ceiling and that mechanism's re-verification "at three heaps - 8g, 64g and 128g" as the
reason no all-JAR graph stands behind it, and adding that this run "may not rebuild a host-shared
graph". Its `schema_version` is **3**, its `written_at_utc` is `2026-09-03T10:33:02Z`, and its
`supersedes` field names what it replaced: the schema-2 edition written in clone `w-013` on
`2026-09-01T16:57:30Z`, which described a graph of 541,309,809 bytes and sha256
`4616845a…4730c7` — a graph the 2026-09-03 re-provisioning has since replaced, so that edition
describes bytes no disk this checkout can reach still carries.

## SUPERSESSION — the generation that wrote this file, the 2026-09-03 re-provisioning, and every figure re-anchored

**Read this before the STATUS block, and before any graph figure anywhere below it.**

**Who wrote this file, and against which host.** Every section below was authored by the run
`w013-20260901T132807Z` in **clone 13**, against the host as it stood on **2026-09-01**. That is stated
rather than left to be inferred, because the host changed underneath it: it was **re-provisioned at
2026-09-03T01:17:07Z**, and that re-provisioning **rebuilt the code-property graph**. Every graph figure
the 2026-09-01 generation measured therefore describes bytes that are no longer on disk.

**Why that needed correcting rather than annotating.** A record that asserts, as a live claim, a value
for bytes that are not on disk is not history — it is a false live claim, and a reader checking it
against the filesystem finds a contradiction with nothing telling them which side is current. So on
**2026-09-03** the graph was re-measured by **three independent `importCpg` loads**, the frontend input
manifest was **re-derived first-hand** against the live staging tree, and the per-module coverage record
was **rewritten to schema 3**. Every section below now states the 2026-09-03 value as its live claim,
and every superseded figure is retained here with its generation and date, which is what AAP §0.1.3
requires of a value this run replaces.

| Figure | The 2026-09-01 generation's value — SUPERSEDED | The value on disk since 2026-09-03 — LIVE | Producer record for the live value |
| --- | --- | --- | --- |
| Graph bytes | 541,309,809 | **547,980,224** | `cpg-verify.log` SUBJECT and PART 3; `joern-preflight.log`; `cpg-module-coverage.json` `.graph.bytes` |
| Graph sha256 | `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` | **`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`** | the same three |
| Methods | 1,396,899 | **1,398,964** | `cpg-verify.log` PHASE 1, re-anchored, and PART 3's three loads |
| — internal (with a body) | 1,307,112 | **1,308,974** | `cpg-verify.log` PART 3.2 |
| — external (declaration only) | 89,787 | **89,990** | `cpg-verify.log` PART 3.2 |
| Type declarations | 119,721 | **119,860** | `cpg-verify.log` PHASE 1 and PART 3 |
| Files | 45,037 | **45,037** — the one count that did not move | `cpg-verify.log` PHASE 1 and PART 3 |
| Methods under `org.apache.spark.*` | 925,445 (66.25 %) | **927,304 (66.29 %)** | `cpg-verify.log` PART 3.2 |
| File nodes: `.class` / `pom.properties` / containing `META-INF` | not measured by that generation | **44,811 / 102 / 216** | `cpg-verify.log` PART 3.2 |
| Delta against the AAP §0.2.1 anchors | methods +498,563, typeDecls +32,340, files +6,219 | **methods +500,628, typeDecls +32,479, files +6,219** | `cpg-verify.log` PHASE 1 |
| Graph input byte total | 285,122,371 | **285,122,375** | `cpg-input-inventory.json` node `byte_total_correction_2026-09-03`; independently `cpg-frontend-input-manifest.json`'s 2026-09-03 delivery node |
| The frontend write that produced the graph | 2026-08-30T19:18:37Z, 50 m 42 s, peak sampled RSS 66.6 GB | **2026-09-03T01:40:31Z → 02:11:54Z, 31 m 23 s, `FRONTEND_EXIT=0`, peak sampled RSS 61 GB** | `cpg-frontend-input-manifest.json`, node `delivery_evidence_for_the_graph_the_runners_load_2026_09_03`; `cpg-module-coverage.json` `.graph.written_by` |
| That write's frontend metrics | 429 AST-creation failures, over the 2026-08-30 graph (`cpg-graph-record.log`, that graph's own write record) | **31,598 `Overwriting class file` warnings over 26,221 distinct class files; 67 AST-creation `RuntimeException`s, all under `org/apache/spark`** | `/opt/blitzy-harness/provision-log/cpg-record.txt` line 11, the write-time record of account; corroborated by `harness/ENVIRONMENT.md`'s §7 *Frontend metrics, observed rather than expected* block |
| Verification-load elapsed | 885,009 ms, one load | **526,605 / 557,041 / 545,644 ms, three loads** | `cpg-verify.log` PART 3 |
| Stage 3 Joern invocation of record | 2026-09-01T14:25:10Z → 14:41:24Z, 974.22 s, artifact 354,817 bytes | **2026-09-03T09:07:47Z → 09:17:43Z, 596.83 s, exit 0, artifact `harness/artifacts/raw/joern.json` at 353,048 bytes, sha256 `f7f5f60e37aacdbf58ca2bf073c0682efeb81e256a516576b12d55aea8edc926`, 690 findings, `bound_reached` false on every query** | `runner-metadata.json` `.tools.joern.stage3_invocation_2026_09_03`; `harness/artifacts/logs/joern.status` |
| The coverage record | `cpg-module-coverage.json` schema 2, clone `w-013`, 2026-09-01T16:57:30Z | **schema 3, `written_at_utc` 2026-09-03T10:33:02Z, 852 lines** | that file's own `schema_version`, `written_at_utc` and `supersedes` |
| `harness/bin/run-joern.sh` | edited after this generation — 13,323 bytes, sha256 `b6449dd0…56cad`, on 2026-09-02 | **reverted to its provisioned bytes: 76 lines, 3,380 bytes, sha256 `32dd647af10709b72d159d67a2b15bd6f1f258af97614a9d2bf577c7a1abe65f`** | `runner-metadata.json` `.tools.joern.runner_script_identity.edited_after_this_generation.reverted_on_2026_09_03` |
| `harness/lib/joern-scan.sc` | edited in the same 2026-09-02 act | **reverted: 122 lines, 5,401 bytes, sha256 `cf7a3622a0635db3932b414427ff1b4b416b6050a024ea37651d5d89b91c0fa4`** | the same node |

**The three loads that produced the live counts, and what makes them three rather than one.** A count
measured once is a count with no corroboration, and these counts are cited by several published
documents, so `cpg-verify.log` PART 3 records **three separate JVMs** importing the same bytes on
2026-09-03 — the verification load (`cpg-verify.sc`), the per-witness detail load
(`cpg-witness-detail.sc`) and the corroboration load (`cpg-corrob.sc`). **All four counts are identical
in all three**: methods 1,398,964, internal 1,308,974, typeDecls 119,860, files 45,037. Common to every
one of them, each property measured **inside the child JVM** rather than requested of it: JDK Temurin
**21.0.12.1+1** with `java.specification.version` **21**; `heap_max_bytes` **68,719,476,736**, read by
`Runtime.maxMemory()` in the child and at or above AAP §0.8.2's floor; `JAVA_TOOL_OPTIONS=-Xmx64g`
alongside `-J-Xmx64g`, because `-J-Xmx` sizes the `joern` launcher and not the forked child that holds
the graph, and PART 3 records that measured distinction rather than assuming either flag reached it;
one fresh workspace per load outside the repository; and the graph's **size and digest re-computed
immediately before the import**, in bash and again from inside the loading JVM, against the record of
account. `METHODS_GT_ZERO` true; the floor is **853,420** and 1,398,964 is **1.64×** it; the 898,336
anchor is exceeded, which AAP §0.9.3 makes a **recorded difference and never a halt** because the anchor
is one-sided — no upper comparison is performed, and adding one would halt the run for succeeding.

**The record of account for the live pair.** `/opt/blitzy-harness/provision-log/cpg-identity.txt`,
written beside the graph by the provisioning that wrote it, corroborated by `cpg-record.txt` in the same
directory. `joern-preflight.log` names it as the record of account, reports `MATCH` on both fields for
the AAP's named path `harness/cpg/spark.cpg` resolved through its symlink, adjudicates
`1,398,964 methods, agreed by 2 record(s) of account`, and prints
`Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420` under **VERDICT: PASS**.
`harness/artifacts/logs/cpg-identity.txt` is this checkout's in-tree transcription of that same record
of account and is owned by its own lane; every figure in this file is cited from the producer records
named in the table above rather than from that transcription.

**What is deliberately left carrying the superseded figures, and must not be read as a live claim.**
Four in-tree records are **verbatim preserved streams** under AAP §0.8.1 and are not edited, so they
legitimately still state 2026-09-01-generation values wherever this file quotes them:
`harness/artifacts/logs/build-reactor.log`, `harness/artifacts/logs/cpg-frontend.log`,
`harness/artifacts/logs/cpg-graph-record.log` — which is the write-time record of the **2026-08-30**
graph and states `62 JARs / 285,122,371 bytes` and the pair `541309809` / `4616845a…4730c7` as that
graph's own figures — and `harness/artifacts/logs/joern.runner-console.log`, which is the
**2026-09-01** Stage 3 console and prints `cpg bytes       : 541309809`. `cpg-verify.log`'s
**APPENDIX A** and **PART 2** are likewise left verbatim, the first being the 2026-09-01 load's own
console and the second a different graph entirely (the all-JAR witness lane). Wherever this file quotes
any of them, the quote is labelled with its generation and the live value is given beside it. **No
number inside a quotation was altered.**

**Line citations into `harness/bin/run-joern.sh` were re-anchored, and three cited constructs no longer
exist.** The runner was reverted to its provisioned 76 lines, so citations framed against the
2026-09-02 edited file could no longer resolve. Two are byte-identical content at new line numbers and
are simply re-pointed: the `printf 'cpg bytes'` line that runs `stat -c%s` moved from 112 to **57**, and
the `printf 'cpg sha256'` line that runs `sha256sum` moved from 113 to **58**. Three are **gone**: the
gate invocation, its status test and its closing branch that the 2026-09-02 edit had introduced. The
provisioned runner **invokes no gate at all** — `joern-preflight.log` states it independently, that
`harness/bin/run-joern.sh` "does NOT read it: it prints its input's identity without comparing it, and
it is REFERENCE under AAP 0.6.1". Those two controls are run by this run **outside** both provisioned
files and are described as prose wherever this file previously cited a line inside them.

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
>
> **And the graph may not be rebuilt in this checkout even if the ceiling could be cleared, which is a
> second and independent reason the requirement stays unmet.** The bytes live at
> `/opt/blitzy-harness/cpg/spark.cpg`, reached from here through the committed symlink
> `harness/cpg/spark.cpg`. That path is **host-global and shared read-only across up to 64 concurrent
> clones**, each of which may be loading it while this run works. Rewriting it in place would hand
> every one of those siblings a **truncated read** mid-load and would break the **recorded identity**
> — `/opt/blitzy-harness/provision-log/cpg-identity.txt` and `cpg-record.txt` — for all of them at
> once, so a run that "fixed" the coverage shortfall that way would have corrupted every other clone's
> evidence to do it. `cpg-module-coverage.json`'s `.graph.written_by` states the same constraint in its
> own words: the graph was written by provisioning "**-- not by this run, which may not rebuild a
> host-shared graph**". **So the correct treatment is the one this file gives: a named limitation, plus
> the 26 coverage verdicts that are established and the 12 that are named as unobtainable — never a
> claim of all-JAR coverage, and never a rebuild.**
>
> **BUILD COMPLETENESS IS A SEPARATE FACT AND IT IS NOT IN DOUBT.** Stated here, distinctly, so the
> coverage shortfall above is not misread as a build failure. The full reactor **built successfully**
> and **all 38 JAR-packaging projects produced their own artifact** — `build-reactor.log` STEP 13
> prints `jar-packaging WITHOUT one : []` — and **all 191 of those own artifacts were staged and
> supplied to this run's own frontend**, which then failed in **persistence** rather than in
> compilation or in ingestion (`cpg-frontend.log` STEP 1 and STEP 8). **Not one of the twelve modules
> without a coverage verdict is missing because its project failed to build.** The shortfall is
> attributable to the **graph's provenance**, and `cpg-module-coverage.json` reaches that attribution
> independently in its `why_the_shortfall_is_not_a_build_outcome` and
> `attribution_of_the_shortfall` fields.
>
> **Both halves of the first paragraph — the writer bound, and the coverage cost of the narrower graph
> — were re-verified first-hand on 2026-09-02**, after runtime testing raised them as blocking
> findings, and both still hold. The array-length bound was re-measured at **three** heaps
> in this clone — `-Xmx8g`, `-Xmx64g`, `-Xmx128g` — and in all three the writer buffers exactly
> 2,147,483,639 bytes and then throws `Required array length 2147483639 + 77 is too large`: over a 16×
> heap range the failure point does not move by one byte, so no heap value clears it and the frontend's
> own option surface (12 named options, enumerated again) offers no chunked or streaming write.
> `harness/artifacts/logs/reverification-f3-writer-bound.txt` and its `.json` carry that measurement.
> And section 6 now carries a **second** coverage measurement, over the 191-archive set this run's
> frontend was given rather than the 62 the loaded graph holds, which establishes that **30 of the 38**
> modules would be verdictable even in the mandated graph and **8** would not — a property of the
> witness rule against Spark's shaded artifacts rather than of the narrowed input.

Five facts bound what the numbers in this file describe, and reading a coverage figure without them
would misread it.

- **The full-reactor build was performed by this run.** `build-reactor.log` records it end to end:
  `BUILD SUCCESS`, Maven exit code 0, 40 of 40 reactor projects `SUCCESS`, no `-pl` and no module
  selector of any kind, and all 38 JAR-packaging projects confirmed on disk to have produced their own
  main artifact. Section 3 owns that outcome.
- **The graph every stage of this run loaded was written by PROVISIONING, on
  2026-09-03T01:40:31Z → 02:11:54Z, not by this run.** Its identity is one pair —
  **547,980,224 bytes**, sha256
  `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6` — and the record of account for
  it is `/opt/blitzy-harness/provision-log/cpg-identity.txt`, written beside the graph at write time
  and corroborated by `cpg-record.txt` in the same directory, both read and in agreement.
  `harness/artifacts/logs/cpg-identity.txt` is this checkout's in-tree transcription of that same
  record, resolved by `harness/lib/preflight_graph_identity.py`'s own `record_of_account()` — the same
  function the Stage 3 identity gate calls, so the record and the gate cannot state different pairs.
  That function prefers this checkout's own frontend write-time pair and falls back to the record
  written beside the graph, and the fallback applied here for one reason only: this run's frontend
  produced no graph, so there was no write-time pair of its own to prefer. The **superseded**
  2026-09-01-generation pair, 541,309,809 / `4616845a…4730c7`, is retained in the SUPERSESSION table
  above; the input set was 62 archives from 31 modules with `-tests` and `spark-connect-shims`
  archives excluded by provisioning's own instruction. The pair was re-measured from the bytes on disk
  for **every** load of 2026-09-03 and **every** comparison ran immediately before the load it gates,
  with every check logged: the **three** `importCpg` loads (`cpg-verify.log`'s pre-load check at
  2026-09-03T09:36:13Z, under the heading "GRAPH IDENTITY, RE-VERIFIED IMMEDIATELY BEFORE THE LOAD",
  and PART 3's record that each of the three re-computed size and digest itself, in bash and again from
  inside the loading JVM), the **Stage 3 Joern runner** (bullet below), and each of the **three**
  Stage 5 probe queries (`harness/artifacts/logs/probe-*.identity.txt`, stamped 09:45:34Z, 09:55:51Z
  and 10:07:18Z, each `verdict=PASS` at `bytes=547980224` and each carrying its own `supersedes=` line
  naming the 2026-09-01 pair it replaced).
- **For the Stage 3 Joern runner the AAP §0.8.2 ordering now holds, and the 2026-09-01 ordering defect
  is retained as history rather than restated as current.** In the invocation of record the gate
  `harness/lib/preflight_graph_identity.py --check-only` ran at **2026-09-03T09:07:46Z** and published
  **VERDICT: PASS** to `harness/artifacts/logs/joern-preflight.log`, and
  `./harness/bin/run-joern.sh` was then invoked with no arguments at **09:07:47Z**, finishing 09:17:43Z
  in **596.83 s** with exit **0** — one second between the comparison and the load, from the same
  clone (`runner-metadata.json` `.tools.joern.stage3_invocation_2026_09_03`). The runner independently
  re-computed the pair itself and printed **547,980,224** / `325887cf…3dc6`, which
  `stage3_invocation_2026_09_03.graph_identity_the_runner_printed` records as agreeing with the records
  of account. **Nothing inside either provisioned file performs that comparison, and this file no
  longer implies otherwise.** `joern-preflight.log` states it plainly: `harness/bin/run-joern.sh`
  "does NOT read it: it prints its input's identity without comparing it, and it is REFERENCE under
  AAP 0.6.1". The runner was reverted to its provisioned 76 bytes-for-bytes lines on 2026-09-03
  precisely because the 2026-09-02 edit that had put a gate inside it changed a file AAP §0.6.1 holds
  as REFERENCE and §0.8.1 says is never edited; the two controls it was reaching for now live where the
  AAP puts them — the identity gate and the method-count floor in
  `harness/lib/preflight_graph_identity.py`, run by this run before the invocation, and the heap in
  `JAVA_TOOL_OPTIONS=-Xmx64g`, the environment override §0.6.5 sanctions, verified externally with
  `jcmd` against the child's own `-XX:MaxHeapSize` at **68,719,476,736** bytes on JDK 21. **The
  superseded 2026-09-01 ordering, kept as history:** that generation's runner printed
  `cpg bytes : 541309809` inside its `2026-09-01T14:25:10Z → 14:41:24Z` invocation
  (`joern.runner-console.log:14-15`, the verbatim console, from the provisioned runner's
  `harness/bin/run-joern.sh:57-58`), while the comparison against the record of account was stamped
  `2026-09-01T14:52:54Z` with `Clone index 0` — about **11.5 minutes after that load ended and from a
  different clone** — so for that load "immediately before every load" was not satisfied, and this
  file never claimed it was. `run-record.md` **D4** carries the ordering and **D25** the runner edit
  and its revert.
- **The requirement that this run create that graph is unmet and unmeetable at this pin**, for the
  measured reason the blockquote states and section 5 evidences from the failing method's own
  bytecode and from a **three**-heap re-verification spanning a sixteenfold range of reported heap. It is published as a divergence, carried in the run's
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
  - **Why no heap clears it, established by measurement rather than by argument, now at three heaps.**
    `cpg-ceiling-reverify.log`, section *THE PROBE, AND ALL THREE ARMS VERBATIM*, re-ran the ceiling
    probe in this clone at **`-Xmx8g`**, **`-Xmx64g`** and **`-Xmx128g`**. All three threw
    `java.lang.OutOfMemoryError: Required array length 2147483639 + 77 is too large` with
    **2,147,483,639** bytes already buffered, while the JVM's reported `maxMemory` spanned **a factor of
    sixteen** across the arms — **8,589,934,592**, **68,719,476,736** and **137,438,953,472** bytes. The
    8 GiB arm's own stdout is `maxMemory bytes = 8589934592`, `buffered bytes = 2147483639`,
    `MESSAGE = Required array length 2147483639 + 77 is too large`. **The failure point did not move by
    one byte across a 16× heap span.** The bound is on one array's length, not on the heap, and it scales
    with the total UTF-8 size of the graph's distinct strings — that is, with the breadth of the input
    set. The probe's message reads `+ 77` because the probe writes the 77 further bytes the frontend's
    next string would have written; the frontend's own message reads `+ 72` for its own next string, and
    the two are each their own measurement rather than a discrepancy.
  - **Why it is not repaired.** `cpg-frontend.log` STEP 10 enumerates every mitigation against the
    frontend's actual flag surface. The only lever that would work is excluding inputs
    (`--exclude`, `--exclude-regex`, dropping pre-shade / `-tests` / shims artifacts, or bounding
    `--depth`), and AAP §0.3.2 forbids trimming the input set while §0.9.2 lists it among the
    conditions that stop the run rather than get repaired. A frontend or flatgraph build whose writer
    chunks the string pool would clear it, and AAP §0.4.3 forbids installing, upgrading or
    substituting any tool. So the input set the AAP mandates and the writer the pinned frontend ships
    are not simultaneously satisfiable on any host at this pin.
  - **Why nothing was written to the AAP's path, and why nothing may be.**
    `/opt/blitzy-harness/cpg/spark.cpg` is **host-global and shared read-only across up to 64
    concurrent clones**, any of which may be loading it while this run works. Writing there would hand
    those siblings a truncated read mid-load and would invalidate the recorded identity
    (`/opt/blitzy-harness/provision-log/cpg-identity.txt`, corroborated by `cpg-record.txt`) for every
    one of them at once. So this is not only that there was nothing valid to install — there was
    nothing valid to install **and** installing it would have been prohibited anyway.
    `cpg-module-coverage.json`'s `.graph.written_by` records the same constraint: the graph is
    provisioning's, "**-- not by this run, which may not rebuild a host-shared graph**".
- **The gate that precedes all of this is not this file's verdict, and this file declares none.**
  `harness/artifacts/logs/gate-record.json` recorded 43 checks — 38 `pass`, 3 `recorded_difference`,
  2 `halt` — with `gate_verdict.overall` = `"halt"`, authorising nothing. One of those two halts was
  the environment record's graph identity contradicting the filesystem, and **that one is RESOLVED as
  of 2026-09-03**: `cpg-verify.log` **PART 3.4** records `harness/ENVIRONMENT.md` §7 and
  `harness/artifacts/MANIFEST.json`'s `.cpg` member re-anchored to **547,980,224 /
  `325887cf…3dc6`**, the pair the graph on disk and provisioning's own write-time records already
  agreed on, with the superseded identities retained in that document's labelled supersession tables
  and never as live claims; and `harness/lib/preflight_graph_identity.py --check-only` consequently
  exits **0** with **VERDICT: PASS** where before the re-anchor it exited **77 VERDICT: HALT** and
  `./harness/bin/run-joern.sh` exited **78 CONFIGURATION FAULT** without loading anything
  (`joern-preflight.log`). The gate record and `oss-scan-results/run-record.md` own the gate verdict
  and its current state; this file records measurements, cites that verdict rather than restating or
  softening it, and states the resolution of the one halt that bore on the graph because section 5 and
  section 7 both carry both of its values.

**The consequence lands on section 6, and it is stated there as a limit rather than worked around.**
Because the graph that loads was built over a narrower input set than this run's build produced, seven
of the 38 JAR-packaging projects have **no bytecode in it at all**, and five of the 31 projects that do
own **no injective witness of either kind the AAP permits**. All twelve are reported as **NO VERDICT
OBTAINABLE**, each with what was tried and why it is unobtainable. No third kind of evidence is
admitted, and no narrower graph is presented as a substitute for the one the AAP mandates.

**A figure that used to read as a third coverage statement has been withdrawn by the record that
carried it, and is not restated here.** `cpg-graph-record.log`'s summary line once read **31 of 31
contributing modules covered, 0 missing** over the provisioning invocation's own 62-JAR input set. That
record now withdraws it in its own words — "THIS RECORD IS SUPERSEDED ON THIS FIGURE … That figure is
withdrawn rather than restated" — because its own witness listing contradicted it: 26 modules with a
class unique to that module and 5 with none, and under AAP §0.5.1's injective test a module with no
exclusive witness is not covered. The same record now names `harness/artifacts/logs/cpg-module-coverage.json`
as the owner of the coverage verdict and **cites** rather than remeasures it, over the same 31 modules
present in the graph's input: **26 COVERED on injective evidence and 5 NO VERDICT OBTAINABLE**. That is
section 6's own first-column figure at that denominator, so there is one verdict cited twice rather than
two verdicts to keep apart, and this file states no third coverage figure. What section 7 still
adjudicates is the *inherited environment record's* copy of the withdrawn claim
(`harness/ENVIRONMENT.md:605`, repeated at `:323`) against section 6's verdict.

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
A digest is therefore only meaningful with the tree **and the moment** it was measured in, and the only
tree whose per-archive digests bear on any conclusion in this file is the graph's own input set, where
`cpg-input-inventory.json` records this artifact at `109,208,027` bytes with 11,910 entries of which
11,070 are class entries. Its digest is the case that proves the point twice over: the byte size has not
moved, and the digest has — **`5fb2b39a901643a9c0c98f2f0e829cc9cf20c05ac492ccc06cba0fcdd6d2c3b1` as
re-measured on 2026-09-03**, superseding
`ab5f23f67b2131fc852b8122a956610e6c023605041545232c063ff8347c394c` as measured on 2026-09-01. The staged
file is a **hard link** into `/opt/spark-src`, so the rebuild that preceded the 2026-09-03 graph rewrote
its content in place at constant length; both values are on the record with their dates in
`cpg-input-inventory.json`'s `byte_total_correction_2026-09-03` node, and the live one is also carried
per entry in `cpg-frontend-input-manifest.json`'s 2026-09-03 delivery manifest at `order` 7.

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

### Staged input set 1 — the 191 archives this run's frontend was given (**invocation A**)

The bundled `jimple2cpg` accepts **one** input path, so "every JAR the build produced" and "one input
path" are reconciled by staging the inventory into a single directory. Everything below is
`cpg-frontend.log`'s record of that invocation, and the honest state of the evidence is stated with it.

**Which invocation this is, named before its figures.** `cpg-frontend.log` opens with an INVOCATION
INDEX recording that **three** distinct frontend invocations exist across this generation's lanes, over
three different input sets, and labelling them so no figure can be read against the wrong one. The set
below is **invocation A**, "THE MANDATED COMPLETE INPUT SET", recorded by that file's STEPS 1 to 12 in
the `w-005` lane: 191 own artifacts, 431,184,822 bytes, `-J-Xmx128g` under JDK major 21, exit **1** in
flatgraph serialization with no graph accepted. The other two are named where they appear and never
here: **invocation B**, the 38-artifact per-module witness graph, in section 5's `sql/connect/shims`
subsection, and **invocation C**, the 189-archive attempt with two archives withheld, in the halt-class
disclosure later in this subsection. Every 191-archive figure in this subsection is invocation A's.

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

**What is measurable, where it is measurable from, and what is not — stated rather than implied.**
**This checkout contains no staging tree**: `harness/artifacts/` holds `MANIFEST.json`, `logs/` and
`raw/` and nothing else, and no `harness/artifacts/cpg-input*` path is tracked. But the tree itself was
not lost with the clone that ran the frontend: `cpg-frontend.log` STEP 11 records that **"the staging
tree of invocation A survives on disk"** in the `w-005` lane, and STEP 1 has since re-run the assertion
against it rather than re-quoting it — **files in the staging directory 191, total bytes 431,184,822,
members byte- and digest-identical to the asserted manifest 191 of 191, members drifted 0**, alongside
the counts an earlier edition of this file reported as unmeasurable: inventory entries 191, staged files
on disk 191, manifest entries 191, distinct staged names 191, distinct sha256 189, entries unmapped 0,
files unmapped 0, digest re-verification 0 mismatches. `harness/artifacts/MANIFEST.json` still records
the two staging trees rather than publishing them, under `cpg_input_records`: it names both as
`not_present_in_this_checkout`, names the artifact that owns each, and — stated in its own
`why_no_per_file_entries` — deliberately restates **no per-file number**, because a previous revision's
per-file copies disagreed with their owners. So the record of the 191-archive set is the aggregate its
owners state plus the member-by-member assertion STEP 1 owns, and no tree in *this* checkout and no
per-archive entry in *this* tree is cited as though a reader could walk it here.

**Per-archive identity for this set is not persisted as an ordered manifest, and what is persisted
instead is named rather than estimated.** The aggregate — 191 archives and 431,184,822 bytes — is
`build-reactor.log` STEP 13's measurement, and STEP 15 of that file declares the ownership in terms this
file honours: "no other document may restate any of the six as a second measurement". `cpg-frontend.log`
STEP 1 cites it as the set actually supplied and, in its RE-MEASURED block, re-hashes every member of the
surviving tree against the asserted manifest at **191 of 191 identical, 0 drifted**. What does **not**
exist is a per-entry list, and `cpg-frontend.log` STEP 11 states that exactly: "A persisted ordered
per-entry manifest of invocation A's 191 members is not among the files of this tree; what is persisted
is the assertion over them, re-measured in STEP 1." The same step names the two files a reader might
mistake for its owner — `cpg-input-inventory.json`, which inventories the 62-archive provisioned staging
tree, and `cpg-frontend-input-manifest.json`, which carries the ordered per-entry manifest of a
different and narrower **189**-archive input set, the withheld-input attempt labelled **invocation C**
by that log's own INVOCATION INDEX and disclosed later in this subsection. Inside `harness/artifacts/` no
name/size/sha256 entry for an individual member of the 191 survives either: `MANIFEST.json`'s
`regenerated.corrections` records that the 191 per-file entries it once carried were withdrawn together
with their **431,184,903** total, which disagreed with the owners' 431,184,822 by 81 bytes, and
`cpg-input-inventory.json` was regenerated in this generation to describe the 62-archive set instead.
This file therefore states the aggregate and the member-by-member assertion, nothing per-archive for the
191, and `run-record.md` §14 carries the absent ordered manifest as a value that could not be
established.

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
withheld-input divergence disclosed later in this subsection — the **number of archives invocation C
supplied**. The two are different measurements that happen to coincide numerically, and neither
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

**A superseded attempt — invocation C — did withhold two archives, and that is a halt-class departure
registered elsewhere.** `cpg-frontend.log`'s INVOCATION INDEX labels it **C**, "A 189-ARCHIVE INPUT SET
WITH TWO ARCHIVES WITHHELD", run in the **w-000** lane at `-J-Xmx160g` under JDK major 21, exit **0**
after 19,482 s — it did serialize, into that lane's own checkout, and no runner or probe ever loaded
it. `harness/artifacts/logs/cpg-frontend-input-manifest.json`, written in that **w-000** clone and
retained in the logs tree as evidence, is the file the index names as owning C's staging figures: it
records `full_inventory_archive_count` **191** against `frontend_input_archive_count` **189**,
`frontend_input_bytes` **308,385,184** and `withheld_archive_count` **2** — its own `assertion` holding for the reduced set with
`assertion_errors` empty, so the trim is declared rather than concealed. Its own
`which_invocation_this_manifest_describes` node states the rest of C's shape and, decisively, its
relation to the graph: clone lane **w-000**, `-J-Xmx160g`, `frontend_exit_code` **0** after **19,482 s**,
a graph of **791,927,027** bytes written to `harness/artifacts/cpg/spark.cpg` in the w-000 checkout —
**not** the sanctioned path — with **28,714** overwrite warnings over **23,168** distinct overwritten
paths and **487** AST-creation failures, and
`relation_to_the_graph_the_runners_loaded: "None."` in its own words. **That body describes invocation C
and nothing else**, and no figure in this file's sections 5 or 6 is taken from it. The two withheld
archives, with the byte size, digest and stated reason the manifest itself gives:

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

**The two input totals are two measurements of two different trees, and this file states them as such
rather than reconciling them.** `cpg-frontend.log`'s INVOCATION INDEX is explicit about how to read
them: "191 own artifacts / 431,184,822 bytes is invocation A's input, the complete set this file's STEP
8 records failing; 189 archives / 308,385,184 bytes is invocation C's input, two archives short of that
inventory. Each figure is one measurement cited once, of a different thing. Neither corrects the other."
Adding invocation C's 189 staged bytes to the two archives it withheld does **not** return invocation
A's total, and `build-reactor.log` STEP 15 records that arithmetic without reconciling it. This file
therefore never states the two as one figure, never sums across them, and takes no position on the
difference: `oss-scan-results/run-record.md` §13 owns the run's divergence register and the
adjudication.

**No delivered measurement in this file rests on that attempt.** Every 191-archive figure above is
`build-reactor.log` STEP 13's and `cpg-frontend.log` STEP 1's, both of the complete set; every graph
count and every coverage verdict in sections 5 and 6 is measured over the **62**-archive input set of
the graph that loads (`cpg-input-inventory.json`); and no figure anywhere in this file is taken from the
189-archive set or from its manifest beyond the disclosure above. The manifest is neither deleted nor
re-registered here: it is retained as evidence under AAP §0.8.1, and its run-level divergence entry is
**D20** in `oss-scan-results/run-record.md` §13, which owns the run's single divergence register and
carries the disposition and the decision a human must take.

**And that manifest had stopped describing anything a reader could check, which was a real gap because
AAP §0.5.1 makes the logged one-to-one manifest — "not a per-module class search" — *the* delivery
evidence. It was closed on 2026-09-03 by re-derivation, and both states are on the record.** The lapse,
measured in `harness/artifacts/logs/cpg-frontend-input-manifest.json`'s own
`invocation_c_inputs_as_they_stand_now` node: invocation C's staging directory sat in the **w-000**
checkout, which is not on this host any more, so of that node's `ordered_manifest_entries` of **189**
only **61** resolve to a file in this checkout's reach and
**128** resolve to nothing at all; and one archive that *is* live was never in C's list,
`common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` — C withheld it deliberately, as the table
above records. The manifest's own verdict on itself, added in the same act, puts it exactly:
*"invocation C's manifest remains a true record of invocation C and a false guide to anything on this
host"* (`invocation_c_inputs_as_they_stand_now`). **Nothing in the C body was edited**; it is retained
verbatim as evidence of invocation C, and the delivery evidence for the graph that actually loads was
**added beside it** as a new top-level node, `delivery_evidence_for_the_graph_the_runners_load_2026_09_03`.
The file is consequently **2,008 lines / 81,168 bytes**, where the revision it supersedes was **1,221
lines / 47,326 bytes**, sha256 `66855a63…`.

**What that node establishes, measured first-hand rather than copied.** Its `measured_by` field names
the method: `os.listdir` of the staging directory, `os.stat` for size, `st_nlink`, `st_ino` and mtime,
and a streaming sha256 over every archive, taken at **2026-09-03T09:33Z** in clone lane `a424a0`, with
no figure copied from another record. It describes provisioning invocation **P** — "the only invocation
whose graph any runner or probe in this run loaded" — and carries the ordered 62-entry manifest with all
five per-entry fields:

| Property of the delivery evidence for the graph that loads | Value |
| --- | --- |
| The invocation | **P**, provisioning's own frontend build — `jimple2cpg` (joern 4.0.607 bundled) over `/opt/blitzy-harness/cpg-input --recurse -J-Xmx64g`, stdin closed, `SL_LOGGING_LEVEL=WARN`, under Temurin 21.0.12.1+1 at `heap_max_bytes` **68,719,476,736**, `frontend_exit_code` **0**, **31 m 23 s** (2026-09-03T01:40:31Z → 02:11:54Z) |
| What it wrote | **547,980,224** bytes, sha256 `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`, to `/opt/blitzy-harness/cpg/spark.cpg` — reached from this checkout through the committed symlink `harness/cpg/spark.cpg`, so "unlike invocations A and C this one IS the graph of record" |
| Staging directory | `/opt/blitzy-harness/cpg-input` |
| Archives / bytes / modules | **62** / **285,122,375** / **31** |
| Distinct sha256 | **62** — one digest per archive, so the archive-to-digest mapping is injective |
| `st_nlink` histogram | `{2: 62}` — every one of the 62 is a hard link, which is the hazard the next paragraph names |
| The one-to-one assertion | **total and injective in BOTH directions**: `manifest_entries` 62 = `files_on_disk` 62, `names_on_disk_not_in_manifest` `[]`, `names_in_manifest_not_on_disk` `[]`, `duplicate_staged_names` 0, `duplicate_sha256` 0, and `assertion_errors` **`[]`** |
| The byte total, agreeing two ways | `sum_of_entry_size_bytes` **285,122,375** = `du_sb_of_the_directory` **285,122,375**, `byte_total_agrees` **true** |
| Cross-check against `cpg-input-inventory.json` | **62** entries compared on size and sha256, **0** disagreeing — so the two files state one figure measured twice, which is what AAP §0.6.4 requires of a count appearing in two documents |
| Per-entry fields carried | `order`, `staged_name`, `module`, `original_filename`, `size_bytes`, `sha256`, `st_nlink`, `st_ino`, `mtime_utc` — for all 62 |
| Archives per module | **2** for each of the 31 modules — each module's main artifact and one sibling |

**The mutability hazard is named rather than left as a footnote, because it is live.** All 62 archives
report `st_nlink=2`: the staging tree is hard-linked into `/opt/spark-src` rather than copied, exactly as
the write-time record says ("staged as hard links"), so a staged archive and the reactor artifact it came
from are **one inode**, and a rebuild of the pinned tree rewrites the frontend's recorded inputs in
place under a manifest that still states the old bytes. That is not hypothetical: the node records
**three** archives having drifted between the 2026-09-02 and 2026-09-03 measurements —
`common_utils__original-spark-common-utils_2.13-4.1.0-SNAPSHOT.jar` (511,979 → 511,981 bytes,
`38627c49…` → `3e915222…`), `common_utils__spark-common-utils_2.13-4.1.0-SNAPSHOT.jar`
(517,232 → 517,234 bytes, `ebeca6c4…` → `f16f1273…`) and
`common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` (109,208,027 bytes unchanged,
`ab5f23f6…` → `5fb2b39a…`). **So every digest in that node, and every digest quoted from it in this
file, is as of 2026-09-03T09:33Z and should be re-measured before being relied on**, which is the node's
own `what_a_reader_should_do`. The fix that would remove the hazard — staging by copy — **belongs to
provisioning and was not performed here**: `/opt/blitzy-harness/cpg-input` is shared read-only across
clones, and re-staging it from a scan run would change what every sibling clone measures mid-flight.
This run did the half that is its own, re-deriving and republishing the manifest against the tree as it
stands, and names the other half as a provisioning change.

**And the node states its own limits, which this file repeats rather than softening.** Its
`what_this_node_does_NOT_establish` field is explicit on three points. It does **not** establish AAP
§0.5.1 all-JAR coverage: 62 archives from 31 modules, against a reactor with 38 JAR-packaging projects
of which **7** contributed no archive, "so this graph is NOT the all-JAR graph AAP 0.5.1 mandates, and
this node must not be read as claiming it is". It does **not** offer a route to closing that shortfall
here: doing so "requires a new frontend build over all 38 projects' artifacts, which is a rebuild of a
host-shared graph this run may not perform". And it does **not** yield per-class provenance for an
overwritten class, because the frontend's `FILE` nodes name the `/tmp/jimple2cpg-<n>/` extraction path
and never the source JAR.

### Staged input set 2 — the 62 archives the graph that loads was built over

This is the input set every figure in sections 5 and 6 belongs to. It was measured member by member
from the tree on disk by `harness/artifacts/logs/cpg-input-inventory.json`, whose
`provenance_of_the_graph_this_inventory_describes` field states — since its 2026-09-03 re-anchoring —
that the graph is `/opt/blitzy-harness/cpg/spark.cpg` at **547,980,224** bytes, sha256
`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`, **written by PROVISIONING over this
staging tree, not by this run**. Every value in the table below was **re-measured against the live tree
on 2026-09-03** and is checkable there today, which is a change from the previous edition of this
subsection: the paragraph after the table states what changed, why, and what remains uncheckable.

| Property | Value |
| --- | --- |
| Staging tree | `/opt/blitzy-harness/cpg-input` — host-global, provisioning's, read-only to this run |
| Archives | **62** — `measured_member_count` 62 on 2026-09-03 |
| Total bytes | **285,122,375** — `measured_total_bytes`, and true three ways at once: the sum of the inventory's own `archives[]` figures, `du -sb` over the staging tree, and the figure `/opt/blitzy-harness/provision-log/cpg-record.txt` line 8 states (`Input : 62 JARs, 285,122,375 bytes`) |
| Distinct sha256 | **62** on the live tree — one digest per archive, so the archive-to-digest mapping is injective in both directions again; the inventory's `one_to_one` key, which the 2026-09-02 census had marked superseded for the live tree, **holds again** |
| Class entries across them | **76,151**, over 80,292 zip entries — unchanged, and the same for both graph generations |
| Reactor projects represented | **31** of the 38 JAR-packaging projects |
| Reactor projects absent entirely | **7**, each named in the record with the same reason: no archive of that project is in the tree |
| Archives marked that module's primary artifact | 32 — `common/network-yarn` has two, its main artifact and its unattached shaded shuffle JAR |

**The published byte total was four bytes low, and it was corrected on measurement rather than
argument.** `cpg-input-inventory.json` had published `total_bytes` **285,122,371**, against
`/opt/blitzy-harness/provision-log/cpg-record.txt`'s `285,122,375` and a `du -sb` of the staging tree
that also measures **285,122,375** — four bytes low against both the record and the disk. The reason it
survived arithmetic is worth stating, because it is the general case: the figure was **self-consistent**,
equalling the sum of the inventory's own `archives[]` entries exactly, so no internal check could see it
and only a member-by-member re-measurement against the tree could. That re-measurement was taken on
**2026-09-03T08:48:33Z** and is published in that file as the node `byte_total_correction_2026-09-03`:
**59** of the 62 members agreed with disk and **3** did not, and correcting those three closes the gap
exactly — `common_utils__original-spark-common-utils_2.13-4.1.0-SNAPSHOT.jar` 511,979 → **511,981**
(+2) and `common_utils__spark-common-utils_2.13-4.1.0-SNAPSHOT.jar` 517,232 → **517,234** (+2), while
`common_network-yarn__spark-4.1.0-SNAPSHOT-yarn-shuffle.jar` changed **content at constant length** and
contributes 0. `2 + 2 = 4`, which is the whole of the difference, and no other member moved. Two things
follow. The corrected total is now the same figure in three independent places, which is what AAP
§0.6.4 asks of a count that appears more than once — and it is confirmed a fourth time, independently,
by `cpg-frontend-input-manifest.json`'s 2026-09-03 delivery node, whose own sum over the same 62 files
is 285,122,375 with `members_disagreeing` **0**. And the class of error is closed rather than merely
fixed: **`harness/lib/verify_status_figures.py` now asserts this arithmetic on every run**, as its
fourth assertion family — `total_bytes` must equal the sum of the inventory's own `archives[]` figures,
`archive_count` must equal the length of `archives[]`, and where the shared staging tree is present both
must equal the measured sum and member count.

**Why the figures are re-provable now, what the earlier census measured, and the hazard that has not
gone away.** The staging tree is **hard links into `/opt/spark-src`**, a build tree shared with the
host's other clones and writable through those other paths, rather than immutable copies — all 62
members report `st_nlink=2`. That is a live staging defect, and its consequence has been observed twice.
`cpg-input-inventory.json`'s node `live_staging_tree_census_2026-09-02` is a **first-hand measurement of
a different moment**, taken at 2026-09-02T14:44Z **mid-rebuild** against the 2026-09-01 edition of
`archives[]`: it found the 62 names all still present but **39 members matching and 23 drifted**, a
`live_total_bytes` of **234,609,958** against a `retained_total_bytes` of **285,122,371** with the whole
50,512,413-byte difference on the 23, and only **45** distinct digests left across the 62 names — and it
concluded, in its own words, that the tree it saw "CANNOT RECREATE THE GRAPH AND CANNOT PROVE THE
GRAPH'S INPUT BYTES". **Those figures are that moment's and are retained as such; they are not restated
as current, and the correction node says so explicitly in its `relation_to_the_census_node` field.** By
the time the rebuild finished and the 2026-09-03 graph was written, the tree had settled at 62 members,
285,122,375 bytes and 62 distinct digests, which is the table above and is re-provable on disk today.
What has **not** changed is the hazard itself: the inodes are still shared, so a future rebuild of the
pinned tree can rewrite these members again, and every digest in this subsection is **as of
2026-09-03**. Staging by copy is the fix and it belongs to **provisioning** — `/opt/blitzy-harness/**`
is shared read-only across up to 64 clones and AAP §0.8.1 forbids this run from touching that tree, so
nothing was re-staged or re-linked here to make anything checkable.

**Three in-tree records legitimately still state 285,122,371, and none of them is wrong.**
`build-reactor.log:10096`, `cpg-frontend.log:128` and `cpg-graph-record.log:28` are **verbatim preserved
streams** under AAP §0.8.1, each stating the figure that was true of the tree at the moment it measured
it, and none is edited by this file. `cpg-graph-record.log` in particular is the write record of the
**2026-08-30** graph and cites the then-published `total_bytes` by name. Meeting 285,122,371 in any of
those three identifies text written before the 2026-09-03 re-measurement; the live figure is
**285,122,375** and its owner is `cpg-input-inventory.json`.

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
| Bytes | **547,980,224** |
| sha256 | **`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`** |
| Written by | **PROVISIONING**, 2026-09-03T01:40:31Z → 02:11:54Z (**31 m 23 s**), `FRONTEND_EXIT=0`, peak sampled RSS 61 GB — **not by this run**, and not rebuildable by it: the target is host-global and shared read-only across up to 64 concurrent clones |
| The frontend that wrote them | `jimple2cpg` (joern 4.0.607 bundled) over `/opt/blitzy-harness/cpg-input --recurse -J-Xmx64g`, stdin closed, `SL_LOGGING_LEVEL=WARN`, under Temurin **21.0.12.1+1** (major 21) at a measured `heap_max_bytes` of **68,719,476,736** |
| Record of account | `/opt/blitzy-harness/provision-log/cpg-identity.txt`, written beside the graph at write time, corroborated by `/opt/blitzy-harness/provision-log/cpg-record.txt` in the same directory; both were read and agree, and a disagreement between them is fatal |
| How it is resolved | `harness/lib/preflight_graph_identity.py` `record_of_account()`, the same function the Stage 3 gate calls, so the record and the gate cannot state different pairs; it prefers this checkout's own frontend write-time pair and fell back to the record written beside the graph because this run's frontend wrote no graph |
| The in-tree transcription of it | `harness/artifacts/logs/cpg-identity.txt`, cited for that standing rather than as a source of a figure |
| Re-measured, and by whom | by `joern-preflight.log` on 2026-09-03T09:07:46Z — `size : 547,980,224 bytes MATCH`, `sha256 : 325887cf… MATCH`, **VERDICT: PASS** — and again independently by each of the three `importCpg` loads and each of the three probe queries, in bash and from inside the loading JVM |
| The superseded pair | **541,309,809 / `4616845a…4730c7`**, the graph the 2026-09-01 lane loaded, replaced by the re-provisioning of 2026-09-03T01:17:07Z; and before it **541,255,894 / `26d327cc…`**, the 2026-08-24 provisioning's. Both are retained with their dates in the SUPERSESSION table at the head of this file, and neither is a live claim anywhere below |

**Re-measured for every one of the seven loads of 2026-09-03, every check logged, and every comparison
immediately before the load it gates.** The graph is inherited, so nothing about it is assumed between
stages: the bytes are re-read and re-hashed for each load and a mismatch halts instead of proceeding.
On the 2026-09-03 lineage all seven checks ran **immediately before** the load they gate, which is what
AAP §0.8.2 requires — including the Stage 3 Joern runner, where the 2026-09-01 generation had an
ordering defect that the paragraph after the table states in full and retains as history.

| Load | When the check ran | Record | Result |
| --- | --- | --- | --- |
| `importCpg` verification load 1 — the three counts, the floor, the 31-module witness table | 2026-09-03T09:36:13Z, before the load | `cpg-verify.log:54-57`, under the heading "GRAPH IDENTITY, RE-VERIFIED IMMEDIATELY BEFORE THE LOAD" at `:51` | `547980224 bytes`, sha256 `325887cf…3dc6` — MATCH on both fields |
| `importCpg` load 2 — the per-witness detail | before the load; the check is re-computed in bash and again from inside the loading JVM | `cpg-verify.log` PART 3, `identity_matches : true` for all three loads | MATCH on both fields |
| `importCpg` load 3 — the method breakdown, the file-node census, the probe surface | before the load, same mechanism | `cpg-verify.log` PART 3 | MATCH on both fields |
| Stage 3 Joern runner — the gate's comparison, **before** the invocation | 2026-09-03T09:07:46Z, one second before the runner started at 09:07:47Z, same clone (424) | `joern-preflight.log:36-37` timestamp and clone, `:47-48` recorded pair, `:56-57` re-measured `MATCH`/`MATCH`, `:60` "All 1 subject(s) resolve to one file: yes" | **VERDICT: PASS** |
| Stage 3 Joern runner — the runner's own recompute, contemporaneous | inside the invocation, 2026-09-03T09:07:47Z → 09:17:43Z | `harness/bin/run-joern.sh:57-58` computing `stat -c%s` and `sha256sum` over the resolved target; recorded in `runner-metadata.json` `.tools.joern.stage3_invocation_2026_09_03.graph_identity_the_runner_printed` | `bytes 547980224`, sha256 `325887cf…3dc6`, `agrees_with_the_records_of_account: true` |
| Stage 5 probe query 01 | 2026-09-03T09:45:34Z, before the load | `probe-01-callgraph-unguarded-driver-launch.identity.txt` | `bytes=547980224`, sha256 `325887cf…3dc6`, `verdict=PASS` |
| Stage 5 probe query 02 | 2026-09-03T09:55:51Z, before the load | `probe-02-dataflow-unguarded-driver-launch.identity.txt` | `bytes=547980224`, same sha256, `verdict=PASS` |
| Stage 5 probe query 03 | 2026-09-03T10:07:18Z, before the load | `probe-03-parameterized-handler-sink-pairs.identity.txt` | `bytes=547980224`, same sha256, `verdict=PASS` |

**Where the comparison lives, stated exactly, because the obvious reading is wrong.** **No provisioned
file performs it.** `harness/bin/run-joern.sh` **measures and prints** its input's size and digest —
lines **57** and **58** of the provisioned 76-line script, `stat -c%s` and `sha256sum` over the resolved
target — and then proceeds without comparing them to anything. `joern-preflight.log` says so in its own
words at lines 22-26 and again at `:33-35`: *"harness/bin/run-joern.sh does NOT read it: it prints its
input's identity without comparing it, and it is REFERENCE under AAP 0.6.1"*, and *"NOT a caller :
harness/bin/run-joern.sh"*. The comparison is performed **by this run, outside both provisioned files**,
by two gates invoked before the runner:

- `python3 harness/lib/preflight_graph_identity.py --check-only` — exit **0**, **VERDICT: PASS**,
  adjudicating `1,398,964 methods, agreed by 2 record(s) of account` and printing
  `Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420`. Its published console is
  `harness/artifacts/logs/joern-preflight.log`.
- `python3 harness/lib/preflight_scan_target.py --check-only` — exit **0**, **VERDICT: PASS**, carrying
  `[PASS] smoke-override-absent`, `[PASS] artifact-tree:HARNESS_RAW_DIR` and
  `[PASS] artifact-tree:HARNESS_LOG_DIR`. Its published console is
  `harness/artifacts/logs/sec-gate-scan-target.log`.

An earlier edition of this file cited three constructs **inside** `harness/bin/run-joern.sh` — a gate
invocation, a gate-status test and its closing branch — as the mechanism. Those were added by a
2026-09-02 edit to a file AAP §0.6.1 holds as REFERENCE and §0.8.1 states is never edited, and they were
**reverted on 2026-09-03**: the runner is byte-identical to its provisioned form again (76 lines, 3,380
bytes, sha256 `32dd647a…be65f`), as is `harness/lib/joern-scan.sc` (122 lines, 5,401 bytes, sha256
`cf7a3622…0cf0a`), and `runner_script_identity.reverted_on_2026_09_03` records the proof that
`git diff a64216aed7f -- harness/bin/ harness/lib/joern-scan.sc` prints nothing. **So nothing inside
either provisioned file gates the load, and this file no longer implies that anything does.** What binds
the Stage 3 load is the run-owned gate above, whose comparison ran one second before the invocation; the
same log notes that `harness/lib/run-joern-gated.sh` is the other binding caller, "which has no branch
reaching the runner after a non-zero gate", and that a **direct** invocation of the runner "is therefore
not bound by this status" — which is exactly why this run ran the gate itself rather than relying on the
runner. The run-level register entry is **D4** in `oss-scan-results/run-record.md` §13, with **D25** the
runner edit and its revert.

**The 2026-09-01 ordering defect, retained as history rather than restated as current.** In that
generation the two halves did not travel together. The **measurement** was contemporaneous: the runner
printed `cpg bytes       : 541309809` and `cpg sha256      : 4616845a…4730c7` at
`joern.runner-console.log:14-15` — a **verbatim preserved stream**, quoted here unaltered — inside the
invocation its own header brackets (`argv=["./harness/bin/run-joern.sh"]`,
`started=2026-09-01T14:25:10Z ended=2026-09-01T14:41:24Z`, `clone_index=13`). The **comparison** ran
late: that generation's `joern-preflight.log` was stamped `2026-09-01T14:52:54Z` with `Clone index : 0`,
about **11.5 minutes after the load ended and from a different clone**, so for that one load AAP §0.8.2's
"immediately before every load" was **not** satisfied, and no sentence in this file ever claimed it was.
What the late comparison did not put in doubt is *which bytes were read*: the pair the runner recomputed
equalled the pair the record of account then stated and the pair every other check of that generation
got, and the resolved file's mtime preceded all five of them. **No substitution occurred — the control
ran late and the outcome was sound**, two findings rather than one. Both the pair and the ordering are
superseded facts of a graph the re-provisioning has replaced; the live lineage is the table above.

**One contradiction, recorded with both values — and since corrected at the record.**
`harness/ENVIRONMENT.md` §7 stated this graph's identity explicitly, and the filesystem
contradicted it:

| Source | Bytes | sha256 | Methods |
| --- | --- | --- | --- |
| Source | Bytes | sha256 | Methods |
| --- | --- | --- | --- |
| `harness/ENVIRONMENT.md` §7 **as the gate read it** — the 2026-08-24 provisioning's graph | 541,255,894 | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | 1,397,339 |
| The 2026-08-30 graph, which the 2026-09-01 lane loaded — **also superseded** | 541,309,809 | `4616845a…4730c7` | 1,396,899 |
| **The bytes on disk, and what every 2026-09-03 load read** | **547,980,224** | **`325887cf…3dc6`** | **1,398,964** |

Neither the byte size nor the digest is a field the request's expected-values table carries, so on those
two fields the record was the only statement and observation contradicted it. The gate read that as AAP
§0.1.3's fourth case, and `harness/artifacts/logs/gate-record.json` carries it as
`gate.environment_record_graph_identity_agreement` — one of the two halts in a gate whose overall
verdict is `halt`. The cause is inherited rather than produced, twice over: the host was re-provisioned
on 2026-08-30 and again at **2026-09-03T01:17:07Z**, each time replacing the shared graph, and this run
built no graph of its own on either occasion.

**RESOLVED 2026-09-03, at the record and not at the graph.** The fourth case applies only where no
anchor exists to adjudicate between record and observation, and one does:
`/opt/blitzy-harness/provision-log/cpg-identity.txt`, written beside the bytes at write time and
corroborated by `cpg-record.txt` in the same directory. `harness/ENVIRONMENT.md` §7 and
`harness/artifacts/MANIFEST.json`'s `.cpg` member were re-anchored to that owner and now state the third
row of the table above — `cpg-verify.log` **PART 3.4** records the re-anchoring and the superseded
identities' retention in that document's labelled supersession tables, "never as live claims". The
consequence is measurable at the gate: `harness/lib/preflight_graph_identity.py --check-only` exits
**0** with **VERDICT: PASS**, where before the re-anchor it exited **77 VERDICT: HALT** naming
`ENVIRONMENT.md` §7 as the sole dissenting record and `./harness/bin/run-joern.sh` exited
**78 CONFIGURATION FAULT** without loading anything. **The graph was not touched, not rebuilt and not
replaced** and nothing was written under `/opt/blitzy-harness` — it could not have been, being shared
read-only across up to 64 clones — so every count, digest and coverage verdict in this file is a
measurement of the bytes that are there. All three pairs remain on the record with their dates, here and
in the SUPERSESSION table; `run-record.md` **D4** carries the divergence and **D25** the record edit as a
disclosed departure from AAP §0.6.1.

### The three counts, against their expected values

These are `cpg-verify.log`'s measurement, PHASE 1 as re-anchored on 2026-09-03 and corroborated by
PART 3, taken by the `importCpg` loads of exactly the bytes above — **three loads, one measurement
each, all four counts identical**, cited here and by section 6 rather than measured again. They describe
**provisioning's graph over its 62-archive input set**, never the complete-input graph the AAP mandates,
which does not exist.

| Count | Expected | Observed | Delta | Rule, and how the difference is classified |
| --- | --- | --- | --- | --- |
| Methods | 898,336 | **1,398,964** | +500,628 | **One-sided**: floor 853,420, no upper bound. The observation is above the floor — at **1.64×** it — and above the anchor, which AAP §0.9.3 **records** rather than halts |
| Type declarations | 87,381 | **119,860** | +32,479 | Anchor, reported; no threshold applies — a **recorded difference** under AAP §0.9.3, never a halt |
| Files | 38,818 | **45,037** | +6,219 | Anchor, reported; no threshold applies — a **recorded difference** under AAP §0.9.3, never a halt |

The loads' own supporting figures, from PHASE 1 and PART 3.2: internal methods **1,308,974**, external
methods **89,990** — and `1,308,974 + 89,990 = 1,398,964`, so no method is unaccounted for — methods
under `org.apache.spark.*` **927,304** (**66.29 %** of all methods), file nodes ending `.class`
**44,811**, file nodes ending `pom.properties` **102**, and file nodes containing `META-INF` **216**.
Each of the three loads ran under Temurin **21.0.12.1+1** with `java.specification.version` **21**, at a
child-JVM heap measured **inside that JVM** by `Runtime.maxMemory()` at **68,719,476,736** bytes — at or
above AAP §0.8.2's floor, and reached through `JAVA_TOOL_OPTIONS=-Xmx64g` because `-J-Xmx` sizes the
`joern` launcher rather than the forked child that holds the graph — each into its own fresh workspace
outside the repository, taking **526,605 ms**, **557,041 ms** and **545,644 ms** respectively.

**The two checks the floor exists for.** `methods > 0` — 1,398,964 is not zero, and a graph that loads
with zero methods is indistinguishable from a clean scan. And 1,398,964 is at or above the one-sided
floor of 853,420, so the truncation signature the floor exists to catch is absent; `joern-preflight.log`
adjudicates the same count from the records of account and prints
`Verdict : FLOOR SATISFIED -- 1,398,964 >= 853,420`. **No input was trimmed or added to move any of
these three numbers**, in either direction, and no upper comparison is performed — AAP §0.9.3 makes the
anchor one-sided, and adding a ceiling would halt the run for succeeding.

**What is not established, and is not guessed.** The *cause* of the excess over the anchors. The AAP's
stated rationale is the six extra JAR producers a full reactor packages, and those six are measured in
section 6 as **absent from this graph's input set**, so that mechanism cannot be the explanation here.
`cpg-verify.log` records the cause as not established and this file does the same rather than inventing
one.

### The two observed metrics

Both metrics below are `harness/artifacts/logs/cpg-frontend.log`'s recount from the frontend's own
preserved output stream, and both are **observed facts of this run's own invocation — the one that
failed and produced no graph — rather than pre-approved expectations**. That invocation is the one that
file's INVOCATION INDEX labels **invocation A**, and the index names STEPS 6 and 7 of the same file as
the owners of both figures, so neither belongs to invocation B's witness graph or to invocation C's
189-archive attempt. They describe processing of the complete 191-artifact set, not the graph on disk,
whose own figures are the subsection above. Neither is treated as acceptable because a document expected
some other number.

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

**Both metrics differ from the provisioning record's, all values are on the record, and they are not
the same measurement.** The write-time record of the graph on disk,
`/opt/blitzy-harness/provision-log/cpg-record.txt` **line 11**, states **31,598** `Overwriting class
file` warnings over **26,221** distinct class files and **67** AST-creation `RuntimeException`s, all
under `org/apache/spark` — corroborated by `harness/ENVIRONMENT.md:311-317`, which carries the same
three figures with a per-package split of its own and notes the runbook had expected roughly 5,700
warnings. Those belong to **provisioning's own frontend run over its 62-archive input set**
(`harness/ENVIRONMENT.md:849`), a different invocation over a different input from this run's
191-archive attempt, so the difference is not a discrepancy to reconcile: this run's figures are higher
because its input carries every pre-shade sibling and every `-tests` artifact that the 62-archive set
leaves out. `cpg-frontend.log` is the sole owner of this run's two figures, and no figure of this run's
is restated from any other record. **For completeness on the superseded generations, whose figures the
SUPERSESSION table retains:** the 2026-08-30 graph's own write record reports **429** AST-creation
failures (`cpg-graph-record.log`, a verbatim preserved stream), and **173** was the 2026-08-24
provisioning's figure for the graph before that. Three generations, three measurements of three
different frontend invocations, none superseding another **as a measurement of its own invocation** —
and only the 67 describes the bytes on disk today. **The per-class provenance limitation applies to
every one of these overwrite counts alike**: the warning names the destination class, never the archive
whose definition survived, so no winner map exists behind any of them (the next subsection states it in
full, and `cpg-record.txt` line 12 states it independently).

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

**The collision was resolved by querying the graph, which is what AAP §0.5.1 directs — never by
inferring a winner.** The plan is explicit that where a specific collision bears on a conclusion it is
settled by asking the graph what is there. It was, on the graph on disk today: `cpg-verify.log`
**PART 3.3**, under the heading *THE CONNECT-SHIMS COLLISION, MEASURED RATHER THAN INFERRED*, queried
the three classes the AAP names against the **547,980,224 / `325887cf…3dc6`** graph, on a load whose
identity was re-verified immediately before it, under JDK spec **21** at a child heap measured at
**68,719,476,736** bytes, by `importCpg` only:

| Class | Type declarations | Methods |
| --- | --- | --- |
| `org.apache.spark.SparkConf` | 2 | **298** |
| `org.apache.spark.SparkContext` | 2 | **1,100** |
| `org.apache.spark.rdd.RDD` | 2 | **1,022** |

All three are present **with full method bodies**, so the definition that survived the frontend's
replacement is **the real one and not the shim stub** — a stub `SparkContext` does not carry 1,100
methods. That is the per-class evidence for the bullet above rather than a new claim: both shims
archives are absent from this graph's input, so no stub displaced anything.

**Eight further classes were measured on the superseded graph only, and are labelled as such.**
`harness/artifacts/logs/cpg-shims-collision-measurement.log` queried all eleven shims classes in the
**2026-09-01 generation's** graph, with the identity re-verified as **541,309,809 /
`4616845a…4730c7`** before and after the load, under JDK major **21** at `-J-Xmx64g`, by `importCpg`
only, over a load reporting **1,396,899** methods — the pair and count the 2026-09-03 re-provisioning
superseded (SUPERSESSION table). It found every one of the eleven carrying its real implementation:
`SparkConf` **298**, `SparkContext` **1,100**, `rdd.RDD` **1,022**, `api.java.JavaRDD` **74**,
`sql.ExperimentalMethods` **7**, `SparkSessionExtensions` **112**, `execution.QueryExecution` **280**,
`internal.SessionState` **76**, `internal.SharedState` **128**, `sources.BaseRelation` **12** and
`util.ExecutionListenerManager` **116**. The three the AAP names were **re-measured on the live graph**
and returned identically, as the table above records; the other eight have **not** been re-measured
since the re-provisioning, and are stated here as that generation's measurement rather than as figures
for the graph on disk.

Neither measurement is **a winner map** — each states what a graph contains, not which archive the
frontend read last. **Per-class provenance for an overwritten class remains not measurable from this
frontend's output**, because its `FILE` nodes name the `/tmp/jimple2cpg-<n>/` extraction path and never
the source JAR; that limitation is stated wherever an overwrite count is cited in this file, and
`/opt/blitzy-harness/provision-log/cpg-record.txt` line 12 states it independently. And neither
establishes anything about a graph that *does* contain the shims archives, which is what the mandated
complete-input graph would have been.

**A narrower graph was retained, and what it does and does not establish is stated rather than
implied.** An earlier edition of this paragraph also said no comparison against a narrower graph was
made because none was retained. One was: the per-module witness graph `cpg-frontend.log`'s INVOCATION
INDEX labels **invocation B** — 38 primary artifacts, one per JAR-producing module, 130,718,491 bytes of
input — persists at **418,777,229** bytes and was loaded first-hand under `importCpg` on 2026-09-02,
recorded as `cpg-verify.log` **PART 2**. Its input *includes* the shims primary artifact, so its STEP P5
holds the other side of the comparison: eleven of the twelve classes it queries at stub-sized method
counts of 2, 4 or 8. Three limits are the records' own and are repeated rather than softened. PART 2
states that graph is "not the mandated graph, not at `harness/cpg/spark.cpg`, loaded by no runner,
contributing no dataset row", and that **no witness recorded in it is a coverage verdict** for the graph
the runners read — those verdicts are section 6's. Which definition survived any one collision is no
more measurable there than here, so still no winner map. And the two records disagree on whether that
graph exists at all: `cpg-shims-collision-measurement.log` states the narrowed graph "does not exist in
this generation", while `cpg-verify.log` PART 2 records loading it, and the file is on disk at the size
PART 2 states. Both statements are carried here with their sources rather than adjudicated, because no
figure in this file's sections 5 or 6 is taken from invocation B and the disagreement therefore moves
nothing; `oss-scan-results/run-record.md` §13 owns the run's divergence register.

---

## 6. The per-module graph coverage verdict

This file owns this verdict **as the document**, which is the ownership AAP §0.6.4 assigns; among the
evidence files the owner of record is `harness/artifacts/logs/cpg-module-coverage.json`, which
`cpg-graph-record.log:83-87` and `cpg-verify.log:258-260` both name as owning it and both cite rather
than remeasure. Neither ownership adds a measurement. The verdict has exactly two measured inputs and
neither is a document: `harness/artifacts/logs/cpg-input-inventory.json`, which computed witness
exclusivity by walking the 62 archives of the graph's input set, and
`harness/artifacts/logs/cpg-verify.log` PHASE 2 with its per-witness detail in **PART 3.1**, which
queried each candidate witness in the graph under the `importCpg` loads whose identity section 5
states. `cpg-module-coverage.json` is **not a third input**: it is a rendering of those same two files,
**rewritten to `schema_version` 3 on 2026-09-03** (`written_at_utc` `2026-09-03T10:33:02Z`, 852 lines)
against the graph identity section 5 states, and it agrees with the tables below at both denominators —
31 modules in the graph's input with 26 COVERED and 5 NO VERDICT OBTAINABLE, and 38 JAR-packaging
reactor projects with 26 covered and 12 without a covered verdict split 5 and 7, under its own
`count_check` — with 0 presence verdicts and 0 prefix verdicts either way. So every figure below is one
of the two measurements read once and cited by both this section and that file, never taken twice; and
that file's `supersedes` field records the **schema-2 edition written in clone `w-013` on
2026-09-01T16:57:30Z**, which described the superseded 541,309,809 / `4616845a…4730c7` graph.

**The arithmetic, stated once and in full, because two denominators are in play and neither substitutes
for the other.** `cpg-module-coverage.json`'s own `totals.count_check` states it in these terms:
**26 + 5 = 31** modules contributing an archive to the graph's input; **26 + 5 + 7 = 38** JAR-packaging
reactor projects, which is the denominator AAP §0.5.1 sets; **5 + 7 = 12** without a COVERED verdict.
Alongside it, `witnesses_absent_from_the_graph` is **0** — every one of the **31** candidate witness
classes, the 26 admissible ones and the 5 inadmissible ones alike, was **observed present** in the graph
as a type declaration under its exact full name (`cpg-verify.log` PART 3.1: `decls` is 2 or more for all
31 and 0 for none, so `WITNESS_ABSENT_COUNT` is 0). **Presence is not coverage**, and the five are
recorded as corroboration only, for the reason the next-but-one subsection measures.

**Which graph generation the first verdict column measures, stated before the table rather than after
it.** It is the graph every stage of this run loaded and the one identity section 5 states:
**547,980,224 bytes, sha256 `325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`**.
`cpg-verify.log` records **three** `importCpg` loads of exactly those bytes, performed by this
checkpoint's re-measurement in clone 424 on 2026-09-03 (`cpg-verify.log:34-35`) — its SUBJECT block
states the pair at `:40-41`, its pre-load identity check re-measures it at `:54-57`, and PART 3 records
all three loads agreeing on all four counts. Every "graph result" and every verdict in the first column
below, and the type-declaration cross-reference at the end of this section, are those loads'
measurements: the PHASE 2 witness queries at `cpg-verify.log:124-266`, with the per-witness detail for
all 31 modules in **PART 3.1**. **The exclusivity half of the test did not need re-measuring and was
not re-measured**: it is a property of the 62-archive input set, which the re-provisioning left with the
same 62 names from the same 31 modules, so `cpg-verify.log` PART 3.1 states in terms that "the PHASE 2
verdicts stand as written". No verdict here is a measurement of either superseded generation — not
**541,309,809 / `4616845a…4730c7`**, the graph the 2026-09-01 lane loaded and the re-provisioning
replaced, and not **541,255,894 / `26d327cc…`**, which was the inherited environment record's pair;
both are retained in the SUPERSESSION table at the head of this file, and `cpg-verify.log` names the
second only at its own `:84-88`, and only to identify the record the filesystem contradicted.

A figure that once read as a **third** coverage statement at a different denominator is **withdrawn by
its own record**, so no third verdict is stated below. `cpg-graph-record.log:78-82` withdraws the summary
line that read **31 of 31 contributing modules covered, 0 missing** — "THIS RECORD IS SUPERSEDED ON THIS
FIGURE … That figure is withdrawn rather than restated" — on the ground that the record's own witness
listing shows 26 modules with an exclusive class and 5 with none, and a module with no exclusive witness
is not covered under the test above. `cpg-graph-record.log:83-87` then names
`harness/artifacts/logs/cpg-module-coverage.json` as the owner of the coverage verdict, established by
the load `cpg-verify.log` PHASE 2 records, and **cites** its **26 COVERED on injective evidence and 5 NO
VERDICT OBTAINABLE** over the 31 modules present in the graph's input rather than remeasuring it. Those
are this section's own first-column figures at that denominator — one measurement cited twice — and they
are still never totalled with the 38-project column. This file's STATUS block states the withdrawal, and
section 7's authority-rule subsection adjudicates the inherited environment record's copy of the
withdrawn claim against the verdict below.

**And the same claim was made again, by the record of account of the graph now on disk — so it is
corrected here on measurement rather than left standing.** `cpg-graph-record.log` is the write record of
the **2026-08-30** graph; the 2026-09-03 re-provisioning wrote its own, and
`/opt/blitzy-harness/provision-log/cpg-record.txt` **line 10** states of the graph that loads today:
*"Coverage : 31 of 31 contributing modules covered (26 by a class unique to that module's artifact, 5 by
the module-exclusive META-INF/maven coordinate node)"*. **The second half of that claim does not hold,
and this run falsified it by measurement.** The measurement, and how it was taken, is the subsection
*The five modules in the input set for which no witness of either kind exists* below and the
`vendoring_that_destroys_exclusivity` node in `cpg-module-coverage.json` that backs it; the short form
is that each of the five has an `exclusive_class_count` of exactly **0** *and* a `pom.properties` node
that another module's archive also carries, so the coordinate-node fallback is unavailable to precisely
the five modules it is claimed for. **This is a recorded correction and not a halt.** AAP §0.1.3's
halting fourth case is for an *inherited* field on which no anchor can adjudicate between record and
observation; this is a derived analytical verdict about **this run's own re-measured output**, taken
under §0.5.1's test with the measurement published, so both values are recorded and the measured one
governs. `cpg-module-coverage.json` reaches the same disposition in its
`vendoring_that_destroys_exclusivity.correction_to_the_provisioning_record` node, and notes there that
its own schema-2 edition already carried the correct 26/5 split — so this file and the provisioning
record disagreed before this checkpoint and now disagree **explicitly** rather than silently.

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

**Two rows report `0` methods, and that is the correct observation rather than a coverage shortfall.**
`common/tags`' witness `org.apache.spark.annotation.AlphaComponent` is an **annotation** and
`sql/pipelines`' witness `org.apache.spark.sql.pipelines.AnalysisWarning` is a **sealed marker type**;
neither carries a method body to compile, so a type declaration with no methods is exactly what
bytecode holds for them. `cpg-verify.log` PART 3.1 states the same in its own words. **Both verdicts
rest on the type declaration being exclusive, which it is** — the method count is corroboration and was
never the test — so neither row is weaker than the other 24 and neither is softened.

**The full per-witness table, with the measurement each row rests on, is `cpg-verify.log` PART 3.1**,
which carries all **31** candidate modules — the 26 admissible above and the 5 inadmissible below — each
with its primary-artifact class count, its exclusive count, and the type-declaration, method and file
counts the graph returned for its witness. The rows in the table above are that table's, cited rather
than re-derived. Six of them, quoted so a reader can spot-check without opening it: `core` →
`org.apache.spark.Aggregator`, primary 5,097 / exclusive 5,096, decls 2 / methods 54 / files 1;
`sql_catalyst` → `AliasIdentifier`, 5,333 / 5,332, 2 / 40 / 1; `common_network-yarn` →
`YarnShuffleService`, 11,070 / 6,175, 2 / 36 / 1; `sql_connect_client_jvm` → `ConnectRepl`,
12,652 / 4,970, 2 / 2 / 1; `resource-managers_kubernetes_core` → `deploy.k8s.Config`, 145 / 144,
2 / 254 / 1; `sql_hive-thriftserver` → `ArrayFetchIterator`, 211 / 210, 2 / 258 / 1.

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
| `sql/connect/common` | 1,879 — **every one of them** also in the archive named right | 0 | 0 | `sql_connect_server__spark-connect_2.13-4.1.0-SNAPSHOT.jar` — which holds all 1,879; `sql_connect_client_jvm` holds 1,868 of them as well | **NO VERDICT OBTAINABLE** |

**How that was measured, so the verdict is checkable rather than asserted — and why it corrects the
provisioning record.** `cpg-module-coverage.json`'s `vendoring_that_destroys_exclusivity` node states
the method in its own words: *"for each of the five, every class of its primary artifact and every one
of its `pom.properties` entries was searched across all 62 archives of the other 30 modules"*. The
result is the table above at `exclusive_class_count` **0** for all five, plus the second half that
closes the fallback: each of the five carries a `META-INF/maven/**/pom.properties` node that **another
module's archive also carries**, so the module-exclusive-coordinate-node witness AAP §0.5.1 names is
unavailable to exactly these five. The node records `pom_properties_also_in` **28** for each of them,
and names the structural reason: `META-INF/maven/org.spark-project.spark/unused/pom.properties` is
present in **29 of the 31** modules' artifacts, so a `pom.properties` node is not automatically
exclusive either.

That is the measurement that falsifies the coverage claim made by the graph's own write-time record,
`/opt/blitzy-harness/provision-log/cpg-record.txt` line 10, which claims *"31 of 31 contributing modules covered
(26 by a class unique to that module's artifact, **5 by the module-exclusive META-INF/maven coordinate
node**)"*. The five modules that half of the claim is about are precisely the five above, and not one of
them owns a module-exclusive coordinate node. So **31 of 31 overstates the obtainable coverage**, and
the verdict at this denominator is **26 COVERED and 5 NO VERDICT OBTAINABLE**. Both values are on the
record with their provenance, and the measured one governs: this is a derived analytical verdict about
this run's own re-measured output, published with its measurement, rather than an unadjudicated
inherited field, so AAP §0.1.3's halting case does not reach it and §0.1.3's retain-both-values
requirement is what applies. `cpg-module-coverage.json`'s
`vendoring_that_destroys_exclusivity.correction_to_the_provisioning_record` sub-node carries the same
correction with the same measurement, and records that its own schema-2 edition already stated 26/5 —
so the disagreement predates this checkpoint and is now explicit rather than silent.

**One thing this measurement does not do.** It does not say these five modules' code is absent from the
graph. All five witness classes are **present** in it — `cpg-verify.log` PART 3.1 measures
`common_network-common` at 3 type declarations and 9 methods, `common_network-shuffle` at 3 and 15,
`common_utils-java` at 3 and 3, `sql_api` at 3 and 90, and `sql_connect_common` at 4 and 28 — which is
why `witnesses_absent_from_the_graph` is 0. Presence is simply not the test: a class the vendoring
archive also ships cannot testify that **this** module's artifact arrived, because the shaded archive
would vouch for it either way. The five are therefore corroboration and no verdict, exactly as
`cpg-verify.log` PART 3.1 records them.

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
This 38-project view of the verdict is the owner's own:
`harness/artifacts/logs/cpg-module-coverage.json` carries it under
`aap_0_5_1_coverage_of_all_38_jar_producing_projects`, with `obtainable_from_this_graph` **false**, the
same 26 and 12 and the same 5-and-7 split, each of the twelve named individually with its reason class,
and the arithmetic asserted in its `count_check` — `5 + 7 = 12; 12 + 26 = 38`. The table above is that
one verdict at that one denominator, not a second count of it. The same file states the attribution in
terms this section reaches independently from `build-reactor.log` STEP 13: the shortfall "is a
consequence of the GRAPH'S PROVENANCE, not of the build", and "not one of the twelve is missing because
its project failed to build".

### The same test over the input set the plan requires — a second measurement, never merged with the first

Everything above is measured over the **62** archives the graph that loads was built from. That
leaves one question the record could not previously answer: **would a graph over every JAR the
build produced be verdictable module by module?** It is worth answering separately because the
answer is not "yes, once D1 is cleared" — the witness rule's shortfall is only partly caused by the
narrowed input.

Measured on **2026-09-02**, first-hand from the pinned tree, over all **191** project outputs — the
set section 4 accounts for, reconciled here independently as **627** JARs enumerated, **191** the
projects' own, **422** copied runtime dependencies, **14** test-resource fixtures and **0**
undecided, with the arithmetic checked — under the same AAP §0.5.1 test and the same two admissible
witness kinds:

| Outcome, over the 191-archive set | Count |
| --- | --- |
| **JAR-producing modules under the test** | **38** |
| Witness kind (a): a class exclusive to the module's primary artifact | **29** |
| Witness kind (b): the module-exclusive `META-INF/maven/**/pom.properties` node — `sql/connect/shims` | **1** |
| **Modules with an accepted witness** | **30** |
| **NO VERDICT OBTAINABLE** — neither witness kind exists | **8** |
| — because the module has no archive in the set | 0 |
| Verdicts resting on presence, or on a shared prefix | **0** |
| Witness kinds admitted beyond the two AAP §0.5.1 names | **0** |

The eight, each with the archive that vendors both its classes and its descriptor, and the count of
the module's own primary classes that archive carries:

| Module | Primary classes | Also carried by | of those |
| --- | --- | --- | --- |
| `common/network-common` | 2,170 | the `network-yarn` shuffle uber-JAR | 2,170 |
| `common/network-shuffle` | 92 | the same | 92 |
| `common/utils-java` | 40 | the same | 40 |
| `common/tags` | 12 | `connector/kafka-0-10-assembly`'s main JAR | 12 |
| `connector/kafka-0-10` | 47 | the same | 47 |
| `connector/kafka-0-10-token-provider` | 17 | the same | 17 |
| `sql/api` | 1,203 | `spark-connect-client-jvm` | 1,203 |
| `sql/connect/common` | 1,879 | `spark-connect` (server) | 1,879 |

**What that means, stated as the measurement and not as a softening of it.** Even a graph built
over every JAR the reactor produced — the graph AAP §0.1.1 requires and **D1** records as
unobtainable — could be verdicted for at most **30 of the 38** modules under the two witness kinds
the plan admits. The shortfall for these eight is a property of the **rule against Spark's shaded
artifacts**, not of the narrowed input: a fat artifact that vendors a module whole vendors its
Maven descriptor with it, so both admissible witnesses disappear together. It is **reported, not
repaired**: no presence-for-exclusivity substitution, no shared package prefix, and no shaded
archive dropped from the comparison to manufacture uniqueness — each of those three would let one
archive vouch for a module whose own artifact might be absent, which is exactly what injectivity
exists to prevent. AAP §0.9.4's required outcome for a value that cannot be established is to name
it, and the eight are named here and in `run-record.md` §14.

**The two measurements differ in both directions, which is why they are two.** `common/tags` has an
exclusive class over the 62-archive set and **none** over the 191, because `kafka-0-10-assembly` —
absent from the graph's input, present in the build — vendors all twelve of its classes.
`sql/connect/shims` has no archive at all in the 62 and gains its descriptor fallback in the 191.
Six modules differ in witness kind between the sets. Neither measurement is a correction of the
other and no total is taken across them: the 62-archive verdict is the coverage **this graph**
supports, and the 191-archive verdict is the coverage the **rule** can support at this pin.

**Provenance of this measurement.** `harness/artifacts/logs/reverification-f4-module-witness-full-input-set.json`
carries the per-module rows for both sets, each with primary artifact, primary and exclusive class
counts, witness kind and name, exclusive descriptor nodes, verdict, and every vendoring archive with
the count of the module's classes it carries;
`harness/artifacts/logs/reverification-f4-module-witness-full-input-set.log` carries the human-readable
record with the commands, the inventory reconciliation and the verdict. Both were written by a
measurement pass that loaded **no** graph — the archive analysis is `zipfile` over the 110
bytecode-carrying archives of the 191, and every graph-side figure in this section remains the
`cpg-verify.log` load's. Two figures in that pass disagree with section 4's cited totals and both
values are published there rather than reconciled: own-artifact bytes **434,306,178** observed
against **431,184,822** cited, and class entries **101,752** against **99,723** — the pinned tree's
`target/` trees having been rebuilt after the lane that staged them, with 11 of 37 `original-`/main
pre-shade pairs now class-identical. Class exclusivity is a question about class names rather than
bytes, and the 62-archive measurement reproduces the existing verdict module for module, which is
the cross-check that bears on it.

### Cross-reference against the counts the verification load reports

The same 2026-09-03 loads produced both this verdict and section 5's counts, so they are one
measurement read two ways rather than two measurements: `cpg-verify.log` PHASE 1, re-anchored on
2026-09-03 and corroborated by PART 3's three loads, reports **1,398,964 methods, 119,860 type
declarations and 45,037 files** with `methods > 0` explicitly confirmed, and the 26 present witnesses
are type declarations counted inside that 119,860. Every PRESENT row's module is in the graph's input
set, and no witness was found for any module outside it — which is a check on both axes at once, because
a module in the input whose witness was missing would be a coverage failure, and a witness found for a
module outside the input would mean the witness was never exclusive. **All 31 candidate witnesses are
present** and `witnesses_absent_from_the_graph` is 0 (PART 3.1); the five that are present without a
verdict are the vendoring case the subsection above measures, not a missing witness.

**The weaker witness kind is functional on this graph, which is why the five and the seven are archive
facts rather than graph facts.** The corroboration load counts **102** `META-INF/maven/**/pom.properties`
file nodes and **216** file nodes containing `META-INF` at all, against **44,811** ending `.class`
(`cpg-verify.log` PART 3.2), so descriptor nodes are represented and queryable here. For the five, the
node exists in the graph's input but is not *exclusive* to the module — measured at
`pom_properties_also_in` 28 apiece, with `.../unused/pom.properties` in 29 of the 31 — and for the seven
no archive of the module is in the input at all. Neither outcome is a limitation of the query or of the
node type.

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
| `harness/ENVIRONMENT.md` §7 **as the gate read it** — the 2026-08-24 provisioning's graph | 541,255,894 | `26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc` | 1,397,339 / 119,691 / 45,037 |
| The 2026-08-30 graph, which the 2026-09-01 lane loaded — **also superseded** | 541,309,809 | `4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7` | 1,396,899 / 119,721 / 45,037 |
| **The file on disk, measured through the symlink and counted by `cpg-verify.log`'s three 2026-09-03 loads** | **547,980,224** | **`325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`** | **1,398,964 / 119,860 / 45,037** |

Neither field is carried by the request's expected-values table, and observation contradicted the
record, which the gate read as AAP §0.1.3's fourth case. `gate-record.json` records this as one of the
run's two halting gate checks — `gate.environment_record_graph_identity_agreement`, carrying both value
sets — and `gate_verdict.overall` is `halt`, with the gate record stating in terms that it authorises
nothing. **The fourth case does not in fact reach these fields, and the record is RESOLVED as of
2026-09-03.** That case applies only where no anchor exists to adjudicate between record and
observation; the graph's own write-time record of account,
`/opt/blitzy-harness/provision-log/cpg-identity.txt`, corroborated by `cpg-record.txt` in the same
directory, states the pair the bytes carry, so an anchor exists and the adjudicating statement governs.
`harness/ENVIRONMENT.md` §7 and `harness/artifacts/MANIFEST.json`'s `.cpg` member were re-anchored to it
and now state the **third** row above; every superseded pair is retained with its provenance in that
document's labelled supersession tables, so none is discarded and nothing about the graph moved. The
resolution is measurable at the gate: `harness/lib/preflight_graph_identity.py --check-only` exits
**0** with **VERDICT: PASS**, where before the re-anchor it exited **77 VERDICT: HALT** and
`./harness/bin/run-joern.sh` exited **78 CONFIGURATION FAULT** without loading anything
(`cpg-verify.log` PART 3.4; `joern-preflight.log`). **The graph itself was not written by this run and
was not touched by the correction** — and could not have been, being host-global and shared read-only
across up to 64 clones. `run-record.md` **D4** carries the divergence and **D25** the record edit.

What is **not** claimed here is that the bytes moved underneath the run. All **seven** loads of
2026-09-03 had the identity re-measured from the bytes and all seven of those records state the same
pair — **547,980,224 / `325887cf…3dc6`** — so this run is internally consistent on exactly one graph,
and on that lineage every comparison ran immediately before the load it gates (section 5's table).
What is **not** claimed either is that the 2026-09-01 generation achieved that ordering: for its Stage 3
Joern runner the recompute was contemporaneous while the comparison against the record of account ran
about 11.5 minutes afterwards from a different clone, which section 5 states in full and which moves no
value in the table above. The disagreement in that table is between the one graph on disk and records of
two earlier graphs at the same shared path.

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
have a write-time record of their own — `/opt/blitzy-harness/provision-log/cpg-identity.txt`,
corroborated by `cpg-record.txt` beside it — which states exactly one identity pair and equals them; and
the record the contradiction was raised against, `harness/ENVIRONMENT.md` §7, has been re-anchored to
that pair, so `harness/lib/preflight_graph_identity.py --check-only` now reaches **VERDICT: PASS** and
the contradiction **no longer holds** (`cpg-verify.log` PART 3.4). The three Stage 5 probe queries each
verified that pair against that record before their load and re-verified it after, each having loaded a
private copy of the verified bytes (section 5). So a **current** load is anchored, and so is the record.
**What has not changed** is that the *cause* was inherited rather than produced — two re-provisionings
replaced a shared graph while this run built none of its own — and that both superseded generations stay
on the record with their provenance. It is carried as **D4** in `oss-scan-results/run-record.md` §13,
which owns the register entry. **A separate note on the in-tree write record.**
`harness/artifacts/logs/cpg-graph-record.log` is the write record of the **2026-08-30** graph and is a
verbatim preserved stream; it is **not** byte-identical to the current
`/opt/blitzy-harness/provision-log/cpg-record.txt`, which the 2026-09-03 provisioning rewrote for its
own graph. Wherever this file cites `cpg-graph-record.log` it cites that generation's measurement, and
the live write-time figures come from `cpg-record.txt` and from
`cpg-frontend-input-manifest.json`'s 2026-09-03 delivery node.

**The Stage 3 lineage is that same one graph, and no superseded pair is part of it.** The Joern runner
of record read **547,980,224 / `325887cf…3dc6`**. `harness/bin/run-joern.sh` lines **57-58** recompute
the byte size and the digest from the resolved target with `stat -c%s` and `sha256sum` and print them —
and print them **only**, without comparing them, which is why the comparison is the run-owned gate's and
is recorded at `joern-preflight.log` with **VERDICT: PASS** at 2026-09-03T09:07:46Z, one second before
the invocation. `runner-metadata.json` `.tools.joern.stage3_invocation_2026_09_03` binds that
invocation end to end: `./harness/bin/run-joern.sh` with **no arguments**, started
**2026-09-03T09:07:47Z**, finished **09:17:43Z**, **596.83 s**, exit **0**, working directory
`/tmp/blitzy-harness-scratch/424/joern-run` outside the repository, child JVM measured externally with
`jcmd` at `-XX:MaxHeapSize` **68,719,476,736** on JDK 21, and artifact
`harness/artifacts/raw/joern.json` at **353,048** bytes, sha256
`f7f5f60e37aacdbf58ca2bf073c0682efeb81e256a516576b12d55aea8edc926`, carrying **690** findings with
`bound_reached` false on every query. **So the dataset's `joern` rows come from a load that read the
bytes on disk**, and both **541,309,809 / `4616845a…4730c7`** and **541,255,894 / `26d327cc…`** are
superseded generations rather than any lineage of the delivered artifact.

**The 2026-09-01 Stage 3 invocation is retained as verbatim history, and its records are not
re-anchored.** `harness/artifacts/logs/joern.runner-console.log` is that invocation's own console and
prints `cpg bytes       : 541309809` and `cpg sha256      : 4616845a…4730c7` at lines 14-15, inside the
header it brackets itself with — `run_id=w013-20260901T132807Z clone_index=13`,
`argv=["./harness/bin/run-joern.sh"]`,
`started=2026-09-01T14:25:10Z ended=2026-09-01T14:41:24Z elapsed_seconds=974.22 exit_status=0`, artifact
354,817 bytes. It is a preserved stream under AAP §0.8.1 and **no number in it was altered**.
`harness/artifacts/logs/runner-sequence.json` binds that console log, that generation's artifact, both of
the runner's streams and a **241-byte** status file to that one invocation by byte size and sha256 —
which is that record's own figure for that invocation, and is stated here as such rather than as a
description of the file now on disk.

**What `joern.status` is, so that nothing is looked for in it that it does not carry.** All nine
`<tool>.status` files are the runner's verbatim seven-line `scope_finish` trailer. As it stands after
the 2026-09-03 Stage 3 invocation, `joern.status` is **7 lines and 235 bytes** carrying exactly `tool`,
`exit_code`, `elapsed_seconds`, `artifact`, `artifact_bytes`, `scan_root` and `scan_root_source`, and its
`elapsed_seconds` is **596** with `artifact_bytes` **353,048**. It records **no graph identity of any
kind** and no command line, and the only figures this file cites from it are that artifact size and
elapsed time, both of which `runner-metadata.json` states independently.

Neither the runner nor its load is re-run **by this file** to settle any of this, and the reason is not
that anything is missing: `harness/bin/run-joern.sh`, `harness/env.sh`, `harness/lib/scope.sh` and
`harness/lib/joern-scan.sc` are all present and readable in this clone, and both provisioned files are
byte-identical to their provisioned form. AAP §0.6.4 makes the measurement already taken the one to cite
rather than a second measurement of the same thing. The unmet all-JAR requirement remains reported and
unrepaired; the inherited-record contradiction is resolved at the record, as stated above.

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
- **The graph as this run's own output** — **attempted and blocked**, not deferred, and now blocked
  twice over. This run invoked the frontend over its complete 191-artifact manifest under JDK 21 at a
  proven-committable 128 GiB heap; after 8 h 01 m, at a 113.3 GiB peak RSS, it failed in persistence at a
  fixed `Integer.MAX_VALUE - 8` array-length bound in flatgraph's string-pool writer, producing no
  graph. And independently of that, **the graph may not be rebuilt in this checkout at all**: the target
  is host-global and shared read-only across up to 64 concurrent clones, so a rewrite would give every
  sibling a truncated read and break the recorded identity for all of them — which
  `cpg-module-coverage.json` `.graph.written_by` states in its own words. Carried as halt-class finding
  **D1** in `run-record.md` §13 and evidenced end to end by `cpg-frontend.log`, with the ceiling
  mechanism re-verified at three heaps in `cpg-ceiling-reverify.log`; reported rather than repaired, and
  nothing was trimmed to obtain a graph.
- **A current-run method, type-declaration or file count** — none exists, because no current-run graph
  exists to load. Every count in this file is a measurement of **provisioning's** graph, taken by this
  checkpoint's three `importCpg` loads of 2026-09-03, and none is estimated.
- **Which archive supplied the surviving definition of any overwritten class, in any generation of the
  graph** — the frontend's `FILE` nodes name the `/tmp/jimple2cpg-<n>/` extraction path and never the
  source JAR, so the question is unanswerable from what it emitted, and
  `/opt/blitzy-harness/provision-log/cpg-record.txt` line 12 states the same limitation independently.
  Where a specific collision bore on a conclusion it was resolved by **querying the graph** for the
  class instead, as AAP §0.5.1 directs — section 5's `sql/connect/shims` table.
- **Whether the eight further `sql/connect/shims` classes still carry their real implementations in the
  graph on disk** — measured on the superseded 2026-09-01 graph and **not** re-measured since the
  2026-09-03 re-provisioning. Only the three classes AAP §0.2.1 names were re-queried (section 5). The
  eight are named with the generation they were measured in rather than restated as current.

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
  complete 191-artifact input set — **invocation A** by `cpg-frontend.log`'s INVOCATION INDEX — split by
  entry kind, attributed by contributing module under the
  log's own overlap caveat, with the **403** nested-archive entries recorded as such, the provenance
  limitation stated and no winner map presented. The provisioning frontend figures for the graph that
  loads — **31,598** warnings over **26,221** distinct class files and **67** AST-creation
  `RuntimeException`s, from `/opt/blitzy-harness/provision-log/cpg-record.txt` line 11 — are recorded
  beside them in section 5 as a different invocation over a different input set rather than as a
  conflict, together with the 2026-08-30 generation's **429** and the 2026-08-24 generation's **173**,
  each labelled with the invocation it measures.
- **A reactor that failed and was then resolved project by project** — this did not occur: the reactor
  succeeded, all 40 projects have an outcome, all 38 JAR-packaging projects produced their artifact,
  and no diagnostic log was needed or written.
- **The graph's three counts against their expected values** — methods **1,398,964** above the 898,336
  anchor and at **1.64×** the one-sided 853,420 floor, type declarations **119,860** against 87,381,
  files **45,037** against 38,818 — together with the input-set difference **D3** that bounds what they
  describe: they are provisioning's graph over 62 archives from 31 modules, never the complete-input
  graph the AAP mandates. The superseded 2026-09-01 readings, 1,396,899 / 119,721 / 45,037, are retained
  in the SUPERSESSION table at the head of this file.
- **The graph identity itself, replaced under this run twice by re-provisioning** — 541,255,894 /
  `26d327cc…` (2026-08-24), then 541,309,809 / `4616845a…4730c7` (2026-08-30), then the
  **547,980,224 / `325887cf…3dc6`** on disk since 2026-09-03T01:40:31Z. Each is on the record with its
  date and its producer; none was produced by this run, and none could have been, the target being
  host-global and shared read-only across up to 64 clones.
- **The four-byte correction to the graph input's published byte total** — `total_bytes` 285,122,371 →
  **285,122,375** in `cpg-input-inventory.json`, traced to three hard-linked staging members rewritten
  in place, with the arithmetic `2 + 2 = 4` closing the gap exactly and the assertion now enforced on
  every run by `harness/lib/verify_status_figures.py` (section 4).
- **The provisioning record's coverage claim, corrected on measurement** —
  `/opt/blitzy-harness/provision-log/cpg-record.txt` line 10's "5 by the module-exclusive
  META-INF/maven coordinate node" does not hold, because each of those five owns no exclusive
  coordinate node either; the verdict is **26 COVERED and 5 NO VERDICT OBTAINABLE**, both values are on
  the record, and section 6 carries the measurement.
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
taint as the **sole** difference. Per-arm figures are each arm's own log and its own SARIF. **Every arm
in this section — the mandated pair, the whole-ruleset pair, the two controls and the separate
`HiveShim.scala` pair — was run at scan root `/opt/spark-src` with `cwd` there, against observed
`HEAD` `59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d` equal to the expected pin, in this checkout under run
id `w022-20260902T144244Z`**, each arm's log stating its own root and observed HEAD. So no figure below
is inherited: all of them are this run's own measurement at the pinned root, and each arm's log records
that the copy retained before its rewrite had measured a *different* root and that none of those earlier
figures was carried over.

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
| **on** | 29 rule-bearing directories, 58 argv elements, 2,006 rules of which 241 multilang and 25 Scala applied to this file | 0 | 72 s | 1 in `DiskStore.scala` at its line 72, traced | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |
| **off** | the same 29 directories, the same rule count | 0 | 77 s | 1 in `DiskStore.scala` at its line 72, traced | 2,939,276 | `fe3d0167960a601c89379fe478ad349d55e4a8ac8c7d02624be12ec5b6096c51` |

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

**The option surface behind that statement, with the two figures it involves kept apart.** APPENDIX C
of `harness/artifacts/logs/taint-ab-off.log` carries the measurement verbatim, and it publishes two
different numbers that a reader must not merge. `opengrep scan --help | grep -c "^ \+--"` returns
**107** at either terminal width — the appendix records that this run re-measured it and **corrected the
figure this evidence set previously published, 106, to 107**, "which is what the command actually
prints", noting that 101 of the 107 matched lines are option declarations and 6 are wrapped description
continuations. The engine's **true long-option surface is 110**, because 9 options are declared with a
short alias first and can never match that pattern. Against either denominator the taint result is the
same and is what this subsection rests on: exactly **two** options have a name mentioning taint, and
the appendix shows them as the engine's own `grep -n` over its help text returned them —
`192:       --guarded-taint-signatures` and `445:       --taint-intrafile`. A case-insensitive search of
the whole 558-line help text for "taint" matches nothing but those two names and their own descriptions,
and **neither disables taint analysis**. There is no `--no-taint`,
`--disable-taint` or `--taint-off` at this version.

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
otherwise trip over, restated because the files it described have changed:** an earlier edition of this
paragraph reported that these two logs recorded their output under the **pre-rename** names
`taint-ab-on.sarif` and `taint-ab-off.sarif`, and that neither of those files existed. Both halves are
now false and are withdrawn. Each `HiveShim.scala` log records its own subject-bearing output path —
`harness/artifacts/logs/taint-ab-hiveshim-on.sarif` and `…-off.sarif` — in its `sarif` field and in its
verbatim command, and the digests above are those files'. And `harness/artifacts/logs/taint-ab-on.sarif`
and `harness/artifacts/logs/taint-ab-off.sarif` **do** exist: they are the mandated-subject arms' own
outputs, 4,753 bytes each and both `7949617b…845778`, the same pair the anchor-named files carry, which
is what `taint-ab-hiveshim-on.log` means when it says this pair "stands BESIDE the mandated pair's unmet
anchor (taint-ab-on.log / taint-ab-off.log), never in place of it".

**This pair does not satisfy the AAP requirement and is not offered as satisfying it.** It is a
discriminating result — 2 against 0 from one flag — on a file the AAP does not name, and the AAP names
one subject. Reporting a different file's pair as though it met the mandated one is exactly the
substitution AAP §0.1.3 forbids. It is recorded here as its own measurement, with its own subject, and
the mandated pair's verdict above stands unchanged.

### Two controls on the mandated file, and what each excludes

| Control | Rule change | Observed | What it excludes |
| --- | --- | --- | --- |
| Search-mode | the same patterns with `mode: taint` **removed**, the rule preserved verbatim at `harness/artifacts/logs/taint-ab-off-control-rule.txt` | **2** findings, `DiskStore.scala` lines **72** and **215**, **no** `codeFlows` on either — `harness/artifacts/logs/taint-ab-search-control.sarif`, **4,589** bytes, sha256 `4dc4aec5f35425f7ff47712baa55a02bcd1f034627d23b0d6f38ba209213b116` | that the taint rule's line-72 result is merely a pattern match: the pattern alone matches a **second** site the taint rule never reports |
| Source-removed | `mode: taint` kept, `pattern-sources` replaced with an unmatchable marker, the rule preserved verbatim at `harness/artifacts/logs/taint-ab-source-removed-control-rule.txt` | **0** findings — `harness/artifacts/logs/taint-ab-source-removed-control.sarif`, **2,455** bytes, sha256 `9c54e593e7a9dda361ef2de373bcdb17f0ed4c219c8f18057cf12ca2b1469172` | that the line-72 result is source-independent: remove the source and it disappears, so it is genuinely source-driven |

**Why both control digests moved while both controls' findings did not, disclosed because a changed
digest beside an unchanged result otherwise reads as tampering.** Both controls were re-run by this run
at the pinned root, from `cd /opt/spark-src`, with the retained control-rule file itself passed as
`--config`, so the rule text published here is the exact text the engine read. Opengrep derives the
SARIF `ruleId` from the path of the directory that `--config` file sits in, and that directory is now
this checkout's `harness/artifacts/logs/` rather than the scratch directory of the clone the previously
retained copies came from — the search-mode control's results carry
`tmp.blitzy.blitzy-spark.blitzy-…-w-022_77c1e7.harness.artifacts.logs.tainted-sql-string` — so the
embedded identifier and therefore the bytes of the report change even though nothing about the analysis
does. `taint-ab-off.log` APPENDIX E states that cause in the same terms and gives the two control rule
files' own identities, 1,982 bytes / `a1039db8…` and 2,498 bytes / `a8bc7f99…`, each verified to
reproduce the retained rule bytes exactly. The findings are what the controls are for and they are
unchanged: **2** results at `DiskStore.scala` lines **72** and **215** with `codeFlows` absent on both,
and **0** results respectively, under driver `Opengrep OSS 1.27.1` in both files. Both byte sizes and
both digests above were re-measured from the files on disk with `stat -c%s` and `sha256sum`, and both
result sets were re-parsed from the same files, at the time this section was written.

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

1. **Every figure names a producer record, and that record is one of the twenty-two listed at the top.**
   PASS — section 1 cites `maven-preflight.log`; sections 2 and 3 cite `build-reactor.log` by step
   (STEPS 3, 4, 6 to 15), and `harness/ENVIRONMENT.md:205-272` only to say where the expected-values
   table's 32 producers came from; section 4 cites `build-reactor.log` STEP 13, `cpg-frontend.log`
   STEPS 1, 4 and 11, `harness/artifacts/MANIFEST.json`, `cpg-input-inventory.json` with its
   `byte_total_correction_2026-09-03` and `live_staging_tree_census_2026-09-02` nodes,
   `cpg-frontend-input-manifest.json` in both of its bodies, and `cpg-graph-record.log`; section 5 cites
   `/opt/blitzy-harness/provision-log/cpg-identity.txt` with `cpg-record.txt`, `cpg-identity.txt` for its
   standing, `cpg-verify.log` including PART 2 and PARTS 3 to 3.4, `joern-preflight.log`,
   `sec-gate-scan-target.log`, `runner-metadata.json`, `joern.runner-console.log`, the three
   `probe-*.identity.txt` files, `cpg-frontend.log` STEPS 6, 7 and 11 and its INVOCATION INDEX,
   `cpg-ceiling-reverify.log`, `cpg-shims-collision-measurement.log` and `gate-record.json`; section 6
   cites `cpg-input-inventory.json`, `cpg-verify.log` PHASES 1 and 2 with PART 3.1 and PART 3.2, for its
   second measurement `reverification-f4-module-witness-full-input-set.json` with its `.log`,
   `cpg-graph-record.log:78-82` for the withdrawal of its former summary coverage figure and `:83-87`
   for the owner it now names, `/opt/blitzy-harness/provision-log/cpg-record.txt` line 10 for the
   coverage claim this file corrects on measurement, and `cpg-module-coverage.json` as that owner for the
   same verdict at its 38-project denominator and for the `vendoring_that_destroys_exclusivity`
   measurement behind the correction; section 7 cites `joern.runner-console.log`, `runner-sequence.json`,
   `runner-metadata.json`, `joern.status` for what it does **not** carry, and `harness/ENVIRONMENT.md` as
   the contradicted record. Checked mechanically as well as by reading: every multi-digit figure in this
   file was extracted and matched against those records, with none unmatched, and every
   `pom.xml`/`build/mvn`/module-pom line citation was resolved in the pinned tree. The absence
   of any `build-<module-path>.log` is itself recorded, and `cpg-module-coverage.json` is named for what
   it is — the owner of record of the coverage verdict, which `cpg-graph-record.log:83-87` and
   `cpg-verify.log:258-260` both name as owning it and both cite rather than remeasure, and which is
   itself a rendering of the two measurements section 6 reads rather than a second measurement of them —
   with its `schema_version` **3**, its `written_at_utc` `2026-09-03T10:33:02Z` and its `supersedes`
   field naming the **schema-2 edition written in clone `w-013` on 2026-09-01T16:57:30Z** that it
   replaced, and nothing taken from that edition.
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
   ships, **0** on a package prefix, and **0** additional witness kinds are admitted. All **31**
   candidate witnesses were observed **present** in the graph — `witnesses_absent_from_the_graph` is 0
   — and the five present-without-a-verdict modules are recorded as corroboration only, with the
   per-module vendoring measurement (`exclusive_class_count` 0 and `pom_properties_also_in` 28 apiece)
   published so the verdict is checkable; that measurement is also what **corrects**
   `/opt/blitzy-harness/provision-log/cpg-record.txt` line 10's claim that those five are covered by a
   module-exclusive coordinate node, and both values are on the record. Two covered witnesses report 0
   methods and are named as annotation and marker types, with the verdict resting on the exclusive type
   declaration rather than on the method count. No second verdict
   column exists, and no narrowed or witness graph is presented as a substitute for the mandated one.
   The **second measurement** added on 2026-09-02 applies the same two witness kinds to the
   **191**-archive set the plan requires and is kept separate from the first rather than merged with
   it: **30** modules with an accepted witness, **8** with neither, every one of the eight named with
   the archive that vendors both its classes and its descriptor, and the same three zeros. Its producer
   records — `reverification-f4-module-witness-full-input-set.json` and its `.log` — are named where it
   is stated, it loaded no graph, and the two figures in it that disagree with section 4's cited totals
   are published with both values rather than reconciled.
4. **The staged input sets are described accurately, and the assertion is described as recorded before
   the frontend ran.** PASS — section 4 keeps the two apart. For the 191-archive set this run's frontend
   was given it reports `cpg-frontend.log` STEP 1's own values, including `assertion result True` taken
   before the invocation, the 191-versus-189-distinct-digest multiset argument for why a bidirectional
   mapping is the only sufficient form, and — stated rather than implied — that STEP 1 has since re-run
   that assertion first-hand against the surviving tree at 191 of 191 identical and 0 drifted, that no
   staging tree exists in **this** checkout, and that no persisted ordered per-entry manifest of those
   191 members exists in this tree at all (STEP 11). It also states, rather than implying, that that
   manifest's ordered body describes **invocation C only** — of whose 189 entries just **61** resolve to
   a file in this checkout's reach — and that the delivery evidence for the graph that loads is the
   **added** node `delivery_evidence_for_the_graph_the_runners_load_2026_09_03`, re-derived first-hand
   on 2026-09-03 against the live staging tree, total and injective in **both** directions over 62
   archives with `assertion_errors` `[]` and a byte total agreeing with `du -sb`. For the 62-archive set
   the graph was built over it reports `cpg-input-inventory.json`'s member-by-member measurement, **now
   re-measured against the live tree on 2026-09-03** at 62 / 285,122,375 / 62 distinct digests, with the
   four-byte correction and its arithmetic published, the 2026-09-02 census retained as a measurement of
   a different moment, and the hard-link mutability hazard named as still live.
5. **No winner map is claimed anywhere; the provenance limitation is stated.** PASS — section 5 states
   the limitation in terms, quotes `cpg-frontend.log` STEP 6's own caveat that module attribution
   overlaps by construction, and publishes no destination-package or containment grouping at all. The
   `sql/connect/shims` collision is reported to exactly the depth the records support: 361 warnings
   attributed to that module, both its archives absent from the graph that loads, and — resolved **by
   query** as AAP §0.5.1 directs — `SparkConf` at 2 type declarations and 298 methods, `SparkContext` at
   2 and 1,100, and `rdd.RDD` at 2 and 1,022, measured against the graph on disk in `cpg-verify.log`
   PART 3.3, so the surviving definitions are the real ones and not the stubs. The eight further classes
   are labelled as `cpg-shims-collision-measurement.log`'s measurement of the **superseded** 2026-09-01
   graph and are not restated as figures for the graph on disk. None of that names a surviving archive
   for any one collision, which remains unmeasurable, and the limitation is repeated wherever an
   overwrite count is cited.
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
   diagnosis of the fixed array-length bound, the three-heap re-verification across a 16× reported-heap
   span, the refused partial write with its size and digest, and the six mitigations examined against the
   frontend's own flag surface.
   The 2026-09-02 pass's re-measurement of that same bound in this clone — 8, 64 and 128 GiB,
   identical buffered byte count and identical message — is cited in the STATUS block with its own
   producer records rather than folded into the earlier figure. The STATUS block also states the
   **second, independent** reason the requirement stays unmet: the graph may not be rebuilt in this
   checkout at all, its target being host-global and shared read-only across up to 64 concurrent clones,
   which `cpg-module-coverage.json` `.graph.written_by` records in the same terms. And it states build
   completeness **distinctly** — `BUILD SUCCESS`, all 38 JAR-packaging projects with their own artifact
   (`jar-packaging WITHOUT one : []`), all 191 own artifacts staged and supplied, the frontend failing in
   persistence — so the coverage shortfall cannot be misread as a build failure.
9. **One graph identity, stated wherever the graph is cited, with its provenance and its
   re-verification; and every superseded identity appears only as something superseded.** PASS — one
   live pair, **547,980,224** bytes and sha256 `325887cf…3dc6`, with
   `/opt/blitzy-harness/provision-log/cpg-identity.txt` (corroborated by `cpg-record.txt`) named as the
   record of account and `record_of_account()` named as how it is resolved; the graph is stated as
   **written by provisioning on 2026-09-03, not by this run and not rebuildable by it**, at every place
   it is cited. Section 5 lists all **seven** loads of 2026-09-03 with their timestamps and results, and
   every one of them had its comparison against the record of account run **immediately before** the
   load it gates, which is what AAP §0.8.2 requires — including Stage 3, whose gate published
   **VERDICT: PASS** at 09:07:46Z one second before the 09:07:47Z invocation. **Three** superseded
   identities appear in this file and every appearance is labelled as such with its date and generation:
   **541,309,809 / `4616845a…4730c7`** (the 2026-08-30 graph, which the 2026-09-01 lane loaded) and
   **541,255,894 / `26d327cc…`** (the 2026-08-24 provisioning's, which the environment record then
   stated) are carried in the SUPERSESSION table at the head of this file and in section 5's and section
   7's contradiction tables, and are quoted verbatim where a preserved stream states them —
   `joern.runner-console.log:14-15`, `cpg-graph-record.log`, `cpg-verify.log` APPENDIX A and PART 2 —
   with the live value given beside each quote and **no number inside any quotation altered**. The
   2026-09-01 Stage 3 ordering defect is likewise retained as history rather than restated as current.
   **No live figure in this file is taken from any superseded generation.**
