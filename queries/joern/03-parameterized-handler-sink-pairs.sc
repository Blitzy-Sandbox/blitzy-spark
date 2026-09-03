/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ===========================================================================
// queries/joern/03-parameterized-handler-sink-pairs.sc
//
// Probe query 3 of 3. Hand-written Joern capability probe: the PARAMETERIZED
// form of the handler-to-privileged-launch question, instantiated on TWO named
// handler/sink pairs in ONE run, over the code-property graph built from the
// pinned tree's bytecode.
//
// WHAT THIS FILE IS FOR
//   The probe answers a capability question about Joern - what a human can
//   express in its query language against THIS graph - and nothing else. It is
//   OBSERVATIONAL: it judges no finding, ranks no tool and interprets nothing
//   across tools (AAP 0.1.3, 0.3.2). It contributes NO dataset row: nothing it
//   writes lands in harness/artifacts/raw/ and nothing it produces is folded
//   into oss-scan-results/findings.json, which would corrupt both Joern's row
//   count and the dataset total. Joern's Stage 3 runner is that tool's first
//   appearance; this tree is the deliberate second (AAP 0.3.2).
//
//   The graph is loaded with importCpg ONLY. The frontend-then-importCpg route
//   is mandated because the alternative spawns a second JVM at the same maximum
//   heap (AAP 0.5.1, 0.6.2). This file must contain no textual occurrence of
//   that alternative at all - the appearance IS the violation.
//
// WHAT THIS FILE OWNS THAT 01 AND 02 DO NOT
//   The third of the probe's three effort measures: PARAMETERIZABILITY. It
//   passes ONLY because this query is actually invoked on the SECOND named pair
//   and that invocation's result is captured in this query's two result files
//   and in its console log. A parameter list that merely exists does not
//   satisfy it (AAP 0.6.2). Queries 01 and 02 both defer the measure here and
//   assert nothing about it; this file is where it is settled, and it is
//   settled by an invocation rather than by a claim. An EMPTY result from a
//   real invocation satisfies the measure; a skipped invocation does not, which
//   is why a malformed pair aborts the run loudly instead of being passed over.
//
// WHAT IS INHERITED RATHER THAN REINVENTED
//   Queries 01 and 02 established the conventions this file restates: the
//   constants, the load protocol, the symlink-FOLLOWING graph-identity block,
//   the marker protocol, the named-bound and traversed/truncated counter
//   conventions, the JSON envelope shape and - critically - the predicate
//   selector set that defines "spurious". That predicate block is carried here
//   BYTE-IDENTICALLY (see the banner around it below): three spurious counts
//   are only comparable if the definition of the term is the same text in all
//   three files.
//
// STAGE 5 POSITION
//   This is one of the four heap-bound JVM invocations the run records
//   separately (frontend build, importCpg verification load, Stage 3 Joern
//   runner, the Stage 5 probe). Stage 5 runs after normalization so that only
//   one 64 GB Joern process is ever live (AAP 0.5.1, 0.5.4).
//
// HOW TO INVOKE (the heap is the part that is easy to get wrong - see below)
//   cd <a scratch directory outside the repository>   # joern eagerly creates
//                                                    # ./workspace in its cwd
//   HARNESS_REPO_ROOT=<repo>  JAVA_HOME="$JAVA_HOME_21" \
//   JAVA_TOOL_OPTIONS="-Xmx64g" SL_LOGGING_LEVEL=WARN \
//     joern --script <repo>/queries/joern/03-parameterized-handler-sink-pairs.sc \
//       -J-Xmx64g < /dev/null
//
//   Joern needs stdin closed (its REPL blocks on an open stdin) and exposes no
//   version flag, so its version is read from the startup banner rather than
//   from --version; this script reports the JDK and the heap it observes and
//   leaves the banner to the console stream.
//
//   MEASURED, NOT ASSUMED: joern's --script path forks a child JVM
//   (replpp.scripting.ScriptRunner spawns `java -classpath ... ` with no JVM
//   options forwarded), so -J-Xmx reaches the LAUNCHER JVM only. On this host
//   the child then runs at the ergonomic default, far below the floor.
//   JAVA_TOOL_OPTIONS is inherited by the child and is the environment
//   override that actually raises the heap the query runs at. This script
//   therefore measures Runtime.maxMemory() and HALTS below the floor: raising a
//   heap is permitted and reported, lowering one is not, because a truncated
//   result's silence cannot be told apart from a clean one (AAP 0.8.2). This
//   query traverses TWO pairs, so a truncation here would be silent twice.
//
//   The provisioned Joern runner defaults its own JAVA_OPTS to a value BELOW
//   the 64 GiB floor; the floor is reached through the documented environment
//   override above, never by editing a runner (AAP 0.8.1). No runner is invoked
//   by this file at all.
//
// THE TWO PAIRS, AND WHY A PARAMETER IS NOT A CONVENIENCE HERE
//   PAIR ONE  handler org.apache.spark.deploy.master.Master.receiveAndReply
//             sink    the privileged process launch on the DriverRunner surface
//             This is the pair queries 01 and 02 address. Instantiating the
//             parameterized form on it is what makes the duplicate-formulation
//             question below answerable on evidence rather than by assertion.
//   PAIR TWO  handler the REST submit servlet's handleSubmit
//             sink    the SAME privileged process launch
//             A different handler, a different enclosing type, a different
//             number of boundaries and - measured below - a different answer to
//             "where does the handler's body live in the graph".
//
//   ONE NAMING RESOLUTION, STATED SO IT IS NOT READ AS A DISCREPANCY. The plan
//   names pair two's handler "StandaloneRestServer handleSubmit" after the FILE
//   it lives in, core/src/main/scala/org/apache/spark/deploy/rest/
//   StandaloneRestServer.scala. The method at :268 is declared inside the
//   separate class StandaloneSubmitRequestServlet at :171 of that same file, so
//   the TYPE the graph carries - and the type this query selects on - is
//   org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet. Selecting on
//   the file's headline class would select a type that does not declare the
//   method, which is exactly the kind of detail a parameterized selector has to
//   get right to generalise. Both names are reported.
//
// THE VERIFIED TARGET SURFACE
//   Every line number below was verified at commit
//   59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d with `git show <sha>:<path>`.
//   These paths resolve inside the PINNED clone exported as SPARK_SRC; the
//   working checkout this file lives in is neither built nor scanned. Where the
//   branch tip moved a line (Worker.scala's LaunchDriver case, for one) the PIN
//   is what is quoted here.
//
//   PAIR ONE HANDLER - core/src/main/scala/org/apache/spark/deploy/master/
//                      Master.scala (1,436 lines at the pin)
//     :239  override def receive: PartialFunction[Any, Unit] = {
//     :409  override def receiveAndReply(context: RpcCallContext)
//             : PartialFunction[Any, Unit] = {          <-- the handler
//     :410    case RequestSubmitDriver(description) =>
//     :411      if (state != RecoveryState.ALIVE) {     <-- a RECOVERY-STATE
//                 check, NOT an authorization or ACL predicate. It is not part
//                 of the predicate set defined below and is not reported as one.
//     :417      val driver = createDriver(description)
//     :419      waitingDrivers += driver
//     :421      schedule()
//     :923  private def canLaunchDriver(...)          (called :964, :983)
//     :944  private def schedule(): Unit = {
//     :967/:986  launchDriver call sites
//     :1130 val newDriver = createDriver(driver.desc)
//     :1356 private def createDriver(desc: DriverDescription): DriverInfo = {
//     :1363 private def launchDriver(worker: WorkerInfo, driver: DriverInfo)
//     :1367   worker.endpoint.send(LaunchDriver(driver.id, driver.desc,
//                                               driver.resources))
//
//   PAIR TWO HANDLER - core/src/main/scala/org/apache/spark/deploy/rest/
//                      StandaloneRestServer.scala (294 lines at the pin)
//     :56   private[deploy] class StandaloneRestServer(
//     :60       masterEndpoint: RpcEndpointRef,
//     :64-75    the six servlets the server mounts
//     :171  private[rest] class StandaloneSubmitRequestServlet(   <-- the type
//     :268    protected override def handleSubmit(                <-- the handler
//     :276-277  masterEndpoint.askSync[DeployMessages.SubmitDriverResponse](
//                 DeployMessages.RequestSubmitDriver(driverDescription))
//
//     FOR THE REPORT, measured in the pinned tree rather than assumed: this
//     file contains NO authorization, ACL, security-manager or credential
//     construct at all. Its only `permission` occurrence is the Apache licence
//     boilerplate at :14, and a case-insensitive search additionally returns
//     :209, :233 and :251, which are false positives - the matched literal is
//     `aCl` inside extr[aCl]assPath / driverExtr[aCl]assPath. That is a
//     statement about which selectors match this file, and nothing more.
//
//   SHARED SINK - core/src/main/scala/org/apache/spark/deploy/worker/
//                 DriverRunner.scala (279 lines at the pin)
//     :47   private[deploy] class DriverRunner(
//     :56       val securityManager: SecurityManager,
//     :88   private[worker] def start() = {
//     :89     new Thread("DriverRunner for " + driverId) {
//     :90       override def run(): Unit = {
//     :99         val exitCode = prepareAndRunDriver()
//     :123    }.start()
//     :178  private[worker] def prepareAndRunDriver(): Int = {
//     :193/:194 CommandUtils.buildProcessBuilder(...), securityManager passed
//                 through on :194
//     :204  runDriver(builder, driverDir, driverDesc.supervise)   (def :207)
//     :221  runCommandWithRetry(ProcessBuilderLike(builder), ...) (def :224)
//     :233  val redactedCommand = Utils.redactCommandLineArgs(conf, ...)
//     :240      process = Some(command.start())          <-- the privileged
//                                                            launch, the sink
//     :269  private[deploy] trait ProcessBuilderLike {
//     :270    def start(): Process                        <-- ABSTRACT
//     :275  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike =
//             new ProcessBuilderLike {
//     :276      override def start(): Process = processBuilder.start()  <-- CONCRETE
//
//   RELAY (on both pairs' routes) - core/src/main/scala/org/apache/spark/
//                                   deploy/worker/Worker.scala (1,046 lines)
//     :523  override def receive: PartialFunction[Any, Unit] = synchronized {
//     :687    case LaunchDriver(driverId, driverDesc, resources_) =>
//     :689      val driver = new DriverRunner(
//     :701      driver.start()
//     :736  override def receiveAndReply(context: RpcCallContext) ...
//
//   MESSAGES - core/src/main/scala/org/apache/spark/deploy/DeployMessage.scala
//              (303 lines at the pin)
//     :34   private[deploy] object DeployMessages {   <-- note the PLURAL object
//             name: the file is DeployMessage.scala, the object is
//             DeployMessages, so the bytecode types this query selects on are
//             org.apache.spark.deploy.DeployMessages$LaunchDriver and
//             org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.
//     :176    case class LaunchDriver(
//     :223    case class RequestSubmitDriver(driverDescription: DriverDescription)
//     :225    case class SubmitDriverResponse(
//
//   PREDICATES - core/src/main/scala/org/apache/spark/SecurityManager.scala
//                (457 lines at the pin) - the five Boolean predicates that
//                define "spurious" below
//     :227  def aclsEnabled(): Boolean = aclsOn
//     :234  def checkAdminPermissions(user: String): Boolean = {
//     :248  def checkUIViewPermissions(user: String): Boolean = {
//     :264  def checkModifyPermissions(user: String): Boolean = {
//     :274  def isAuthenticationEnabled(): Boolean = authOn
//     :59   private var aclsOn = sparkConf.get(ACLS_ENABLE)   <-- see the
//             bytecode-accessor collision handled in STAGE F
//
//   THE HANDLER SURFACE IS AMPLE FOR PARAMETERIZATION, and that is measured in
//   the pinned tree rather than argued: EIGHT receive/receiveAndReply
//   declarations across FIVE files under core/src/main/scala/org/apache/spark/
//   deploy - Client.scala :207; client/StandaloneAppClient.scala :161 and :209;
//   master/Master.scala :239 and :409; worker/Worker.scala :523 and :736;
//   worker/WorkerWatcher.scala :66. The two pairs below are two instantiations
//   of a form that has at least eight handler candidates and one shared sink to
//   draw on, which is the evidence that the parameterization generalises rather
//   than fitting exactly the two cases it was written against.
//
// THE BOUNDARIES - AND WHY A ZERO IS THE FINDING, TWICE OVER
//   NEITHER pair is call-graph-connected end to end, and the two do not break
//   in the same places. A bounded reachability walk therefore returns zero
//   routes for both, and that zero is the capability finding - it is not a
//   broken query, and it is not repaired by loosening or removing the bound.
//   Every boundary is MEASURED against the graph and reported per pair with its
//   hop and its reason. Where a hop is on the part of the route the two pairs
//   SHARE, it is measured ONCE and cited by both pairs, never measured twice.
//
//     PAIR ONE, four boundaries
//       B-rpc-LaunchDriver   Master.launchDriver :1367 sends LaunchDriver over
//                            an RpcEndpointRef; the receiving handler is in
//                            Worker. No call edge crosses a message send.
//                            Modelled explicitly by pairing on the MESSAGE TYPE.
//       B-thread             DriverRunner.start :123 calls Thread.start(); the
//                            body that continues the route is run() :90 on the
//                            anonymous Thread subclass. start() -> run() is a
//                            JVM scheduling relation, not a call edge.
//       B-interface          runCommandWithRetry :240 invokes the ABSTRACT
//                            ProcessBuilderLike.start :270; the JDK launch is
//                            reached only through the anonymous implementation
//                            at :276.
//       B-partial-function   receiveAndReply returns PartialFunction[Any, Unit],
//                            so its body compiles into a synthetic class and the
//                            graph entry point is
//                            Master$$anonfun$receiveAndReply$1.applyOrElse, NOT
//                            a method named receiveAndReply.
//
//     PAIR TWO, five boundaries - the four above plus, FIRST,
//       B-rpc-RequestSubmitDriver
//                            handleSubmit :268 does not CALL the Master: it
//                            SENDS RequestSubmitDriver by askSync at :276-277,
//                            and that is the very message pair one's handler
//                            receives at Master.scala :410. A call graph does
//                            not join a send to its receiving handler. This
//                            query MODELS the hop explicitly by pairing on the
//                            message type, and stage H reports the graph
//                            evidence for the model: the constructor call site
//                            inside handleSubmit is the producer end, the
//                            accessor call site inside the Master handler's
//                            synthetic method is the consumer end, and whether
//                            a CALL edge joins them is measured rather than
//                            asserted. Pair two therefore crosses FIVE
//                            boundaries where pair one crosses four.
//
//     The partial-function boundary is measured PER PAIR because the two pairs
//     answer it differently, and the difference is the parameterization detail
//     worth reporting in its own right: pair one's handler returns a partial
//     function and its body lives in a synthetic class, while pair two's
//     handler is an ordinary method whose body is its own. The selector
//     therefore resolves a handler by taking the UNION of the synthetic arm and
//     the source-level arm and reporting which of the two carried the route -
//     for pair one the synthetic arm does, for pair two the synthetic arm
//     matches nothing at all and the source-level arm does. A selector that
//     took only one arm would silently miss one of the two pairs.
//
// OUTPUTS (slugs are locked; harness/artifacts/logs/probe-03-parameterized-
//          handler-sink-pairs.log names exactly these two as consumers)
//   queries/joern/results/03-parameterized-handler-sink-pairs.json  envelope
//   queries/joern/results/03-parameterized-handler-sink-pairs.md    prose
//   harness/artifacts/logs/probe-03-parameterized-handler-sink-pairs.log
//                                                                   console
//
//   Both result files are DETERMINISTIC: no timestamp, no elapsed time and no
//   workspace or project name enters them, and the pairs are iterated in the
//   declared order of the PAIRS list, so an unchanged source over an unchanged
//   graph emits byte-identical bytes. Elapsed times live in the console log
//   only.
//
//   NOTHING IN THE OUTPUT IS SUMMED ACROSS PAIRS. Routes, spurious counts,
//   bound flags and entry-point counters are reported per pair and are keyed by
//   pair id; no field anywhere adds pair one's routes to pair two's, and no
//   field adds this query's returns to query 01's or 02's.
//
// MARKER PROTOCOL (restated from query 01 rather than reinvented)
//   ---BLITZY-START---            printed first, before any work
//   ---BLITZY-RESULT-BEGIN---     opens the result region, only ever printed
//   ---BLITZY-RESULT-END---       after every stage has succeeded
//   ---BLITZY-OK---               the run completed
//   ---BLITZY-FAILURE---          printed instead; the failing stage and the
//                                 exception go to stderr, the exception is
//                                 re-raised (joern exits 1) and NO result
//                                 region is emitted. A partial result region is
//                                 worse than none: it looks like a completed run.
// ===========================================================================

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, LinkOption, Path, Paths}
import java.security.MessageDigest

import io.shiftleft.codepropertygraph.generated.nodes.{Call, Method}
import io.shiftleft.semanticcpg.language.NoResolve

// ===========================================================================
// NAMED CONSTANTS. No inline literal selects a node, an edge, a type, a
// method, a call site, a handler, a sink or a route anywhere below: every
// selector the walks, the boundary measurements, the surface sweep and the
// predicate search use is a named constant declared in this block, so what the
// query looks for can be read off in one place and cited in the result without
// reading the traversal.
//
// Three kinds of literal do appear below, and none of them selects anything in
// the graph. First, the lexical tokens of the two text readers - the Scala
// string-literal scanner that extracts a sibling query's declared constants,
// and the identity-record reader - where the literal describes the syntax of
// the file being read. Second, the boundary and walk identifiers a record is
// stamped with, which name a published field rather than choose a node; each is
// a named constant or is composed from named constants. Third, the field names
// and prose of the envelope and the prose report, which are output rather than
// selection. The one regex the surface sweep builds is composed from a named
// constant and the quoting suffix named beside it, never from a bare pattern.
// ===========================================================================

/** The slug. Both result filenames and the console log name derive from it. */
val QUERY_ID = "03-parameterized-handler-sink-pairs"

/** The probe's own scratch workspace, repo-relative as the AAP names it. */
val WORKSPACE_PATH = "queries/joern/.workspace"

/** This run's own workspace, created fresh under the AAP-named root above and
 *  never reused: the prefix and the query id make it readable, the random suffix
 *  makes the name unique so `createDirectory` fails rather than inheriting a
 *  previous run's project state, and the lock file inside it is what stops two
 *  Joern processes in one clone from writing one workspace. */
val WORKSPACE_RUN_DIR_PREFIX = "run-"
val WORKSPACE_RUN_RANDOM_BYTES = 12
val WORKSPACE_LOCK_FILENAME = ".lock"

/** Repo-relative output paths. Resolved against the repository root below. */
val RESULTS_DIR = "queries/joern/results"
val LOG_DIR = "harness/artifacts/logs"

/**
 * The graph, and the records that can fix its identity.
 *
 * The record of account is resolved by PROVENANCE - who wrote the bytes - and
 * never by which candidate happens to match, in the same fixed order
 * `harness/lib/preflight_graph_identity.py` uses for the Stage 3 gate, so the
 * probe and that gate adjudicate a load against one record under one
 * convention:
 *
 *   1. `CPG_FRONTEND_RECORD_PATH`, the in-checkout frontend log, WHEN it
 *      carries exactly one strict `bytes:`/`sha256:` pair. Such a pair exists
 *      only if this checkout's frontend wrote a graph, so where it exists it
 *      governs. This checkout's own frontend invocation terminated in
 *      serialization and produced no accepted graph (run-record.md divergence
 *      D1), so that log records a REJECTED partial and carries no such pair -
 *      which is a fact about the record rather than a failure, and is exactly
 *      why the order below has a second entry.
 *   2. Otherwise the provisioning record of account beside the RESOLVED graph:
 *      `<the graph's directory>/../<CPG_PROVISION_RECORD_DIR>/`, holding the
 *      two named records. The directory is DERIVED from the graph this load
 *      will actually open rather than hardcoded, so a clone whose
 *      `$HARNESS_CPG` points elsewhere is adjudicated against that graph's own
 *      record instead of against this one's.
 *
 * Every candidate that exists is read. More than one distinct pair inside one
 * record, or two records that disagree, halts the run: ambiguity is refused
 * rather than resolved, because a check that accepts whichever candidate
 * matches is not a check. There is deliberately no environment override for the
 * record: an override would let a load be adjudicated by a record that no
 * environment contract defines and that the published reproduction command does
 * not name, so a reader of the result could not tell which record the identity
 * comparison turned on. All three probe queries resolve the record this way.
 */
val CPG_ENV_VAR = "HARNESS_CPG"
val CPG_PATH_DEFAULT = "harness/cpg/spark.cpg"
val CPG_FRONTEND_RECORD_PATH = "harness/artifacts/logs/cpg-frontend.log"
val CPG_PROVISION_RECORD_DIR = "provision-log"
val CPG_PROVISION_RECORD_NAMES = List("cpg-identity.txt", "cpg-record.txt")

/**
 * Where the private, immutable copy of the graph is made, and how it is named.
 *
 * The identity comparison is worthless unless the bytes it measured are the
 * bytes `importCpg` reads. The graph itself is a host-shared read-only file
 * reached through a symlink, so measuring it and then handing its path to
 * `importCpg` leaves a window in which the path could resolve elsewhere. This
 * query therefore copies it ONCE into a directory it creates itself, digests
 * it IN THE SAME PASS as the copy, verifies that digest against the record of
 * account, imports only the copy, and re-measures the copy's digest and inode
 * after the load. Both pairs are then measured against one set of bytes whose
 * identity was established once.
 */
val CPG_PRIVATE_INPUT_DIR_PREFIX = "probe-graph-input-"
val CPG_PRIVATE_INPUT_FILENAME = "spark.cpg"
val CPG_PRIVATE_INPUT_RANDOM_BYTES = 12
val CPG_COPY_CHUNK_BYTES = 8388608

/** The repository root, and the environment variable that names it. */
val REPO_ROOT_ENV_VAR = "HARNESS_REPO_ROOT"

/** The clone-private scratch root `harness/env.sh` exports. The private graph
 *  copy is made under it because it is per-clone by construction
 *  (`/tmp/blitzy-harness-scratch/<clone index>`) and outside the checkout, so a
 *  half-gigabyte copy never enters a git-collected tree. Where the variable is
 *  unset the query falls back to the system temporary directory and says so. */
val SCRATCH_ROOT_ENV_VAR = "HARNESS_SCRATCH_DIR"

/** The sibling probe queries this one reports a duplicate-formulation verdict against. */
val SIBLING_CALLGRAPH_QUERY = "01-callgraph-unguarded-driver-launch"
val SIBLING_DATAFLOW_QUERY = "02-dataflow-unguarded-driver-launch"
val SIBLING_QUERY_IDS = List(SIBLING_CALLGRAPH_QUERY, SIBLING_DATAFLOW_QUERY)

/** Where a query source lives, repo-relative. This query reads its OWN source
 *  from here to digest it, and its siblings' to compare formulations. */
val QUERY_SOURCE_DIR = "queries/joern"

// ------------------------------------------------------ reproducing this run
/**
 * The COMPLETE command this query is reproduced by - runnable as written, with
 * every element it genuinely needs and nothing it does not. Each element earns
 * its place:
 *
 *   - the working directory is `$HARNESS_SCRATCH_DIR`, outside the repository,
 *     because joern eagerly creates ./workspace in its own working directory
 *     and exposes no flag to move it, and nothing named workspace is ignored by
 *     the repository's root .gitignore. It is per-clone by construction, which
 *     is what keeps two clones' Joern processes from corrupting one workspace;
 *   - HARNESS_REPO_ROOT is REQUIRED by that choice: with the working directory
 *     outside the repository, it is the only thing that tells the query where
 *     the graph, the identity record, the results directory and the log
 *     directory are;
 *   - HARNESS_CPG is named EXPLICITLY, because it selects the graph bytes the
 *     query loads - the one set of bytes BOTH pairs are measured against.
 *     Omitting it published a command whose most consequential input was
 *     invisible: a reader who had it set to another graph would reproduce a
 *     different load and read this envelope as describing it;
 *   - JAVA_HOME selects the JDK major the pinned Joern release documents;
 *   - JAVA_TOOL_OPTIONS is what actually raises the heap, because joern
 *     --script forks a child JVM and does not forward -J-Xmx to it;
 *   - stdin is closed because joern's REPL blocks on an open one.
 *
 * The three `HARNESS_*` values are written as variable references rather than
 * as literal paths for one reason: an absolute path is a property of a checkout
 * rather than of the measurement, and the envelope and the report are held to
 * byte-identity across checkouts. Sourcing `harness/env.sh` in the checkout -
 * with BLITZY_CLONE_INDEX set, as the clone's own instructions require - exports
 * all three, which is why the precondition is published beside the command.
 *
 * No other environment variable changes what this query loads or what it
 * publishes. In particular there is still no override for the identity record:
 * the record of account is resolved by provenance from the in-checkout frontend
 * log and the provisioning record beside the resolved graph, both reached from
 * values this command names, so a load can never be adjudicated by a record
 * this command does not reach.
 */
val REPRODUCTION_COMMAND_PRECONDITION =
  "run from a checkout of this branch after `BLITZY_CLONE_INDEX=<this clone's index> ; " +
    ". harness/env.sh`, which exports $" + REPO_ROOT_ENV_VAR + ", $" + CPG_ENV_VAR +
    " and $" + SCRATCH_ROOT_ENV_VAR + " - the three values the command below reads"
val REPRODUCTION_COMMAND =
  "cd \"$" + SCRATCH_ROOT_ENV_VAR + "\" && " +
    REPO_ROOT_ENV_VAR + "=\"$" + REPO_ROOT_ENV_VAR + "\" " +
    CPG_ENV_VAR + "=\"$" + CPG_ENV_VAR + "\" " +
    "JAVA_HOME=\"$JAVA_HOME_21\" " +
    "JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script " +
    "\"$" + REPO_ROOT_ENV_VAR + "/" + QUERY_SOURCE_DIR + "/" + QUERY_ID +
    ".sc\" -J-Xmx64g < /dev/null"

// ------------------------------------------------ JVM argument reporting
/**
 * The heap this query runs at is evidence and is logged. The JVM's argument
 * list is not evidence of anything this query reports, and it is a disclosure
 * channel: a -D property carries whatever the invoker put in it, and these
 * console streams are preserved verbatim as evidence, so a token or a password
 * passed on a command line would be published along with them.
 *
 * So the policy is a whitelist, not a filter: only an argument whose key is one
 * of the memory and stack flags below is logged as written, and every other
 * argument is reduced to its key with the value replaced by a fixed token. The
 * number reduced is reported, so the reduction is visible rather than silent.
 */
val JVM_ARG_VALUE_WHITELIST_PREFIXES = List(
  "-Xms",
  "-Xmx",
  "-Xmn",
  "-Xss",
  "-XX:MaxMetaspaceSize",
  "-XX:MetaspaceSize",
  "-XX:MaxDirectMemorySize",
  "-XX:MaxRAMPercentage",
  "-XX:ThreadStackSize")
val JVM_ARG_REDACTION_TOKEN = "<redacted>"
val JVM_ARG_KEY_VALUE_SEPARATORS = List("=", ":")
val JVM_ARG_REDACTION_POLICY =
  "a whitelist: an argument whose key is one of the memory or stack flags the query " +
    "names is logged as written, because the heap it establishes is the evidence. Every " +
    "other argument is reduced to its key and its value replaced by " +
    JVM_ARG_REDACTION_TOKEN + ", and an argument carrying no key/value separator is a " +
    "key with no value and is logged as the key it is. No value this query has not " +
    "whitelisted reaches any log, status field or published record, and the count of " +
    "reduced arguments is reported so the reduction cannot pass unnoticed"

// --------------------------------------------------------------- the bounds
/** Maximum call-graph hops walked from an entry point, per pair. */
val MAX_CALL_DEPTH = 12
/** Maximum distinct routes retained PER PAIR. Never a shared budget: one pair
 *  filling a shared budget would silently truncate the other. */
val MAX_ROUTES_PER_PAIR = 64
/**
 * Per-entry-point step cap: method expansions, not edges. Enforced at the scope
 * its name states - the counter is reset at EACH entry point, so one entry point
 * cannot spend another's allowance - and the peak reached at any single entry
 * point is reported per walk beside the walk's own total.
 */
val MAX_EXPANSIONS_PER_ENTRY = 200000
/**
 * Per-pair step cap across all of that pair's walks: call sites considered.
 * Enforced at the scope its name states - ONE counter per pair, shared by both
 * of that pair's walks - so the published figure is the pair's spend rather than
 * either walk's. It is never shared BETWEEN pairs: one pair filling a shared
 * budget would silently truncate the other.
 */
val MAX_STEPS_PER_PAIR = 400000
/** Total-returns cap across every record kind this query emits. */
val MAX_TOTAL_RETURNS = 256
/** Maximum entry points traversed PER PAIR; the remainder are counted as truncated. */
val MAX_ENTRY_POINTS_PER_PAIR = 16
/** Cap on the indexed call-name sweeps: the shared sink sweep, the predicate
 *  call-site sweep and the two message call-site sweeps. Every sweep it governs
 *  publishes its own observed count and truncation flag, and the cap's reported
 *  reached flag is the disjunction over all of them. */
val MAX_CALL_SCAN = 200000
/**
 * Cap on the indexed type-declaration sweep that measures each route surface
 * prefix, and on the keyed type and method lookups - each pair's entry-point
 * selection, the predicate type and its methods, the message types, the thread
 * hosts and the JDK-launch declaration. A prefix sweep with no cap would be an
 * unbounded traversal in the query, so it carries a named bound like every other
 * traversal here; the keyed lookups return a handful of nodes at this pin and
 * register under the same cap so that "no traversal in this query materializes
 * without a cap" is literally true rather than true of the big ones. Every sweep
 * it governs publishes its own observed count and truncation flag, and the cap's
 * reported reached flag is the disjunction over all of them.
 */
val MAX_TYPE_SCAN = 200000
/**
 * The suffix appended to a regex-QUOTED surface prefix to turn it into a prefix
 * match for the type-declaration sweep. Named because it governs behaviour: it
 * is what makes the sweep match a prefix rather than an exact full name, and a
 * reader checking what the sweep selects should find it here rather than inside
 * the traversal. The prefix itself is quoted first, so a dot in a package name
 * cannot widen the match.
 */
val TYPE_PREFIX_REGEX_SUFFIX = ".*"
/**
 * A call site whose resolved callee set is wider than this is recorded as a
 * dynamic-dispatch FAN-OUT site. Expanding, say, scala.Function1.apply models
 * "any lambda in the program may be invoked here", which is a property of the
 * call linker rather than of either route. Walk A follows those sites anyway;
 * walk B records them and does not. Both walks are reported per pair and their
 * routes are deduplicated within the pair, never summed.
 */
val FANOUT_CALLEE_THRESHOLD = 32

// ------------------------------------------------------------- the heap floor
/** 64 GiB. Measured, not requested: the query halts below this. */
val HEAP_FLOOR_BYTES = 64L * 1024L * 1024L * 1024L
/** The JDK major the pinned Joern release documents as its tested requirement. */
val REQUIRED_JDK_MAJOR = "21"

// =========================================================== the parameters
// The handler/sink pair is a PARAMETER of this query, not a hard-coded target.
// Everything a pair needs is named here, one constant per selector, so that the
// pair literals in stage E carry no inline string of their own and a third pair
// would be added by adding constants rather than by editing a traversal.
// ---------------------------------------------------------------------------

/** The sink, shared by both pairs. Expressed as a callee full name, an indexed
 *  call name, and a host-type constraint so that no unrelated `start` anywhere
 *  in a 1.4 M-method graph can stand in for the privileged launch. */
val SINK_CALLEE_REGEX =
  """^(java\.lang\.ProcessBuilder\.start|org\.apache\.spark\.deploy\.worker\.ProcessBuilderLike\.start).*"""
val SINK_CALL_NAME = "start"
val SINK_HOST_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.worker\.(DriverRunner|ProcessBuilderLike).*"""
val SINK_SOURCE_FILE = "core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala"
val SINK_SOURCE_LINE = 240
/** The abstract declaration the launch call site names, and the JDK method the
 *  concrete anonymous implementation reaches. Used by the interface boundary. */
val ABSTRACT_LAUNCH_CALLEE_PREFIX = "org.apache.spark.deploy.worker.ProcessBuilderLike.start"
val JDK_LAUNCH_METHOD_FULL_NAME = "java.lang.ProcessBuilder.start:java.lang.Process()"

/** The thread hop, on the shared part of both routes. */
val THREAD_HOST_TYPE = "org.apache.spark.deploy.worker.DriverRunner"
val THREAD_HOST_METHOD = "start"
val THREAD_BODY_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.worker\.DriverRunner\$\$anon\$\d+$"""
val THREAD_BODY_METHOD = "run"

/** Pair one: the standalone Master's driver-submission handler. */
val PAIR_ONE_ID = "pair-one"
val PAIR_ONE_LABEL =
  "the standalone Master's driver-submission handler to the privileged process " +
    "launch on the DriverRunner surface"
val PAIR_ONE_HANDLER_TYPE = "org.apache.spark.deploy.master.Master"
val PAIR_ONE_HANDLER_METHOD = "receiveAndReply"
val PAIR_ONE_HANDLER_SYNTHETIC_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$"""
val PAIR_ONE_HANDLER_SOURCE_FILE =
  "core/src/main/scala/org/apache/spark/deploy/master/Master.scala"
val PAIR_ONE_HANDLER_SOURCE_LINE = 409
/** A callee substring that, if present among a candidate entry point's own call
 *  sites, shows that candidate carries the handler BODY rather than merely
 *  constructing a partial function. Measured, never assumed. */
val PAIR_ONE_HANDLER_BODY_WITNESS = "createDriver"

/** Pair two: the REST submit servlet's handler. */
val PAIR_TWO_ID = "pair-two"
val PAIR_TWO_LABEL =
  "the REST submit servlet's handleSubmit to the SAME privileged process launch " +
    "on the DriverRunner surface"
val PAIR_TWO_HANDLER_TYPE = "org.apache.spark.deploy.rest.StandaloneSubmitRequestServlet"
/** The name the plan uses for the same handler, after the file it lives in. Both
 *  are reported so the resolution is visible rather than looking like a slip. */
val PAIR_TWO_HANDLER_PLAN_NAME = "StandaloneRestServer handleSubmit"
val PAIR_TWO_HANDLER_METHOD = "handleSubmit"
/**
 * The synthetic arm is asked for pair two too, and it is EXPECTED to match
 * nothing: handleSubmit is an ordinary method, not a partial function, so no
 * `$$anonfun$handleSubmit$N` class exists. The selector still asks, because
 * "the synthetic arm is empty" is a measured fact about this handler rather
 * than an assumption, and because a parameterized selector that only worked for
 * partial-function handlers would not generalise past pair one.
 */
val PAIR_TWO_HANDLER_SYNTHETIC_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.rest\.StandaloneSubmitRequestServlet\$\$anonfun\$handleSubmit\$\d+$"""
val PAIR_TWO_HANDLER_SOURCE_FILE =
  "core/src/main/scala/org/apache/spark/deploy/rest/StandaloneRestServer.scala"
val PAIR_TWO_HANDLER_SOURCE_LINE = 268
val PAIR_TWO_HANDLER_BODY_WITNESS = "DeployMessages$RequestSubmitDriver.<init>"
/** The abstract declaration the override satisfies. Present in the graph, and
 *  excluded by the pair's exact type selector - reported, not silently dropped. */
val PAIR_TWO_HANDLER_BASE_TYPE = "org.apache.spark.deploy.rest.SubmitRequestServlet"

/** The synthetic method name a partial-function handler's body compiles into. */
val HANDLER_SYNTHETIC_METHOD = "applyOrElse"

/** The two RPC message hops, by the message type each pairs on. */
val MESSAGE_HOP_LAUNCH_DRIVER_ID = "LaunchDriver"
val MESSAGE_TYPE_LAUNCH_DRIVER = "org.apache.spark.deploy.DeployMessages$LaunchDriver"
val MESSAGE_ACCESSORS_LAUNCH_DRIVER = List("driverDesc", "driverId", "resources")
val MESSAGE_HOP_LAUNCH_DRIVER_DIRECTION = "Master to Worker"
val MESSAGE_HOP_LAUNCH_DRIVER_SOURCE = PAIR_ONE_HANDLER_SOURCE_FILE + ":1367"

val MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_ID = "RequestSubmitDriver"
val MESSAGE_TYPE_REQUEST_SUBMIT_DRIVER =
  "org.apache.spark.deploy.DeployMessages$RequestSubmitDriver"
val MESSAGE_ACCESSORS_REQUEST_SUBMIT_DRIVER = List("driverDescription")
val MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_DIRECTION = "the REST submit servlet to Master"
val MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_SOURCE = PAIR_TWO_HANDLER_SOURCE_FILE + ":276-277"
/** The constructor name a case-class message send resolves to in bytecode. */
val MESSAGE_CTOR_NAME = "<init>"

/**
 * The two hosts the sink sits on, and the base of each pair's OWN route surface.
 *
 * Each pair's surface is derived from that pair's own handler, relay and sink
 * types, and the query-wide surface declared further down is derived from the
 * union of every route end BOTH pairs measure. Both are reported, per pair and
 * shared, because a pair's structural basis is a statement about that pair's
 * route while the shared surface is what makes the two pairs' bases
 * commensurable. Neither is a set that merely looks plausible: stage I asserts
 * that every route end this query measured is covered.
 */
val SINK_SURFACE_TYPE_PREFIXES = List(
  "org.apache.spark.deploy.worker.DriverRunner",
  "org.apache.spark.deploy.worker.ProcessBuilderLike")


// ---------------------------------------------------------- the predicate surface
/**
 * The mechanical definition of "spurious": a route is spurious ONLY where the
 * handler does pass an authorization or ACL predicate before reaching the sink.
 * The selectors that constitute those predicates are named here and repeated in
 * the report. This judges THE QUERY, not Spark.
 *
 * The four constants below, and nothing else, are the predicate block: they are
 * intended to be the same text in all three queries of the probe, because the
 * three spurious counts are only comparable while the definition of "spurious"
 * is the same text. That sameness is not asserted here - it is MEASURED, as the
 * block-end comment below sets out. The route surface that used to sit inside
 * this block is a property of one query's own route rather than of the shared
 * definition, so it is declared separately below.
 */
val PREDICATE_TYPE = "org.apache.spark.SecurityManager"
val PREDICATE_NAME_REGEX = """^(check.*Permissions|acls.*|isAuthenticationEnabled)$"""
/** Scala compiles `private var aclsOn` into accessors, so the broad selector also
 *  matches the bytecode setter. Excluded by suffix, then constrained to the five. */
val PREDICATE_SETTER_SUFFIX = "_$eq"
val PREDICATE_NAMED_FIVE = List(
  "aclsEnabled",
  "checkAdminPermissions",
  "checkModifyPermissions",
  "checkUIViewPermissions",
  "isAuthenticationEnabled")
// -------------------------------------------------- end of the predicate surface
// The block above, from the `the predicate surface` banner to this comment, is
// intended to be the same text as the corresponding block of
// queries/joern/01-callgraph-unguarded-driver-launch.sc and of
// queries/joern/02-dataflow-unguarded-driver-launch.sc, and that is not asserted
// here: stage J MEASURES it. The four predicate constants are named in the
// formulation identity block below, the comparison reads each sibling source's
// own literals by those names, and the outcome is published per sibling as
// predicate_selector_literals_identical. The reason the sameness matters is that
// the three queries' spurious counts are only comparable while the definition of
// "spurious" is the same text; a divergence is therefore a measured, published
// fact rather than a claim a reader has to take on trust. The block holds the
// four predicate constants and nothing else. The route surface is NOT part of
// it: a surface is a property of one query's own route, and this query's route
// spans two pairs with two different entry-point owners, so it is DERIVED below
// from this query's own route ends and asserted in stage I to cover every route
// end this query measured.

/**
 * The INTERMEDIATE route hop, declared here rather than inside the block above
 * because that block is carried verbatim across all three queries and is not
 * edited to add anything.
 *
 * Both pairs' routes run handler -> RPC -> Worker -> DriverRunner -> launch: the
 * Worker is the component that receives the launch message and constructs and
 * starts the runner that hosts the sink. A route surface that named only the
 * handler and the sink host would therefore leave one hop of the route
 * unsearched, and a "no predicate on the route" statement drawn from it would be
 * a statement about part of the route while reading as one about all of it.
 * Including the Worker makes the predicate evidence cover the whole route
 * surface. Whether that changes any count is MEASURED below and published per
 * surface prefix, never assumed either way.
 */
val ROUTE_HOP_SURFACE_TYPE_PREFIXES = List(
  "org.apache.spark.deploy.worker.Worker")
/** The intermediate hop's source file, at the pinned commit. */
val ROUTE_HOP_WORKER_SOURCE_FILE =
  "core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala"
/** Its length at the pin, so a reader can see which revision the anchors index. */
val ROUTE_HOP_WORKER_SOURCE_FILE_LINES = 1046
/**
 * The hop's own anchors at the pin, ascending by line so the order is a fixed
 * function of the content. These are SOURCE facts, checked line by line in the
 * pinned clone; the graph measurements that stand on them are taken separately
 * and are reported as measurements.
 */
val ROUTE_HOP_WORKER_ANCHORS = List(
  ROUTE_HOP_WORKER_SOURCE_FILE + ":523 override def receive declares the handler that " +
    "receives the launch message",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":687 case LaunchDriver(driverId, driverDesc, " +
    "resources_) matches it",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":689 constructs the DriverRunner that hosts the sink",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":701 calls driver.start(), the hop into the sink host",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":736 override def receiveAndReply, the second handler " +
    "on the same type and not on this route")
/**
 * Every reference to the anchored predicate type in the intermediate hop's file
 * at the pin. None of them invokes any of the five named selectors, which is the
 * source-level counterpart of the graph measurement, and they are listed for the
 * same reason the other route files' references are: a held reference is not an
 * invocation, and the distinction has to be visible rather than implied.
 */
val ROUTE_HOP_WORKER_PREDICATE_REFERENCES = List(
  ROUTE_HOP_WORKER_SOURCE_FILE + ":33 imports SecurityManager",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":61 declares val securityMgr: SecurityManager",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":195 passes securityMgr on as an argument",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":698 passes securityMgr on as an argument to the " +
    "DriverRunner constructed at :689",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":1018 constructs a SecurityManager",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":1019 passes it on as an argument",
  ROUTE_HOP_WORKER_SOURCE_FILE + ":1022 passes it on as an argument")

// -------------------------------------------------- the route surface, DERIVED
/**
 * The types whose methods carry THIS query's route, used to establish the
 * structural basis for the expected-spurious absence. Synthetic `$$anonfun$`
 * and `$$anon$` classes of these types are included by prefix.
 *
 * DERIVED from this query's own route ends across BOTH pairs rather than
 * declared as a set that merely looks plausible, and every entry names the role
 * it plays:
 *
 *   - `PAIR_ONE_HANDLER_TYPE` owns pair one's entry points;
 *   - `PAIR_TWO_HANDLER_TYPE` owns pair two's entry point - the class
 *     `handleSubmit` is actually declared in. The previous shared list named
 *     `StandaloneRestServer` instead, which is not a prefix of the servlet's
 *     full name, so pair two's own handler was not covered by the surface its
 *     structural basis was drawn from;
 *   - `PAIR_TWO_ROUTE_ENTRY_HOST_TYPE` is the REST server that constructs and
 *     mounts that servlet: the component a remote submit request reaches before
 *     the handler, and therefore part of pair two's route surface even though no
 *     route end MEASURED here is owned by it. It is retained and its reach is
 *     published per prefix, so "searched and nothing found" is visible rather
 *     than an omission;
 *   - `MESSAGE_RELAY_HOST_TYPE` owns the consumer end of each pair's RPC hop -
 *     the Worker that receives the launch message and creates the runner - so it
 *     is the relay both routes pass through;
 *   - `SINK_HOST_RUNNER_TYPE` and `SINK_HOST_LAUNCHER_TYPE` are the two hosts
 *     `SINK_HOST_TYPE_REGEX` admits, and are where the privileged launch call
 *     sites actually sit. Both were absent from the previous list, so the two
 *     types holding the sink were not on the surface searched for a predicate.
 *
 * This is deliberately NOT part of the byte-identical predicate block above.
 * The predicate constants define the term "spurious" and are compared across the
 * three sources under the names the formulation identity block declares, so they
 * must be the same text in all three. A route surface is a property of one
 * query's own route, and this query's route has two entry-point owners where
 * queries 01 and 02 have one. Stage I asserts that every route end this query
 * MEASURED, in either pair, is covered by the surface below, and publishes
 * per-prefix reach evidence, so the derivation is checked rather than claimed.
 *
 * The relay and the two sink hosts are taken from the lists that already declare
 * them rather than restated as literals, so the surface cannot drift from the
 * selectors it is derived from.
 */
val PAIR_TWO_ROUTE_ENTRY_HOST_TYPE = "org.apache.spark.deploy.rest.StandaloneRestServer"
val MESSAGE_RELAY_HOST_TYPE = ROUTE_HOP_SURFACE_TYPE_PREFIXES.head
val SINK_HOST_RUNNER_TYPE = SINK_SURFACE_TYPE_PREFIXES.head
val SINK_HOST_LAUNCHER_TYPE = SINK_SURFACE_TYPE_PREFIXES.last
val ROUTE_SURFACE_TYPE_PREFIXES = List(
  PAIR_ONE_HANDLER_TYPE,
  PAIR_TWO_HANDLER_TYPE,
  PAIR_TWO_ROUTE_ENTRY_HOST_TYPE,
  MESSAGE_RELAY_HOST_TYPE,
  SINK_HOST_RUNNER_TYPE,
  SINK_HOST_LAUNCHER_TYPE).distinct
/** The role each prefix plays on this route, published beside the reach
 *  evidence so a reader sees why each one is in the surface. */
val ROUTE_SURFACE_TYPE_ROLES = List(
  PAIR_ONE_HANDLER_TYPE ->
    ("entry-point owner: the type pair one's walks start from, and also the consumer " +
      "end of pair two's " + MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_ID + " RPC hop"),
  PAIR_TWO_HANDLER_TYPE ->
    "entry-point owner: the type pair two's handler method is declared in",
  PAIR_TWO_ROUTE_ENTRY_HOST_TYPE ->
    ("entry host: the REST server that constructs and mounts pair two's handler " +
      "servlet, reached by a remote submit request before the handler is"),
  MESSAGE_RELAY_HOST_TYPE ->
    ("relay: the consumer end of the " + MESSAGE_HOP_LAUNCH_DRIVER_ID + " RPC hop, " +
      "measured once as boundary B-rpc-" + MESSAGE_HOP_LAUNCH_DRIVER_ID +
      " and cited by both pairs"),
  SINK_HOST_RUNNER_TYPE -> "sink host: the runner that holds the launch call site",
  SINK_HOST_LAUNCHER_TYPE ->
    "sink host: the launcher surface the privileged start is declared on")

// ------------------------------------------- the formulation identity block
/**
 * What makes this query's FORMULATION what it is, declared in one place and
 * under names that all three queries of the probe share, so that the
 * duplicate-formulation comparison in stage J extracts the SIBLING sources'
 * own declarations by those same names and applies one shared predicate to
 * both directions. Two consequences, and both are the point:
 *
 *   - the verdict this envelope states against a sibling and the verdict that
 *     sibling states against this query are computed from the same inputs by
 *     the same predicate, so the relation is symmetric BY CONSTRUCTION rather
 *     than by one envelope transcribing the other's conclusion. A transcription
 *     can drift from what it transcribes; this cannot;
 *   - nothing about a sibling is written down here. A sibling's edge kinds,
 *     pair set, bound, selector literals and API list are read out of its
 *     source at run time, so a sibling that changes its formulation changes
 *     this verdict rather than silently contradicting it.
 *
 * One scope limitation, stated here and published in the envelope rather than
 * left for a reader to find: the entry-selector constant names below are pair
 * ONE's. This query addresses two pairs, and the second pair's selectors are
 * published in full in the pairs block of the envelope; they are not part of
 * the selector comparison, whose scope is the pair the two queries share.
 * FORMULATION_PAIR_IDS is what expresses the additional pair, and it is the
 * difference in that list that makes a duplication SCOPED rather than total.
 *
 * FORMULATION_BOUND_VALUE repeats the bound as a bare literal because the
 * comparison reads the sibling's value out of its TEXT and must read this
 * query's the same way. It is asserted against MAX_CALL_DEPTH at run time, so
 * the repetition cannot drift unnoticed.
 */
val FORMULATION_EDGE_KINDS = List("CALL")
val FORMULATION_END_NODE_KINDS = List("METHOD")
val FORMULATION_PAIR_IDS = List("pair-one", "pair-two")
val FORMULATION_BOUND_NAME = "MAX_CALL_DEPTH"
val FORMULATION_BOUND_KIND = "call-graph hops expanded from an entry point"
val FORMULATION_BOUND_VALUE = 12
val FORMULATION_TRAVERSAL_SEMANTICS =
  "reachability over CALL edges, selecting whole METHOD nodes as its ends, run " +
    "through one reusable pair structure invoked once per declared handler/sink pair"
/** The constants that select pair one's entry points, the shared sink and the
 *  predicate set, named rather than repeated: the comparison extracts each
 *  source's own literals by these names and compares literal TEXT with literal
 *  text, so no unescaping step can make two equal selectors look different. */
val FORMULATION_ENTRY_SELECTOR_CONSTANT_NAMES = List(
  "PAIR_ONE_HANDLER_TYPE",
  "PAIR_ONE_HANDLER_METHOD",
  "PAIR_ONE_HANDLER_SYNTHETIC_TYPE_REGEX",
  "HANDLER_SYNTHETIC_METHOD")
val FORMULATION_SINK_SELECTOR_CONSTANT_NAMES = List(
  "SINK_CALLEE_REGEX",
  "SINK_CALL_NAME",
  "SINK_HOST_TYPE_REGEX")
val FORMULATION_PREDICATE_CONSTANT_NAMES = List(
  "PREDICATE_TYPE",
  "PREDICATE_NAME_REGEX",
  "PREDICATE_SETTER_SUFFIX",
  "PREDICATE_NAMED_FIVE")
val FORMULATION_API_CONSTRUCTS_CONSTANT_NAME = "JOERN_API_CONSTRUCTS"
/** The scope limitation above, in the form the envelope publishes it. */
val FORMULATION_SELECTOR_SCOPE_LIMITATION =
  "the entry-selector literals compared are pair one's, the pair this query and the " +
    "call-graph query share. This query's second pair is expressed through " +
    "FORMULATION_PAIR_IDS, and its selectors are published in full in the pairs block " +
    "of this envelope rather than folded into the comparison. The consequence is stated " +
    "rather than hidden: two queries declaring the same pair id set but different " +
    "selectors for a pair other than the compared one would not be distinguished by " +
    "the selector component alone, and a reader checking that case reads the pairs " +
    "block"

// ------------------------------------------------------------- effort measures
/**
 * Effort measure 1 - query revisions committed. Convention: the number of git
 * commits touching THIS .sc path in the history of the HEAD the run measured at.
 * The value is MEASURED from the repository's own history at run time (stage A)
 * and published together with the commit list, the HEAD and the branch, never
 * written down here: a hard-coded revision count is a figure that stops being
 * true the next time the file is committed, and a count published without the
 * window it was taken in is a figure nobody can reproduce.
 */
val QUERY_REVISIONS_CONVENTION =
  "commits touching queries/joern/" + QUERY_ID +
    ".sc in the history of the HEAD this run measured at, newest first, counted at run " +
    "time from the repository's own history. ONE convention, with three parts that " +
    "make the number reproducible: the range is HEAD's own ancestry, named explicitly " +
    "rather than defaulted, and the HEAD and the branch it was on are published beside " +
    "the count; every commit returned is verified to be an ancestor of that HEAD, so a " +
    "commit reachable only from another ref cannot enter the count - which is what " +
    "happened to earlier figures once per-clone branches were reconciled and the " +
    "commits a previous run had listed stopped being ancestors of the branch carrying " +
    "its files; and the commit that PUBLISHES these result files is necessarily not " +
    "among them, because it cannot exist while the run that writes them is still in " +
    "progress. A later reader whose git log shows one more commit than the count " +
    "reconciles against that window rather than against a bare number"
/** How the revision count is measured. The command is named rather than
 *  inlined, it is given a bound so a stuck child cannot stall the probe, and
 *  its output is validated against the shape a commit identifier has. */
/** The APPROVED ABSOLUTE executables this query will invoke, and nothing else.
 *  A bare program name is resolved through the inherited PATH, so a substituted
 *  `git` earlier on it would be executed by a probe that runs as root
 *  (CWE-426, CWE-427). Each candidate is checked to be an absolute path, a
 *  regular file and executable, and is then required to identify itself as git
 *  within the bounded wait below; the first candidate that satisfies all of
 *  those is used and the rest are refused with the reason recorded. Where none
 *  qualifies the revision measurement is reported as NOT ESTABLISHED rather
 *  than taken from an unverified program. */
val GIT_EXECUTABLE_CANDIDATES = List("/usr/bin/git", "/bin/git", "/usr/local/bin/git")
/** Published output names the executable by ROLE, never by path: a candidate
 *  path in a preserved stream discloses host layout for no benefit. */
val GIT_EXECUTABLE_LABEL = "the approved absolute git executable"
val GIT_VERSION_PREFIX = "git version"
/** Why each rejected candidate was rejected, in candidate order. */
val gitExecutableRefusals = scala.collection.mutable.ArrayBuffer.empty[String]
val GIT_WAIT_SECONDS = 30L
val GIT_COMMIT_SHA_REGEX = """^[0-9a-f]{40}$"""
val GIT_OUTPUT_LINES_REPORTED = 4

/**
 * Effort measure 2 - the distinct Joern API constructs this query uses, listed
 * explicitly and deduplicated so the count is auditable from the list rather
 * than asserted. One entry per distinct API member this query invokes, named
 * <receiver kind>.<member>, and every member name below is invoked somewhere in
 * this file - grep the source for the member to audit an entry. The count is
 * computed from the list, never written down separately.
 *
 * The set difference against each sibling's published list is computed in stage
 * J as evidence for the duplicate-formulation verdict, rather than eyeballed.
 */
val JOERN_API_CONSTRUCTS = List(
  "Call.code",
  "Call.dispatchType",
  "Call.lineNumber",
  "Call.method",
  "Call.methodFullName",
  "Call.name",
  "Call.order",
  "Method.callIn",
  "Method.callOut",
  "Method.fullName",
  "Method.lineNumber",
  "Method.name",
  "Method.typeDecl",
  "NoResolve.getCalledMethodsAsTraversal",
  "Steps.fullName",
  "Steps.fullNameExact",
  "Steps.l",
  "Steps.nameExact",
  "Steps.size",
  "Steps.take",
  "TypeDecl.fullName",
  "TypeDecl.method",
  "cpg.call",
  "cpg.file",
  "cpg.method",
  "cpg.typeDecl",
  "importCpg",
  "switchWorkspace")

/*
 * DELIBERATELY ABSENT: the sibling queries' published API construct lists, their
 * published figures, and the verdicts their envelopes state against this query.
 *
 * Every one of those was a TRANSCRIPTION of a sibling's RESULT, and a
 * transcription can drift from the file it was copied out of without anything
 * detecting it - which is precisely the integrity defect this query is not
 * allowed to carry. They are replaced in stage K by a comparison of the two
 * SOURCES: each sibling's declared formulation identity block is read out of its
 * own .sc file at run time, under names all three queries share, and one shared
 * predicate is applied to both sides. Both directions therefore evaluate
 * identical inputs through identical code, so the relation is symmetric BY
 * CONSTRUCTION rather than by a copied verdict, and a sibling's own numbers are
 * never restated here at all.
 */

/**
 * Effort measure 3 - parameterizability. THIS FILE OWNS IT. It is not a
 * constant and it is not claimed here: it is decided in stage K from whether
 * the second named pair's invocation actually ran, and it is reported with both
 * pairs, both invocations and both outcomes. These two constants only name the
 * measure and state its pass condition, so that the condition is in the source
 * a reviewer reads rather than only in the report.
 */
val PARAMETERIZABILITY_OWNER = QUERY_ID
val PARAMETERIZABILITY_PASS_CONDITION =
  "passes ONLY where the parameterized query is actually invoked on the second " +
    "named pair and that invocation's result is captured in this query's result " +
    "files and console log; an empty result from a real invocation satisfies it, " +
    "a skipped invocation does not, and a parameter list that merely exists does not"

// -------------------------------------------------------------------- markers
val MARKER_START = "---BLITZY-START---"
val MARKER_RESULT_BEGIN = "---BLITZY-RESULT-BEGIN---"
val MARKER_RESULT_END = "---BLITZY-RESULT-END---"
val MARKER_OK = "---BLITZY-OK---"
val MARKER_FAILURE = "---BLITZY-FAILURE---"


// ===========================================================================
// CONSOLE, STAGE TRACKING AND FAIL-LOUD HELPERS
// (restated from queries 01 and 02 so all three probes' console records read alike)
// ===========================================================================

/** Every console line this query prints, in order, for the canonical log. */
val consoleLines = scala.collection.mutable.ArrayBuffer.empty[String]

/** The five markers, in the order a successful run must print them exactly
 *  once. The console stream is parsed on these markers by every consumer of
 *  this log, so the protocol is validated before the log is published rather
 *  than assumed from the fact that the code prints them. */
val MARKER_TOKENS = List(
  MARKER_START, MARKER_RESULT_BEGIN, MARKER_RESULT_END, MARKER_OK, MARKER_FAILURE)
/** The common prefix of every marker. A line of untrusted text beginning with it
 *  is neutralised rather than dropped, so nothing is lost and nothing forged. */
val MARKER_PREFIX = "---BLITZY-"
val MARKER_NEUTRALISED_PREFIX = "[quoted]"
val CONTROL_CHARACTER_ESCAPES =
  "CR as \\r, LF as \\n, TAB as \\t and every other control character as \\u<4 hex digits>"

/**
 * Neutralise one line of text for a marker-parsed, verbatim-preserved stream.
 *
 * Two hazards, both from data this query does not author: a path, a git
 * diagnostic or an exception message can contain a newline, and it can begin
 * with one of the marker tokens. A raw newline turns one logged line into two,
 * so a value ending in "\n---BLITZY-OK---" would forge a completion marker in a
 * log that never completed; and a value that merely STARTS with the marker
 * prefix forges one without needing a newline at all. Both are closed here,
 * centrally, so no call site has to remember:
 *
 *   - every control character is escaped, which makes a line exactly one line;
 *   - a line whose text would begin with the marker prefix is prefixed with
 *     MARKER_NEUTRALISED_PREFIX, so it can still be read but can no longer be
 *     parsed as a marker.
 *
 * The markers this query itself emits go through `logMarker`, which is the only
 * writer allowed to produce a bare marker line.
 */
def sanitizeForLog(line: String): String = {
  val escaped = line.flatMap {
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case c if c.isControl => "\\u%04x".format(c.toInt)
    case c => c.toString
  }
  if (escaped.startsWith(MARKER_PREFIX)) MARKER_NEUTRALISED_PREFIX + " " + escaped
  else escaped
}

/** The characters that open a BLOCK construct when a value begins a line, and
 *  that `mdSafe`'s per-character pass has not already neutralised. `>` (block
 *  quote), `|` (table row), backtick and `~` (fences) and `*` (bullet) are
 *  neutralised there, so only these four remain to be escaped at position 0. */
val MD_LINE_BLOCK_OPENERS: Set[Char] = Set('#', '-', '+', '=')

/** The two delimiters that turn a leading digit run into an ordered-list marker. */
val MD_ORDERED_LIST_DELIMITERS: Set[Char] = Set('.', ')')

/** CommonMark's shortest fence, and the floor `mdFence` never goes below. */
val MD_FENCE_MINIMUM_BACKTICKS = 3

/**
 * One untrusted value, made safe for a PLAIN-TEXT stream (CWE-117).
 *
 * Every control character is replaced by its Unicode code point and nothing else
 * is touched, so the value stays as readable as it was. The replacement DESCRIBES
 * the character rather than reproducing it, which is the whole point: a bare CR
 * rewrites the line an operator is reading, an ESC introduces a terminal control
 * sequence, a NUL truncates the line for some readers and a LF forges a new one
 * in a stream whose lines are its records. `Char.isControl` is
 * `Character.isISOControl`, which is exactly U+0000-U+001F, U+007F and
 * U+0080-U+009F - the set that acts rather than prints. A tab is inside it and is
 * replaced too: this query writes no column-aligned console field that a tab
 * belongs in, so nothing is lost, and exempting one control on the grounds that
 * it is usually harmless is the kind of exception this boundary exists to avoid.
 *
 * A `null` renders as `"null"` rather than throwing, and that is a compatibility
 * requirement rather than defensiveness. Every call site of this helper and of
 * `mdSafe` replaced a direct `${...}` interpolation, and Scala's interpolator
 * renders a null reference as the four characters `null`; an escaper that threw
 * where the code it replaced printed would have introduced a new failure mode -
 * an unhandled NullPointerException part-way through writing a report - in the
 * name of fixing an output-encoding one. No value reaching either helper is
 * expected to be null: a Joern string property is non-null by the CPG schema,
 * and the one genuinely nullable value in this source, `Throwable.getMessage`,
 * is already put through `String.valueOf` at its own call site. The guard is
 * here so the expectation is not load-bearing.
 */
def plainSafe(untrusted: String): String =
  if (untrusted == null) "null"
  else if (!untrusted.exists(_.isControl)) untrusted
  else untrusted.flatMap(c => if (c.isControl) f"U+${c.toInt}%04X" else c.toString)

/**
 * One untrusted value, made safe for the CommonMark report (CWE-116, CWE-117).
 *
 * The result is safe to interpolate INLINE, inside a CODE SPAN or inside a TABLE
 * CELL, which is what lets one helper serve every untrusted call site in the
 * report rather than a set of context-specific ones a later editor would have to
 * choose between correctly.
 *
 * Controls go first, through `plainSafe`, because a LF or a CR would otherwise
 * break the report's line and table structure before any punctuation rule got a
 * chance to matter. Then each character that can change CommonMark's meaning is
 * neutralised, and the mechanism differs for exactly one of them:
 *
 *   - a BACKTICK is REPLACED by its code point, not backslash-escaped, because a
 *     backslash escape is not processed inside a code span - so a backtick that
 *     survived would still close the span and hand the rest of the value to the
 *     document as markup. It is also the character that terminates a fence, which
 *     is the second half of the same hazard;
 *   - everything else is BACKSLASH-ESCAPED, which CommonMark defines for every
 *     ASCII punctuation character: `\` so it cannot consume the next character,
 *     `<` and `>` because they open raw HTML and autolinks and close them, `&`
 *     because it opens an entity, `|` because GFM splits a table cell on it even
 *     inside a code span, `[` and `]` because they label a link or an image, `*`
 *     because it can open emphasis intraword, and `~` because a pair of them
 *     opens GFM strikethrough.
 *
 * `_` is deliberately NOT escaped: CommonMark's left-flanking rule means an
 * intraword `_` cannot open emphasis, and the worst a flanking pair could do is
 * italicise part of a line - a formatting effect rather than an escape from the
 * context. Escaping it would put a backslash into every bytecode identifier that
 * carries one, which is a large, permanent cost against no hazard. `!` is not
 * escaped either, because it only means anything immediately before a `[`, and
 * `[` is escaped.
 *
 * Finally, a value that starts a LINE can open a block construct with its first
 * character, so a leading `#`, `-`, `+` or `=` is escaped, a leading digit run
 * followed by `.` or `)` has that delimiter escaped, and a leading space run is
 * described rather than reproduced - four or more of them open an indented code
 * block, and a space is the one opener a backslash cannot escape. The check runs
 * unconditionally rather than only at the call sites that begin a line: a value
 * that reaches the report mid-line is unharmed by it, and a later editor moving
 * a call site does not have to remember which rule applied.
 *
 * THE ONE COSMETIC COST, stated rather than hidden: inside a code span a
 * backslash escape is not processed, so a value that really does carry a `<`
 * shows with its backslash in the report. It is paid only by the values that
 * carry those characters, and the safety property does not depend on the context,
 * because the only character that can end a code span is replaced rather than
 * escaped.
 */
def mdSafe(untrusted: String): String = {
  val neutralised = plainSafe(untrusted).flatMap {
    case '`'  => "U+0060"
    case '\\' => "\\\\"
    case '<'  => "\\<"
    case '>'  => "\\>"
    case '&'  => "\\&"
    case '|'  => "\\|"
    case '['  => "\\["
    case ']'  => "\\]"
    case '*'  => "\\*"
    case '~'  => "\\~"
    case c    => c.toString
  }
  val indent = neutralised.takeWhile(_ == ' ').length
  val body = neutralised.drop(indent)
  val described = List.fill(indent)("U+0020").mkString
  val opened =
    if (body.isEmpty) body
    else if (MD_LINE_BLOCK_OPENERS.contains(body.charAt(0))) "\\" + body
    else {
      val digits = body.takeWhile(_.isDigit)
      if (digits.nonEmpty && body.length > digits.length &&
        MD_ORDERED_LIST_DELIMITERS.contains(body.charAt(digits.length)))
        digits + "\\" + body.substring(digits.length)
      else body
    }
  described + opened
}

/**
 * The fence for one fenced block, MEASURED from the payload (CWE-116).
 *
 * A fixed three-backtick fence is terminated by any three-backtick run inside
 * the payload, which ends the block early and publishes the remainder of the
 * payload as document markup. CommonMark closes a fence only on a run at least
 * as long as the one that opened it, so the opening run is chosen one backtick
 * longer than the longest run the payload actually carries, with three as the
 * floor. The same string opens and closes the block.
 *
 * Used for EVERY fenced block in the report, including one whose payload is
 * composed only from literals declared in this source: the point is that the
 * fence length stays a measurement of the payload rather than an assumption
 * about it. Today's single payload is measured to carry no backtick at all, so
 * the chosen fence is the minimum three and the published bytes are unchanged.
 *
 * A `null` payload yields the minimum fence, for the same reason `plainSafe`
 * renders one as text: the helper must not be the thing that fails while a
 * report is half written. There is no run in which it can happen today - the
 * only payload is a literal declared in this source - so this is a guard on an
 * expectation rather than a handler for an observed case.
 */
def mdFence(payload: String): String = {
  if (payload == null) return List.fill(MD_FENCE_MINIMUM_BACKTICKS)("`").mkString
  var longestRun = 0
  var currentRun = 0
  payload.foreach { c =>
    if (c == '`') {
      currentRun += 1
      if (currentRun > longestRun) longestRun = currentRun
    } else currentRun = 0
  }
  List.fill(math.max(MD_FENCE_MINIMUM_BACKTICKS, longestRun + 1))("`").mkString
}

def log(line: String): Unit = {
  val safe = sanitizeForLog(line)
  consoleLines += safe
  println(safe)
}

/** The only writer that may emit a bare marker line. Refuses anything that is
 *  not one of the five declared tokens, so a marker cannot be printed by
 *  accident from a string that happened to look like one. */
def logMarker(marker: String): Unit = {
  if (!MARKER_TOKENS.contains(marker)) {
    throw new IllegalStateException(
      s"refusing to print '$marker' as a marker: it is not one of the declared tokens " +
        MARKER_TOKENS.mkString(", "))
  }
  consoleLines += marker
  println(marker)
}

/**
 * Validate the marker protocol over the lines about to be published, and return
 * the count of each marker found. A console log is evidence only while its
 * markers mean what a consumer reads them to mean, so the check is made on the
 * exact lines that will be written, immediately before they are staged - not on
 * the code's intent to write them.
 *
 * `expectedOk` distinguishes the two legitimate shapes: a completed run prints
 * START, RESULT_BEGIN, RESULT_END and OK exactly once each, in that order, and
 * no FAILURE; a failed run prints START and FAILURE exactly once each, no OK,
 * and no result region at all - a partial result region looks like a completed
 * run, which is why its absence is checked rather than hoped for.
 */
def validateMarkerProtocol(lines: Seq[String], expectedOk: Boolean): List[(String, Int)] = {
  val counts = MARKER_TOKENS.map(t => t -> lines.count(_ == t))
  val byToken = counts.toMap
  def positionOf(token: String): Int = lines.indexOf(token)
  val required =
    if (expectedOk) List(MARKER_START, MARKER_RESULT_BEGIN, MARKER_RESULT_END, MARKER_OK)
    else List(MARKER_START, MARKER_FAILURE)
  val forbidden =
    if (expectedOk) List(MARKER_FAILURE)
    else List(MARKER_RESULT_BEGIN, MARKER_RESULT_END, MARKER_OK)
  val problems = scala.collection.mutable.ArrayBuffer.empty[String]
  required.foreach { t =>
    if (byToken.getOrElse(t, 0) != 1) {
      problems += s"$t appears ${byToken.getOrElse(t, 0)} time(s), expected exactly 1"
    }
  }
  forbidden.foreach { t =>
    if (byToken.getOrElse(t, 0) != 0) {
      problems += s"$t appears ${byToken.getOrElse(t, 0)} time(s), expected none"
    }
  }
  if (problems.isEmpty) {
    val ordered = required.map(positionOf)
    if (ordered != ordered.sorted) {
      problems += "the markers appear out of order: " +
        required.zip(ordered).map { case (t, i) => s"$t at line ${i + 1}" }.mkString(", ")
    }
  }
  if (problems.nonEmpty) {
    throw new IllegalStateException(
      "the console stream about to be published does not satisfy the marker protocol " +
        s"for an ${if (expectedOk) "completed" else "failed"} run: " +
        problems.mkString("; ") +
        ". Every line of untrusted text is escaped and marker-prefix-neutralised by " +
        "sanitizeForLog, so a violation here is this query's own emission rather than " +
        "injected content, and publishing it would hand a consumer a log whose markers " +
        "do not mean what they say")
  }
  counts
}

/** The stage name a failure is attributed to. Named, never guessed. */
var currentStage: String = "A-startup"
def stage(name: String): Unit = {
  currentStage = name
  log(s"[stage] $name")
}

/** Stop the run. The message names the condition and, where relevant, the path. */
def abortRun(reason: String): Nothing =
  throw new IllegalStateException(s"HALT in stage $currentStage: $reason")

def elapsedMs(startNanos: Long): Long = (System.nanoTime() - startNanos) / 1000000L

// ------------------------------------------------------------ JSON primitives
/** Minimal, deterministic JSON encoding. No dependency, no key reordering. */
def jesc(s: String): String =
  s.flatMap {
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case c if c.isControl => "?"
    case c => c.toString
  }

def jstr(s: String): String = "\"" + jesc(s) + "\""
def jnum(n: Long): String = n.toString
def jbool(b: Boolean): String = if (b) "true" else "false"
def jstrArr(xs: Seq[String]): String =
  if (xs.isEmpty) "[]" else xs.map(jstr).mkString("[", ", ", "]")

def jobj(indent: Int, fields: Seq[(String, String)]): String = {
  val pad = " " * indent
  if (fields.isEmpty) "{}"
  else fields.map { case (k, v) => s"$pad  ${jstr(k)}: $v" }
    .mkString("{\n", ",\n", s"\n$pad}")
}

def jrawArr(indent: Int, items: Seq[String]): String = {
  val pad = " " * indent
  if (items.isEmpty) "[]"
  else items.map(i => s"$pad  $i").mkString("[\n", ",\n", s"\n$pad]")
}

/** A per-pair scalar, keyed by pair id. Used everywhere a per-pair figure would
 *  otherwise be tempting to add up: an object cannot be summed by accident. */
def jbyPair(indent: Int, entries: Seq[(String, String)]): String = jobj(indent, entries)

// ------------------------------------------------- JVM argument redaction
/** True when this argument's KEY is one of the whitelisted memory or stack
 *  flags, which is the only case in which its value may be logged. */
def jvmArgumentValueIsWhitelisted(arg: String): Boolean =
  JVM_ARG_VALUE_WHITELIST_PREFIXES.exists(p => arg == p || arg.startsWith(p))

/**
 * Reduce one JVM argument to what may be published.
 *
 *   - a whitelisted memory or stack flag is returned unchanged, because the
 *     heap it establishes is the evidence this query reports;
 *   - anything with a key/value separator keeps its key and loses its value:
 *     `-Dsome.property=<value>` becomes `-Dsome.property=<redacted>`, so the
 *     presence of the property is still visible while a token, a password or a
 *     connection string passed in it cannot reach a preserved log;
 *   - an argument with no separator is a key with no value and is returned as
 *     the key it is.
 *
 * The first separator wins, so a value that itself contains one is redacted
 * whole rather than partly.
 */
def redactJvmArgument(arg: String): String =
  if (jvmArgumentValueIsWhitelisted(arg)) arg
  else {
    val cuts = JVM_ARG_KEY_VALUE_SEPARATORS.map(s => arg.indexOf(s)).filter(_ > 0)
    if (cuts.isEmpty) arg
    else arg.substring(0, cuts.min + 1) + JVM_ARG_REDACTION_TOKEN
  }

// -------------------------------------------------------------- file helpers
/**
 * PUBLICATION. Every file this query writes - the JSON envelope, the prose
 * report and the console log - is a member of one evidence set, and the three
 * hazards a naive write runs into are all closed here rather than in each call
 * site:
 *
 *   - a symlink planted at the target, or at the directory holding it, makes a
 *     direct write land wherever the link points. So a target's parent is
 *     required to be a real directory that is not itself a link, the target
 *     itself is refused if it is a link, and the parent's RESOLVED path is
 *     required to still be inside the resolved repository root;
 *   - a predictable temporary name is a race: a second writer, or an attacker
 *     who can create files in the directory, can occupy the name between the
 *     check and the write. So the temporary carries 16 random bytes from a
 *     SecureRandom, is created in the SAME directory as its target (so the
 *     final move is a rename within one filesystem and therefore atomic),
 *     EXCLUSIVELY with CREATE_NEW so an existing name fails rather than being
 *     reused, and with NOFOLLOW_LINKS so it can never be written through a
 *     link;
 *   - a member-by-member publication leaves a mixed generation behind when it
 *     fails half way. So every member is written, flushed and fsynced as a
 *     staged temporary FIRST, and only once every member of the set is on disk
 *     are they moved onto their targets. A failure before that point leaves
 *     every target holding its previous generation, and the staged temporaries
 *     are removed.
 *
 * Staging every member before the first rename closes the window BEFORE the
 * renames, and it is necessary, but it is not sufficient: N renames are N
 * atomic operations rather than one, so a fault between the first and the last
 * still leaves a mixed generation on disk, and nothing already written can be
 * undone. POSIX offers no way to make N renames one operation. What it does
 * offer is a commit record, and that is what closes the remaining window:
 *
 *   - a COMPLETION MANIFEST is staged after every content member has been
 *     staged and measured, carrying each member's target path, byte size and
 *     sha256 and a member-set identifier derived from those digests. It is
 *     renamed LAST, after every content member is in place. So the manifest's
 *     presence is the completion signal: absent, or disagreeing with a member
 *     on disk, means the set on disk is not one generation, and a consumer can
 *     see that without knowing what the previous generation was;
 *   - the manifest is REQUIRED BY THE PRODUCER ITSELF as the first consumer:
 *     immediately after publishing, every member named in it is re-measured
 *     from the disk and required to equal what it records, and the run stops if
 *     any does not. A manifest nothing checks is decoration.
 *
 * The member-set identifier is derived from MEMBER BYTES, deliberately, and is
 * distinct from `publicationId`, which is derived from the query, its source
 * and the graph. The latter answers "which query, source and graph produced
 * this" - repeatably, so two invocations over an unchanged source and graph share
 * it and it names no execution; only the former can answer "is this set complete
 * and self-consistent", because an identifier computed before the members exist
 * cannot depend on them.
 */
/** Shortest absolute path the determinism check searches the rendered envelope
 *  for. A two- or three-character prefix matches ordinary prose, so a path
 *  below this length is declared unsearched rather than producing a hit nobody
 *  can act on. */
val ABSOLUTE_PATH_SEARCH_MIN_LENGTH = 6

val PUBLICATION_TEMP_PREFIX = ".publish-"
val PUBLICATION_TEMP_SUFFIX = ".tmp"
val PUBLICATION_TEMP_RANDOM_BYTES = 16
val PUBLICATION_TEMP_MAX_ATTEMPTS = 32

/** Read-back chunk for verifying a staged or published member. The digest is
 *  streamed so peak memory does not track the member's size. */
val PUBLICATION_VERIFY_CHUNK_BYTES = 1048576

/** Schema name the completion manifest carries, so a consumer can recognise it
 *  by content rather than by filename. */
val PUBLICATION_MANIFEST_SCHEMA = "joern-probe-publication/1.0.0"

/** Role recorded for the manifest itself, which is the only member the manifest
 *  does not record a digest for: it cannot contain its own digest. */
val PUBLICATION_MANIFEST_ROLE = "completion_manifest"

/** The resolved repository root, set in stage A and consulted by every write. */
var repoRootRealPath: Option[Path] = None

/** Where the completion manifest publishes, set in stage A beside the console
 *  log. `None` before stage A has resolved the output paths, in which case a
 *  publication runs without a manifest and SAYS SO rather than pretending to
 *  have one - the only such publication is the early-abort console log, which
 *  is a single member and therefore has no mixed-generation window to close. */
var manifestTargetPath: Option[Path] = None

/** The publication identifier, held where the publication machinery can reach it.
 *  It is computed in the main body once the graph's identity is known, and the
 *  completion manifest carries it so a consumer can refuse a complete-looking
 *  manifest that belongs to another generation of the same query. */
var publicationIdOfRecord: Option[String] = None

/** THIS QUERY'S DECLARED PUBLICATION SCHEMA - the exact member set a complete
 *  generation of this query consists of, stated as a constant rather than read
 *  back from whatever happened to be staged.
 *
 *  This is what makes the manifest check non-circular. Verifying a manifest
 *  against the members that were just staged asks "does this describe what I
 *  wrote", which a one-member record left by a failed three-member publication
 *  answers yes to. Verifying it against the schema asks "is this a complete
 *  generation of this query", which only the full set answers yes to. */
val DECLARED_MEMBER_ROLES: List[String] =
  List(s"$QUERY_ID.json", s"$QUERY_ID.md", s"probe-$QUERY_ID.log")

val DECLARED_MEMBER_PATHS: Map[String, String] = Map(
  s"$QUERY_ID.json" -> s"$RESULTS_DIR/$QUERY_ID.json",
  s"$QUERY_ID.md" -> s"$RESULTS_DIR/$QUERY_ID.md",
  s"probe-$QUERY_ID.log" -> s"$LOG_DIR/probe-$QUERY_ID.log")

val publicationRandom = new java.security.SecureRandom()

final case class StagedMember(
  target: Path,
  temp: Path,
  byteSize: Int,
  sha256: String)

val stagedMembers = scala.collection.mutable.ArrayBuffer.empty[StagedMember]

/** Set once a MULTI-MEMBER publication has begun renaming, and cleared only when
 *  that publication's completion manifest is in place and verified. While it is
 *  true the set on disk may be a mixed generation, and the failure path must not
 *  publish a manifest that would describe a complete set of fewer members. */
var mixedGenerationRisk = false

/** The last publication's member-set identifier and manifest path, for the
 *  envelope and the report to cite. Set by publishStagedMembers. */
var lastPublicationMemberSetId: Option[String] = None
var lastPublicationManifest: Option[String] = None

// ------------------------------------- descriptor-relative publication access
/**
 * One open directory DESCRIPTOR per publication directory, and every operation
 * on that directory performed relative to it.
 *
 * WHY. Validating a path and then using the path is two resolutions of the same
 * name, and between them any ancestor component can be replaced - by a link, or
 * by another directory - by anything running as this user. The validation then
 * describes a directory that is no longer the one being written to. Java's
 * `SecureDirectoryStream` closes that gap: it is bound to an open descriptor, so
 * `newByteChannel`, `move`, `deleteFile` and `getFileAttributeView` on it are
 * resolved against THAT directory rather than against a name, whatever happens
 * to the path afterwards.
 *
 * HOW A DIRECTORY IS REACHED. The descent starts at the resolved repository root
 * and opens each component with `newDirectoryStream(name, NOFOLLOW_LINKS)`,
 * which refuses a symbolic link outright (ELOOP) rather than following it. So no
 * component is ever resolved by a full pathname, and containment in the
 * repository root is a property of where the descent STARTED rather than a
 * string comparison made afterwards.
 *
 * FAIL CLOSED. `Files.newDirectoryStream` is only required to return a
 * `SecureDirectoryStream` on platforms that support it. Where it does not, this
 * query stops: publishing through pathnames while claiming descriptor-relative
 * writes would be a false claim, and the alternative - falling back silently -
 * is the defect this closes.
 */
type SecureDir = java.nio.file.SecureDirectoryStream[Path]

/** Cast one directory stream to a secure one, or stop. */
def asSecureDir(dir: Path, stream: java.nio.file.DirectoryStream[Path]): SecureDir =
  stream match {
    case secure: java.nio.file.SecureDirectoryStream[_] =>
      secure.asInstanceOf[SecureDir]
    case other =>
      try other.close() catch { case _: Throwable => () }
      abortRun(s"this platform's directory stream for $dir is " +
        s"${other.getClass.getName}, not a SecureDirectoryStream, so a write cannot be " +
        "bound to an open descriptor and every publication would be resolved by " +
        "pathname. Failing closed rather than publishing through names while claiming " +
        "otherwise")
  }

/** Every directory descriptor opened for publication, keyed by absolute path, so
 *  one directory is descended to ONCE per run and every later operation on it
 *  reuses that descriptor rather than re-resolving the name. */
val publicationDirDescriptors = scala.collection.mutable.LinkedHashMap.empty[String, SecureDir]

/** Close every publication descriptor. Called on the way out of the run. */
def closePublicationDescriptors(): Unit = {
  publicationDirDescriptors.values.foreach { d =>
    try d.close() catch { case _: Throwable => () }
  }
  publicationDirDescriptors.clear()
}

/**
 * Descend to `absolute` from the resolved repository root by descriptor,
 * creating any missing component one at a time, and return the open descriptor.
 *
 * Creation is still by name - `SecureDirectoryStream` exposes no directory
 * creation - so each component is created and then IMMEDIATELY re-entered by
 * descriptor with NOFOLLOW, which is what refuses a component that was replaced
 * by a link in between. Every publication directory this query uses already
 * exists in the checkout, so the creation path is a corner case rather than the
 * normal one; it is kept because a checkout could legitimately lack the results
 * directory.
 */
def openPublicationDescriptor(absolute: Path): SecureDir =
  publicationDirDescriptors.getOrElseUpdate(absolute.toString, {
    val root = repoRootRealPath.getOrElse(
      abortRun(s"the repository root is not resolved yet, so the descent to $absolute " +
        "has no verified starting point"))
    if (!absolute.startsWith(root)) {
      abortRun(s"refusing to publish outside the repository root: $absolute is not " +
        s"inside $root")
    }
    var current: SecureDir =
      asSecureDir(root, Files.newDirectoryStream(root))
    val relative = root.relativize(absolute)
    var index = 0
    while (index < relative.getNameCount) {
      val name = relative.getName(index)
      index += 1
      val asPath = Paths.get(name.toString)
      // Create the component if it is absent. The existence test and the
      // creation are both descriptor-relative: the attribute view throws when
      // the name does not exist, and createDirectory is by name because there is
      // no descriptor-relative form - which is why the very next step re-enters
      // the component by descriptor with NOFOLLOW and refuses a link.
      val exists =
        try {
          current
            .getFileAttributeView(asPath, classOf[java.nio.file.attribute.BasicFileAttributeView],
              LinkOption.NOFOLLOW_LINKS)
            .readAttributes()
          true
        } catch {
          case _: java.nio.file.NoSuchFileException => false
        }
      if (!exists) {
        val byName = (0 until index).foldLeft(root)((acc, i) => acc.resolve(relative.getName(i)))
        try Files.createDirectory(byName)
        catch { case _: java.nio.file.FileAlreadyExistsException => () }
      }
      val next =
        try asSecureDir(absolute, current.newDirectoryStream(asPath, LinkOption.NOFOLLOW_LINKS))
        catch {
          case t: java.nio.file.FileSystemException =>
            try current.close() catch { case _: Throwable => () }
            abortRun(s"refusing to publish through $name on the path to $absolute: " +
              s"${t.getClass.getSimpleName} on a no-follow descent, which is what a " +
              "symbolic link or a non-directory component produces. A write through it " +
              "would land wherever the link points rather than at the path this run " +
              "records")
        }
      try current.close() catch { case _: Throwable => () }
      current = next
    }
    // The descriptor's own inode, compared against the pathname's. Equality is
    // what lets the directory fsync below - which has no descriptor-relative
    // form - be attributed to the directory that was descended to.
    val throughDescriptor = current
      .getFileAttributeView(Paths.get("."),
        classOf[java.nio.file.attribute.BasicFileAttributeView], LinkOption.NOFOLLOW_LINKS)
      .readAttributes()
    val throughName =
      Files.readAttributes(absolute, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
    if (throughDescriptor.fileKey != throughName.fileKey) {
      try current.close() catch { case _: Throwable => () }
      abortRun(s"the directory $absolute reached by descriptor is inode " +
        s"${throughDescriptor.fileKey} while the same path resolves to " +
        s"${throughName.fileKey}, so a component was replaced during the descent")
    }
    current
  })

/** The descriptor for a member's parent directory. */
def publicationDescriptorOf(target: Path): SecureDir = {
  val parent = Option(target.getParent).getOrElse(
    abortRun(s"a publication target must name a parent directory: $target"))
  openPublicationDescriptor(parent.toAbsolutePath.normalize())
}

/**
 * Validate where a member is about to be published and return the parent
 * directory's real path. Refuses rather than repairs: a link at the target or
 * at its parent, or a parent that resolves outside the repository root, stops
 * the run instead of writing somewhere the record does not name.
 *
 * This remains the containment and no-link check on the PATH. It is no longer
 * what the write is bound to: `openPublicationDescriptor` above descends to the
 * same directory by descriptor and every create, read, rename and delete below
 * goes through that descriptor. The two are kept together deliberately - the
 * path check produces the diagnostic a reader can act on, and the descriptor
 * produces the guarantee.
 */
def publicationParentOf(target: Path): Path = {
  val parent = Option(target.getParent).getOrElse(
    abortRun(s"a publication target must name a parent directory: $target"))
  val absolute = parent.toAbsolutePath.normalize()

  // EVERY component is checked, not only the last one. Checking the immediate
  // parent and then calling toRealPath() accepted an ancestor link silently:
  // toRealPath FOLLOWS links, so a link two levels up resolved to a directory
  // that was still inside the repository root and the containment test passed.
  // The walk below refuses any component that is a link, whether or not the
  // redirection stays inside the root.
  //
  // And each missing component is created ONE AT A TIME, with the component
  // re-checked immediately after creation. createDirectories(parent) ran BEFORE
  // any of these checks, so a missing descendant under a linked ancestor was
  // created at the redirected destination and only then refused - the write was
  // prevented, but the directory had already been made in the wrong place.
  var walked: Path = absolute.getRoot
  if (walked == null) {
    abortRun(s"a publication target must be absolute: $target")
  }
  var componentIndex = 0
  while (componentIndex < absolute.getNameCount) {
    walked = walked.resolve(absolute.getName(componentIndex))
    componentIndex += 1
    if (!Files.exists(walked, LinkOption.NOFOLLOW_LINKS)) {
      try Files.createDirectory(walked)
      catch {
        case _: java.nio.file.FileAlreadyExistsException => ()
      }
    }
    if (Files.isSymbolicLink(walked)) {
      abortRun(s"refusing to publish through a symbolic link: $walked, on the path " +
        s"to $target, is a link. A write through it lands wherever the link points " +
        "rather than at the path this run records, and every component of the path " +
        "is checked rather than only the immediate parent")
    }
    if (!Files.isDirectory(walked, LinkOption.NOFOLLOW_LINKS)) {
      abortRun(s"a component of a publication target's path is not a directory: $walked")
    }
  }
  if (Files.isSymbolicLink(target)) {
    abortRun(s"refusing to publish onto a symbolic link: $target is a link, and " +
      "writing through it would modify its target rather than this path")
  }
  // No component is a link, so this cannot differ from `absolute` - it is
  // computed anyway, and required to agree, because the assertion is cheap and
  // its failure would mean a component was replaced during the walk.
  val realParent = absolute.toRealPath()
  if (realParent != absolute) {
    abortRun(s"refusing to publish to $target: its parent $absolute resolves to " +
      s"$realParent even though no component measured as a link, which means a " +
      "component was replaced while the path was being validated")
  }
  repoRootRealPath.foreach { root =>
    if (!realParent.startsWith(root)) {
      abortRun(s"refusing to publish outside the repository root: $target resolves " +
        s"into $realParent, which is not inside $root. A link on the path is the " +
        "usual cause")
    }
  }
  realParent
}

/**
 * Stream a file's byte size and sha256 back off the disk, opened NOFOLLOW so a
 * link swapped in after the write is refused rather than measured.
 *
 * This is what makes "the bytes on disk are the bytes that were validated" a
 * measurement rather than an assumption: the digest recorded for a member is
 * taken from the file, never from the string that was handed to the writer.
 */
def measureFileNoFollow(p: Path): (Int, String) = {
  val dir = publicationDescriptorOf(p)
  val channel = dir.newByteChannel(
    Paths.get(p.getFileName.toString),
    java.util.Set.of[java.nio.file.OpenOption](
      java.nio.file.StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
  try {
    val digest = MessageDigest.getInstance("SHA-256")
    val buffer = java.nio.ByteBuffer.allocate(PUBLICATION_VERIFY_CHUNK_BYTES)
    var total = 0L
    var read = channel.read(buffer)
    while (read > 0) {
      buffer.flip()
      digest.update(buffer)
      buffer.clear()
      total += read
      read = channel.read(buffer)
    }
    (total.toInt, digest.digest().map("%02x".format(_)).mkString)
  } finally channel.close()
}

/** A file's whole contents, read through its directory's descriptor with
 *  NOFOLLOW. Used for the members this query reads back - the completion
 *  manifest and the published envelope - all of which are a few kilobytes. */
def readFileNoFollow(p: Path): Array[Byte] = {
  val dir = publicationDescriptorOf(p)
  val channel = dir.newByteChannel(
    Paths.get(p.getFileName.toString),
    java.util.Set.of[java.nio.file.OpenOption](
      java.nio.file.StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
  try {
    val out = new java.io.ByteArrayOutputStream()
    val buffer = java.nio.ByteBuffer.allocate(PUBLICATION_VERIFY_CHUNK_BYTES)
    var read = channel.read(buffer)
    while (read > 0) {
      out.write(buffer.array(), 0, read)
      buffer.clear()
      read = channel.read(buffer)
    }
    out.toByteArray
  } finally channel.close()
}

/** True when `p` is a regular file, tested through its directory's descriptor
 *  with NOFOLLOW so the answer is about the file the later read will open. */
def isRegularFileNoFollow(p: Path): Boolean =
  try {
    publicationDescriptorOf(p)
      .getFileAttributeView(Paths.get(p.getFileName.toString),
        classOf[java.nio.file.attribute.BasicFileAttributeView], LinkOption.NOFOLLOW_LINKS)
      .readAttributes()
      .isRegularFile
  } catch {
    case _: java.nio.file.NoSuchFileException => false
  }

/**
 * Rename a staged temporary onto its target within one directory, relative to
 * that directory's open descriptor.
 *
 * `SecureDirectoryStream.move` between the SAME stream is a rename(2) within one
 * directory, which is atomic on this filesystem - the property the publication
 * protocol depends on - and neither name is resolved through the path's
 * ancestors. Both must sit in the same directory for that to hold, which is
 * guaranteed by construction (`stageMember` creates the temporary beside its
 * target) and asserted here rather than assumed.
 */
def moveWithinPublicationDir(temp: Path, target: Path): Unit = {
  val tempParent = Option(temp.getParent).map(_.toAbsolutePath.normalize())
  val targetParent = Option(target.getParent).map(_.toAbsolutePath.normalize())
  if (tempParent != targetParent) {
    abortRun(s"refusing to publish $target from $temp: a descriptor-relative rename " +
      "requires both names in one directory, and these are in two")
  }
  val dir = publicationDescriptorOf(target)
  dir.move(
    Paths.get(temp.getFileName.toString), dir, Paths.get(target.getFileName.toString))
}

/**
 * Write one member to a private temporary beside its target, fsync it, and
 * remember it. Nothing is visible at the target until the whole set publishes.
 */
def stageMember(target: Path, content: String): StagedMember = {
  val realParent = publicationParentOf(target)
  val dir = publicationDescriptorOf(target)
  val bytes = content.getBytes(StandardCharsets.UTF_8)
  var channel: java.nio.channels.FileChannel = null
  var temp: Path = null
  var attempts = 0
  while (channel == null) {
    attempts += 1
    if (attempts > PUBLICATION_TEMP_MAX_ATTEMPTS) {
      abortRun(s"could not create a private temporary beside $target after " +
        s"$PUBLICATION_TEMP_MAX_ATTEMPTS attempts")
    }
    val suffix = new Array[Byte](PUBLICATION_TEMP_RANDOM_BYTES)
    publicationRandom.nextBytes(suffix)
    val candidateName =
      PUBLICATION_TEMP_PREFIX + target.getFileName.toString + "." +
        suffix.map("%02x".format(_)).mkString + PUBLICATION_TEMP_SUFFIX
    val candidate = realParent.resolve(candidateName)
    try {
      // Created RELATIVE TO THE DIRECTORY DESCRIPTOR, so the file lands in the
      // directory that was descended to whatever happened to the pathname since.
      // CREATE_NEW keeps the name exclusive and NOFOLLOW_LINKS refuses a link
      // planted at it; the channel is a FileChannel on this platform, which is
      // what makes force(true) below - the durability the manifest asserts -
      // available at all. Anything else fails closed.
      channel = dir.newByteChannel(
        Paths.get(candidateName),
        java.util.Set.of[java.nio.file.OpenOption](
          java.nio.file.StandardOpenOption.CREATE_NEW,
          java.nio.file.StandardOpenOption.WRITE,
          LinkOption.NOFOLLOW_LINKS)) match {
        case fc: java.nio.channels.FileChannel => fc
        case other =>
          try other.close() catch { case _: Throwable => () }
          try dir.deleteFile(Paths.get(candidateName)) catch { case _: Throwable => () }
          abortRun(s"the descriptor-relative channel for $candidateName is " +
            s"${other.getClass.getName}, which exposes no force(): the durability this " +
            "publication asserts could not be established, so nothing is written")
      }
      temp = candidate
    } catch {
      case _: java.nio.file.FileAlreadyExistsException => channel = null
    }
  }
  try {
    // FileChannel.write is documented to write "a sequence of bytes", not
    // necessarily all of them: a single call can return a short count. The
    // unlooped call this replaced would then have staged a truncated member
    // whose recorded digest was computed from the full byte array, so the
    // member and its digest would have disagreed with nothing detecting it.
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    while (buffer.hasRemaining) {
      val written = channel.write(buffer)
      if (written <= 0 && buffer.hasRemaining) {
        abortRun(s"the staged temporary $temp stopped accepting bytes with " +
          s"${buffer.remaining} of ${bytes.length} still to write")
      }
    }
    channel.force(true)
  } catch {
    // The member is not in `stagedMembers` yet, so discardStagedMembers would
    // never reach this temporary: a write or force that throws here would have
    // left a `.publish-*.tmp` sibling behind forever. It is removed on the way
    // out, and the original throwable is re-raised rather than replaced.
    case t: Throwable =>
      try deleteStagedTemp(temp)
      catch { case _: Throwable => () }
      throw t
  } finally channel.close()

  // The staged bytes are read BACK off the disk and required to equal what was
  // intended. Recording the digest of the in-memory array asserted what the
  // writer meant to write; this measures what is actually there, which is what
  // the manifest publishes and what a reader will hash.
  val intendedDigest = sha256OfBytes(bytes)
  val (stagedSize, stagedDigest) = measureFileNoFollow(temp)
  if (stagedSize != bytes.length || stagedDigest != intendedDigest) {
    try deleteStagedTemp(temp) catch { case _: Throwable => () }
    abortRun(s"the staged temporary for $target does not hold the bytes that were " +
      s"written: measured $stagedSize bytes / sha256 $stagedDigest against an " +
      s"intended ${bytes.length} bytes / sha256 $intendedDigest. Nothing is published")
  }
  val member = StagedMember(target, temp, stagedSize, stagedDigest)
  stagedMembers += member
  member
}

/** Remove one staged temporary, relative to its directory's descriptor. */
def deleteStagedTemp(temp: Path): Unit =
  try publicationDescriptorOf(temp).deleteFile(Paths.get(temp.getFileName.toString))
  catch { case _: java.nio.file.NoSuchFileException => () }

/** Remove every staged temporary. Called when a set will not be published, so
 *  a failure leaves neither a mixed generation nor litter behind. */
def discardStagedMembers(): Unit = {
  stagedMembers.toList.foreach { m =>
    try deleteStagedTemp(m.temp)
    catch {
      case t: Throwable =>
        System.err.println(sanitizeForLog(
          s"could not remove the staged temporary ${m.temp}: ${t.getMessage}"))
    }
  }
  stagedMembers.clear()
}

/** fsync each directory so a rename in it survives a crash. */
def fsyncPublicationDirs(dirs: List[Path]): Unit = dirs.foreach { dir =>
  // FATAL, deliberately. An earlier form recorded a failure and continued, on the
  // reasoning that directory fsync is unsupported on some filesystems. That
  // reasoning is about portability and the consequence is about correctness: the
  // manifest's presence is supposed to mean "every member named here is in place
  // AND durable there", and continuing past a failed sync renames the commit
  // record while that is not established. So a filesystem that cannot establish
  // it does not get a commit record from this query - it gets a halt, with the
  // content renames already done and NO manifest, which is the state a consumer
  // is built to detect. Every directory this run publishes into was verified to
  // support it before this was made fatal.
  try {
    // A directory fsync has no descriptor-relative form in Java, so the pathname
    // open is unavoidable - and it is BOUND to the descriptor that was descended
    // to: the inode reached by name must equal the inode the descriptor holds,
    // or the sync would be attributed to a directory this run never validated.
    val descriptor = openPublicationDescriptor(dir.toAbsolutePath.normalize())
    val throughDescriptor = descriptor
      .getFileAttributeView(Paths.get("."),
        classOf[java.nio.file.attribute.BasicFileAttributeView], LinkOption.NOFOLLOW_LINKS)
      .readAttributes()
    val throughName =
      Files.readAttributes(dir, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
    if (throughDescriptor.fileKey != throughName.fileKey) {
      abortRun(s"refusing to fsync $dir by name: the name resolves to inode " +
        s"${throughName.fileKey} while the validated descriptor holds " +
        s"${throughDescriptor.fileKey}, so the durability would be established for a " +
        "directory other than the one the members were written into")
    }
    val ch = java.nio.channels.FileChannel.open(
      dir, java.nio.file.StandardOpenOption.READ)
    try ch.force(true) finally ch.close()
  } catch {
    case t: Throwable =>
      val note = s"could not fsync the publication directory $dir: ${t.getMessage}"
      System.err.println(note)
      consoleLines += sanitizeForLog(s"[publication] $note")
      abortRun(s"$note. Durability of the renames in that directory is therefore " +
        "not established, so no completion manifest is published for this set: a " +
        "manifest asserting members a crash could lose is worse than none")
  }
}

/**
 * Require a completion manifest at `path` and every member it names, by reading
 * the manifest's PUBLISHED BYTES back and re-measuring each member from disk.
 * Returns the member count it verified.
 *
 * The producer is the first consumer, and it runs the check a later consumer
 * would run rather than a cheaper one it happens to be able to run: the manifest
 * is parsed from disk, its member_set_id is RECOMPUTED from the digests it
 * lists, and each member is re-measured. Recomputing the identifier is what
 * catches a manifest whose per-member digest was edited to match a changed
 * member - re-measuring alone would pass that.
 */
def requirePublicationManifest(
  path: Path,
  expectedRoles: List[String] = Nil,
  expectedPaths: Map[String, String] = Map.empty,
  expectedPublicationId: Option[String] = None
): Int = {
  if (!isRegularFileNoFollow(path)) {
    abortRun(s"the completion manifest $path is absent or is not a regular file, so " +
      "the published set cannot be established as one generation. A publication that " +
      "failed between its renames leaves exactly this state")
  }
  // Read through the directory's own descriptor rather than by pathname: this is
  // the check a later consumer would run, and it must not be the one operation
  // in the publication that a swapped ancestor could redirect.
  val text = new String(readFileNoFollow(path), StandardCharsets.UTF_8)
  if (!text.contains(PUBLICATION_MANIFEST_SCHEMA)) {
    abortRun(s"$path does not carry the $PUBLICATION_MANIFEST_SCHEMA schema, so it is " +
      "not a completion manifest for this publication")
  }
  // The manifest is written by completionManifestContent above, so its shape is
  // known exactly and a full JSON parser is not needed to read it back: each
  // member contributes one "role"/"path"/"bytes"/"sha256" quadruple in that
  // order. The scan is confined to the text from the "members" key onward, so no
  // envelope key and no prose value above it can contribute a match - an earlier
  // form scanned the whole document and dropped the first "path" match on the
  // belief that the manifest's own path was one, which it is not: the manifest
  // names itself under "document". That dropped a real member and made a
  // well-formed three-member manifest read as two.
  val membersAt = text.indexOf("\"members\"")
  if (membersAt < 0) {
    abortRun(s"the completion manifest $path carries no members array, so it " +
      "describes no publication")
  }
  val membersText = text.substring(membersAt)
  val pathRe = """"path"\s*:\s*"([^"]+)"""".r
  val bytesRe = """"bytes"\s*:\s*(\d+)""".r
  val shaRe = """"sha256"\s*:\s*"([0-9a-f]{64})"""".r
  val roleRe = """"role"\s*:\s*"([^"]+)"""".r
  val declaredCount = """"member_count"\s*:\s*(\d+)""".r
    .findFirstMatchIn(text).map(_.group(1).toInt).getOrElse(
      abortRun(s"the completion manifest $path declares no member_count"))
  val declaredSetId = """"member_set_id"\s*:\s*"([0-9a-f]{32})"""".r
    .findFirstMatchIn(text).map(_.group(1)).getOrElse(
      abortRun(s"the completion manifest $path declares no member_set_id"))
  val paths = pathRe.findAllMatchIn(membersText).map(_.group(1)).toList
  val roles = roleRe.findAllMatchIn(membersText).map(_.group(1)).toList
  val sizes = bytesRe.findAllMatchIn(membersText).map(_.group(1).toInt).toList
  val shas = shaRe.findAllMatchIn(membersText).map(_.group(1)).toList
  if (paths.size != declaredCount || roles.size != declaredCount ||
    sizes.size != declaredCount || shas.size != declaredCount) {
    abortRun(s"the completion manifest $path declares $declaredCount member(s) but " +
      s"holds ${paths.size} path(s), ${roles.size} role(s), ${sizes.size} size(s) and " +
      s"${shas.size} digest(s). It is malformed and nothing may rely on it")
  }
  if (roles.distinct.size != roles.size) {
    abortRun(s"the completion manifest $path repeats a member role: " +
      s"${roles.mkString(", ")}. A role names one member of one publication")
  }
  val root = repoRootRealPath
  paths.zip(sizes).zip(shas).foreach { case ((rel, size), sha) =>
    val abs = root.map(_.resolve(rel)).getOrElse(Path.of(rel))
    if (!isRegularFileNoFollow(abs)) {
      abortRun(s"the completion manifest $path names $rel, which is not a regular " +
        "file. The published set is incomplete")
    }
    val (actualSize, actualSha) = measureFileNoFollow(abs)
    if (actualSize != size || actualSha != sha) {
      abortRun(s"$rel holds $actualSize bytes / sha256 $actualSha, not the $size " +
        s"bytes / sha256 $sha the completion manifest $path records. The set is not " +
        "one generation")
    }
  }
  val recomputed = sha256OfBytes(
    roles.zip(shas).map { case (r, s) => s"$r\u0000$s" }.mkString("\n")
      .getBytes(StandardCharsets.UTF_8)).take(32)
  if (recomputed != declaredSetId) {
    abortRun(s"the completion manifest $path records member_set_id $declaredSetId but " +
      s"its own member digests yield $recomputed, so the manifest and the set it " +
      "describes disagree")
  }

  // THE EXACT SCHEMA, where the caller states it. Counts, unique roles and member
  // digests establish that the manifest is internally consistent and matches the
  // files on disk; they do NOT establish that it describes THIS publication. A
  // one-member record left by a failed three-member generation satisfies every
  // check above, and so does a complete manifest for a different query. Only the
  // expected role set, the expected paths and the publication identity refuse
  // those, which is why a consumer that knows its schema passes it in.
  if (expectedRoles.nonEmpty) {
    val missing = expectedRoles.filterNot(roles.contains)
    val extra = roles.filterNot(expectedRoles.contains)
    if (missing.nonEmpty || extra.nonEmpty || roles.size != expectedRoles.size) {
      abortRun(s"the completion manifest $path names roles ${roles.sorted.mkString(", ")} " +
        s"where this publication requires ${expectedRoles.sorted.mkString(", ")}" +
        (if (missing.nonEmpty) s"; missing ${missing.mkString(", ")}" else "") +
        (if (extra.nonEmpty) s"; unexpected ${extra.mkString(", ")}" else "") +
        ". A manifest naming fewer members than the schema requires is exactly what " +
        "a publication that failed between its renames leaves behind, and it is " +
        "internally consistent, so only the expected role set refuses it")
    }
  }
  if (expectedPaths.nonEmpty) {
    roles.zip(paths).foreach { case (role, rel) =>
      // `rel` is what completionManifestContent wrote: the repository-relative
      // path where the repository root resolved, and the bare filename where it
      // did not. Both are accepted, because the fallback is the writer's
      // documented behaviour rather than a defect - what is refused is a path
      // that is neither, which is a record describing some other file.
      val acceptable = expectedPaths.get(role)
        .map(w => Set(w, w.substring(w.lastIndexOf('/') + 1)))
      acceptable match {
        case Some(allowed) if !allowed.contains(rel) =>
          val wanted = expectedPaths(role)
          abortRun(s"the completion manifest $path records $rel for role $role where " +
            s"this consumer expects $wanted. The record describes files other than " +
            "the ones this publication wrote")
        case None if expectedRoles.nonEmpty =>
          abortRun(s"the completion manifest $path carries role $role, which this " +
            "publication's schema does not define")
        case _ => ()
      }
    }
  }
  expectedPublicationId.foreach { wanted =>
    val declaredPub = """"publication_id"\s*:\s*"([0-9a-f]{64})"""".r
      .findFirstMatchIn(text).map(_.group(1))
    if (!declaredPub.contains(wanted)) {
      abortRun(s"the completion manifest $path records publication_id " +
        s"${declaredPub.getOrElse("none")}, not the $wanted this publication " +
        "computed, so it describes another generation")
    }
  }
  declaredCount
}

/** Minimal JSON string escaping for the completion manifest, which carries only
 *  paths, hex digests and ASCII role names - but escapes anyway, so a path
 *  containing a quote or a backslash cannot produce an unparseable manifest. */
def manifestJsonString(value: String): String = {
  val out = new StringBuilder("\"")
  value.foreach {
    case '"'  => out.append("\\\"")
    case '\\' => out.append("\\\\")
    case '\n' => out.append("\\n")
    case '\r' => out.append("\\r")
    case '\t' => out.append("\\t")
    case c if c < ' ' => out.append("\\u%04x".format(c.toInt))
    case c    => out.append(c)
  }
  out.append("\"").toString
}

/**
 * The member-set identifier: sha256 over each content member's target filename
 * and digest, in published order, truncated to 32 hex characters.
 *
 * Derived from MEMBER BYTES, which is the property `publicationId` cannot have:
 * that one is computed from the query, its source and the graph before any
 * member exists, so it identifies only that input tuple - repeatably, across
 * separate invocations - and can say nothing about whether the
 * set on disk is complete. This one changes if any member's bytes change, if a
 * member is missing, or if the members came from two different generations.
 */
def memberSetIdentifier(members: List[StagedMember]): String = {
  val material = members
    .map(m => s"${m.target.getFileName}\u0000${m.sha256}")
    .mkString("\n")
  sha256OfBytes(material.getBytes(StandardCharsets.UTF_8)).take(32)
}

/** The completion manifest's bytes, describing every content member. */
def completionManifestContent(
    members: List[StagedMember], manifestTarget: Path): String = {
  val setId = memberSetIdentifier(members)
  val root = repoRootRealPath
  def rel(p: Path): String =
    root.filter(r => p.startsWith(r)).map(r => r.relativize(p).toString)
      .getOrElse(p.getFileName.toString)
  val memberJson = members.map { m =>
    "    {\n" +
      s"      ${manifestJsonString("role")}: ${manifestJsonString(m.target.getFileName.toString)},\n" +
      s"      ${manifestJsonString("path")}: ${manifestJsonString(rel(m.target))},\n" +
      s"      ${manifestJsonString("bytes")}: ${m.byteSize},\n" +
      s"      ${manifestJsonString("sha256")}: ${manifestJsonString(m.sha256)}\n" +
      "    }"
  }.mkString(",\n")
  "{\n" +
    s"  ${manifestJsonString("schema")}: ${manifestJsonString(PUBLICATION_MANIFEST_SCHEMA)},\n" +
    s"  ${manifestJsonString("document")}: ${manifestJsonString(rel(manifestTarget))},\n" +
    s"  ${manifestJsonString("query_id")}: ${manifestJsonString(QUERY_ID)},\n" +
    s"  ${manifestJsonString("member_set_id")}: ${manifestJsonString(setId)},\n" +
    s"  ${manifestJsonString("member_set_id_derivation")}: ${manifestJsonString(
      "sha256 over each content member's target filename and sha256, NUL-separated " +
      "within a member and newline-separated between members, in published order, " +
      "truncated to 32 hex characters. Derived from member BYTES, so it changes if " +
      "any member changes, is missing, or came from another generation")},\n" +
    s"  ${manifestJsonString("member_count")}: ${members.size},\n" +
    s"  ${manifestJsonString("publication_id")}: ${manifestJsonString(
      publicationIdOfRecord.getOrElse("not established when this manifest was written"))},\n" +
    s"  ${manifestJsonString("commit_protocol")}: ${manifestJsonString(
      "Every content member is staged, fsynced and measured from the disk; this " +
      "manifest is staged after all of them; every content member is then renamed " +
      "onto its target; and this manifest is renamed LAST. Its presence is " +
      "therefore the completion signal for the set, and a member on disk " +
      "disagreeing with the digest recorded here means the set is not one " +
      "generation")},\n" +
    s"  ${manifestJsonString("required_by_consumers")}: ${manifestJsonString(
      "REQUIRED. The producer re-measures every member named here immediately " +
      "after publishing and stops the run on any disagreement, so a manifest that " +
      "nothing checks cannot occur. A downstream consumer must do the same before " +
      "treating the set as one generation")},\n" +
    s"  ${manifestJsonString("self_digest")}: ${manifestJsonString(
      "absent by construction: a manifest cannot carry its own digest. Its own " +
      "identity is published in the run record's per-file manifest")},\n" +
    s"  ${manifestJsonString("members")}: [\n$memberJson\n  ]\n" +
    "}\n"
}

/**
 * Move every staged member onto its target atomically, publish the completion
 * manifest LAST, then fsync each parent directory and verify the published set
 * against the manifest.
 *
 * The order is the whole point. Staging everything before the first rename
 * closes the window before the renames; the manifest closes the window BETWEEN
 * them, which no amount of staging can. Until the manifest is in place the set
 * on disk is not claimed to be a generation, and after it is in place every
 * member it names has been re-measured from the disk and required to agree.
 */
def publishStagedMembers(writeManifest: Boolean = true): List[StagedMember] = {
  val contentMembers = stagedMembers.toList
  if (contentMembers.size > 1) mixedGenerationRisk = true
  // A manifest is written ONLY for a set that is this query's COMPLETE declared
  // generation. That is stronger than the risk flag it replaces as the decisive
  // test: the flag was raised when a multi-member publication began, so a failure
  // BEFORE that point still left the flag down and the failure handler free to
  // write a valid one-log manifest over the previous generation's three-member
  // record. Comparing against DECLARED_MEMBER_ROLES cannot be fooled that way -
  // a one-member set is not the declared set whatever the flag says.
  val stagedRoles = contentMembers.map(_.target.getFileName.toString)
  val isCompleteGeneration =
    stagedRoles.sorted == DECLARED_MEMBER_ROLES.sorted
  val manifestMember = manifestTargetPath match {
    case Some(manifestTarget)
      if contentMembers.nonEmpty && writeManifest && isCompleteGeneration =>
      // Staged through the same machinery, so it gets the same component
      // validation, the same exclusive no-follow temporary, the same looped
      // write and the same read-back verification as every other member.
      Some(stageMember(
        manifestTarget, completionManifestContent(contentMembers, manifestTarget)))
    case _ => None
  }

  // Renamed RELATIVE TO ONE DIRECTORY DESCRIPTOR: SecureDirectoryStream.move is
  // a rename within the directory the descent validated, so neither the source
  // nor the destination name is re-resolved against a pathname whose ancestors
  // could have been replaced since.
  contentMembers.foreach { m => moveWithinPublicationDir(m.temp, m.target) }

  // The CONTENT renames are made durable BEFORE the manifest rename, not after
  // both. Ordering matters to the one reader that matters - a reader after a
  // crash. Renaming the manifest first and syncing everything afterwards can
  // persist the commit record while a content rename is still only in the page
  // cache, so recovery would find a manifest asserting members that are not
  // there. Syncing content first makes the manifest's presence imply theirs.
  fsyncPublicationDirs(contentMembers.map(_.target.getParent).distinct)

  // LAST. Everything the manifest describes is already at its published path
  // AND durable there.
  manifestMember.foreach { m =>
    moveWithinPublicationDir(m.temp, m.target)
    fsyncPublicationDirs(List(m.target.getParent))
  }

  val members = contentMembers ++ manifestMember.toList
  stagedMembers.clear()

  // THE PRODUCER IS THE FIRST CONSUMER. Every member the manifest names is
  // re-measured from its published path and required to equal the digest the
  // manifest records. A manifest nothing verifies is decoration.
  manifestMember match {
    case Some(m) =>
      // The manifest's own BYTES are re-read from the published path and its
      // recorded members are re-measured from theirs. Comparing the staged
      // in-memory values would verify what this process believed it wrote; the
      // point of a commit record is to be checkable by something that was not
      // here, so it is checked the way such a reader would check it.
      val required = requirePublicationManifest(
        m.target, DECLARED_MEMBER_ROLES, DECLARED_MEMBER_PATHS, publicationIdOfRecord)
      if (required != contentMembers.size) {
        abortRun(s"the completion manifest ${m.target} names $required member(s) but " +
          s"${contentMembers.size} were published. The set on disk is not one generation")
      }
      val setId = memberSetIdentifier(contentMembers)
      lastPublicationMemberSetId = Some(setId)
      lastPublicationManifest = Some(m.target.getFileName.toString)
      mixedGenerationRisk = false
      println(s"completion manifest published last: ${m.target} " +
        s"(member_set_id $setId, $required member(s) re-read from disk and verified)")
    case None =>
      lastPublicationMemberSetId = None
      lastPublicationManifest = None
      if (!writeManifest || !isCompleteGeneration) {
        System.err.println("the console log was published WITHOUT a completion " +
          "manifest, deliberately: a multi-member publication had already begun " +
          "renaming when it failed, and writing a one-member manifest over that set " +
          "would assert a complete generation of one file while the other members " +
          "sit at whichever generation the failure left them in. Leaving the manifest " +
          "absent, or stale from the previous generation, is what lets a consumer " +
          "detect the incomplete set: a stale manifest names these same paths with " +
          "the PREVIOUS generation's digests, so re-measuring them fails it, and " +
          "where no content member was renamed at all the previous generation is " +
          "still internally consistent and accepting it is correct")
      } else if (contentMembers.size > 1) {
        // Never reached on the paths this query takes - the only manifest-less
        // publication is the single-member early-abort console log - but stated
        // rather than assumed, because a multi-member set with no commit record
        // is the exact hazard the manifest exists to close.
        System.err.println(s"published ${contentMembers.size} members with no " +
          "completion manifest: the output paths were not resolved, so a consumer " +
          "cannot establish that the set is one generation")
      }
  }
  members
}

/** Streaming sha256 so a 500 MB graph is never held in memory. */
def sha256Of(p: Path): String = {
  val md = MessageDigest.getInstance("SHA-256")
  val in = Files.newInputStream(p)
  try {
    val buf = new Array[Byte](1 << 20)
    var n = in.read(buf)
    while (n > 0) {
      md.update(buf, 0, n)
      n = in.read(buf)
    }
  } finally in.close()
  md.digest().map("%02x".format(_)).mkString
}

/** sha256 of bytes already in memory - the members this query publishes, and
 *  the derivation of the publication identifier. */
def sha256OfBytes(bytes: Array[Byte]): String =
  MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString

// -------------------------------------------------- bounded materialization
/**
 * Every traversal this query materializes goes through here, and each one is
 * capped and RECORDED.
 *
 * WHY A HELPER RATHER THAN A `take` AT EACH SITE. A cap applied at one site and
 * omitted at the next produces exactly the state this replaced: a query that
 * publishes named bounds and a claim that nothing runs unbounded, while two of
 * its sweeps walk every call site in a 1.4 M-method graph. Routing every
 * materialization through one function makes the cap unavoidable and makes the
 * evidence uniform: each sweep contributes an entry naming the cap that governs
 * it, the count it observed, and whether the cap bound the result.
 *
 * HOW TRUNCATION IS DETECTED. `cap + 1` elements are taken. Observing more than
 * `cap` means the traversal had further elements to give, so the sweep is
 * truncated and the retained list is cut back to `cap`. Taking exactly `cap` and
 * comparing sizes cannot distinguish "exactly cap elements exist" from "the cap
 * bound it", and the difference is the whole point of the flag.
 */
final case class BoundedSweep(
    label: String,
    capName: String,
    cap: Int,
    observed: Int,
    truncated: Boolean,
    basis: String)

val boundedSweeps = scala.collection.mutable.ArrayBuffer.empty[BoundedSweep]

def boundedList[A](label: String, capName: String, cap: Int, basis: String)(
    it: => Iterator[A]): List[A] = {
  val taken = it.take(cap + 1).toList
  val truncated = taken.size > cap
  val kept = if (truncated) taken.take(cap) else taken
  boundedSweeps += BoundedSweep(label, capName, cap, kept.size, truncated, basis)
  kept
}

/** True when any sweep governed by the named cap was truncated. This is what the
 *  published `bounds_reached` entry for that cap is derived from, so a cap
 *  governing several sweeps cannot report the state of only one of them. */
def sweepCapReached(capName: String): Boolean =
  boundedSweeps.exists(s => s.capName == capName && s.truncated)

/** Every sweep governed by the named cap, for that cap's published basis. */
def sweepsGovernedBy(capName: String): List[BoundedSweep] =
  boundedSweeps.toList.filter(_.capName == capName)

/** The per-cap basis sentence: every governed sweep, its observed count and its
 *  own truncation flag, so a combined reached flag can still be read back to the
 *  sweep that set it. */
def sweepBasisFor(capName: String): String = {
  val governed = sweepsGovernedBy(capName)
  if (governed.isEmpty) "no sweep governed by this cap ran"
  else governed.map(s =>
    s"${s.label} observed ${s.observed} of ${s.cap} (truncated=${s.truncated})")
    .mkString("; ")
}

/** Occurrences of a literal token in a text. Used to MEASURE the absence of the
 *  alternative loader from this query's own source rather than assert it. */
def occurrencesOf(text: String, token: String): Int = {
  var count = 0
  var at = text.indexOf(token)
  while (at >= 0) {
    count += 1
    at = text.indexOf(token, at + token.length)
  }
  count
}

// ------------------------------------------- reading a query source as data
// The duplicate-formulation comparison in stage J is a statement about two
// SOURCES, so it reads them as data. These three helpers are the whole of that
// reading: they find a top-level `val NAME` declaration, and return the literal
// text it is built from. Literal TEXT is what is compared, never a runtime
// string, so an escape sequence written the same way in two sources compares
// equal without either side being unescaped first.

/** The text of a top-level `val NAME` declaration: its own line, plus every
 *  following line that is indented (a continuation) and non-blank. `None` when
 *  the source declares no such val. */
def declarationTextOf(text: String, name: String): Option[String] = {
  val header = ("(?m)^val " + java.util.regex.Pattern.quote(name) + "(?![A-Za-z0-9_])").r
  header.findFirstMatchIn(text).map { m =>
    val rest = text.substring(m.start)
    val lines = rest.split("\n", -1)
    val out = scala.collection.mutable.ArrayBuffer(lines.head)
    var i = 1
    var running = true
    while (running && i < lines.length) {
      val line = lines(i)
      if (line.isEmpty || !line.head.isWhitespace) running = false
      else {
        out += line
        i += 1
      }
    }
    out.mkString("\n")
  }
}

/** Every string literal in a fragment of Scala source, in order, as written -
 *  triple-quoted literals contribute their body, ordinary ones keep their
 *  escape sequences exactly as the source spells them. */
def stringLiteralsIn(fragment: String): List[String] = {
  val out = scala.collection.mutable.ArrayBuffer.empty[String]
  var i = 0
  while (i < fragment.length) {
    if (fragment.startsWith("\"\"\"", i)) {
      val end = fragment.indexOf("\"\"\"", i + 3)
      if (end < 0) i = fragment.length
      else {
        out += fragment.substring(i + 3, end)
        i = end + 3
      }
    } else if (fragment.charAt(i) == '"') {
      val sb = new StringBuilder
      var j = i + 1
      var closed = false
      while (!closed && j < fragment.length) {
        val c = fragment.charAt(j)
        if (c == '\\' && j + 1 < fragment.length) {
          sb.append(c).append(fragment.charAt(j + 1))
          j += 2
        } else if (c == '"') {
          closed = true
          j += 1
        } else {
          sb.append(c)
          j += 1
        }
      }
      out += sb.toString
      i = j
    } else i += 1
  }
  out.toList
}

/** The string literals a named declaration is built from: one entry for a
 *  single literal, one per element for a List of literals, and the pieces in
 *  order for a concatenation. `None` when the declaration is absent. */
def declaredLiteralsOf(text: String, name: String): Option[List[String]] =
  declarationTextOf(text, name).map(stringLiteralsIn)

/** The first integer literal in a named declaration. */
def declaredIntOf(text: String, name: String): Option[Long] =
  declarationTextOf(text, name).flatMap(d => """(-?\d+)""".r.findFirstIn(d).map(_.toLong))

// ----------------------------------------------- the revision count, measured
/**
 * The commits touching one repository-relative path, newest first, read from
 * the repository's own history rather than written down. Returns whether the
 * measurement was established, a note that says why when it was not, and the
 * commit identifiers themselves so the count is auditable from the list.
 *
 * Three properties make this safe to run from inside a probe: the child gets no
 * shell, its stdin is closed immediately so it can never wait for input, and
 * the wait is bounded - a child that has not exited within the bound is
 * destroyed and the measurement is reported as not established rather than
 * being guessed at. Git's output for one path is a few dozen bytes per commit,
 * comfortably inside the pipe buffer; a path with more history than fits would
 * block before exiting and so be reported as not established, never
 * under-counted.
 */
/** The approved absolute git executable, resolved once and validated in its own
 *  right: absolute, a regular file, executable, and identifying itself as git
 *  within GIT_WAIT_SECONDS. `None` means no candidate qualified, which makes a
 *  revision measurement NOT ESTABLISHED rather than unverified. */
lazy val approvedGitExecutable: Option[String] = {
  var chosen: Option[String] = None
  GIT_EXECUTABLE_CANDIDATES.foreach { candidate =>
    if (chosen.isEmpty) {
      val path = java.nio.file.Path.of(candidate)
      if (!path.isAbsolute) {
        gitExecutableRefusals += s"$candidate: not an absolute path"
      } else if (!java.nio.file.Files.isRegularFile(path)) {
        gitExecutableRefusals += s"$candidate: not an existing regular file"
      } else if (!java.nio.file.Files.isExecutable(path)) {
        gitExecutableRefusals += s"$candidate: not executable"
      } else {
        val argv = new java.util.ArrayList[String]()
        List(candidate, "--version").foreach(argv.add)
        try {
          val builder = new java.lang.ProcessBuilder(argv)
          builder.redirectErrorStream(true)
          val proc = builder.start()
          proc.getOutputStream.close()
          val exited = proc.waitFor(GIT_WAIT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
          if (!exited) {
            proc.destroyForcibly()
            gitExecutableRefusals += s"$candidate: did not exit within $GIT_WAIT_SECONDS seconds"
          } else {
            val out = new String(proc.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
            if (proc.exitValue() != 0) {
              gitExecutableRefusals += s"$candidate: exited ${proc.exitValue()}"
            } else if (!out.trim.startsWith(GIT_VERSION_PREFIX)) {
              gitExecutableRefusals += s"$candidate: did not identify itself as git"
            } else {
              chosen = Some(candidate)
            }
          }
        } catch {
          case t: Throwable =>
            gitExecutableRefusals += s"$candidate: ${t.getClass.getName}"
        }
      }
    }
  }
  chosen
}

def gitRun(root: Path, args: List[String]): (Boolean, Int, String) = {
  val executable = approvedGitExecutable
  if (executable.isEmpty) return (false, -3, "")
  val argv = new java.util.ArrayList[String]()
  (List(executable.get, "-C", root.toString) ::: args).foreach(argv.add)
  try {
    val builder = new java.lang.ProcessBuilder(argv)
    builder.redirectErrorStream(true)
    val proc = builder.start()
    proc.getOutputStream.close()
    val exited = proc.waitFor(GIT_WAIT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
    if (!exited) {
      proc.destroyForcibly()
      (false, -1, "")
    } else {
      val out = new String(proc.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      (true, proc.exitValue(), out)
    }
  } catch {
    case t: Throwable => (false, -2, s"${t.getClass.getName}: ${t.getMessage}")
  }
}

/** The commit HEAD names, and the branch it is on, so the window a revision
 *  count was taken in is published rather than left implicit. */
def gitHeadOf(root: Path): (Option[String], Option[String]) = {
  val (ranSha, codeSha, outSha) = gitRun(root, List("rev-parse", "HEAD"))
  val head =
    if (ranSha && codeSha == 0) outSha.linesIterator.map(_.trim)
      .find(_.matches(GIT_COMMIT_SHA_REGEX))
    else None
  val (ranRef, codeRef, outRef) = gitRun(root, List("rev-parse", "--abbrev-ref", "HEAD"))
  val branch =
    if (ranRef && codeRef == 0) outRef.linesIterator.map(_.trim).find(_.nonEmpty)
    else None
  (head, branch)
}

/** True when `commit` is an ancestor of, or equal to, `HEAD`. */
def gitIsAncestorOfHead(root: Path, commit: String): Boolean = {
  val (ran, code, _) = gitRun(root, List("merge-base", "--is-ancestor", commit, "HEAD"))
  ran && code == 0
}

def gitRevisionsOf(root: Path, repoRelativePath: String): (Boolean, String, List[String]) = {
  val argv = new java.util.ArrayList[String]()
  // HEAD is named EXPLICITLY as the revision range. `git log -- <path>` already
  // walks from HEAD, but naming it makes the window the count was taken in a
  // published property of the measurement rather than a default a reader has to
  // know: this query publishes the HEAD it measured at, and every commit it
  // returns is then required to be an ancestor of that HEAD. A count that
  // included a commit reachable only from another ref would be a count nobody
  // could reproduce from this branch - which is exactly what happened when
  // per-clone branches were reconciled and the commits a previous run had listed
  // stopped being ancestors of the branch that carries its files.
  val executable = approvedGitExecutable
  if (executable.isEmpty) {
    return (false,
      s"not established: no candidate qualified as $GIT_EXECUTABLE_LABEL, so no " +
        "program was invoked; a count is not taken from an unverified executable",
      Nil)
  }
  List(executable.get, "-C", root.toString, "log", "--format=%H", "HEAD", "--",
    repoRelativePath).foreach(argv.add)
  try {
    val builder = new java.lang.ProcessBuilder(argv)
    builder.redirectErrorStream(true)
    val proc = builder.start()
    proc.getOutputStream.close()
    val exited = proc.waitFor(GIT_WAIT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
    if (!exited) {
      proc.destroyForcibly()
      (false,
        s"not established: $GIT_EXECUTABLE_LABEL did not exit within $GIT_WAIT_SECONDS " +
          "seconds and was destroyed; a count is not guessed at when the measurement " +
          "did not complete",
        Nil)
    } else {
      val out = new String(proc.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      val code = proc.exitValue()
      if (code != 0) {
        val quoted = out.linesIterator.map(_.trim).filter(_.nonEmpty)
          .take(GIT_OUTPUT_LINES_REPORTED).mkString(" / ")
        (false,
          s"not established: $GIT_EXECUTABLE_LABEL exited $code for $repoRelativePath" +
            (if (quoted.isEmpty) "" else s", saying: $quoted"),
          Nil)
      } else {
        val commits = out.linesIterator.map(_.trim)
          .filter(_.matches(GIT_COMMIT_SHA_REGEX)).toList
        // Every returned commit is required to be an ancestor of HEAD. `git log
        // HEAD -- <path>` cannot return anything else, so this is a check on the
        // measurement rather than a filter on the data: a non-ancestor here would
        // mean the range was not the one this query asked for, and a count taken
        // over a range nobody can name is not auditable.
        val nonAncestors = commits.filterNot(c => gitIsAncestorOfHead(root, c))
        if (nonAncestors.nonEmpty) {
          (false,
            s"not established: ${nonAncestors.size} of ${commits.size} commit(s) " +
              s"returned for $repoRelativePath are not ancestors of HEAD " +
              s"(${nonAncestors.map(_.take(11)).mkString(", ")}), so the range walked was " +
              "not the branch history this convention counts over",
            Nil)
        } else {
          (true,
            "commits touching this path in HEAD's own history, newest first, every one " +
              "verified an ancestor of the HEAD published beside this count",
            commits)
        }
      }
    }
  } catch {
    case t: Throwable =>
      (false,
        s"not established: ${t.getClass.getName}: ${t.getMessage}",
        Nil)
  }
}

/**
 * Write the console log wherever the run got to - success or failure - through
 * the same staging machinery as every other member.
 *
 * On the FAILURE path the console log is the only member that publishes: any
 * envelope or report already staged is discarded first, so the previous
 * generation of those two files is left intact rather than half replaced. On
 * the success path the console log is staged last, as the third member of the
 * set, because its content names the other two.
 */
var logTargetPath: Option[Path] = None
var publicationCompleted = false
def flushConsoleLog(): Unit =
  if (!publicationCompleted) logTargetPath.foreach { p =>
    try {
      // The FAILED-run marker shape is validated HERE, on the exact lines about to
      // be written, for the same reason the completed shape is validated before its
      // own staging - and here rather than at each call site because this is the
      // single publication point every failure path reaches, so no caller can
      // bypass it. It fails CLOSED. A failure raised after the result region had
      // already been emitted leaves a stream carrying RESULT-BEGIN, RESULT-END and
      // OK alongside FAILURE, which is exactly the combination
      // validateMarkerProtocol defines as forbidden for a failed run; publishing it
      // would hand a consumer a success-shaped result region under a failure
      // marker. The throw is caught by this method's own handler, which discards
      // every staged member and names the violation on stderr, where the catch
      // block has already written the failing stage and the exception. Nothing is
      // published in that case, and that is the intended outcome: no log is better
      // than a log whose shape misreports how the run ended.
      val failedRunMarkers = validateMarkerProtocol(consoleLines.toList, expectedOk = false)
      discardStagedMembers()
      stageMember(p, consoleLines.mkString("", "\n", "\n"))
      // A manifest is written here ONLY if no multi-member publication had begun
      // renaming. Writing one otherwise would replace a three-member commit
      // record with a valid one-member record, and a generic consumer checking
      // that manifest would then find a complete, self-consistent set of one
      // file - the omitted envelope and report invisible to it. That is the
      // failure the manifest exists to make visible, so it must not be papered
      // over by the failure handler itself.
      publishStagedMembers(writeManifest = !mixedGenerationRisk)
      publicationCompleted = true
      println("marker protocol           : " +
        failedRunMarkers.map { case (t, n) => s"$t=$n" }.mkString(" ") +
        " (validated, failed-run shape)")
      println(s"console log written: $p")
    } catch {
      case t: Throwable =>
        discardStagedMembers()
        System.err.println(
          s"NOT PUBLISHED - the console log was not written to $p: ${t.getMessage}")
    }
  }

// ===========================================================================
// THE QUERY. Everything below runs inside one try so that a failure names its
// stage, re-raises, and emits no result region.
// ===========================================================================

val runStartNanos = System.nanoTime()
logMarker(MARKER_START)

try {

  // -------------------------------------------------------------------------
  stage("A-paths-and-runtime: repository root, console log target, JDK major " +
    "and the heap ACTUALLY used")
  // -------------------------------------------------------------------------
  // The root and the log target are resolved FIRST, before any check that can
  // stop the run, so that every later failure leaves its console record on disk
  // at the declared path rather than only on the terminal.
  val repoRootEnv = sys.env.get(REPO_ROOT_ENV_VAR).filter(_.nonEmpty)
  val repoRoot: Path = repoRootEnv match {
    case Some(v) => Paths.get(v).toAbsolutePath.normalize
    case None    => Paths.get("").toAbsolutePath.normalize
  }
  val repoRootSource =
    if (repoRootEnv.isDefined) REPO_ROOT_ENV_VAR else "the process working directory"
  log(s"repository root           : $repoRoot (from $repoRootSource)")
  val cpgAapNamed = repoRoot.resolve(CPG_PATH_DEFAULT).toAbsolutePath.normalize
  if (!Files.isDirectory(cpgAapNamed.getParent)) {
    abortRun(s"the resolved repository root does not contain the directory of " +
      s"$CPG_PATH_DEFAULT: $repoRoot. Set $REPO_ROOT_ENV_VAR, or invoke from the " +
      "repository root")
  }
  logTargetPath = Some(repoRoot.resolve(LOG_DIR).resolve(s"probe-$QUERY_ID.log"))
  manifestTargetPath =
    Some(repoRoot.resolve(LOG_DIR).resolve(s"probe-$QUERY_ID.publication.json"))
  log(s"completion manifest       : ${manifestTargetPath.get}")
  log(s"console log target        : ${logTargetPath.get}")
  // The real path of the publication root, resolved once. Every member this
  // query publishes must land inside it, and the check is made against the
  // REAL path so a symlinked component cannot place a member elsewhere.
  repoRootRealPath = Some(repoRoot.toRealPath())
  log(s"publication root (real)   : ${repoRootRealPath.get}")

  val jdkMajor = System.getProperty("java.specification.version")
  val jvmVersion = System.getProperty("java.vm.version")
  val heapMaxBytes = Runtime.getRuntime.maxMemory()
  val jvmInputArgs = {
    import scala.jdk.CollectionConverters._
    java.lang.management.ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toList
  }
  log(s"jdk.specification.version : $jdkMajor (required major $REQUIRED_JDK_MAJOR)")
  log(s"java.vm.version           : $jvmVersion")
  log(s"heap actually used (bytes): $heapMaxBytes")
  log(f"heap actually used (GiB)  : ${heapMaxBytes.toDouble / (1024L * 1024L * 1024L)}%.3f")
  log(s"heap floor (bytes)        : $HEAP_FLOOR_BYTES")
  // CWE-532. The JVM's raw input arguments are NOT logged: a -D property can
  // carry a token or a password, and these console streams are preserved
  // verbatim as evidence. Only a whitelisted memory or stack flag is logged as
  // written - the heap it establishes is the evidence this record needs - and
  // every other argument is reduced to its key with its value replaced by a
  // fixed token. The count reduced is reported, so the reduction is visible.
  val jvmArgsKept = jvmInputArgs.filter(jvmArgumentValueIsWhitelisted)
  val jvmArgsRedacted = jvmInputArgs.filterNot(jvmArgumentValueIsWhitelisted)
  log(s"JVM memory/stack args     : " +
    (if (jvmArgsKept.isEmpty) "<none>" else jvmArgsKept.mkString(" ")))
  log(s"JVM other args (reduced)  : " +
    (if (jvmArgsRedacted.isEmpty) "<none>"
     else jvmArgsRedacted.map(redactJvmArgument).mkString(" ")))
  log(s"JVM args kept / reduced   : ${jvmArgsKept.size} logged as written, " +
    s"${jvmArgsRedacted.size} reduced to their keys, ${jvmInputArgs.size} observed")
  log("Joern exposes no --version flag and its REPL blocks on an open stdin, so its")
  log("version is read from the startup banner on the console stream, not from a flag.")

  if (jdkMajor != REQUIRED_JDK_MAJOR) {
    abortRun(s"the pinned Joern release documents JDK major $REQUIRED_JDK_MAJOR as its " +
      s"tested requirement and this JVM reports major $jdkMajor; a Joern process on " +
      "another major is a wrong assignment")
  }
  if (heapMaxBytes < HEAP_FLOOR_BYTES) {
    abortRun(s"the heap ACTUALLY available to this query is $heapMaxBytes bytes, below " +
      s"the floor of $HEAP_FLOOR_BYTES bytes (64 GiB). joern --script forks a child JVM " +
      "and does NOT forward -J-Xmx to it: set JAVA_TOOL_OPTIONS=-Xmx64g (or larger) so " +
      "the override is inherited. Raising a heap is permitted and reported; lowering one " +
      "is not, because a truncated result's silence cannot be told apart from a clean " +
      "one - and this query traverses two pairs, so it would be silent twice")
  }
  log("heap floor                : PASS (measured, not requested)")

  // ------------------------- the source of record, digested -----------------
  // The file that wrote a result must be identifiable FROM that result, or a
  // result and the source it claims to come from can drift apart with nothing
  // recording it. So this query reads its own source at run time, digests it,
  // and stamps that digest into every member it publishes. Three checks make
  // the digest mean what it says:
  //
  //   - the file read must declare THIS query's id, so a digest of some other
  //     file cannot be published as this one's;
  //   - the alternative loader's absence is MEASURED in that text rather than
  //     asserted, with the token assembled at run time so this check does not
  //     itself put the token in the source it is checking;
  //   - the declared formulation bound is compared against the bound the
  //     traversal actually runs at, so the declaration cannot drift from
  //     behaviour.
  val sourceRepoRelative = s"$QUERY_SOURCE_DIR/$QUERY_ID.sc"
  val sourcePath = repoRoot.resolve(sourceRepoRelative)
  if (!Files.isRegularFile(sourcePath)) {
    abortRun(s"this query's own source is not a regular file at $sourcePath. The " +
      "digest that ties every published member to the source that wrote it cannot be " +
      "computed, so nothing is published")
  }
  val sourceBytes = Files.readAllBytes(sourcePath)
  val sourceText = new String(sourceBytes, StandardCharsets.UTF_8)
  val sourceSha256 = sha256OfBytes(sourceBytes)
  val sourceByteSize = sourceBytes.length.toLong
  val selfIdentification = "val QUERY_ID = \"" + QUERY_ID + "\""
  if (!sourceText.contains(selfIdentification)) {
    abortRun(s"the file read at $sourceRepoRelative does not declare $selfIdentification, " +
      "so it is not this query's source and its digest must not be published as one")
  }
  val alternativeLoaderToken = "import" + "Code"
  val alternativeLoaderOccurrences = occurrencesOf(sourceText, alternativeLoaderToken)
  if (alternativeLoaderOccurrences > 0) {
    abortRun(s"the alternative loader appears $alternativeLoaderOccurrences time(s) in " +
      s"$sourceRepoRelative. The probe loads with importCpg ONLY, and the token's " +
      "absence from every committed query source is a checked contract rather than a " +
      "convention")
  }
  val declaredBoundValue = declaredIntOf(sourceText, "FORMULATION_BOUND_VALUE")
  if (!declaredBoundValue.contains(MAX_CALL_DEPTH.toLong)) {
    abortRun("the formulation identity block declares FORMULATION_BOUND_VALUE=" +
      declaredBoundValue.map(_.toString).getOrElse("<absent>") + " while the traversal " +
      s"runs at $FORMULATION_BOUND_NAME=$MAX_CALL_DEPTH. The declared block is what the " +
      "duplicate-formulation comparison reads, so a declaration that has drifted from " +
      "the behaviour it describes would make that comparison wrong")
  }
  log(s"query source              : $sourceRepoRelative")
  log(s"query source bytes        : $sourceByteSize")
  log(s"query source sha256       : $sourceSha256")
  log(s"alternative loader        : absent (measured: $alternativeLoaderOccurrences " +
    "occurrences in the source text)")
  log(s"declared bound            : $FORMULATION_BOUND_NAME=" +
    s"${declaredBoundValue.map(_.toString).getOrElse("<absent>")} (agrees with the " +
    "traversal)")
  // The per-pair step budget must sit ABOVE the per-entry expansion cap times
  // the per-pair entry-point cap, so that a pair's own budget can never
  // pre-empt an entry point's allowance before that allowance is spent. The
  // relation is asserted rather than left as a coincidence of three literals.
  if (MAX_STEPS_PER_PAIR < MAX_EXPANSIONS_PER_ENTRY) {
    abortRun(s"MAX_STEPS_PER_PAIR=$MAX_STEPS_PER_PAIR is below " +
      s"MAX_EXPANSIONS_PER_ENTRY ($MAX_EXPANSIONS_PER_ENTRY). The two scopes are " +
      "published separately and the pair budget is documented as sitting above the " +
      "per-entry cap, so a pair budget below it would silently pre-empt the per-entry " +
      "cap it is meant to sit above")
  }
  log(s"step scopes               : per entry point $MAX_EXPANSIONS_PER_ENTRY " +
    s"expansions, per pair $MAX_STEPS_PER_PAIR call sites, " +
    s"$MAX_ENTRY_POINTS_PER_PAIR entry points per pair")

  // Effort measure 1, measured rather than declared: the commits touching this
  // source. The list is published beside the count so the number is auditable.
  val (revisionsEstablished, revisionsNote, revisionCommits) =
    gitRevisionsOf(repoRoot, sourceRepoRelative)
  val (revisionHead, revisionBranch) = gitHeadOf(repoRoot)
  log(s"revision window HEAD      : ${revisionHead.getOrElse("not established")}" +
    revisionBranch.map(b => s" on $b").getOrElse(""))
  log(s"query revisions committed : " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established") +
    s" ($revisionsNote)")
  if (revisionsEstablished && revisionCommits.nonEmpty) {
    log(s"query revision commits    : ${revisionCommits.mkString(", ")}")
  }
  log(s"revision convention       : $QUERY_REVISIONS_CONVENTION")


  // -------------------------------------------------------------------------
  stage("B-graph-path: the two names for the graph")
  // -------------------------------------------------------------------------
  val cpgEnvValue = sys.env.get(CPG_ENV_VAR).filter(_.nonEmpty)
  val cpgPathSource =
    if (cpgEnvValue.isDefined) CPG_ENV_VAR else s"repo-relative default $CPG_PATH_DEFAULT"
  val cpgNamed =
    Paths.get(cpgEnvValue.getOrElse(cpgAapNamed.toString)).toAbsolutePath.normalize
  log(s"graph path source         : $cpgPathSource")
  log(s"graph path (as used)      : $cpgNamed")
  log(s"graph path (AAP name)     : $cpgAapNamed")

  if (!Files.exists(cpgNamed, LinkOption.NOFOLLOW_LINKS)) {
    abortRun(s"code-property graph not found at $cpgNamed (resolved from $cpgPathSource). " +
      "harness/bin/run-joern.sh guards the same input and exits 78 naming the missing " +
      "graph; that is a configuration fault to fix, never a scanning outcome")
  }
  if (!Files.exists(cpgNamed)) {
    abortRun(s"$cpgNamed exists as a link whose target does not resolve to a file: " +
      s"${Files.readSymbolicLink(cpgNamed)}")
  }

  // -------------------------------------------------------------------------
  stage("C-identity: the graph's byte size and sha256, symlink-FOLLOWING")
  // -------------------------------------------------------------------------
  // harness/cpg/spark.cpg is git-tracked while harness/artifacts/** is ignored,
  // which is why the provisioned path is a small SYMLINK to a host-global graph.
  // Measuring the link instead of its target would record a few dozen bytes and
  // the comparison would fail spuriously, so the link is resolved first and the
  // TARGET is what is sized and hashed.
  val identityNanos = System.nanoTime()
  val cpgIsLink = Files.isSymbolicLink(cpgNamed)
  val cpgLinkTarget = if (cpgIsLink) Files.readSymbolicLink(cpgNamed).toString else ""
  val cpgResolved = cpgNamed.toRealPath()
  val sizeNoFollow = Files
    .readAttributes(cpgNamed, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
    .size()
  val sizeFollow = Files.size(cpgNamed)
  log(s"named path is a symlink   : $cpgIsLink" +
    (if (cpgIsLink) s" -> $cpgLinkTarget" else ""))
  log(s"resolved target           : $cpgResolved")
  log(s"size WITHOUT following    : $sizeNoFollow  (recorded to be discarded)")
  log(s"size WITH following       : $sizeFollow  (the measurement of record)")

  // ---------------- the private, immutable input this load actually reads -----
  // A digest taken from the shared graph and a later importCpg of that same path
  // are two separate opens of one name. Between them the name can resolve
  // elsewhere - the graph is a host-shared file reached through a symlink, shared
  // with every other clone on this host - so the pair the identity check compared
  // would not be the pair the engine read. That is the whole defect, and copying
  // is what closes it:
  //
  //   - the bytes are copied ONCE into a directory this query creates itself,
  //     with a random name and mode 0700, under the clone-private scratch root;
  //   - the sha256 is computed FROM THE BYTES BEING COPIED, in the same pass, so
  //     the digest is of the copy's contents by construction rather than by a
  //     second read of anything;
  //   - the copy is then made read-only and its directory non-writable, so this
  //     process cannot modify it either;
  //   - importCpg is given THAT path and no other, and BOTH pairs are measured
  //     against the one graph it loaded;
  //   - after the load the copy's size, digest and INODE are re-measured and
  //     required unchanged, which detects a swap across the load window rather
  //     than assuming one cannot happen.
  //
  // The residual limit is stated rather than papered over: importCpg accepts a
  // path, so the engine's own open is by name. What the steps above establish is
  // that the name is one this run created, in a directory only this run can
  // write, holding bytes whose digest was taken as they were written, and whose
  // identity is re-verified after the engine has finished with it.
  val scratchRoot = sys.env.get(SCRATCH_ROOT_ENV_VAR).filter(_.nonEmpty)
    .map(v => Paths.get(v).toAbsolutePath.normalize)
    .getOrElse(Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath.normalize)
  val scratchRootSource =
    if (sys.env.get(SCRATCH_ROOT_ENV_VAR).exists(_.nonEmpty)) "$" + SCRATCH_ROOT_ENV_VAR
    else "java.io.tmpdir, because $" + SCRATCH_ROOT_ENV_VAR + " is unset"
  Files.createDirectories(scratchRoot)
  val privateInputSuffix = {
    val raw = new Array[Byte](CPG_PRIVATE_INPUT_RANDOM_BYTES)
    publicationRandom.nextBytes(raw)
    raw.map("%02x".format(_)).mkString
  }
  val privateInputDir = scratchRoot.resolve(CPG_PRIVATE_INPUT_DIR_PREFIX + privateInputSuffix)
  // createDirectory rather than createDirectories, with the owner-only mode set
  // AT CREATION: an existing name fails here rather than being reused, which is
  // what makes the directory this run's own.
  Files.createDirectory(privateInputDir, java.nio.file.attribute.PosixFilePermissions
    .asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------")))
  val privateInput = privateInputDir.resolve(CPG_PRIVATE_INPUT_FILENAME)
  val copyNanos = System.nanoTime()
  val copyDigest = MessageDigest.getInstance("SHA-256")
  var copiedBytes = 0L
  val readChannel = java.nio.channels.FileChannel.open(
    cpgResolved, java.nio.file.StandardOpenOption.READ)
  try {
    val writeChannel = java.nio.channels.FileChannel.open(
      privateInput,
      java.nio.file.StandardOpenOption.CREATE_NEW,
      java.nio.file.StandardOpenOption.WRITE,
      LinkOption.NOFOLLOW_LINKS)
    try {
      val buffer = java.nio.ByteBuffer.allocate(CPG_COPY_CHUNK_BYTES)
      var read = readChannel.read(buffer)
      while (read > 0) {
        buffer.flip()
        // The digest sees exactly the bytes the write sees, from one buffer, so
        // "the digest is of what was copied" needs no second read to be true.
        copyDigest.update(buffer.duplicate())
        while (buffer.hasRemaining) {
          val written = writeChannel.write(buffer)
          if (written <= 0 && buffer.hasRemaining) {
            abortRun(s"the private graph copy stopped accepting bytes with " +
              s"${buffer.remaining} of this chunk still to write")
          }
        }
        copiedBytes += read
        buffer.clear()
        read = readChannel.read(buffer)
      }
      writeChannel.force(true)
    } finally writeChannel.close()
  } finally readChannel.close()
  val shaObserved = copyDigest.digest().map("%02x".format(_)).mkString
  // Read-only, and its directory non-writable, so nothing - including this
  // process - rewrites the input between the check and the load.
  Files.setPosixFilePermissions(privateInput,
    java.nio.file.attribute.PosixFilePermissions.fromString("r--------"))
  Files.setPosixFilePermissions(privateInputDir,
    java.nio.file.attribute.PosixFilePermissions.fromString("r-x------"))
  val privateInputAttributes =
    Files.readAttributes(privateInput, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
  val privateInputInode = String.valueOf(privateInputAttributes.fileKey)
  val privateInputSize = privateInputAttributes.size()
  log(s"private input dir source  : $scratchRootSource")
  log(s"private input (created)   : $privateInput")
  log(s"private input mode        : 0400 in a 0500 directory this run created")
  log(s"bytes copied              : $copiedBytes in ${elapsedMs(copyNanos)} ms")
  log(s"private input size        : $privateInputSize")
  log(s"private input inode       : $privateInputInode")
  log(s"sha256 of the copied bytes: $shaObserved  (taken in the copy pass)")
  if (privateInputSize != copiedBytes || privateInputSize != sizeFollow) {
    abortRun(s"the private graph copy holds $privateInputSize bytes against " +
      s"$copiedBytes copied and $sizeFollow at the source, so the copy is not the graph")
  }

  // ------------------------ the record of account, resolved by PROVENANCE -----
  // One record adjudicates this load, and which record that is follows from WHO
  // WROTE THE BYTES rather than from which candidate happens to match. The order
  // is the one harness/lib/preflight_graph_identity.py uses for the Stage 3 gate,
  // so the probe and the gate cannot disagree about what a load is checked
  // against, and all three probe queries resolve it the same way. Every candidate
  // that exists is read; ambiguity inside one record and disagreement between two
  // are both fatal. There is still no environment override, so the record a
  // reader can reach from the published command is exactly the record this
  // comparison turned on.
  final case class IdentityCandidate(
      label: String, provenance: String, size: Long, sha256: String)

  /** The strict write-time pair: `bytes: <n>` and `sha256: <64 hex>`, each on its
   *  own line, lower-case keys, no space before the colon. Exactly this shape,
   *  because it is the shape the frontend writes when it has written a graph -
   *  and a looser reader would accept a size printed in a sentence somewhere in a
   *  7 MB log. `None` means the record carries neither half, which is a fact
   *  about the record rather than a failure: a frontend that produced no graph
   *  writes no write-time identity. */
  def strictIdentityIn(text: String): Option[(Long, String)] = {
    val sizes = """(?m)^\s*bytes:\s*(\d+)\s*$""".r
      .findAllMatchIn(text).map(_.group(1).toLong).toList.distinct
    val shas = """(?m)^\s*sha256:\s*([0-9a-f]{64})\s*$""".r
      .findAllMatchIn(text).map(_.group(1)).toList.distinct
    if (sizes.isEmpty && shas.isEmpty) None
    else if (sizes.size != 1 || shas.size != 1) {
      abortRun(s"the in-checkout frontend record $CPG_FRONTEND_RECORD_PATH must carry " +
        s"exactly one 'bytes:' value and one 'sha256:' value; it carries " +
        s"${sizes.size} and ${shas.size} distinct. An ambiguous record cannot " +
        "adjudicate an identity check, and choosing the one that matched would not be " +
        "a check")
    } else Some((sizes.head, shas.head))
  }

  /** A provisioning record's pair, in either of the two shapes the provisioner
   *  writes beside the graph: a bare `<bytes> <sha256>` line, and a labelled
   *  `Bytes : <n>` / `sha256 : <hex>` pair. */
  def provisioningIdentityIn(text: String): Option[(Long, String)] = {
    val inline = """(?m)^\s*(\d+)\s+([0-9a-f]{64})\s*$""".r.findAllMatchIn(text)
      .map(m => (m.group(1).toLong, m.group(2))).toList
    val sizes = (inline.map(_._1) ::: """(?m)^\s*[Bb]ytes\s*:\s*([\d,]+)\s*$""".r
      .findAllMatchIn(text).map(_.group(1).replace(",", "").toLong).toList).distinct
    val shas = (inline.map(_._2) ::: """(?m)^\s*sha256\s*:\s*([0-9a-f]{64})\s*$""".r
      .findAllMatchIn(text).map(_.group(1)).toList).distinct
    if (sizes.isEmpty && shas.isEmpty) None
    else if (sizes.size != 1 || shas.size != 1) {
      abortRun(s"a provisioning record beside the resolved graph must yield exactly one " +
        s"identity pair; it yields ${sizes.size} distinct size(s) and ${shas.size} " +
        "distinct digest(s), so it cannot adjudicate an identity check")
    } else Some((sizes.head, shas.head))
  }

  /** The provisioning records that sit beside the graph, most specific first.
   *  DERIVED from the graph this load will open - <graph dir>/../provision-log/ -
   *  so a clone pointing $HARNESS_CPG elsewhere is adjudicated against that
   *  graph's own record rather than against this one's. */
  val provisionRecordPaths: List[Path] = {
    val base = cpgResolved.getParent.getParent.resolve(CPG_PROVISION_RECORD_DIR)
    CPG_PROVISION_RECORD_NAMES.map(base.resolve)
  }

  val frontendRecordPath = repoRoot.resolve(CPG_FRONTEND_RECORD_PATH)
  val identityCandidates = scala.collection.mutable.ArrayBuffer.empty[IdentityCandidate]
  if (Files.isRegularFile(frontendRecordPath)) {
    val text = new String(Files.readAllBytes(frontendRecordPath), StandardCharsets.UTF_8)
    strictIdentityIn(text).foreach { case (size, sha) =>
      identityCandidates += IdentityCandidate(CPG_FRONTEND_RECORD_PATH,
        "write-time record: this checkout's frontend wrote the graph", size, sha)
    }
    log(s"frontend record           : $CPG_FRONTEND_RECORD_PATH present, " +
      (if (identityCandidates.isEmpty)
         "carries no write-time bytes:/sha256: pair (this checkout's frontend wrote no " +
           "accepted graph), so it does not own this load's identity"
       else "carries the write-time pair, which governs"))
  } else {
    log(s"frontend record           : $CPG_FRONTEND_RECORD_PATH absent")
  }
  provisionRecordPaths.foreach { rp =>
    if (Files.isRegularFile(rp)) {
      val text = new String(Files.readAllBytes(rp), StandardCharsets.UTF_8)
      provisioningIdentityIn(text).foreach { case (size, sha) =>
        identityCandidates += IdentityCandidate(
          CPG_PROVISION_RECORD_DIR + "/" + rp.getFileName.toString,
          "provisioning record of account for the graph this run did not write",
          size, sha)
      }
    }
  }
  if (identityCandidates.isEmpty) {
    abortRun(s"no record of account carries an identity pair for the graph: " +
      s"$CPG_FRONTEND_RECORD_PATH records no accepted graph and no provisioning record " +
      s"was found beside the resolved graph at " +
      provisionRecordPaths.map(_.getFileName.toString).mkString(", ") +
      ". The pair recorded at write time is what every later load re-verifies, so " +
      "there is nothing to verify against")
  }
  val distinctPairs = identityCandidates.map(c => (c.size, c.sha256)).toList.distinct
  if (distinctPairs.size != 1) {
    abortRun("the records of account disagree, so no identity can be adjudicated and " +
      "nothing may be loaded: " +
      identityCandidates.map(c => s"${c.label} says ${c.size} / ${c.sha256}")
        .mkString("; "))
  }
  val recordOfAccount = identityCandidates.head
  val recordedSize = recordOfAccount.size
  val recordedSha = recordOfAccount.sha256
  val recordOfAccountLabel = recordOfAccount.label
  val recordCorroborators = identityCandidates.toList.tail.map(_.label)
  log(s"recorded at write time    : bytes=$recordedSize sha256=$recordedSha")
  log(s"record of account         : $recordOfAccountLabel")
  log(s"its provenance            : ${recordOfAccount.provenance}")
  log(s"corroborated by           : " +
    (if (recordCorroborators.isEmpty) "no second record carries a pair"
     else recordCorroborators.mkString(", ") + ", which agree"))

  val sizeMatches = sizeFollow == recordedSize
  val shaMatches = shaObserved == recordedSha
  log(s"byte size matches         : ${if (sizeMatches) "YES" else "NO"}")
  log(s"sha256 matches            : ${if (shaMatches) "YES" else "NO"}")
  if (!(sizeMatches && shaMatches)) {
    abortRun("graph identity mismatch: observed bytes=" + sizeFollow + " sha256=" +
      shaObserved + " against recorded bytes=" + recordedSize + " sha256=" + recordedSha +
      " in " + recordOfAccountLabel + ". A load against different bytes than the record " +
      "describes produces conclusions about a graph nobody has")
  }

  // ------------------------- portable labels for the written artefacts ------
  // The envelope and the prose report are held to byte-identity: an unchanged
  // source over an unchanged graph must emit the same bytes from any checkout.
  // An absolute host path cannot appear in either, because the clone root is a
  // property of the checkout rather than of the measurement, so the same graph
  // reached through two clones would otherwise produce two different files.
  // Queries 01 and 02 express these same fields by environment-variable NAME
  // and by repository-relative form, and this query follows them so all three
  // envelopes describe the graph the same way. Nothing is lost: the literals
  // stay in the console stream, which is deliberately not held to
  // byte-identity, and the size-and-digest pair - not a path - is what the
  // identity comparison turns on.
  def portableLabel(p: Path, outsideRoot: String): String =
    if (p.startsWith(repoRoot)) repoRoot.relativize(p).toString else outsideRoot
  val cpgNamedLabel =
    if (cpgEnvValue.isDefined) "$" + CPG_ENV_VAR else CPG_PATH_DEFAULT
  val cpgNamedRepoRelativeLabel = portableLabel(cpgNamed,
    "a path outside the repository root, named by $" + CPG_ENV_VAR)
  val cpgResolvedLabel = portableLabel(cpgResolved,
    "a host-shared read-only file outside the repository root, reached by following " +
      "the symlink")
  log(s"graph path label          : $cpgNamedLabel")
  log(s"resolved target label     : $cpgResolvedLabel")

  // Both names for the graph must resolve to the bytes just measured.
  val aapNameExists = Files.exists(cpgAapNamed, LinkOption.NOFOLLOW_LINKS)
  val bothNamesSameFile =
    aapNameExists && Files.exists(cpgAapNamed) && cpgAapNamed.toRealPath() == cpgResolved
  val aapNameReconciliation =
    if (!aapNameExists) "absent"
    else if (bothNamesSameFile) "same file (equal resolved target)"
    else {
      val otherSha = sha256Of(cpgAapNamed.toRealPath())
      if (otherSha == shaObserved) "different path, identical bytes (reconciled by digest)"
      else abortRun(s"irreconcilable mismatch between the two names for the graph: " +
        s"$cpgNamed resolves to $cpgResolved (sha256 $shaObserved) while $cpgAapNamed " +
        s"resolves to ${cpgAapNamed.toRealPath()} (sha256 $otherSha). The runner would " +
        "then read bytes the record does not describe")
    }
  log(s"AAP-named path            : $aapNameReconciliation")
  log(s"identity check elapsed_ms : ${elapsedMs(identityNanos)}")
  log("graph identity            : PASS - re-verified immediately before the load")

  // -------------------------------------------------------------------------
  stage("D-parameters: the pair structure, both pair literals, and validation")
  // -------------------------------------------------------------------------
  // This stage runs BEFORE the load on purpose. Nothing in it needs the graph,
  // and a malformed pair is a fault in the query's own configuration: catching
  // it here costs nothing, while catching it after the load would spend several
  // minutes and a 64 GiB heap to reach the same conclusion.
  /** Operator pseudo-calls are CPG artefacts, not method calls. Named so the
   *  exclusion is a stated modelling decision rather than a silent filter. */
  val OPERATOR_CALL_PREFIX = "<operator"
  def isOperatorCall(c: Call): Boolean = c.methodFullName.startsWith(OPERATOR_CALL_PREFIX)

  def lineOf(c: Call): Int = c.lineNumber.map(_.toInt).getOrElse(-1)
  def lineOfMethod(m: Method): Int = m.lineNumber.map(_.toInt).getOrElse(-1)
  def owningTypes(m: Method): List[String] = m.typeDecl.fullName.l.distinct.sorted

  /** Every real call site of a method, duplicate class definitions unioned. The
   *  graph holds more than one node per class where two staged jars carried the
   *  same class, so nodes are grouped by full name and their call sites unioned
   *  rather than one node being picked. */
  def callSitesOf(nodes: List[Method]): List[Call] =
    nodes
      .flatMap(_.callOut.l)
      .filterNot(isOperatorCall)
      .distinctBy(c => (c.methodFullName, lineOf(c), c.order, c.code))
      .sortBy(c => (c.methodFullName, lineOf(c), c.order))

  def calleesOf(c: Call): List[Method] =
    NoResolve.getCalledMethodsAsTraversal(c).l.sortBy(_.fullName)

  /** An RPC message hop: the message type a send pairs on, its constructor and
   *  the field accessors a receiving handler reads. */
  final case class MessageHop(
      id: String,
      messageType: String,
      ctorName: String,
      accessorNames: List[String],
      direction: String,
      sourceAnchor: String)

  /**
   * THE PARAMETER. One value of this type is one instantiation of the query: a
   * handler end, a sink end, the message hops on the route between them, and the
   * labels the report uses. No traversal below refers to a handler or a sink by
   * name - every selector it applies comes out of the pair it was handed, which
   * is what makes the second invocation a real invocation rather than a copy.
   */
  final case class HandlerSinkPair(
      id: String,
      label: String,
      handlerPlanName: String,
      handlerType: String,
      handlerMethod: String,
      handlerSyntheticTypeRegex: String,
      handlerSyntheticMethod: String,
      handlerBodyWitness: String,
      handlerBaseType: String,
      handlerSourceFile: String,
      handlerSourceLine: Int,
      sinkCalleeRegex: String,
      sinkCallName: String,
      sinkHostTypeRegex: String,
      sinkSourceFile: String,
      sinkSourceLine: Int,
      messageHops: List[MessageHop],
      routeSurfaceTypePrefixes: List[String])

  val messageHopLaunchDriver = MessageHop(
    id = MESSAGE_HOP_LAUNCH_DRIVER_ID,
    messageType = MESSAGE_TYPE_LAUNCH_DRIVER,
    ctorName = MESSAGE_CTOR_NAME,
    accessorNames = MESSAGE_ACCESSORS_LAUNCH_DRIVER,
    direction = MESSAGE_HOP_LAUNCH_DRIVER_DIRECTION,
    sourceAnchor = MESSAGE_HOP_LAUNCH_DRIVER_SOURCE)

  val messageHopRequestSubmitDriver = MessageHop(
    id = MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_ID,
    messageType = MESSAGE_TYPE_REQUEST_SUBMIT_DRIVER,
    ctorName = MESSAGE_CTOR_NAME,
    accessorNames = MESSAGE_ACCESSORS_REQUEST_SUBMIT_DRIVER,
    direction = MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_DIRECTION,
    sourceAnchor = MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_SOURCE)

  val pairOne = HandlerSinkPair(
    id = PAIR_ONE_ID,
    label = PAIR_ONE_LABEL,
    handlerPlanName = PAIR_ONE_HANDLER_TYPE + "." + PAIR_ONE_HANDLER_METHOD,
    handlerType = PAIR_ONE_HANDLER_TYPE,
    handlerMethod = PAIR_ONE_HANDLER_METHOD,
    handlerSyntheticTypeRegex = PAIR_ONE_HANDLER_SYNTHETIC_TYPE_REGEX,
    handlerSyntheticMethod = HANDLER_SYNTHETIC_METHOD,
    handlerBodyWitness = PAIR_ONE_HANDLER_BODY_WITNESS,
    handlerBaseType = "",
    handlerSourceFile = PAIR_ONE_HANDLER_SOURCE_FILE,
    handlerSourceLine = PAIR_ONE_HANDLER_SOURCE_LINE,
    sinkCalleeRegex = SINK_CALLEE_REGEX,
    sinkCallName = SINK_CALL_NAME,
    sinkHostTypeRegex = SINK_HOST_TYPE_REGEX,
    sinkSourceFile = SINK_SOURCE_FILE,
    sinkSourceLine = SINK_SOURCE_LINE,
    messageHops = List(messageHopLaunchDriver),
    // Handler, the intermediate Worker hop, then the sink host: the WHOLE route
    // surface, so the predicate evidence below is not silently scoped to its ends.
    routeSurfaceTypePrefixes = (PAIR_ONE_HANDLER_TYPE ::
      (ROUTE_HOP_SURFACE_TYPE_PREFIXES ::: SINK_SURFACE_TYPE_PREFIXES)).distinct)

  val pairTwo = HandlerSinkPair(
    id = PAIR_TWO_ID,
    label = PAIR_TWO_LABEL,
    handlerPlanName = PAIR_TWO_HANDLER_PLAN_NAME,
    handlerType = PAIR_TWO_HANDLER_TYPE,
    handlerMethod = PAIR_TWO_HANDLER_METHOD,
    handlerSyntheticTypeRegex = PAIR_TWO_HANDLER_SYNTHETIC_TYPE_REGEX,
    handlerSyntheticMethod = HANDLER_SYNTHETIC_METHOD,
    handlerBodyWitness = PAIR_TWO_HANDLER_BODY_WITNESS,
    handlerBaseType = PAIR_TWO_HANDLER_BASE_TYPE,
    handlerSourceFile = PAIR_TWO_HANDLER_SOURCE_FILE,
    handlerSourceLine = PAIR_TWO_HANDLER_SOURCE_LINE,
    sinkCalleeRegex = SINK_CALLEE_REGEX,
    sinkCallName = SINK_CALL_NAME,
    sinkHostTypeRegex = SINK_HOST_TYPE_REGEX,
    sinkSourceFile = SINK_SOURCE_FILE,
    sinkSourceLine = SINK_SOURCE_LINE,
    // Ordered as the route runs: the servlet's own send FIRST, then the send the
    // Master handler it reaches makes. Pair two therefore carries one more hop
    // than pair one, which is exactly why its boundary count is five to four.
    messageHops = List(messageHopRequestSubmitDriver, messageHopLaunchDriver),
    // Pair two's route runs through pair one's handler as well, and then through
    // the same intermediate Worker hop to the same sink host.
    routeSurfaceTypePrefixes =
      (PAIR_TWO_HANDLER_TYPE :: PAIR_ONE_HANDLER_TYPE ::
        (ROUTE_HOP_SURFACE_TYPE_PREFIXES ::: SINK_SURFACE_TYPE_PREFIXES)).distinct)

  /**
   * The declared iteration order. Every per-pair collection below is built by
   * mapping over THIS list, so the order in both result files is this order and
   * an unchanged source over an unchanged graph emits byte-identical bytes.
   */
  val PAIRS: List[HandlerSinkPair] = List(pairOne, pairTwo)

  /**
   * A malformed pair must fail LOUDLY. A pair with an empty selector or an
   * uncompilable regex would select nothing, its invocation would return an
   * empty result, and an empty result from a real invocation is exactly what
   * satisfies the parameterizability measure - so a silently skipped pair would
   * FALSELY satisfy it. Every field a selector is built from is therefore
   * checked before any traversal runs.
   */
  def validatePair(p: HandlerSinkPair): Unit = {
    def req(field: String, value: String): Unit =
      if (value == null || value.trim.isEmpty) {
        abortRun(s"pair '${p.id}' is malformed: $field is empty. A pair with an empty " +
          "selector selects nothing, and its empty result would be indistinguishable " +
          "from a real invocation that found nothing - which would falsely satisfy the " +
          "parameterizability measure")
      }
    def reqRegex(field: String, value: String): Unit = {
      req(field, value)
      try value.r
      catch {
        case t: Throwable =>
          abortRun(s"pair '${p.id}' is malformed: $field is not a compilable regular " +
            s"expression ($value): ${t.getClass.getName}: ${t.getMessage}")
      }
    }
    req("id", p.id)
    req("label", p.label)
    req("handlerPlanName", p.handlerPlanName)
    req("handlerType", p.handlerType)
    req("handlerMethod", p.handlerMethod)
    req("handlerSyntheticMethod", p.handlerSyntheticMethod)
    req("handlerBodyWitness", p.handlerBodyWitness)
    req("handlerSourceFile", p.handlerSourceFile)
    req("sinkCallName", p.sinkCallName)
    req("sinkSourceFile", p.sinkSourceFile)
    reqRegex("handlerSyntheticTypeRegex", p.handlerSyntheticTypeRegex)
    reqRegex("sinkCalleeRegex", p.sinkCalleeRegex)
    reqRegex("sinkHostTypeRegex", p.sinkHostTypeRegex)
    if (p.handlerSourceLine <= 0) {
      abortRun(s"pair '${p.id}' is malformed: handlerSourceLine must be a positive line " +
        s"number at the pin, got ${p.handlerSourceLine}")
    }
    if (p.sinkSourceLine <= 0) {
      abortRun(s"pair '${p.id}' is malformed: sinkSourceLine must be a positive line " +
        s"number at the pin, got ${p.sinkSourceLine}")
    }
    if (p.messageHops.isEmpty) {
      abortRun(s"pair '${p.id}' is malformed: it declares no message hop, so the RPC " +
        "boundary on its route could not be measured and the route count would be " +
        "uninterpretable")
    }
    if (p.routeSurfaceTypePrefixes.isEmpty) {
      abortRun(s"pair '${p.id}' is malformed: it declares no route surface, so its " +
        "expected-spurious basis would have nothing to rest on")
    }
    p.messageHops.foreach { h =>
      req(s"messageHop.${h.id}.messageType", h.messageType)
      req(s"messageHop.${h.id}.ctorName", h.ctorName)
      req(s"messageHop.${h.id}.direction", h.direction)
      req(s"messageHop.${h.id}.sourceAnchor", h.sourceAnchor)
      if (h.accessorNames.isEmpty || h.accessorNames.exists(_.trim.isEmpty)) {
        abortRun(s"pair '${p.id}' is malformed: message hop '${h.id}' declares no usable " +
          "accessor name, so the consumer end of that hop could not be measured")
      }
    }
  }

  if (PAIRS.size < 2) {
    abortRun(s"the parameterizability measure requires at least two pairs and this run " +
      s"declares ${PAIRS.size}: the measure passes only on the SECOND named pair being " +
      "actually invoked")
  }
  if (PAIRS.map(_.id).distinct.size != PAIRS.size) {
    abortRun(s"pair ids are not distinct (${PAIRS.map(_.id).mkString(", ")}); per-pair " +
      "figures are keyed by id, so two pairs sharing one id would overwrite each other")
  }
  PAIRS.foreach(validatePair)
  log(s"pairs declared            : ${PAIRS.size}")
  PAIRS.zipWithIndex.foreach { case (p, i) =>
    log(s"  pair ${i + 1} id            : ${p.id}")
    log(s"    label                 : ${p.label}")
    log(s"    handler (plan name)   : ${p.handlerPlanName}")
    log(s"    handler type.method   : ${p.handlerType}.${p.handlerMethod}")
    log(s"    handler source anchor : ${p.handlerSourceFile}:${p.handlerSourceLine}")
    log(s"    sink source anchor    : ${p.sinkSourceFile}:${p.sinkSourceLine}")
    log(s"    message hops          : ${p.messageHops.map(_.id).mkString(", ")}")
    log(s"    route surface         : ${p.routeSurfaceTypePrefixes.mkString(", ")}")
  }
  log("pair validation           : PASS (every selector present and compilable)")


  // -------------------------------------------------------------------------
  stage("E-load: switchWorkspace then importCpg")
  // -------------------------------------------------------------------------
  // The workspace is the probe's own, never a shared or default one: joern
  // writes an ~800 MB project tree (including a working copy of the graph) into
  // it, removes and rewrites project state inside it, and two Joern processes
  // sharing one corrupt each other. It is switched BEFORE any load.
  // queries/joern/.workspace is the AAP-named location and carries its own
  // .gitignore, so the scratch stays out of the commit without editing upstream
  // Spark's .gitignore.
  //
  // Four properties, none of which a bare createDirectories provides:
  //
  //   - NO-FOLLOW AND CONTAINED. The root is reached by a descriptor descent from
  //     the resolved repository root, which refuses a symbolic link at any
  //     component. A link there would send the engine's writes and its project
  //     removals somewhere else entirely;
  //   - FRESH. The directory this run switches to is created with createDirectory
  //     under a name carrying random bytes, so an existing name fails rather than
  //     being reused and no previous run's project state can be inherited;
  //   - LOCKED. An exclusive file lock is held on a lock file inside it for the
  //     rest of the run. Two Joern processes in one clone are what corrupts a
  //     workspace, and the lock is what makes that impossible rather than
  //     unlikely;
  //   - VERIFIED. The real path is re-resolved after creation and required to be
  //     the path that was created, and it is that verified real path - not the
  //     name it was built from - that is handed to switchWorkspace.
  val workspaceRoot = repoRoot.resolve(WORKSPACE_PATH).toAbsolutePath.normalize
  // The descent both validates every component and creates the root if a
  // checkout lacks it. Its descriptor stays open for the run.
  openPublicationDescriptor(workspaceRoot)
  val workspaceRunSuffix = {
    val raw = new Array[Byte](WORKSPACE_RUN_RANDOM_BYTES)
    publicationRandom.nextBytes(raw)
    raw.map("%02x".format(_)).mkString
  }
  val workspaceRunDir = workspaceRoot.resolve(
    WORKSPACE_RUN_DIR_PREFIX + QUERY_ID + "-" + workspaceRunSuffix)
  Files.createDirectory(workspaceRunDir, java.nio.file.attribute.PosixFilePermissions
    .asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------")))
  val workspaceLockPath = workspaceRunDir.resolve(WORKSPACE_LOCK_FILENAME)
  val workspaceLockChannel = java.nio.channels.FileChannel.open(
    workspaceLockPath,
    java.nio.file.StandardOpenOption.CREATE_NEW,
    java.nio.file.StandardOpenOption.WRITE,
    LinkOption.NOFOLLOW_LINKS)
  val workspaceLock = workspaceLockChannel.tryLock()
  if (workspaceLock == null) {
    abortRun(s"could not take an exclusive lock on $workspaceLockPath, so another " +
      "process may already be writing this workspace. Two Joern processes sharing one " +
      "workspace corrupt each other's project state")
  }
  workspaceLockChannel.write(java.nio.ByteBuffer.wrap(
    (s"$QUERY_ID pid=${ProcessHandle.current().pid()}\n").getBytes(StandardCharsets.UTF_8)))
  workspaceLockChannel.force(true)
  val workspaceResolved = workspaceRunDir.toRealPath()
  if (workspaceResolved != workspaceRunDir) {
    abortRun(s"the workspace $workspaceRunDir resolves to $workspaceResolved, so a " +
      "component was replaced after it was created")
  }
  val workspaceRunRepoRelative = repoRoot.relativize(workspaceResolved).toString
  log(s"workspace root (AAP name) : $WORKSPACE_PATH")
  log(s"workspace root descent    : no-follow, contained in the repository root, by " +
    "descriptor")
  log(s"workspace (this run)      : $workspaceRunRepoRelative")
  log(s"workspace freshness       : created by this run with createDirectory (an " +
    "existing name would have failed), mode 0700")
  log(s"workspace lock            : exclusive, held on " +
    s"${repoRoot.relativize(workspaceLockPath)} for the rest of the run")
  log(s"workspace (resolved real) : $workspaceResolved")
  switchWorkspace(workspaceResolved.toString)

  val loadNanos = System.nanoTime()
  // THE PRIVATE COPY, and nothing else, is what is imported: the bytes whose
  // digest was taken in the copy pass and compared against the record of
  // account above. Both pairs below are traversed over this one load.
  log(s"loading the graph with importCpg: $privateInput")
  log(s"loaded bytes are           : the private copy verified above, not the " +
    "host-shared path")
  val loaded = importCpg(privateInput.toString)
  if (loaded.isEmpty) {
    abortRun(s"importCpg returned no graph for $privateInput")
  }
  val methodCount = cpg.method.size
  val typeDeclCount = cpg.typeDecl.size
  val fileCount = cpg.file.size
  log(s"load elapsed_ms           : ${elapsedMs(loadNanos)}")
  log(s"graph methods             : $methodCount")
  log(s"graph typeDecls           : $typeDeclCount")
  log(s"graph files               : $fileCount")
  if (methodCount <= 0) {
    abortRun("the loaded graph reports zero methods; there is nothing to traverse")
  }

  // The input is re-measured AFTER the load, by digest and by inode. This is what
  // binds the identity check to the bytes the engine read: a copy swapped or
  // rewritten across the load window changes one of the three and is refused
  // here, where the alternative is a conclusion about a graph nobody has.
  val postLoadAttributes = Files
    .readAttributes(privateInput, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
  val postLoadInode = String.valueOf(postLoadAttributes.fileKey)
  val postLoadSha = sha256Of(privateInput)
  log(s"private input after load  : ${postLoadAttributes.size()} bytes, inode " +
    s"$postLoadInode, sha256 $postLoadSha")
  if (postLoadAttributes.size() != privateInputSize || postLoadInode != privateInputInode ||
    postLoadSha != shaObserved) {
    abortRun(s"the private graph input changed across the load: " +
      s"${postLoadAttributes.size()} bytes / inode $postLoadInode / sha256 $postLoadSha " +
      s"against $privateInputSize bytes / inode $privateInputInode / sha256 $shaObserved " +
      "measured before it. The identity this run published would not describe the bytes " +
      "the engine read")
  }
  log("import binding             : PASS - the imported copy is byte-for-byte and " +
    "inode-for-inode the input the identity check measured")

  // NOTHING IS DELETED HERE, DELIBERATELY (AAP 0.8.1 - "Do not tear anything down.
  // No cleanup, no reset, no temp purging. What the run built stays where it is").
  // The form this replaced removed the copy and its exclusive directory on the
  // reasoning that a half-gigabyte copy per invocation is worth reclaiming. That
  // reasoning is about disk and the constraint is about evidence: this copy carries
  // the exact bytes the engine loaded, so it is the only artifact against which the
  // private-copy identity figures published in this record can be re-measured. It
  // stays where the run put it, its directory keeps its owner-only mode, and
  // reclaiming the space is a decision for a human outside this run. The only
  // deletion left anywhere in this section is the error path of the copy itself,
  // which removes a copy that failed mid-write and never became the verified one.
  // The permissions the copy step set are LEFT AS THEY ARE: the file is r-------- and
  // its directory r-x------, owner only, and the removal this replaced had to widen the
  // directory to unlink. Nothing is unlinked now, so nothing needs write permission and
  // the copy keeps the mode it was verified under.
  val privateInputRetained =
    try {
      Files.exists(privateInput, LinkOption.NOFOLLOW_LINKS) &&
        Files.exists(privateInputDir, LinkOption.NOFOLLOW_LINKS)
    } catch {
      case t: Throwable =>
        System.err.println(sanitizeForLog(
          s"could not confirm the private graph input $privateInput is retained: " +
            s"${t.getMessage}"))
        false
    }
  log(s"private input retained    : $privateInputRetained (created by this run and left " +
    "in place under AAP 0.8.1, so the digest above can be re-measured from the bytes " +
    "the engine read)")

  // The workspace is removed on the way out by a shutdown hook rather than here:
  // the engine holds project state in it until this JVM exits, so removing it now
  // would pull the graph out from under the traversals below. The hook is
  // registered now, and confined to the directory this run created.
  val workspaceCleanupHook = new Thread(new Runnable {
    def run(): Unit = {
      try {
        if (workspaceLock != null && workspaceLock.isValid) workspaceLock.release()
        workspaceLockChannel.close()
      } catch { case _: Throwable => () }
      try {
        // Confined by construction: the walk starts at the directory this run
        // created under the workspace root and refuses to leave it.
        if (workspaceResolved.startsWith(workspaceRoot) &&
          workspaceResolved != workspaceRoot) {
          Files.walk(workspaceResolved)
            .sorted(java.util.Comparator.reverseOrder[Path]())
            .forEach(pth => if (pth.startsWith(workspaceResolved)) {
              try Files.deleteIfExists(pth) catch { case _: Throwable => () }
            })
        }
        System.out.println("workspace removed on exit : " +
          !Files.exists(workspaceResolved, LinkOption.NOFOLLOW_LINKS))
      } catch {
        case t: Throwable =>
          System.err.println("could not remove the workspace: " + t.getMessage)
      }
      closePublicationDescriptors()
    }
  }, "probe-workspace-cleanup")
  Runtime.getRuntime.addShutdownHook(workspaceCleanupHook)
  log("workspace removal         : registered for JVM exit, confined to the directory " +
    "this run created; the engine holds project state in it until then")

  // -------------------------------------------------------------------------
  stage("F-selection: each pair's entry points and the shared sink")
  // -------------------------------------------------------------------------
  /** One indexed sweep per distinct call name, cached so a name shared by both
   *  pairs is ONE measurement cited twice rather than two measurements. */
  val callScanCache =
    scala.collection.mutable.LinkedHashMap.empty[String, (List[Call], Boolean)]
  def scanCallsNamed(name: String): (List[Call], Boolean) = {
    val label = s"calls named $name (indexed sweep, shared by every pair using that name)"
    callScanCache.getOrElseUpdate(name, {
      val scanned = boundedList(label, "MAX_CALL_SCAN", MAX_CALL_SCAN,
        s"the indexed sweep over every call named $name in the graph, before any " +
          "callee-regex or host-type constraint")(cpg.call.nameExact(name))
      (scanned, boundedSweeps.find(_.label == label).exists(_.truncated))
    })
  }

  final case class PairSelection(
      pair: HandlerSinkPair,
      syntheticTypeNames: List[String],
      syntheticEntryNodes: List[Method],
      sourceLevelNodes: List[Method],
      baseDeclarationNames: List[String],
      entryGroups: List[(String, List[Method])],
      entryGroupsTraversed: List[(String, List[Method])],
      entryPointsDiscovered: Int,
      entryPointsTraversed: Int,
      entryPointsTruncated: Int,
      syntheticCallees: List[String],
      sourceLevelCallees: List[String],
      syntheticCarriesBody: Boolean,
      sourceLevelCarriesBody: Boolean,
      sinkCallsScanned: Int,
      sinkScanTruncated: Boolean,
      sinkCallsAnyHost: Int,
      sinkCalls: List[Call],
      sinkHostNames: Set[String])

  /**
   * How the parameterized selector resolves a handler to the method the graph
   * actually carries its body in. It asks BOTH arms and reports both:
   *
   *   the SYNTHETIC arm - every method named handlerSyntheticMethod on every
   *   type matching handlerSyntheticTypeRegex. A handler returning a
   *   PartialFunction compiles its case bodies into such a class, so for that
   *   shape this arm is where the route begins.
   *
   *   the SOURCE-LEVEL arm - the method named handlerMethod on the exact
   *   handlerType. For an ordinary method this arm IS the body; for a
   *   partial-function handler it only constructs the partial function.
   *
   * The two are unioned and grouped by method full name, and which of them
   * carries the route is then MEASURED by looking for the pair's declared body
   * witness among each arm's own call sites. Taking only one arm would silently
   * miss one of the two pairs, and that is the parameterization detail this
   * stage exists to get right.
   */
  def selectFor(p: HandlerSinkPair): PairSelection = {
    val syntheticTypeDecls = boundedList(
      s"pair ${p.id} entry: synthetic partial-function type declarations",
      "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"type declarations whose full name matches ${p.handlerSyntheticTypeRegex}")(
      cpg.typeDecl.fullName(p.handlerSyntheticTypeRegex))
    val syntheticTypeNames = syntheticTypeDecls.map(_.fullName).distinct.sorted
    val syntheticEntryNodes = boundedList(
      s"pair ${p.id} entry: methods on those synthetic types",
      "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"methods declared on those type declarations, before the " +
        s"${p.handlerSyntheticMethod} name filter")(
      syntheticTypeDecls.iterator.flatMap(_.method))
      .filter(_.name == p.handlerSyntheticMethod)
    val sourceLevelNodes = boundedList(
      s"pair ${p.id} entry: source-level handler methods",
      "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"methods named ${p.handlerMethod} declared on ${p.handlerType}")(
      cpg.typeDecl.fullNameExact(p.handlerType).method.nameExact(p.handlerMethod))
    val baseDeclarationNames =
      if (p.handlerBaseType.isEmpty) Nil
      else boundedList(
        s"pair ${p.id} entry: base declarations on ${p.handlerBaseType}",
        "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
        s"methods named ${p.handlerMethod} declared on the base type " +
          s"${p.handlerBaseType}, selected to be REPORTED as excluded rather than to " +
          "be traversed")(
        cpg.typeDecl.fullNameExact(p.handlerBaseType).method.nameExact(p.handlerMethod))
        .map(_.fullName)
        .distinct
        .sorted
    val entryGroups: List[(String, List[Method])] =
      (syntheticEntryNodes ++ sourceLevelNodes)
        .groupBy(_.fullName)
        .toList
        .sortBy(_._1)
    val entryGroupsTraversed = entryGroups.take(MAX_ENTRY_POINTS_PER_PAIR)
    val syntheticCallees = callSitesOf(syntheticEntryNodes).map(_.methodFullName).distinct.sorted
    val sourceLevelCallees = callSitesOf(sourceLevelNodes).map(_.methodFullName).distinct.sorted

    val (scanned, scanTruncated) = scanCallsNamed(p.sinkCallName)
    val sinkCallsAnyHost = scanned.filter(_.methodFullName.matches(p.sinkCalleeRegex))
    val sinkCalls = sinkCallsAnyHost
      .filter(c => owningTypes(c.method).exists(_.matches(p.sinkHostTypeRegex)))
      .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
      .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))

    PairSelection(
      pair = p,
      syntheticTypeNames = syntheticTypeNames,
      syntheticEntryNodes = syntheticEntryNodes,
      sourceLevelNodes = sourceLevelNodes,
      baseDeclarationNames = baseDeclarationNames,
      entryGroups = entryGroups,
      entryGroupsTraversed = entryGroupsTraversed,
      entryPointsDiscovered = entryGroups.size,
      entryPointsTraversed = entryGroupsTraversed.size,
      entryPointsTruncated = entryGroups.size - entryGroupsTraversed.size,
      syntheticCallees = syntheticCallees,
      sourceLevelCallees = sourceLevelCallees,
      syntheticCarriesBody = syntheticCallees.exists(_.contains(p.handlerBodyWitness)),
      sourceLevelCarriesBody = sourceLevelCallees.exists(_.contains(p.handlerBodyWitness)),
      sinkCallsScanned = scanned.size,
      sinkScanTruncated = scanTruncated,
      sinkCallsAnyHost = sinkCallsAnyHost.size,
      sinkCalls = sinkCalls,
      sinkHostNames = sinkCalls.map(_.method.fullName).distinct.sorted.toSet)
  }

  val selections: List[PairSelection] = PAIRS.map(selectFor)
  selections.foreach { s =>
    val p = s.pair
    log(s"pair ${p.id}: synthetic typeDecls matching ${p.handlerSyntheticTypeRegex}: " +
      s"${s.syntheticTypeNames.size}")
    s.syntheticTypeNames.foreach(t => log(s"    synthetic type        : $t"))
    log(s"pair ${p.id}: entry points discovered=${s.entryPointsDiscovered} " +
      s"traversed=${s.entryPointsTraversed} (cap $MAX_ENTRY_POINTS_PER_PAIR) " +
      s"truncated=${s.entryPointsTruncated}")
    s.entryGroups.foreach { case (fn, nodes) =>
      log(s"    entry                 : $fn  nodes=${nodes.size} " +
        s"graph_line=${lineOfMethod(nodes.head)}")
    }
    if (s.baseDeclarationNames.nonEmpty) {
      log(s"pair ${p.id}: base declaration(s) on ${p.handlerBaseType} present in the " +
        "graph and EXCLUDED by the pair's exact type selector: " +
        s.baseDeclarationNames.mkString(", "))
    }
    log(s"pair ${p.id}: body witness '${p.handlerBodyWitness}' among the synthetic arm's " +
      s"own call sites: ${s.syntheticCarriesBody} (${s.syntheticCallees.size} callees); " +
      s"among the source-level arm's: ${s.sourceLevelCarriesBody} " +
      s"(${s.sourceLevelCallees.size} callees)")
    log(s"pair ${p.id}: calls named ${p.sinkCallName} scanned=${s.sinkCallsScanned} " +
      s"(cap $MAX_CALL_SCAN, truncated=${s.sinkScanTruncated}); matching the callee " +
      s"regex on any host=${s.sinkCallsAnyHost}; on the sink host=${s.sinkCalls.size}")
    s.sinkCalls.foreach { c =>
      log(s"    sink                  : ${c.method.fullName} -> ${c.methodFullName} " +
        s"graph_line=${lineOf(c)} dispatch=${c.dispatchType}")
    }
    if (s.entryPointsDiscovered == 0) {
      abortRun(s"pair '${p.id}' selected NO entry point: neither a type matching " +
        s"${p.handlerSyntheticTypeRegex} carrying ${p.handlerSyntheticMethod} nor " +
        s"${p.handlerType}.${p.handlerMethod} is present in the graph. A pair whose " +
        "invocation cannot begin is not an invocation, and passing over it would " +
        "falsely satisfy the parameterizability measure")
    }
    if (s.sinkCalls.isEmpty) {
      abortRun(s"pair '${p.id}' found no privileged-launch call site on the sink surface: " +
        s"no call matching ${p.sinkCalleeRegex} is hosted by a type matching " +
        p.sinkHostTypeRegex)
    }
    if (!s.syntheticCarriesBody && !s.sourceLevelCarriesBody) {
      log(s"pair ${p.id}: NEITHER arm's own call sites contain the declared body witness; " +
        "the route is reported as beginning at the selected entry points regardless and " +
        "this condition is carried into the report as measured")
    }
  }
  log("pair selection            : PASS (both pairs resolved an entry point and the sink)")


  // -------------------------------------------------------------------------
  stage("G-predicates: the selector, its bytecode collision, and the constraint")
  // -------------------------------------------------------------------------
  // The broad anchored selector is the one the AAP names, and it is the SAME
  // TEXT as queries 01 and 02 carry. On BYTECODE it matches more than the five
  // source-level predicates, because Scala compiles `private var aclsOn`
  // (SecurityManager.scala:59) into accessors, so the graph carries aclsOn() AND
  // aclsOn_$eq(boolean) and both satisfy the `acls.*` alternative. The
  // constraint chain is therefore three steps and ALL THREE SETS ARE REPORTED,
  // so the narrowing is auditable rather than asserted:
  //   1. the broad anchored selector on methods of the SecurityManager type
  //   2. minus every bytecode setter (name ending in the setter suffix)
  //   3. intersected with the five named source-level predicates
  val predicateTypeDecls = boundedList(
    "predicate: type declarations for " + PREDICATE_TYPE, "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"type declarations whose full name is exactly $PREDICATE_TYPE")(
    cpg.typeDecl.fullNameExact(PREDICATE_TYPE))
  if (predicateTypeDecls.isEmpty) {
    abortRun(s"$PREDICATE_TYPE is not present in the graph, so the mechanical " +
      "definition of a spurious route has no predicate set to rest on")
  }
  val predicateTypeMethods = boundedList(
    "predicate: methods declared on " + PREDICATE_TYPE, "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    "every method declared on those type declarations, before the name selector")(
    predicateTypeDecls.iterator.flatMap(_.method))
  val predicateTypeMethodNames = predicateTypeMethods.map(_.name).distinct.sorted
  val predicateBroad = predicateTypeMethods.filter(_.name.matches(PREDICATE_NAME_REGEX))
  val predicateBroadNames = predicateBroad.map(_.name).distinct.sorted
  val predicateAfterSetterExclusion =
    predicateBroad.filterNot(_.name.endsWith(PREDICATE_SETTER_SUFFIX))
  val predicateAfterSetterNames =
    predicateAfterSetterExclusion.map(_.name).distinct.sorted
  val predicateSetterExcludedNames =
    predicateBroadNames.filter(_.endsWith(PREDICATE_SETTER_SUFFIX))
  val predicateFinal =
    predicateAfterSetterExclusion.filter(n => PREDICATE_NAMED_FIVE.contains(n.name))
  val predicateFinalNames = predicateFinal.map(_.name).distinct.sorted
  val predicateNonPredicateResidue =
    predicateAfterSetterNames.filterNot(PREDICATE_NAMED_FIVE.contains)
  log(s"predicate type nodes      : ${predicateTypeDecls.size}")
  log(s"methods on that type      : ${predicateTypeMethods.size} nodes, " +
    s"${predicateTypeMethodNames.size} distinct names")
  log(s"step 1 broad selector     : ${predicateBroadNames.mkString(", ")}")
  log(s"step 2 setters excluded   : ${predicateSetterExcludedNames.mkString(", ")}")
  log(s"step 2 remaining          : ${predicateAfterSetterNames.mkString(", ")}")
  log(s"step 3 non-predicate residue dropped: " +
    s"${predicateNonPredicateResidue.mkString(", ")}")
  log(s"final predicate set       : ${predicateFinalNames.mkString(", ")}")
  if (predicateFinalNames != PREDICATE_NAMED_FIVE.sorted) {
    abortRun("the predicate selector does not resolve to exactly the five named " +
      s"source-level predicates: resolved ${predicateFinalNames.mkString(",")} against " +
      s"expected ${PREDICATE_NAMED_FIVE.sorted.mkString(",")}")
  }
  val predicateFullNames = predicateFinal.map(_.fullName).distinct.sorted
  log("predicate selector        : PASS (asserted against the graph, not the source)")

  // Where those predicates are actually CALLED. This is the structural basis for
  // the expected-spurious question and it is measured, never inferred. It is one
  // measurement, reported against the SHARED route surface the byte-identical
  // block names AND against each pair's own surface.
  val predicateCallSites = boundedList(
    "predicate: incoming call sites of the five predicates", "MAX_CALL_SCAN",
    MAX_CALL_SCAN,
    "every call site whose callee is one of the five resolved predicate methods, " +
      "graph-wide, before any route-surface filter")(
    predicateFinal.iterator.flatMap(_.callIn))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val predicateCallerNames = predicateCallSites.map(_.method.fullName).distinct.sorted
  def onSurface(prefixes: List[String])(m: Method): Boolean =
    owningTypes(m).exists(t => prefixes.exists(p => t.startsWith(p)))
  val predicateCallSitesOnSharedSurface =
    predicateCallSites.filter(c => onSurface(ROUTE_SURFACE_TYPE_PREFIXES)(c.method))
  log(s"predicate call sites (graph-wide): ${predicateCallSites.size} in " +
    s"${predicateCallerNames.size} distinct callers")
  log(s"predicate call sites on the SHARED surface " +
    s"(${ROUTE_SURFACE_TYPE_PREFIXES.mkString(", ")}): " +
    s"${predicateCallSitesOnSharedSurface.size}")
  predicateCallSitesOnSharedSurface.foreach { c =>
    log(s"  on-shared-surface predicate call: ${c.method.fullName} -> " +
      s"${c.methodFullName} graph_line=${lineOf(c)}")
  }
  val predicateCallSitesByPair: Map[String, List[Call]] = PAIRS.map { p =>
    p.id -> predicateCallSites.filter(c => onSurface(p.routeSurfaceTypePrefixes)(c.method))
  }.toMap
  PAIRS.foreach { p =>
    val hits = predicateCallSitesByPair(p.id)
    log(s"pair ${p.id}: predicate call sites on ITS OWN surface " +
      s"(${p.routeSurfaceTypePrefixes.mkString(", ")}): ${hits.size}")
    hits.foreach { c =>
      log(s"    on-pair-surface predicate call: ${c.method.fullName} -> " +
        s"${c.methodFullName} graph_line=${lineOf(c)}")
    }
  }
  /**
   * Per SURFACE PREFIX rather than per pair, so every prefix on either pair's
   * route surface reports the SAME two measured fields and no prefix's zero is
   * indistinguishable from a prefix that is not in the graph at all.
   *
   * The reach fields are why this exists. "No predicate call site on the Worker
   * hop" is only evidence if the Worker is in the graph to be searched: a zero
   * drawn from an absent surface would be unfalsifiable, and it would read
   * exactly like a searched surface that came back clean. So the type-declaration
   * and method counts on each prefix are measured and published beside the
   * call-site count, and a prefix the graph carries NO declaration for aborts the
   * run rather than contributing a zero nobody can falsify. A prefix present with
   * no methods does not abort - it is a different and visible state, published as
   * such under surfaces_present_with_no_methods.
   */
  final case class SurfacePrefixEvidence(
      prefix: String,
      typeDeclsInTheGraph: Int,
      methodsInTheGraph: Int,
      predicateCallSites: Int,
      onPairs: List[String])

  val allSurfacePrefixes =
    (ROUTE_SURFACE_TYPE_PREFIXES ::: PAIRS.flatMap(_.routeSurfaceTypePrefixes)).distinct.sorted
  var surfaceTypeScanTruncated = false
  val surfacePrefixEvidence: List[SurfacePrefixEvidence] = allSurfacePrefixes.map { pref =>
    // An indexed prefix sweep, bounded and reported: the prefix is regex-quoted
    // so a dot in a package name cannot widen the match, and the type-declaration
    // side is what onSurface itself keys on, so this measurement and the surface
    // predicate exactly agree on what "on this prefix" means.
    val declsOnIt = boundedList(
      s"route surface: type declarations on prefix $pref", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      "type declarations whose full name starts with this regex-quoted prefix")(
      cpg.typeDecl.fullName(java.util.regex.Pattern.quote(pref) + TYPE_PREFIX_REGEX_SUFFIX))
    if (boundedSweeps.find(_.label == s"route surface: type declarations on prefix $pref")
      .exists(_.truncated)) surfaceTypeScanTruncated = true
    val methodsOnIt = boundedList(
      s"route surface: methods on prefix $pref", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      "every method declared on those type declarations, counted by distinct full name")(
      declsOnIt.iterator.flatMap(_.method)).map(_.fullName).distinct.size
    val callsOnIt =
      predicateCallSites.count(c => owningTypes(c.method).exists(_.startsWith(pref)))
    SurfacePrefixEvidence(
      prefix = pref,
      typeDeclsInTheGraph = declsOnIt.size,
      methodsInTheGraph = methodsOnIt,
      predicateCallSites = callsOnIt,
      onPairs = PAIRS.filter(_.routeSurfaceTypePrefixes.contains(pref)).map(_.id))
  }
  surfacePrefixEvidence.foreach { e =>
    log(f"  surface prefix ${e.prefix}%-58s type_decls=${e.typeDeclsInTheGraph}%5d " +
      f"methods=${e.methodsInTheGraph}%6d predicate_call_sites=${e.predicateCallSites}%4d " +
      f"pairs=${if (e.onPairs.isEmpty) "shared-list-only" else e.onPairs.mkString("+")}")
  }
  log(s"surface type-declaration sweep truncated at $MAX_TYPE_SCAN: " +
    s"$surfaceTypeScanTruncated")
  val surfacePrefixesAbsentFromTheGraph =
    surfacePrefixEvidence.filter(_.typeDeclsInTheGraph == 0).map(_.prefix)
  if (surfacePrefixesAbsentFromTheGraph.nonEmpty) {
    abortRun("a declared route surface prefix has NO type declaration in the graph: " +
      surfacePrefixesAbsentFromTheGraph.mkString(", ") + ". A predicate search over a " +
      "surface the graph does not carry returns zero for a reason that has nothing to do " +
      "with the route, and that zero would be indistinguishable from a searched surface " +
      "that came back clean, so the run stops rather than publishing it.")
  }
  val surfacePrefixesPresentWithNoMethods =
    surfacePrefixEvidence.filter(e => e.typeDeclsInTheGraph > 0 && e.methodsInTheGraph == 0)
      .map(_.prefix)
  log(s"route surface prefixes measured: ${surfacePrefixEvidence.size}, every one present " +
    s"in the graph, present-but-method-less " +
    (if (surfacePrefixesPresentWithNoMethods.isEmpty) "none"
     else surfacePrefixesPresentWithNoMethods.mkString(", ")) +
    s", total predicate call sites on them " +
    s"${surfacePrefixEvidence.map(_.predicateCallSites).sum}")

  /**
   * A measured property of the query-wide prefix list, published rather than
   * asserted: the list is DERIVED from both pairs' own route ends, so both
   * handler types are expected to be covered - and "expected" is not evidence,
   * which is why the check is made here and its outcome published. Each pair's
   * own surface is what makes that pair's basis correct; the derived query-wide
   * surface is what makes the two pairs' bases commensurable. Stage I separately
   * asserts that every measured route end of every pair is covered.
   */
  val pairsNotCoveredBySharedSurface = PAIRS
    .filterNot(p => ROUTE_SURFACE_TYPE_PREFIXES.exists(pref => p.handlerType.startsWith(pref)))
    .map(_.handlerType)
  log(s"handler types NOT covered by the shared prefix list: " +
    (if (pairsNotCoveredBySharedSurface.isEmpty) "none"
     else pairsNotCoveredBySharedSurface.mkString(", ")))

  // -------------------------------------------------------------------------
  stage("H-traversal: two bounded call-graph walks PER PAIR, entry points to sink")
  // -------------------------------------------------------------------------
  final case class Hop(fromMethod: String, callSite: String, callSiteLine: Int, toMethod: String)
  final case class RouteRecord(
      pairId: String,
      walkId: String,
      entryPoint: String,
      sinkHost: String,
      hops: List[Hop])
  final case class WalkResult(
      pairId: String,
      walkId: String,
      followsFanOut: Boolean,
      entryPointsTraversed: Int,
      expansions: Int,
      methodsVisited: Int,
      callSitesConsidered: Int,
      fanOutSitesEncountered: Int,
      fanOutSitesNotFollowed: Int,
      maxExpansionsAtOneEntry: Int,
      maxDepthUsed: Int,
      depthBoundReached: Boolean,
      entryExpansionCapReached: Boolean,
      pairStepBudgetExhausted: Boolean,
      routeCapReached: Boolean,
      /** Arrivals at a sink host already witnessed from the same entry point in
       *  this walk. COUNTED, not retained - see the retention comment in `walk`. */
      alternateSinkArrivalsNotRetained: Int,
      routes: List[RouteRecord])

  /**
   * One route's complete ordered hop sequence rendered as a single ordering key.
   * It is the last component of the route sort, which is what makes that sort
   * TOTAL: two routes sharing their pair, walk, endpoints and hop count still
   * differ here unless every hop is identical, and a route identical in every
   * hop has already been removed by the deduplication that precedes the sort.
   */
  def hopSequenceKey(r: RouteRecord): String =
    r.hops.map(h => s"${h.fromMethod}|${h.callSite}|${h.callSiteLine}|${h.toMethod}")
      .mkString(">>")

  /**
   * One pair's step budget, shared by both of that pair's walks. It exists
   * because MAX_STEPS_PER_PAIR is documented as a PER-PAIR cap: a counter local
   * to a walk would enforce it per walk instead, and the published label would
   * then name a scope the implementation does not hold. One instance is created
   * per pair and handed to each of its walks; nothing is shared between pairs.
   */
  final class PairStepBudget {
    var used: Int = 0
    var exhausted: Boolean = false
    /** Routes retained across BOTH of this pair's walks.
     *
     *  MAX_ROUTES_PER_PAIR names a per-pair cap and is published as one, but it
     *  was checked against a collection local to each walk while two walks run
     *  per pair - so the effective retention was up to 64 PER WALK, twice the
     *  published cap, and `route_cap_reached` could stay false with 128 routes
     *  retained. The count lives here for the same reason `used` does: the
     *  budget object is the one thing both walks of a pair share and nothing
     *  shares between pairs.
     */
    var routesRetained: Int = 0
    var routeCapReached: Boolean = false
  }

  /**
   * Render an `exists` aggregate in words that match the quantifier.
   *
   * `exists` true means true in AT LEAST ONE member; `exists` FALSE means false
   * in EVERY member, not "false in at least one". The earlier wording said
   * "in AT LEAST ONE" unconditionally, which read correctly for true and
   * inverted the meaning for false.
   */
  def existsPhrase(values: List[Boolean]): String =
    if (values.exists(identity))
      s"true in AT LEAST ONE of this pair's ${values.size} walks"
    else
      s"false in EVERY one of this pair's ${values.size} walks"

  /** Retain one route against the PER-PAIR cap, reporting whether it may be. */
  def pairRouteAvailable(b: PairStepBudget): Boolean =
    if (b.routesRetained >= MAX_ROUTES_PER_PAIR) { b.routeCapReached = true; false }
    else { b.routesRetained += 1; true }

  /**
   * Consume one pair step if the cap allows it, and report whether it did.
   *
   * This is called INSIDE the counted loop - once per call site - rather than
   * only before the enclosing method expansion. Checking the cap only at the
   * outer level let a single method overshoot MAX_STEPS_PER_PAIR by up to its
   * own call-site count, because `used` was incremented per call site with no
   * check between increments; and a traversal that happened to finish on that
   * overshoot left `exhausted` false while the cap had in fact been passed. So
   * the published `pair_step_budget_exhausted` flag would have understated a
   * truncation, which is the one direction a bound must never be wrong in.
   */
  def pairStepAvailable(b: PairStepBudget): Boolean =
    if (b.used >= MAX_STEPS_PER_PAIR) { b.exhausted = true; false } else true

  /**
   * One bounded breadth-first walk over CALL edges, for ONE pair. Every bound it
   * respects is a named constant and every counter it fills is reported for that
   * pair: nothing here is shared with the other pair's budget, so one pair
   * cannot silently truncate the other.
   */
  def walk(
      s: PairSelection,
      walkId: String,
      followFanOut: Boolean,
      pairSteps: PairStepBudget): WalkResult = {
    val p = s.pair
    var methodsVisited = 0
    // Two expansion counters at two scopes, because two different things are
    // being bounded and reported. entryExpansions is reset at EACH entry point
    // and is what MAX_EXPANSIONS_PER_ENTRY caps; walkExpansions accumulates
    // across the walk and is reported as the walk's total, capping nothing.
    var entryExpansions = 0
    var walkExpansions = 0
    var maxExpansionsAtOneEntry = 0
    var callSitesConsidered = 0
    var fanOutEncountered = 0
    var fanOutNotFollowed = 0
    var maxDepthUsed = 0
    var depthBoundReached = false
    var entryExpansionCapReached = false
    var routeCapReached = false
    var alternateSinkArrivalsNotRetained = 0
    val routes = scala.collection.mutable.ArrayBuffer.empty[RouteRecord]

    s.entryGroupsTraversed.foreach { case (entryName, entryNodes) =>
      // The per-entry-point allowance, reset here so the cap is enforced at the
      // scope its name and its published label both state.
      entryExpansions = 0
      val visited = scala.collection.mutable.HashSet[String](entryName)
      val parent = scala.collection.mutable.HashMap.empty[String, Hop]
      var frontier: List[(String, List[Method])] = List(entryName -> entryNodes)
      var depth = 0
      var stop = false
      while (frontier.nonEmpty && depth < MAX_CALL_DEPTH && !stop) {
        val nextByName = scala.collection.mutable.LinkedHashMap.empty[String, List[Method]]
        val ordered = frontier.sortBy(_._1)
        var i = 0
        while (i < ordered.size && !stop) {
          val (fromName, fromNodes) = ordered(i)
          i += 1
          if (entryExpansions >= MAX_EXPANSIONS_PER_ENTRY) {
            entryExpansionCapReached = true
            stop = true
          } else if (pairSteps.used >= MAX_STEPS_PER_PAIR) {
            pairSteps.exhausted = true
            stop = true
          } else {
            entryExpansions += 1
            walkExpansions += 1
            if (entryExpansions > maxExpansionsAtOneEntry)
              maxExpansionsAtOneEntry = entryExpansions
            callSitesOf(fromNodes).foreach { c => if (pairStepAvailable(pairSteps)) {
              callSitesConsidered += 1
              pairSteps.used += 1
              val callees = calleesOf(c)
              val distinctNames = callees.map(_.fullName).distinct
              val isFanOut = distinctNames.size > FANOUT_CALLEE_THRESHOLD
              if (isFanOut) fanOutEncountered += 1
              if (isFanOut && !followFanOut) {
                fanOutNotFollowed += 1
              } else {
                callees.groupBy(_.fullName).toList.sortBy(_._1).foreach {
                  case (toName, toNodes) =>
                    if (!visited.contains(toName)) {
                      visited += toName
                      parent(toName) = Hop(fromName, c.methodFullName, lineOf(c), toName)
                      nextByName.getOrElseUpdate(toName, toNodes)
                    }
                    // WHAT IS RETAINED, AND WHAT IS ONLY COUNTED. One witness
                    // chain is retained per (walk, entry point, sink host): the
                    // shortest, reconstructed from the breadth-first parent map.
                    // A later arrival at a sink host already witnessed from this
                    // entry point in this walk is a DIFFERENT hop sequence, and
                    // the identity function published for distinct_routes names
                    // the full hop sequence - so those arrivals are counted here
                    // rather than silently dropped, and the count is published
                    // per walk and per pair. Retaining them would change what
                    // distinct_routes means for both pairs and for the two
                    // siblings this query is compared against; counting them
                    // makes the difference between the metric and its identity
                    // function visible instead of leaving the cap unexercisable
                    // and unexplained.
                    if (s.sinkHostNames.contains(toName) &&
                      routes.exists(r => r.walkId == walkId && r.entryPoint == entryName &&
                        r.sinkHost == toName)) {
                      alternateSinkArrivalsNotRetained += 1
                    } else if (s.sinkHostNames.contains(toName)) {
                      if (!pairRouteAvailable(pairSteps)) routeCapReached = true
                      else {
                        // Reconstruct the shortest route from the parent map.
                        val chain = scala.collection.mutable.ListBuffer.empty[Hop]
                        var cursor = toName
                        var guard = 0
                        while (parent.contains(cursor) && guard <= MAX_CALL_DEPTH + 1) {
                          val hop = parent(cursor)
                          chain.prepend(hop)
                          cursor = hop.fromMethod
                          guard += 1
                        }
                        routes += RouteRecord(p.id, walkId, entryName, toName, chain.toList)
                      }
                    }
                }
              }
            } }
            // The per-call-site guard above sets `exhausted` the moment the cap
            // is reached; this is what turns that into termination, so the walk
            // stops on the cap rather than running to the end of the frontier
            // with the flag already set.
            if (pairSteps.exhausted) stop = true
          }
        }
        frontier = nextByName.toList
        depth += 1
        if (depth > maxDepthUsed) maxDepthUsed = depth
        if (frontier.nonEmpty && depth >= MAX_CALL_DEPTH) depthBoundReached = true
      }
      methodsVisited += visited.size
      log(f"  walk ${p.id}%-9s $walkId%-18s entry=$entryName visited=${visited.size}%8d " +
        f"depth=$depth%2d entry_expansions=$entryExpansions%8d " +
        f"pair_steps=${pairSteps.used}%8d")
    }

    WalkResult(p.id, walkId, followFanOut, s.entryPointsTraversed, walkExpansions,
      methodsVisited, callSitesConsidered, fanOutEncountered, fanOutNotFollowed,
      maxExpansionsAtOneEntry, maxDepthUsed, depthBoundReached, entryExpansionCapReached,
      pairSteps.exhausted, routeCapReached, alternateSinkArrivalsNotRetained,
      routes.toList)
  }

  val WALK_A_ID = "A-follows-fan-out"
  val WALK_B_ID = "B-fan-out-recorded"
  /** The two walk modes every pair is put through, named so that "how many walks
   *  a pair should have run" is a constant rather than a literal in a predicate. */
  val WALK_MODE_IDS = List(WALK_A_ID, WALK_B_ID)

  /** Per-pair traversal outcome. Routes are deduplicated WITHIN the pair across
   *  its two walks and are never combined with the other pair's. */
  final case class PairTraversal(
      pairId: String,
      walks: List[WalkResult],
      distinctRoutes: List[RouteRecord],
      boundReached: Boolean,
      /** The most routes this pair's walks could retain on this graph: one per
       *  (walk, entry point, sink host). Published so a reader can see whether
       *  MAX_ROUTES_PER_PAIR could bind at all rather than deriving it from
       *  three other numbers. */
      routeCapReachableMaximum: Int,
      routeCapCanBind: Boolean,
      alternateSinkArrivalsNotRetained: Int,
      invoked: Boolean)

  val traversals: List[PairTraversal] = selections.map { s =>
    val p = s.pair
    // ONE step budget per pair, handed to both of that pair's walks, because
    // MAX_STEPS_PER_PAIR is a per-pair cap. Nothing is shared between pairs.
    val pairSteps = new PairStepBudget
    val nanosA = System.nanoTime()
    val walkA = walk(s, WALK_A_ID, followFanOut = true, pairSteps)
    log(s"pair ${p.id}: walk $WALK_A_ID elapsed_ms=${elapsedMs(nanosA)}")
    val nanosB = System.nanoTime()
    val walkB = walk(s, WALK_B_ID, followFanOut = false, pairSteps)
    log(s"pair ${p.id}: walk $WALK_B_ID elapsed_ms=${elapsedMs(nanosB)}")
    val walks = List(walkA, walkB)
    log(s"pair ${p.id}: pair step budget used ${pairSteps.used} of " +
      s"$MAX_STEPS_PER_PAIR across both walks, exhausted=${pairSteps.exhausted}")
    walks.foreach { w =>
      log(s"pair ${p.id}: walk ${w.walkId}: routes=${w.routes.size} " +
        s"expansions=${w.expansions} call_sites=${w.callSitesConsidered} " +
        s"fanout_seen=${w.fanOutSitesEncountered} " +
        s"fanout_not_followed=${w.fanOutSitesNotFollowed} max_depth=${w.maxDepthUsed} " +
        s"depth_bound_reached=${w.depthBoundReached} " +
        s"max_expansions_at_one_entry=${w.maxExpansionsAtOneEntry} " +
        s"entry_expansion_cap_reached=${w.entryExpansionCapReached} " +
        s"pair_step_budget_exhausted=${w.pairStepBudgetExhausted} " +
        s"route_cap_reached=${w.routeCapReached} " +
        s"alternate_sink_arrivals_not_retained=${w.alternateSinkArrivalsNotRetained}")
    }
    // Deduplicated on the full hop sequence, and then sorted on the COMPLETE
    // published tuple - pair id, walk id, endpoints, hop count and the whole
    // ordered hop sequence rendered as one key. The hop sequence is the last
    // component precisely so that two routes sharing their endpoints and their
    // hop count still have a defined order: no two records can share the whole
    // key, so the order is TOTAL rather than merely stable.
    val distinct = walks
      .flatMap(_.routes)
      // Deduplicated on hopSequenceKey rather than on a hand-rolled triple, so
      // the identity a route is collapsed on is exactly the identity the sort
      // key below and the published record use. The triple this replaced
      // omitted callSiteLine, so two routes crossing the same caller/callee
      // pair at two different source lines collapsed into one before the sort
      // saw them, while the comment above claimed the full hop sequence.
      .distinctBy(r => (r.entryPoint, r.sinkHost, hopSequenceKey(r)))
      .sortBy(r => (r.pairId, r.walkId, r.entryPoint, r.sinkHost, r.hops.size,
        hopSequenceKey(r)))
    val boundReached = walks.exists(w =>
      w.depthBoundReached || w.entryExpansionCapReached || w.pairStepBudgetExhausted ||
        w.routeCapReached)
    // What the per-pair route cap could bind on: one retained route per (walk,
    // entry point, sink host). Measured rather than assumed, because a cap that
    // cannot be reached on this graph is a fact about the cap and is published
    // as one instead of leaving `route_cap_reached=false` to be read as evidence
    // that the traversal was complete.
    val routeCapReachableMaximum =
      walks.size * s.entryPointsTraversed * s.sinkHostNames.size
    val routeCapCanBind = routeCapReachableMaximum > MAX_ROUTES_PER_PAIR
    val alternateArrivals = walks.map(_.alternateSinkArrivalsNotRetained).sum
    log(s"pair ${p.id}: distinct routes (its own two walks, deduplicated): " +
      s"${distinct.size}")
    log(s"pair ${p.id}: route cap reachable max $routeCapReachableMaximum " +
      s"(${walks.size} walk(s) x ${s.entryPointsTraversed} entry point(s) x " +
      s"${s.sinkHostNames.size} sink host(s)) against a per-pair cap of " +
      s"$MAX_ROUTES_PER_PAIR, so the cap can bind: $routeCapCanBind")
    log(s"pair ${p.id}: alternate sink arrivals counted and not retained: " +
      s"$alternateArrivals")
    log(s"pair ${p.id}: any bound reached: $boundReached")
    PairTraversal(p.id, walks, distinct, boundReached, routeCapReachableMaximum,
      routeCapCanBind, alternateArrivals, invoked = true)
  }
  /** The invocation record for the parameterizability measure: every declared
   *  pair must have produced a traversal, and the walks must have run. */
  if (traversals.size != PAIRS.size || traversals.exists(!_.invoked)) {
    abortRun(s"not every declared pair was invoked: ${PAIRS.size} pairs declared, " +
      s"${traversals.count(_.invoked)} invoked. The parameterizability measure is " +
      "decided by the second pair's invocation actually running, so an uninvoked pair " +
      "stops the run rather than being reported as a pass")
  }
  log(s"pairs invoked             : ${traversals.count(_.invoked)} of ${PAIRS.size} " +
    s"(${traversals.map(_.pairId).mkString(", ")})")
  log("distinct routes are reported PER PAIR and are never summed across pairs, and " +
    "never added to query 01's or 02's returns.")


  // -------------------------------------------------------------------------
  stage("I-boundaries: every hop on every pair's route, measured once and cited")
  // -------------------------------------------------------------------------
  // A hop that lies on the part of the route the two pairs SHARE is measured
  // ONCE and cited by both pairs (AAP 0.6.4: a count appearing in two places is
  // one measurement cited twice, never two measurements). A hop that differs
  // between the pairs - the partial-function hop does - is measured per pair.
  final case class BoundaryRecord(
      id: String,
      kind: String,
      hop: String,
      fromEnd: String,
      toEnd: String,
      reason: String,
      modelling: String,
      crossedByACallEdge: Boolean,
      measured: List[(String, String)])

  val boundaryStore = scala.collection.mutable.LinkedHashMap.empty[String, BoundaryRecord]
  val boundaryCitations =
    scala.collection.mutable.LinkedHashMap.empty[String, scala.collection.mutable.ArrayBuffer[String]]
  def citeBoundary(pairId: String, id: String)(build: => BoundaryRecord): Unit = {
    if (!boundaryStore.contains(id)) boundaryStore(id) = build
    val cites = boundaryCitations.getOrElseUpdate(
      id, scala.collection.mutable.ArrayBuffer.empty[String])
    if (!cites.contains(pairId)) cites += pairId
  }

  /** The RPC hop, modelled explicitly by pairing on the MESSAGE TYPE. */
  def messageBoundary(h: MessageHop): BoundaryRecord = {
    val typeDecls = boundedList(
      s"boundary B-rpc-${h.id}: type declarations for ${h.messageType}",
      "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"type declarations whose full name is exactly ${h.messageType}")(
      cpg.typeDecl.fullNameExact(h.messageType))
    if (typeDecls.isEmpty) {
      abortRun(s"message type ${h.messageType} (hop ${h.id}) is not present in the graph, " +
        "so the hop it models cannot be measured and the route count would be " +
        "uninterpretable")
    }
    val methods = boundedList(
      s"boundary B-rpc-${h.id}: methods on ${h.messageType}",
      "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      "every method declared on that message type, before the constructor and " +
        "accessor filters")(typeDecls.iterator.flatMap(_.method))
    val ctors = methods.filter(_.name == h.ctorName)
    val accessors = methods.filter(m => h.accessorNames.contains(m.name))
    if (ctors.isEmpty) {
      abortRun(s"message type ${h.messageType} carries no ${h.ctorName}, so the producer " +
        "end of hop " + h.id + " cannot be measured")
    }
    if (accessors.isEmpty) {
      abortRun(s"message type ${h.messageType} carries none of the declared accessors " +
        s"(${h.accessorNames.mkString(", ")}), so the consumer end of hop ${h.id} cannot " +
        "be measured")
    }
    /** Call sites inside the message type or its companion are the case class's own
     *  generated machinery (apply, copy, unapply, equals, productElement), not a
     *  send or a receive. They are excluded by owning type so the producer and
     *  consumer sets are the two real ends of the hop. */
    val ownTypes = Set(h.messageType, h.messageType + "$")
    def outsideMessageType(m: Method): Boolean = !owningTypes(m).exists(ownTypes.contains)
    val producerSites = boundedList(
      s"boundary B-rpc-${h.id}: incoming call sites of ${h.ctorName}",
      "MAX_CALL_SCAN", MAX_CALL_SCAN,
      s"every call site whose callee is the message type's ${h.ctorName}, before the " +
        "own-type exclusion")(ctors.iterator.flatMap(_.callIn))
      .filter(c => outsideMessageType(c.method))
      .distinctBy(c => (c.method.fullName, lineOf(c)))
      .sortBy(c => (c.method.fullName, lineOf(c)))
    val consumerSites = boundedList(
      s"boundary B-rpc-${h.id}: incoming call sites of the declared accessors",
      "MAX_CALL_SCAN", MAX_CALL_SCAN,
      s"every call site whose callee is one of the accessors " +
        s"(${h.accessorNames.mkString(", ")}), before the own-type exclusion")(
      accessors.iterator.flatMap(_.callIn))
      .filter(c => outsideMessageType(c.method))
      .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
      .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    val producers = producerSites.map(_.method.fullName).distinct.sorted
    val consumers = consumerSites.map(_.method.fullName).distinct.sorted
    val producerToConsumerEdge = producerSites
      .map(_.method)
      .distinctBy(_.fullName)
      .exists { p =>
        callSitesOf(List(p)).flatMap(calleesOf).map(_.fullName).exists(consumers.contains)
      }
    /** Which pairs' declared handlers sit at each end. For the servlet's hop this
     *  is the whole evidence for the model: the producer end IS pair two's entry
     *  point and the consumer end IS pair one's, so pairing on the message type
     *  joins one pair's handler to the other's - measured, not assumed. */
    val producerEndIsEntryOf = selections
      .filter(s => s.entryGroups.map(_._1).exists(producers.contains))
      .map(_.pair.id)
    val consumerEndIsEntryOf = selections
      .filter(s => s.entryGroups.map(_._1).exists(consumers.contains))
      .map(_.pair.id)
    log(s"boundary B-rpc-${h.id}: producers=${producers.mkString(", ")}")
    log(s"boundary B-rpc-${h.id}: consumers=${consumers.mkString(", ")}")
    log(s"boundary B-rpc-${h.id}: producer->consumer call edge=$producerToConsumerEdge")
    log(s"boundary B-rpc-${h.id}: producer end is the declared entry point of " +
      (if (producerEndIsEntryOf.isEmpty) "no pair" else producerEndIsEntryOf.mkString(", ")))
    log(s"boundary B-rpc-${h.id}: consumer end is the declared entry point of " +
      (if (consumerEndIsEntryOf.isEmpty) "no pair" else consumerEndIsEntryOf.mkString(", ")))
    BoundaryRecord(
      id = "B-rpc-" + h.id,
      kind = "rpc",
      hop = "RpcEndpointRef send of " + h.messageType + ", " + h.direction +
        ", at " + h.sourceAnchor,
      fromEnd = producers.mkString(", "),
      toEnd = consumers.mkString(", "),
      reason = "a message send is not a call: the sender enqueues a value on an " +
        "endpoint reference and the receiving handler is dispatched later, so no CALL " +
        "edge joins the two ends",
      modelling = "modelled explicitly by pairing on the MESSAGE TYPE - call sites of " +
        h.messageType + "." + h.ctorName + " are the producer end and call sites of its " +
        "field accessors (" + h.accessorNames.mkString(", ") + ") are the consumer end, " +
        "with the message type's and companion's own generated machinery excluded by " +
        "owning type",
      crossedByACallEdge = producerToConsumerEdge,
      measured = List(
        "message_type" -> jstr(h.messageType),
        "direction" -> jstr(h.direction),
        "source_anchor_at_the_pin" -> jstr(h.sourceAnchor),
        "producer_call_sites" -> jnum(producerSites.size.toLong),
        "consumer_call_sites" -> jnum(consumerSites.size.toLong),
        "producers" -> jstrArr(producers),
        "consumers" -> jstrArr(consumers),
        "producer_call_site_graph_lines" ->
          jstrArr(producerSites.map(c => s"${c.method.fullName}#${lineOf(c)}")),
        "consumer_call_site_graph_lines" ->
          jstrArr(consumerSites.map(c => s"${c.method.fullName}#${lineOf(c)}").distinct.sorted),
        "direct_call_edge_producer_to_consumer" -> jbool(producerToConsumerEdge),
        "producer_end_is_the_declared_entry_point_of" -> jstrArr(producerEndIsEntryOf),
        "consumer_end_is_the_declared_entry_point_of" -> jstrArr(consumerEndIsEntryOf)))
  }

  /** The thread hop, on the shared part of both routes. */
  def threadBoundary(): BoundaryRecord = {
    val threadHostMethods = boundedList(
      "boundary B-thread: thread host methods", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"methods named $THREAD_HOST_METHOD declared on $THREAD_HOST_TYPE")(
      cpg.typeDecl.fullNameExact(THREAD_HOST_TYPE).method.nameExact(THREAD_HOST_METHOD))
    val threadStartSites = callSitesOf(threadHostMethods).filter(_.name == THREAD_HOST_METHOD)
    val threadStartCallees =
      threadStartSites.flatMap(calleesOf).map(_.fullName).distinct.sorted
    val threadBodyMethods = boundedList(
      "boundary B-thread: thread body methods", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"methods named $THREAD_BODY_METHOD on type declarations matching " +
        THREAD_BODY_TYPE_REGEX)(
      cpg.typeDecl.fullName(THREAD_BODY_TYPE_REGEX).method.nameExact(THREAD_BODY_METHOD))
    val threadBodyNames = threadBodyMethods.map(_.fullName).distinct.sorted
    val crossed = threadStartCallees.exists(threadBodyNames.contains)
    log(s"boundary B-thread: Thread.start call sites=${threadStartSites.size} " +
      s"callees=${threadStartCallees.mkString(", ")}")
    log(s"boundary B-thread: thread body methods=${threadBodyNames.mkString(", ")}")
    log(s"boundary B-thread: start->run call edge=$crossed")
    BoundaryRecord(
      id = "B-thread",
      kind = "thread",
      hop = THREAD_HOST_TYPE + "." + THREAD_HOST_METHOD + " calls Thread.start(); the " +
        "route continues in " + THREAD_BODY_METHOD + "() on the anonymous Thread " +
        "subclass (DriverRunner.scala:123 and :90 at the pin)",
      fromEnd = threadStartSites.map(_.method.fullName).distinct.sorted.mkString(", "),
      toEnd = threadBodyNames.mkString(", "),
      reason = "Thread.start() -> run() is a JVM scheduling relation, not a call: the " +
        "start frame returns immediately and run() is entered on another thread, so no " +
        "CALL edge joins them",
      modelling = "not modelled - the two ends are reported as measured and the hop is " +
        "left uncrossed",
      crossedByACallEdge = crossed,
      measured = List(
        "thread_start_call_sites" -> jnum(threadStartSites.size.toLong),
        "thread_start_call_site_graph_lines" ->
          jstrArr(threadStartSites.map(c => s"${c.method.fullName}#${lineOf(c)}").distinct.sorted),
        "thread_start_dispatch_types" ->
          jstrArr(threadStartSites.map(_.dispatchType).distinct.sorted),
        "callees_of_thread_start" -> jstrArr(threadStartCallees),
        "thread_body_methods" -> jstrArr(threadBodyNames),
        "call_edge_start_to_run" -> jbool(crossed)))
  }

  /** The interface hop at the sink, on the shared part of both routes. */
  def interfaceBoundary(sinkCalls: List[Call]): BoundaryRecord = {
    val sinkCallCallees = sinkCalls.flatMap(calleesOf).map(_.fullName).distinct.sorted
    val abstractLaunchNames =
      sinkCallCallees.filter(_.startsWith(ABSTRACT_LAUNCH_CALLEE_PREFIX))
    val concreteLaunchNames =
      sinkCallCallees.filterNot(_.startsWith(ABSTRACT_LAUNCH_CALLEE_PREFIX))
    val jdkLaunchMethodNodes = boundedList(
      "boundary B-interface: JDK launch method declarations", "MAX_TYPE_SCAN",
      MAX_TYPE_SCAN,
      s"methods whose full name is exactly $JDK_LAUNCH_METHOD_FULL_NAME")(
      cpg.method.fullNameExact(JDK_LAUNCH_METHOD_FULL_NAME))
    val crossed = concreteLaunchNames.nonEmpty
    log(s"boundary B-interface: sink call callees=${sinkCallCallees.mkString(", ")}")
    log(s"boundary B-interface: concrete implementations reached=" +
      s"${concreteLaunchNames.mkString(", ")}")
    log(s"boundary B-interface: abstract declarations reached=" +
      s"${abstractLaunchNames.mkString(", ")}")
    log(s"boundary B-interface: jdk launch method nodes=${jdkLaunchMethodNodes.size}")
    log(s"boundary B-interface: interface->implementation call edge=$crossed")
    BoundaryRecord(
      id = "B-interface",
      kind = "interface",
      hop = "the launch call site invokes the ABSTRACT ProcessBuilderLike.start " +
        "(DriverRunner.scala:270 at the pin); the JDK launch is reached only through " +
        "the anonymous implementation at :276",
      fromEnd = sinkCalls.map(_.method.fullName).distinct.sorted.mkString(", "),
      toEnd = concreteLaunchNames.mkString(", "),
      reason = "an interface invocation names the declaring type, so linking it to an " +
        "implementation needs the type hierarchy rather than the call's own name",
      modelling = "not modelled by this query - whether the hop is crossed is a property " +
        "of the graph's call linker and is reported as measured",
      crossedByACallEdge = crossed,
      measured = List(
        "sink_call_sites" -> jnum(sinkCalls.size.toLong),
        "sink_call_dispatch_types" -> jstrArr(sinkCalls.map(_.dispatchType).distinct.sorted),
        "callees_of_sink_call_sites" -> jstrArr(sinkCallCallees),
        "abstract_declarations_reached" -> jstrArr(abstractLaunchNames),
        "concrete_implementations_reached" -> jstrArr(concreteLaunchNames),
        "jdk_launch_method_nodes_present" -> jnum(jdkLaunchMethodNodes.size.toLong),
        "call_edge_interface_to_implementation" -> jbool(crossed)))
  }

  /** The partial-function hop, measured PER PAIR because the pairs differ here. */
  def partialFunctionBoundary(s: PairSelection): BoundaryRecord = {
    val p = s.pair
    val syntheticNames = s.syntheticEntryNodes.map(_.fullName).distinct.sorted
    val sourceLevelNames = s.sourceLevelNodes.map(_.fullName).distinct.sorted
    val resolution =
      if (syntheticNames.nonEmpty && s.syntheticCarriesBody)
        "the SYNTHETIC arm carries the route: the handler returns a partial function, " +
          "its body compiles into " + syntheticNames.mkString(", ") + ", and the " +
          "source-level method of the same name only constructs the partial function"
      else if (syntheticNames.isEmpty && s.sourceLevelCarriesBody)
        "the SOURCE-LEVEL arm carries the route: no type matching " +
          p.handlerSyntheticTypeRegex + " exists, because this handler is an ordinary " +
          "method rather than a partial function, so its body is its own and the " +
          "synthetic arm is legitimately empty"
      else if (syntheticNames.nonEmpty && s.sourceLevelCarriesBody)
        "BOTH arms resolved and the source-level arm carries the declared body witness; " +
          "both are traversed and both are reported"
      else
        "neither arm's own call sites contain the declared body witness; both arms are " +
          "traversed as selected and the condition is reported as measured"
    log(s"boundary B-partial-function-${p.id}: synthetic=${syntheticNames.size} " +
      s"source_level=${sourceLevelNames.size} resolution=$resolution")
    BoundaryRecord(
      id = "B-partial-function-" + p.id,
      kind = "partial-function",
      hop = p.handlerType + "." + p.handlerMethod + " (" + p.handlerSourceFile + ":" +
        p.handlerSourceLine + " at the pin): where the graph carries this handler's body",
      fromEnd = sourceLevelNames.mkString(", "),
      toEnd = syntheticNames.mkString(", "),
      reason = "a Scala handler that returns PartialFunction[Any, Unit] compiles its case " +
        "bodies into a synthetic class, so for that shape the graph's entry point is the " +
        "synthetic " + p.handlerSyntheticMethod + " and NOT a method of the handler's own " +
        "name; an ordinary method has no such class at all",
      modelling = "modelled by selecting BOTH arms - the synthetic " +
        p.handlerSyntheticMethod + " on every type matching " +
        p.handlerSyntheticTypeRegex + ", and the source-level " + p.handlerMethod +
        " on " + p.handlerType + " - and then MEASURING which of them carries the " +
        "declared body witness '" + p.handlerBodyWitness + "'. Resolved here as: " +
        resolution + ". The crossed flag is read strictly: it is true only where a CALL " +
        "edge joins the source-level method to the body, which is the hop this boundary " +
        "names. Where no synthetic class exists the hop does not arise at all, and the " +
        "flag then simply records that the source-level method - which IS the body - " +
        "reaches it; hop_arises_for_this_handler distinguishes the two cases",
      // Strictly what the field name says: does a CALL edge join the SOURCE-LEVEL
      // method to the handler's body? For a partial-function handler it does not,
      // which is the whole point of the boundary. For a handler with no synthetic
      // class the hop does not arise, and the source-level method is the body.
      crossedByACallEdge = s.sourceLevelCarriesBody,
      measured = List(
        "hop_arises_for_this_handler" -> jbool(s.syntheticTypeNames.nonEmpty),
        "synthetic_class_exists_for_this_handler" -> jbool(s.syntheticTypeNames.nonEmpty),
        "synthetic_type_matches" -> jnum(s.syntheticTypeNames.size.toLong),
        "synthetic_types" -> jstrArr(s.syntheticTypeNames),
        "synthetic_entry_methods" -> jstrArr(syntheticNames),
        "source_level_methods" -> jstrArr(sourceLevelNames),
        "base_declarations_excluded_by_the_type_selector" -> jstrArr(s.baseDeclarationNames),
        "synthetic_arm_call_site_count" -> jnum(s.syntheticCallees.size.toLong),
        "source_level_arm_call_site_count" -> jnum(s.sourceLevelCallees.size.toLong),
        "body_witness" -> jstr(p.handlerBodyWitness),
        "body_witness_found_in_the_synthetic_arm" -> jbool(s.syntheticCarriesBody),
        "body_witness_found_in_the_source_level_arm" -> jbool(s.sourceLevelCarriesBody),
        "selector_resolution" -> jstr(resolution)))
  }

  // The shared sink measurement, asserted shared rather than assumed: both pairs
  // declare the same sink selectors, so the interface hop is ONE measurement.
  val sinkSelectorKeys = selections
    .map(s => (s.pair.sinkCalleeRegex, s.pair.sinkCallName, s.pair.sinkHostTypeRegex))
    .distinct
  val sinkCallSetKeys = selections
    .map(s => s.sinkCalls.map(c => (c.method.fullName, c.methodFullName, lineOf(c))))
    .distinct
  val sinkIsShared = sinkSelectorKeys.size == 1 && sinkCallSetKeys.size == 1
  log(s"sink selectors distinct across pairs: ${sinkSelectorKeys.size}; resolved sink " +
    s"call sets distinct across pairs: ${sinkCallSetKeys.size}; sink shared: $sinkIsShared")

  PAIRS.zip(selections).foreach { case (p, s) =>
    p.messageHops.foreach(h => citeBoundary(p.id, "B-rpc-" + h.id)(messageBoundary(h)))
    citeBoundary(p.id, "B-thread")(threadBoundary())
    citeBoundary(p.id, "B-interface")(interfaceBoundary(s.sinkCalls))
    citeBoundary(p.id, "B-partial-function-" + p.id)(partialFunctionBoundary(s))
  }

  val boundaryIdsByPair: Map[String, List[String]] = PAIRS.map { p =>
    p.id -> (p.messageHops.map(h => "B-rpc-" + h.id) ++
      List("B-thread", "B-interface", "B-partial-function-" + p.id))
  }.toMap
  val boundaries = boundaryStore.values.toList
  def citationsOf(id: String): List[String] =
    boundaryCitations.get(id).map(_.toList).getOrElse(Nil)
  PAIRS.foreach { p =>
    val ids = boundaryIdsByPair(p.id)
    val notCrossed = ids.filter(id => !boundaryStore(id).crossedByACallEdge)
    log(s"pair ${p.id}: boundaries=${ids.size} (${ids.mkString(", ")})")
    log(s"pair ${p.id}: boundaries NOT crossed by a call edge=${notCrossed.mkString(", ")}")
  }
  log(s"boundary measurements taken: ${boundaries.size} for " +
    s"${boundaryIdsByPair.values.map(_.size).sum} pair-boundary citations - the shared " +
    "hops are one measurement cited by both pairs, never measured twice")

  // ---------------- the route surface, CHECKED against the measured route -----
  // The surface is DERIVED from this query's own route ends, and that derivation
  // is asserted here against what was actually measured rather than left as a
  // claim beside the declaration. Every end of every pair's route - the entry
  // points, the consumer end of each pair's RPC hop, and the sink hosts - must be
  // owned by a type on the surface, or the structural basis for the
  // expected-spurious absence would rest on a surface the route leaves.
  val measuredRouteEnds: List[(String, String, String)] =
    PAIRS.zip(selections).flatMap { case (p, s) =>
      s.entryGroups.map(g => (p.id, "entry point", g._1)) :::
        p.messageHops.flatMap { h =>
          boundaryStore("B-rpc-" + h.id).toEnd
            .split(", ").toList.map(_.trim).filter(_.nonEmpty)
            .map(c => (p.id, s"B-rpc-${h.id} consumer end (the relay)", c))
        } :::
        s.sinkHostNames.toList.sorted.map(h => (p.id, "sink host", h))
    }
  val routeEndsNotCovered = measuredRouteEnds.filterNot { case (_, _, fullName) =>
    ROUTE_SURFACE_TYPE_PREFIXES.exists(pref => fullName.startsWith(pref))
  }
  measuredRouteEnds.foreach { case (pairId, role, fullName) =>
    log(s"  route end ($pairId, $role): $fullName")
  }
  if (routeEndsNotCovered.nonEmpty) {
    abortRun("a measured route end is not covered by any route-surface prefix: " +
      routeEndsNotCovered.map { case (pairId, role, fn) => s"$pairId $role $fn" }
        .mkString("; ") + ". The predicate evidence would then describe a surface the " +
      "route leaves, and a zero drawn from it would read as a searched surface that " +
      "came back clean")
  }
  log(s"route surface coverage    : PASS - ${measuredRouteEnds.size} measured route " +
    s"end(s) across ${PAIRS.size} pair(s), every one owned by a type on the " +
    s"${ROUTE_SURFACE_TYPE_PREFIXES.size}-prefix derived surface")
  /** Per prefix on the derived query-wide surface: what role it plays, how much
   *  of the graph it owns, which measured ends of which pair it accounts for, and
   *  how many predicate call sites sit on it. Published so each prefix earns its
   *  place from the measurement rather than from the derivation being taken on
   *  trust, and so a prefix that accounts for no measured end is visible as a
   *  zero rather than being invisible inside a covered surface. */
  /** Prefixes on the derived surface that account for NO measured route end. Not a
   *  defect and not hidden: a type the route reaches before an end - pair two's
   *  REST server is the case here - earns its place on the surface from the
   *  derivation while owning no end, and publishing the zero is what keeps the
   *  coverage assertion from reading as though every prefix were an end. */
  val routeSurfacePrefixesAccountingForNoEnd: List[String] =
    ROUTE_SURFACE_TYPE_PREFIXES
      .filterNot(pref => measuredRouteEnds.exists(_._3.startsWith(pref)))
  log(s"route surface prefixes owning no measured end: " +
    (if (routeSurfacePrefixesAccountingForNoEnd.isEmpty) "none"
     else routeSurfacePrefixesAccountingForNoEnd.mkString(", ")))
  val routeSurfaceReach: List[String] = ROUTE_SURFACE_TYPE_PREFIXES.map { prefix =>
    val role = ROUTE_SURFACE_TYPE_ROLES.toMap.getOrElse(prefix, "unstated")
    val ev = surfacePrefixEvidence.find(_.prefix == prefix)
    val endsAccountedFor = measuredRouteEnds.filter(_._3.startsWith(prefix))
    log(s"  surface $prefix: role=$role " +
      s"typeDecls=${ev.map(_.typeDeclsInTheGraph).getOrElse(0)} " +
      s"measured_ends=${endsAccountedFor.size} " +
      s"predicate_call_sites=${ev.map(_.predicateCallSites).getOrElse(0)}")
    jobj(6, List(
      "type_prefix" -> jstr(prefix),
      "role_on_this_route" -> jstr(role),
      "type_declarations_owned" ->
        jnum(ev.map(_.typeDeclsInTheGraph).getOrElse(0).toLong),
      "measured_route_ends_accounted_for" -> jnum(endsAccountedFor.size.toLong),
      "measured_route_ends" -> jstrArr(endsAccountedFor.map {
        case (pairId, endRole, fn) => s"$pairId $endRole: $fn" }.distinct.sorted),
      "predicate_call_sites_here" -> jnum(ev.map(_.predicateCallSites).getOrElse(0).toLong),
      "measured_on_pairs" -> jstrArr(endsAccountedFor.map(_._1).distinct.sorted),
      "reached_by_the_route" -> jbool(endsAccountedFor.nonEmpty)))
  }


  // -------------------------------------------------------------------------
  stage("J-spurious: the mechanical definition, applied per pair to its own set")
  // -------------------------------------------------------------------------
  // A route is spurious ONLY where it passes one of the five named predicates
  // before reaching the sink. Nothing else makes a route spurious, and this
  // judges the QUERY's own output - it says nothing about Spark, about any Spark
  // component or about any configuration.
  val predicateCallerNameSet = predicateCallerNames.toSet
  def routeMethods(r: RouteRecord): List[String] =
    (r.entryPoint :: r.hops.map(_.toMethod)).distinct
  def routeIsSpurious(r: RouteRecord): Boolean =
    routeMethods(r).exists(predicateCallerNameSet.contains)

  final case class PairSpurious(
      pairId: String,
      routesConsidered: Int,
      spuriousCount: Int,
      expectedSpuriousAbsent: Boolean,
      absenceIsStructural: Boolean,
      predicateCallSitesOnOwnSurface: Int,
      basis: String)

  val spuriousByPair: List[PairSpurious] = PAIRS.zip(traversals).map { case (p, t) =>
    val spurious = t.distinctRoutes.count(routeIsSpurious)
    val onOwn = predicateCallSitesByPair(p.id).size
    val structural = onOwn == 0
    val basis =
      if (structural)
        "structural - no call site of any of the five named predicates exists on this " +
          "pair's own route surface (" + p.routeSurfaceTypePrefixes.mkString(", ") +
          "), so no route of this pair could have passed one. The predicate set exists " +
          "and is invoked elsewhere in the program: " + predicateCallSites.size +
          " call sites graph-wide in " + predicateCallerNames.size + " distinct callers"
      else
        "filtering - " + onOwn.toString + " call site(s) of the five named predicates DO " +
          "exist on this pair's own route surface, so the count reflects this query's " +
          "filtering rather than a structural absence"
    log(s"pair ${p.id}: spurious routes=$spurious of ${t.distinctRoutes.size}; " +
      s"expected-spurious absent=${spurious == 0}; basis=" +
      (if (structural) "structural" else "filtering") +
      s" (predicate call sites on its own surface=$onOwn)")
    PairSpurious(p.id, t.distinctRoutes.size, spurious, spurious == 0, structural, onOwn, basis)
  }
  log("The permitted statement, per pair: no route in the emitted set passed an auth/ACL " +
    "predicate as defined by these five named selectors. That is a statement about this " +
    "query's own output under this query's own definition of the term, and about nothing " +
    "else.")

  // -------------------------------------------------------------------------
  stage("K-duplicate-formulation: the verdict against 01 AND 02, on evidence")
  // -------------------------------------------------------------------------
  // The question this query raises and must answer: instantiated on PAIR ONE, is
  // this the same formulation as query 01 (call-graph), the same as query 02
  // (dataflow), or a third? It is answered by COMPARING THE TWO SOURCES rather
  // than by restating either sibling's published numbers, and if this query IS
  // one of them restated at some scope then that is said plainly - a legitimate
  // probe finding, not a defect to hide.
  val apiConstructsHere = JOERN_API_CONSTRUCTS.distinct.sorted
  log(s"API constructs here       : ${apiConstructsHere.size}")

  /** Query 01 numbered the four hops it measured B1..B4. This query names them
   *  after the hop, and the same hop must be comparable across the two, so the
   *  translation is declared rather than left to a reader. It is a naming map,
   *  not a transcribed measurement: no verdict is drawn from it. */
  val BOUNDARY_ID_TO_SIBLING_01 = Map(
    "B-rpc-" + MESSAGE_HOP_LAUNCH_DRIVER_ID -> "B1-rpc",
    "B-thread" -> "B2-thread",
    "B-interface" -> "B3-interface",
    "B-partial-function-" + PAIR_ONE_ID -> "B4-partial-function")

  // Pair one's own figures, MEASURED here against this host's graph. They are
  // published as this query's measurements and are not compared against any
  // sibling's published number: a sibling measured its own against the graph of
  // its own run, and this query does not re-measure that.
  val pairOneTraversal = traversals.head
  val pairOneSelection = selections.head
  val pairOneBoundaryIds = boundaryIdsByPair(PAIR_ONE_ID)
  val pairOneNotCrossedHere = pairOneBoundaryIds
    .filter(id => !boundaryStore(id).crossedByACallEdge)
    .flatMap(id => BOUNDARY_ID_TO_SIBLING_01.get(id))
    .sorted
  log(s"pair-one entry points     : ${pairOneSelection.entryGroups.size}")
  log(s"pair-one distinct routes  : ${pairOneTraversal.distinctRoutes.size}")
  log(s"pair-one boundaries not crossed (in 01's numbering): " +
    pairOneNotCrossedHere.mkString(", "))

  // ----------- the duplicate-formulation comparison, over the SOURCES -------
  // Whether two queries are the same formulation is a property of the two
  // QUERIES, not of either run's numbers, so it is answered from the two
  // sources' declared formulation identity blocks - read at run time, extracted
  // by one shared extractor, compared as literal text. Nothing about a sibling
  // is written down in this file, and both directions of every comparison are
  // computed from the same inputs by the same predicate, which is what makes
  // the relation symmetric by construction instead of by transcription.
  val DUPLICATE_STATUS_NOT_ESTABLISHED = "not_established"
  val DUPLICATE_STATUS_NOT_DUPLICATE = "not_duplicate"
  val DUPLICATE_STATUS_DUPLICATE = "duplicate_formulation"
  val DUPLICATE_STATUS_SCOPED_PREFIX = "duplicate_formulation_on_"
  val AGGREGATE_DUPLICATE = "duplicate"
  val AGGREGATE_PARTIAL = "partial_duplicate"

  final case class Formulation(
      queryId: String,
      sourceRepoRelative: String,
      established: Boolean,
      note: String,
      sourceSha256: String,
      sourceByteSize: Long,
      edgeKinds: List[String],
      endNodeKinds: List[String],
      pairIds: List[String],
      boundName: String,
      boundKind: String,
      boundValue: Long,
      traversalSemantics: String,
      entrySelectorNames: List[String],
      entrySelectorLiterals: List[String],
      sinkSelectorLiterals: List[String],
      predicateLiterals: List[String],
      apiConstructs: List[String])

  /** One query's declared formulation identity, read out of its own source.
   *  This query's own is read the same way, through this same function, so no
   *  side of any comparison below is privileged. */
  def formulationOf(qid: String): Formulation = {
    val rel = s"$QUERY_SOURCE_DIR/$qid.sc"
    val blank = Formulation(qid, rel, false, "", "", 0L, Nil, Nil, Nil, "", "", 0L, "",
      Nil, Nil, Nil, Nil, Nil)
    val p = repoRoot.resolve(rel)
    if (!Files.isRegularFile(p)) {
      blank.copy(note = s"not established: no query source is present at $rel, and a " +
        "verdict is not inferred from a file this run could not read")
    } else {
      val bytes = Files.readAllBytes(p)
      val text = new String(bytes, StandardCharsets.UTF_8)
      val edge = declaredLiteralsOf(text, "FORMULATION_EDGE_KINDS")
      val ends = declaredLiteralsOf(text, "FORMULATION_END_NODE_KINDS")
      val pairs = declaredLiteralsOf(text, "FORMULATION_PAIR_IDS")
      val boundName = declaredLiteralsOf(text, "FORMULATION_BOUND_NAME").map(_.mkString)
      val boundKind = declaredLiteralsOf(text, "FORMULATION_BOUND_KIND").map(_.mkString)
      val boundValue = declaredIntOf(text, "FORMULATION_BOUND_VALUE")
      val semantics =
        declaredLiteralsOf(text, "FORMULATION_TRAVERSAL_SEMANTICS").map(_.mkString)
      val entryNames =
        declaredLiteralsOf(text, "FORMULATION_ENTRY_SELECTOR_CONSTANT_NAMES")
      val sinkNames = declaredLiteralsOf(text, "FORMULATION_SINK_SELECTOR_CONSTANT_NAMES")
      val predNames = declaredLiteralsOf(text, "FORMULATION_PREDICATE_CONSTANT_NAMES")
      // Two steps, and deliberately so: the source under inspection names the
      // constant holding its own API construct list, and that name is then
      // resolved in the SAME source. Using this query's own name for it would
      // read a sibling's list through this file's vocabulary and would report
      // an empty list as a difference wherever a sibling named it something
      // else.
      val apiPointer =
        declaredLiteralsOf(text, "FORMULATION_API_CONSTRUCTS_CONSTANT_NAME")
          .map(_.mkString)
      val apiList = apiPointer.flatMap(n => declaredLiteralsOf(text, n))
      val missing = List(
        "FORMULATION_EDGE_KINDS" -> edge.isEmpty,
        "FORMULATION_END_NODE_KINDS" -> ends.isEmpty,
        "FORMULATION_PAIR_IDS" -> pairs.isEmpty,
        "FORMULATION_BOUND_NAME" -> boundName.isEmpty,
        "FORMULATION_BOUND_KIND" -> boundKind.isEmpty,
        "FORMULATION_BOUND_VALUE" -> boundValue.isEmpty,
        "FORMULATION_TRAVERSAL_SEMANTICS" -> semantics.isEmpty,
        "FORMULATION_ENTRY_SELECTOR_CONSTANT_NAMES" -> entryNames.isEmpty,
        "FORMULATION_SINK_SELECTOR_CONSTANT_NAMES" -> sinkNames.isEmpty,
        "FORMULATION_PREDICATE_CONSTANT_NAMES" -> predNames.isEmpty,
        "FORMULATION_API_CONSTRUCTS_CONSTANT_NAME" -> apiPointer.isEmpty,
        (apiPointer.getOrElse(FORMULATION_API_CONSTRUCTS_CONSTANT_NAME) +
          " (the API construct list the source above names)") -> apiList.isEmpty)
        .collect { case (n, true) => n }
      if (missing.nonEmpty) {
        blank.copy(note = s"not established: $rel declares no " + missing.mkString(", ") +
          ", so its formulation identity cannot be read and no verdict is inferred")
      } else {
        /** The literals a named list of constant names resolves to, in the
         *  order the names are declared in. An absent constant is named in
         *  place rather than dropped, so a comparison can never silently
         *  succeed on a short list. */
        def literalsFor(names: List[String]): List[String] =
          names.map(n =>
            declaredLiteralsOf(text, n).map(_.mkString)
              .getOrElse(s"<not declared in $rel: $n>"))
        Formulation(
          queryId = qid,
          sourceRepoRelative = rel,
          established = true,
          note = "measured from the source text at run time",
          sourceSha256 = sha256OfBytes(bytes),
          sourceByteSize = bytes.length.toLong,
          edgeKinds = edge.get,
          endNodeKinds = ends.get,
          pairIds = pairs.get,
          boundName = boundName.get,
          boundKind = boundKind.get,
          boundValue = boundValue.get,
          traversalSemantics = semantics.get,
          entrySelectorNames = entryNames.get,
          entrySelectorLiterals = literalsFor(entryNames.get),
          sinkSelectorLiterals = literalsFor(sinkNames.get),
          predicateLiterals = literalsFor(predNames.get),
          apiConstructs = apiList.get.distinct.sorted)
      }
    }
  }

  final case class Relation(
      theirs: Formulation,
      status: String,
      scope: String,
      basis: String,
      sharedPairs: List[String],
      sameEdgeKinds: Boolean,
      sameEndNodeKinds: Boolean,
      sameEntrySelectors: Boolean,
      sameSinkSelectors: Boolean,
      samePredicateSelectors: Boolean,
      sameBoundKind: Boolean,
      sameBoundValue: Boolean,
      apiOnlyHere: List[String],
      apiOnlyThere: List[String],
      apiShared: List[String],
      coversEveryPair: Boolean)

  /** THE shared predicate. Two formulations are the same formulation, at the
   *  scope of the pairs they both address, when every component below agrees;
   *  the predicate set is deliberately NOT a component, because it defines the
   *  term "spurious" rather than the traversal, and it is reported separately. */
  def relate(mine: Formulation, theirs: Formulation): Relation = {
    if (!theirs.established) {
      Relation(theirs, DUPLICATE_STATUS_NOT_ESTABLISHED, "none",
        "no verdict is inferred: " + theirs.note, Nil,
        false, false, false, false, false, false, false, Nil, Nil, Nil, false)
    } else {
      val shared = mine.pairIds.filter(theirs.pairIds.contains)
      val sameEdge = mine.edgeKinds == theirs.edgeKinds
      val sameEnds = mine.endNodeKinds == theirs.endNodeKinds
      val sameEntry = mine.entrySelectorLiterals == theirs.entrySelectorLiterals
      val sameSink = mine.sinkSelectorLiterals == theirs.sinkSelectorLiterals
      val samePred = mine.predicateLiterals == theirs.predicateLiterals
      val sameBoundKind = mine.boundKind == theirs.boundKind
      val sameBoundValue = mine.boundValue == theirs.boundValue
      val onlyHere = mine.apiConstructs.filterNot(theirs.apiConstructs.contains)
      val onlyThere = theirs.apiConstructs.filterNot(mine.apiConstructs.contains)
      val shareds = mine.apiConstructs.filter(theirs.apiConstructs.contains)
      val apiIdentical = onlyHere.isEmpty && onlyThere.isEmpty
      // COMPONENT LABELS ARE POLARITY-NEUTRAL, and the detail that follows each
      // one is rendered from the measured boolean.
      //
      // The form this replaces paired each boolean with a label whose TEXT
      // asserted agreement - "byte for byte", "at the same value", "empty in
      // BOTH directions" - and then listed the FALSE ones after the words "the
      // formulations differ on". The published sentence therefore said that two
      // formulations differed on the bound being the same value, which reverses
      // the structured verdict sitting a few fields below it in the same
      // envelope. A reader trusting the prose read the opposite of the
      // measurement, and this query publishes a relation against BOTH siblings,
      // so the reversal was rendered twice in one document.
      //
      // So each component now carries a neutral NAME plus two details, and the
      // renderer picks the detail the measurement supports. The name is what
      // appears in either list; the detail is what makes the entry informative.
      final case class Component(agrees: Boolean, name: String,
        agreeDetail: String, differDetail: String)
      def rendered(c: Component): String =
        c.name + " (" + (if (c.agrees) c.agreeDetail else c.differDetail) + ")"
      val components = List(
        Component(sameEdge, "the edge kinds traversed",
          "both traverse " + mine.edgeKinds.mkString(", "),
          "this query traverses " + mine.edgeKinds.mkString(", ") + " where " +
            theirs.queryId + " traverses " + theirs.edgeKinds.mkString(", ")),
        Component(sameEnds, "the node kinds selected as a route's ends",
          "both select " + mine.endNodeKinds.mkString(", "),
          "this query selects " + mine.endNodeKinds.mkString(", ") + " where " +
            theirs.queryId + " selects " + theirs.endNodeKinds.mkString(", ")),
        Component(shared.nonEmpty, "the handler/sink pairs addressed",
          "at least one pair in common: " + shared.mkString(", "),
          "no pair in common: this query addresses " + mine.pairIds.mkString(", ") +
            " and " + theirs.queryId + " addresses " + theirs.pairIds.mkString(", ")),
        Component(sameEntry, "the entry-point selector literals",
          "identical byte for byte",
          "different as literal text: " + mine.entrySelectorLiterals.size +
            " literal(s) here against " + theirs.entrySelectorLiterals.size + " there"),
        Component(sameSink, "the sink selector literals",
          "identical byte for byte",
          "different as literal text: " + mine.sinkSelectorLiterals.size +
            " literal(s) here against " + theirs.sinkSelectorLiterals.size + " there"),
        Component(sameBoundKind && sameBoundValue,
          "the bound, as a named kind of quantity and a value",
          "both bound " + mine.boundKind + " at " + mine.boundValue,
          "this query bounds " + mine.boundKind + " at " + mine.boundValue + " where " +
            theirs.queryId + " bounds " + theirs.boundKind + " at " + theirs.boundValue),
        Component(apiIdentical, "the Joern API construct sets",
          "set difference empty in BOTH directions over " + shareds.size + " construct(s)",
          onlyHere.size + " construct(s) only here and " + onlyThere.size +
            " only there, over " + shareds.size + " shared"))
      val agreed = components.filter(_.agrees).map(rendered)
      val differed = components.filterNot(_.agrees).map(rendered)
      val duplicate = differed.isEmpty
      val coversAll =
        duplicate && shared.size == mine.pairIds.size && shared.size == theirs.pairIds.size
      val status =
        if (!duplicate) DUPLICATE_STATUS_NOT_DUPLICATE
        else if (coversAll) DUPLICATE_STATUS_DUPLICATE
        else DUPLICATE_STATUS_SCOPED_PREFIX + shared.mkString("_and_")
      // The scope note names, in BOTH directions, the pairs the other query addresses
      // and this one does not AND the pairs this query addresses and the other does not.
      // Enumerating only one direction renders an empty list whenever the asymmetry runs
      // the other way, which is a published sentence that states nothing.
      val onlyTheirPairs = theirs.pairIds.filterNot(shared.contains)
      val onlyMyPairs = mine.pairIds.filterNot(shared.contains)
      val scope =
        if (!duplicate) "none"
        else if (coversAll) "every handler/sink pair either query addresses"
        else {
          val asymmetries = List(
            if (onlyTheirPairs.nonEmpty)
              Some(theirs.queryId + " additionally addresses " +
                onlyTheirPairs.mkString(", ") + ", which this query does not")
            else None,
            if (onlyMyPairs.nonEmpty)
              Some("this query additionally addresses " + onlyMyPairs.mkString(", ") +
                ", which " + theirs.queryId + " does not")
            else None).flatten
          shared.mkString(", ") + " only; " + asymmetries.mkString("; and ")
        }
      val basis =
        if (duplicate)
          "every component of the formulation identity agrees" +
            (if (coversAll) "" else " at the scope named above") + ": " +
            agreed.mkString("; ") + ". The comparison is over the two SOURCES' own " +
            "declarations, so it is a property of the two formulations rather than of " +
            "either run's numbers"
        else
          "the formulations differ on " + differed.mkString("; ") +
            (if (agreed.isEmpty) "" else ", while agreeing on " + agreed.mkString("; ")) +
            ". Neither traversal establishes the other's conclusion, so the two " +
            "results are reported side by side and never summed"
      Relation(theirs, status, scope, basis, shared, sameEdge, sameEnds, sameEntry,
        sameSink, samePred, sameBoundKind, sameBoundValue, onlyHere, onlyThere, shareds,
        coversAll)
    }
  }

  val myFormulation = formulationOf(QUERY_ID)
  if (!myFormulation.established) {
    abortRun("this query's own formulation identity block could not be read from its " +
      s"own source: ${myFormulation.note}. The duplicate-formulation verdict is " +
      "computed from that block, so publishing one without it would be publishing a " +
      "guess")
  }
  if (myFormulation.sourceSha256 != sourceSha256) {
    abortRun("the source read for the formulation identity block digests to " +
      s"${myFormulation.sourceSha256} while the source digested in stage A digests to " +
      s"$sourceSha256; the file changed under the run, so nothing is published")
  }
  val relations = SIBLING_QUERY_IDS.map(qid => relate(myFormulation, formulationOf(qid)))
  val relationsEstablished = relations.filter(_.status != DUPLICATE_STATUS_NOT_ESTABLISHED)
  val relationsNotEstablished =
    relations.filter(_.status == DUPLICATE_STATUS_NOT_ESTABLISHED)
  val duplicateFormulationAggregate =
    if (relationsEstablished.exists(_.status == DUPLICATE_STATUS_DUPLICATE))
      AGGREGATE_DUPLICATE
    else if (relationsEstablished.exists(_.status.startsWith(DUPLICATE_STATUS_SCOPED_PREFIX)))
      AGGREGATE_PARTIAL
    else if (relationsEstablished.isEmpty) DUPLICATE_STATUS_NOT_ESTABLISHED
    else DUPLICATE_STATUS_NOT_DUPLICATE
  val duplicateFormulationAggregation =
    "the top-level verdict aggregates the per-query entries below and names the " +
      "strongest relation any one of them carries: " +
      relations.map(r => r.status + " against " + r.theirs.queryId).mkString(", ") +
      ". " + (duplicateFormulationAggregate match {
        case AGGREGATE_DUPLICATE =>
          "One entry is a duplicate over every pair either query addresses, so the " +
            "aggregate is a duplicate outright"
        case AGGREGATE_PARTIAL =>
          "One entry is a duplicate at a scope NARROWER than the whole pair set, which " +
            "makes the aggregate partial rather than absent. The scope is stated in " +
            "that entry rather than hidden in this label"
        case DUPLICATE_STATUS_NOT_ESTABLISHED =>
          "No sibling source could be read, so no relation was established and the " +
            "aggregate says so rather than defaulting to an absence"
        case _ =>
          "No entry carries a duplicate relation at any scope, so the aggregate is an " +
            "absence rather than a partial"
      }) + (if (relationsNotEstablished.isEmpty) ""
            else ". " + relationsNotEstablished.size + " of " + relations.size +
              " entries could not be established and are named as such rather than " +
              "counted as absences") +
      ". It was NOT inferred from the file names differing"
  val duplicateFormulationRelation =
    "a SYMMETRIC pairwise relation: the verdict this envelope states against a query " +
      "is the same verdict that query's envelope states against this one. It is one " +
      "measurement cited twice rather than two measurements, and here it is symmetric " +
      "BY CONSTRUCTION rather than by transcription - every entry below is computed by " +
      "applying ONE shared predicate to the two queries' own declared formulation " +
      "identity blocks, read out of the two SOURCE files at run time under names all " +
      "three queries share. Both directions therefore evaluate identical inputs " +
      "through identical code, so a disagreement between them is not expressible; a " +
      "transcribed verdict could disagree with the envelope it was copied from, which " +
      "is exactly what this replaces"
  log(s"duplicate formulation     : $duplicateFormulationAggregate " +
    relations.map(r => s"(${r.theirs.queryId}: ${r.status})").mkString(" "))
  relations.foreach { r =>
    log(s"  vs ${r.theirs.queryId}: source sha256 ${
      if (r.theirs.established) r.theirs.sourceSha256 else "not read"
    }, api only here ${r.apiOnlyHere.size}, only there ${r.apiOnlyThere.size}, " +
      s"shared ${r.apiShared.size}")
  }

  /**
   * The scalar's prose companion, COMPUTED from the relations rather than
   * asserted, and keeping the key the committed envelope carried. It names the
   * scope of any duplication explicitly, because a partial verdict whose scope is
   * hidden in a label is not interpretable.
   */
  val duplicateFormulationSummary = {
    val scoped =
      relations.filter(_.status.startsWith(DUPLICATE_STATUS_SCOPED_PREFIX))
    val outright = relations.filter(_.status == DUPLICATE_STATUS_DUPLICATE)
    val absent = relations.filter(_.status == DUPLICATE_STATUS_NOT_DUPLICATE)
    val unread = relations.filter(_.status == DUPLICATE_STATUS_NOT_ESTABLISHED)
    (if (outright.nonEmpty)
      "A duplicate of " + outright.map(_.theirs.queryId).mkString(" and ") +
        " over every pair either query addresses"
     else if (scoped.nonEmpty)
      "A duplicate of " + scoped.map(r => r.theirs.queryId + " on " +
        r.sharedPairs.mkString(" and ")).mkString("; ") +
        ", not a duplicate as a whole"
     else if (absent.nonEmpty && unread.isEmpty)
      "Not a duplicate of either sibling in any instantiation"
     else
      "No relation could be established against " +
        unread.map(_.theirs.queryId).mkString(" and ")) +
      (if (absent.nonEmpty && scoped.nonEmpty)
        ", and not a duplicate of " + absent.map(_.theirs.queryId).mkString(" or ") +
          " in any instantiation"
       else "") +
      ". The scope of any duplication is stated rather than hidden: where it is one " +
      "pair only, the parameterized form's remaining instantiation" +
      (if (myFormulation.pairIds.size > 1) "s are" else " is") +
      " the part that is new. Every clause here is computed from the per-query entries " +
      "below, so it cannot disagree with them."
  }

  val duplicateFormulationJson = jrawArr(4, relations.map { r =>
    jobj(6, List(
      "against" -> jstr(r.theirs.queryId),
      "status" -> jstr(r.status),
      "scope_of_the_duplication" -> jstr(r.scope),
      "basis" -> jstr(r.basis),
      "evidence_relied_on" -> jstrArr(List(
        "the two sources' declared formulation identity blocks, read at run time",
        "the entry-point and sink selector literals each source declares, compared as " +
          "literal text",
        "the bound each source declares, as a named quantity and a value",
        "the Joern API construct list each source declares, differenced in both " +
          "directions")),
      "sibling_source" -> jstr(r.theirs.sourceRepoRelative),
      "sibling_source_sha256" ->
        jstr(if (r.theirs.established) r.theirs.sourceSha256 else "not established"),
      "sibling_source_byte_size" -> jnum(r.theirs.sourceByteSize),
      "sibling_source_read_note" -> jstr(r.theirs.note),
      "same_target_pair" -> jbool(r.sharedPairs.nonEmpty),
      "pair_ids_here" -> jstrArr(myFormulation.pairIds),
      "pair_ids_there" -> jstrArr(r.theirs.pairIds),
      "pair_ids_shared" -> jstrArr(r.sharedPairs),
      "same_edge_kinds" -> jbool(r.sameEdgeKinds),
      "edge_kinds_here" -> jstrArr(myFormulation.edgeKinds),
      "edge_kinds_there" -> jstrArr(r.theirs.edgeKinds),
      "same_end_node_kinds" -> jbool(r.sameEndNodeKinds),
      "end_node_kinds_here" -> jstrArr(myFormulation.endNodeKinds),
      "end_node_kinds_there" -> jstrArr(r.theirs.endNodeKinds),
      "same_entry_point_granularity" -> jbool(r.sameEntrySelectors),
      "entry_selector_constant_names_here" ->
        jstrArr(myFormulation.entrySelectorNames),
      "entry_selector_constant_names_there" -> jstrArr(r.theirs.entrySelectorNames),
      "entry_selector_literals_here" -> jstrArr(myFormulation.entrySelectorLiterals),
      "entry_selector_literals_there" -> jstrArr(r.theirs.entrySelectorLiterals),
      "same_sink_selector_literals" -> jbool(r.sameSinkSelectors),
      "predicate_selector_literals_identical" -> jbool(r.samePredicateSelectors),
      "predicate_selector_note" -> jstr("the predicate set defines the term " +
        "\"spurious\" rather than the traversal, so it is not a component of the " +
        "formulation predicate; it is compared and reported here because the two " +
        "queries' spurious counts are only comparable while the definition is the " +
        "same text"),
      "bound_name_here" -> jstr(myFormulation.boundName),
      "bound_name_there" -> jstr(r.theirs.boundName),
      "bound_kind_here" -> jstr(myFormulation.boundKind),
      "bound_kind_there" -> jstr(r.theirs.boundKind),
      "bound_value_here" -> jnum(myFormulation.boundValue),
      "bound_value_there" -> jnum(r.theirs.boundValue),
      "bound_values_are_the_same_kind_of_quantity" -> jbool(r.sameBoundKind),
      "bound_values_agree" -> jbool(r.sameBoundValue),
      "traversal_semantics_here" -> jstr(myFormulation.traversalSemantics),
      "traversal_semantics_there" -> jstr(r.theirs.traversalSemantics),
      "api_constructs_only_here" -> jstrArr(r.apiOnlyHere),
      "api_constructs_only_there" -> jstrArr(r.apiOnlyThere),
      "api_constructs_shared" -> jnum(r.apiShared.size.toLong),
      "api_construct_set_difference_both_directions_empty" ->
        jbool(r.apiOnlyHere.isEmpty && r.apiOnlyThere.isEmpty),
      "can_differ_for_some_input" ->
        jbool(r.status != DUPLICATE_STATUS_DUPLICATE),
      "one_expressible_as_the_other" ->
        jbool(r.status == DUPLICATE_STATUS_DUPLICATE ||
          r.status.startsWith(DUPLICATE_STATUS_SCOPED_PREFIX)),
      "verdict_computed_not_transcribed" -> jbool(true),
      "verdict_symmetry_basis" -> jstr("one shared predicate over the two sources' own " +
        "declarations; the sibling's envelope is not read, so there is nothing to " +
        "transcribe and nothing that can drift"),
      "results_summed" -> jbool(false)))
  })


  // -------------------------------------------------------------------------
  stage("L-records: the returned set, capped, per pair and deterministic")
  // -------------------------------------------------------------------------
  def hopJson(h: Hop): String = jobj(10, List(
    "from_method" -> jstr(h.fromMethod),
    "call_site_callee" -> jstr(h.callSite),
    "call_site_graph_line" -> jnum(h.callSiteLine.toLong),
    "to_method" -> jstr(h.toMethod)))

  def routeJson(r: RouteRecord): String = jobj(6, List(
    "kind" -> jstr("route"),
    "pair_id" -> jstr(r.pairId),
    "walk_id" -> jstr(r.walkId),
    "entry_point" -> jstr(r.entryPoint),
    "sink_host" -> jstr(r.sinkHost),
    "hop_count" -> jnum(r.hops.size.toLong),
    "hops" -> jrawArr(8, r.hops.map(hopJson)),
    "passed_auth_or_acl_predicate" -> jbool(routeIsSpurious(r)),
    "spurious" -> jbool(routeIsSpurious(r))))

  def boundaryJson(b: BoundaryRecord): String = jobj(6, List(
    "kind" -> jstr("boundary"),
    "boundary_id" -> jstr(b.id),
    "boundary_kind" -> jstr(b.kind),
    "cited_by_pairs" -> jstrArr(citationsOf(b.id)),
    "one_measurement_cited_once_per_citing_pair" -> jbool(true),
    "hop" -> jstr(b.hop),
    "from_end" -> jstr(b.fromEnd),
    "to_end" -> jstr(b.toEnd),
    "reason" -> jstr(b.reason),
    "modelling" -> jstr(b.modelling),
    "crossed_by_a_call_edge" -> jbool(b.crossedByACallEdge),
    "measured" -> jobj(8, b.measured)))

  // Concatenation, in the declared pair order. This is a LIST of records, not an
  // arithmetic total: no field anywhere adds one pair's route count to another's.
  val recordJsonAll =
    boundaries.map(boundaryJson) ++ traversals.flatMap(_.distinctRoutes).map(routeJson)
  val totalReturnsCapReached = recordJsonAll.size > MAX_TOTAL_RETURNS
  val recordJson = recordJsonAll.take(MAX_TOTAL_RETURNS)
  val returnedRecordCount = recordJson.size
  log(s"records returned          : $returnedRecordCount " +
    s"(${boundaries.size} boundary measurement(s); route records per pair " +
    traversals.map(t => s"${t.pairId}=${t.distinctRoutes.size}").mkString(", ") +
    s"; cap $MAX_TOTAL_RETURNS, reached=$totalReturnsCapReached). This is a count of " +
    "RECORDS emitted, not a route total: no field adds one pair's routes to another's.")

  // ------------------- per-pair, per-constant bound outcomes ----------------
  // Every named bound carries a VALUE (the `bounds` object) and a REACHED FLAG
  // (here), per pair, measured from that pair's own walks and sweeps rather
  // than asserted - a bare flag is not interpretable, and a bound with no flag
  // leaves open whether the traversal was trimmed. MAX_TOTAL_RETURNS is a
  // QUERY-level cap rather than a per-pair one: both pairs' entries carry the
  // same value, which is one measurement cited twice, never two measurements.
  def boundsReachedFor(s: PairSelection, t: PairTraversal): List[(String, Boolean)] = List(
    "MAX_CALL_DEPTH" -> t.walks.exists(_.depthBoundReached),
    "MAX_ROUTES_PER_PAIR" -> t.walks.exists(_.routeCapReached),
    "MAX_EXPANSIONS_PER_ENTRY" -> t.walks.exists(_.entryExpansionCapReached),
    "MAX_STEPS_PER_PAIR" -> t.walks.exists(_.pairStepBudgetExhausted),
    "MAX_TOTAL_RETURNS" -> totalReturnsCapReached,
    "MAX_ENTRY_POINTS_PER_PAIR" -> (s.entryPointsTruncated > 0),
    "MAX_CALL_SCAN" -> sweepCapReached("MAX_CALL_SCAN"),
    "MAX_TYPE_SCAN" -> sweepCapReached("MAX_TYPE_SCAN"),
    "FANOUT_CALLEE_THRESHOLD" -> t.walks.exists(_.fanOutSitesEncountered > 0))

  def boundsBasisFor(s: PairSelection, t: PairTraversal): List[(String, String)] = {
    def yn(b: Boolean) = if (b) "reached" else "not reached"
    val deepest = if (t.walks.isEmpty) 0 else t.walks.map(_.maxDepthUsed).max
    // The figure cited against a cap must be measured at the cap's OWN scope.
    // MAX_EXPANSIONS_PER_ENTRY is per entry point, so the figure is the peak any
    // single entry point reached; MAX_STEPS_PER_PAIR is per pair, so the figure
    // is the pair's total across both walks rather than either walk's own count.
    val peakEntryExpansions =
      if (t.walks.isEmpty) 0 else t.walks.map(_.maxExpansionsAtOneEntry).max
    val pairStepsUsed = t.walks.map(_.callSitesConsidered).sum
    val walkExpansionTotals =
      t.walks.map(w => s"${w.walkId}=${w.expansions}").mkString(", ")
    val routesPerWalk = t.walks.map(w => s"${w.walkId}=${w.routes.size}").mkString(", ")
    val fanOutPerWalk =
      t.walks.map(w => s"${w.walkId}=${w.fanOutSitesEncountered}").mkString(", ")
    List(
      "MAX_CALL_DEPTH" -> (s"${yn(t.walks.exists(_.depthBoundReached))}: " +
        s"depth_bound_reached is ${t.walks.exists(_.depthBoundReached)} across this " +
        s"pair's walks and the deepest walk used $deepest of $MAX_CALL_DEPTH hops"),
      "MAX_ROUTES_PER_PAIR" -> (s"${yn(t.walks.exists(_.routeCapReached))}: " +
        s"route_cap_reached is ${existsPhrase(t.walks.map(_.routeCapReached))} - the " +
        s"quantifier is `exists`, and the per-walk values are " +
        s"${t.walks.map(w => s"${w.walkId}=${w.routeCapReached}").mkString(", ")} - with " +
        s"routes returned per walk $routesPerWalk against a per-pair cap of " +
        s"$MAX_ROUTES_PER_PAIR enforced on ONE counter shared by both of this pair's " +
        "walks, never shared between pairs, because one pair filling another's budget " +
        "would silently truncate it. Within one walk one witness chain is retained per " +
        s"(entry point, sink host), so the most this pair's walks can retain on this " +
        s"graph is ${t.routeCapReachableMaximum} (${t.walks.size} walk(s) x " +
        s"${s.entryPointsTraversed} entry point(s) traversed x ${s.sinkHostNames.size} " +
        s"sink host(s)); the cap can therefore bind here: ${t.routeCapCanBind}. Where it " +
        "cannot, that is stated rather than left for a reader to derive from three other " +
        "numbers, and the arrivals not retained are counted separately as " +
        s"${t.alternateSinkArrivalsNotRetained} for this pair"),
      "MAX_EXPANSIONS_PER_ENTRY" ->
        (s"${yn(t.walks.exists(_.entryExpansionCapReached))}: " +
          s"entry_expansion_cap_reached is " +
          s"${existsPhrase(t.walks.map(_.entryExpansionCapReached))} - the quantifier " +
          s"is `exists`, and the per-walk values are " +
          s"${t.walks.map(w => s"${w.walkId}=${w.entryExpansionCapReached}").mkString(", ")}" +
          s". The counter is " +
          s"reset at each entry point, so the figure measured against this cap is the " +
          s"peak any ONE entry point reached, $peakEntryExpansions of " +
          s"$MAX_EXPANSIONS_PER_ENTRY. Each walk's total across all of its entry points " +
          s"is reported separately and caps nothing: $walkExpansionTotals"),
      "MAX_STEPS_PER_PAIR" -> (s"${yn(t.walks.exists(_.pairStepBudgetExhausted))}: " +
        s"pair_step_budget_exhausted is ${t.walks.exists(_.pairStepBudgetExhausted)}. " +
        s"Both of this pair's walks draw on ONE budget, so the figure measured against " +
        s"this cap is the pair's total, $pairStepsUsed of $MAX_STEPS_PER_PAIR"),
      "MAX_TOTAL_RETURNS" -> (s"${yn(totalReturnsCapReached)}: $returnedRecordCount " +
        s"record(s) emitted by the query against a cap of $MAX_TOTAL_RETURNS. This is a " +
        "query-level cap, so this entry is the same measurement in both pairs' " +
        "objects rather than a second one"),
      "MAX_ENTRY_POINTS_PER_PAIR" -> (s"${yn(s.entryPointsTruncated > 0)}: " +
        s"${s.entryPointsDiscovered} entry point(s) discovered, " +
        s"${s.entryPointsTraversed} traversed and ${s.entryPointsTruncated} truncated, " +
        s"against a per-pair cap of $MAX_ENTRY_POINTS_PER_PAIR"),
      "MAX_CALL_SCAN" -> (s"${yn(sweepCapReached("MAX_CALL_SCAN"))}: the flag is the " +
        "DISJUNCTION over every sweep this cap governs, each named with its own " +
        "observed count and its own truncation flag - " +
        sweepBasisFor("MAX_CALL_SCAN") +
        s". This pair's own sink sweep observed ${s.sinkCallsScanned} call(s) named " +
        s"${s.pair.sinkCallName} of $MAX_CALL_SCAN with truncated=${s.sinkScanTruncated}. " +
        "The sweeps are query-level, so this entry is one measurement cited in both " +
        "pairs' objects rather than two"),
      "MAX_TYPE_SCAN" -> (s"${yn(sweepCapReached("MAX_TYPE_SCAN"))}: the flag is the " +
        "DISJUNCTION over every sweep this cap governs, each named with its own " +
        "observed count and its own truncation flag - " +
        sweepBasisFor("MAX_TYPE_SCAN") +
        s". The route-surface prefix sweep alone took at most $MAX_TYPE_SCAN type " +
        s"declarations per prefix over ${surfacePrefixEvidence.size} prefixes, the " +
        "widest being " +
        s"${if (surfacePrefixEvidence.isEmpty) 0 else surfacePrefixEvidence.map(_.typeDeclsInTheGraph).max}" +
        s", and its own truncation flag was $surfaceTypeScanTruncated. The sweeps are " +
        "query-level, so this entry is one measurement cited in both pairs' objects " +
        "rather than two"),
      "FANOUT_CALLEE_THRESHOLD" ->
        ((if (t.walks.exists(_.fanOutSitesEncountered > 0))
          "exceeded, which is what \"reached\" means for a threshold rather than a cap"
         else "not exceeded") +
          s": fan-out sites encountered per walk $fanOutPerWalk, a site counting as " +
          s"fan-out when its resolved callee set exceeds $FANOUT_CALLEE_THRESHOLD " +
          "distinct methods"))
  }


  def walkJson(w: WalkResult): String = jobj(8, List(
    "pair_id" -> jstr(w.pairId),
    "walk_id" -> jstr(w.walkId),
    "follows_dynamic_dispatch_fan_out" -> jbool(w.followsFanOut),
    "entry_points_traversed" -> jnum(w.entryPointsTraversed.toLong),
    "method_expansions" -> jnum(w.expansions.toLong),
    "methods_visited" -> jnum(w.methodsVisited.toLong),
    "call_sites_considered" -> jnum(w.callSitesConsidered.toLong),
    "fan_out_sites_encountered" -> jnum(w.fanOutSitesEncountered.toLong),
    "fan_out_sites_not_followed" -> jnum(w.fanOutSitesNotFollowed.toLong),
    "max_expansions_at_one_entry_point" -> jnum(w.maxExpansionsAtOneEntry.toLong),
    "max_depth_used" -> jnum(w.maxDepthUsed.toLong),
    "depth_bound_reached" -> jbool(w.depthBoundReached),
    "entry_expansion_cap_reached" -> jbool(w.entryExpansionCapReached),
    "pair_step_budget_exhausted" -> jbool(w.pairStepBudgetExhausted),
    "route_cap_reached" -> jbool(w.routeCapReached),
    "alternate_sink_arrivals_not_retained" ->
      jnum(w.alternateSinkArrivalsNotRetained.toLong),
    "routes_returned" -> jnum(w.routes.size.toLong)))

  def messageHopJson(h: MessageHop): String = jobj(8, List(
    "id" -> jstr(h.id),
    "message_type" -> jstr(h.messageType),
    "constructor" -> jstr(h.ctorName),
    "accessors" -> jstrArr(h.accessorNames),
    "direction" -> jstr(h.direction),
    "source_anchor_at_the_pin" -> jstr(h.sourceAnchor),
    "boundary_id" -> jstr("B-rpc-" + h.id)))

  /** One object per pair: the pair as parameterized, as selected, as traversed
   *  and as reported. Every figure inside it belongs to that pair alone. */
  def pairJson(p: HandlerSinkPair, s: PairSelection, t: PairTraversal,
      sp: PairSpurious): String = {
    val ids = boundaryIdsByPair(p.id)
    val notCrossed = ids.filter(id => !boundaryStore(id).crossedByACallEdge)
    jobj(4, List(
      "pair_id" -> jstr(p.id),
      "label" -> jstr(p.label),
      "invoked" -> jbool(t.invoked),
      "handler" -> jobj(6, List(
        "type" -> jstr(p.handlerType),
        "method" -> jstr(p.handlerMethod),
        "name_used_by_the_plan" -> jstr(p.handlerPlanName),
        "source_file_at_the_pin" -> jstr(p.handlerSourceFile),
        "source_line_at_the_pin" -> jnum(p.handlerSourceLine.toLong),
        "synthetic_type_regex" -> jstr(p.handlerSyntheticTypeRegex),
        "synthetic_method" -> jstr(p.handlerSyntheticMethod),
        "synthetic_types_matched" -> jstrArr(s.syntheticTypeNames),
        "body_witness" -> jstr(p.handlerBodyWitness),
        "body_witness_in_the_synthetic_arm" -> jbool(s.syntheticCarriesBody),
        "body_witness_in_the_source_level_arm" -> jbool(s.sourceLevelCarriesBody),
        "base_type_declaring_the_method" -> jstr(p.handlerBaseType),
        "base_declarations_excluded_by_the_type_selector" ->
          jstrArr(s.baseDeclarationNames))),
      "sink" -> jobj(6, List(
        "callee_regex" -> jstr(p.sinkCalleeRegex),
        "call_name" -> jstr(p.sinkCallName),
        "host_type_regex" -> jstr(p.sinkHostTypeRegex),
        "source_file_at_the_pin" -> jstr(p.sinkSourceFile),
        "source_line_at_the_pin" -> jnum(p.sinkSourceLine.toLong),
        "calls_named_scanned" -> jnum(s.sinkCallsScanned.toLong),
        "scan_truncated" -> jbool(s.sinkScanTruncated),
        "call_sites_on_any_host" -> jnum(s.sinkCallsAnyHost.toLong),
        "call_sites_on_the_sink_host" -> jnum(s.sinkCalls.size.toLong),
        "call_sites" -> jstrArr(s.sinkCalls.map(c =>
          s"${c.method.fullName} -> ${c.methodFullName} #${lineOf(c)}")),
        "sink_hosts" -> jstrArr(s.sinkHostNames.toList.sorted),
        "shared_with_the_other_pair" -> jbool(sinkIsShared))),
      "message_hops" -> jrawArr(6, p.messageHops.map(messageHopJson)),
      "entry_points_discovered" -> jnum(s.entryPointsDiscovered.toLong),
      "entry_points_traversed" -> jnum(s.entryPointsTraversed.toLong),
      "entry_points_truncated" -> jnum(s.entryPointsTruncated.toLong),
      "entry_points" -> jstrArr(s.entryGroups.map(_._1)),
      "distinct_routes" -> jnum(t.distinctRoutes.size.toLong),
      "route_cap_reachable_maximum" -> jnum(t.routeCapReachableMaximum.toLong),
      "route_cap_can_bind" -> jbool(t.routeCapCanBind),
      "alternate_sink_arrivals_not_retained" ->
        jnum(t.alternateSinkArrivalsNotRetained.toLong),
      "spurious_count" -> jnum(sp.spuriousCount.toLong),
      "expected_spurious_route_absent" -> jbool(sp.expectedSpuriousAbsent),
      "expected_spurious_absence_basis" ->
        jstr(if (sp.absenceIsStructural) "structural" else "filtering"),
      "expected_spurious_absence_detail" -> jstr(sp.basis),
      "predicate_call_sites_on_its_own_route_surface" ->
        jnum(sp.predicateCallSitesOnOwnSurface.toLong),
      "route_surface_type_prefixes" -> jstrArr(p.routeSurfaceTypePrefixes),
      "covered_by_the_shared_route_surface_prefixes" ->
        jbool(ROUTE_SURFACE_TYPE_PREFIXES.exists(pref => p.handlerType.startsWith(pref))),
      "bound_reached" -> jbool(t.boundReached),
      "boundary_count" -> jnum(ids.size.toLong),
      "boundary_ids" -> jstrArr(ids),
      "boundaries_not_crossed_by_a_call_edge" -> jstrArr(notCrossed),
      "walks" -> jrawArr(6, t.walks.map(walkJson))))
  }

  val pairObjects = PAIRS.indices.toList.map { i =>
    pairJson(PAIRS(i), selections(i), traversals(i), spuriousByPair(i))
  }

  // ---------------------------- parameterizability --------------------------
  // Effort measure 3, decided from the run rather than claimed. It passes ONLY
  // because the SECOND named pair was actually invoked and its result is
  // captured in the two result files and the console log below.
  val secondPair = PAIRS(1)
  val secondSelection = selections(1)
  val secondTraversal = traversals(1)
  val secondSpurious = spuriousByPair(1)
  val secondPairWalksRan =
    secondTraversal.walks.map(_.walkId) == WALK_MODE_IDS &&
      secondTraversal.walks.exists(_.callSitesConsidered > 0)
  val secondPairBoundaryIds = boundaryIdsByPair(secondPair.id)
  val secondPairCitedBoundaries =
    boundaries.filter(b => citationsOf(b.id).contains(secondPair.id)).map(_.id)
  val parameterizabilityPassed =
    traversals.forall(_.invoked) && secondTraversal.invoked && secondPairWalksRan &&
      secondSelection.entryPointsTraversed > 0 && secondPairCitedBoundaries.nonEmpty
  val parameterizabilityVerdict = if (parameterizabilityPassed) "passed" else "not passed"
  val secondPairOutcome =
    "invoked; entry points traversed " + secondSelection.entryPointsTraversed.toString +
      " of " + secondSelection.entryPointsDiscovered.toString + "; walks run " +
      secondTraversal.walks.map(_.walkId).mkString(" and ") + "; call sites considered " +
      secondTraversal.walks.map(w => w.walkId + "=" + w.callSitesConsidered.toString)
        .mkString(", ") + "; distinct routes " +
      secondTraversal.distinctRoutes.size.toString + "; spurious " +
      secondSpurious.spuriousCount.toString + "; boundaries measured or cited " +
      secondPairBoundaryIds.size.toString + " (" + secondPairBoundaryIds.mkString(", ") + ")"
  val firstPairOutcome =
    "invoked; entry points traversed " + selections.head.entryPointsTraversed.toString +
      " of " + selections.head.entryPointsDiscovered.toString + "; distinct routes " +
      traversals.head.distinctRoutes.size.toString + "; spurious " +
      spuriousByPair.head.spuriousCount.toString + "; boundaries measured or cited " +
      boundaryIdsByPair(PAIRS.head.id).size.toString
  log(s"parameterizability        : $parameterizabilityVerdict")
  log(s"  first pair  (${PAIRS.head.id}) : $firstPairOutcome")
  log(s"  second pair (${secondPair.id}) : $secondPairOutcome")
  if (!parameterizabilityPassed) {
    log("  the measure is reported as NOT PASSED rather than claimed: the second pair's " +
      "invocation did not complete in this run")
  }

  val parameterizabilityJson = jobj(2, List(
    "measure" -> jstr("parameterizability"),
    "owner" -> jstr(PARAMETERIZABILITY_OWNER),
    "pass_condition" -> jstr(PARAMETERIZABILITY_PASS_CONDITION),
    "verdict" -> jstr(parameterizabilityVerdict),
    "pairs_declared" -> jnum(PAIRS.size.toLong),
    "pairs_invoked" -> jnum(traversals.count(_.invoked).toLong),
    "pair_iteration_order" -> jstrArr(PAIRS.map(_.id)),
    "first_pair_id" -> jstr(PAIRS.head.id),
    "first_pair_handler" ->
      jstr(PAIRS.head.handlerType + "." + PAIRS.head.handlerMethod + " (" +
        PAIRS.head.handlerSourceFile + ":" + PAIRS.head.handlerSourceLine +
        " at the pin)"),
    "first_pair_sink" ->
      jstr(PAIRS.head.sinkSourceFile + ":" + PAIRS.head.sinkSourceLine + " at the pin"),
    "first_pair_outcome" -> jstr(firstPairOutcome),
    "second_pair_id" -> jstr(secondPair.id),
    "second_pair_handler" ->
      jstr(secondPair.handlerType + "." + secondPair.handlerMethod + " (" +
        secondPair.handlerSourceFile + ":" + secondPair.handlerSourceLine + " at the pin)"),
    "second_pair_sink" ->
      jstr(secondPair.sinkSourceFile + ":" + secondPair.sinkSourceLine + " at the pin"),
    "second_pair_invoked" -> jbool(secondTraversal.invoked && secondPairWalksRan),
    "second_pair_outcome" -> jstr(secondPairOutcome),
    "second_pair_result_captured_in" -> jstrArr(List(
      RESULTS_DIR + "/" + QUERY_ID + ".json",
      RESULTS_DIR + "/" + QUERY_ID + ".md",
      LOG_DIR + "/probe-" + QUERY_ID + ".log")),
    "parameter_values_supplied" -> jbyPair(4, PAIRS.map(p => p.id -> jobj(6, List(
      "handler_type" -> jstr(p.handlerType),
      "handler_method" -> jstr(p.handlerMethod),
      "handler_synthetic_type_regex" -> jstr(p.handlerSyntheticTypeRegex),
      "handler_synthetic_method" -> jstr(p.handlerSyntheticMethod),
      "handler_body_witness" -> jstr(p.handlerBodyWitness),
      "handler_base_type" -> jstr(
        if (p.handlerBaseType.isEmpty) "none declared for this pair" else p.handlerBaseType),
      "handler_source_file_at_the_pin" -> jstr(p.handlerSourceFile),
      "handler_source_line_at_the_pin" -> jnum(p.handlerSourceLine.toLong),
      "sink_callee_regex" -> jstr(p.sinkCalleeRegex),
      "sink_call_name" -> jstr(p.sinkCallName),
      "sink_host_type_regex" -> jstr(p.sinkHostTypeRegex),
      "sink_source_file_at_the_pin" -> jstr(p.sinkSourceFile),
      "sink_source_line_at_the_pin" -> jnum(p.sinkSourceLine.toLong),
      "message_hop_ids" -> jstrArr(p.messageHops.map(_.id)),
      "route_surface_type_prefixes" -> jstrArr(p.routeSurfaceTypePrefixes),
      "pair_label" -> jstr(p.label))))),
    "parameter_values_note" -> jstr("these are the literals the ONE query body was " +
      "driven by, listed per pair so a reader can see two invocations of one traversal " +
      "rather than two queries: no traversal in this source names a handler or a sink " +
      "itself, every selector it applies comes out of the pair it was handed, and the " +
      "predicate set is not among the parameters"),
    "zero_record_outcome_and_the_verdict" -> jstr("a zero-record outcome on the second " +
      "pair does not affect this measure and did not affect this verdict. The measure " +
      "asks whether the query is parameterizable - whether the second named pair was " +
      "really supplied to the same body and its result captured - and not whether that " +
      "pair is connected over this graph by this formulation. The two are reported " +
      "separately for that reason: the verdict here, and the pair's own distinct-route " +
      "count and boundary measurements in its per-pair object, where a zero is a " +
      "capability finding about the traversal rather than a failure of either"),
    "statement" -> jstr(
      "the measure is settled by an invocation, not by a parameter list: both pairs were " +
        "invoked in this single run, in the declared order, and the second pair's " +
        "selection, walk counters, boundary measurements, distinct-route count and " +
        "spurious count are all published above and in both result files. An empty " +
        "result from a real invocation satisfies the measure; a skipped invocation " +
        "would not, and a malformed pair aborts the run rather than being passed over")))

  /**
   * Pair one's own measured figures, published so a reader can compare them
   * against query 01's published figures THEMSELVES rather than against a copy
   * of them made inside this file. That is the whole change: the comparison is
   * still available, but the sibling's numbers are read from the sibling's own
   * result file by whoever wants to compare, and nothing here can drift from it.
   */
  val pairOneMeasuredJson = jobj(2, List(
    "pair_id" -> jstr(PAIR_ONE_ID),
    "entry_points_here" -> jstrArr(pairOneSelection.entryGroups.map(_._1)),
    "distinct_routes_here" -> jnum(pairOneTraversal.distinctRoutes.size.toLong),
    "bound_value_here" -> jnum(MAX_CALL_DEPTH.toLong),
    "boundaries_not_crossed_here_in_the_sibling_numbering" ->
      jstrArr(pairOneNotCrossedHere),
    "boundary_id_translation" -> jstrArr(BOUNDARY_ID_TO_SIBLING_01.toList.sorted
      .map { case (mine, theirs) => mine + " -> " + theirs }),
    "boundary_id_translation_role" -> jstr("a declared NAMING map so the same hop is " +
      "identifiable across the two queries. No verdict is drawn from it, and no " +
      "sibling measurement is restated beside it"),
    "comparison_scope" -> jstr("these are THIS run's measurements against THIS host's " +
      "graph. A sibling measured its own against the graph of its own run, so a " +
      "cross-run comparison of counts is a reader's to make from the two result files " +
      "and is deliberately not made here - which is also why no sibling figure is " +
      "copied into this file to make it")))



  // -------------------------------------------------------------------------
  stage("M-write: the envelope, the prose report and the console log")
  // -------------------------------------------------------------------------
  val resultsDir = repoRoot.resolve(RESULTS_DIR)
  Files.createDirectories(resultsDir)
  val jsonPath = resultsDir.resolve(s"$QUERY_ID.json")
  val mdPath = resultsDir.resolve(s"$QUERY_ID.md")

  /**
   * One identifier shared by every member of this publication, so a consumer
   * holding two members whose identifiers DIFFER knows it holds two generations.
   * The converse does not hold: equal identifiers mean equal (query, source,
   * graph), which two separate invocations share, so the member-set identifier is
   * what settles sameness of generation. It is
   * DERIVED rather than drawn from a clock: a nanotime component would
   * distinguish invocations and would also break the byte-identity contract this
   * envelope states, because an unchanged source over an unchanged graph would
   * then emit different bytes every run.
   */
  val publicationId = sha256OfBytes(
    List(QUERY_ID, sourceSha256, shaObserved, sizeFollow.toString,
      methodCount.toString).mkString("\n").getBytes(StandardCharsets.UTF_8))
  log(s"publication id            : $publicationId")
  publicationIdOfRecord = Some(publicationId)

  def byPairNum(f: (PairSelection, PairTraversal, PairSpurious) => Long): String =
    jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jnum(f(selections(i), traversals(i), spuriousByPair(i)))))
  def byPairBool(f: (PairSelection, PairTraversal, PairSpurious) => Boolean): String =
    jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jbool(f(selections(i), traversals(i), spuriousByPair(i)))))
  def byPairStr(f: (PairSelection, PairTraversal, PairSpurious) => String): String =
    jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jstr(f(selections(i), traversals(i), spuriousByPair(i)))))

  // ---- the predicate set's source-level surface, quoted from the pinned tree --
  // These are SOURCE facts about the five selectors and about the three route
  // files, quoted from the pinned clone at the SHA the header names and checked
  // there line by line. They live here rather than inside the byte-identical
  // selector block, which is carried verbatim and is not edited to report
  // anything. Every list ascends by source line so its order is a fixed
  // function of its content.
  val predicateSourceFile = "core/src/main/scala/org/apache/spark/SecurityManager.scala"
  val predicateSourceFileLines = 457
  val predicateNamedFiveWithSourceLines = List(
    "aclsEnabled() at " + predicateSourceFile + ":227",
    "checkAdminPermissions at " + predicateSourceFile + ":234",
    "checkUIViewPermissions at " + predicateSourceFile + ":248",
    "checkModifyPermissions at " + predicateSourceFile + ":264",
    "isAuthenticationEnabled() at " + predicateSourceFile + ":274")
  val predicateDeliberateNonSelectors = List(
    "isEncryptionEnabled() at " + predicateSourceFile + ":280",
    "isSslRpcEnabled() at " + predicateSourceFile + ":295")
  val predicateOverMatchHazards = List(
    "setViewAcls at " + predicateSourceFile + ":123",
    "setViewAcls at " + predicateSourceFile + ":128",
    "setViewAclsGroups at " + predicateSourceFile + ":136",
    "getViewAcls at " + predicateSourceFile + ":144",
    "getViewAclsGroups at " + predicateSourceFile + ":152",
    "setModifyAcls at " + predicateSourceFile + ":164",
    "setModifyAclsGroups at " + predicateSourceFile + ":173",
    "getModifyAcls at " + predicateSourceFile + ":182",
    "getModifyAclsGroups at " + predicateSourceFile + ":190",
    "setAdminAcls at " + predicateSourceFile + ":202",
    "setAdminAclsGroups at " + predicateSourceFile + ":211",
    "setAcls at " + predicateSourceFile + ":216")
  val predicateCallSitesInsideItsOwnType = List(
    "aclsEnabled() invoked at " + predicateSourceFile + ":249",
    "aclsEnabled() invoked at " + predicateSourceFile + ":265",
    "aclsEnabled() invoked at " + predicateSourceFile + ":407, inside the private " +
      "isUserInACL declared at :402")
  val predicateReferencesThatAreNotInvocations = List(
    PAIR_ONE_HANDLER_SOURCE_FILE + ":28 imports SecurityManager",
    PAIR_ONE_HANDLER_SOURCE_FILE + ":53 declares val securityMgr: SecurityManager",
    PAIR_ONE_HANDLER_SOURCE_FILE + ":139 reads the companion constant " +
      "SecurityManager.SPARK_AUTH_SECRET_CONF",
    PAIR_ONE_HANDLER_SOURCE_FILE + ":1429 constructs a SecurityManager",
    SINK_SOURCE_FILE + ":27 imports SecurityManager",
    SINK_SOURCE_FILE + ":56 declares val securityManager: SecurityManager",
    SINK_SOURCE_FILE + ":194 passes securityManager on as an argument")
      .:::(ROUTE_HOP_WORKER_PREDICATE_REFERENCES).sorted
  // Every file on the route, not only its two ends: the intermediate Worker hop
  // is what receives the launch message and starts the sink's host, so a search
  // that skipped it would leave one hop of the route unsearched while the field
  // name said "route files".
  val predicateRouteFilesSearched = List(
    PAIR_ONE_HANDLER_SOURCE_FILE,
    PAIR_TWO_HANDLER_SOURCE_FILE,
    ROUTE_HOP_WORKER_SOURCE_FILE,
    SINK_SOURCE_FILE).sorted

  val envelopeCoreFields: List[(String, String)] = List(
    "query_id" -> jstr(QUERY_ID),
    "query_source" -> jstr(s"queries/joern/$QUERY_ID.sc"),
    "source_integrity" -> jobj(2, List(
      "query_source" -> jstr(sourceRepoRelative),
      "query_source_sha256" -> jstr(sourceSha256),
      "query_source_byte_size" -> jnum(sourceByteSize),
      "digested_at" -> jstr("run time, by the running script, from the file at the " +
        "path above"),
      "self_identification_checked" -> jbool(true),
      "self_identification_basis" -> jstr("the file digested must declare this query's " +
        "own id; a digest of any other file is refused rather than published"),
      "loader" -> jstr("importCpg"),
      "alternative_loader_occurrences_in_the_source" ->
        jnum(alternativeLoaderOccurrences.toLong),
      "alternative_loader_absence_is_measured" -> jbool(true),
      "contract" -> jstr("every member of this publication - this envelope, the prose " +
        "report and the console log - carries this digest, so a member can be checked " +
        "against the source that wrote it rather than assumed to come from it. A result " +
        "whose digest does not match the source beside it was not written by that " +
        "source, and that is a defect in the result rather than a matter of opinion"))),
    "publication" -> jobj(2, List(
      "publication_id" -> jstr(publicationId),
      "members" -> jstrArr(List(
        RESULTS_DIR + "/" + QUERY_ID + ".json",
        RESULTS_DIR + "/" + QUERY_ID + ".md",
        LOG_DIR + "/probe-" + QUERY_ID + ".log")),
      "derivation" -> jstr("sha256 over the query id, the query source's sha256, the " +
        "graph's sha256, the graph's byte size and the graph's method count, joined by " +
        "newlines. Deterministic by construction: two publications sharing this " +
        "identifier came from the same source over the same graph"),
      "why_not_a_timestamp" -> jstr("a wall-clock or nanotime component would " +
        "distinguish generations and would also break the byte-identity contract " +
        "stated under determinism, because an unchanged source over an unchanged graph " +
        "would then emit different bytes every run"),
      "atomicity" -> jstr("every member is written to a private temporary in the " +
        "target's own directory, created exclusively and without following links, " +
        "flushed and fsynced, measured by reading the staged bytes back, and only " +
        "then moved onto its final name; the moves happen after every member has been " +
        "staged, so a failure part-way leaves the previous generation in place rather " +
        "than a mixed one. That closes the window BEFORE the renames; the window " +
        "BETWEEN them is closed by the completion manifest below"),
      "marker_location" -> jstr("in the members themselves AND in a completion " +
        "manifest published last. The identifier two members carry lets a consumer " +
        "compare them; the manifest is what makes an INCOMPLETE set detectable, " +
        "which comparing two members that are both present cannot do"),
      "completion_manifest" -> jstr(LOG_DIR + "/probe-" + QUERY_ID + ".publication.json"),
      "completion_manifest_role" -> jstr("the commit record for this member set. It " +
        "carries each content member's path, byte size and sha256 and a member_set_id " +
        "derived from those digests, and it is renamed AFTER every content member is " +
        "in place, so its presence is the completion signal. Absent, or disagreeing " +
        "with a member on disk, means the set on disk is not one generation"),
      "completion_manifest_verified_by_producer" -> jbool(true),
      "member_set_id" -> lastPublicationMemberSetId.map(jstr).getOrElse(
        jstr("published with the manifest after this envelope's bytes were fixed, so " +
          "the value is in the manifest rather than here: an envelope cannot carry an " +
          "identifier derived from its own digest")),
      "member_set_id_versus_publication_id" -> jstr("publication_id is derived from " +
        "the query, its source and the graph, before any member exists, so it " +
        "identifies the QUERY/SOURCE/GRAPH INPUT TUPLE and nothing more. It is " +
        "REPEATABLE ACROSS SEPARATE INVOCATIONS by design - two runs of the same " +
        "source over the same graph share it, which is the byte-identity contract " +
        "under determinism rather than a defect - so it does NOT identify an " +
        "invocation, does NOT encode a nonce, and can say nothing about whether the " +
        "set on disk is complete or about which execution produced it. member_set_id " +
        "is derived from MEMBER BYTES, which is what makes it a completion record: it " +
        "changes when any member's bytes change, so it distinguishes generations that " +
        "share a publication_id. Neither identifier encodes an invocation, and two " +
        "invocations producing byte-identical members are indistinguishable by " +
        "construction. Both are published, and neither substitutes for the other"))),
    "formulation" -> jstr("bounded call-graph reachability over CALL edges, PARAMETERIZED " +
      "over handler/sink pairs and instantiated on two named pairs in one run: the " +
      "standalone Master's driver-submission handler, and the REST submit servlet's " +
      "handleSubmit, each to the privileged process launch hosted on the DriverRunner " +
      "surface"),
    "observational_only" -> jbool(true),
    "contributes_dataset_rows" -> jbool(false),
    "compile_status" -> jstr("compiled"),
    "compile_status_convention" -> jstr("this field is written by the running script, " +
      "so its presence is itself the evidence: a compile failure produces no envelope " +
      "at all and the compiler's diagnostic lands in the console stream"),
    "run_status" -> jstr("completed"),
    "run_status_convention" -> jstr("completed means every stage passed and the result " +
      "region was emitted; failed is the only other value and is what the marker " +
      "protocol reports instead, with no result region at all"),
    "failure_representation" -> jobj(2, List(
      "compile_status_values" -> jstrArr(List("compiled", "failed")),
      "run_status_values" -> jstrArr(List("completed", "failed")),
      "marker_protocol" -> jstr("the query prints " + MARKER_START + " before any work " +
        "and brackets its result region with " + MARKER_RESULT_BEGIN + " and " +
        MARKER_RESULT_END + " only once every stage has passed, closing with " +
        MARKER_OK),
      "on_failure_result_region" -> jstr("NO result region is emitted on failure: " +
        MARKER_FAILURE + " is printed instead, the failing stage and the exception go to " +
        "the console stream, and the exception is re-raised. A partial result region is " +
        "never emitted, because one would read like a completed run"),
      "on_failure_measurement_fields" -> jstr("when compile_status or run_status carries " +
        "its failure value, every measurement field - returned_record_count, " +
        "returned_record_kinds, distinct_routes, spurious_count, bound_reached, " +
        "bounds_reached_by_pair, entry_points_discovered, entry_points_traversed, " +
        "entry_points_truncated, pairs, walks, records and the graph counts - is null. " +
        "Null is used consistently and no zero is ever written in its place, because a " +
        "zero would read as a successful empty result"),
      "query_level_failure_versus_a_per_pair_outcome" -> jstr("the two are represented " +
        "differently and must not be conflated. A QUERY-LEVEL failure - a compile " +
        "failure, a heap below the floor, a graph identity mismatch, a malformed pair - " +
        "produces no envelope at all and therefore no per-pair object for either pair: " +
        "pairs_invoked would be absent rather than 0. A SUCCESSFUL run may legitimately " +
        "return zero records for one or both pairs, and that state is this envelope's " +
        "compile_status compiled with run_status completed, pairs_invoked equal to " +
        "pairs_declared, a per-pair object present for each pair with invoked true, and " +
        "distinct_routes 0 for the pair concerned. A zero there is a measured capability " +
        "finding about what this formulation reaches over this graph, not a failure"),
      "value_not_established_convention" -> jstr("a value that could not be established " +
        "is named as such in the field that would have carried it, never omitted and " +
        "never guessed: a value missing from the record is a value nothing downstream " +
        "can check"))),
    "pairs_declared" -> jnum(PAIRS.size.toLong),
    "pairs_invoked" -> jnum(traversals.count(_.invoked).toLong),
    "pair_iteration_order" -> jstrArr(PAIRS.map(_.id)),
    "returned_record_count" -> jnum(returnedRecordCount.toLong),
    "returned_record_count_convention" -> jstr("a count of RECORDS emitted - boundary " +
      "measurements plus per-pair route records - and never a route total: no field in " +
      "this envelope adds one pair's routes to another's, or this query's returns to " +
      "query 01's or 02's"),
    "returned_record_kinds" -> jobj(2, List(
      "boundary" -> jnum(boundaries.size.toLong),
      "route_by_pair" -> jbyPair(4, traversals.map(t =>
        t.pairId -> jnum(t.distinctRoutes.size.toLong))))),
    "distinct_routes" -> byPairNum((_, t, _) => t.distinctRoutes.size.toLong),
    "distinct_routes_convention" -> jstr("per pair, deduplicated within that pair on " +
      "(entry point, sink host, hop sequence) across its own two walks. The two pairs' " +
      "figures are reported side by side and are NEVER SUMMED, and neither is added to " +
      "query 01's or query 02's published returns"),
    "distinct_routes_identity_function" -> jstr("a route identity is the triple (entry " +
      "point method full name, sink host method full name, the ordered sequence of " +
      "method full names from the entry point to the sink), evaluated WITHIN one pair. " +
      "Two returns with equal triples are ONE route however many traversal orders " +
      "produced them, and this pair's two walks are deduplicated against each other on " +
      "that triple rather than added together. The function is never applied across " +
      "pairs: two pairs are two results, so no identity ever merges a pair-one route " +
      "with a pair-two route. WHAT IS ENUMERATED AND WHAT IS ONLY COUNTED, stated " +
      "because the identity function names the whole hop sequence while the traversal " +
      "does not enumerate every sequence that satisfies it: each walk ENUMERATES one " +
      "witness chain per (entry point, sink host) - the shortest, from the " +
      "breadth-first parent map - and every later arrival at a sink host already " +
      "witnessed from that entry point in that walk is COUNTED as " +
      "alternate_sink_arrivals_not_retained, per walk and per pair, rather than " +
      "retained. So distinct_routes is a count of witnessed (entry point, sink host) " +
      "reachability within a pair, each witness carrying one full hop sequence, and the " +
      "per-pair route cap can bind only on that quantity - which is why " +
      "route_cap_reachable_maximum and route_cap_can_bind are published per pair " +
      "instead of leaving route_cap_reached=false to be read as a complete traversal"),
    "alternate_sink_arrivals_not_retained" ->
      byPairNum((_, t, _) => t.alternateSinkArrivalsNotRetained.toLong),
    "alternate_sink_arrivals_meaning" -> jstr("arrivals at a sink host already witnessed " +
      "from the same entry point in the same walk. They are counted, not retained, and " +
      "the counts are reported per pair and never summed across pairs; a non-zero count " +
      "means the graph offers more hop sequences between those two ends than this query " +
      "enumerates, which bears on how distinct_routes is read and on nothing about " +
      "Spark"),
    "route_cap_reachable_maximum" ->
      byPairNum((_, t, _) => t.routeCapReachableMaximum.toLong),
    "route_cap_can_bind_by_pair" -> jbyPair(4, traversals.map(t =>
      t.pairId -> jbool(t.routeCapCanBind))),
    "never_summed_with" -> jstrArr(List(
      "the other pair in this query",
      SIBLING_CALLGRAPH_QUERY,
      SIBLING_DATAFLOW_QUERY)),
    "spurious_count" -> byPairNum((_, _, sp) => sp.spuriousCount.toLong),
    "spurious_definition" -> jstr("a route is spurious ONLY where it passes an " +
      "authorization or ACL predicate before reaching the sink, the predicate set being " +
      "exactly the five named selectors below; this judges the query, not Spark. The " +
      "selector block is byte-identical to queries 01 and 02, which is what makes the " +
      "three counts comparable"),
    "spurious_definition_limit" -> jstr("the definition evaluates ONLY those five " +
      "predicates, and it applies unchanged to BOTH pairs. Any other conditional on " +
      "either route is outside it and is NOT assessed by it. Concretely, for pair one " +
      PAIR_ONE_HANDLER_SOURCE_FILE + ":411 if (state != RecoveryState.ALIVE) guards the " +
      "branch that reaches createDriver at " + PAIR_ONE_HANDLER_SOURCE_FILE + ":417, and " +
      "it is a recovery-state check rather than one of the five, so it is neither " +
      "counted as a predicate nor reported as one; and for pair two whatever request " +
      "validation " + PAIR_TWO_HANDLER_METHOD + "'s own requestMessage match performs at " +
      PAIR_TWO_HANDLER_SOURCE_FILE + ":" + PAIR_TWO_HANDLER_SOURCE_LINE + " is likewise " +
      "outside the definition and unassessed. A spurious count of 0 therefore means " +
      "exactly and only what the definition says, and does not mean that either route " +
      "carries no conditional"),
    "expected_spurious_route_absent" -> byPairBool((_, _, sp) => sp.expectedSpuriousAbsent),
    "expected_spurious_absence_basis" ->
      byPairStr((_, _, sp) => if (sp.absenceIsStructural) "structural" else "filtering"),
    "expected_spurious_absence_detail" -> byPairStr((_, _, sp) => sp.basis),
    "expected_spurious_absence_statement" -> jstr(
      "per pair: no route in the emitted set passed an auth/ACL predicate as defined by " +
        "these five named selectors, and where the basis is structural no call site of " +
        "any of the five exists on that pair's own route surface at all, so no route " +
        "could have passed one. The absence is therefore a property of where those five " +
        "methods are invoked in the program, not evidence that the query filtered well - " +
        "and it is a statement about this query's own output under this query's own " +
        "definition of the term, about nothing else"),
    "bound_value" -> jnum(MAX_CALL_DEPTH.toLong),
    "bound_value_meaning" -> jstr("MAX_CALL_DEPTH, the maximum call-graph hops walked " +
      "from an entry point, applied per pair; it exceeds the hop count of either " +
      "documented route, so a route absent within it is not an artefact of a short bound"),
    "bound_reached" -> byPairBool((_, t, _) => t.boundReached),
    "bound_reached_any" -> jbool(traversals.exists(_.boundReached)),
    "bound_reached_any_convention" -> jstr("a disjunction over the per-pair flags, never " +
      "an arithmetic total"),
    "bounds" -> jobj(2, List(
      "MAX_CALL_DEPTH" -> jnum(MAX_CALL_DEPTH.toLong),
      "MAX_ROUTES_PER_PAIR" -> jnum(MAX_ROUTES_PER_PAIR.toLong),
      "MAX_EXPANSIONS_PER_ENTRY" -> jnum(MAX_EXPANSIONS_PER_ENTRY.toLong),
      "MAX_STEPS_PER_PAIR" -> jnum(MAX_STEPS_PER_PAIR.toLong),
      "MAX_TOTAL_RETURNS" -> jnum(MAX_TOTAL_RETURNS.toLong),
      "MAX_ENTRY_POINTS_PER_PAIR" -> jnum(MAX_ENTRY_POINTS_PER_PAIR.toLong),
      "MAX_CALL_SCAN" -> jnum(MAX_CALL_SCAN.toLong),
      "MAX_TYPE_SCAN" -> jnum(MAX_TYPE_SCAN.toLong),
      "FANOUT_CALLEE_THRESHOLD" -> jnum(FANOUT_CALLEE_THRESHOLD.toLong))),
    "bounds_meaning" -> jobj(2, List(
      "MAX_CALL_DEPTH" -> jstr("maximum call-graph hops walked from an entry point, " +
        "applied per pair"),
      "MAX_ROUTES_PER_PAIR" -> jstr("maximum distinct routes retained PER PAIR; never a " +
        "shared budget, because one pair filling a shared budget would silently truncate " +
        "the other"),
      "MAX_EXPANSIONS_PER_ENTRY" -> jstr("the per-entry-point step cap, counted in " +
        "method expansions rather than in edges, and ENFORCED at that scope: the " +
        "counter is reset at each entry point, so the figure compared against this " +
        "value is the peak any one entry point reached, published per walk as " +
        "max_expansions_at_one_entry_point. Each walk's total across all of its entry " +
        "points is published separately as method_expansions and caps nothing"),
      "MAX_STEPS_PER_PAIR" -> jstr("the per-pair step cap across all of that pair's " +
        "walks, counted in call sites considered, and ENFORCED at that scope: both of " +
        "a pair's walks draw on ONE budget, so the figure compared against this value " +
        "is the pair's total rather than either walk's own count, and the flag " +
        "pair_step_budget_exhausted is the pair's. It is never shared BETWEEN pairs"),
      "MAX_TOTAL_RETURNS" -> jstr("the total-returns cap across every record kind this " +
        "query emits; a QUERY-level cap rather than a per-pair one"),
      "MAX_ENTRY_POINTS_PER_PAIR" -> jstr("maximum entry points traversed per pair; the " +
        "remainder are counted as truncated rather than dropped silently"),
      "MAX_CALL_SCAN" -> jstr("cap on the indexed call-name sweeps: each pair's sink " +
        "name sweep, the predicate call-site sweep, and each message hop's producer and " +
        "consumer call-site sweeps. Every sweep it governs is listed with its own " +
        "observed count and truncation flag under bounded_sweeps, and the reached flag " +
        "for this cap is the DISJUNCTION over all of them. The sweeps are query-level, " +
        "so both pairs' entries carry the same flag: one measurement cited twice"),
      "MAX_TYPE_SCAN" -> jstr("cap on the indexed type-declaration sweep that measures " +
        "each route surface prefix's reach in the graph, and on the keyed type and " +
        "method lookups - each pair's entry-point and base-declaration selections, the " +
        "predicate type and its methods, each message type and its methods, the thread " +
        "hosts and bodies, and the JDK-launch declaration - so that no traversal in this " +
        "query materializes without a cap. Every sweep it governs is listed under " +
        "bounded_sweeps and the reached flag is the DISJUNCTION over all of them; a " +
        "QUERY-level cap rather than a per-pair one"),
      "FANOUT_CALLEE_THRESHOLD" -> jstr("a THRESHOLD rather than a cap: a call site " +
        "whose resolved callee set is wider than this is recorded as a dynamic-dispatch " +
        "fan-out site, and walk " + WALK_B_ID + " records it without expanding it"))),
    "bounds_reached_by_pair" -> jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jobj(4, boundsReachedFor(selections(i), traversals(i))
        .map { case (k, v) => k -> jbool(v) }))),
    "bounds_reached_basis_by_pair" -> jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jobj(4, boundsBasisFor(selections(i), traversals(i))
        .map { case (k, v) => k -> jstr(v) }))),
    "bounded_sweeps" -> jrawArr(4, boundedSweeps.toList.map(sw => jobj(6, List(
      "sweep" -> jstr(sw.label),
      "cap_name" -> jstr(sw.capName),
      "cap" -> jnum(sw.cap.toLong),
      "observed" -> jnum(sw.observed.toLong),
      "truncated" -> jbool(sw.truncated),
      "basis" -> jstr(sw.basis))))),
    "bounded_sweeps_completeness" -> jstr("every traversal this query materializes " +
      "OUTSIDE the walks is listed here, for BOTH pairs, and each goes through one " +
      "bounded-materialization helper that takes cap+1 elements and reports truncation " +
      "when it saw more than cap - so a cap applied at one site and forgotten at the " +
      "next is not expressible, and a sweep absent from this list did not run. A sweep " +
      "whose label names a pair was taken for that pair; a sweep whose label names none " +
      "is shared, taken once and cited by both, which is why the sweep list is not a sum " +
      "over pairs. The walks' own expansions are node-local rather than graph-wide (one " +
      "method group's call sites, one call site's linked callees) and are governed " +
      "instead by MAX_CALL_DEPTH, MAX_EXPANSIONS_PER_ENTRY, MAX_STEPS_PER_PAIR and " +
      "MAX_ENTRY_POINTS_PER_PAIR, each published above with its own per-pair reached " +
      "flag and basis. Stating the division rather than claiming one helper covers both " +
      "is the point: the counters that bound a walk are a different mechanism from the " +
      "cap that bounds a sweep, and a reader checking the claim needs to know which " +
      "governs what"),
    "bounds_reached_convention" -> jstr("every named bound above carries a value and, " +
      "here, a reached flag with the measurement it was read from - per pair, in the " +
      "declared pair order, and never aggregated across pairs. MAX_TOTAL_RETURNS is a " +
      "query-level cap, so its entry is the same measurement in both pairs' objects: one " +
      "measurement cited twice rather than two measurements. FANOUT_CALLEE_THRESHOLD is " +
      "a threshold rather than a cap, so \"reached\" there means exceeded and is not a " +
      "truncation of either traversal"),
    "entry_points_discovered" -> byPairNum((s, _, _) => s.entryPointsDiscovered.toLong),
    "entry_points_traversed" -> byPairNum((s, _, _) => s.entryPointsTraversed.toLong),
    "entry_points_truncated" -> byPairNum((s, _, _) => s.entryPointsTruncated.toLong),
    "entry_points_truncated_meaning" -> jstr("the two counters are reported separately, " +
      "per pair, so that a sweep cannot run unbounded and a trimmed traversal cannot " +
      "pass for a complete one. A truncated count above zero is a measured property of " +
      "that pair's traversal, to be reported rather than hidden; it is zero for both " +
      "pairs here, each pair's discovered count sitting well inside the per-pair cap of " +
      MAX_ENTRY_POINTS_PER_PAIR.toString),
    "operator_pseudo_calls_excluded" -> jstr("a CPG <operator>.* call is an artefact of " +
      "the representation rather than a method call, so it is not expanded and not " +
      "counted as a hop: expanding them would inflate every counter of both pairs " +
      "without adding a call-graph edge"),
    "duplicate_class_definitions_unioned" -> jstr("the graph carries more than one node " +
      "per class where two staged archives carried the same class, so method nodes are " +
      "grouped by full name and their call sites unioned rather than one node being " +
      "picked. Reachability is keyed on the method full name, identically for both pairs"),
    "entry_point_selection" -> jstr("per pair, the UNION of two arms: the synthetic " +
      "method named by the pair on every type matching the pair's synthetic type regex, " +
      "and the source-level method named by the pair on the pair's exact handler type. " +
      "Which arm carries the route is then MEASURED against the pair's declared body " +
      "witness rather than assumed, because the two pairs answer it differently: a " +
      "partial-function handler's body lives in a synthetic class, an ordinary method's " +
      "is its own"),
    "pairs" -> jrawArr(2, pairObjects),
    "graph" -> jobj(2, List(
      "path_source" -> jstr(cpgPathSource),
      "named_path" -> jstr(cpgNamedLabel),
      "named_path_repo_relative" -> jstr(cpgNamedRepoRelativeLabel),
      "resolved_target" -> jstr(cpgResolvedLabel),
      "absolute_host_paths_emitted" -> jbool(false),
      "resolved_target_identification" -> jstr("no absolute host path is emitted " +
        "anywhere in this envelope, so the resolved target is identified by the " +
        "symlink-FOLLOWING byte size and sha256 below rather than by a host path. That " +
        "pair is the identity of record and is what every load re-verifies; a host path " +
        "would additionally vary between two checkouts of one branch and so could not be " +
        "part of a deterministic envelope"),
      "resolved_target_path_literal_lives_in" -> jstr(LOG_DIR + "/probe-" + QUERY_ID +
        ".log, the console stream this invocation wrote. Console records are not held " +
        "to byte-identity and may carry a host path; this envelope is, and does not. " +
        "The identity comparison turns on the size and digest pair below rather than " +
        "on any path literal"),
      "measurement_semantics" -> jstr("symlink-FOLLOWING. The named path is a small " +
        "symlink, so measuring the link itself records a few dozen bytes rather than the " +
        "graph: byte_size_without_following is recorded only to be discarded. The size " +
        "below is the resolved target's; the sha256 below is taken from the bytes as " +
        "they were copied into the private input this run imported, and the copy's size " +
        "is asserted equal to the target's, so the pair describes one set of bytes " +
        "rather than two measurements of two files"),
      "named_path_is_symlink" -> jbool(cpgIsLink),
      "byte_size_following_the_link" -> jnum(sizeFollow),
      "byte_size_without_following" -> jnum(sizeNoFollow),
      "sha256" -> jstr(shaObserved),
      "identity_record" -> jstr(recordOfAccountLabel),
      "identity_record_provenance" -> jstr(recordOfAccount.provenance),
      "identity_record_resolution" -> jstr("resolved by PROVENANCE in a fixed order, " +
        "never by which candidate matched: the in-checkout frontend log " +
        "(" + CPG_FRONTEND_RECORD_PATH + ") governs where it carries exactly one strict " +
        "bytes:/sha256: pair, because such a pair exists only if this checkout's " +
        "frontend wrote a graph; otherwise the provisioning record of account beside the " +
        "RESOLVED graph governs, at <the graph's directory>/../" +
        CPG_PROVISION_RECORD_DIR + "/ over " + CPG_PROVISION_RECORD_NAMES.mkString(", ") +
        ". Every candidate that exists is read, ambiguity inside one record and " +
        "disagreement between two are both fatal, and the order therefore selects a " +
        "writer rather than an outcome. This is the same resolution " +
        "harness/lib/preflight_graph_identity.py applies to the Stage 3 runner, so the " +
        "probe and that gate cannot adjudicate one load against two records"),
      "identity_record_corroborated_by" -> jstrArr(recordCorroborators),
      "identity_record_candidates_read" -> jnum(identityCandidates.size.toLong),
      "identity_record_source" -> jstr("resolved from this query's own source: no " +
        "environment variable selects it, so the record a reader can reach from the " +
        "published reproduction command is exactly the record this comparison turned on"),
      "identity_record_role" -> jstr("the declared owner of this pair, which computed " +
        "it at write time with the same symlink-following semantics; this envelope " +
        "cites that measurement rather than establishing a second one. The comparison " +
        "is made immediately before the load and a mismatch halts the run rather than " +
        "being weakened or skipped"),
      "identity_recorded_byte_size" -> jnum(recordedSize),
      "identity_recorded_sha256" -> jstr(recordedSha),
      "identity_comparison_result" -> jstr(
        if (sizeMatches && shaMatches)
          "match - the observed byte size and sha256 equal the pair the record of " +
            "account owns, on both values"
        else "mismatch - unreachable in a written envelope, because a mismatch halts " +
          "the run before anything is written"),
      "identity_reverified_before_load" -> jbool(true),
      "identity_record_override_exists" -> jbool(false),
      "identity_record_override_note" -> jstr("this query reads no environment " +
        "variable that could point the identity comparison at another record. An " +
        "override would let a load be adjudicated by a record no environment contract " +
        "defines and the reproduction command does not name; where the host's graph and " +
        "the resolved record disagree, the run halts and the disagreement is reported"),
      "loaded_bytes_are_a_private_copy" -> jbool(true),
      "private_copy_protocol" -> jstr("the digest above is taken FROM THE BYTES BEING " +
        "COPIED, in one pass, into a directory this run created under the clone-private " +
        "scratch root with a random name and mode 0700; the copy is then set read-only " +
        "in a non-writable directory, and importCpg is given that copy and nothing else. " +
        "Measuring the host-shared path and then importing it would be two opens of one " +
        "name, so the pair compared would not be the pair the engine read - and both " +
        "pairs' measurements would rest on it"),
      "private_copy_verified_after_load" -> jbool(true),
      "private_copy_post_load_check" -> jstr("size, sha256 and INODE re-measured after " +
        "the load and required unchanged, which detects a swap across the load window " +
        "rather than assuming one cannot happen. Residual limit, stated rather than " +
        "papered over: importCpg accepts a path, so the engine's own open is by name - " +
        "what is established is that the name is one this run created, in a directory " +
        "only this run can write, holding bytes whose digest was taken as they were " +
        "written and re-verified once the engine was done with them"),
      "private_copy_retained_after_verification" -> jbool(privateInputRetained),
      "aap_named_path_reconciliation" -> jstr(aapNameReconciliation),
      "methods" -> jnum(methodCount.toLong),
      "type_declarations" -> jnum(typeDeclCount.toLong),
      "files" -> jnum(fileCount.toLong))),
    "runtime" -> jobj(2, List(
      "jdk_major" -> jstr(jdkMajor),
      "jdk_major_numeric" -> jnum(jdkMajor.toLong),
      "jdk_major_required" -> jstr(REQUIRED_JDK_MAJOR),
      "jvm_version" -> jstr(jvmVersion),
      "jvm_arguments_observed_count" -> jnum(jvmInputArgs.size.toLong),
      "jvm_arguments_kept" -> jstrArr(jvmArgsKept),
      "jvm_arguments_redacted_count" -> jnum(jvmArgsRedacted.size.toLong),
      "jvm_arguments_redacted_keys" -> jstrArr(jvmArgsRedacted.map(redactJvmArgument)),
      "jvm_arguments_redaction_policy" -> jstr(JVM_ARG_REDACTION_POLICY),
      "heap_actually_used_bytes" -> jnum(heapMaxBytes),
      "heap_actually_used_gib" -> jnum(heapMaxBytes / (1024L * 1024L * 1024L)),
      "heap_floor_bytes" -> jnum(HEAP_FLOOR_BYTES),
      "heap_floor_gib" -> jnum(HEAP_FLOOR_BYTES / (1024L * 1024L * 1024L)),
      "heap_at_or_above_floor" -> jbool(heapMaxBytes >= HEAP_FLOOR_BYTES),
      "heap_above_floor" -> jbool(heapMaxBytes > HEAP_FLOOR_BYTES),
      "heap_pre_touch_proof" -> jstr("the floor value's own commit proof is the gate's " +
        "java -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version, which exited 0. A heap above " +
        "the floor is permitted and reported, and is proven committable by that same " +
        "pre-touch test before use; a heap below it is not, which is why this query " +
        "measures Runtime.maxMemory() and halts rather than trusting the flag it was given"),
      "heap_direction_rule" -> jstr("the floor is a minimum and a default, never a " +
        "ceiling: a larger heap is permitted and reported, and a smaller one is not, " +
        "because a truncated result's silence cannot be told apart from a clean one - and " +
        "this query traverses two pairs, so a truncation would be silent twice"),
      "heap_override_mechanism" -> jstr("MEASURED, NOT ASSUMED: joern's --script path " +
        "forks a child JVM with no JVM options forwarded, so -J-Xmx reaches the launcher " +
        "only and the child would otherwise run at an ergonomic default. " +
        "JAVA_TOOL_OPTIONS is inherited by the child and is what actually raises the heap " +
        "the query runs at, which is why the heap above is Runtime.maxMemory() rather " +
        "than a transcription of a flag"),
      "loader" -> jstr("importCpg into a switched workspace; the frontend-then-importCpg " +
        "route is mandated because the alternative spawns a second JVM at the same heap"),
      "loader_is_importcpg_only" -> jbool(true),
      "loader_alternative_absent_from_the_source" ->
        jbool(alternativeLoaderOccurrences == 0),
      "loader_alternative_absence_basis" -> jstr("MEASURED, not asserted: this query " +
        "reads its own source file at run time and counts occurrences of the alternative " +
        "loader's name, assembled at run time so the search term is not itself a literal " +
        "occurrence. The count observed was " + alternativeLoaderOccurrences.toString +
        ", and any count above zero halts the run before anything is written, so the " +
        "true value is the only one an envelope can carry"),
      "workspace" -> jstr(WORKSPACE_PATH),
      "workspace_run_directory" -> jstr(workspaceRunRepoRelative),
      "workspace_safety" -> jstr("the AAP-named root is reached by a descriptor descent " +
        "from the resolved repository root that refuses a symbolic link at any " +
        "component; the directory actually switched to is created by this run with " +
        "createDirectory under a random name carrying this query's id, so an existing " +
        "name fails rather than being reused and no previous run's project state is " +
        "inherited; an exclusive file lock is held inside it for the rest of the run, " +
        "because two Joern processes in one clone are what corrupts a workspace; and it " +
        "is its verified real path that is handed to switchWorkspace. It is removed by a " +
        "shutdown hook confined to that directory, since the engine holds project state " +
        "in it until the JVM exits - and both pairs are traversed before it does"),
      "workspace_lock_held" -> jbool(true),
      "heap_bound_jvm_position" -> jstr("the Stage 5 probe, one of the four heap-bound " +
        "JVM invocations this run records separately (frontend build, importCpg " +
        "verification load, Stage 3 Joern runner, this probe)"),
      "command" -> jstr(REPRODUCTION_COMMAND),
      "command_precondition" -> jstr(REPRODUCTION_COMMAND_PRECONDITION),
      "command_working_directory" -> jstr("$" + SCRATCH_ROOT_ENV_VAR + ", the " +
        "clone-private scratch root, because joern eagerly creates ./workspace in its " +
        "own working directory and nothing named workspace is ignored by the " +
        "repository's root .gitignore"),
      "command_graph_selector" -> jstr("$" + CPG_ENV_VAR + ", named explicitly in the " +
        "command because it selects the graph bytes this query loads - the one set of " +
        "bytes both pairs are measured against. Where it is unset the repo-relative " +
        "default " + CPG_PATH_DEFAULT + " is used, and which of the two applied is " +
        "published as graph.path_source"),
      "command_completeness" -> jstr("the command above is COMPLETE and runnable as " +
        "written: every environment value this query reads appears in it, and it reads " +
        "no other. The three HARNESS_* values are written as variable references rather " +
        "than literal paths because an absolute path is a property of a checkout rather " +
        "than of the measurement, and the precondition published beside the command " +
        "names what exports them. There is still no variable that selects the identity " +
        "record: the record of account is resolved by provenance from the in-checkout " +
        "frontend log and the provisioning record beside the resolved graph, both " +
        "reached from values this command names, so a reader running it reproduces the " +
        "run this envelope describes rather than a differently adjudicated one"),
      "parameters_passed_on_the_command_line" -> jstr("none: the pairs are declared as " +
        "named constants in the query source and both are invoked in a single run, so " +
        "the invocation is reproducible from this command alone"))),
    "predicate_selector" -> jobj(2, List(
      "type" -> jstr(PREDICATE_TYPE),
      "name_regex" -> jstr(PREDICATE_NAME_REGEX),
      "setter_suffix_excluded" -> jstr(PREDICATE_SETTER_SUFFIX),
      "named_five" -> jstrArr(PREDICATE_NAMED_FIVE.sorted),
      "source_file" -> jstr(predicateSourceFile),
      "source_file_lines_at_the_pin" -> jnum(predicateSourceFileLines.toLong),
      "named_five_with_source_lines" -> jstrArr(predicateNamedFiveWithSourceLines),
      "bytecode_constraint" -> jstr("a type anchor on " + PREDICATE_TYPE + " together " +
        "with an explicit " + PREDICATE_SETTER_SUFFIX + " exclusion. On bytecode the " +
        "anchored name pattern alone is not enough, so the narrowing is three steps and " +
        "all three sets are reported below rather than asserted"),
      "bytecode_collision_source" -> jstr(predicateSourceFile + ":59 declares private " +
        "var aclsOn, and Scala compiles a private var into accessors, so the graph " +
        "carries both aclsOn() and aclsOn_$eq(boolean) and both satisfy the acls.* " +
        "alternative of the anchored pattern"),
      "over_match_hazards_excluded" -> jstrArr(predicateOverMatchHazards),
      "over_match_hazards_note" -> jstr("these are further methods on the same anchored " +
        "type whose names a naive acls.* pattern would match; they are not predicates " +
        "and the three-step narrowing leaves none of them in the final set"),
      "deliberate_non_selectors" -> jstrArr(predicateDeliberateNonSelectors),
      "selector_set_was_widened" -> jbool(false),
      "selector_set_widening_statement" -> jstr("the set is exactly the five named " +
        "selectors and was not widened: the two Boolean methods listed as deliberate " +
        "non-selectors are on the same anchored type and are excluded on purpose, " +
        "because widening the set would change what a spurious count means and would " +
        "break comparability with queries 01 and 02"),
      "identical_across_both_pairs" -> jbool(true),
      "identical_across_both_pairs_statement" -> jstr("the parameterization varies the " +
        "handler and the sink, never the predicate set: the same five selectors, the " +
        "same anchor and the same exclusion apply to both pairs. Varying them per pair " +
        "would make the two pairs' spurious counts mean different things, which is the " +
        "one way a parameterization could silently change the measurement"),
      "block_is_byte_identical_to" -> jstrArr(List(
        "queries/joern/" + SIBLING_CALLGRAPH_QUERY + ".sc",
        "queries/joern/" + SIBLING_DATAFLOW_QUERY + ".sc")),
      "type_method_nodes" -> jnum(predicateTypeMethods.size.toLong),
      "type_distinct_method_names" -> jnum(predicateTypeMethodNames.size.toLong),
      "step1_broad_matches" -> jstrArr(predicateBroadNames),
      "step2_setters_excluded" -> jstrArr(predicateSetterExcludedNames),
      "step2_remaining" -> jstrArr(predicateAfterSetterNames),
      "step3_non_predicate_residue_dropped" -> jstrArr(predicateNonPredicateResidue),
      "final_names" -> jstrArr(predicateFinalNames),
      "final_full_names" -> jstrArr(predicateFullNames),
      "call_sites_graph_wide" -> jnum(predicateCallSites.size.toLong),
      "distinct_callers_graph_wide" -> jnum(predicateCallerNames.size.toLong),
      "intermediate_route_hop_surface_type_prefixes" ->
        jstrArr(ROUTE_HOP_SURFACE_TYPE_PREFIXES),
      "intermediate_route_hop_rationale" -> jstr("both pairs' routes run handler -> RPC " +
        "-> Worker -> DriverRunner -> launch, so the Worker is a hop of the route and not " +
        "an end of it. It is on each pair's route surface for that reason: a surface " +
        "naming only the handler and the sink host would leave one hop unsearched while " +
        "the resulting statement read as one about the whole route"),
      "intermediate_route_hop_source_file" -> jstr(ROUTE_HOP_WORKER_SOURCE_FILE),
      "intermediate_route_hop_source_file_lines_at_the_pin" ->
        jnum(ROUTE_HOP_WORKER_SOURCE_FILE_LINES.toLong),
      "intermediate_route_hop_anchors" -> jstrArr(ROUTE_HOP_WORKER_ANCHORS),
      "route_surface_prefix_evidence" -> jrawArr(4, surfacePrefixEvidence.map(e =>
        jobj(6, List(
          "prefix" -> jstr(e.prefix),
          "type_declarations_in_the_graph" -> jnum(e.typeDeclsInTheGraph.toLong),
          "methods_in_the_graph" -> jnum(e.methodsInTheGraph.toLong),
          "predicate_call_sites_on_it" -> jnum(e.predicateCallSites.toLong),
          "on_pairs" -> jstrArr(e.onPairs))))),
      "route_surface_prefix_evidence_convention" -> jstr("one entry per prefix on either " +
        "pair's route surface or on the shared list, reporting THE SAME fields for every " +
        "prefix including the intermediate hop. The reach fields are what make a zero " +
        "call-site count falsifiable: a zero over a surface the graph does not carry " +
        "would read exactly like a searched surface that came back clean, so a prefix " +
        "with no type declaration aborts the run instead of contributing one"),
      "route_surface_prefixes_present_with_no_methods" ->
        jstrArr(surfacePrefixesPresentWithNoMethods),
      "route_surface_type_declaration_sweep_truncated" -> jbool(surfaceTypeScanTruncated),
      "shared_route_surface_type_prefixes" -> jstrArr(ROUTE_SURFACE_TYPE_PREFIXES),
      "route_surface_derivation" -> jstr("DERIVED from this query's own route " +
        "ends across BOTH pairs rather than declared: pair one's entry-point owner, pair " +
        "two's entry-point owner (the servlet class handleSubmit is declared in), the " +
        "REST server that constructs and mounts that servlet, the consumer end of " +
        "every RPC hop on either pair's route - the Worker for the " +
        MESSAGE_HOP_LAUNCH_DRIVER_ID + " hop on both pairs, and the Master for pair " +
        "two's " + MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_ID + " hop, which is already on " +
        "the surface as pair one's entry-point owner - and the two sink hosts the " +
        "launch call sites actually sit on. It is NOT part of the byte-identical " +
        "predicate block: " +
        "the four predicate constants define the term spurious and are compared across " +
        "the three sources under the names the formulation identity block declares, " +
        "while a route surface is a property of one query's own route - and this query's " +
        "route has two entry-point owners where its siblings have one. Stage I asserts " +
        "that every route end this query measured, in either pair, is covered by one of " +
        "these prefixes, and the per-prefix reach evidence above is what makes each " +
        "zero falsifiable"),
      "route_surface_type_roles" -> jstrArr(
        ROUTE_SURFACE_TYPE_ROLES.map { case (pref, role) => pref + ": " + role }),
      "measured_route_ends" -> jstrArr(measuredRouteEnds.map {
        case (pairId, role, fn) => s"$pairId | $role | $fn" }.distinct.sorted),
      "route_surface_reach_evidence" -> jrawArr(4, routeSurfaceReach),
      "route_surface_prefixes_accounting_for_no_measured_route_end" ->
        jstrArr(routeSurfacePrefixesAccountingForNoEnd),
      "route_surface_prefixes_accounting_for_no_measured_route_end_note" ->
        jstr("a prefix here is on the surface because the route reaches that type " +
          "before reaching an end of the route, not because it owns one: pair two's " +
          "remote entry is served by the REST server, which constructs and mounts the " +
          "servlet whose method is the measured entry point. The zero is published " +
          "rather than absorbed into a covered surface, because a surface reported as " +
          "covering every measured end says nothing about a prefix that covers none"),
      "route_surface_covers_every_measured_end" -> jbool(routeEndsNotCovered.isEmpty),
      "call_sites_on_the_shared_route_surface" ->
        jnum(predicateCallSitesOnSharedSurface.size.toLong),
      "call_sites_on_each_pair_own_route_surface" -> jbyPair(4, PAIRS.map(p =>
        p.id -> jnum(predicateCallSitesByPair(p.id).size.toLong))),
      "handler_types_not_covered_by_the_shared_prefixes" ->
        jstrArr(pairsNotCoveredBySharedSurface),
      "invocation_scope" -> jstr("the zero counts are scoped to the two pairs' routes, " +
        "not to the program: the five predicates ARE invoked elsewhere, including inside " +
        "the anchored type itself, and those call sites are listed next so the scope of " +
        "the claim is visible rather than implied"),
      "call_sites_inside_the_anchored_type_itself" ->
        jstrArr(predicateCallSitesInsideItsOwnType),
      "route_files_searched_for_the_five" -> jstrArr(predicateRouteFilesSearched),
      "route_files_occurrences_of_the_five" -> jstr("none: searching all five names " +
        "across the " + predicateRouteFilesSearched.size.toString + " route files at the " +
        "pin - the two handlers, the intermediate Worker hop and the sink host - returns " +
        "nothing in any of them, which is the source-level counterpart of the graph " +
        "measurement above"),
      "reference_is_not_invocation" -> jstr("a held reference is not an invocation. " +
        "Every mention of the anchored type on the route surface is a reference of one " +
        "of the kinds listed next, and none of them invokes any of the five, which is " +
        "why the wording throughout this envelope is invoked rather than referenced"),
      "references_that_are_not_invocations" ->
        jstrArr(predicateReferencesThatAreNotInvocations),
      "pair_two_handler_file_references_to_the_anchored_type" -> jstr("none: " +
        PAIR_TWO_HANDLER_SOURCE_FILE + " carries no reference to " + PREDICATE_TYPE + " " +
        "at all at the pin - not an import, not a field, not a constant read - which is " +
        "the additional detail that makes pair two's expected-spurious basis structural " +
        "rather than a property of this query's filtering"))),
    "boundaries" -> jrawArr(2, boundaries.map(b => jobj(4, List(
      "boundary_id" -> jstr(b.id),
      "boundary_kind" -> jstr(b.kind),
      "cited_by_pairs" -> jstrArr(citationsOf(b.id)),
      "crossed_by_a_call_edge" -> jbool(b.crossedByACallEdge))))),
    "boundary_ids_by_pair" -> jbyPair(2, PAIRS.map(p =>
      p.id -> jstrArr(boundaryIdsByPair(p.id)))),
    "boundary_count_by_pair" -> jbyPair(2, PAIRS.map(p =>
      p.id -> jnum(boundaryIdsByPair(p.id).size.toLong))),
    "boundaries_not_crossed_by_pair" -> jbyPair(2, PAIRS.map(p =>
      p.id -> jstrArr(boundaryIdsByPair(p.id)
        .filter(id => !boundaryStore(id).crossedByACallEdge)))),
    "shared_hops_are_one_measurement_cited_by_both_pairs" -> jbool(true),
    "duplicate_formulation" -> jstr(duplicateFormulationAggregate),
    "duplicate_formulation_summary" -> jstr(duplicateFormulationSummary),
    "duplicate_formulation_aggregation" -> jstr(duplicateFormulationAggregation),
    "duplicate_formulation_relation" -> jstr(duplicateFormulationRelation),
    "duplicate_formulation_detail" -> duplicateFormulationJson,
    "duplicate_formulation_pair_one_measured_here" -> pairOneMeasuredJson,
    "effort_query_revisions_committed" ->
      (if (revisionsEstablished) jnum(revisionCommits.size.toLong) else "null"),
    "effort_query_revisions_established" -> jbool(revisionsEstablished),
    "effort_query_revisions_measurement" -> jstr(revisionsNote),
    "effort_query_revisions_commits" -> jstrArr(revisionCommits),
    "effort_query_revisions_convention" -> jstr(QUERY_REVISIONS_CONVENTION),
    "effort_query_revisions_measured_at_head" ->
      (revisionHead.map(jstr).getOrElse("null")),
    "effort_query_revisions_measured_on_branch" ->
      (revisionBranch.map(jstr).getOrElse("null")),
    "effort_query_revisions_ancestry_verified" -> jbool(revisionsEstablished),
    "effort_joern_api_constructs" -> jstrArr(JOERN_API_CONSTRUCTS),
    "effort_joern_api_construct_count" -> jnum(JOERN_API_CONSTRUCTS.distinct.size.toLong),
    "effort_joern_api_constructs_not_used_by_01" -> jstrArr(
      relations.find(_.theirs.queryId == SIBLING_CALLGRAPH_QUERY)
        .map(_.apiOnlyHere).getOrElse(Nil)),
    "effort_joern_api_constructs_not_used_by_02" -> jstrArr(
      relations.find(_.theirs.queryId == SIBLING_DATAFLOW_QUERY)
        .map(_.apiOnlyHere).getOrElse(Nil)),
    "effort_joern_api_constructs_difference_basis" -> jstr("each list is the set " +
      "difference computed at run time between this query's declared construct list and " +
      "that sibling's own declared list, read out of the sibling's SOURCE file. Where a " +
      "sibling source could not be read the list is empty and the relation entry above " +
      "says so, so an empty list is never a silent claim of identity"),
    "effort_parameterizability" -> jstr(parameterizabilityVerdict),
    "parameterizability" -> parameterizabilityJson,
    "total_returns_cap_reached" -> jbool(totalReturnsCapReached),
    "records_order" -> jstr("an explicit TOTAL sort key. Boundary records come first, in " +
      "the fixed boundary-identifier order this query declares - " +
      boundaries.map(_.id).mkString(", ") + " - and each names the pair or pairs citing " +
      "it; route records follow, grouped by pair IN THE DECLARED PAIR ORDER (" +
      PAIRS.map(_.id).mkString(", ") + ") and ordered WITHIN a pair by the COMPLETE " +
      "published tuple (pair id, walk id, entry point, sink host, hop count, then the " +
      "whole ordered hop sequence rendered as one key). The hop sequence is last " +
      "precisely so two routes sharing their endpoints and their hop count still have a " +
      "defined order, and a route identical in every hop has already been removed by " +
      "the deduplication that precedes the sort. No two records share the key, so the " +
      "order is total rather than merely stable, and the whole sequence is truncated to " +
      "MAX_TOTAL_RETURNS. Grouping by pair is an ordering, never an aggregation: no " +
      "record and no count is shared between the two pairs' groups"),
    "collection_order" -> jstr("every other collection carries an explicit order too. " +
      "Per-pair objects and every by-pair map follow the declared pair order, which is " +
      "fixed in the source as a List and never taken from a map or set iteration. Lists " +
      "of SOURCE constructs ascend by pinned-tree source line - " +
      "named_five_with_source_lines, deliberate_non_selectors, " +
      "over_match_hazards_excluded, call_sites_inside_the_anchored_type_itself and " +
      "references_that_are_not_invocations, the last grouped by file in route order. " +
      "Lists of graph or query identifiers ascend lexicographically: named_five, " +
      "final_names, final_full_names, entry_points, sink_hosts, " +
      "route_files_searched_for_the_five, never_summed_with, " +
      "effort_joern_api_constructs and the api-construct difference lists. Boundary " +
      "lists follow the declared boundary-identifier order, the bounds objects follow " +
      "the declaration order of the named constants in the source, and each pair's walk " +
      "list follows the declared walk-mode order. Every one of these is a fixed function " +
      "of the inputs, so no collection depends on an iteration order"))

  val provenanceJson = jobj(2, List(
      "measured_values_cited_from" -> jstr(LOG_DIR + "/probe-" + QUERY_ID + ".log"),
      "measured_values_note" -> jstr("every count, status, flag, walk figure, selector " +
        "figure and graph figure in this envelope is this invocation's own measurement, " +
        "and the console stream is the same measurement written a second time. One " +
        "measurement cited twice, never two measurements, so a disagreement between this " +
        "envelope and that stream would be a defect in this envelope"),
      "graph_identity_owner" -> jstr(recordOfAccountLabel + " is the one record of " +
        "account for this run, chosen by provenance from the candidates that exist " +
        "rather than named by a constant: a write-time record left by THIS checkout's " +
        "frontend governs where it states exactly one identity pair, and otherwise the " +
        "provisioning record beside the graph does. No environment variable can select " +
        "another. Every candidate that exists was read, more than one distinct pair " +
        "inside any single record and any disagreement between two records each abort " +
        "the run, and the corroborating records are named beside the chosen one" +
        (if (recordCorroborators.isEmpty) ""
         else ": " + recordCorroborators.mkString(", ")) +
        ". The pair it states is what this query re-measured the resolved target and " +
        "its private copy against before loading, and the pair observed is published " +
        "beside it so the comparison is checkable rather than asserted"),
      "query_source" -> jstr("queries/joern/" + QUERY_ID + ".sc"),
      "bound_constants_defined_by" -> jstr("the query source, as named vals; no inline " +
        "literal governs behaviour and no bound value is chosen in this envelope"),
      "sibling_figures" -> jstr("NONE is transcribed. No figure published by " +
        SIBLING_CALLGRAPH_QUERY + " or " + SIBLING_DATAFLOW_QUERY + " is copied into " +
        "this file or restated in this envelope. What this query reads of a sibling is " +
        "its SOURCE - the declared formulation identity block, read at run time under " +
        "names all three queries share - and the duplicate-formulation relation is " +
        "computed from that by one shared predicate. A sibling's counts were measured " +
        "against the graph of its own run and stay in its own result files, where a " +
        "reader comparing them reads the measurement itself rather than a copy that " +
        "could drift from it"),
      "query_source_sha256" -> jstr(sourceSha256),
      "publication_id" -> jstr(publicationId),
      "line_numbers_verified_against" -> jstr("the pinned tree at " +
        "59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d, exported as SPARK_SRC. Every source " +
        "line cited here is that tree's, not this checkout's, which differ on " +
        "core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala"),
      "graph_path_expression" -> jstr("the graph path fields are expressed by " +
        "environment-variable name and by repository-relative form, so this envelope " +
        "carries no value that varies between two checkouts of one branch"),
      "contributes_dataset_rows" -> jbool(false),
      "dataset_separation" -> jstr("nothing here is written into harness/artifacts/raw/ " +
        "and nothing is folded into oss-scan-results/findings.json. This tree is the " +
        "deliberate second appearance of this tool, as the subject of the capability " +
        "probe rather than as one of the scanned runners, and folding it in would " +
        "corrupt both that tool's row count and the dataset total")))

  val envelopeWithoutDeterminism = jobj(0, envelopeCoreFields ++ List(
    "provenance" -> provenanceJson,
    "records" -> jrawArr(2, recordJson)))
  // MEASURED, not asserted: every absolute path this run resolved is searched for
  // in the rendered envelope, and only the determinism block - which carries the
  // LABELS of any hit rather than the paths - stays outside the searched text, so
  // reporting a hit cannot itself emit one.
  val absolutePathsSearchedFor: List[(String, String)] = (List(
    "the repository root" -> repoRoot.toString,
    "the graph's named path" -> cpgNamed.toString,
    "the graph's resolved target" -> cpgResolved.toString,
    "the results directory" -> resultsDir.toString,
    "this envelope's own path" -> jsonPath.toString,
    "the prose report's path" -> mdPath.toString) ++
    repoRootRealPath.map("the resolved repository root" -> _.toString).toList ++
    logTargetPath.map("the console log's path" -> _.toString).toList ++
    Option(System.getProperty("java.io.tmpdir"))
      .map("the JVM temporary directory" -> _).toList ++
    Option(System.getProperty("user.home")).map("the JVM home directory" -> _).toList)
    .filter { case (_, v) => v != null && v.length >= ABSOLUTE_PATH_SEARCH_MIN_LENGTH }
    .distinct
  val absolutePathsFound = absolutePathsSearchedFor
    .filter { case (_, v) => envelopeWithoutDeterminism.contains(v) }
    .map(_._1).distinct.sorted
  log(s"absolute host paths       : ${absolutePathsFound.size} of " +
    s"${absolutePathsSearchedFor.size} searched-for paths occur in the envelope" +
    (if (absolutePathsFound.isEmpty) "" else ": " + absolutePathsFound.mkString(", ")))

  val determinismJson = jobj(2, List(
      "byte_identity_contract" -> jstr("an unchanged query source, run over an " +
        "unchanged graph with an unchanged commit history for that source path, emits " +
        "byte-identical bytes from any checkout. Commit history is part of the contract " +
        "because the revision count above is measured from git rather than written " +
        "down, so a new commit on this path legitimately changes one field. Every " +
        "collection carries an explicit total order, the serialization uses a fixed key " +
        "order and a fixed layout, and the file ends in a single trailing newline"),
      "pair_iteration_order_is_fixed" -> jbool(true),
      "pair_iteration_order_basis" -> jstr("the pairs are declared as a List in the " +
        "source and are selected, walked, classified, recorded and reported by index in " +
        "that order (" + PAIRS.map(_.id).mkString(", ") + "). No stage iterates a map or " +
        "a set of pairs, which is the determinism requirement specific to a " +
        "parameterized query: a pair order taken from an unspecified iteration would " +
        "reorder the per-pair objects and the record groups between two runs"),
      "non_deterministic_quantities_excluded" -> jstrArr(List(
        "absolute host paths",
        "elapsed times",
        "host names",
        "process identifiers",
        "scratch and temporary directory names",
        "wall-clock timestamps",
        "workspace instance names")),
      "elapsed_times_live_in" -> jstr(LOG_DIR + "/probe-" + QUERY_ID + ".log, a console " +
        "stream that is deliberately not held to byte-identity"),
      "absolute_host_paths_emitted" -> jbool(absolutePathsFound.nonEmpty),
      "absolute_host_paths_measurement" -> jstr("MEASURED, not asserted: the " +
        absolutePathsSearchedFor.size + " absolute paths this run resolved were each " +
        "searched for in the rendered envelope, and what is reported is the outcome of " +
        "that search. Only this determinism block is outside the searched text, and it " +
        "carries the LABELS of any path found rather than the paths themselves, so " +
        "reporting a hit cannot itself emit one. A path shorter than " +
        ABSOLUTE_PATH_SEARCH_MIN_LENGTH + " characters is not searched for, because a " +
        "very short prefix matches ordinary prose"),
      "absolute_host_path_labels_found" -> jstrArr(absolutePathsFound),
      "trailing_newline" -> jbool(true),
      "publication_id" -> jstr(publicationId),
      "reproduction_command" -> jstr(REPRODUCTION_COMMAND),
      "mixed_generation_detection" -> jstr("every member of this publication carries " +
        "the same publication identifier and the same query source sha256. A consumer " +
        "holding two members whose identifiers differ is holding two generations, and " +
        "that is detectable from the members themselves rather than from a separate " +
        "marker file. The converse does NOT hold and is not claimed: equal publication " +
        "identifiers mean equal query, source and graph, which two separate invocations " +
        "share, so sameness of generation is settled by the completion manifest's " +
        "member_set_id over MEMBER BYTES and not by this identifier"),
      "reproduction_check_method" -> jstr("invoke this source again over the same graph " +
        "from an isolated repository root and compare the two envelopes byte for byte, " +
        "including the pair order and the record grouping. The isolation matters: the " +
        "console log is written to a fixed repository-relative path, so a re-run inside " +
        "a checkout would overwrite the record of the invocation it is being compared " +
        "against. Two roots also test the property the path fields exist to have - that " +
        "no field varies with the checkout")))

  val envelope = jobj(0, envelopeCoreFields ++ List(
    "determinism" -> determinismJson,
    "provenance" -> provenanceJson,
    "records" -> jrawArr(2, recordJson))) + "\n"


  stageMember(jsonPath, envelope)
  log(s"envelope staged           : $jsonPath (${envelope.length} chars)")


  // ---------------------------- the prose report ----------------------------
  val md = scala.collection.mutable.ArrayBuffer.empty[String]
  def md0(line: String): Unit = md += line

  md0(s"# Joern capability probe $QUERY_ID")
  md0("")
  md0("Bounded **call-graph** reachability over CALL edges, **parameterized over")
  md0("handler/sink pairs** and instantiated on **two** named pairs in one run, over the")
  md0("code-property graph built from the pinned tree's bytecode.")
  md0("")
  md0("This report is **observational**. It judges no finding - not real, not important,")
  md0("not a false positive, not a duplicate - and makes no comparison between tools. It")
  md0("contributes no row to `oss-scan-results/findings.json` and writes nothing into")
  md0("`harness/artifacts/raw/`.")
  md0("")
  md0(s"The slug `$QUERY_ID` is the **identifier** the plan assigns this query, and the")
  md0(s"slugs `$SIBLING_CALLGRAPH_QUERY` and")
  md0(s"`$SIBLING_DATAFLOW_QUERY` are likewise identifiers assigned to the two")
  md0("sibling queries. A slug names the question a query was written to ask. It is not a")
  md0("finding, and nothing in this report should be read as an assessment of Spark, of")
  md0("any Spark component or of any Spark configuration.")
  md0("")
  md0("| | |")
  md0("| --- | --- |")
  md0(s"| Query source | `$sourceRepoRelative` |")
  md0(s"| Query source sha256 | `$sourceSha256` |")
  md0(s"| Query source byte size | $sourceByteSize |")
  md0(s"| Publication id | `$publicationId` |")
  md0(s"| Envelope | `$RESULTS_DIR/$QUERY_ID.json` |")
  md0(s"| Console log | `$LOG_DIR/probe-$QUERY_ID.log` |")
  md0(s"| Loader | `importCpg` into a switched workspace (`$WORKSPACE_PATH`) |")
  md0(s"| JDK major | $jdkMajor |")
  md0(s"| Heap actually used | $heapMaxBytes bytes (floor $HEAP_FLOOR_BYTES) |")
  md0(s"| Graph | $sizeFollow bytes, sha256 `${mdSafe(shaObserved)}` |")
  md0(s"| Graph identity re-verified before the load | yes, against " +
    s"`$recordOfAccountLabel` |")
  md0("| Bytes actually imported | a private copy this run made, digested in the copy " +
    "pass, verified against that record, and re-verified by digest and inode after the " +
    "load; both pairs were traversed over that one load |")
  md0(s"| Graph methods / typeDecls / files | $methodCount / $typeDeclCount / $fileCount |")
  md0("| Compile status | compiled |")
  md0("| Run status | completed |")
  md0(s"| Pairs declared / invoked | ${PAIRS.size} / ${traversals.count(_.invoked)} |")
  md0(s"| Pair iteration order | ${PAIRS.map(p => s"`${p.id}`").mkString(", ")} |")
  md0(s"| Records returned | $returnedRecordCount (${boundaries.size} boundary " +
    "measurement(s) plus per-pair route records) |")
  md0(s"| Parameterizability | **$parameterizabilityVerdict** |")
  md0(s"| Duplicate formulation | **$duplicateFormulationAggregate** |")
  md0("")
  md0("## Which source wrote this report")
  md0("")
  md0(s"This report was written by `$sourceRepoRelative`, whose bytes digest to")
  md0(s"`$sourceSha256` ($sourceByteSize bytes). The script read its own file at run time")
  md0("and digested it, so the digest above is a measurement of the writer rather than a")
  md0("label attached to it. The same digest appears in the envelope under")
  md0("`source_integrity.query_source_sha256` and in the console log, and all three")
  md0(s"members of this publication carry the identifier `$publicationId`.")
  md0("")
  md0("That is what makes the relationship between a source and its results checkable")
  md0("rather than assumed: digest the `.sc` file and compare. A result whose digest does")
  md0("not match the source beside it was not written by that source, and no amount of")
  md0("agreement in the prose changes that. The three members are published together -")
  md0("each staged, fsynced and only then moved onto its final name - so a reader never")
  md0("sees one member from this generation beside another from a previous one.")
  md0("")
  md0("## The result, per pair")
  md0("")
  md0("**Distinct routes are reported per pair and are never summed.** There is no total")
  md0("distinct-route figure anywhere in this report, in the envelope or in the console")
  md0("log: adding one pair's routes to the other's would describe a question neither")
  md0("pair asks, and adding either to query 01's or query 02's returns would do the same")
  md0("across queries.")
  md0("")
  md0("| pair | handler | entry points (discovered / traversed / truncated) | " +
    "distinct routes | spurious | boundaries | bound reached |")
  md0("| --- | --- | --- | --- | --- | --- | --- |")
  PAIRS.indices.foreach { i =>
    val p = PAIRS(i)
    val s = selections(i)
    val t = traversals(i)
    val sp = spuriousByPair(i)
    md0(s"| `${p.id}` | `${p.handlerType}.${p.handlerMethod}` | " +
      s"${s.entryPointsDiscovered} / ${s.entryPointsTraversed} / " +
      s"${s.entryPointsTruncated} | ${t.distinctRoutes.size} | ${sp.spuriousCount} | " +
      s"${boundaryIdsByPair(p.id).size} | ${t.boundReached} |")
  }
  md0("")
  PAIRS.indices.foreach { i =>
    val p = PAIRS(i)
    val s = selections(i)
    val t = traversals(i)
    val sp = spuriousByPair(i)
    md0(s"### Pair `${p.id}` - ${p.label}")
    md0("")
    md0(s"- **handler**: `${p.handlerType}.${p.handlerMethod}`, at " +
      s"`${p.handlerSourceFile}:${p.handlerSourceLine}` at the pin")
    if (p.handlerPlanName != p.handlerType + "." + p.handlerMethod) {
      md0(s"  - the plan names this handler **${p.handlerPlanName}**, after the file it")
      md0("    lives in; the type the method is declared in, and therefore the type this")
      md0(s"    query selects on, is `${p.handlerType}`. Both names are recorded so the")
      md0("    resolution is visible rather than looking like a slip.")
    }
    if (p.handlerBaseType.nonEmpty) {
      md0(s"  - the base declaration on `${p.handlerBaseType}` is present in the graph " +
        s"(${s.baseDeclarationNames.size} node name(s)) and is **excluded** by the pair's")
      md0("    exact type selector. Recorded rather than silently dropped.")
    }
    md0(s"- **sink**: `${p.sinkSourceFile}:${p.sinkSourceLine}` at the pin, resolved to " +
      s"${s.sinkCalls.size} call site(s) on the sink host surface out of " +
      s"${s.sinkCallsScanned} call(s) named `${p.sinkCallName}` scanned " +
      s"(scan truncated: ${s.sinkScanTruncated})")
    s.sinkCalls.foreach { c =>
      md0(s"  - `${mdSafe(c.method.fullName)}` calls `${mdSafe(c.methodFullName)}` at graph line " +
        s"${lineOf(c)} (dispatch `${mdSafe(c.dispatchType)}`)")
    }
    md0(s"- **entry points selected** (${s.entryPointsDiscovered}):")
    s.entryGroups.foreach { case (fn, nodes) =>
      md0(s"  - `$fn` (${nodes.size} node(s), graph line ${lineOfMethod(nodes.head)})")
    }
    md0(s"- **which arm carries the body**: the declared body witness " +
      s"`${p.handlerBodyWitness}` appears among the synthetic arm's own call sites: " +
      s"**${s.syntheticCarriesBody}**; among the source-level arm's: " +
      s"**${s.sourceLevelCarriesBody}**. Synthetic types matched by " +
      s"`${p.handlerSyntheticTypeRegex}`: ${s.syntheticTypeNames.size}.")
    md0(s"- **message hops on its route**: " +
      p.messageHops.map(h => s"`${h.id}` (${h.direction}, ${h.sourceAnchor})").mkString(", "))
    md0(s"- **distinct routes**: ${t.distinctRoutes.size}")
    if (t.distinctRoutes.isEmpty) {
      md0("  - No route from an entry point to a sink host was returned within the stated")
      md0("    bound. That is a capability finding about what this formulation can express")
      md0("    over this graph, and it is reported as measured: the bound was not")
      md0("    loosened, removed or re-run unbounded to produce a non-empty result. The")
      md0(s"    ${boundaryIdsByPair(p.id).size} boundaries below are the measured reason.")
    } else {
      t.distinctRoutes.foreach { r =>
        md0(s"  - walk `${r.walkId}`, ${r.hops.size} hops, entry `${mdSafe(r.entryPoint)}` to " +
          s"sink host `${mdSafe(r.sinkHost)}`")
      }
    }
    md0(s"- **walks** (its own two, never combined with the other pair's):")
    t.walks.foreach { w =>
      md0(s"  - `${w.walkId}`: follows fan-out ${w.followsFanOut}, expansions " +
        s"${w.expansions} across all of its entry points with " +
        s"${w.maxExpansionsAtOneEntry} the peak at any ONE entry point, call sites " +
        s"${w.callSitesConsidered}, fan-out seen ${w.fanOutSitesEncountered}, fan-out " +
        s"not followed ${w.fanOutSitesNotFollowed}, max depth ${w.maxDepthUsed}, depth " +
        s"bound reached ${w.depthBoundReached}, per-entry expansion cap reached " +
        s"${w.entryExpansionCapReached}, pair step budget exhausted " +
        s"${w.pairStepBudgetExhausted}, route cap reached ${w.routeCapReached}, routes " +
        s"${w.routes.size}, alternate sink arrivals counted but not retained " +
        s"${w.alternateSinkArrivalsNotRetained}")
    }
    md0(s"- **route surface for its own expected-spurious basis**: " +
      p.routeSurfaceTypePrefixes.map(x => s"`$x`").mkString(", ") +
      s"; predicate call sites on it: ${sp.predicateCallSitesOnOwnSurface}")
    md0("")
  }
  md0("## Whether the bound was reached")
  md0("")
  md0(s"The primary bound is `MAX_CALL_DEPTH` = $MAX_CALL_DEPTH call-graph hops from an")
  md0("entry point, applied per pair. Every traversal in this query carries an explicit")
  md0("named bound; none runs unbounded, and no bound is shared between the pairs, so one")
  md0("pair cannot consume the other's budget.")
  md0("")
  PAIRS.indices.foreach { i =>
    val p = PAIRS(i)
    val t = traversals(i)
    md0(s"- pair `${p.id}`: `bound_reached` = **${t.boundReached}**")
    t.walks.foreach { w =>
      val bits = List(
        if (w.depthBoundReached)
          Some(s"the frontier was still non-empty at depth $MAX_CALL_DEPTH") else None,
        if (w.entryExpansionCapReached)
          Some("the per-entry-point expansion cap was reached at some entry point")
        else None,
        if (w.pairStepBudgetExhausted)
          Some("the pair's shared step budget was exhausted") else None,
        if (w.routeCapReached) Some("the per-pair route cap was reached") else None).flatten
      md0(s"  - walk `${w.walkId}`: " +
        (if (bits.isEmpty) "no bound was reached; the walk ran to exhaustion"
         else bits.mkString("; ")) +
        s". Each figure is measured at its cap's own scope: the peak expansion count at " +
        s"any ONE entry point was ${w.maxExpansionsAtOneEntry} of " +
        s"$MAX_EXPANSIONS_PER_ENTRY, the counter being reset at each entry point, and " +
        s"the walk's total across all of its entry points was ${w.expansions}, which " +
        s"caps nothing; this walk contributed ${w.callSitesConsidered} call sites to " +
        s"the ONE step budget of $MAX_STEPS_PER_PAIR that both of the pair's walks draw " +
        s"on; routes returned ${w.routes.size} of $MAX_ROUTES_PER_PAIR.")
    }
  }
  md0("")
  md0("A depth bound reached with a non-empty frontier says only that the walk stopped")
  md0("expanding, so on its own it would leave open whether a deeper walk would reach a")
  md0("sink host. What settles that here is the boundary measurement below rather than the")
  md0("bound: the hops that break these routes are not CALL edges at all, and no increase")
  md0("in depth introduces an edge that does not exist.")
  md0("")
  md0("| bound | value |")
  md0("| --- | --- |")
  md0(s"| MAX_CALL_DEPTH | $MAX_CALL_DEPTH |")
  md0(s"| MAX_ROUTES_PER_PAIR | $MAX_ROUTES_PER_PAIR |")
  md0(s"| MAX_EXPANSIONS_PER_ENTRY | $MAX_EXPANSIONS_PER_ENTRY |")
  md0(s"| MAX_STEPS_PER_PAIR | $MAX_STEPS_PER_PAIR |")
  md0(s"| MAX_TOTAL_RETURNS | $MAX_TOTAL_RETURNS |")
  md0(s"| MAX_ENTRY_POINTS_PER_PAIR | $MAX_ENTRY_POINTS_PER_PAIR |")
  md0(s"| MAX_CALL_SCAN | $MAX_CALL_SCAN |")
  md0(s"| MAX_TYPE_SCAN | $MAX_TYPE_SCAN |")
  md0(s"| FANOUT_CALLEE_THRESHOLD | $FANOUT_CALLEE_THRESHOLD |")
  md0("")
  md0("Every bound above is published with its reached flag and its basis in the")
  md0("envelope's `bounds_reached` and `bounds_reached_basis`, and for the two caps that")
  md0("govern more than one sweep the flag is the **disjunction** over every sweep the cap")
  md0("governs, with the basis naming each sweep, its observed count and its own flag - so")
  md0("a cap reached at one site cannot be hidden by another site that stayed inside it.")
  md0("")
  md0(s"`MAX_ROUTES_PER_PAIR` can bind on this graph, per pair: " +
    PAIRS.indices.map { i =>
      val p = PAIRS(i); val t = traversals(i)
      s"`${p.id}` ${t.routeCapCanBind} (reachable maximum " +
        s"${t.routeCapReachableMaximum} against the cap of $MAX_ROUTES_PER_PAIR)"
    }.mkString(", ") + ".")
  md0("Within one walk a route is retained per (entry point, sink host) pair, so the most")
  md0("a walk can retain is bounded by that product rather than by the full number of")
  md0("method sequences that arrive. Alternate arrivals at a sink host already witnessed")
  md0("from the same entry point in the same walk are **counted rather than retained**, so")
  md0("the difference between what arrived and what was enumerated is published rather")
  md0("than invisible: " +
    PAIRS.indices.map { i =>
      val p = PAIRS(i); val t = traversals(i)
      s"`${p.id}` " + t.walks.map(w =>
        s"`${w.walkId}` ${w.alternateSinkArrivalsNotRetained}").mkString(" / ")
    }.mkString(", ") + ".")
  md0("Neither figure is summed across the pairs.")
  md0("")
  md0("### Every traversal this query materialized, and the cap that governed it")
  md0("")
  md0("| sweep | cap | value | observed | truncated |")
  md0("| --- | --- | --- | --- | --- |")
  boundedSweeps.toList.foreach { sw =>
    md0(s"| ${sw.label} | `${sw.capName}` | ${sw.cap} | ${sw.observed} | " +
      s"${sw.truncated} |")
  }
  md0("")
  md0("Every materialization **outside the walks** goes through one bounded helper that")
  md0("takes `cap + 1` elements and reports truncation when it saw more than `cap`, so a")
  md0("cap applied at one site and forgotten at the next is not expressible: a sweep")
  md0("absent from this table did not run. Sweeps whose label names a pair were taken")
  md0("once per pair against that pair's own selectors, and sweeps whose label does not")
  md0("were taken once for the whole query and are cited by both pairs - which is why")
  md0("this table is a list of measurements rather than a sum over the pairs. The")
  md0("graph-wide sweeps are the ones that matter: the shared sink name sweep, the")
  md0("predicate call-site sweep, and each message hop's constructor and accessor")
  md0("call-site sweeps - two hops across the two pairs, so four of them - all governed")
  md0("by `MAX_CALL_SCAN`; the keyed type and method lookups - each pair's entry-point")
  md0("selection, the predicate type and its methods, the message types, the thread")
  md0("hosts, the JDK-launch declaration and the route-surface prefixes - are capped")
  md0("under `MAX_TYPE_SCAN`.")
  md0("")
  md0("The walks' own expansions are a different mechanism and are named as such: they")
  md0("are node-local (one method group's call sites, one call site's linked callees) and")
  md0("are governed by `MAX_CALL_DEPTH`, `MAX_EXPANSIONS_PER_ENTRY`, `MAX_STEPS_PER_PAIR`")
  md0("and `MAX_ENTRY_POINTS_PER_PAIR`, each published above with its own reached flag and")
  md0("basis, and each applied per pair so one pair cannot consume the other's budget. A")
  md0("reader checking the boundedness claim needs to know which mechanism governs what,")
  md0("so the division is stated rather than collapsed into one sentence about a single")
  md0("helper.")
  md0("")
  md0("## The boundaries, per pair")
  md0("")
  md0("Each hop below is measured against the graph, not asserted. `crossed` states")
  md0("whether a CALL edge in fact joins the two ends. A hop on the part of the route the")
  md0("two pairs **share** is measured **once** and cited by both, which is why the")
  md0("boundary count per pair is larger than the number of measurements taken: " +
    s"${boundaryIdsByPair.values.map(_.size).sum} citations over ${boundaries.size} " +
    "measurements.")
  md0("")
  PAIRS.foreach { p =>
    val ids = boundaryIdsByPair(p.id)
    val notCrossed = ids.filter(id => !boundaryStore(id).crossedByACallEdge)
    md0(s"### Pair `${p.id}`: ${ids.size} boundaries")
    md0("")
    ids.foreach { id =>
      val b = boundaryStore(id)
      md0(s"#### `${b.id}` - crossed by a call edge: **${b.crossedByACallEdge}** " +
        s"(cited by ${citationsOf(b.id).map(x => s"`$x`").mkString(", ")})")
      md0("")
      md0(s"- **hop**: ${b.hop}")
      md0(s"- **from**: ${if (b.fromEnd.isEmpty) "(none measured)" else "`" + mdSafe(b.fromEnd) + "`"}")
      md0(s"- **to**: ${if (b.toEnd.isEmpty) "(none measured)" else "`" + mdSafe(b.toEnd) + "`"}")
      md0(s"- **reason**: ${b.reason}")
      md0(s"- **modelling**: ${b.modelling}")
      md0("")
    }
    md0("Boundaries not crossed by a call edge: " +
      (if (notCrossed.isEmpty) "none" else notCrossed.map(x => s"`$x`").mkString(", ")) +
      ".")
    md0("")
  }
  md0("### The one hop this query models rather than reports as not-connectable")
  md0("")
  md0(s"Pair `$PAIR_TWO_ID` crosses a message-send boundary at its **first** step: the")
  md0("servlet's handler does not call the Master, it **sends** a message by `askSync`")
  md0(s"at `$PAIR_TWO_HANDLER_SOURCE_FILE:276-277` at the pin, and that is the very")
  md0("message pair one's handler receives at")
  md0(s"`$PAIR_ONE_HANDLER_SOURCE_FILE:410`. A call graph does not join a send to its")
  md0("receiving handler, so this query **models the hop explicitly by pairing on the")
  md0(s"message type** `$MESSAGE_TYPE_REQUEST_SUBMIT_DRIVER`,")
  md0("and the graph evidence for the model is measured rather than asserted: the")
  md0("constructor call sites of that type are the producer end, its declared field")
  md0("accessors' call sites are the consumer end, and the message type's and companion's")
  md0("own generated machinery is excluded by owning type. The measured ends, and whether")
  md0(s"a CALL edge joins them, are in the `B-rpc-$MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_ID`")
  md0("record above and in the envelope.")
  md0("")
  val rsdBoundary = boundaryStore.get("B-rpc-" + MESSAGE_HOP_REQUEST_SUBMIT_DRIVER_ID)
  rsdBoundary.foreach { b =>
    /** The JSON array this measurement was recorded as, rendered for prose: the
     *  figure is the same measurement, quoted once here and once in the envelope. */
    def pairListOf(key: String): String = {
      val raw = b.measured.toMap.getOrElse(key, "[]")
      val inner = raw.stripPrefix("[").stripSuffix("]").replace("\"", "").trim
      if (inner.isEmpty) "no pair" else inner.split(",").map(x => s"`${x.trim}`").mkString(", ")
    }
    md0("What the model buys, measured: the producer end of that hop is the declared entry")
    md0(s"point of ${pairListOf("producer_end_is_the_declared_entry_point_of")} and its")
    md0(s"consumer end is the declared entry point of " +
      s"${pairListOf("consumer_end_is_the_declared_entry_point_of")}. Pairing on the")
    md0("message type is")
    md0("therefore what joins one pair's handler to the other's entry point, and it is the")
    md0("reason this pair is reported as crossing one boundary more than pair one rather")
    md0("than as a route that cannot be expressed at all.")
    md0("")
  }
  md0("### The partial-function boundary, and why it is measured per pair")
  md0("")
  md0("A Scala handler that returns `PartialFunction[Any, Unit]` compiles its case bodies")
  md0("into a synthetic class, so for that shape the graph's entry point is the synthetic")
  md0(s"`$HANDLER_SYNTHETIC_METHOD` and **not** a method of the handler's own name. An")
  md0("ordinary method has no such class at all. The parameterized selector therefore")
  md0("takes the **union** of both arms and then **measures** which one carries the")
  md0("declared body witness. The two pairs answer it differently, and that difference is")
  md0("a capability observation in its own right:")
  md0("")
  PAIRS.indices.foreach { i =>
    val p = PAIRS(i)
    val s = selections(i)
    md0(s"- pair `${p.id}`: synthetic types matched ${s.syntheticTypeNames.size}, " +
      s"body witness in the synthetic arm ${s.syntheticCarriesBody}, in the source-level " +
      s"arm ${s.sourceLevelCarriesBody}")
  }
  md0("")
  md0("The `crossed_by_a_call_edge` flag on that boundary is read strictly: it is true only")
  md0("where a CALL edge joins the **source-level** method to the body. For a")
  md0("partial-function handler it is false, which is exactly what the boundary names; for")
  md0("a handler with no synthetic class the hop does not arise at all, and")
  md0("`hop_arises_for_this_handler` in the record distinguishes those two cases so the")
  md0("flag is never read as if the same hop had been crossed.")
  md0("")
  md0("A selector that took only one arm would silently miss one of the two pairs - which")
  md0("is exactly the kind of detail a parameterized query has to get right to generalise")
  md0("past the pair it was written against.")
  md0("")
  md0("## The predicate set, and the source types it came from")
  md0("")
  md0("The mechanical definition: a route is spurious **only** where it passes an")
  md0("authorization or ACL predicate before reaching the sink. The predicate set is")
  md0("exactly these five Boolean methods, and their source is")
  md0("`core/src/main/scala/org/apache/spark/SecurityManager.scala` at the pin")
  md0(s"(457 lines), on the single source type `$PREDICATE_TYPE`:")
  md0("")
  md0("| predicate | source line at the pin |")
  md0("| --- | --- |")
  md0("| `aclsEnabled()` | 227 |")
  md0("| `checkAdminPermissions` | 234 |")
  md0("| `checkUIViewPermissions` | 248 |")
  md0("| `checkModifyPermissions` | 264 |")
  md0("| `isAuthenticationEnabled()` | 274 |")
  md0("")
  md0(s"`${PAIR_ONE_HANDLER_SOURCE_FILE}:411`'s `if (state != RecoveryState.ALIVE)` is a")
  md0("**recovery-state** check and is deliberately not in this set.")
  md0("")
  md0("Whether the four selector constants in this query's source carry the same literal")
  md0(s"text as those of `queries/joern/$SIBLING_CALLGRAPH_QUERY.sc` and")
  md0(s"`queries/joern/$SIBLING_DATAFLOW_QUERY.sc` is **measured** at run time rather")
  md0("than asserted here, by reading each sibling source and comparing literal to")
  md0("literal; the outcome is published per sibling as `predicate_selector_literals`")
  md0("`_identical`. It matters because three spurious counts are only comparable while")
  md0("the definition of the term is the same text in all three files.")
  md0("")
  md0("### How the bytecode-level selector was constrained")
  md0("")
  md0(s"The anchored selector is `$PREDICATE_NAME_REGEX`, paired with a type selector on")
  md0(s"`$PREDICATE_TYPE`. On **bytecode** that is not enough. `SecurityManager.scala:59`")
  md0("declares `private var aclsOn`, and Scala compiles a private var into accessors, so")
  md0("the graph carries both a getter and a setter whose names satisfy the `acls.*`")
  md0("alternative. The narrowing is therefore three steps, and all three sets are")
  md0("reported so it is auditable rather than asserted:")
  md0("")
  md0(s"1. broad anchored selector on the ${predicateTypeMethods.size} method nodes " +
    s"(${predicateTypeMethodNames.size} distinct names) of that type: " +
    predicateBroadNames.map(n => s"`${mdSafe(n)}`").mkString(", "))
  md0(s"2. minus every name ending in `$PREDICATE_SETTER_SUFFIX`, which drops " +
    (if (predicateSetterExcludedNames.isEmpty) "nothing"
     else predicateSetterExcludedNames.map(n => s"`${mdSafe(n)}`").mkString(", ")) +
    ", leaving " + predicateAfterSetterNames.map(n => s"`${mdSafe(n)}`").mkString(", "))
  md0("3. intersected with the five named source-level predicates, which drops " +
    (if (predicateNonPredicateResidue.isEmpty) "nothing"
     else predicateNonPredicateResidue.map(n => s"`${mdSafe(n)}`").mkString(", ") +
       " - a private-var getter, not one of the five") +
    ", leaving exactly " + predicateFinalNames.map(n => s"`${mdSafe(n)}`").mkString(", "))
  md0("")
  md0("The final set is asserted against the graph, not against the source.")
  md0("")
  md0("### The query-wide route surface, and each pair's own")
  md0("")
  md0("The byte-identical predicate block carries the **four predicate constants and")
  md0("nothing else**. The route surface is not part of it: the predicate constants define")
  md0("the term *spurious* and are compared across the three sources under the names the")
  md0("formulation identity block declares, whereas a route surface is a property of one")
  md0("query's own route - and this query's route has **two** entry-point owners where its")
  md0("siblings have one. `ROUTE_SURFACE_TYPE_PREFIXES` is therefore **derived** from this")
  md0("query's own route ends across both pairs. Five of the six entries are expressed in")
  md0("terms of the selector constants they come from rather than restated as literals;")
  md0("the sixth, the REST server that mounts pair two's handler servlet, is named by no")
  md0("existing selector and is therefore declared once as its own constant:")
  md0("")
  ROUTE_SURFACE_TYPE_ROLES.foreach { case (pref, role) =>
    md0(s"- `$pref` - $role")
  }
  md0("")
  md0("Measured here rather than assumed: " +
    (if (pairsNotCoveredBySharedSurface.isEmpty)
      "every pair's handler type is covered by one of those prefixes."
     else "the handler type(s) " +
       pairsNotCoveredBySharedSurface.map(t => s"`$t`").mkString(", ") +
       " are **not** covered by any of those prefixes, because the type a method is " +
       "declared in is not always the headline class of the file it lives in."))
  md0(s"Stage I additionally asserts the derivation against what was actually measured: " +
    s"all ${measuredRouteEnds.size} measured route end(s) across ${PAIRS.size} pair(s) -")
  md0("every pair's entry points, the consumer end of each pair's RPC hop and every sink")
  md0("host - are owned by a prefix on the derived surface, and a route end that was not")
  md0("would have halted the run rather than being published beside a surface the route")
  md0("leaves. Which ends each prefix accounts for is published per prefix as")
  md0("`route_surface_reach_evidence`, so a prefix that earns its place from the")
  md0("derivation but accounts for no measured end is visible as a zero. Measured here: " +
    (if (routeSurfacePrefixesAccountingForNoEnd.isEmpty)
       "every prefix on the surface accounts for at least one measured route end."
     else routeSurfacePrefixesAccountingForNoEnd.map(x => s"`$x`").mkString(", ") +
       " account(s) for no measured route end - it is on the surface because it is a " +
       "type the route reaches before the handler rather than an end of the route, and " +
       "the zero is published rather than hidden inside a covered surface."))
  md0("")
  md0("Each pair additionally carries its **own** route surface, derived from its own")
  md0("handler, the intermediate hop and its sink types, which is what makes that pair's")
  md0("expected-spurious basis correct; the query-wide surface is what makes the two")
  md0("pairs' bases commensurable. Both counts are published and neither is summed.")
  md0("")
  md0("### The intermediate route hop")
  md0("")
  md0("Both pairs' routes run handler -> RPC -> **Worker** -> DriverRunner -> launch, so")
  md0("the Worker is a *hop* of the route rather than an end of it, and it is on each")
  md0("pair's own route surface for that reason. A surface naming only the handler and the")
  md0("sink host would leave one hop of the route unsearched while the resulting statement")
  md0("read as one about the whole route. Its anchors at the pin:")
  md0("")
  ROUTE_HOP_WORKER_ANCHORS.foreach(a => md0(s"- `$a`"))
  md0("")
  md0(s"`$ROUTE_HOP_WORKER_SOURCE_FILE` is " +
    s"$ROUTE_HOP_WORKER_SOURCE_FILE_LINES lines at the pin, and it is one of the " +
    s"${predicateRouteFilesSearched.size} route files searched for the five names.")
  md0("")
  md0("Every prefix on either pair's surface reports the **same** fields, the intermediate")
  md0("hop included. The reach columns are what make a zero falsifiable: a zero over a")
  md0("surface the graph does not carry would read exactly like a searched surface that")
  md0("came back clean, so a prefix with no type declaration stops the run instead of")
  md0("contributing one.")
  md0("")
  md0("| surface prefix | type decls | methods | predicate call sites | on pairs |")
  md0("| --- | --- | --- | --- | --- |")
  surfacePrefixEvidence.foreach { e =>
    md0(s"| `${e.prefix}` | ${e.typeDeclsInTheGraph} | ${e.methodsInTheGraph} | " +
      s"${e.predicateCallSites} | " +
      (if (e.onPairs.isEmpty) "shared list only" else e.onPairs.mkString(", ")) + " |")
  }
  md0("")
  md0(s"The sweep behind those reach columns is bounded by `MAX_TYPE_SCAN` = " +
    s"$MAX_TYPE_SCAN type declarations per prefix and reported truncated = " +
    s"**$surfaceTypeScanTruncated** for this sweep specifically; the cap governs " +
    "several other keyed lookups as well, and the envelope's `bounds_reached` entry " +
    "for it is the disjunction over all of them." +
    (if (surfacePrefixesPresentWithNoMethods.isEmpty) ""
     else " Present in the graph but carrying no methods: " +
       surfacePrefixesPresentWithNoMethods.map(x => s"`$x`").mkString(", ") + "."))
  md0("")
  md0("## Whether an expected-spurious route was absent")
  md0("")
  PAIRS.indices.foreach { i =>
    val p = PAIRS(i)
    val sp = spuriousByPair(i)
    md0(s"### Pair `${p.id}`: `spurious_count` = **${sp.spuriousCount}**")
    md0("")
    md0("No route in the emitted set passed an auth/ACL predicate as defined by these five")
    md0("named selectors.")
    md0("")
    if (sp.absenceIsStructural) {
      md0("**The absence is structural, not a consequence of the query filtering well.**")
      md0(s"Measured against the graph: ${predicateCallSites.size} call sites of the five")
      md0(s"predicates exist graph-wide, in ${predicateCallerNames.size} distinct calling")
      md0(s"methods, and **${sp.predicateCallSitesOnOwnSurface}** of them sit on this")
      md0("pair's own route surface (" +
        p.routeSurfaceTypePrefixes.map(x => s"`$x`").mkString(", ") + ").")
      md0("The predicate set exists and is invoked elsewhere in the program; it is not")
      md0("invoked anywhere on this pair's route, so no route of this pair could have")
      md0("passed one.")
    } else {
      md0(s"Call sites of the five predicates DO exist on this pair's own route surface " +
        s"(${sp.predicateCallSitesOnOwnSurface} of ${predicateCallSites.size} graph-wide),")
      md0("so the count above reflects the query's filtering rather than a structural")
      md0("absence.")
    }
    md0("")
  }
  md0("For pair `" + PAIR_TWO_ID + "` one further selector measurement is worth recording,")
  md0("because it is what makes that pair's basis structural: in the pinned tree the file")
  md0(s"`$PAIR_TWO_HANDLER_SOURCE_FILE` is matched by **none** of the")
  md0("five named selectors and carries no reference to the predicate type at all. Its")
  md0("only `permission` occurrence is the Apache licence boilerplate at line 14, and a")
  md0("case-insensitive search additionally returns lines 209, 233 and 251, which are")
  md0("false positives - the matched literal is `aCl` inside `extraClassPath` /")
  md0("`driverExtraClassPath`. That is a statement about which of these selectors match")
  md0("that file, and nothing more.")
  md0("")
  md0("### What this section does not say")
  md0("")
  md0("These are statements about **this query's own output** under **this query's own**")
  md0("mechanical definition of one word. They are not an assessment of Spark, of any")
  md0("Spark component, of any Spark configuration or of any submission path, and nothing")
  md0("here should be read as one. In particular, nothing in this report states or implies")
  md0("anything about how Spark authorizes any operation: the five selectors above are a")
  md0("query-side definition used to classify this query's own returns, and where a route")
  md0("count is zero there are no returns to classify at all.")
  md0("")
  md0("## Whether this formulation duplicates another query's")
  md0("")
  md0(s"`duplicate_formulation` = **$duplicateFormulationAggregate**. " +
    duplicateFormulationSummary)
  md0("")
  md0(s"Aggregation: ${mdSafe(duplicateFormulationAggregation)}")
  md0("")
  md0(s"Relation: $duplicateFormulationRelation")
  md0("")
  md0("The comparison reads each sibling's **source**, not its published result. Every")
  md0("entry below is produced by applying one shared predicate to the two queries' own")
  md0("declared formulation identity blocks, so both directions of the relation evaluate")
  md0("identical inputs through identical code and a disagreement between them is not")
  md0("expressible. No sibling figure is transcribed into this file, which is what makes")
  md0("the verdict incapable of drifting from the file it describes.")
  md0("")
  relations.foreach { r =>
    md0(s"### Against `${mdSafe(r.theirs.queryId)}`: **${mdSafe(r.status)}**")
    md0("")
    md0(s"- scope: ${r.scope}")
    md0(s"- basis: ${r.basis}")
    md0(s"- sibling source: `${r.theirs.sourceRepoRelative}`, sha256 " +
      (if (r.theirs.established) s"`${r.theirs.sourceSha256}`, " +
        s"${r.theirs.sourceByteSize} bytes"
       else "not read") + s"; read note: ${r.theirs.note}")
    if (r.theirs.established) {
      md0(s"- pair ids here: ${myFormulation.pairIds.mkString(", ")}; there: " +
        s"${r.theirs.pairIds.mkString(", ")}; shared: " +
        (if (r.sharedPairs.isEmpty) "none" else mdSafe(r.sharedPairs.mkString(", "))))
      md0(s"- edge kinds: here ${myFormulation.edgeKinds.mkString(", ")}, there " +
        s"${r.theirs.edgeKinds.mkString(", ")} (same = ${r.sameEdgeKinds})")
      md0(s"- end node kinds: here ${mdSafe(myFormulation.endNodeKinds.mkString(", "))}, there " +
        s"${mdSafe(r.theirs.endNodeKinds.mkString(", "))} (same = ${r.sameEndNodeKinds})")
      md0(s"- bound: here `${myFormulation.boundName}` = ${myFormulation.boundValue} " +
        s"(${myFormulation.boundKind}); there `${r.theirs.boundName}` = " +
        s"${r.theirs.boundValue} (${r.theirs.boundKind}); same kind = " +
        s"${r.sameBoundKind}, same value = ${r.sameBoundValue}")
      md0(s"- entry selector literals identical: ${r.sameEntrySelectors}; sink selector " +
        s"literals identical: ${r.sameSinkSelectors}")
      md0(s"- predicate selector literals identical: ${r.samePredicateSelectors} " +
        "(reported, not a component of the formulation predicate: the predicate set " +
        "defines the word \"spurious\" rather than the traversal)")
      md0(s"- API constructs: ${r.apiShared.size} shared, ${r.apiOnlyHere.size} only " +
        s"here, ${r.apiOnlyThere.size} only there")
    }
    md0("")
  }
  md0(s"$FORMULATION_SELECTOR_SCOPE_LIMITATION")
  md0("")
  md0("Pair one's figures **measured here**, published so a reader can compare them")
  md0("against query 01's own published figures rather than against a copy of them made")
  md0(s"inside this file: ${pairOneSelection.entryGroups.size} entry point(s), " +
    s"${pairOneTraversal.distinctRoutes.size} distinct route(s), bound value " +
    s"$MAX_CALL_DEPTH, boundaries not crossed " +
    (if (pairOneNotCrossedHere.isEmpty) "none"
     else pairOneNotCrossedHere.map(b => s"`$b`").mkString(", ")) +
    " in query 01's numbering, under the naming map " +
    BOUNDARY_ID_TO_SIBLING_01.toList.sorted
      .map { case (mine, theirs) => s"`$mine` -> `$theirs`" }.mkString(", ") + ".")
  md0("A cross-run comparison of *counts* is a reader's to make from the two result")
  md0("files: each query measured its own against the graph of its own run.")
  md0("")
  md0("## The three effort measures")
  md0("")
  md0(s"1. **Query revisions committed: " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established") +
    ".** Convention: " + QUERY_REVISIONS_CONVENTION + ".")
  md0(s"   Measurement: ${mdSafe(revisionsNote)}.")
  md0(s"   Window: the history of HEAD " +
    revisionHead.map(h => s"`$h`").getOrElse("(not established)") +
    revisionBranch.map(b => s", on branch `$b`").getOrElse("") +
    ", named explicitly rather than defaulted, with every commit counted verified an")
  md0("   ancestor of that HEAD.")
  if (revisionsEstablished) {
    md0("   The commits counted, newest first, so the number is auditable rather than")
    md0("   asserted:")
    md0("")
    revisionCommits.foreach(c => md0(s"   - `${mdSafe(c)}`"))
    md0("")
  } else {
    md0("   The count is published as `null` rather than as a number, because a measure")
    md0("   this run could not establish is not a measure it may assert.")
    md0("")
  }
  md0(s"2. **Distinct Joern API constructs used: " +
    s"${JOERN_API_CONSTRUCTS.distinct.size}.** Listed explicitly and deduplicated so the")
  md0("   count is auditable from the list rather than asserted; every entry appears")
  md0("   literally in the query source:")
  md0("")
  JOERN_API_CONSTRUCTS.foreach(c => md0(s"   - `$c`"))
  md0("")
  relations.foreach { r =>
    md0(s"   Constructs declared here that `${mdSafe(r.theirs.queryId)}` does not declare: " +
      (if (!r.theirs.established) "not established - " + r.theirs.note
       else if (r.apiOnlyHere.isEmpty) "none"
       else r.apiOnlyHere.map(c => s"`$c`").mkString(", ")) + ".")
  }
  md0("")
  md0(s"3. **Parameterizability: $parameterizabilityVerdict.** This file owns the measure.")
  md0(s"   It $PARAMETERIZABILITY_PASS_CONDITION.")
  md0("")
  md0(s"   - first pair `${PAIRS.head.id}`: $firstPairOutcome")
  md0(s"   - second pair `${secondPair.id}` " +
    s"(`${secondPair.handlerType}.${secondPair.handlerMethod}` at " +
    s"`${secondPair.handlerSourceFile}:${secondPair.handlerSourceLine}` to the launch at " +
    s"`${secondPair.sinkSourceFile}:${secondPair.sinkSourceLine}`, both at the pin): " +
    secondPairOutcome)
  md0("")
  if (parameterizabilityPassed) {
    md0("   The verdict rests on the second pair's invocation having actually run in this")
    md0("   same run, and on its result being captured here, in the envelope and in the")
    md0("   console log - not on the existence of a parameter. Its selection, its walk")
    md0("   counters, its boundary measurements, its distinct-route count and its spurious")
    md0("   count are all published above. An empty result from a real invocation")
    md0("   satisfies the measure; a skipped invocation would not, and a malformed pair")
    md0("   aborts the run rather than being passed over.")
  } else {
    md0("   The verdict is **not passed**, and it is reported as such rather than claimed:")
    md0("   the second pair's invocation did not complete in this run. Nothing above should")
    md0("   be read as evidence for the measure.")
  }
  md0("")
  md0("   The handler surface this parameterization draws on is ample rather than exactly")
  md0("   the two cases it was written against, and that is measured in the pinned tree:")
  md0("   **eight** `receive`/`receiveAndReply` declarations across **five** files under")
  md0("   `core/src/main/scala/org/apache/spark/deploy` - `Client.scala:207`,")
  md0("   `client/StandaloneAppClient.scala:161` and `:209`, `master/Master.scala:239` and")
  md0("   `:409`, `worker/Worker.scala:523` and `:736`, `worker/WorkerWatcher.scala:66` -")
  md0("   alongside the one shared sink both pairs here use.")
  md0("")
  md0("## Modelling decisions, stated so the counts stay interpretable")
  md0("")
  md0("- **Nothing is summed across pairs.** Routes, spurious counts, bound flags and")
  md0("  entry-point counters are per pair and keyed by pair id in the envelope. The one")
  md0("  overall figure, `returned_record_count`, is a count of records emitted, and the")
  md0("  one overall flag, `bound_reached_any`, is a disjunction - neither is a total over")
  md0("  routes.")
  md0("- **A hop the two pairs share is one measurement.** The thread hop and the")
  md0("  interface hop at the sink are measured once and cited by both pairs, with")
  md0("  `cited_by_pairs` on the record naming who cites it. A count appearing in two")
  md0("  places here is one measurement cited twice, never two measurements.")
  md0("- **Operator pseudo-calls are excluded.** A CPG `<operator>.*` call is an artefact")
  md0("  of the representation, not a method call, and expanding them would inflate every")
  md0("  counter without adding a call-graph hop.")
  md0("- **Duplicate class definitions are unioned.** The graph carries more than one node")
  md0("  per class where two staged archives carried the same class, so method nodes are")
  md0("  grouped by full name and their call sites unioned rather than one node being")
  md0("  picked. Reachability is keyed on the method full name.")
  md0("- **Callee resolution is explicit.** Each call site's callees are taken from")
  md0("  `NoResolve.getCalledMethodsAsTraversal`, which is exactly the statically linked")
  md0("  CALL-edge callees of that site.")
  md0("- **Two walks per pair, reported side by side.** Walk `" + WALK_A_ID + "` expands")
  md0("  every call site. Walk `" + WALK_B_ID + "` records but does not expand a call site")
  md0(s"  whose resolved callee set exceeds $FANOUT_CALLEE_THRESHOLD distinct methods:")
  md0("  expanding such a site models \"any implementation in the program may be invoked")
  md0("  here\", which is a property of the call linker rather than of either route. Both")
  md0("  walks' counters are published per pair and their routes are deduplicated within")
  md0("  the pair, never summed.")
  md0("- **Graph line numbers are the graph's own.** A method or call node's `lineNumber`")
  md0("  comes from the bytecode line-number table and can differ by a line from the `def`")
  md0("  or statement line cited from the source. Source anchors in this report are quoted")
  md0("  from the pinned tree; graph lines are labelled as such.")
  md0("- **A bytecode file path is not a source path.** The frontend records an extraction")
  md0("  path under a temporary directory for every class, so this query reports types,")
  md0("  methods and lines rather than presenting that path as a source location.")
  md0("")
  md0("## The graph this query loaded, and its identity")
  md0("")
  md0(s"- named path `${mdSafe(cpgNamedLabel)}` (repository-relative `${mdSafe(cpgNamedRepoRelativeLabel)}`)" +
    (if (cpgIsLink) ", a symlink" else ""))
  md0(s"- resolved target: ${mdSafe(cpgResolvedLabel)}, **$sizeFollow** bytes, sha256 `${mdSafe(shaObserved)}`")
  md0(s"- the link itself measures $sizeNoFollow bytes; that figure is recorded only to be")
  md0("  discarded, because measuring the link rather than its target is the mistake this")
  md0("  check exists to avoid")
  md0("- no absolute host path appears in this report or in the envelope: the clone root is")
  md0("  a property of the checkout rather than of the measurement, and the size-and-digest")
  md0(s"  pair above is what the identity comparison turns on. The literals are in")
  md0(s"  `$LOG_DIR/probe-$QUERY_ID.log`, a console stream not held to byte-identity")
  md0(s"- record of account: `$recordOfAccountLabel`, which states bytes $recordedSize")
  md0(s"  and sha256 `$recordedSha` - re-verified immediately before the load, and a")
  md0("  mismatch would have halted the run")
  md0(s"- its provenance: ${recordOfAccount.provenance}")
  md0("- it is resolved by **provenance**, not named by a constant: a write-time record")
  md0(s"  left by this checkout's own frontend (`$CPG_FRONTEND_RECORD_PATH`) governs where")
  md0("  it states exactly one identity pair, and otherwise the provisioning record")
  md0("  beside the resolved graph does. Every candidate that exists is read; more than")
  md0("  one distinct pair inside a single record, and any disagreement between two")
  md0("  records, each halt the run rather than being adjudicated silently")
  md0("- corroborated by " +
    (if (recordCorroborators.isEmpty)
       "no other record: only one candidate carried an identity pair, and that is " +
         "published as the fact it is rather than presented as agreement"
     else recordCorroborators.map(c => s"`$c`").mkString(", ") +
       ", which agree with it pair for pair"))
  md0("- there is **no environment override** for that resolution. This query reads no")
  md0("  variable that could point the comparison at a different record, so the record a")
  md0("  reader can read is exactly the record the comparison turned on. Where the")
  md0("  host's graph and this record disagree, the run halts and the disagreement is")
  md0("  reported rather than routed around")
  md0("- the bytes actually imported were a **private copy** this run made under")
  md0(s"  `$$$SCRATCH_ROOT_ENV_VAR`, digested in the copy pass, verified against that")
  md0("  record, read-only for the load, re-verified by size, digest and inode after the")
  md0("  load, and then removed. Both pairs were traversed over that one load")
  md0(s"- the AAP-named path `$CPG_PATH_DEFAULT`: $aapNameReconciliation")
  md0("")
  md0("## Reproducing this")
  md0("")
  md0(s"Precondition: $REPRODUCTION_COMMAND_PRECONDITION.")
  md0("")
  val reproductionFence = mdFence(REPRODUCTION_COMMAND)
  md0(reproductionFence)
  md0(REPRODUCTION_COMMAND)
  md0(reproductionFence)
  md0("")
  md0("That is the **whole** command: the repository root, the JDK, the heap override,")
  md0("the log level and the script path. Both pairs are declared as named constants in")
  md0("the query source and both are invoked by that one command, so no per-pair")
  md0("parameter has to be passed on the command line and the second pair's invocation")
  md0("is reproducible from this record alone. This query reads no other environment")
  md0("variable that changes what it loads or what it publishes, and in particular there")
  md0("is no variable that selects the identity record. The record of account is")
  md0("resolved by provenance - this checkout's own frontend log where it carries a")
  md0("write-time `bytes:`/`sha256:` pair, and otherwise the provisioning record beside")
  md0("the resolved graph - and both are reached through values this command names. For")
  md0(s"this run it was `$recordOfAccountLabel`.")
  md0("")
  md0("`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so")
  md0("`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs")
  md0("at. The query measures the heap it received and stops below the floor: raising a")
  md0("heap is permitted and reported, lowering one is not.")
  md0("")

  // Trailing blank entries are dropped before joining: a report ending on an
  // empty line produced a blank line before the final newline, which
  // `git diff --check` reports as whitespace at EOF. The content is
  // unchanged; only the trailing padding goes.
  val reportLines = md.toList.reverse.dropWhile(_.trim.isEmpty).reverse
  val reportMember = stageMember(mdPath, reportLines.mkString("", "\n", "\n"))
  // reportLines.size, not md.size: the trailing blank entries are dropped above,
  // so the buffer's length overstates the file by one line for every padding
  // entry removed. A logged line count that does not match the file it describes
  // is a figure a reader cannot check against anything.
  log(s"prose report staged       : $mdPath (${reportLines.size} lines written, " +
    s"${md.size - reportLines.size} trailing blank entr(y/ies) dropped, " +
    s"${reportMember.byteSize} bytes, sha256 ${reportMember.sha256})")
  log(s"publication members staged: ${stagedMembers.size} of 3 " +
    "(the console log is staged last, because it names the other two)")

  // -------------------------------------------------------------------------
  stage("N-result: the result region, emitted only now that every stage passed")
  // -------------------------------------------------------------------------
  log(s"total elapsed_ms          : ${elapsedMs(runStartNanos)}")
  logMarker(MARKER_RESULT_BEGIN)
  log(s"query_id                  : $QUERY_ID")
  log(s"compile_status            : compiled")
  log(s"run_status                : completed")
  log(s"pairs_declared            : ${PAIRS.size}")
  log(s"pairs_invoked             : ${traversals.count(_.invoked)}")
  log(s"pair_iteration_order      : ${PAIRS.map(_.id).mkString(", ")}")
  log(s"returned_record_count     : $returnedRecordCount")
  PAIRS.indices.foreach { i =>
    val p = PAIRS(i)
    val s = selections(i)
    val t = traversals(i)
    val sp = spuriousByPair(i)
    log(s"pair ${p.id}: distinct_routes=${t.distinctRoutes.size} " +
      s"spurious_count=${sp.spuriousCount} " +
      s"expected_spurious_absent=${sp.expectedSpuriousAbsent} " +
      s"(${if (sp.absenceIsStructural) "structural" else "filtering"}) " +
      s"bound_reached=${t.boundReached} " +
      s"entry_points_traversed=${s.entryPointsTraversed} " +
      s"entry_points_truncated=${s.entryPointsTruncated} " +
      s"boundaries=${boundaryIdsByPair(p.id).size}")
  }
  log("distinct_routes           : reported per pair above; never summed across pairs " +
    s"and never added to $SIBLING_CALLGRAPH_QUERY's or $SIBLING_DATAFLOW_QUERY's returns")
  log(s"bound_value               : $MAX_CALL_DEPTH")
  log(s"bound_reached_any         : ${traversals.exists(_.boundReached)}")
  log(s"duplicate_formulation     : $duplicateFormulationAggregate")
  relations.foreach { r =>
    log(s"  vs ${r.theirs.queryId}: ${r.status} (scope: ${r.scope})")
  }
  log(s"joern_api_constructs      : ${JOERN_API_CONSTRUCTS.distinct.size}")
  log(s"query_revisions_committed : " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established") +
    s" ($revisionsNote)")
  log(s"parameterizability        : $parameterizabilityVerdict " +
    s"(second pair ${secondPair.id} invoked: " +
    s"${secondTraversal.invoked && secondPairWalksRan})")
  log(s"graph                     : $sizeFollow bytes sha256=$shaObserved")
  log(s"graph_identity_record     : $recordOfAccountLabel " +
    "(resolved by provenance; no override exists)")
  log(s"query_source_sha256       : $sourceSha256")
  log(s"publication_id            : $publicationId")
  log(s"envelope                  : $jsonPath")
  log(s"prose report              : $mdPath")
  logMarker(MARKER_RESULT_END)
  logMarker(MARKER_OK)

  // The publication, all three members at once. The two staged members are on
  // disk and fsynced; the console log is staged now, as the last member, because
  // its content names the other two - and only then is anything moved onto a
  // published path. A failure anywhere above this line leaves all three targets
  // holding their previous generation rather than a mixed one.
  logTargetPath.foreach { p =>
    // The marker protocol is validated on the EXACT lines about to be written,
    // immediately before they are staged. Every line of text this query did not
    // author has already been control-character escaped and marker-prefix
    // neutralised by sanitizeForLog, so this establishes that the stream a
    // consumer will parse carries this query's own markers, once each and in
    // order, and no others.
    val markerCounts = validateMarkerProtocol(consoleLines.toList, expectedOk = true)
    println("marker protocol           : " +
      markerCounts.map { case (t, n) => s"$t=$n" }.mkString(" ") + " (validated)")
    stageMember(p, consoleLines.mkString("", "\n", "\n"))
    val published = publishStagedMembers()
    publicationCompleted = true
    println(s"publication $publicationId published ${published.size} member(s):")
    published.foreach(m =>
      println(s"  ${m.target} (${m.byteSize} bytes, sha256 ${m.sha256})"))
  }
  flushConsoleLog()

} catch {
  case t: Throwable =>
    // No result region is emitted: a partial one looks like a completed run.
    logMarker(MARKER_FAILURE)
    log(s"failing stage : $currentStage")
    log(s"exception     : ${t.getClass.getName}: ${t.getMessage}")
    System.err.println(s"$MARKER_FAILURE stage=$currentStage")
    System.err.println(s"exception: ${t.getClass.getName}: ${t.getMessage}")
    t.printStackTrace(System.err)
    flushConsoleLog()
    throw t
}
