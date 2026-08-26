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
// NAMED CONSTANTS - no inline literal governs behaviour anywhere below
// ===========================================================================

/** The slug. Both result filenames and the console log name derive from it. */
val QUERY_ID = "03-parameterized-handler-sink-pairs"

/** The probe's own scratch workspace, repo-relative as the AAP names it. */
val WORKSPACE_PATH = "queries/joern/.workspace"

/** Repo-relative output paths. Resolved against the repository root below. */
val RESULTS_DIR = "queries/joern/results"
val LOG_DIR = "harness/artifacts/logs"

/** The graph, and the record that fixes its identity. */
val CPG_ENV_VAR = "HARNESS_CPG"
val CPG_PATH_DEFAULT = "harness/cpg/spark.cpg"
val CPG_RECORD_PATH = "harness/artifacts/logs/cpg-frontend.log"

/**
 * The identity record's LOCATION may be named explicitly, and the reason is a
 * property of this pipeline rather than a convenience. The graph is built by
 * provisioning, which is re-executed from scratch on every provisioning, while
 * CPG_RECORD_PATH above is a committed deliverable describing the graph of the
 * provisioning that wrote it. Where the host carries a graph built by a LATER
 * provisioning, the committed record describes a different file, and comparing
 * against it would halt a run whose graph is perfectly sound - or, far worse,
 * would tempt someone to weaken the comparison.
 *
 * So the comparison is never weakened and never skipped. It is pointed, when
 * needed, at the frontend's own write-time record FOR THE GRAPH ACTUALLY
 * LOADED. With this variable unset the behaviour is exactly queries 01 and 02's:
 * the repo-relative record above is the record of account. With it set, that
 * record is STILL read and its pair STILL reported, the divergence is recorded
 * with both pairs and their provenance, and a mismatch against the SELECTED
 * record still halts the run (AAP 0.8.2, 0.9.2).
 */
val CPG_RECORD_ENV_VAR = "HARNESS_CPG_RECORD"

/** The repository root, and the environment variable that names it. */
val REPO_ROOT_ENV_VAR = "HARNESS_REPO_ROOT"

/** The sibling probe queries this one reports a duplicate-formulation verdict against. */
val SIBLING_CALLGRAPH_QUERY = "01-callgraph-unguarded-driver-launch"
val SIBLING_DATAFLOW_QUERY = "02-dataflow-unguarded-driver-launch"

// --------------------------------------------------------------- the bounds
/** Maximum call-graph hops walked from an entry point, per pair. */
val MAX_CALL_DEPTH = 12
/** Maximum distinct routes retained PER PAIR. Never a shared budget: one pair
 *  filling a shared budget would silently truncate the other. */
val MAX_ROUTES_PER_PAIR = 64
/** Per-entry-point step cap: method expansions, not edges. */
val MAX_EXPANSIONS_PER_ENTRY = 200000
/** Per-pair step cap across all of that pair's walks: call sites considered. */
val MAX_STEPS_PER_PAIR = 400000
/** Total-returns cap across every record kind this query emits. */
val MAX_TOTAL_RETURNS = 256
/** Maximum entry points traversed PER PAIR; the remainder are counted as truncated. */
val MAX_ENTRY_POINTS_PER_PAIR = 16
/** Cap on the indexed call-name sweeps used to find sink and message call sites. */
val MAX_CALL_SCAN = 200000
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
 * Each pair's OWN route surface, used for that pair's structural
 * expected-spurious basis. It is derived from the pair's own handler and sink
 * types rather than from the shared prefix list below, and both are reported.
 * The reason is measured rather than stylistic: the shared list names
 * org.apache.spark.deploy.rest.StandaloneRestServer, and pair two's handler
 * type - StandaloneSubmitRequestServlet, the class the method is actually
 * declared in - does not start with that prefix, so the shared list does not
 * cover pair two's handler at all. The shared list stays byte-identical for
 * comparability with queries 01 and 02; the per-pair list is what makes each
 * pair's own basis correct.
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
/** The types whose methods carry this route, used to establish the structural
 *  basis for the expected-spurious absence. Synthetic `$$anonfun$` classes of
 *  these types are included by prefix. */
val ROUTE_SURFACE_TYPE_PREFIXES = List(
  "org.apache.spark.deploy.master.Master",
  "org.apache.spark.deploy.rest.StandaloneRestServer",
  "org.apache.spark.deploy.worker.DriverRunner")
// -------------------------------------------------- end of the predicate surface
// The block above, from the `the predicate surface` banner to this comment, is
// BYTE-IDENTICAL to the corresponding block of
// queries/joern/01-callgraph-unguarded-driver-launch.sc and of
// queries/joern/02-dataflow-unguarded-driver-launch.sc. It has to be: the three
// queries' spurious counts are only comparable if the definition of "spurious"
// is the same text, and the duplicate-formulation verdict below rests on that
// comparability. A divergence here would silently invalidate all three. It is
// carried verbatim rather than adapted, which is why ROUTE_SURFACE_TYPE_PREFIXES
// keeps its three entries even though pair two's handler type is not among them:
// stage I reports the shared surface AND each pair's own, and the difference
// between the two is a reported finding rather than an edit to this block.

// ------------------------------------------------------------- effort measures
/**
 * Effort measure 1 - query revisions committed. Convention: the number of git
 * commits touching THIS .sc path, from its first appearance to the end of the
 * probe. This run introduces the file in a single commit, so the value is 1.
 * The convention is stated so the number is interpretable rather than bare.
 */
val QUERY_REVISIONS_COMMITTED = 1
val QUERY_REVISIONS_CONVENTION =
  "commits touching queries/joern/" + QUERY_ID +
    ".sc from its first appearance to the end of the probe"

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

/**
 * Query 01's published construct list, transcribed from
 * queries/joern/results/01-callgraph-unguarded-driver-launch.json so that the
 * overlap and the difference between the two queries can be COMPUTED in stage J.
 * It is evidence for the duplicate-formulation verdict, not an input to any
 * traversal, and it is labelled as transcribed rather than measured wherever it
 * is used.
 */
val SIBLING_CALLGRAPH_API_CONSTRUCTS = List(
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

/**
 * Query 02's published construct list, transcribed from
 * queries/joern/results/02-dataflow-unguarded-driver-launch.json for the same
 * purpose. The dataflow-engine members in it are the ones this query does not
 * use at all, and that difference is the auditable corroboration for the
 * verdict against 02 in stage J.
 */
val SIBLING_DATAFLOW_API_CONSTRUCTS = List(
  "AstNode.code",
  "AstNode.label",
  "AstNode.lineNumber",
  "Call.argument",
  "Call.dispatchType",
  "Call.lineNumber",
  "Call.method",
  "Call.methodFullName",
  "Call.name",
  "Call.receiver",
  "CfgNode.method",
  "EngineConfig.maxCallDepth",
  "EngineContext.config",
  "EngineContext.copy",
  "EngineContext.semantics",
  "Method.call",
  "Method.callIn",
  "Method.fullName",
  "Method.lineNumber",
  "Method.name",
  "Method.parameter",
  "Method.typeDecl",
  "MethodParameterIn.index",
  "MethodParameterIn.method",
  "MethodParameterIn.name",
  "MethodParameterIn.typeFullName",
  "Path.elements",
  "Steps.fullName",
  "Steps.fullNameExact",
  "Steps.l",
  "Steps.nameExact",
  "Steps.size",
  "Steps.take",
  "Traversal.reachableByFlows",
  "TypeDecl.fullName",
  "TypeDecl.method",
  "cpg.call",
  "cpg.file",
  "cpg.method",
  "cpg.typeDecl",
  "importCpg",
  "switchWorkspace")

/**
 * Query 01's published figures for the pair BOTH it and this query address,
 * transcribed from its envelope so that stage J can COMPARE rather than assume.
 * Transcribed, never measured here, and labelled as such wherever used.
 *
 * One caveat travels with them and is reported: query 01 measured these against
 * the graph of the provisioning that ran it, and this query measures its own
 * against the graph on this host. Structural facts - which entry points a
 * selector resolves to, which method hosts the launch, which hops are not call
 * edges - are expected to agree across both; a count that differs is reported as
 * a difference with both values rather than reconciled.
 */
val SIBLING_CALLGRAPH_ENTRY_POINTS = List(
  "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:" +
    "java.lang.Object(java.lang.Object,scala.Function1)",
  "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(" +
    "org.apache.spark.rpc.RpcCallContext)")
val SIBLING_CALLGRAPH_DISTINCT_ROUTES = 0
val SIBLING_CALLGRAPH_BOUND_VALUE = 12
val SIBLING_CALLGRAPH_BOUNDARIES_NOT_CROSSED = List(
  "B1-rpc",
  "B2-thread",
  "B4-partial-function")
/** Query 02's published figures, transcribed for the same purpose. */
val SIBLING_DATAFLOW_DISTINCT_ROUTES = 0
val SIBLING_DATAFLOW_BOUND_VALUE = 6

/**
 * What each sibling published as ITS duplicate-formulation verdict against THIS
 * query, transcribed from their envelopes. Both recorded `not_duplicate`, on the
 * ground that this query is parameterized over handler/sink pairs and covers a
 * second pair they do not address. Carried here so the cross-reference is
 * reconciled in the report rather than left for a reader to reconcile: a verdict
 * of "duplicate on pair one" from this side and "not a duplicate" from theirs are
 * the same finding at two different scopes, and neither sibling could have
 * measured the pair-one scope, because the parameterized form did not exist when
 * they ran.
 */
val SIBLING_CALLGRAPH_VERDICT_AGAINST_THIS = "not_duplicate"
val SIBLING_DATAFLOW_VERDICT_AGAINST_THIS = "not_duplicate"

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

def log(line: String): Unit = {
  consoleLines += line
  println(line)
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

// -------------------------------------------------------------- file helpers
def writeUtf8(p: Path, content: String): Unit = {
  Option(p.getParent).foreach(Files.createDirectories(_))
  Files.write(p, content.getBytes(StandardCharsets.UTF_8))
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

/**
 * The identity pair a frontend record states, parsed out of it. The record must
 * state exactly ONE byte size and exactly ONE sha256: a record stating two
 * identities cannot adjudicate a load. Both label forms this pipeline's records
 * use are accepted - the composed deliverable writes `bytes:` / `byte size:` /
 * `sha256:`, the frontend's own run log writes `graph bytes:` / `graph sha256:` -
 * and nothing else is accepted, so a stray digest elsewhere in a 6 MB log cannot
 * be mistaken for the record.
 */
def identityPairOf(recordPath: Path, label: String): (Long, String) = {
  if (!Files.isRegularFile(recordPath)) {
    abortRun(s"the $label graph identity record is missing: $recordPath. The pair " +
      "recorded at write time is what every later load re-verifies, so there is " +
      "nothing to verify against")
  }
  val text = new String(Files.readAllBytes(recordPath), StandardCharsets.UTF_8)
  val sizeLineRe = """(?i)^\s*(?:graph\s+)?(?:bytes|byte size)\s*:\s*(\d+)\s*$""".r
  val shaLineRe = """(?i)^\s*(?:graph\s+)?sha256\s*:\s*([0-9a-fA-F]{64})\s*$""".r
  val sizes = text.linesIterator.collect { case sizeLineRe(v) => v }.toList.distinct
  val shas = text.linesIterator.collect { case shaLineRe(v) => v.toLowerCase }.toList.distinct
  if (sizes.size != 1 || shas.size != 1) {
    abortRun(s"$recordPath (the $label record) does not state one unambiguous identity " +
      s"pair: byte sizes found = ${sizes.mkString(",")}, sha256 values found = " +
      s"${shas.mkString(",")}. A record that states two identities cannot adjudicate " +
      "a load")
  }
  (sizes.head.toLong, shas.head)
}

/** Write the console log wherever the run got to - success or failure. */
var logTargetPath: Option[Path] = None
def flushConsoleLog(): Unit =
  logTargetPath.foreach { p =>
    try {
      writeUtf8(p, consoleLines.mkString("", "\n", "\n"))
      println(s"console log written: $p")
    } catch {
      case t: Throwable =>
        System.err.println(s"could not write the console log to $p: ${t.getMessage}")
    }
  }


// ===========================================================================
// THE QUERY. Everything below runs inside one try so that a failure names its
// stage, re-raises, and emits no result region.
// ===========================================================================

val runStartNanos = System.nanoTime()
println(MARKER_START)
consoleLines += MARKER_START

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
  log(s"console log target        : ${logTargetPath.get}")

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
  log(s"script JVM input args     : " +
    (if (jvmInputArgs.isEmpty) "<none>" else jvmInputArgs.mkString(" ")))
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
  val shaObserved = sha256Of(cpgResolved)
  log(s"named path is a symlink   : $cpgIsLink" +
    (if (cpgIsLink) s" -> $cpgLinkTarget" else ""))
  log(s"resolved target           : $cpgResolved")
  log(s"size WITHOUT following    : $sizeNoFollow  (recorded to be discarded)")
  log(s"size WITH following       : $sizeFollow  (the measurement of record)")
  log(s"sha256 of the target      : $shaObserved")

  // The record of account, and - when they are not the same file - the
  // repo-relative record as well. Both pairs are reported either way.
  val recordDefaultPath = repoRoot.resolve(CPG_RECORD_PATH).toAbsolutePath.normalize
  val recordEnvValue = sys.env.get(CPG_RECORD_ENV_VAR).filter(_.nonEmpty)
  val recordSelectedPath = recordEnvValue
    .map(v => Paths.get(v).toAbsolutePath.normalize)
    .getOrElse(recordDefaultPath)
  val recordSelectedSource =
    if (recordEnvValue.isDefined) CPG_RECORD_ENV_VAR
    else s"repo-relative default $CPG_RECORD_PATH"
  val recordsAreOneFile = recordSelectedPath == recordDefaultPath
  log(s"identity record source    : $recordSelectedSource")
  log(s"identity record (of account): $recordSelectedPath")
  log(s"identity record (repo-relative): $recordDefaultPath" +
    (if (recordsAreOneFile) "  (the same file)" else "  (a different file)"))

  val (recordedSize, recordedSha) = identityPairOf(recordSelectedPath, "selected")
  log(s"recorded at write time    : bytes=$recordedSize sha256=$recordedSha")

  val sizeMatches = sizeFollow == recordedSize
  val shaMatches = shaObserved == recordedSha
  log(s"byte size matches         : ${if (sizeMatches) "YES" else "NO"}")
  log(s"sha256 matches            : ${if (shaMatches) "YES" else "NO"}")
  if (!(sizeMatches && shaMatches)) {
    abortRun("graph identity mismatch: observed bytes=" + sizeFollow + " sha256=" +
      shaObserved + " against recorded bytes=" + recordedSize + " sha256=" + recordedSha +
      " in " + recordSelectedPath + ". A load against different bytes than the record " +
      "describes produces conclusions about a graph nobody has")
  }

  // The repo-relative record is read even when it is not the record of account,
  // so that a divergence is REPORTED with both pairs and their provenance rather
  // than left implicit. It is not used to adjudicate the load: it describes
  // whichever graph the provisioning that wrote it produced, and this run's
  // subject is the graph on this host.
  val (defaultRecordedSize, defaultRecordedSha) =
    if (recordsAreOneFile) (recordedSize, recordedSha)
    else identityPairOf(recordDefaultPath, "repo-relative")
  val defaultRecordAgrees =
    defaultRecordedSize == sizeFollow && defaultRecordedSha == shaObserved
  val identityDivergenceNote =
    if (recordsAreOneFile)
      "none: the record of account IS the repo-relative record " + CPG_RECORD_PATH +
        ", and the graph loaded matches the pair it states"
    else if (defaultRecordAgrees)
      "none: the record of account is " + recordSelectedPath + " and the repo-relative " +
        "record " + CPG_RECORD_PATH + " states the same pair"
    else
      "the repo-relative record " + CPG_RECORD_PATH + " states bytes=" +
        defaultRecordedSize + " sha256=" + defaultRecordedSha + ", which is NOT the " +
        "graph on this host (bytes=" + sizeFollow + " sha256=" + shaObserved + "). That " +
        "record is a committed deliverable describing the graph of the provisioning " +
        "that wrote it; the record of account for THIS load is " + recordSelectedPath +
        ", the frontend's own write-time record for the graph actually loaded, and the " +
        "load was verified against it. Both pairs are recorded with their provenance " +
        "and neither is discarded"
  log(s"repo-relative record pair : bytes=$defaultRecordedSize sha256=$defaultRecordedSha")
  log(s"repo-relative record agrees: $defaultRecordAgrees")
  log(s"identity divergence       : $identityDivergenceNote")

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
    routeSurfaceTypePrefixes = (PAIR_ONE_HANDLER_TYPE :: SINK_SURFACE_TYPE_PREFIXES).distinct)

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
    routeSurfaceTypePrefixes =
      (PAIR_TWO_HANDLER_TYPE :: PAIR_ONE_HANDLER_TYPE :: SINK_SURFACE_TYPE_PREFIXES).distinct)

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
  // it, and two Joern processes sharing one corrupt each other. It is switched
  // BEFORE any load. queries/joern/.workspace carries its own .gitignore, so the
  // scratch stays out of the commit without editing upstream Spark's .gitignore.
  val workspaceResolved = repoRoot.resolve(WORKSPACE_PATH).toAbsolutePath.normalize
  Files.createDirectories(workspaceResolved)
  log(s"workspace (AAP name)      : $WORKSPACE_PATH")
  log(s"workspace (resolved)      : $workspaceResolved")
  switchWorkspace(workspaceResolved.toString)

  val loadNanos = System.nanoTime()
  log(s"loading the graph with importCpg: $cpgResolved")
  val loaded = importCpg(cpgResolved.toString)
  if (loaded.isEmpty) {
    abortRun(s"importCpg returned no graph for $cpgResolved")
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

  // -------------------------------------------------------------------------
  stage("F-selection: each pair's entry points and the shared sink")
  // -------------------------------------------------------------------------
  /** One indexed sweep per distinct call name, cached so a name shared by both
   *  pairs is ONE measurement cited twice rather than two measurements. */
  val callScanCache =
    scala.collection.mutable.LinkedHashMap.empty[String, (List[Call], Boolean)]
  def scanCallsNamed(name: String): (List[Call], Boolean) =
    callScanCache.getOrElseUpdate(name, {
      val scanned = cpg.call.nameExact(name).take(MAX_CALL_SCAN).l
      (scanned, scanned.size >= MAX_CALL_SCAN)
    })

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
    val syntheticTypeDecls = cpg.typeDecl.fullName(p.handlerSyntheticTypeRegex).l
    val syntheticTypeNames = syntheticTypeDecls.map(_.fullName).distinct.sorted
    val syntheticEntryNodes = syntheticTypeDecls
      .flatMap(_.method.l)
      .filter(_.name == p.handlerSyntheticMethod)
    val sourceLevelNodes = cpg.typeDecl
      .fullNameExact(p.handlerType)
      .method
      .nameExact(p.handlerMethod)
      .l
    val baseDeclarationNames =
      if (p.handlerBaseType.isEmpty) Nil
      else cpg.typeDecl
        .fullNameExact(p.handlerBaseType)
        .method
        .nameExact(p.handlerMethod)
        .l
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
  val predicateTypeDecls = cpg.typeDecl.fullNameExact(PREDICATE_TYPE).l
  if (predicateTypeDecls.isEmpty) {
    abortRun(s"$PREDICATE_TYPE is not present in the graph, so the mechanical " +
      "definition of a spurious route has no predicate set to rest on")
  }
  val predicateTypeMethods = predicateTypeDecls.flatMap(_.method.l)
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
  val predicateCallSites = predicateFinal
    .flatMap(_.callIn.l)
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
   * A measured property of the SHARED prefix list, reported rather than fixed by
   * editing the byte-identical block: pair two's handler type is the class the
   * method is declared in, and it does not start with any prefix in that list.
   * The per-pair surfaces above are what make each pair's own basis correct, and
   * the shared list is retained exactly so all three queries' spurious counts
   * stay comparable.
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
      maxDepthUsed: Int,
      depthBoundReached: Boolean,
      expansionBudgetExhausted: Boolean,
      stepCapReached: Boolean,
      routeCapReached: Boolean,
      routes: List[RouteRecord])

  /**
   * One bounded breadth-first walk over CALL edges, for ONE pair. Every bound it
   * respects is a named constant and every counter it fills is reported for that
   * pair: nothing here is shared with the other pair's budget, so one pair
   * cannot silently truncate the other.
   */
  def walk(s: PairSelection, walkId: String, followFanOut: Boolean): WalkResult = {
    val p = s.pair
    var methodsVisited = 0
    var expansions = 0
    var callSitesConsidered = 0
    var fanOutEncountered = 0
    var fanOutNotFollowed = 0
    var maxDepthUsed = 0
    var depthBoundReached = false
    var budgetExhausted = false
    var stepCapReached = false
    var routeCapReached = false
    val routes = scala.collection.mutable.ArrayBuffer.empty[RouteRecord]

    s.entryGroupsTraversed.foreach { case (entryName, entryNodes) =>
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
          if (expansions >= MAX_EXPANSIONS_PER_ENTRY) {
            budgetExhausted = true
            stop = true
          } else if (callSitesConsidered >= MAX_STEPS_PER_PAIR) {
            stepCapReached = true
            stop = true
          } else {
            expansions += 1
            callSitesOf(fromNodes).foreach { c =>
              callSitesConsidered += 1
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
                    if (s.sinkHostNames.contains(toName) &&
                      !routes.exists(r => r.walkId == walkId && r.entryPoint == entryName &&
                        r.sinkHost == toName)) {
                      if (routes.size >= MAX_ROUTES_PER_PAIR) routeCapReached = true
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
            }
          }
        }
        frontier = nextByName.toList
        depth += 1
        if (depth > maxDepthUsed) maxDepthUsed = depth
        if (frontier.nonEmpty && depth >= MAX_CALL_DEPTH) depthBoundReached = true
      }
      methodsVisited += visited.size
      log(f"  walk ${p.id}%-9s $walkId%-18s entry=$entryName visited=${visited.size}%8d " +
        f"depth=$depth%2d expansions=$expansions%8d")
    }

    WalkResult(p.id, walkId, followFanOut, s.entryPointsTraversed, expansions,
      methodsVisited, callSitesConsidered, fanOutEncountered, fanOutNotFollowed,
      maxDepthUsed, depthBoundReached, budgetExhausted, stepCapReached, routeCapReached,
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
      invoked: Boolean)

  val traversals: List[PairTraversal] = selections.map { s =>
    val p = s.pair
    val nanosA = System.nanoTime()
    val walkA = walk(s, WALK_A_ID, followFanOut = true)
    log(s"pair ${p.id}: walk $WALK_A_ID elapsed_ms=${elapsedMs(nanosA)}")
    val nanosB = System.nanoTime()
    val walkB = walk(s, WALK_B_ID, followFanOut = false)
    log(s"pair ${p.id}: walk $WALK_B_ID elapsed_ms=${elapsedMs(nanosB)}")
    val walks = List(walkA, walkB)
    walks.foreach { w =>
      log(s"pair ${p.id}: walk ${w.walkId}: routes=${w.routes.size} " +
        s"expansions=${w.expansions} call_sites=${w.callSitesConsidered} " +
        s"fanout_seen=${w.fanOutSitesEncountered} " +
        s"fanout_not_followed=${w.fanOutSitesNotFollowed} max_depth=${w.maxDepthUsed} " +
        s"depth_bound_reached=${w.depthBoundReached} " +
        s"budget_exhausted=${w.expansionBudgetExhausted} " +
        s"step_cap_reached=${w.stepCapReached} " +
        s"route_cap_reached=${w.routeCapReached}")
    }
    val distinct = walks
      .flatMap(_.routes)
      .distinctBy(r => (r.entryPoint, r.sinkHost,
        r.hops.map(h => (h.fromMethod, h.callSite, h.toMethod))))
      .sortBy(r => (r.entryPoint, r.sinkHost, r.hops.size))
    val boundReached = walks.exists(w =>
      w.depthBoundReached || w.expansionBudgetExhausted || w.stepCapReached ||
        w.routeCapReached)
    log(s"pair ${p.id}: distinct routes (its own two walks, deduplicated): " +
      s"${distinct.size}")
    log(s"pair ${p.id}: any bound reached: $boundReached")
    PairTraversal(p.id, walks, distinct, boundReached, invoked = true)
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
    val typeDecls = cpg.typeDecl.fullNameExact(h.messageType).l
    if (typeDecls.isEmpty) {
      abortRun(s"message type ${h.messageType} (hop ${h.id}) is not present in the graph, " +
        "so the hop it models cannot be measured and the route count would be " +
        "uninterpretable")
    }
    val methods = typeDecls.flatMap(_.method.l)
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
    val producerSites = ctors
      .flatMap(_.callIn.l)
      .filter(c => outsideMessageType(c.method))
      .distinctBy(c => (c.method.fullName, lineOf(c)))
      .sortBy(c => (c.method.fullName, lineOf(c)))
    val consumerSites = accessors
      .flatMap(_.callIn.l)
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
    val threadHostMethods = cpg.typeDecl
      .fullNameExact(THREAD_HOST_TYPE)
      .method
      .nameExact(THREAD_HOST_METHOD)
      .l
    val threadStartSites = callSitesOf(threadHostMethods).filter(_.name == THREAD_HOST_METHOD)
    val threadStartCallees =
      threadStartSites.flatMap(calleesOf).map(_.fullName).distinct.sorted
    val threadBodyMethods = cpg.typeDecl
      .fullName(THREAD_BODY_TYPE_REGEX)
      .method
      .nameExact(THREAD_BODY_METHOD)
      .l
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
    val jdkLaunchMethodNodes = cpg.method.fullNameExact(JDK_LAUNCH_METHOD_FULL_NAME).l
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
  // (dataflow), or a third? It is answered from measured or checkable properties
  // rather than asserted, and if it IS one of them restated then that is said
  // plainly - a legitimate probe finding, not a defect to hide.
  val apiConstructsHere = JOERN_API_CONSTRUCTS.distinct.sorted
  val apiConstructs01 = SIBLING_CALLGRAPH_API_CONSTRUCTS.distinct.sorted
  val apiConstructs02 = SIBLING_DATAFLOW_API_CONSTRUCTS.distinct.sorted
  val apiOnlyHereVs01 = apiConstructsHere.filterNot(apiConstructs01.contains)
  val apiOnlyIn01 = apiConstructs01.filterNot(apiConstructsHere.contains)
  val apiSharedWith01 = apiConstructsHere.filter(apiConstructs01.contains)
  val apiOnlyHereVs02 = apiConstructsHere.filterNot(apiConstructs02.contains)
  val apiOnlyIn02 = apiConstructs02.filterNot(apiConstructsHere.contains)
  val apiSharedWith02 = apiConstructsHere.filter(apiConstructs02.contains)
  log(s"API constructs here       : ${apiConstructsHere.size}")
  log(s"API constructs in 01      : ${apiConstructs01.size} (transcribed from its envelope)")
  log(s"API constructs in 02      : ${apiConstructs02.size} (transcribed from its envelope)")
  log(s"only here vs 01           : ${apiOnlyHereVs01.mkString(", ")}")
  log(s"only in 01                : ${apiOnlyIn01.mkString(", ")}")
  log(s"shared with 01            : ${apiSharedWith01.size}")
  log(s"only here vs 02           : ${apiOnlyHereVs02.mkString(", ")}")
  log(s"only in 02                : ${apiOnlyIn02.mkString(", ")}")
  log(s"shared with 02            : ${apiSharedWith02.size}")

  /** Query 01 numbered the four hops it measured B1..B4. This query names them
   *  after the hop, and the same hop must be comparable across the two, so the
   *  translation is declared rather than left to a reader. */
  val BOUNDARY_ID_TO_SIBLING_01 = Map(
    "B-rpc-" + MESSAGE_HOP_LAUNCH_DRIVER_ID -> "B1-rpc",
    "B-thread" -> "B2-thread",
    "B-interface" -> "B3-interface",
    "B-partial-function-" + PAIR_ONE_ID -> "B4-partial-function")

  val pairOneTraversal = traversals.head
  val pairOneSelection = selections.head
  val pairOneBoundaryIds = boundaryIdsByPair(PAIR_ONE_ID)
  val pairOneNotCrossedHere = pairOneBoundaryIds
    .filter(id => !boundaryStore(id).crossedByACallEdge)
    .flatMap(id => BOUNDARY_ID_TO_SIBLING_01.get(id))
    .sorted
  val sibling01NotCrossed = SIBLING_CALLGRAPH_BOUNDARIES_NOT_CROSSED.sorted
  val boundaryVerdictsAgreeWith01 = pairOneNotCrossedHere == sibling01NotCrossed
  val entryPointsAgreeWith01 =
    pairOneSelection.entryGroups.map(_._1).sorted == SIBLING_CALLGRAPH_ENTRY_POINTS.sorted
  val routeCountAgreesWith01 =
    pairOneTraversal.distinctRoutes.size == SIBLING_CALLGRAPH_DISTINCT_ROUTES
  val boundValueAgreesWith01 = MAX_CALL_DEPTH == SIBLING_CALLGRAPH_BOUND_VALUE
  val apiSetsIdenticalTo01 = apiOnlyHereVs01.isEmpty && apiOnlyIn01.isEmpty
  log(s"pair-one entry points agree with 01 (transcribed): $entryPointsAgreeWith01")
  log(s"pair-one distinct-route count agrees with 01     : $routeCountAgreesWith01 " +
    s"(here ${pairOneTraversal.distinctRoutes.size}, 01 published " +
    s"$SIBLING_CALLGRAPH_DISTINCT_ROUTES)")
  log(s"bound value agrees with 01                       : $boundValueAgreesWith01 " +
    s"(here $MAX_CALL_DEPTH, 01 published $SIBLING_CALLGRAPH_BOUND_VALUE)")
  log(s"pair-one boundary verdicts agree with 01         : $boundaryVerdictsAgreeWith01 " +
    s"(here ${pairOneNotCrossedHere.mkString(", ")}; 01 ${sibling01NotCrossed.mkString(", ")})")
  log(s"API construct sets identical to 01               : $apiSetsIdenticalTo01")

  val sameFormulationAsO1OnPairOne =
    apiSetsIdenticalTo01 && boundValueAgreesWith01 && entryPointsAgreeWith01 &&
      boundaryVerdictsAgreeWith01
  val duplicateVerdictAgainst01 =
    if (sameFormulationAsO1OnPairOne) "duplicate_formulation_on_pair_one"
    else "not_duplicate"
  val duplicateBasisAgainst01 =
    (if (sameFormulationAsO1OnPairOne)
      "SAID PLAINLY: instantiated on pair one this query IS query 01's formulation " +
        "restated in parameterized form, and the evidence is measured rather than " +
        "asserted - the same edge kind (CALL edges only, no data edge and no flow " +
        "engine), the same entry-point resolution (the synthetic partial-function " +
        "method together with the source-level method), the same sink constraint, the " +
        "same bound value " + MAX_CALL_DEPTH.toString + ", the same two walk modes, and " +
        "an API construct list whose set difference against query 01's published list " +
        "is empty in BOTH directions. On this run the two also agree on pair one's " +
        "entry-point set, on its distinct-route count and on the four boundary verdicts " +
        "after the declared id translation. "
     else
      "instantiated on pair one this query differs from query 01 on at least one " +
        "checkable property: API set identical=" + apiSetsIdenticalTo01.toString +
        ", bound value equal=" + boundValueAgreesWith01.toString + ", entry points " +
        "equal=" + entryPointsAgreeWith01.toString + ", boundary verdicts equal=" +
        boundaryVerdictsAgreeWith01.toString + ". ") +
      "WHAT IS NOT A DUPLICATE: the query as a whole. It takes the handler/sink pair as " +
      "a parameter and is invoked on a SECOND pair (" + PAIR_TWO_ID + ", " +
      PAIR_TWO_HANDLER_TYPE + "." + PAIR_TWO_HANDLER_METHOD + ") that query 01 does not " +
      "address at all, it measures " +
      boundaryIdsByPair(PAIR_TWO_ID).size.toString + " boundaries on that pair against " +
      boundaryIdsByPair(PAIR_ONE_ID).size.toString + " on pair one, and it models one " +
      "hop query 01 never reaches - the servlet's own message send, whose producer and " +
      "consumer ends are measured in stage I. RECONCILED WITH WHAT QUERY 01 PUBLISHED: " +
      "its envelope records '" + SIBLING_CALLGRAPH_VERDICT_AGAINST_THIS + "' against this " +
      "query, on the ground that this query covers a second pair and a different target " +
      "set. This report AGREES at that scope - as wholes the two are not duplicates - and " +
      "adds the pair-one scope, which query 01 could not have measured because the " +
      "parameterized form did not exist when it ran. The two verdicts are the same " +
      "finding at two scopes rather than a disagreement, and the scope is named in both " +
      "directions so neither reads as a contradiction of the other. Neither query's " +
      "returns are added to the other's anywhere: they are reported side by side, per " +
      "pair, and NEVER SUMMED."
  val duplicateVerdictAgainst02 = "not_duplicate"
  val duplicateBasisAgainst02 =
    "A different formulation over different edges and different nodes. Query 02 " +
      "traverses reaching-definition (data) edges through the OSS dataflow layer and " +
      "selects PARAMETER and EXPRESSION nodes as its ends; this query traverses CALL " +
      "edges and selects whole METHODS, and it loads no flow engine at all. Auditable " +
      "corroboration, computed here as a set difference against query 02's published " +
      "list rather than eyeballed: " + apiOnlyIn02.size.toString + " of query 02's " +
      apiConstructs02.size.toString + " API constructs do not appear in this query's " +
      "list (" + apiOnlyIn02.mkString(", ") + "), and " + apiOnlyHereVs02.size.toString +
      " of this query's do not appear in query 02's (" + apiOnlyHereVs02.mkString(", ") +
      "). The two also carry different bounds - this query's bound value is " +
      MAX_CALL_DEPTH.toString + " call-graph hops, query 02 published " +
      SIBLING_DATAFLOW_BOUND_VALUE.toString + " for its own flow-call depth - so the two " +
      "numbers are not even the same kind of quantity. Their returns are likewise never " +
      "summed."
  val duplicateFormulationScalar =
    if (sameFormulationAsO1OnPairOne) "partial_duplicate" else "not_duplicate"
  val duplicateFormulationSummary =
    (if (sameFormulationAsO1OnPairOne)
      "A duplicate of query 01's formulation ON PAIR ONE, not a duplicate as a whole, " +
        "and not a duplicate of query 02 in any instantiation"
     else
      "Not a duplicate of either sibling in any instantiation") +
      ". The scope of the duplication is stated rather than hidden: it is exactly the " +
      "pair-one instantiation, and it is what makes the parameterized form's second " +
      "instantiation the part that is new."
  log(s"duplicate vs 01           : $duplicateVerdictAgainst01")
  log(s"duplicate vs 02           : $duplicateVerdictAgainst02")
  log(s"duplicate_formulation     : $duplicateFormulationScalar")


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
    "max_depth_used" -> jnum(w.maxDepthUsed.toLong),
    "depth_bound_reached" -> jbool(w.depthBoundReached),
    "expansion_budget_exhausted" -> jbool(w.expansionBudgetExhausted),
    "step_cap_reached" -> jbool(w.stepCapReached),
    "route_cap_reached" -> jbool(w.routeCapReached),
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
    "statement" -> jstr(
      "the measure is settled by an invocation, not by a parameter list: both pairs were " +
        "invoked in this single run, in the declared order, and the second pair's " +
        "selection, walk counters, boundary measurements, distinct-route count and " +
        "spurious count are all published above and in both result files. An empty " +
        "result from a real invocation satisfies the measure; a skipped invocation " +
        "would not, and a malformed pair aborts the run rather than being passed over")))

  val duplicateFormulationJson = jrawArr(4, List(
    jobj(6, List(
      "against" -> jstr(SIBLING_CALLGRAPH_QUERY),
      "status" -> jstr(duplicateVerdictAgainst01),
      "scope_of_the_duplication" ->
        jstr(if (sameFormulationAsO1OnPairOne) PAIR_ONE_ID + " only" else "none"),
      "basis" -> jstr(duplicateBasisAgainst01),
      "evidence" -> jobj(8, List(
        "api_construct_sets_identical" -> jbool(apiSetsIdenticalTo01),
        "api_constructs_only_here" -> jstrArr(apiOnlyHereVs01),
        "api_constructs_only_in_the_sibling" -> jstrArr(apiOnlyIn01),
        "api_constructs_shared" -> jnum(apiSharedWith01.size.toLong),
        "bound_value_here" -> jnum(MAX_CALL_DEPTH.toLong),
        "bound_value_published_by_the_sibling" ->
          jnum(SIBLING_CALLGRAPH_BOUND_VALUE.toLong),
        "bound_values_agree" -> jbool(boundValueAgreesWith01),
        "pair_one_entry_points_here" -> jstrArr(pairOneSelection.entryGroups.map(_._1)),
        "entry_points_published_by_the_sibling_transcribed" ->
          jstrArr(SIBLING_CALLGRAPH_ENTRY_POINTS),
        "entry_point_sets_agree" -> jbool(entryPointsAgreeWith01),
        "pair_one_distinct_routes_here" ->
          jnum(pairOneTraversal.distinctRoutes.size.toLong),
        "distinct_routes_published_by_the_sibling_transcribed" ->
          jnum(SIBLING_CALLGRAPH_DISTINCT_ROUTES.toLong),
        "distinct_route_counts_agree" -> jbool(routeCountAgreesWith01),
        "pair_one_boundaries_not_crossed_translated_to_the_sibling_ids" ->
          jstrArr(pairOneNotCrossedHere),
        "boundaries_not_crossed_published_by_the_sibling_transcribed" ->
          jstrArr(sibling01NotCrossed),
        "boundary_verdicts_agree" -> jbool(boundaryVerdictsAgreeWith01),
        "boundary_id_translation" -> jstrArr(BOUNDARY_ID_TO_SIBLING_01.toList.sorted
          .map { case (mine, theirs) => mine + " -> " + theirs }),
        "sibling_published_verdict_against_this_query_transcribed" ->
          jstr(SIBLING_CALLGRAPH_VERDICT_AGAINST_THIS),
        "verdicts_are_the_same_finding_at_two_scopes" -> jbool(true),
        "sibling_figures_are_transcribed_not_measured_here" -> jbool(true),
        "sibling_figures_were_measured_against_the_graph_of_its_own_run" -> jbool(true))))),
    jobj(6, List(
      "against" -> jstr(SIBLING_DATAFLOW_QUERY),
      "status" -> jstr(duplicateVerdictAgainst02),
      "scope_of_the_duplication" -> jstr("none"),
      "basis" -> jstr(duplicateBasisAgainst02),
      "evidence" -> jobj(8, List(
        "api_constructs_only_here" -> jstrArr(apiOnlyHereVs02),
        "api_constructs_only_in_the_sibling" -> jstrArr(apiOnlyIn02),
        "api_constructs_shared" -> jnum(apiSharedWith02.size.toLong),
        "bound_value_here" -> jnum(MAX_CALL_DEPTH.toLong),
        "bound_value_published_by_the_sibling" -> jnum(SIBLING_DATAFLOW_BOUND_VALUE.toLong),
        "bound_values_are_the_same_kind_of_quantity" -> jbool(false),
        "distinct_routes_published_by_the_sibling_transcribed" ->
          jnum(SIBLING_DATAFLOW_DISTINCT_ROUTES.toLong),
        "flow_engine_loaded_here" -> jbool(false),
        "sibling_published_verdict_against_this_query_transcribed" ->
          jstr(SIBLING_DATAFLOW_VERDICT_AGAINST_THIS),
        "verdicts_agree_in_both_directions" -> jbool(
          duplicateVerdictAgainst02 == SIBLING_DATAFLOW_VERDICT_AGAINST_THIS),
        "sibling_figures_are_transcribed_not_measured_here" -> jbool(true)))))))


  // -------------------------------------------------------------------------
  stage("M-write: the envelope, the prose report and the console log")
  // -------------------------------------------------------------------------
  val resultsDir = repoRoot.resolve(RESULTS_DIR)
  Files.createDirectories(resultsDir)
  val jsonPath = resultsDir.resolve(s"$QUERY_ID.json")
  val mdPath = resultsDir.resolve(s"$QUERY_ID.md")

  def byPairNum(f: (PairSelection, PairTraversal, PairSpurious) => Long): String =
    jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jnum(f(selections(i), traversals(i), spuriousByPair(i)))))
  def byPairBool(f: (PairSelection, PairTraversal, PairSpurious) => Boolean): String =
    jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jbool(f(selections(i), traversals(i), spuriousByPair(i)))))
  def byPairStr(f: (PairSelection, PairTraversal, PairSpurious) => String): String =
    jbyPair(2, PAIRS.indices.toList.map(i =>
      PAIRS(i).id -> jstr(f(selections(i), traversals(i), spuriousByPair(i)))))

  val envelope = jobj(0, List(
    "query_id" -> jstr(QUERY_ID),
    "query_source" -> jstr(s"queries/joern/$QUERY_ID.sc"),
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
      "FANOUT_CALLEE_THRESHOLD" -> jnum(FANOUT_CALLEE_THRESHOLD.toLong))),
    "entry_points_discovered" -> byPairNum((s, _, _) => s.entryPointsDiscovered.toLong),
    "entry_points_traversed" -> byPairNum((s, _, _) => s.entryPointsTraversed.toLong),
    "entry_points_truncated" -> byPairNum((s, _, _) => s.entryPointsTruncated.toLong),
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
      "named_path" -> jstr(cpgNamed.toString),
      "resolved_path" -> jstr(cpgResolved.toString),
      "named_path_is_symlink" -> jbool(cpgIsLink),
      "byte_size_following_the_link" -> jnum(sizeFollow),
      "byte_size_without_following" -> jnum(sizeNoFollow),
      "sha256" -> jstr(shaObserved),
      "identity_record_of_account" -> jstr(recordSelectedPath.toString),
      "identity_record_source" -> jstr(recordSelectedSource),
      "identity_recorded_byte_size" -> jnum(recordedSize),
      "identity_recorded_sha256" -> jstr(recordedSha),
      "identity_reverified_before_load" -> jbool(true),
      "identity_record_repo_relative" -> jstr(CPG_RECORD_PATH),
      "identity_record_repo_relative_byte_size" -> jnum(defaultRecordedSize),
      "identity_record_repo_relative_sha256" -> jstr(defaultRecordedSha),
      "identity_record_repo_relative_agrees" -> jbool(defaultRecordAgrees),
      "identity_divergence" -> jstr(identityDivergenceNote),
      "aap_named_path_reconciliation" -> jstr(aapNameReconciliation),
      "methods" -> jnum(methodCount.toLong),
      "type_declarations" -> jnum(typeDeclCount.toLong),
      "files" -> jnum(fileCount.toLong))),
    "runtime" -> jobj(2, List(
      "jdk_major" -> jstr(jdkMajor),
      "jvm_version" -> jstr(jvmVersion),
      "heap_actually_used_bytes" -> jnum(heapMaxBytes),
      "heap_floor_bytes" -> jnum(HEAP_FLOOR_BYTES),
      "loader" -> jstr("importCpg into a switched workspace; the frontend-then-importCpg " +
        "route is mandated because the alternative spawns a second JVM at the same heap"),
      "workspace" -> jstr(WORKSPACE_PATH),
      "heap_bound_jvm_position" -> jstr("the Stage 5 probe, one of the four heap-bound " +
        "JVM invocations this run records separately (frontend build, importCpg " +
        "verification load, Stage 3 Joern runner, this probe)"),
      "command" -> jstr("cd <scratch outside the repository> && " +
        "HARNESS_REPO_ROOT=<repo> JAVA_HOME=\"$JAVA_HOME_21\" " +
        "JAVA_TOOL_OPTIONS=\"-Xmx64g\" SL_LOGGING_LEVEL=WARN joern --script " +
        "<repo>/queries/joern/" + QUERY_ID + ".sc -J-Xmx64g < /dev/null"),
      "parameters_passed_on_the_command_line" -> jstr("none: the pairs are declared as " +
        "named constants in the query source and both are invoked in a single run, so " +
        "the invocation is reproducible from this command alone"))),
    "predicate_selector" -> jobj(2, List(
      "type" -> jstr(PREDICATE_TYPE),
      "name_regex" -> jstr(PREDICATE_NAME_REGEX),
      "setter_suffix_excluded" -> jstr(PREDICATE_SETTER_SUFFIX),
      "named_five" -> jstrArr(PREDICATE_NAMED_FIVE.sorted),
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
      "shared_route_surface_type_prefixes" -> jstrArr(ROUTE_SURFACE_TYPE_PREFIXES),
      "call_sites_on_the_shared_route_surface" ->
        jnum(predicateCallSitesOnSharedSurface.size.toLong),
      "call_sites_on_each_pair_own_route_surface" -> jbyPair(4, PAIRS.map(p =>
        p.id -> jnum(predicateCallSitesByPair(p.id).size.toLong))),
      "handler_types_not_covered_by_the_shared_prefixes" ->
        jstrArr(pairsNotCoveredBySharedSurface))),
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
    "duplicate_formulation" -> jstr(duplicateFormulationScalar),
    "duplicate_formulation_summary" -> jstr(duplicateFormulationSummary),
    "duplicate_formulation_detail" -> duplicateFormulationJson,
    "effort_query_revisions_committed" -> jnum(QUERY_REVISIONS_COMMITTED.toLong),
    "effort_query_revisions_convention" -> jstr(QUERY_REVISIONS_CONVENTION),
    "effort_joern_api_constructs" -> jstrArr(JOERN_API_CONSTRUCTS),
    "effort_joern_api_construct_count" -> jnum(JOERN_API_CONSTRUCTS.distinct.size.toLong),
    "effort_joern_api_constructs_not_used_by_01" -> jstrArr(apiOnlyHereVs01),
    "effort_joern_api_constructs_not_used_by_02" -> jstrArr(apiOnlyHereVs02),
    "effort_parameterizability" -> jstr(parameterizabilityVerdict),
    "parameterizability" -> parameterizabilityJson,
    "total_returns_cap_reached" -> jbool(totalReturnsCapReached),
    "records" -> jrawArr(2, recordJson))) + "\n"

  writeUtf8(jsonPath, envelope)
  log(s"envelope written          : $jsonPath (${envelope.length} chars)")


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
  md0(s"| Query source | `queries/joern/$QUERY_ID.sc` |")
  md0(s"| Envelope | `$RESULTS_DIR/$QUERY_ID.json` |")
  md0(s"| Console log | `$LOG_DIR/probe-$QUERY_ID.log` |")
  md0(s"| Loader | `importCpg` into a switched workspace (`$WORKSPACE_PATH`) |")
  md0(s"| JDK major | $jdkMajor |")
  md0(s"| Heap actually used | $heapMaxBytes bytes (floor $HEAP_FLOOR_BYTES) |")
  md0(s"| Graph | $sizeFollow bytes, sha256 `$shaObserved` |")
  md0(s"| Graph identity re-verified before the load | yes, against " +
    s"`${recordSelectedPath.getFileName}` (source: $recordSelectedSource) |")
  md0(s"| Graph methods / typeDecls / files | $methodCount / $typeDeclCount / $fileCount |")
  md0("| Compile status | compiled |")
  md0("| Run status | completed |")
  md0(s"| Pairs declared / invoked | ${PAIRS.size} / ${traversals.count(_.invoked)} |")
  md0(s"| Pair iteration order | ${PAIRS.map(p => s"`${p.id}`").mkString(", ")} |")
  md0(s"| Records returned | $returnedRecordCount (${boundaries.size} boundary " +
    "measurement(s) plus per-pair route records) |")
  md0(s"| Parameterizability | **$parameterizabilityVerdict** |")
  md0(s"| Duplicate formulation | **$duplicateFormulationScalar** |")
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
      md0(s"  - `${c.method.fullName}` calls `${c.methodFullName}` at graph line " +
        s"${lineOf(c)} (dispatch `${c.dispatchType}`)")
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
        md0(s"  - walk `${r.walkId}`, ${r.hops.size} hops, entry `${r.entryPoint}` to " +
          s"sink host `${r.sinkHost}`")
      }
    }
    md0(s"- **walks** (its own two, never combined with the other pair's):")
    t.walks.foreach { w =>
      md0(s"  - `${w.walkId}`: follows fan-out ${w.followsFanOut}, expansions " +
        s"${w.expansions}, call sites ${w.callSitesConsidered}, fan-out seen " +
        s"${w.fanOutSitesEncountered}, fan-out not followed ${w.fanOutSitesNotFollowed}, " +
        s"max depth ${w.maxDepthUsed}, depth bound reached ${w.depthBoundReached}, " +
        s"expansion budget exhausted ${w.expansionBudgetExhausted}, step cap reached " +
        s"${w.stepCapReached}, route cap reached ${w.routeCapReached}, routes " +
        s"${w.routes.size}")
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
        if (w.expansionBudgetExhausted)
          Some("the per-entry expansion budget was exhausted") else None,
        if (w.stepCapReached) Some("the per-pair step cap was reached") else None,
        if (w.routeCapReached) Some("the per-pair route cap was reached") else None).flatten
      md0(s"  - walk `${w.walkId}`: " +
        (if (bits.isEmpty) "no bound was reached; the walk ran to exhaustion"
         else bits.mkString("; ")) +
        s". Expansion budget used ${w.expansions} of $MAX_EXPANSIONS_PER_ENTRY per entry " +
        s"point; call sites considered ${w.callSitesConsidered} of $MAX_STEPS_PER_PAIR " +
        s"for the pair; routes returned ${w.routes.size} of $MAX_ROUTES_PER_PAIR.")
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
  md0(s"| FANOUT_CALLEE_THRESHOLD | $FANOUT_CALLEE_THRESHOLD |")
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
      md0(s"- **from**: ${if (b.fromEnd.isEmpty) "(none measured)" else "`" + b.fromEnd + "`"}")
      md0(s"- **to**: ${if (b.toEnd.isEmpty) "(none measured)" else "`" + b.toEnd + "`"}")
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
  md0("The selector block in this query's source is **byte-identical** to the")
  md0(s"corresponding block of `queries/joern/$SIBLING_CALLGRAPH_QUERY.sc` and")
  md0(s"`queries/joern/$SIBLING_DATAFLOW_QUERY.sc`. It has to be: three spurious")
  md0("counts are only comparable if the definition of the term is the same text in all")
  md0("three files.")
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
    predicateBroadNames.map(n => s"`$n`").mkString(", "))
  md0(s"2. minus every name ending in `$PREDICATE_SETTER_SUFFIX`, which drops " +
    (if (predicateSetterExcludedNames.isEmpty) "nothing"
     else predicateSetterExcludedNames.map(n => s"`$n`").mkString(", ")) +
    ", leaving " + predicateAfterSetterNames.map(n => s"`$n`").mkString(", "))
  md0("3. intersected with the five named source-level predicates, which drops " +
    (if (predicateNonPredicateResidue.isEmpty) "nothing"
     else predicateNonPredicateResidue.map(n => s"`$n`").mkString(", ") +
       " - a private-var getter, not one of the five") +
    ", leaving exactly " + predicateFinalNames.map(n => s"`$n`").mkString(", "))
  md0("")
  md0("The final set is asserted against the graph, not against the source.")
  md0("")
  md0("### The shared route surface, and each pair's own")
  md0("")
  md0("The byte-identical block also carries `ROUTE_SURFACE_TYPE_PREFIXES` = " +
    ROUTE_SURFACE_TYPE_PREFIXES.map(p => s"`$p`").mkString(", ") + ".")
  md0("Measured here rather than assumed: " +
    (if (pairsNotCoveredBySharedSurface.isEmpty)
      "every pair's handler type is covered by one of those prefixes."
     else "the handler type(s) " +
       pairsNotCoveredBySharedSurface.map(t => s"`$t`").mkString(", ") +
       " are **not** covered by any of those prefixes, because the type a method is " +
       "declared in is not always the headline class of the file it lives in."))
  md0("The shared list is kept exactly as it stands so all three queries' spurious counts")
  md0("remain comparable, and each pair additionally carries its **own** route surface,")
  md0("derived from its own handler and sink types, which is what makes that pair's")
  md0("expected-spurious basis correct. Both counts are published.")
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
  md0(s"`duplicate_formulation` = **$duplicateFormulationScalar**. $duplicateFormulationSummary")
  md0("")
  md0(s"### Against `$SIBLING_CALLGRAPH_QUERY`: **$duplicateVerdictAgainst01**")
  md0("")
  md0(duplicateBasisAgainst01)
  md0("")
  md0("| property | here (measured) | query 01 (transcribed from its envelope) | agree |")
  md0("| --- | --- | --- | --- |")
  md0(s"| API construct count | ${apiConstructsHere.size} | ${apiConstructs01.size} | " +
    s"$apiSetsIdenticalTo01 |")
  md0(s"| bound value | $MAX_CALL_DEPTH | $SIBLING_CALLGRAPH_BOUND_VALUE | " +
    s"$boundValueAgreesWith01 |")
  md0(s"| pair-one entry points | ${pairOneSelection.entryGroups.size} | " +
    s"${SIBLING_CALLGRAPH_ENTRY_POINTS.size} | $entryPointsAgreeWith01 |")
  md0(s"| pair-one distinct routes | ${pairOneTraversal.distinctRoutes.size} | " +
    s"$SIBLING_CALLGRAPH_DISTINCT_ROUTES | $routeCountAgreesWith01 |")
  md0(s"| boundaries not crossed | ${pairOneNotCrossedHere.mkString(", ")} | " +
    s"${sibling01NotCrossed.mkString(", ")} | $boundaryVerdictsAgreeWith01 |")
  md0("")
  md0("The sibling's figures are **transcribed** from its published envelope, never")
  md0("re-measured here, and they were measured against the graph of its own run. The")
  md0("boundary ids are translated by the mapping declared in the query source: " +
    BOUNDARY_ID_TO_SIBLING_01.toList.sorted
      .map { case (mine, theirs) => s"`$mine` -> `$theirs`" }.mkString(", ") + ".")
  md0("")
  md0(s"### Against `$SIBLING_DATAFLOW_QUERY`: **$duplicateVerdictAgainst02**")
  md0("")
  md0(duplicateBasisAgainst02)
  md0("")
  md0("## The three effort measures")
  md0("")
  md0(s"1. **Query revisions committed: $QUERY_REVISIONS_COMMITTED.** Convention: " +
    QUERY_REVISIONS_CONVENTION + ". This run introduces the file in a single commit.")
  md0(s"2. **Distinct Joern API constructs used: " +
    s"${JOERN_API_CONSTRUCTS.distinct.size}.** Listed explicitly and deduplicated so the")
  md0("   count is auditable from the list rather than asserted; every entry appears")
  md0("   literally in the query source:")
  md0("")
  JOERN_API_CONSTRUCTS.foreach(c => md0(s"   - `$c`"))
  md0("")
  md0(s"   Constructs used here that query 01 does not publish: " +
    (if (apiOnlyHereVs01.isEmpty) "none"
     else apiOnlyHereVs01.map(c => s"`$c`").mkString(", ")) + ". Constructs used here " +
    "that query 02 does not publish: " +
    (if (apiOnlyHereVs02.isEmpty) "none"
     else apiOnlyHereVs02.map(c => s"`$c`").mkString(", ")) + ".")
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
  md0(s"- named path `${cpgNamed}`" +
    (if (cpgIsLink) s", a symlink to `$cpgLinkTarget`" else ""))
  md0(s"- resolved target `$cpgResolved`, **$sizeFollow** bytes, sha256 `$shaObserved`")
  md0(s"- the link itself measures $sizeNoFollow bytes; that figure is recorded only to be")
  md0("  discarded, because measuring the link rather than its target is the mistake this")
  md0("  check exists to avoid")
  md0(s"- record of account: `$recordSelectedPath` (source: $recordSelectedSource), which")
  md0(s"  states bytes $recordedSize and sha256 `$recordedSha` - re-verified immediately")
  md0("  before the load, and a mismatch would have halted the run")
  md0(s"- repo-relative record `$CPG_RECORD_PATH` states bytes $defaultRecordedSize and")
  md0(s"  sha256 `$defaultRecordedSha`; agrees with the graph loaded: " +
    s"**$defaultRecordAgrees**")
  md0(s"- divergence: $identityDivergenceNote")
  md0(s"- the AAP-named path `$CPG_PATH_DEFAULT`: $aapNameReconciliation")
  md0("")
  md0("## Reproducing this")
  md0("")
  md0("```")
  md0("cd <a scratch directory outside the repository>")
  md0("HARNESS_REPO_ROOT=<repo> JAVA_HOME=\"$JAVA_HOME_21\" \\")
  md0("  JAVA_TOOL_OPTIONS=\"-Xmx64g\" SL_LOGGING_LEVEL=WARN \\")
  md0(s"  joern --script <repo>/queries/joern/$QUERY_ID.sc -J-Xmx64g < /dev/null")
  md0("```")
  md0("")
  md0("Both pairs are declared as named constants in the query source and both are")
  md0("invoked by that one command, so no per-pair parameter has to be passed on the")
  md0("command line and the second pair's invocation is reproducible from this record")
  md0("alone. Where the record of account above is not the repo-relative one, the")
  md0(s"variable `$CPG_RECORD_ENV_VAR` names it, and its value is the path printed above.")
  md0("")
  md0("`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so")
  md0("`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs")
  md0("at. The query measures the heap it received and stops below the floor: raising a")
  md0("heap is permitted and reported, lowering one is not.")
  md0("")

  writeUtf8(mdPath, md.mkString("", "\n", "\n"))
  log(s"prose report written      : $mdPath (${md.size} lines)")

  // -------------------------------------------------------------------------
  stage("N-result: the result region, emitted only now that every stage passed")
  // -------------------------------------------------------------------------
  log(s"total elapsed_ms          : ${elapsedMs(runStartNanos)}")
  log(MARKER_RESULT_BEGIN)
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
  log(s"duplicate_formulation     : $duplicateFormulationScalar")
  log(s"  vs $SIBLING_CALLGRAPH_QUERY: $duplicateVerdictAgainst01")
  log(s"  vs $SIBLING_DATAFLOW_QUERY : $duplicateVerdictAgainst02")
  log(s"joern_api_constructs      : ${JOERN_API_CONSTRUCTS.distinct.size}")
  log(s"query_revisions_committed : $QUERY_REVISIONS_COMMITTED")
  log(s"parameterizability        : $parameterizabilityVerdict " +
    s"(second pair ${secondPair.id} invoked: " +
    s"${secondTraversal.invoked && secondPairWalksRan})")
  log(s"graph                     : $sizeFollow bytes sha256=$shaObserved")
  log(s"graph_identity_record     : $recordSelectedPath ($recordSelectedSource)")
  log(s"envelope                  : $jsonPath")
  log(s"prose report              : $mdPath")
  log(MARKER_RESULT_END)
  log(MARKER_OK)
  flushConsoleLog()

} catch {
  case t: Throwable =>
    // No result region is emitted: a partial one looks like a completed run.
    println(MARKER_FAILURE)
    consoleLines += MARKER_FAILURE
    consoleLines += s"failing stage : $currentStage"
    consoleLines += s"exception     : ${t.getClass.getName}: ${t.getMessage}"
    System.err.println(s"$MARKER_FAILURE stage=$currentStage")
    System.err.println(s"exception: ${t.getClass.getName}: ${t.getMessage}")
    t.printStackTrace(System.err)
    flushConsoleLog()
    throw t
}

