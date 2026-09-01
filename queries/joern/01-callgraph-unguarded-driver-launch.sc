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
// queries/joern/01-callgraph-unguarded-driver-launch.sc
//
// Probe query 1 of 3. Hand-written Joern capability probe: CALL-GRAPH
// reachability from the Spark standalone Master's driver-submission handler to
// the privileged process launch in DriverRunner, over the code-property graph
// built from the pinned tree's bytecode.
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
// STAGE 5 POSITION
//   This is the fourth and last of the four heap-bound JVM invocations the run
//   records separately (frontend build, importCpg verification load, Stage 3
//   Joern runner, this probe). It runs after normalization so that only one
//   64 GB Joern process is ever live (AAP 0.5.1, 0.5.4).
//
// HOW TO INVOKE (the heap is the part that is easy to get wrong - see below)
//   cd <a scratch directory outside the repository>   # joern eagerly creates
//                                                    # ./workspace in its cwd
//   HARNESS_REPO_ROOT=<repo>  JAVA_HOME="$JAVA_HOME_21" \
//   JAVA_TOOL_OPTIONS="-Xmx64g" SL_LOGGING_LEVEL=WARN \
//     joern --script <repo>/queries/joern/01-callgraph-unguarded-driver-launch.sc \
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
//   the child then runs at the ergonomic default of 29.97 GiB. JAVA_TOOL_OPTIONS
//   is inherited by the child and is the environment override that actually
//   raises the heap the query runs at. This script therefore measures
//   Runtime.maxMemory() and HALTS below the floor: raising a heap is permitted
//   and reported, lowering one is not, because a truncated result's silence
//   cannot be told apart from a clean one (AAP 0.8.2).
//
// THE VERIFIED TARGET SURFACE
//   Every line number below was verified at commit
//   59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d with `git show <sha>:<path>`.
//   These paths resolve inside the PINNED clone exported as SPARK_SRC; the
//   working checkout this file lives in is neither built nor scanned.
//
//   HANDLER - core/src/main/scala/org/apache/spark/deploy/master/Master.scala
//             (1,436 lines at the pin)
//     :239  override def receive: PartialFunction[Any, Unit] = {
//     :409  override def receiveAndReply(context: RpcCallContext)
//             : PartialFunction[Any, Unit] = {          <-- the handler
//     :410    case RequestSubmitDriver(description) =>
//     :411      if (state != RecoveryState.ALIVE) {     <-- a RECOVERY-STATE
//                 check, NOT an authorization or ACL predicate. It is not part
//                 of the predicate set defined below and is not reported as one.
//     :417      val driver = createDriver(description)
//     :418      persistenceEngine.addDriver(driver)
//     :419      waitingDrivers += driver
//     :420      drivers.add(driver)
//     :421      schedule()
//     :923  private def canLaunchDriver(...)
//     :944  private def schedule(): Unit = {
//     :964/:983  canLaunchDriver call sites
//     :967/:986  launchDriver call sites
//     :1130 val newDriver = createDriver(driver.desc)
//     :1356 private def createDriver(desc: DriverDescription): DriverInfo = {
//     :1363 private def launchDriver(worker: WorkerInfo, driver: DriverInfo)
//     :1367   worker.endpoint.send(LaunchDriver(driver.id, driver.desc,
//                                               driver.resources))
//
//   SINK - core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala
//          (279 lines at the pin)
//     :47   private[deploy] class DriverRunner(
//     :56     val securityManager: SecurityManager,
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
//   RELAY - core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala
//           (1,046 lines at the pin)
//     :523  override def receive: PartialFunction[Any, Unit] = synchronized {
//     :687    case LaunchDriver(driverId, driverDesc, resources_) =>
//     :689      val driver = new DriverRunner(
//     :701      driver.start()
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
// FOUR BOUNDARIES ON THIS ROUTE - AND WHY A ZERO IS THE FINDING
//   The route from the handler to the launch is NOT call-graph-connected. Four
//   hops on it are not call edges, so a bounded reachability walk returns zero
//   routes. That zero is the capability finding - it is not a broken query, and
//   it is not repaired by loosening or removing the bound. Each boundary is
//   MEASURED against the graph below and reported with its hop and its reason.
//
//     B1 RPC HOP        Master.launchDriver :1367 sends LaunchDriver over an
//                       RpcEndpointRef; the receiving handler is in Worker. No
//                       call edge crosses a message send. Modelled explicitly
//                       here by pairing on the MESSAGE TYPE (constructor call
//                       sites versus field-accessor call sites of
//                       DeployMessages$LaunchDriver) - a modelling decision,
//                       stated so the route count stays interpretable.
//     B2 THREAD HOP     DriverRunner.start :123 calls Thread.start(); the body
//                       that continues the route is run() :90 on the anonymous
//                       Thread subclass. Thread.start() -> run() is a JVM
//                       scheduling relation, not a call edge.
//     B3 INTERFACE HOP  runCommandWithRetry :240 invokes the ABSTRACT
//                       ProcessBuilderLike.start :270; java.lang.ProcessBuilder
//                       .start() is reached only through the anonymous
//                       implementation at :276.
//     B4 PARTIAL-FN HOP receiveAndReply returns PartialFunction[Any, Unit], so
//                       its body compiles into a synthetic class. The entry
//                       point in the graph is
//                       Master$$anonfun$receiveAndReply$1.applyOrElse, NOT a
//                       method named receiveAndReply. Both are selected below
//                       and the selection is reported.
//
// OUTPUTS (slugs are locked; harness/artifacts/logs/probe-01-callgraph-
//          unguarded-driver-launch.log names exactly these two as consumers)
//   queries/joern/results/01-callgraph-unguarded-driver-launch.json  envelope
//   queries/joern/results/01-callgraph-unguarded-driver-launch.md    prose
//   harness/artifacts/logs/probe-01-callgraph-unguarded-driver-launch.log
//                                                                    console
//
//   Both result files are DETERMINISTIC: no timestamp, no elapsed time and no
//   workspace or project name enters them, so an unchanged source over an
//   unchanged graph emits byte-identical bytes. Elapsed times live in the
//   console log only.
//
// MARKER PROTOCOL (the shape queries 02 and 03 restate rather than reinvent)
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
// method, a call site or a route anywhere below: every selector the traversal,
// the boundary measurements and the predicate search use is a named constant
// declared in this block, so what the query looks for can be read off in one
// place and cited in the result without reading the traversal.
//
// The only literals below this block are the lexical tokens of the two text
// readers - the Scala string-literal scanner that extracts a sibling query's
// declared constants, and the identity-record reader - where the literal
// describes the syntax of the file being read rather than anything about the
// graph or the route surface.
// ===========================================================================

/** The slug. Both result filenames and the console log name derive from it. */
val QUERY_ID = "01-callgraph-unguarded-driver-launch"

/** The probe's own scratch workspace, repo-relative as the AAP names it. */
val WORKSPACE_PATH = "queries/joern/.workspace"

/** Repo-relative output paths. Resolved against the repository root below. */
val RESULTS_DIR = "queries/joern/results"
val LOG_DIR = "harness/artifacts/logs"

/** The graph, and the record that fixes its identity. */
val CPG_ENV_VAR = "HARNESS_CPG"
val CPG_PATH_DEFAULT = "harness/cpg/spark.cpg"
val CPG_RECORD_PATH = "harness/artifacts/logs/cpg-frontend.log"

/** The repository root, and the environment variable that names it. */
val REPO_ROOT_ENV_VAR = "HARNESS_REPO_ROOT"

// --------------------------------------------------------------- the bounds
/** Maximum call-graph hops walked from an entry point. */
val MAX_CALL_DEPTH = 12
/** Maximum distinct routes retained. */
val MAX_ROUTES = 64
/** Per-entry-point step cap: method expansions, not edges. Counted from zero at
 *  EACH entry point, which is the scope this name states and the scope the
 *  traversal enforces - a counter shared across entry points would publish a
 *  per-entry label over a per-walk quantity. */
val MAX_EXPANSIONS_PER_ENTRY = 200000
/** The whole-walk expansion budget: one walk's total across every entry point
 *  it traverses. Its value is the per-entry cap times the entry-point cap, and
 *  that relation is asserted at run time, so this budget bounds the walk in one
 *  number without ever pre-empting an entry point's own allowance. Both scopes
 *  are reported separately, under the names they enforce. */
val MAX_EXPANSIONS_PER_WALK = 3200000
/** Total-returns cap across every record kind this query emits. */
val MAX_TOTAL_RETURNS = 256
/** Maximum entry points traversed; the remainder are counted as truncated. */
val MAX_ENTRY_POINTS = 16
/** Cap on the indexed call-name sweeps used to find sink and message call sites. */
val MAX_CALL_SCAN = 200000

// ------------------------------------------------------------- the heap floor
/** 64 GiB. Measured, not requested: the query halts below this. */
val HEAP_FLOOR_BYTES = 64L * 1024L * 1024L * 1024L
/** The JDK major the pinned Joern release documents as its tested requirement. */
val REQUIRED_JDK_MAJOR = "21"

// ------------------------------------------------------ the entry-point surface
val HANDLER_TYPE = "org.apache.spark.deploy.master.Master"
val HANDLER_METHOD = "receiveAndReply"
/** BOUNDARY 4: the synthetic partial-function class the handler body compiles to. */
val ENTRY_SYNTHETIC_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.master\.Master\$\$anonfun\$receiveAndReply\$\d+$"""
val ENTRY_SYNTHETIC_METHOD = "applyOrElse"

// -------------------------------------------------------------- the sink surface
/** The privileged launch, as a CALLEE full name. */
val SINK_CALLEE_REGEX =
  """^(java\.lang\.ProcessBuilder\.start|org\.apache\.spark\.deploy\.worker\.ProcessBuilderLike\.start).*"""
/** The indexed call name both sink forms share. */
val SINK_CALL_NAME = "start"
/** The launch must be hosted by the DriverRunner surface, not by any `start` anywhere. */
val SINK_HOST_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.worker\.(DriverRunner|ProcessBuilderLike).*"""

// ------------------------------------------------------- the RPC message surface
val MESSAGE_TYPE = "org.apache.spark.deploy.DeployMessages$LaunchDriver"
val MESSAGE_CTOR_NAME = "<init>"
val MESSAGE_ACCESSOR_NAMES = List("driverDesc", "driverId", "resources")

// ------------------------------------------------------------- the thread surface
/** The abstract launch declaration the sink call site names, and the JDK method
 *  the concrete implementation reaches. Both are selectors: B3 measures whether
 *  the graph's call linker connects the interface invocation to an
 *  implementation, and it partitions the callees it observed on the first of
 *  these names and looks the second up by full name. Named here rather than
 *  written into the boundary measurement so that what B3 searches for is
 *  declared where every other selector is. */
val ABSTRACT_LAUNCH_CALLEE_PREFIX =
  "org.apache.spark.deploy.worker.ProcessBuilderLike.start"
val JDK_LAUNCH_METHOD_FULL_NAME = "java.lang.ProcessBuilder.start:java.lang.Process()"

/** The witness B4 uses to decide whether a handler method reaches the case
 *  bodies: the name of the call the handler's body makes. A source-level
 *  handler that constructs the partial function only will not name it, and the
 *  synthetic applyOrElse that holds the case bodies will. */
val HANDLER_BODY_WITNESS = "createDriver"

val THREAD_HOST_TYPE = "org.apache.spark.deploy.worker.DriverRunner"
val THREAD_HOST_METHOD = "start"
val THREAD_BODY_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.worker\.DriverRunner\$\$anon\$\d+$"""
val THREAD_BODY_METHOD = "run"

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

// --------------------------------------------------- the sibling query slugs
/** The other two queries of the probe. Their sources sit beside this one. */
val SIBLING_DATAFLOW_QUERY = "02-dataflow-unguarded-driver-launch"
val SIBLING_PARAMETERIZED_QUERY = "03-parameterized-handler-sink-pairs"
val SIBLING_QUERY_IDS = List(SIBLING_DATAFLOW_QUERY, SIBLING_PARAMETERIZED_QUERY)
/** Where a query source lives, repo-relative. This query reads its OWN source
 *  from here to digest it, and its siblings' to compare formulations. */
val QUERY_SOURCE_DIR = "queries/joern"

// ------------------------------------------------------ reproducing this run
/**
 * The COMPLETE command this query is reproduced by - every element it genuinely
 * needs and nothing it does not. Each element earns its place:
 *
 *   - the working directory is outside the repository because joern eagerly
 *     creates ./workspace in its own working directory and exposes no flag to
 *     move it, and nothing named workspace is ignored by the repository's root
 *     .gitignore;
 *   - HARNESS_REPO_ROOT is REQUIRED by that choice: with the working directory
 *     outside the repository, it is the only thing that tells the query where
 *     the graph, the identity record, the results directory and the log
 *     directory are;
 *   - JAVA_HOME selects the JDK major the pinned Joern release documents;
 *   - JAVA_TOOL_OPTIONS is what actually raises the heap, because joern
 *     --script forks a child JVM and does not forward -J-Xmx to it;
 *   - stdin is closed because joern's REPL blocks on an open one.
 *
 * No other environment variable changes what this query loads or what it
 * publishes: the graph path may be overridden by $HARNESS_CPG, and where it is
 * not, the repo-relative default is used and reported. There is no override for
 * the identity record - the record of account is the repo-relative one, so a
 * load is never adjudicated by a record the published command does not name.
 */
val REPRODUCTION_COMMAND =
  "cd <a scratch directory outside the repository> && " +
    REPO_ROOT_ENV_VAR + "=<the repository root> JAVA_HOME=\"$JAVA_HOME_21\" " +
    "JAVA_TOOL_OPTIONS=-Xmx64g SL_LOGGING_LEVEL=WARN joern --script " +
    "<the repository root>/" + QUERY_SOURCE_DIR + "/" + QUERY_ID +
    ".sc -J-Xmx64g < /dev/null"

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
 * FORMULATION_BOUND_VALUE repeats the bound as a bare literal because the
 * comparison reads the sibling's value out of its TEXT and must read this
 * query's the same way. It is asserted against MAX_CALL_DEPTH at run time, so
 * the repetition cannot drift unnoticed.
 */
val FORMULATION_EDGE_KINDS = List("CALL")
val FORMULATION_END_NODE_KINDS = List("METHOD")
val FORMULATION_PAIR_IDS = List("pair-one")
val FORMULATION_BOUND_NAME = "MAX_CALL_DEPTH"
val FORMULATION_BOUND_KIND = "call-graph hops expanded from an entry point"
val FORMULATION_BOUND_VALUE = 12
val FORMULATION_TRAVERSAL_SEMANTICS =
  "reachability over CALL edges, selecting whole METHOD nodes as its ends"
/** The constants that select the entry points, the sink and the predicate set,
 *  named rather than repeated: the comparison extracts each source's own
 *  literals by these names and compares literal TEXT with literal text, so no
 *  unescaping step can make two equal selectors look different. */
val FORMULATION_ENTRY_SELECTOR_CONSTANT_NAMES = List(
  "HANDLER_TYPE",
  "HANDLER_METHOD",
  "ENTRY_SYNTHETIC_TYPE_REGEX",
  "ENTRY_SYNTHETIC_METHOD")
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

// ---------------------------------------------- the route surface at the pin
/**
 * Every source anchor this query's report cites, quoted from the PINNED tree at
 * the commit named below and re-verified against that tree. Named here so that
 * no inline literal governs the report, and so a reader checks each one against
 * the pinned tree rather than against this checkout: the two differ on
 * Worker.scala by a uniform +11, which is exactly the kind of difference a
 * silent "correction" would introduce.
 */
val PINNED_COMMIT = "59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d"
val MASTER_SOURCE_FILE =
  "core/src/main/scala/org/apache/spark/deploy/master/Master.scala"
val WORKER_SOURCE_FILE =
  "core/src/main/scala/org/apache/spark/deploy/worker/Worker.scala"
val DRIVER_RUNNER_SOURCE_FILE =
  "core/src/main/scala/org/apache/spark/deploy/worker/DriverRunner.scala"
val DEPLOY_MESSAGE_SOURCE_FILE =
  "core/src/main/scala/org/apache/spark/deploy/DeployMessage.scala"
/** Ordered by route position, so the object reads as the route does. */
val ROUTE_SURFACE_ANCHORS: List[(String, String)] = List(
  "handler" ->
    (MASTER_SOURCE_FILE + ":409 override def receiveAndReply(context: RpcCallContext)" +
      ": PartialFunction[Any, Unit]"),
  "handler_case" ->
    (MASTER_SOURCE_FILE + ":410 case RequestSubmitDriver(description) =>"),
  "recovery_state_check" ->
    (MASTER_SOURCE_FILE + ":411 if (state != RecoveryState.ALIVE) - a recovery-state " +
      "check, outside the predicate set defined above and not reported as one"),
  "alive_branch" ->
    (MASTER_SOURCE_FILE + ":415 } else { - the branch taken when the state is ALIVE, " +
      "which is the branch that continues"),
  "create_driver_call" ->
    (MASTER_SOURCE_FILE + ":417 val driver = createDriver(description)"),
  "schedule_call" -> (MASTER_SOURCE_FILE + ":421 schedule()"),
  "create_driver_def" ->
    (MASTER_SOURCE_FILE + ":1356 private def createDriver(desc: DriverDescription)" +
      ": DriverInfo"),
  "schedule_def" -> (MASTER_SOURCE_FILE + ":944 private def schedule(): Unit"),
  "can_launch_driver_def" ->
    (MASTER_SOURCE_FILE + ":923 private def canLaunchDriver(worker: WorkerInfo, " +
      "desc: DriverDescription): Boolean, called at :964 and :983"),
  "launch_driver_def" ->
    (MASTER_SOURCE_FILE + ":1363 private def launchDriver(worker: WorkerInfo, " +
      "driver: DriverInfo): Unit"),
  "rpc_send" ->
    (MASTER_SOURCE_FILE + ":1367 worker.endpoint.send(LaunchDriver(driver.id, " +
      "driver.desc, driver.resources)) - the RPC boundary"),
  "second_entry_path" ->
    (MASTER_SOURCE_FILE + ":1121 private def relaunchDriver reaches the same " +
      "createDriver at " + MASTER_SOURCE_FILE + ":1130"),
  "message_type" ->
    (DEPLOY_MESSAGE_SOURCE_FILE + ":176 case class LaunchDriver, inside the object " +
      "DeployMessages declared at " + DEPLOY_MESSAGE_SOURCE_FILE + ":34, so the " +
      "bytecode type the query selects on is " + MESSAGE_TYPE),
  "relay_receive" ->
    (WORKER_SOURCE_FILE + ":523 override def receive: PartialFunction[Any, Unit] = " +
      "synchronized"),
  "relay_case" ->
    (WORKER_SOURCE_FILE + ":687 case LaunchDriver(driverId, driverDesc, resources_) " +
      "=>, constructing a DriverRunner at :689 and calling driver.start() at :701"),
  "thread_hop" ->
    (DRIVER_RUNNER_SOURCE_FILE + ":123 }.start(), closing the Thread opened at :89 " +
      "whose run() body is at :90"),
  "sink" ->
    (DRIVER_RUNNER_SOURCE_FILE + ":240 process = Some(command.start()) - the " +
      "privileged process launch this query takes as its sink"),
  "sink_abstract_declaration" ->
    (DRIVER_RUNNER_SOURCE_FILE + ":270 def start(): Process, on the trait declared " +
      "at :269"),
  "sink_concrete_implementation" ->
    (DRIVER_RUNNER_SOURCE_FILE + ":276 override def start(): Process = " +
      "processBuilder.start(), the anonymous implementation created at :275"))
val ROUTE_SURFACE_LAUNCH_DRIVER_CALLS = List(
  MASTER_SOURCE_FILE + ":967 launchDriver(worker, driver)",
  MASTER_SOURCE_FILE + ":986 launchDriver(worker, driver)")
/** The four hops the route crosses that no CALL edge can join, in boundary-id
 *  order, each stated as the reason rather than as an assertion. */
val ROUTE_SURFACE_NON_CONNECTIVITY_HOPS = List(
  "B1 rpc: " + MASTER_SOURCE_FILE + ":1367 sends the message over an RpcEndpointRef " +
    "and " + WORKER_SOURCE_FILE + ":523 / :687 receives it. A message send is not a " +
    "call, so no CALL edge joins the two ends",
  "B2 thread: " + DRIVER_RUNNER_SOURCE_FILE + ":123 calls Thread.start() and the " +
    "route continues in the run() body at :90 on another thread. Thread.start() to " +
    "run() is a JVM scheduling relation, not a call",
  "B3 interface: the launch call site invokes the abstract ProcessBuilderLike.start " +
    "declared at " + DRIVER_RUNNER_SOURCE_FILE + ":270, and java.lang.ProcessBuilder" +
    ".start() is reached only through the anonymous implementation at :276",
  "B4 partial function: the handler at " + MASTER_SOURCE_FILE + ":409 returns a " +
    "PartialFunction, so the case bodies compile into a synthetic class and the graph " +
    "entry point is the synthetic " + ENTRY_SYNTHETIC_METHOD + " rather than any " +
    "method named " + HANDLER_METHOD)
val ROUTE_SURFACE_WORKING_CHECKOUT_OFFSET =
  "these are the PINNED tree line numbers. The working checkout this envelope is " +
    "committed in differs on " + WORKER_SOURCE_FILE + " by a uniform +11 - receive at " +
    ":534 and case LaunchDriver at :698 there - so the pinned values above must not be " +
    "\"corrected\" against it. The other anchors coincide, which is what makes the two " +
    "that do not easy to get wrong"

// ------------------------------------------------------------- effort measures
/**
 * Effort measure 1 - query revisions committed. Convention: the number of git
 * commits touching THIS .sc path, from its first appearance to the end of the
 * probe. The count is MEASURED at run time from the repository's own history
 * (stage A) and published together with the commit list, never written down
 * here: a hard-coded revision count is a figure that stops being true the next
 * time the file is committed.
 */
val QUERY_REVISIONS_CONVENTION =
  "commits touching queries/joern/" + QUERY_ID +
    ".sc from its first appearance to the end of the probe, counted at run time " +
    "from the repository's own history. The commit that publishes these result " +
    "files is necessarily NOT among them: it cannot exist while the run that " +
    "writes them is still in progress"
/** How the revision count is measured. The command is named rather than
 *  inlined, it is given a bound so a stuck child cannot stall the probe, and
 *  its output is validated against the shape a commit identifier has. */
val GIT_EXECUTABLE = "git"
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
 * Effort measure 3 - parameterizability. NOT claimed here: it is proven by
 * query 03 actually invoking its parameterized form on the second named
 * handler/sink pair (the deploy/rest/StandaloneRestServer handler to the
 * deploy/worker/DriverRunner sink) and capturing that invocation's result.
 * This query references that and asserts nothing about it.
 */
val PARAMETERIZABILITY_OWNER = "03-parameterized-handler-sink-pairs"

// -------------------------------------------------------------------- markers
val MARKER_START = "---BLITZY-START---"
val MARKER_RESULT_BEGIN = "---BLITZY-RESULT-BEGIN---"
val MARKER_RESULT_END = "---BLITZY-RESULT-END---"
val MARKER_OK = "---BLITZY-OK---"
val MARKER_FAILURE = "---BLITZY-FAILURE---"

// ===========================================================================
// CONSOLE, STAGE TRACKING AND FAIL-LOUD HELPERS
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
 * and the graph. The latter answers "which run produced this"; only the former
 * can answer "is this set complete and self-consistent", because an identifier
 * computed before the members exist cannot depend on them.
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

/**
 * Validate where a member is about to be published and return the parent
 * directory's real path. Refuses rather than repairs: a link at the target or
 * at its parent, or a parent that resolves outside the repository root, stops
 * the run instead of writing somewhere the record does not name.
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
  val channel = java.nio.channels.FileChannel.open(
    p, java.nio.file.StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
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

/**
 * Write one member to a private temporary beside its target, fsync it, and
 * remember it. Nothing is visible at the target until the whole set publishes.
 */
def stageMember(target: Path, content: String): StagedMember = {
  val realParent = publicationParentOf(target)
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
    val candidate = realParent.resolve(
      PUBLICATION_TEMP_PREFIX + target.getFileName.toString + "." +
        suffix.map("%02x".format(_)).mkString + PUBLICATION_TEMP_SUFFIX)
    try {
      channel = java.nio.channels.FileChannel.open(
        candidate,
        java.nio.file.StandardOpenOption.CREATE_NEW,
        java.nio.file.StandardOpenOption.WRITE,
        LinkOption.NOFOLLOW_LINKS)
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
      try Files.deleteIfExists(temp)
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
    try Files.deleteIfExists(temp) catch { case _: Throwable => () }
    abortRun(s"the staged temporary for $target does not hold the bytes that were " +
      s"written: measured $stagedSize bytes / sha256 $stagedDigest against an " +
      s"intended ${bytes.length} bytes / sha256 $intendedDigest. Nothing is published")
  }
  val member = StagedMember(target, temp, stagedSize, stagedDigest)
  stagedMembers += member
  member
}

/** Remove every staged temporary. Called when a set will not be published, so
 *  a failure leaves neither a mixed generation nor litter behind. */
def discardStagedMembers(): Unit = {
  stagedMembers.toList.foreach { m =>
    try Files.deleteIfExists(m.temp)
    catch {
      case t: Throwable =>
        System.err.println(s"could not remove the staged temporary ${m.temp}: " +
          s"${t.getMessage}")
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
    val ch = java.nio.channels.FileChannel.open(
      dir, java.nio.file.StandardOpenOption.READ)
    try ch.force(true) finally ch.close()
  } catch {
    case t: Throwable =>
      val note = s"could not fsync the publication directory $dir: ${t.getMessage}"
      System.err.println(note)
      consoleLines += s"[publication] $note"
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
  if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
    abortRun(s"the completion manifest $path is absent or is not a regular file, so " +
      "the published set cannot be established as one generation. A publication that " +
      "failed between its renames leaves exactly this state")
  }
  val text = new String(
    java.nio.file.Files.readAllBytes(path), StandardCharsets.UTF_8)
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
    if (!Files.isRegularFile(abs, LinkOption.NOFOLLOW_LINKS)) {
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
 * member exists, so it identifies the run and can say nothing about whether the
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

  contentMembers.foreach { m =>
    Files.move(m.temp, m.target, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
  }

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
    Files.move(m.temp, m.target, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
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
def gitRevisionsOf(root: Path, repoRelativePath: String): (Boolean, String, List[String]) = {
  val argv = new java.util.ArrayList[String]()
  List(GIT_EXECUTABLE, "-C", root.toString, "log", "--format=%H", "--", repoRelativePath)
    .foreach(argv.add)
  try {
    val builder = new java.lang.ProcessBuilder(argv)
    builder.redirectErrorStream(true)
    val proc = builder.start()
    proc.getOutputStream.close()
    val exited = proc.waitFor(GIT_WAIT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
    if (!exited) {
      proc.destroyForcibly()
      (false,
        s"not established: $GIT_EXECUTABLE did not exit within $GIT_WAIT_SECONDS " +
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
          s"not established: $GIT_EXECUTABLE exited $code for $repoRelativePath" +
            (if (quoted.isEmpty) "" else s", saying: $quoted"),
          Nil)
      } else {
        val commits = out.linesIterator.map(_.trim)
          .filter(_.matches(GIT_COMMIT_SHA_REGEX)).toList
        (true,
          "measured from the repository's own history at run time, newest first",
          commits)
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
      println(s"console log written: $p")
    } catch {
      case t: Throwable =>
        discardStagedMembers()
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
  manifestTargetPath =
    Some(repoRoot.resolve(LOG_DIR).resolve(s"probe-$QUERY_ID.publication.json"))
  log(s"completion manifest       : ${manifestTargetPath.get}")
  log(s"console log target        : ${logTargetPath.get}")

  // Every member this query publishes must land inside the repository root as
  // it REALLY is, with every symbolic link on the way already resolved. Holding
  // the resolved root here lets each write check that its own target's resolved
  // parent is still inside it, so a link planted at or above a publication path
  // cannot redirect a write out of the tree the run is publishing into.
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
  // The heap above is the evidence, and it is measured rather than taken from
  // the argument list. The argument list itself is reported under the whitelist
  // policy: a memory or stack flag is logged as written, and every other
  // argument is reduced to its key with the value replaced. These console
  // streams are preserved verbatim as evidence, so an argument value nobody
  // whitelisted must never reach one.
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
      "is not, because a truncated result's silence cannot be told apart from a clean one")
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
  // The walk budget's value is the per-entry cap times the entry-point cap, so
  // it bounds a whole walk without ever pre-empting an entry point's own
  // allowance. The relation is asserted rather than left as a coincidence of
  // three literals.
  if (MAX_EXPANSIONS_PER_WALK != MAX_EXPANSIONS_PER_ENTRY * MAX_ENTRY_POINTS) {
    abortRun(s"MAX_EXPANSIONS_PER_WALK=$MAX_EXPANSIONS_PER_WALK is not " +
      s"MAX_EXPANSIONS_PER_ENTRY ($MAX_EXPANSIONS_PER_ENTRY) times MAX_ENTRY_POINTS " +
      s"($MAX_ENTRY_POINTS). The two expansion scopes are published separately and the " +
      "walk budget is documented as the product, so a walk budget below it would " +
      "silently pre-empt the per-entry cap it is meant to sit above")
  }
  log(s"expansion scopes          : per entry point $MAX_EXPANSIONS_PER_ENTRY, per " +
    s"walk $MAX_EXPANSIONS_PER_WALK (= per-entry cap x entry-point cap)")

  // Effort measure 1, measured rather than declared: the commits touching this
  // source. The list is published beside the count so the number is auditable.
  val (revisionsEstablished, revisionsNote, revisionCommits) =
    gitRevisionsOf(repoRoot, sourceRepoRelative)
  log(s"query revisions committed : " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established") +
    s" ($revisionsNote)")
  if (revisionsEstablished && revisionCommits.nonEmpty) {
    log(s"query revision commits    : ${revisionCommits.mkString(", ")}")
  }

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

  // The pair recorded at write time, parsed out of the frontend's own record.
  val recordPath = repoRoot.resolve(CPG_RECORD_PATH)
  if (!Files.isRegularFile(recordPath)) {
    abortRun(s"the graph identity record is missing: $recordPath. The pair recorded at " +
      "write time is what every later load re-verifies, so there is nothing to verify " +
      "against")
  }
  val recordText = new String(Files.readAllBytes(recordPath), StandardCharsets.UTF_8)
  val sizeLineRe = """(?i)^\s*(?:bytes|byte size)\s*:\s*(\d+)\s*$""".r
  val shaLineRe = """(?i)^\s*sha256\s*:\s*([0-9a-fA-F]{64})\s*$""".r
  val recordedSizes = recordText.linesIterator.collect {
    case sizeLineRe(v) => v
  }.toList.distinct
  val recordedShas = recordText.linesIterator.collect {
    case shaLineRe(v) => v.toLowerCase
  }.toList.distinct
  if (recordedSizes.size != 1 || recordedShas.size != 1) {
    abortRun(s"$CPG_RECORD_PATH does not state one unambiguous identity pair: " +
      s"byte sizes found = ${recordedSizes.mkString(",")}, sha256 values found = " +
      s"${recordedShas.mkString(",")}. A record that states two identities cannot " +
      "adjudicate a load")
  }
  val recordedSize = recordedSizes.head.toLong
  val recordedSha = recordedShas.head
  log(s"recorded at write time    : bytes=$recordedSize sha256=$recordedSha")
  log(s"recorded in               : $CPG_RECORD_PATH")

  val sizeMatches = sizeFollow == recordedSize
  val shaMatches = shaObserved == recordedSha
  log(s"byte size matches         : ${if (sizeMatches) "YES" else "NO"}")
  log(s"sha256 matches            : ${if (shaMatches) "YES" else "NO"}")
  if (!(sizeMatches && shaMatches)) {
    abortRun("graph identity mismatch: observed bytes=" + sizeFollow + " sha256=" +
      shaObserved + " against recorded bytes=" + recordedSize + " sha256=" + recordedSha +
      ". A load against different bytes than the record describes produces conclusions " +
      "about a graph nobody has")
  }

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

  // ------------------------- portable labels for the written artefacts ------
  // The envelope and the prose report are held to byte-identity: an unchanged
  // source over an unchanged graph must emit the same bytes from any checkout.
  // An absolute host path cannot appear in either, because the clone root is a
  // property of the checkout rather than of the measurement, so the same graph
  // reached through two clones would otherwise produce two different files.
  // Nothing is lost: the literals stay in the console stream, which is
  // deliberately not held to byte-identity, and the size-and-digest pair - not
  // a path - is what the identity comparison turns on.
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

  // -------------------------------------------------------------------------
  stage("D-load: switchWorkspace then importCpg")
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
  stage("E-selection: entry points (BOUNDARY 4) and the sink")
  // -------------------------------------------------------------------------
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

  // BOUNDARY 4: the handler body compiles into a synthetic partial-function
  // class, so the graph's entry point is that class's applyOrElse. The
  // source-level method of the same name is ALSO selected, so the report can
  // show what each of the two actually contains.
  val syntheticTypeDecls = cpg.typeDecl.fullName(ENTRY_SYNTHETIC_TYPE_REGEX).l
  val syntheticEntryNodes = syntheticTypeDecls
    .flatMap(_.method.l)
    .filter(_.name == ENTRY_SYNTHETIC_METHOD)
  val sourceLevelHandlerNodes = cpg.typeDecl
    .fullNameExact(HANDLER_TYPE)
    .method
    .nameExact(HANDLER_METHOD)
    .l
  val entryGroups: List[(String, List[Method])] =
    (syntheticEntryNodes ++ sourceLevelHandlerNodes)
      .groupBy(_.fullName)
      .toList
      .sortBy(_._1)
  val entryPointsDiscovered = entryGroups.size
  val entryGroupsTraversed = entryGroups.take(MAX_ENTRY_POINTS)
  val entryPointsTraversed = entryGroupsTraversed.size
  val entryPointsTruncated = entryPointsDiscovered - entryPointsTraversed
  log(s"synthetic typeDecls       : ${syntheticTypeDecls.size} matching " +
    ENTRY_SYNTHETIC_TYPE_REGEX)
  log(s"entry points discovered   : $entryPointsDiscovered")
  log(s"entry points traversed    : $entryPointsTraversed (cap $MAX_ENTRY_POINTS)")
  log(s"entry points truncated    : $entryPointsTruncated")
  entryGroups.foreach { case (fn, nodes) =>
    log(s"  entry: $fn  nodes=${nodes.size} graph_line=${lineOfMethod(nodes.head)}")
  }
  if (entryPointsDiscovered == 0) {
    abortRun("no entry point was selected: neither the synthetic partial-function " +
      s"class matching $ENTRY_SYNTHETIC_TYPE_REGEX nor $HANDLER_TYPE.$HANDLER_METHOD " +
      "is present in the graph")
  }

  // The sink: the privileged launch, taken from its indexed call name and then
  // constrained to the DriverRunner surface so an unrelated `start` elsewhere in
  // the graph cannot stand in for it.
  val startCallsScanned = cpg.call.nameExact(SINK_CALL_NAME).take(MAX_CALL_SCAN).l
  val sinkScanTruncated = startCallsScanned.size >= MAX_CALL_SCAN
  val sinkCallsAll = startCallsScanned.filter(_.methodFullName.matches(SINK_CALLEE_REGEX))
  val sinkCalls = sinkCallsAll
    .filter(c => owningTypes(c.method).exists(_.matches(SINK_HOST_TYPE_REGEX)))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val sinkHostNames = sinkCalls.map(_.method.fullName).distinct.sorted.toSet
  log(s"calls named $SINK_CALL_NAME scanned : ${startCallsScanned.size} " +
    s"(cap $MAX_CALL_SCAN, truncated=$sinkScanTruncated)")
  log(s"launch call sites (any host): ${sinkCallsAll.size}")
  log(s"launch call sites (sink host): ${sinkCalls.size}")
  sinkCalls.foreach { c =>
    log(s"  sink: ${c.method.fullName} -> ${c.methodFullName} graph_line=${lineOf(c)} " +
      s"dispatch=${c.dispatchType}")
  }
  log(s"sink host methods         : ${sinkHostNames.toList.sorted.mkString(", ")}")
  if (sinkCalls.isEmpty) {
    abortRun("no privileged-launch call site was found on the sink surface: no call " +
      s"matching $SINK_CALLEE_REGEX is hosted by a type matching $SINK_HOST_TYPE_REGEX")
  }

  // -------------------------------------------------------------------------
  stage("F-predicates: the selector, its bytecode collision, and the constraint")
  // -------------------------------------------------------------------------
  // The broad anchored selector is the one the AAP names. On BYTECODE it matches
  // more than the five source-level predicates, because Scala compiles
  // `private var aclsOn` (SecurityManager.scala:59) into accessors, so the graph
  // carries aclsOn() AND aclsOn_$eq(boolean) and both satisfy the `acls.*`
  // alternative. The constraint chain is therefore three steps and ALL THREE
  // SETS ARE REPORTED, so the narrowing is auditable rather than asserted:
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
  // the expected-spurious question and it is measured, never inferred.
  val predicateCallSites = predicateFinal
    .flatMap(_.callIn.l)
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  def onRouteSurface(m: Method): Boolean =
    owningTypes(m).exists(t => ROUTE_SURFACE_TYPE_PREFIXES.exists(p => t.startsWith(p)))
  val predicateCallSitesOnRoute = predicateCallSites.filter(c => onRouteSurface(c.method))
  val predicateCallerNames = predicateCallSites.map(_.method.fullName).distinct.sorted
  log(s"predicate call sites (graph-wide): ${predicateCallSites.size} in " +
    s"${predicateCallerNames.size} distinct callers")
  log(s"predicate call sites on the route surface " +
    s"(${ROUTE_SURFACE_TYPE_PREFIXES.mkString(", ")}): ${predicateCallSitesOnRoute.size}")
  predicateCallSitesOnRoute.foreach { c =>
    log(s"  on-route predicate call: ${c.method.fullName} -> ${c.methodFullName} " +
      s"graph_line=${lineOf(c)}")
  }

  // -------------------------------------------------------------------------
  stage("G-traversal: two bounded call-graph walks, entry points to the sink")
  // -------------------------------------------------------------------------
  /**
   * A call site whose resolved callee set is wider than this is recorded as a
   * dynamic-dispatch FAN-OUT site. Expanding, say, scala.Function1.apply models
   * "any lambda in the program may be invoked here", which is a property of the
   * call linker rather than of this route. Walk A follows those sites anyway;
   * walk B records them and does not. Both walks are reported and their routes
   * are deduplicated, never summed.
   */
  val FANOUT_CALLEE_THRESHOLD = 32

  final case class Hop(fromMethod: String, callSite: String, callSiteLine: Int, toMethod: String)
  final case class RouteRecord(walkId: String, entryPoint: String, sinkHost: String, hops: List[Hop])
  final case class WalkResult(
      walkId: String,
      followsFanOut: Boolean,
      entryPointsTraversed: Int,
      expansions: Int,
      maxExpansionsAtOneEntry: Int,
      methodsVisited: Int,
      callSitesConsidered: Int,
      fanOutSitesEncountered: Int,
      fanOutSitesNotFollowed: Int,
      maxDepthUsed: Int,
      depthBoundReached: Boolean,
      entryExpansionCapReached: Boolean,
      walkExpansionBudgetExhausted: Boolean,
      routeCapReached: Boolean,
      routes: List[RouteRecord])

  def walk(walkId: String, followFanOut: Boolean): WalkResult = {
    var methodsVisited = 0
    // Two expansion counters, at the two scopes this query publishes. The
    // per-entry counter is reset at every entry point, so the cap named
    // MAX_EXPANSIONS_PER_ENTRY is enforced at exactly the scope its name and
    // the reported field state. The walk total is kept separately, under its
    // own name, so neither figure has to stand in for the other.
    var expansions = 0
    var maxExpansionsAtOneEntry = 0
    var callSitesConsidered = 0
    var fanOutEncountered = 0
    var fanOutNotFollowed = 0
    var maxDepthUsed = 0
    var depthBoundReached = false
    var entryCapReached = false
    var walkBudgetExhausted = false
    var routeCapReached = false
    val routes = scala.collection.mutable.ArrayBuffer.empty[RouteRecord]

    entryGroupsTraversed.foreach { case (entryName, entryNodes) =>
      val visited = scala.collection.mutable.HashSet[String](entryName)
      val parent = scala.collection.mutable.HashMap.empty[String, Hop]
      var frontier: List[(String, List[Method])] = List(entryName -> entryNodes)
      var depth = 0
      var stop = false
      var entryExpansions = 0
      while (frontier.nonEmpty && depth < MAX_CALL_DEPTH && !stop) {
        val nextByName = scala.collection.mutable.LinkedHashMap.empty[String, List[Method]]
        val ordered = frontier.sortBy(_._1)
        var i = 0
        while (i < ordered.size && !stop) {
          val (fromName, fromNodes) = ordered(i)
          i += 1
          if (entryExpansions >= MAX_EXPANSIONS_PER_ENTRY) {
            entryCapReached = true
            stop = true
          } else if (expansions >= MAX_EXPANSIONS_PER_WALK) {
            walkBudgetExhausted = true
            stop = true
          } else {
            entryExpansions += 1
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
                    if (sinkHostNames.contains(toName) &&
                      !routes.exists(r => r.walkId == walkId && r.entryPoint == entryName &&
                        r.sinkHost == toName)) {
                      if (routes.size >= MAX_ROUTES) routeCapReached = true
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
                        routes += RouteRecord(walkId, entryName, toName, chain.toList)
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
      if (entryExpansions > maxExpansionsAtOneEntry) maxExpansionsAtOneEntry = entryExpansions
      log(f"  walk $walkId%-18s entry=$entryName visited=${visited.size}%8d " +
        f"depth=$depth%2d entry_expansions=$entryExpansions%8d " +
        f"walk_expansions=$expansions%8d")
    }

    WalkResult(walkId, followFanOut, entryPointsTraversed, expansions,
      maxExpansionsAtOneEntry, methodsVisited, callSitesConsidered, fanOutEncountered,
      fanOutNotFollowed, maxDepthUsed, depthBoundReached, entryCapReached,
      walkBudgetExhausted, routeCapReached, routes.toList)
  }

  val walkNanos = System.nanoTime()
  val walkA = walk("A-follows-fan-out", followFanOut = true)
  log(s"walk A elapsed_ms         : ${elapsedMs(walkNanos)}")
  val walkBNanos = System.nanoTime()
  val walkB = walk("B-fan-out-recorded", followFanOut = false)
  log(s"walk B elapsed_ms         : ${elapsedMs(walkBNanos)}")
  val walks = List(walkA, walkB)
  walks.foreach { w =>
    log(s"walk ${w.walkId}: routes=${w.routes.size} walk_expansions=${w.expansions} " +
      s"max_expansions_at_one_entry=${w.maxExpansionsAtOneEntry} " +
      s"call_sites=${w.callSitesConsidered} fanout_seen=${w.fanOutSitesEncountered} " +
      s"fanout_not_followed=${w.fanOutSitesNotFollowed} max_depth=${w.maxDepthUsed} " +
      s"depth_bound_reached=${w.depthBoundReached} " +
      s"entry_expansion_cap_reached=${w.entryExpansionCapReached} " +
      s"walk_expansion_budget_exhausted=${w.walkExpansionBudgetExhausted} " +
      s"route_cap_reached=${w.routeCapReached}")
  }

  /** Distinct routes across both walks: deduplicated on the hop sequence, never
   *  summed. Two walks over one handler/sink pair are two formulations of the
   *  same question, so their returns are reported side by side and counted once. */
  /** One route's hop sequence as a single ordered string, used both as part of
   *  the identity a route is deduplicated on and as the last component of the
   *  sort key. Every field the record publishes for a hop is in it, in the
   *  order the record publishes them, so two routes that differ anywhere in
   *  their hops differ here. */
  def hopSequenceKey(r: RouteRecord): String =
    r.hops.map(h => s"${h.fromMethod}|${h.callSite}|${h.callSiteLine}|${h.toMethod}")
      .mkString(">>")

  val distinctRoutes = walks
    .flatMap(_.routes)
    // Deduplicated on hopSequenceKey rather than on a hand-rolled triple, so the
    // identity a route is collapsed on is exactly the identity the sort key and
    // the published record use. The triple this replaced omitted callSiteLine,
    // which meant two routes crossing the SAME caller/callee pair at two
    // DIFFERENT source lines were collapsed into one before the sort ever saw
    // them - and the comment above hopSequenceKey claimed every published hop
    // field participated. One key, used everywhere, is what makes that true.
    .distinctBy(r => (r.entryPoint, r.sinkHost, hopSequenceKey(r)))
    // A TOTAL sort over the whole of what a route record publishes, in the
    // order it publishes it: the walk that returned it, its two endpoints, its
    // hop count and then its complete ordered hop sequence. Sorting on the
    // endpoints and the hop count alone left two routes that share all three
    // in whichever order the traversal happened to produce them, which is not
    // a reproducible order however stable the sort itself is.
    .sortBy(r => (r.walkId, r.entryPoint, r.sinkHost, r.hops.size, hopSequenceKey(r)))
  val boundReached = walks.exists(w =>
    w.depthBoundReached || w.entryExpansionCapReached ||
      w.walkExpansionBudgetExhausted || w.routeCapReached)
  log(s"distinct routes (both walks, deduplicated): ${distinctRoutes.size}")
  log(s"any bound reached         : $boundReached")

  // -------------------------------------------------------------------------
  stage("H-boundaries: each of the four hops, measured against the graph")
  // -------------------------------------------------------------------------
  final case class BoundaryRecord(
      id: String,
      hop: String,
      fromEnd: String,
      toEnd: String,
      reason: String,
      modelling: String,
      crossedByACallEdge: Boolean,
      measured: List[(String, String)])

  // --- B1: the RPC hop, modelled by pairing on the message type --------------
  val messageTypeDecls = cpg.typeDecl.fullNameExact(MESSAGE_TYPE).l
  val messageMethods = messageTypeDecls.flatMap(_.method.l)
  val messageCtors = messageMethods.filter(_.name == MESSAGE_CTOR_NAME)
  val messageAccessors = messageMethods.filter(m => MESSAGE_ACCESSOR_NAMES.contains(m.name))
  /** Call sites inside the message type or its companion are the case class's own
   *  generated machinery (apply, copy, unapply, equals, productElement), not a
   *  send or a receive. They are excluded by owning type so the producer and
   *  consumer sets are the two real ends of the hop. */
  val messageOwnTypes = Set(MESSAGE_TYPE, MESSAGE_TYPE + "$")
  def outsideMessageType(m: Method): Boolean =
    !owningTypes(m).exists(messageOwnTypes.contains)
  val messageProducerSites = messageCtors
    .flatMap(_.callIn.l)
    .filter(c => outsideMessageType(c.method))
    .distinctBy(c => (c.method.fullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, lineOf(c)))
  val messageConsumerSites = messageAccessors
    .flatMap(_.callIn.l)
    .filter(c => outsideMessageType(c.method))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val messageProducers = messageProducerSites.map(_.method.fullName).distinct.sorted
  val messageConsumers = messageConsumerSites.map(_.method.fullName).distinct.sorted
  val producerToConsumerEdge = messageProducerSites
    .map(_.method)
    .distinctBy(_.fullName)
    .exists { p =>
      callSitesOf(List(p)).flatMap(calleesOf).map(_.fullName).exists(messageConsumers.contains)
    }
  log(s"B1 message producers      : ${messageProducers.mkString(", ")}")
  log(s"B1 message consumers      : ${messageConsumers.mkString(", ")}")
  log(s"B1 producer->consumer call edge: $producerToConsumerEdge")
  val boundaryB1 = BoundaryRecord(
    id = "B1-rpc",
    hop = "RpcEndpointRef.send of " + MESSAGE_TYPE + ", Master to Worker",
    fromEnd = messageProducers.mkString(", "),
    toEnd = messageConsumers.mkString(", "),
    reason = "a message send is not a call: the sender enqueues a value on an " +
      "endpoint reference and the receiving handler is dispatched later, so no CALL " +
      "edge joins the two ends",
    modelling = "modelled explicitly by pairing on the MESSAGE TYPE - call sites of " +
      MESSAGE_TYPE + "." + MESSAGE_CTOR_NAME + " are the producer end and call sites " +
      "of its field accessors (" + MESSAGE_ACCESSOR_NAMES.mkString(", ") + ") are the " +
      "consumer end, with the message type's and companion's own generated machinery " +
      "excluded by owning type",
    crossedByACallEdge = producerToConsumerEdge,
    measured = List(
      "producer_call_sites" -> jnum(messageProducerSites.size.toLong),
      "consumer_call_sites" -> jnum(messageConsumerSites.size.toLong),
      "producers" -> jstrArr(messageProducers),
      "consumers" -> jstrArr(messageConsumers),
      "producer_call_site_graph_lines" ->
        jstrArr(messageProducerSites.map(c => s"${c.method.fullName}#${lineOf(c)}")),
      "consumer_call_site_graph_lines" ->
        jstrArr(messageConsumerSites.map(c => s"${c.method.fullName}#${lineOf(c)}").distinct.sorted),
      "direct_call_edge_producer_to_consumer" -> jbool(producerToConsumerEdge)))

  // --- B2: the thread hop ---------------------------------------------------
  val threadHostMethods = cpg.typeDecl
    .fullNameExact(THREAD_HOST_TYPE)
    .method
    .nameExact(THREAD_HOST_METHOD)
    .l
  val threadStartSites = callSitesOf(threadHostMethods)
    .filter(_.name == THREAD_HOST_METHOD)
  val threadStartCallees =
    threadStartSites.flatMap(calleesOf).map(_.fullName).distinct.sorted
  val threadBodyMethods = cpg.typeDecl
    .fullName(THREAD_BODY_TYPE_REGEX)
    .method
    .nameExact(THREAD_BODY_METHOD)
    .l
  val threadBodyNames = threadBodyMethods.map(_.fullName).distinct.sorted
  val threadHopCrossed = threadStartCallees.exists(threadBodyNames.contains)
  log(s"B2 Thread.start call sites: ${threadStartSites.size}")
  log(s"B2 callees of those sites : ${threadStartCallees.mkString(", ")}")
  log(s"B2 thread body methods    : ${threadBodyNames.mkString(", ")}")
  log(s"B2 start->run call edge   : $threadHopCrossed")
  val boundaryB2 = BoundaryRecord(
    id = "B2-thread",
    hop = THREAD_HOST_TYPE + "." + THREAD_HOST_METHOD + " calls Thread.start(); the " +
      "route continues in " + THREAD_BODY_METHOD + "() on the anonymous Thread subclass",
    fromEnd = threadStartSites.map(_.method.fullName).distinct.sorted.mkString(", "),
    toEnd = threadBodyNames.mkString(", "),
    reason = "Thread.start() -> run() is a JVM scheduling relation, not a call: the " +
      "start frame returns immediately and run() is entered on another thread, so no " +
      "CALL edge joins them",
    modelling = "not modelled - the two ends are reported as measured and the hop is " +
      "left uncrossed",
    crossedByACallEdge = threadHopCrossed,
    measured = List(
      "thread_start_call_sites" -> jnum(threadStartSites.size.toLong),
      "thread_start_call_site_graph_lines" ->
        jstrArr(threadStartSites.map(c => s"${c.method.fullName}#${lineOf(c)}").distinct.sorted),
      "thread_start_dispatch_types" ->
        jstrArr(threadStartSites.map(_.dispatchType).distinct.sorted),
      "callees_of_thread_start" -> jstrArr(threadStartCallees),
      "thread_body_methods" -> jstrArr(threadBodyNames),
      "call_edge_start_to_run" -> jbool(threadHopCrossed)))

  // --- B3: the interface hop ------------------------------------------------
  val sinkCallCallees = sinkCalls.flatMap(calleesOf).map(_.fullName).distinct.sorted
  val abstractLaunchNames =
    sinkCallCallees.filter(_.startsWith(ABSTRACT_LAUNCH_CALLEE_PREFIX))
  val concreteLaunchNames =
    sinkCallCallees.filterNot(_.startsWith(ABSTRACT_LAUNCH_CALLEE_PREFIX))
  val jdkLaunchMethodNodes = cpg.method.fullNameExact(JDK_LAUNCH_METHOD_FULL_NAME).l
  val interfaceHopCrossed = concreteLaunchNames.nonEmpty
  log(s"B3 sink call callees      : ${sinkCallCallees.mkString(", ")}")
  log(s"B3 concrete implementations reached: ${concreteLaunchNames.mkString(", ")}")
  log(s"B3 abstract declarations reached   : ${abstractLaunchNames.mkString(", ")}")
  log(s"B3 jdk launch method nodes: ${jdkLaunchMethodNodes.size}")
  log(s"B3 interface hop crossed  : $interfaceHopCrossed")
  val boundaryB3 = BoundaryRecord(
    id = "B3-interface",
    hop = "the launch call site invokes the ABSTRACT ProcessBuilderLike.start; the " +
      "JDK launch is reached only through the anonymous implementation",
    fromEnd = sinkCalls.map(_.method.fullName).distinct.sorted.mkString(", "),
    toEnd = concreteLaunchNames.mkString(", "),
    reason = "an interface invocation names the declaring type, so linking it to an " +
      "implementation needs the type hierarchy rather than the call's own name",
    modelling = "not modelled by this query - whether the hop is crossed is a property " +
      "of the graph's call linker and is reported as measured",
    crossedByACallEdge = interfaceHopCrossed,
    measured = List(
      "sink_call_sites" -> jnum(sinkCalls.size.toLong),
      "sink_call_dispatch_types" -> jstrArr(sinkCalls.map(_.dispatchType).distinct.sorted),
      "callees_of_sink_call_sites" -> jstrArr(sinkCallCallees),
      "abstract_declarations_reached" -> jstrArr(abstractLaunchNames),
      "concrete_implementations_reached" -> jstrArr(concreteLaunchNames),
      "jdk_launch_method_nodes_present" -> jnum(jdkLaunchMethodNodes.size.toLong),
      "call_edge_interface_to_implementation" -> jbool(interfaceHopCrossed)))

  // --- B4: the partial-function hop -----------------------------------------
  val sourceLevelHandlerCallees = callSitesOf(sourceLevelHandlerNodes)
    .map(_.methodFullName).distinct.sorted
  val syntheticEntryCallees = callSitesOf(syntheticEntryNodes)
    .map(_.methodFullName).distinct.sorted
  val handlerReachesBody =
    sourceLevelHandlerCallees.exists(_.contains(HANDLER_BODY_WITNESS))
  val syntheticReachesBody =
    syntheticEntryCallees.exists(_.contains(HANDLER_BODY_WITNESS))
  log(s"B4 source-level handler call sites : ${sourceLevelHandlerCallees.size}")
  log(s"B4 synthetic entry call sites      : ${syntheticEntryCallees.size}")
  log(s"B4 handler body reached from the source-level method: $handlerReachesBody")
  log(s"B4 handler body reached from the synthetic method   : $syntheticReachesBody")
  val boundaryB4 = BoundaryRecord(
    id = "B4-partial-function",
    hop = HANDLER_TYPE + "." + HANDLER_METHOD + " returns a PartialFunction whose body " +
      "compiles into a synthetic class",
    fromEnd = sourceLevelHandlerNodes.map(_.fullName).distinct.sorted.mkString(", "),
    toEnd = syntheticEntryNodes.map(_.fullName).distinct.sorted.mkString(", "),
    reason = "the method named " + HANDLER_METHOD + " only constructs the partial " +
      "function; the case bodies live in the synthetic class's " +
      ENTRY_SYNTHETIC_METHOD + ", so a selector on the source-level name would " +
      "traverse from a method that contains none of the route",
    modelling = "modelled by selecting BOTH: the synthetic " + ENTRY_SYNTHETIC_METHOD +
      " on every type matching " + ENTRY_SYNTHETIC_TYPE_REGEX + ", and the source-level " +
      HANDLER_METHOD + ", so the difference between them is measured rather than assumed",
    crossedByACallEdge = handlerReachesBody,
    measured = List(
      "source_level_handler_methods" ->
        jstrArr(sourceLevelHandlerNodes.map(_.fullName).distinct.sorted),
      "synthetic_entry_methods" ->
        jstrArr(syntheticEntryNodes.map(_.fullName).distinct.sorted),
      "source_level_handler_call_site_count" ->
        jnum(sourceLevelHandlerCallees.size.toLong),
      "synthetic_entry_call_site_count" -> jnum(syntheticEntryCallees.size.toLong),
      "source_level_handler_callees" -> jstrArr(sourceLevelHandlerCallees),
      "route_body_reached_from_source_level_name" -> jbool(handlerReachesBody),
      "route_body_reached_from_synthetic_name" -> jbool(syntheticReachesBody)))

  val boundaries = List(boundaryB1, boundaryB2, boundaryB3, boundaryB4)
  val boundariesNotCrossed = boundaries.filterNot(_.crossedByACallEdge)
  log(s"boundaries measured       : ${boundaries.size}")
  log(s"boundaries NOT crossed    : ${boundariesNotCrossed.map(_.id).mkString(", ")}")

  // -------------------------------------------------------------------------
  stage("I-spurious: the mechanical definition, applied to the emitted set")
  // -------------------------------------------------------------------------
  // A route is spurious ONLY where it passes one of the five named predicates
  // before reaching the sink. Nothing else makes a route spurious, and this
  // judges the QUERY's own output - it says nothing about Spark.
  val predicateCallerNameSet = predicateCallerNames.toSet
  def routeMethods(r: RouteRecord): List[String] =
    (r.entryPoint :: r.hops.map(_.toMethod)).distinct
  def routeIsSpurious(r: RouteRecord): Boolean =
    routeMethods(r).exists(predicateCallerNameSet.contains)
  val spuriousRoutes = distinctRoutes.filter(routeIsSpurious)
  val spuriousCount = spuriousRoutes.size
  val expectedSpuriousAbsent = spuriousCount == 0
  val absenceIsStructural = predicateCallSitesOnRoute.isEmpty
  log(s"spurious routes           : $spuriousCount of ${distinctRoutes.size}")
  log(s"expected-spurious absent  : $expectedSpuriousAbsent")
  log(s"absence basis             : " +
    (if (absenceIsStructural) "structural - no call site of any of the five named " +
      "predicates exists on the route surface, so no route could have passed one"
     else "filtering - predicate call sites do exist on the route surface"))

  // -------------------------------------------------------------------------
  stage("J-records: the returned set, capped and deterministic")
  // -------------------------------------------------------------------------
  def hopJson(h: Hop): String = jobj(10, List(
    "from_method" -> jstr(h.fromMethod),
    "call_site_callee" -> jstr(h.callSite),
    "call_site_graph_line" -> jnum(h.callSiteLine.toLong),
    "to_method" -> jstr(h.toMethod)))

  def routeJson(r: RouteRecord): String = jobj(6, List(
    "kind" -> jstr("route"),
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
    "hop" -> jstr(b.hop),
    "from_end" -> jstr(b.fromEnd),
    "to_end" -> jstr(b.toEnd),
    "reason" -> jstr(b.reason),
    "modelling" -> jstr(b.modelling),
    "crossed_by_a_call_edge" -> jbool(b.crossedByACallEdge),
    "measured" -> jobj(8, b.measured)))

  val recordJsonAll = boundaries.map(boundaryJson) ++ distinctRoutes.map(routeJson)
  val totalReturnsCapReached = recordJsonAll.size > MAX_TOTAL_RETURNS
  val recordJson = recordJsonAll.take(MAX_TOTAL_RETURNS)
  val returnedRecordCount = recordJson.size
  log(s"records returned          : $returnedRecordCount " +
    s"(${boundaries.size} boundary, ${distinctRoutes.size} route; cap " +
    s"$MAX_TOTAL_RETURNS, reached=$totalReturnsCapReached)")

  def walkJson(w: WalkResult): String = jobj(6, List(
    "walk_id" -> jstr(w.walkId),
    "follows_dynamic_dispatch_fan_out" -> jbool(w.followsFanOut),
    "entry_points_traversed" -> jnum(w.entryPointsTraversed.toLong),
    "method_expansions_this_walk" -> jnum(w.expansions.toLong),
    "max_method_expansions_at_one_entry_point" ->
      jnum(w.maxExpansionsAtOneEntry.toLong),
    "methods_visited" -> jnum(w.methodsVisited.toLong),
    "call_sites_considered" -> jnum(w.callSitesConsidered.toLong),
    "fan_out_sites_encountered" -> jnum(w.fanOutSitesEncountered.toLong),
    "fan_out_sites_not_followed" -> jnum(w.fanOutSitesNotFollowed.toLong),
    "max_depth_used" -> jnum(w.maxDepthUsed.toLong),
    "depth_bound_reached" -> jbool(w.depthBoundReached),
    "entry_expansion_cap_reached" -> jbool(w.entryExpansionCapReached),
    "walk_expansion_budget_exhausted" -> jbool(w.walkExpansionBudgetExhausted),
    "route_cap_reached" -> jbool(w.routeCapReached),
    "routes_returned" -> jnum(w.routes.size.toLong)))

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
      val components = List(
        sameEdge -> ("the edge kinds traversed (" + mine.edgeKinds.mkString(", ") + ")"),
        sameEnds -> ("the node kinds selected as a route's ends (" +
          mine.endNodeKinds.mkString(", ") + ")"),
        shared.nonEmpty -> "at least one handler/sink pair in common",
        sameEntry -> "the entry-point selector literals, byte for byte",
        sameSink -> "the sink selector literals, byte for byte",
        (sameBoundKind && sameBoundValue) -> ("the bound, as the same kind of quantity " +
          "at the same value (" + mine.boundValue + " " + mine.boundKind + ")"),
        apiIdentical -> ("the Joern API construct sets, whose set difference is empty " +
          "in BOTH directions"))
      val agreed = components.filter(_._1).map(_._2)
      val differed = components.filterNot(_._1).map(_._2)
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
  stage("K-write: the envelope, the prose report and the console log")
  // -------------------------------------------------------------------------
  val resultsDir = repoRoot.resolve(RESULTS_DIR)
  Files.createDirectories(resultsDir)
  val jsonPath = resultsDir.resolve(s"$QUERY_ID.json")
  val mdPath = resultsDir.resolve(s"$QUERY_ID.md")

  // The publication identifier. DERIVED, never a timestamp: every member of one
  // publication carries it, so a consumer holding two members can tell whether
  // they belong to the same generation. A nanotime would do that too and would
  // break the byte-identity contract this envelope states, because an unchanged
  // source over an unchanged graph would then emit different bytes on every
  // run. Deriving it from the source digest and the graph's identity gives the
  // same guarantee for free: two publications sharing an identifier were
  // produced from the same source over the same graph and are byte-identical,
  // so mixing their members cannot produce an inconsistent set.
  val publicationId = sha256OfBytes(
    List(QUERY_ID, sourceSha256, shaObserved, sizeFollow.toString,
      methodCount.toString).mkString("\n").getBytes(StandardCharsets.UTF_8))
  log(s"publication id            : $publicationId")
  publicationIdOfRecord = Some(publicationId)

  /** What the route set MEANS, computed from the route set rather than written
   *  down: a zero-route outcome is a statement about this formulation over this
   *  graph and the four non-connectivity hops it measures, and a non-empty one
   *  points at the records that carry the routes. Neither form says anything
   *  about Spark. */
  val routeOutcomeStatement =
    if (distinctRoutes.isEmpty)
      "zero routes returned. That is a property of THIS formulation over THIS graph, " +
        "not of the code it was run against: the surface above crosses " +
        ROUTE_SURFACE_NON_CONNECTIVITY_HOPS.size + " hops that no CALL edge models, " +
        "each measured individually and published as a boundary record, so a " +
        "call-graph walk cannot connect these endpoints however the program behaves. " +
        "The query judges nothing about that"
    else
      distinctRoutes.size + " distinct route(s) returned, each published in full under " +
        "records with the walk that returned it, its ordered hop sequence and whether " +
        "it passed one of the named predicates. A route is a traversal result and " +
        "carries no judgement of the code it traverses"

  /**
   * Effort measure 2, audited by measurement rather than by assertion. Every
   * entry in the declared construct list names a member this source invokes,
   * and that is checked here against the source text with the list's OWN
   * declaration excised first, so no entry can satisfy itself by appearing in
   * the list. The token searched for is the member name with its leading dot,
   * or the bare name where the entry names no type.
   */
  def apiConstructToken(construct: String): String = {
    val cut = construct.lastIndexOf('.')
    if (cut < 0) construct else construct.substring(cut)
  }
  val apiListDeclarationText =
    declarationTextOf(sourceText, FORMULATION_API_CONSTRUCTS_CONSTANT_NAME).getOrElse("")
  val sourceTextWithoutApiList = sourceText.replace(apiListDeclarationText, "")
  val apiConstructsConfirmed = JOERN_API_CONSTRUCTS.distinct
    .filter(c => sourceTextWithoutApiList.contains(apiConstructToken(c)))
  val apiConstructsNotConfirmed =
    JOERN_API_CONSTRUCTS.distinct.filterNot(apiConstructsConfirmed.contains)
  log(s"api constructs audited    : ${apiConstructsConfirmed.size} of " +
    s"${JOERN_API_CONSTRUCTS.distinct.size} confirmed in the source with the list " +
    s"excised" +
    (if (apiConstructsNotConfirmed.isEmpty) ""
     else "; not confirmed: " + apiConstructsNotConfirmed.mkString(", ")))

  val envelopeCoreFields: List[(String, String)] = List(
    "query_id" -> jstr(QUERY_ID),
    "query_source" -> jstr(sourceRepoRelative),
    "source_integrity" -> jobj(2, List(
      "query_source" -> jstr(sourceRepoRelative),
      "query_source_sha256" -> jstr(sourceSha256),
      "query_source_byte_size" -> jnum(sourceByteSize),
      "digested_at" -> jstr("run time, by the running script, from the file at the " +
        "path above"),
      "self_identification_checked" -> jbool(true),
      "self_identification_basis" -> jstr("the file digested must declare this " +
        "query's own id; a digest of any other file is refused rather than published"),
      "loader" -> jstr("importCpg"),
      "alternative_loader_occurrences_in_the_source" ->
        jnum(alternativeLoaderOccurrences.toLong),
      "alternative_loader_absence_is_measured" -> jbool(true),
      "contract" -> jstr("every member of this publication - this envelope, the prose " +
        "report and the console log - carries this digest, so a member can be checked " +
        "against the source that wrote it rather than assumed to come from it. A " +
        "result whose digest does not match the source beside it was not written by " +
        "that source, and that is a defect in the result rather than a matter of " +
        "opinion"))),
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
        "target's own directory, flushed and fsynced, measured by reading the staged " +
        "bytes back, and only then moved onto its final name; the moves happen after " +
        "every member has been staged, so a failure part-way leaves the previous " +
        "generation in place rather than a mixed one. That closes the window BEFORE " +
        "the renames. The window BETWEEN them - N renames being N atomic operations " +
        "rather than one - is closed by the completion manifest below, which is " +
        "renamed last and required by the producer before the run continues"),
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
        "identifies the RUN and can say nothing about whether the set on disk is " +
        "complete. member_set_id is derived from MEMBER BYTES, which is what makes it " +
        "a completion record. Both are published, and neither substitutes for the " +
        "other"))),
    "formulation" -> jstr("bounded call-graph reachability over CALL edges, from the " +
      "standalone Master's driver-submission handler to the privileged process launch " +
      "hosted on the DriverRunner surface"),
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
        MARKER_FAILURE + " is printed instead, the failing stage and the exception go " +
        "to the console stream, and the exception is re-raised. A partial result region " +
        "is never emitted, because one would read like a completed run"),
      "on_failure_published_members" -> jstr("no envelope and no prose report are " +
        "published on failure. Both are staged before either is moved into place, so a " +
        "failure part-way abandons the staged copies and leaves the previous generation " +
        "intact; only the console log is published, and it carries the failing stage"),
      "value_not_established_convention" -> jstr("a value that could not be " +
        "established is named as such in the field that would have carried it, never " +
        "omitted and never guessed: a value missing from the record is a value nothing " +
        "downstream can check"))),
    "returned_record_count" -> jnum(returnedRecordCount.toLong),
    "returned_record_kinds" -> jobj(2, List(
      "boundary" -> jnum(boundaries.size.toLong),
      "route" -> jnum(distinctRoutes.size.toLong))),
    "distinct_routes" -> jnum(distinctRoutes.size.toLong),
    "distinct_routes_convention" -> jstr("routes from both walks deduplicated on " +
      "(entry point, sink host, hop sequence); the walks' returns are never summed"),
    "distinct_routes_identity_function" -> jstr("a route identity is the triple (entry " +
      "point method full name, sink host method full name, the ordered sequence of " +
      "(from method, call site callee, to method) hops from the entry point to the " +
      "sink). Two returns with equal triples are ONE route however many traversal " +
      "orders produced them, and the two walks below are deduplicated against each " +
      "other on it rather than added together"),
    "never_summed_with" -> jstrArr(SIBLING_QUERY_IDS),
    "spurious_count" -> jnum(spuriousCount.toLong),
    "spurious_definition" -> jstr("a route is spurious ONLY where it passes an " +
      "authorization or ACL predicate before reaching the sink, the predicate set being " +
      "exactly the five named selectors below; this judges the query, not Spark"),
    "spurious_definition_limit" -> jstr("the definition evaluates ONLY those five " +
      "predicates. Any other conditional on the route is outside it and is NOT assessed " +
      "by it: concretely, " + MASTER_SOURCE_FILE + ":411 if (state != " +
      "RecoveryState.ALIVE) guards the branch that reaches createDriver at " +
      MASTER_SOURCE_FILE + ":417, and it is a recovery-state check rather than one of " +
      "the five, so it is neither counted as a predicate nor reported as one. A " +
      "spurious count of 0 therefore means exactly and only what the definition says, " +
      "and does not mean that the route carries no conditional"),
    "expected_spurious_route_absent" -> jbool(expectedSpuriousAbsent),
    "expected_spurious_absence_basis" ->
      jstr(if (absenceIsStructural) "structural" else "filtering"),
    "expected_spurious_absence_statement" -> jstr(
      "no route in the emitted set passed an auth/ACL predicate as defined by these " +
        "five named selectors, and no call site of any of the five exists on the route " +
        "surface at all, so the absence is structural rather than a consequence of the " +
        "query filtering well"),
    "bound_value" -> jnum(MAX_CALL_DEPTH.toLong),
    "bound_value_meaning" -> jstr("MAX_CALL_DEPTH, the maximum call-graph hops walked " +
      "from an entry point; it exceeds the hop count of the documented route, so a " +
      "route absent within it is not an artefact of a short bound"),
    "bound_reached" -> jbool(boundReached),
    "bounds" -> jobj(2, List(
      "MAX_CALL_DEPTH" -> jnum(MAX_CALL_DEPTH.toLong),
      "MAX_ROUTES" -> jnum(MAX_ROUTES.toLong),
      "MAX_EXPANSIONS_PER_ENTRY" -> jnum(MAX_EXPANSIONS_PER_ENTRY.toLong),
      "MAX_EXPANSIONS_PER_WALK" -> jnum(MAX_EXPANSIONS_PER_WALK.toLong),
      "MAX_TOTAL_RETURNS" -> jnum(MAX_TOTAL_RETURNS.toLong),
      "MAX_ENTRY_POINTS" -> jnum(MAX_ENTRY_POINTS.toLong),
      "MAX_CALL_SCAN" -> jnum(MAX_CALL_SCAN.toLong),
      "FANOUT_CALLEE_THRESHOLD" -> jnum(FANOUT_CALLEE_THRESHOLD.toLong))),
    "bounds_meaning" -> jobj(2, List(
      "MAX_CALL_DEPTH" -> jstr("maximum call-graph hops walked from an entry point"),
      "MAX_ROUTES" -> jstr("maximum distinct routes retained"),
      "MAX_EXPANSIONS_PER_ENTRY" -> jstr("the per-entry-point step cap, counted in " +
        "method expansions rather than in edges and reset at each entry point"),
      "MAX_EXPANSIONS_PER_WALK" -> jstr("the whole-walk expansion budget, which bounds " +
        "one walk across every entry point it traverses"),
      "MAX_TOTAL_RETURNS" -> jstr("the total-returns cap, across every record kind " +
        "this query emits"),
      "MAX_ENTRY_POINTS" -> jstr("maximum entry points traversed; the remainder are " +
        "counted as truncated rather than dropped silently"),
      "MAX_CALL_SCAN" -> jstr("cap on the indexed call-name sweeps used to find the " +
        "sink and message call sites"),
      "FANOUT_CALLEE_THRESHOLD" -> jstr("a THRESHOLD rather than a cap: a call site " +
        "whose resolved callee set is wider than this is recorded as a dynamic-dispatch " +
        "fan-out site"))),
    "bounds_reached" -> jobj(2, List(
      "MAX_CALL_DEPTH" -> jbool(walks.exists(_.depthBoundReached)),
      "MAX_ROUTES" -> jbool(walks.exists(_.routeCapReached)),
      "MAX_EXPANSIONS_PER_ENTRY" -> jbool(walks.exists(_.entryExpansionCapReached)),
      "MAX_EXPANSIONS_PER_WALK" -> jbool(walks.exists(_.walkExpansionBudgetExhausted)),
      "MAX_TOTAL_RETURNS" -> jbool(totalReturnsCapReached),
      "MAX_ENTRY_POINTS" -> jbool(entryPointsTruncated > 0),
      "MAX_CALL_SCAN" -> jbool(sinkScanTruncated),
      "FANOUT_CALLEE_THRESHOLD" ->
        jbool(walks.exists(_.fanOutSitesEncountered > 0)))),
    "bounds_reached_basis" -> jobj(2, List(
      "MAX_CALL_DEPTH" -> jstr(
        (if (walks.exists(_.depthBoundReached)) "reached: " else "not reached: ") +
          walks.map(w => s"${w.walkId} used depth ${w.maxDepthUsed} of $MAX_CALL_DEPTH " +
            s"(frontier still non-empty at the bound: ${w.depthBoundReached})")
            .mkString("; ")),
      "MAX_ROUTES" -> jstr(
        (if (walks.exists(_.routeCapReached)) "reached: " else "not reached: ") +
          walks.map(w => s"${w.walkId} retained ${w.routes.size} of $MAX_ROUTES")
            .mkString("; ")),
      "MAX_EXPANSIONS_PER_ENTRY" -> jstr(
        (if (walks.exists(_.entryExpansionCapReached)) "reached: " else "not reached: ") +
          walks.map(w => s"${w.walkId} peaked at ${w.maxExpansionsAtOneEntry} " +
            s"expansions at a single entry point, of $MAX_EXPANSIONS_PER_ENTRY")
            .mkString("; ")),
      "MAX_EXPANSIONS_PER_WALK" -> jstr(
        (if (walks.exists(_.walkExpansionBudgetExhausted)) "reached: "
         else "not reached: ") +
          walks.map(w => s"${w.walkId} spent ${w.expansions} of " +
            s"$MAX_EXPANSIONS_PER_WALK across every entry point").mkString("; ")),
      "MAX_TOTAL_RETURNS" -> jstr(
        (if (totalReturnsCapReached) "reached: " else "not reached: ") +
          s"$returnedRecordCount records of $MAX_TOTAL_RETURNS"),
      "MAX_ENTRY_POINTS" -> jstr(
        (if (entryPointsTruncated > 0) "reached: " else "not reached: ") +
          s"$entryPointsDiscovered entry points discovered, $entryPointsTraversed " +
          s"traversed, $entryPointsTruncated truncated, cap $MAX_ENTRY_POINTS"),
      "MAX_CALL_SCAN" -> jstr(
        (if (sinkScanTruncated) "reached: " else "not reached: ") +
          s"${startCallsScanned.size} calls named $SINK_CALL_NAME scanned of " +
          s"$MAX_CALL_SCAN, and the sweep reported truncated=$sinkScanTruncated"),
      "FANOUT_CALLEE_THRESHOLD" -> jstr(
        (if (walks.exists(_.fanOutSitesEncountered > 0))
           "exceeded, which is what \"reached\" means for a threshold: "
         else "not exceeded: ") +
          walks.map(w => s"${w.walkId} encountered ${w.fanOutSitesEncountered} fan-out " +
            s"site(s) wider than $FANOUT_CALLEE_THRESHOLD callees").mkString("; ")))),
    "entry_points_discovered" -> jnum(entryPointsDiscovered.toLong),
    "entry_points_traversed" -> jnum(entryPointsTraversed.toLong),
    "entry_points_truncated" -> jnum(entryPointsTruncated.toLong),
    "entry_points_truncated_meaning" -> jstr("the two counters exist so that a sweep " +
      "cannot run unbounded and so that a trimmed traversal cannot pass for a complete " +
      "one. A truncated count above zero is a measured property of the traversal, to be " +
      "reported rather than hidden; here " + entryPointsDiscovered + " entry point(s) " +
      "were discovered against a cap of " + MAX_ENTRY_POINTS + ", of which " +
      entryPointsTraversed + " were traversed and " + entryPointsTruncated +
      " truncated"),
    "entry_point_selection" -> jstr("BOUNDARY 4: the handler body compiles into a " +
      "synthetic partial-function class, so the synthetic " + ENTRY_SYNTHETIC_METHOD +
      " on every type matching " + ENTRY_SYNTHETIC_TYPE_REGEX + " is selected together " +
      "with the source-level " + HANDLER_TYPE + "." + HANDLER_METHOD),
    "entry_points" -> jstrArr(entryGroups.map(_._1)),
    "sink_hosts" -> jstrArr(sinkHostNames.toList.sorted),
    "sink_call_sites" -> jstrArr(sinkCalls.map(c =>
      s"${c.method.fullName} -> ${c.methodFullName} #${lineOf(c)}")),
    "route_surface" -> jobj(2, List(
      "pinned_commit" -> jstr(PINNED_COMMIT),
      "paths_relative_to" -> jstr("the SPARK_SRC root - the separately cloned pinned " +
        "tree, which is the only tree the probe reads. No absolute path is emitted"),
      "line_numbers_verified_at" -> jstr("the pinned commit above, re-verified " +
        "directly against that tree"),
      "working_checkout_offset_warning" ->
        jstr(ROUTE_SURFACE_WORKING_CHECKOUT_OFFSET)) ++
      ROUTE_SURFACE_ANCHORS.map { case (k, v) => k -> jstr(v) } ++
      List(
        "launch_driver_calls" -> jstrArr(ROUTE_SURFACE_LAUNCH_DRIVER_CALLS),
        "non_connectivity_hops" -> jstrArr(ROUTE_SURFACE_NON_CONNECTIVITY_HOPS),
        "route_outcome" -> jstr(routeOutcomeStatement))),
    "route_record_schema" -> jstrArr(List(
      "kind - the literal route",
      "walk_id - which of the two walks returned it",
      "entry_point - the method full name the walk started from",
      "sink_host - the method full name hosting the launch call site it reached",
      "hop_count - the number of hops in the route",
      "hops - the ordered sequence, each hop carrying from_method, call_site_callee, " +
        "call_site_graph_line and to_method",
      "passed_auth_or_acl_predicate - whether the route passed one of the five",
      "spurious - the same value under the name the definition uses")),
    "operator_pseudo_calls_excluded" -> jbool(true),
    "duplicate_class_definitions_unioned" -> jbool(true),
    "graph" -> jobj(2, List(
      "path_source" -> jstr(cpgPathSource),
      "named_path" -> jstr(cpgNamedLabel),
      "named_path_repo_relative" -> jstr(cpgNamedRepoRelativeLabel),
      "named_path_is_symlink" -> jbool(cpgIsLink),
      "resolved_target" -> jstr(cpgResolvedLabel),
      "resolved_target_identification" -> jstr("no absolute host path is emitted " +
        "anywhere in this envelope, so the resolved target is identified by the " +
        "symlink-FOLLOWING byte size and sha256 below rather than by a host path. That " +
        "pair is the identity of record and is what every load re-verifies; a host path " +
        "would additionally vary between two checkouts of one branch and so could not " +
        "be part of a deterministic envelope"),
      "measurement_semantics" -> jstr("symlink-FOLLOWING. Where the named path is a " +
        "symlink, measuring the link itself records a few dozen bytes rather than the " +
        "graph: byte_size_without_following is recorded only to be discarded"),
      "byte_size_following_the_link" -> jnum(sizeFollow),
      "byte_size_without_following" -> jnum(sizeNoFollow),
      "sha256" -> jstr(shaObserved),
      "identity_record" -> jstr(CPG_RECORD_PATH),
      "identity_record_role" -> jstr("the declared owner of this pair, which computed " +
        "it at write time with the same symlink-following semantics; this envelope " +
        "cites that measurement rather than establishing a second one"),
      "identity_recorded_byte_size" -> jnum(recordedSize),
      "identity_recorded_sha256" -> jstr(recordedSha),
      "identity_comparison_result" -> jstr("match - the observed byte size and sha256 " +
        "equal the pair the identity record owns, on both values; a mismatch halts the " +
        "run in the identity stage and publishes no envelope"),
      "identity_reverified_before_load" -> jbool(true),
      "aap_named_path_reconciliation" -> jstr(aapNameReconciliation),
      "methods" -> jnum(methodCount.toLong),
      "type_declarations" -> jnum(typeDeclCount.toLong),
      "files" -> jnum(fileCount.toLong))),
    "runtime" -> jobj(2, List(
      "jdk_major" -> jstr(jdkMajor),
      "jdk_major_required" -> jstr(REQUIRED_JDK_MAJOR),
      "jvm_version" -> jstr(jvmVersion),
      "command" -> jstr(REPRODUCTION_COMMAND),
      "command_completeness" -> jstr("the command above is the whole of what this " +
        "query needs: the repository root, the JDK, the heap override, the log level " +
        "and the script path. It reads no other environment variable that changes what " +
        "it loads or what it publishes"),
      "jvm_arguments_kept" -> jstrArr(jvmArgsKept),
      "jvm_arguments_redacted_count" -> jnum(jvmArgsRedacted.size.toLong),
      "jvm_arguments_redaction_policy" -> jstr(JVM_ARG_REDACTION_POLICY),
      "heap_actually_used_bytes" -> jnum(heapMaxBytes),
      "heap_floor_bytes" -> jnum(HEAP_FLOOR_BYTES),
      "heap_at_or_above_floor" -> jbool(heapMaxBytes >= HEAP_FLOOR_BYTES),
      "heap_direction_rule" -> jstr("the floor is a minimum and a default, never a " +
        "ceiling: a larger heap is permitted and reported, and a smaller one is not, " +
        "because a truncated result's silence cannot be told apart from a clean one"),
      "heap_override_mechanism" -> jstr("MEASURED, NOT ASSUMED: joern's --script path " +
        "forks a child JVM with no JVM options forwarded, so -J-Xmx reaches the " +
        "launcher only and the child would otherwise run at an ergonomic default. " +
        "JAVA_TOOL_OPTIONS is inherited by the child and is what actually raises the " +
        "heap the query runs at, which is why the query measures Runtime.maxMemory() " +
        "and halts below the floor rather than trusting the flag it was given"),
      "loader" -> jstr("importCpg into a switched workspace; the frontend-then-importCpg " +
        "route is mandated because the alternative spawns a second JVM at the same heap"),
      "loader_is_importcpg_only" -> jbool(true),
      "loader_alternative_absent_from_the_source" ->
        jbool(alternativeLoaderOccurrences == 0),
      "workspace" -> jstr(WORKSPACE_PATH),
      "heap_bound_jvm_position" -> jstr("4 of 4 (frontend build, importCpg verification " +
        "load, Stage 3 Joern runner, this probe)"))),
    "predicate_selector" -> jobj(2, List(
      "type" -> jstr(PREDICATE_TYPE),
      "name_regex" -> jstr(PREDICATE_NAME_REGEX),
      "setter_suffix_excluded" -> jstr(PREDICATE_SETTER_SUFFIX),
      "named_five" -> jstrArr(PREDICATE_NAMED_FIVE.sorted),
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
      "call_sites_on_the_route_surface" -> jnum(predicateCallSitesOnRoute.size.toLong),
      "route_surface_type_prefixes" -> jstrArr(ROUTE_SURFACE_TYPE_PREFIXES))),
    "walks" -> jrawArr(4, walks.map(walkJson)),
    "boundaries_not_crossed" -> jstrArr(boundariesNotCrossed.map(_.id)),
    "duplicate_formulation" -> jstr(duplicateFormulationAggregate),
    "duplicate_formulation_aggregation" -> jstr(duplicateFormulationAggregation),
    "duplicate_formulation_relation" -> jstr(duplicateFormulationRelation),
    "duplicate_formulation_detail" -> duplicateFormulationJson,
    "effort_query_revisions_committed" ->
      (if (revisionsEstablished) jnum(revisionCommits.size.toLong) else "null"),
    "effort_query_revisions_established" -> jbool(revisionsEstablished),
    "effort_query_revisions_measurement" -> jstr(revisionsNote),
    "effort_query_revisions_commits" -> jstrArr(revisionCommits),
    "effort_query_revisions_convention" -> jstr(QUERY_REVISIONS_CONVENTION),
    "effort_joern_api_constructs" -> jstrArr(JOERN_API_CONSTRUCTS),
    "effort_joern_api_construct_count" -> jnum(JOERN_API_CONSTRUCTS.distinct.size.toLong),
    "effort_joern_api_constructs_audit" -> jobj(2, List(
      "measure" -> jstr("the LIST is the measure and the count is computed from it, so " +
        "the number is auditable rather than asserted"),
      "method" -> jstr("every entry is searched for in this query's own source text " +
        "with the construct list's own declaration excised first, so no entry can " +
        "satisfy itself by appearing in the list. The token searched for is the member " +
        "name with its leading dot, or the bare name where the entry names no type"),
      "declared_entries" -> jnum(JOERN_API_CONSTRUCTS.size.toLong),
      "distinct_entries" -> jnum(JOERN_API_CONSTRUCTS.distinct.size.toLong),
      "confirmed_in_the_source" -> jnum(apiConstructsConfirmed.size.toLong),
      "not_confirmed_in_the_source" -> jstrArr(apiConstructsNotConfirmed),
      "method_limitation" -> jstr("a member-name search establishes that the source " +
        "names the member, not that every occurrence is an invocation of THIS type's " +
        "member; a short member name can therefore be confirmed by a longer name that " +
        "contains it. The limitation is stated rather than left for a reader to " +
        "discover, and any entry the search could not confirm is listed above rather " +
        "than dropped from the count"))),
    "effort_parameterizability" -> jstr("not claimed here; proven by " +
      PARAMETERIZABILITY_OWNER + " invoking its parameterized form on the second named " +
      "handler/sink pair and capturing that invocation's result"),
    "total_returns_cap_reached" -> jbool(totalReturnsCapReached),
    "records_order" -> jstr("an explicit TOTAL sort key. Boundary records come first, " +
      "in the fixed boundary order " + boundaries.map(_.id).mkString(", ") + ", then " +
      "route records ordered by (walk_id, entry_point, sink_host, hop_count, the " +
      "complete ordered hop sequence). The hop sequence is the last component " +
      "precisely so that two routes sharing their endpoints and their hop count still " +
      "have a defined order: no two records can share the whole key, so the order is " +
      "TOTAL rather than merely stable, and it is then truncated to MAX_TOTAL_RETURNS"),
    "collection_order" -> jstr("every list this envelope publishes is ordered " +
      "explicitly rather than left in traversal order. Source-construct lists ascend " +
      "by their position on the route surface, which is the order the route surface " +
      "block itself declares (route_surface, launch_driver_calls, " +
      "non_connectivity_hops). Graph and query identifier lists ascend " +
      "lexicographically (entry_points, sink_hosts, sink_call_sites, named_five, " +
      "step1_broad_matches, step2_setters_excluded, step2_remaining, " +
      "step3_non_predicate_residue_dropped, final_names, final_full_names, " +
      "route_surface_type_prefixes, never_summed_with, effort_joern_api_constructs, " +
      "api_constructs_only_here, api_constructs_only_there, " +
      "non_deterministic_quantities_excluded). Boundary lists follow the fixed " +
      "boundary order. Bounds objects follow the order the bounds are declared in the " +
      "source. Walks follow the order the two walks are run in, which is declared in " +
      "the source and not data-dependent. Per-sibling duplicate entries follow the " +
      "sibling order declared in the source. Effort commit lists are newest first, as " +
      "git reports them, which is a stated order rather than an incidental one"))

  // ------------------- provenance, then the measured determinism ------------
  val provenanceJson = jobj(2, List(
    "measured_values_cited_from" -> jstr("the graph loaded in this run, this query's " +
      "own source text, the identity record named above and the repository's own " +
      "commit history for this source path"),
    "measured_values_note" -> jstr("every number in this envelope was computed during " +
      "this run from one of those four sources. No figure is transcribed from another " +
      "document, from a sibling query's envelope or from a previous run, which is why " +
      "a figure this run could not establish is published as null with its reason " +
      "beside it rather than carried over"),
    "graph_identity_owner" -> jstr(CPG_RECORD_PATH),
    "query_source" -> jstr(sourceRepoRelative),
    "query_source_sha256" -> jstr(sourceSha256),
    "bound_constants_defined_by" -> jstr(sourceRepoRelative),
    "line_numbers_verified_against" ->
      jstr("the pinned tree at " + PINNED_COMMIT + ", read directly"),
    "graph_path_expression" -> jstr("repo-relative and portable: the graph is named by " +
      CPG_ENV_VAR + " where that is set and by " + CPG_PATH_DEFAULT + " otherwise, and " +
      "the resolved target is identified by its byte size and sha256 rather than by a " +
      "host path"),
    "contributes_dataset_rows" -> jbool(false),
    "dataset_separation" -> jstr("this probe writes outside " +
      "harness/artifacts/raw/, contributes no row to findings.json or findings.csv, " +
      "and is counted in neither. It is a capability measurement of the query " +
      "language, not a scan")))

  // Two passes. "No absolute host path is emitted" is a claim about the text
  // this run produced, so it is MEASURED against that text rather than asserted
  // about it: the first pass renders every field except the determinism block,
  // the search runs over that text, and the second pass renders the block that
  // reports the result. Only the determinism block itself is outside the
  // searched text, and it carries the LABELS of any path found rather than the
  // paths, so reporting a hit cannot itself emit one.
  val envelopeWithoutDeterminism = jobj(0, envelopeCoreFields ++ List(
    "provenance" -> provenanceJson,
    "records" -> jrawArr(2, recordJson)))
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
    .filter { case (_, v) =>
      v != null && v.length >= ABSOLUTE_PATH_SEARCH_MIN_LENGTH
    }.distinct
  val absolutePathsFound = absolutePathsSearchedFor
    .filter { case (_, v) => envelopeWithoutDeterminism.contains(v) }
    .map(_._1).distinct.sorted
  log(s"absolute host paths       : ${absolutePathsFound.size} of " +
    s"${absolutePathsSearchedFor.size} searched-for paths occur in the envelope" +
    (if (absolutePathsFound.isEmpty) "" else ": " + absolutePathsFound.mkString(", ")))

  val determinismJson = jobj(2, List(
    "byte_identity_contract" -> jstr("an unchanged source, run over an unchanged graph " +
      "with an unchanged commit history for that source path, emits a byte-identical " +
      "envelope. Commit history is part of the contract because the revision count " +
      "above is measured from it rather than written down, so a new commit on this " +
      "path legitimately changes one field"),
    "non_deterministic_quantities_excluded" -> jstrArr(List(
      "absolute host paths",
      "elapsed times",
      "host names",
      "process identifiers",
      "project names",
      "scratch and temporary directory names",
      "wall-clock timestamps",
      "workspace names").sorted),
    "elapsed_times_live_in" -> jstr(LOG_DIR + "/probe-" + QUERY_ID + ".log"),
    "absolute_host_paths_emitted" -> jbool(absolutePathsFound.nonEmpty),
    "absolute_host_paths_measurement" -> jstr("MEASURED, not asserted: the " +
      absolutePathsSearchedFor.size + " absolute paths this run resolved were each " +
      "searched for in the rendered envelope, and what is reported is the outcome of " +
      "that search. Only this determinism block is outside the searched text, and it " +
      "carries the labels of any path found rather than the paths themselves, so " +
      "reporting a hit cannot itself emit one. A path shorter than " +
      ABSOLUTE_PATH_SEARCH_MIN_LENGTH + " characters is not searched for, because a " +
      "very short prefix matches ordinary prose"),
    "absolute_host_path_labels_found" -> jstrArr(absolutePathsFound),
    "trailing_newline" -> jbool(true),
    "publication_id" -> jstr(publicationId),
    "reproduction_command" -> jstr(REPRODUCTION_COMMAND),
    "reproduction_check_status" -> jstr("not attempted from inside this run: a run " +
      "cannot launch and compare a second copy of itself without becoming the thing it " +
      "is measuring. The contract above is therefore stated as the condition a " +
      "reproducer checks, and the publication identifier is what makes the check " +
      "mechanical - two publications sharing it were produced from the same source " +
      "over the same graph and must be byte-identical"),
    "mixed_generation_detection" -> jstr("every member of this publication carries the " +
      "same publication identifier and the same query source sha256. A consumer " +
      "holding two members whose identifiers differ is holding two generations, and " +
      "that is detectable from the members themselves rather than from a separate " +
      "marker file")))

  val envelope = jobj(0, envelopeCoreFields ++ List(
    "determinism" -> determinismJson,
    "provenance" -> provenanceJson,
    "records" -> jrawArr(2, recordJson))) + "\n"

  // Staged, not written: the envelope goes to a private temporary beside its
  // target and becomes visible only when every member of the publication has
  // been written and fsynced. Nothing is published one member at a time.
  val envelopeMember = stageMember(jsonPath, envelope)
  log(s"envelope staged           : $jsonPath (${envelopeMember.byteSize} bytes, " +
    s"sha256 ${envelopeMember.sha256})")

  // ---------------------------- the prose report ----------------------------
  val md = scala.collection.mutable.ArrayBuffer.empty[String]
  def md0(line: String): Unit = md += line

  md0(s"# Joern capability probe $QUERY_ID")
  md0("")
  md0("Bounded **call-graph** reachability from the Spark standalone Master's")
  md0("driver-submission handler to the privileged process launch hosted on the")
  md0("`DriverRunner` surface, over the code-property graph built from the pinned tree's")
  md0("bytecode.")
  md0("")
  md0("This report is **observational**. It judges no finding - not real, not important,")
  md0("not a false positive, not a duplicate - and makes no comparison between tools. It")
  md0("contributes no row to `oss-scan-results/findings.json` and writes nothing into")
  md0("`harness/artifacts/raw/`.")
  md0("")
  md0(s"The slug `$QUERY_ID` is the **identifier**")
  md0("the plan assigns this query. It names the question the query was written to ask -")
  md0("whether a call-graph formulation can join this handler to this sink, and whether")
  md0("any route it returns passes one of five named predicates first. It is not a")
  md0("finding, and nothing in this report should be read as an assessment of Spark, of")
  md0("any Spark component or of any Spark configuration.")
  md0("")
  md0("| | |")
  md0("| --- | --- |")
  md0(s"| Query source | `$sourceRepoRelative` |")
  md0(s"| Query source sha256 | `$sourceSha256` ($sourceByteSize bytes) |")
  md0(s"| Publication id | `$publicationId` |")
  md0(s"| Envelope | `$RESULTS_DIR/$QUERY_ID.json` |")
  md0(s"| Console log | `$LOG_DIR/probe-$QUERY_ID.log` |")
  md0(s"| Loader | `importCpg` into a switched workspace (`$WORKSPACE_PATH`) |")
  md0(s"| JDK major | $jdkMajor |")
  md0(s"| Heap actually used | $heapMaxBytes bytes (floor $HEAP_FLOOR_BYTES) |")
  md0(s"| Graph | $sizeFollow bytes, sha256 `$shaObserved` |")
  md0(s"| Graph identity re-verified before the load | yes, against `$CPG_RECORD_PATH` |")
  md0(s"| Graph methods / typeDecls / files | $methodCount / $typeDeclCount / $fileCount |")
  md0(s"| Compile status | compiled |")
  md0(s"| Run status | completed |")
  md0(s"| Records returned | $returnedRecordCount (${boundaries.size} boundary, " +
    s"${distinctRoutes.size} route) |")
  md0(s"| Distinct routes | ${distinctRoutes.size} |")
  md0(s"| Spurious routes | $spuriousCount |")
  md0("")
  md0("## Which source wrote this report")
  md0("")
  md0(s"This report was written by `$sourceRepoRelative`, whose contents at the moment")
  md0(s"of the run digest to sha256 `$sourceSha256` over $sourceByteSize bytes. The")
  md0("query read its own source at run time and computed that digest itself; it")
  md0("verified that the file it digested declares this query's own identifier, and it")
  md0("refuses to publish anything if it does not.")
  md0("")
  md0(s"The envelope beside this report carries the same digest and the same publication")
  md0(s"identifier `$publicationId`, as does the console log. Every figure below was")
  md0("measured during that run from the graph, from this source's own text, from the")
  md0("identity record or from the repository's commit history for this source path -")
  md0("nothing here is transcribed from another document or from a previous run. **A")
  md0("result whose digest does not match the source beside it was not written by that")
  md0("source**, which makes drift between a query and its published result a")
  md0("mechanical check rather than a matter of opinion.")
  md0("")
  md0("## The result")
  md0("")
  md0(s"**Distinct routes: ${distinctRoutes.size}.** Routes are counted distinct on")
  md0("(entry point, sink host, hop sequence) across both walks below and are **never")
  md0("summed**.")
  md0("")
  if (distinctRoutes.isEmpty) {
    md0("No route from an entry point to a sink host was returned within the stated")
    md0("bound. That is a capability finding about what this formulation can express")
    md0("over this graph, and it is reported as measured: the bound was not loosened,")
    md0("removed or re-run unbounded to produce a non-empty result. The four boundaries")
    md0("below are the measured reason.")
  } else {
    md0("Routes were returned. Each is listed in the envelope with its full hop")
    md0("sequence, the call site at each hop and the graph line of that call site.")
    distinctRoutes.foreach { r =>
      md0("")
      md0(s"- walk `${r.walkId}`, ${r.hops.size} hops, entry `${r.entryPoint}` to sink " +
        s"host `${r.sinkHost}`")
    }
  }
  md0("")
  md0("## Whether the bound was reached")
  md0("")
  md0(s"`bound_reached` = **$boundReached**. The primary bound is `MAX_CALL_DEPTH` = " +
    s"$MAX_CALL_DEPTH")
  md0("call-graph hops from an entry point. Every traversal in this query carries an")
  md0("explicit named bound; none runs unbounded.")
  md0("")
  md0("Which bound bit, per walk, so the flag is interpretable rather than bare:")
  md0("")
  walks.foreach { w =>
    val bits = List(
      if (w.depthBoundReached) Some(s"the frontier was still non-empty at depth $MAX_CALL_DEPTH") else None,
      if (w.entryExpansionCapReached)
        Some(s"the PER-ENTRY-POINT expansion cap of $MAX_EXPANSIONS_PER_ENTRY was " +
          "reached at at least one entry point") else None,
      if (w.walkExpansionBudgetExhausted)
        Some(s"the WHOLE-WALK expansion budget of $MAX_EXPANSIONS_PER_WALK was " +
          "exhausted") else None,
      if (w.routeCapReached) Some("the route cap was reached") else None).flatten
    md0(s"- walk `${w.walkId}`: " +
      (if (bits.isEmpty) "no bound was reached; the walk ran to exhaustion"
       else bits.mkString("; ")) +
      s". Expansions: ${w.maxExpansionsAtOneEntry} of $MAX_EXPANSIONS_PER_ENTRY at the " +
      s"busiest single entry point, ${w.expansions} of $MAX_EXPANSIONS_PER_WALK across " +
      s"the whole walk; routes returned ${w.routes.size} of $MAX_ROUTES.")
  }
  md0("")
  md0("A depth bound reached with a non-empty frontier says only that the walk stopped")
  md0("expanding, so on its own it would leave open whether a deeper walk would reach a")
  md0("sink host. What settles that here is the boundary measurement below rather than")
  md0("the bound: the hops that break this route are not CALL edges at all, and no")
  md0("increase in depth introduces an edge that does not exist. The bound is therefore")
  md0("reported as reached, and the absence of a route is attributed to the measured")
  md0("boundaries, not to the bound.")
  md0("")
  md0("| bound | value |")
  md0("| --- | --- |")
  md0(s"| MAX_CALL_DEPTH | $MAX_CALL_DEPTH |")
  md0(s"| MAX_ROUTES | $MAX_ROUTES |")
  md0(s"| MAX_EXPANSIONS_PER_ENTRY | $MAX_EXPANSIONS_PER_ENTRY |")
  md0(s"| MAX_EXPANSIONS_PER_WALK | $MAX_EXPANSIONS_PER_WALK |")
  md0(s"| MAX_TOTAL_RETURNS | $MAX_TOTAL_RETURNS |")
  md0(s"| MAX_ENTRY_POINTS | $MAX_ENTRY_POINTS |")
  md0(s"| MAX_CALL_SCAN | $MAX_CALL_SCAN |")
  md0(s"| FANOUT_CALLEE_THRESHOLD | $FANOUT_CALLEE_THRESHOLD |")
  md0("")
  md0("| walk | follows fan-out | expansions (walk) | expansions (busiest entry) | " +
    "call sites | fan-out seen | fan-out not followed | max depth | depth bound " +
    "reached | per-entry cap reached | walk budget exhausted | routes |")
  md0("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
  walks.foreach { w =>
    md0(s"| `${w.walkId}` | ${w.followsFanOut} | ${w.expansions} | " +
      s"${w.maxExpansionsAtOneEntry} | " +
      s"${w.callSitesConsidered} | ${w.fanOutSitesEncountered} | " +
      s"${w.fanOutSitesNotFollowed} | ${w.maxDepthUsed} | ${w.depthBoundReached} | " +
      s"${w.entryExpansionCapReached} | ${w.walkExpansionBudgetExhausted} | " +
      s"${w.routes.size} |")
  }
  md0("")
  md0("## Entry points, and how they were selected")
  md0("")
  md0(s"Discovered $entryPointsDiscovered, traversed $entryPointsTraversed, truncated " +
    s"$entryPointsTruncated.")
  md0("")
  md0(s"`$HANDLER_METHOD` returns a `PartialFunction`, so its body compiles into a")
  md0("synthetic class and the entry point in the graph is that class's")
  md0(s"`$ENTRY_SYNTHETIC_METHOD`, not a method named `$HANDLER_METHOD`. Both are")
  md0("selected, so the difference between them is measured rather than assumed:")
  md0("")
  entryGroups.foreach { case (fn, nodes) =>
    md0(s"- `$fn` (${nodes.size} node(s), graph line ${lineOfMethod(nodes.head)})")
  }
  md0("")
  md0("## The sink")
  md0("")
  sinkCalls.foreach { c =>
    md0(s"- `${c.method.fullName}` calls `${c.methodFullName}` at graph line " +
      s"${lineOf(c)} (dispatch `${c.dispatchType}`)")
  }
  md0("")
  md0("Sink host methods a route must reach: " +
    sinkHostNames.toList.sorted.map(n => s"`$n`").mkString(", ") + ".")
  md0("")
  md0("## The four boundaries, as capability findings")
  md0("")
  md0("Each hop below is measured against the graph, not asserted. `crossed` states")
  md0("whether a CALL edge in fact joins the two ends.")
  md0("")
  boundaries.foreach { b =>
    md0(s"### ${b.id} - crossed by a call edge: **${b.crossedByACallEdge}**")
    md0("")
    md0(s"- **hop**: ${b.hop}")
    md0(s"- **from**: ${if (b.fromEnd.isEmpty) "(none measured)" else "`" + b.fromEnd + "`"}")
    md0(s"- **to**: ${if (b.toEnd.isEmpty) "(none measured)" else "`" + b.toEnd + "`"}")
    md0(s"- **reason**: ${b.reason}")
    md0(s"- **modelling**: ${b.modelling}")
    md0("")
  }
  md0("Boundaries not crossed: " +
    (if (boundariesNotCrossed.isEmpty) "none"
     else boundariesNotCrossed.map(b => s"`${b.id}`").mkString(", ")) + ".")
  md0("")
  md0("## The predicate set, and the source types it came from")
  md0("")
  md0(s"The mechanical definition: a route is spurious **only** where it passes an")
  md0("authorization or ACL predicate before reaching the sink. The predicate set is")
  md0("exactly these five Boolean methods, and their source is")
  md0("`core/src/main/scala/org/apache/spark/SecurityManager.scala` at the pin")
  md0("(457 lines), on the single source type `org.apache.spark.SecurityManager`:")
  md0("")
  md0("| predicate | source line at the pin |")
  md0("| --- | --- |")
  md0("| `aclsEnabled()` | 227 |")
  md0("| `checkAdminPermissions` | 234 |")
  md0("| `checkUIViewPermissions` | 248 |")
  md0("| `checkModifyPermissions` | 264 |")
  md0("| `isAuthenticationEnabled()` | 274 |")
  md0("")
  md0("`Master.scala:411`'s `if (state != RecoveryState.ALIVE)` is a **recovery-state**")
  md0("check and is deliberately not in this set.")
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
  md0("## Whether an expected-spurious route was absent")
  md0("")
  md0(s"`spurious_count` = **$spuriousCount**. No route in the emitted set passed an")
  md0("auth/ACL predicate as defined by these five named selectors.")
  md0("")
  if (absenceIsStructural) {
    md0("**The absence is structural, not a consequence of the query filtering well.**")
    md0(s"Measured against the graph: ${predicateCallSites.size} call sites of the five")
    md0(s"predicates exist graph-wide, in ${predicateCallerNames.size} distinct calling")
    md0("methods, and **" + predicateCallSitesOnRoute.size + "** of them sit on the route")
    md0("surface (" + ROUTE_SURFACE_TYPE_PREFIXES.map(p => s"`$p`").mkString(", ") + ").")
    md0("The predicate set exists and is invoked elsewhere in the program; it is not")
    md0("invoked anywhere on this route, so no route could have passed one.")
  } else {
    md0(s"Call sites of the five predicates DO exist on the route surface " +
      s"(${predicateCallSitesOnRoute.size} of ${predicateCallSites.size} graph-wide), so")
    md0("the count above reflects the query's filtering rather than a structural absence.")
  }
  md0("")
  md0("This is a statement about **this query's own output** under **this query's own**")
  md0("definition of the term. It is not an assessment of Spark, of any Spark component")
  md0("or of any configuration, and nothing here should be read as one.")
  md0("")
  md0("## Whether this formulation duplicates another query's")
  md0("")
  md0(s"`duplicate_formulation` = **$duplicateFormulationAggregate**.")
  md0("")
  md0("Every verdict below is **computed at run time**, by applying one shared predicate")
  md0("to the two queries' own declared formulation identity blocks read out of the two")
  md0("source files. No verdict about a sibling query is written down in this source, so")
  md0("there is nothing here that can drift from what that query publishes.")
  md0("")
  md0(duplicateFormulationAggregation + ".")
  md0("")
  relations.foreach { r =>
    val siblingDigest =
      if (r.theirs.established) s"source sha256 `${r.theirs.sourceSha256}`"
      else "source not read"
    md0(s"- Against `${r.theirs.queryId}` ($siblingDigest): **${r.status}**.")
    md0(s"  Scope: ${r.scope}.")
    md0(s"  Basis: ${r.basis}.")
    md0(s"  Joern API constructs only here: ${r.apiOnlyHere.size}; only there: " +
      s"${r.apiOnlyThere.size}; shared: ${r.apiShared.size}. Predicate selector " +
      s"literals identical: ${r.samePredicateSelectors}.")
  }
  md0("")
  md0(duplicateFormulationRelation + ".")
  md0("")
  md0("## The three effort measures")
  md0("")
  md0(s"1. **Query revisions committed: " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established") +
    ".** Convention: " + QUERY_REVISIONS_CONVENTION + ".")
  md0(s"   Measurement: $revisionsNote.")
  if (revisionsEstablished) {
    md0("   The commits counted, newest first, so the number is auditable rather than")
    md0("   asserted:")
    md0("")
    revisionCommits.foreach(c => md0(s"   - `$c`"))
    md0("")
  } else {
    md0("   The count is published as `null` rather than as a number, because a measure")
    md0("   this run could not establish is not a measure it may assert.")
    md0("")
  }
  md0(s"2. **Distinct Joern API constructs used: " +
    s"${JOERN_API_CONSTRUCTS.distinct.size}.** Listed explicitly and deduplicated so the")
  md0("   count is auditable from the list rather than asserted. Each entry was searched")
  md0("   for in this query's own source text with the list's own declaration excised")
  md0(s"   first, so no entry can satisfy itself: ${apiConstructsConfirmed.size} of")
  md0(s"   ${JOERN_API_CONSTRUCTS.distinct.size} were confirmed" +
    (if (apiConstructsNotConfirmed.isEmpty) "."
     else ", and the entries the search could not confirm are listed in the envelope " +
       "rather than dropped from the count."))
  md0("")
  JOERN_API_CONSTRUCTS.foreach(c => md0(s"   - `$c`"))
  md0("")
  md0(s"3. **Parameterizability: not claimed here.** It is proven by")
  md0(s"   `$PARAMETERIZABILITY_OWNER` actually invoking its parameterized form on the")
  md0("   second named handler/sink pair (the `deploy/rest/StandaloneRestServer` handler")
  md0("   to the `deploy/worker/DriverRunner` sink) and capturing that invocation's")
  md0("   result. A parameter list that merely exists does not satisfy it.")
  md0("")
  md0("## Modelling decisions, stated so the counts stay interpretable")
  md0("")
  md0("- **Operator pseudo-calls are excluded.** A CPG `<operator>.*` call is an")
  md0("  artefact of the representation, not a method call, and expanding them would")
  md0("  inflate every counter without adding a call-graph hop.")
  md0("- **Duplicate class definitions are unioned.** The graph carries more than one")
  md0("  node per class where two staged archives carried the same class, so method")
  md0("  nodes are grouped by full name and their call sites unioned rather than one")
  md0("  node being picked. Reachability is keyed on the method full name.")
  md0("- **Callee resolution is explicit.** Each call site's callees are taken from")
  md0("  `NoResolve.getCalledMethodsAsTraversal`, which is exactly the statically")
  md0("  linked CALL-edge callees of that site.")
  md0("- **Two walks, reported side by side.** Walk `A-follows-fan-out` expands every")
  md0("  call site. Walk `B-fan-out-recorded` records but does not expand a call site")
  md0(s"  whose resolved callee set exceeds $FANOUT_CALLEE_THRESHOLD distinct methods:")
  md0("  expanding such a site models \"any implementation in the program may be")
  md0("  invoked here\", which is a property of the call linker rather than of this")
  md0("  route. Both walks' counters are published above and their routes are")
  md0("  deduplicated, never summed.")
  md0("- **Graph line numbers are the graph's own.** A method or call node's")
  md0("  `lineNumber` comes from the bytecode line-number table and can differ by a")
  md0("  line from the `def` or statement line cited from the source. Source anchors in")
  md0("  this report are quoted from the pinned tree; graph lines are labelled as such.")
  md0("- **A bytecode file path is not a source path.** The frontend records an")
  md0("  extraction path under a temporary directory for every class, so this query")
  md0("  reports types, methods and lines rather than presenting that path as a source")
  md0("  location.")
  md0("")
  md0("## Reproducing this")
  md0("")
  md0("```")
  md0(REPRODUCTION_COMMAND)
  md0("```")
  md0("")
  md0("That is the **whole** command: the repository root, the JDK, the heap override,")
  md0("the log level and the script path. This query reads no other environment variable")
  md0("that changes what it loads or what it publishes, and in particular there is no")
  md0("override for the identity record - the record of account is")
  md0(s"`$CPG_RECORD_PATH`, so a load can never be adjudicated by a record this command")
  md0("does not name.")
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
  log(s"prose report staged       : $mdPath (${md.size} lines, " +
    s"${reportMember.byteSize} bytes, sha256 ${reportMember.sha256})")
  log(s"publication members staged: ${stagedMembers.size} of 3 " +
    "(the console log is staged last, because it names the other two)")

  // -------------------------------------------------------------------------
  stage("L-result: the result region, emitted only now that every stage passed")
  // -------------------------------------------------------------------------
  log(s"total elapsed_ms          : ${elapsedMs(runStartNanos)}")
  log(MARKER_RESULT_BEGIN)
  log(s"query_id                  : $QUERY_ID")
  log(s"compile_status            : compiled")
  log(s"run_status                : completed")
  log(s"returned_record_count     : $returnedRecordCount")
  log(s"distinct_routes           : ${distinctRoutes.size}")
  log(s"spurious_count            : $spuriousCount")
  log(s"expected_spurious_absent  : $expectedSpuriousAbsent " +
    s"(${if (absenceIsStructural) "structural" else "filtering"})")
  log(s"bound_value               : $MAX_CALL_DEPTH")
  log(s"bound_reached             : $boundReached")
  log(s"entry_points_traversed    : $entryPointsTraversed")
  log(s"entry_points_truncated    : $entryPointsTruncated")
  log(s"duplicate_formulation     : $duplicateFormulationAggregate")
  log(s"query_revisions_committed : " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established"))
  log(s"joern_api_constructs      : ${JOERN_API_CONSTRUCTS.distinct.size}")
  log(s"boundaries_not_crossed    : ${boundariesNotCrossed.map(_.id).mkString(", ")}")
  log(s"query_source_sha256       : $sourceSha256")
  log(s"publication_id            : $publicationId")
  log(s"envelope                  : $jsonPath")
  log(s"prose report              : $mdPath")
  log(MARKER_RESULT_END)
  log(MARKER_OK)

  // The publication, all three members at once. The two staged members are on
  // disk and fsynced; the console log is staged now, as the last member,
  // because its content names the other two - and only then is anything moved
  // onto a published path. A failure anywhere above this line leaves all three
  // targets holding their previous generation rather than a mixed one.
  logTargetPath.foreach { p =>
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
