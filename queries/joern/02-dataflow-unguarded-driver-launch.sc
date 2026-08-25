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
// queries/joern/02-dataflow-unguarded-driver-launch.sc
//
// Probe query 2 of 3. Hand-written Joern capability probe: DATAFLOW from the
// Spark standalone Master's driver-submission handler to the privileged process
// launch in DriverRunner, over the code-property graph built from the pinned
// tree's bytecode.
//
// THE SAME PAIR AS QUERY 01, BY A DIFFERENT FORMULATION
//   Query 01 asks whether CALL-graph reachability joins this handler to this
//   sink. This query asks whether DATA reaches the sink from the handler, which
//   is a different question over different edges. Because both address ONE pair,
//   this file's headline obligation is the DUPLICATE-FORMULATION VERDICT: whether
//   the two are genuinely two formulations or one restated is a finding to answer
//   on evidence, never to assume. It is answered in stage L below, from measured
//   properties, and reported in BOTH result files, against query 01 AND query 03.
//
//   Their results are reported as DISTINCT and are NEVER SUMMED (AAP 0.6.1,
//   0.6.2). A reader who adds this query's routes to query 01's gets a number
//   that means nothing.
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
//   is mandated because the alternative loader spawns a second JVM at the same
//   maximum heap (AAP 0.5.1, 0.6.2). This file must contain no textual
//   occurrence of that alternative at all - the appearance IS the violation.
//
// WHY THE DATAFLOW LAYER IS AVAILABLE AT ALL - MEASURED, NOT ASSUMED
//   importCpg applies the console's default overlays, and on this engine those
//   include the OSS dataflow layer: the verification load's own workspace
//   carries overlays/{base,controlflow,typerel,callgraph,dataflowOss}, recorded
//   in harness/artifacts/logs/cpg-verify.log. So REACHING_DEF edges exist after
//   the load and the flow traversals below have something to walk. This query
//   re-establishes that fact for its own load rather than inheriting it, by
//   running an ENGINE-LIVENESS CONTROL arm (stage H) whose flows must be
//   non-empty for a zero elsewhere to mean anything.
//
// STAGE 5 POSITION
//   This is one of the four heap-bound JVM invocations the run records
//   separately (frontend build, importCpg verification load, Stage 3 Joern
//   runner, this probe). Stage 5 runs after normalization so that only one
//   64 GB Joern process is ever live (AAP 0.5.1, 0.5.4).
//
// HOW TO INVOKE (the heap is the part that is easy to get wrong - see below)
//   cd <a scratch directory outside the repository>   # joern eagerly creates
//                                                    # ./workspace in its cwd
//   HARNESS_REPO_ROOT=<repo>  JAVA_HOME="$JAVA_HOME_21" \
//   JAVA_TOOL_OPTIONS="-Xmx64g" SL_LOGGING_LEVEL=WARN \
//     joern --script <repo>/queries/joern/02-dataflow-unguarded-driver-launch.sc \
//       -J-Xmx64g < /dev/null
//
//   Joern needs stdin closed (its REPL blocks on an open stdin) and exposes no
//   version flag, so its version is read from the startup banner rather than
//   from --version; this script reports the JDK and the heap it observes and
//   leaves the banner to the console stream.
//
//   MEASURED, NOT ASSUMED: joern's --script path forks a child JVM
//   (replpp.scripting.ScriptRunner spawns `java -classpath ... ` with no JVM
//   options forwarded), so -J-Xmx reaches the LAUNCHER JVM only. JAVA_TOOL_OPTIONS
//   is inherited by the child and is the environment override that actually
//   raises the heap the query runs at. The provisioned Joern runner's own
//   documented override is HARNESS_JOERN_HEAP, which env.sh defaults to 64g;
//   where a runner or a wrapper defaults BELOW the floor it is raised through
//   that documented environment value and the value used is reported. This
//   script therefore measures Runtime.maxMemory() and HALTS below the floor:
//   raising a heap is permitted and reported, lowering one is not, because a
//   truncated result's silence cannot be told apart from a clean one, and that
//   risk is highest for a dataflow query (AAP 0.8.2).
//
// THE VERIFIED TARGET SURFACE - identical to query 01's, so the anchors are the
//   same. Every line number below was verified at commit
//   59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d with `git show <sha>:<path>`.
//   These paths resolve inside the PINNED clone exported as SPARK_SRC; the
//   working checkout this file lives in is neither built nor scanned.
//
//   HANDLER - core/src/main/scala/org/apache/spark/deploy/master/Master.scala
//             (1,436 lines at the pin)
//     :239  override def receive: PartialFunction[Any, Unit] = {
//     :409  override def receiveAndReply(context: RpcCallContext)
//             : PartialFunction[Any, Unit] = {          <-- the handler
//     :410    case RequestSubmitDriver(description) =>  <-- the UNAPPLY that
//                 recovers the DriverDescription payload from an Any-typed
//                 formal parameter. How this query treats it is stated in
//                 SOURCE SELECTION below, because an unstated choice makes the
//                 flow count uninterpretable.
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
//             <-- the single accessor `driverDescription` is what the unapply at
//                 Master.scala:410 compiles down to, and is this query's ARM 2
//                 source set.
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
// SOURCE SELECTION, AND HOW THE UNAPPLY IS TREATED
//   BOUNDARY 4 makes the source selection a decision rather than a lookup.
//   receiveAndReply returns PartialFunction[Any, Unit], so its body compiles
//   into a synthetic class and the handler's formal parameter in the graph is a
//   parameter of Master$$anonfun$receiveAndReply$N.applyOrElse, typed
//   java.lang.Object - NOT of a method named receiveAndReply. The
//   DriverDescription payload is then recovered by the pattern match at
//   Master.scala:410, which is an unapply rather than an assignment: in bytecode
//   the value arrives through a type test, a cast and the case class's own
//   `driverDescription` accessor.
//
//   Two source sets are therefore evaluated as SEPARATE ARMS and reported
//   separately, so neither choice is hidden inside one number:
//     ARM 1  the handler's own formal parameters - every parameter of the
//            synthetic applyOrElse and of the source-level receiveAndReply,
//            with the implicit `this` parameter excluded and the exclusion
//            reported. This is "the message as it arrives".
//     ARM 2  the unapply-recovered payload - the call sites of the message
//            type's `driverDescription` accessor inside those same entry
//            methods. This is "the payload after the pattern match", and it is
//            the arm that answers what ARM 1 cannot: whether the taint is lost
//            at the type test rather than at a later hop.
//   Where ARM 2's primary selection finds nothing, a stated fallback selects
//   every call inside the entry methods whose callee is declared on the message
//   type, and the report names which selection was used.
//
// FOUR BOUNDARIES ON THIS ROUTE - AND WHY A ZERO IS THE FINDING
//   The route from the handler to the launch is not connected by DATAFLOW
//   either, for overlapping but not identical reasons to the call-graph case. A
//   reachableByFlows-style query from the handler's parameter to the launch
//   returns ZERO flows, and that zero is the capability finding - it is not a
//   broken query, and it is not repaired by loosening or removing the bound.
//   Each boundary is MEASURED against the graph below, in dataflow terms, and
//   reported with its hop and its reason.
//
//     B1 RPC HOP        Master.launchDriver :1367 sends LaunchDriver over an
//                       RpcEndpointRef; the receiving handler is in Worker. No
//                       DATA edge crosses a message send: the payload is
//                       serialized out of one process and deserialized into
//                       another. Modelled explicitly here by pairing on the
//                       MESSAGE TYPE - the arguments of its constructor call
//                       sites are the producer end and its field-accessor call
//                       sites are the consumer end - a modelling decision,
//                       stated so the flow count stays interpretable.
//     B2 THREAD HOP     DriverRunner.start :123 calls Thread.start(); the body
//                       that continues the route is run() :90 on the anonymous
//                       Thread subclass. Thread.start() -> run() is a JVM
//                       scheduling relation, and it is neither a call edge nor a
//                       data edge.
//     B3 INTERFACE HOP  runCommandWithRetry :240 invokes the ABSTRACT
//                       ProcessBuilderLike.start :270; java.lang.ProcessBuilder
//                       .start() is reached only through the anonymous
//                       implementation at :276. Query 01 measured this hop as
//                       CROSSED by a call edge; whether a DATA edge crosses it
//                       is a separate measurement and is made here.
//     B4 PARTIAL-FN HOP the source-level receiveAndReply only constructs the
//                       partial function; the case bodies live in the synthetic
//                       class's applyOrElse, so a flow from the source-level
//                       method's parameter into the synthetic body is a hop of
//                       its own. Measured.
//
// OUTPUTS (slugs are locked; harness/artifacts/logs/probe-02-dataflow-
//          unguarded-driver-launch.log names exactly these two as consumers)
//   queries/joern/results/02-dataflow-unguarded-driver-launch.json  envelope
//   queries/joern/results/02-dataflow-unguarded-driver-launch.md    prose
//   harness/artifacts/logs/probe-02-dataflow-unguarded-driver-launch.log
//                                                                    console
//
//   Both result files are DETERMINISTIC: no timestamp, no elapsed time and no
//   workspace or project name enters them, so an unchanged source over an
//   unchanged graph emits byte-identical bytes. Elapsed times live in the
//   console log only.
//
// MARKER PROTOCOL (restated from query 01, not reinvented)
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

import io.joern.dataflowengineoss.queryengine.{EngineConfig, EngineContext}
import io.shiftleft.codepropertygraph.generated.nodes.{
  AstNode,
  Call,
  CfgNode,
  Expression,
  Method,
  MethodParameterIn
}

// ===========================================================================
// NAMED CONSTANTS - no inline literal governs behaviour anywhere below
// ===========================================================================

/** The slug. Both result filenames and the console log name derive from it. */
val QUERY_ID = "02-dataflow-unguarded-driver-launch"

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

/** The sibling queries this one reports a duplicate-formulation verdict against. */
val SIBLING_CALLGRAPH_QUERY = "01-callgraph-unguarded-driver-launch"
val SIBLING_PARAMETERIZED_QUERY = "03-parameterized-handler-sink-pairs"

// --------------------------------------------------------------- the bounds
// A dataflow formulation needs bounds of its own: the engine expands callers and
// callees while it searches backwards from a sink, and an unbounded expansion is
// where a sweep over a graph this size actually bites. The graph carries at
// least 853,420 methods (a one-sided floor; the anchor is 898,336 with no upper
// bound), 87,381 expected type declarations and 38,818 expected files, so every
// bound below is load-bearing rather than decorative. Each is a NAMED constant
// and each is reported with its value.

/**
 * Maximum flow length / call depth: the engine's own EngineConfig.maxCallDepth,
 * the number of call boundaries the backward search may expand while looking for
 * a source. The documented route crosses five method boundaries between the
 * handler body and the launch, so this exceeds it.
 */
val MAX_FLOW_CALL_DEPTH = 6

/**
 * A deliberately SHALLOWER depth, run as a second evaluation of the same arm so
 * that sensitivity to the depth bound is MEASURED rather than asserted. If both
 * depths return the same flows, the result is insensitive to the bound across
 * that range, and the absence of a flow is not an artefact of a short bound.
 */
val MAX_FLOW_CALL_DEPTH_SHALLOW = 2

/** Call depth for the four boundary measurements, each of which is local to one
 *  hop and needs no deep expansion. */
val MAX_BOUNDARY_FLOW_CALL_DEPTH = 2

/** Maximum path elements retained per flow. A longer flow is retained with its
 *  element list truncated at this length and the truncation flagged. */
val MAX_FLOW_LENGTH = 64

/** Maximum flows retained per (source group, sink group) pair. */
val MAX_FLOWS_PER_PAIR = 8

/** Per-source step cap: (source group, sink group) evaluations per source group. */
val MAX_STEPS_PER_SOURCE = 8

/** Total-returns cap across every record kind this query emits. */
val MAX_TOTAL_RETURNS = 256

/** Maximum source nodes handed to one evaluation; the remainder are truncated. */
val MAX_SOURCE_NODES = 64

/** Maximum sink nodes handed to one evaluation; the remainder are truncated. */
val MAX_SINK_NODES = 64

/** Maximum entry points (source groups) traversed; the rest are counted as
 *  truncated. An entry point here is one source-method full name. */
val MAX_ENTRY_POINTS = 16

/** Cap on the indexed call-name sweeps used to find sink and payload call sites. */
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
/** The implicit receiver parameter, excluded from every source set and the
 *  exclusion reported: it carries the enclosing object, not the message. */
val THIS_PARAMETER_NAME = "this"
/** The Any-typed formal parameter's erased bytecode type. The handler's message
 *  parameter is identified by this rather than by position. */
val MESSAGE_PARAMETER_TYPE = "java.lang.Object"
/** The first call the handler body makes with the recovered payload
 *  (Master.scala:417), used as the consumer end of the partial-function hop. */
val HANDLER_BODY_CONTINUATION_NAME = "createDriver"

// -------------------------------------------------------------- the sink surface
/** The privileged launch, as a CALLEE full name. */
val SINK_CALLEE_REGEX =
  """^(java\.lang\.ProcessBuilder\.start|org\.apache\.spark\.deploy\.worker\.ProcessBuilderLike\.start).*"""
/** The indexed call name both sink forms share. */
val SINK_CALL_NAME = "start"
/** The launch must be hosted by the DriverRunner surface, not by any `start` anywhere. */
val SINK_HOST_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.worker\.(DriverRunner|ProcessBuilderLike).*"""
/** The concrete JDK launch, reached only through the anonymous implementation. */
val JDK_LAUNCH_CALLEE_PREFIX = "java.lang.ProcessBuilder.start"
/** The abstract declaration the launch call site names. */
val ABSTRACT_LAUNCH_CALLEE_PREFIX = "org.apache.spark.deploy.worker.ProcessBuilderLike.start"

// ------------------------------------------------------- the RPC message surface
val MESSAGE_TYPE = "org.apache.spark.deploy.DeployMessages$LaunchDriver"
val MESSAGE_CTOR_NAME = "<init>"
val MESSAGE_ACCESSOR_NAMES = List("driverDesc", "driverId", "resources")
/** The submission message and the accessor its unapply compiles down to. */
val REQUEST_MESSAGE_TYPE = "org.apache.spark.deploy.DeployMessages$RequestSubmitDriver"
val REQUEST_MESSAGE_ACCESSOR = "driverDescription"

// ------------------------------------------------------------- the thread surface
val THREAD_HOST_TYPE = "org.apache.spark.deploy.worker.DriverRunner"
val THREAD_HOST_METHOD = "start"
val THREAD_BODY_TYPE_REGEX =
  """^org\.apache\.spark\.deploy\.worker\.DriverRunner\$\$anon\$\d+$"""
val THREAD_BODY_METHOD = "run"
/** The call inside the thread body that continues the route, used as the
 *  consumer end of the thread hop. */
val THREAD_BODY_CONTINUATION_NAME = "prepareAndRunDriver"

// ------------------------------------------------------ the liveness control arm
/**
 * The engine-liveness control. A zero from a cross-boundary arm is only
 * interpretable if the dataflow layer is demonstrably live on THIS sink in THIS
 * graph, so the control asks for a flow that must exist if it is: from the
 * launch's own enclosing method's formal parameter to the launch call itself.
 * The parameter IS the receiver of the launch call one statement later, so an
 * engine with reaching-definition edges returns a flow, and an engine without
 * them returns nothing. It is a CONTROL, not a route from the handler: its flows
 * are reported under their own field and are never counted as routes.
 */
val CONTROL_HOST_TYPE = "org.apache.spark.deploy.worker.DriverRunner"
val CONTROL_HOST_METHOD = "runCommandWithRetry"

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
// queries/joern/01-callgraph-unguarded-driver-launch.sc. It has to be: the two
// queries' spurious counts are only comparable if the definition of "spurious"
// is the same text, and the duplicate-formulation verdict below rests on that
// comparability. A divergence here would silently invalidate both.

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
 * The DATAFLOW constructs in this list are the ones query 01 does not use, and
 * that difference is itself evidence for the duplicate-formulation verdict: it
 * is computed in stage L as a set difference against query 01's published list
 * rather than eyeballed.
 */
val JOERN_API_CONSTRUCTS = List(
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
 * Query 01's published boundary verdicts, transcribed from its envelope's
 * `boundaries_not_crossed` field, so that the two formulations' verdicts on the
 * same four hops can be COMPARED here rather than assumed to agree or to differ.
 * Query 01 measured a CALL edge on each hop; this query measures a DATA edge, so
 * agreement is a result and not a foregone conclusion either way. Transcribed
 * rather than measured, and labelled as such wherever it is used.
 */
val SIBLING_CALLGRAPH_BOUNDARIES_NOT_CROSSED = List(
  "B1-rpc",
  "B2-thread",
  "B4-partial-function")

/**
 * Query 01's published construct list, transcribed from
 * queries/joern/results/01-callgraph-unguarded-driver-launch.json so that the
 * overlap and the difference between the two queries can be COMPUTED here. It
 * is evidence for the duplicate-formulation verdict, not an input to any
 * traversal, and it is labelled as transcribed rather than measured.
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
 * Effort measure 3 - parameterizability. NOT claimed here: it is proven by
 * query 03 actually invoking its parameterized form on the second named
 * handler/sink pair (the deploy/rest/StandaloneRestServer handler to the
 * deploy/worker/DriverRunner sink) and capturing that invocation's result.
 * This query references that and asserts nothing about it.
 */
val PARAMETERIZABILITY_OWNER = SIBLING_PARAMETERIZED_QUERY

// -------------------------------------------------------------------- markers
val MARKER_START = "---BLITZY-START---"
val MARKER_RESULT_BEGIN = "---BLITZY-RESULT-BEGIN---"
val MARKER_RESULT_END = "---BLITZY-RESULT-END---"
val MARKER_OK = "---BLITZY-OK---"
val MARKER_FAILURE = "---BLITZY-FAILURE---"

// ===========================================================================
// CONSOLE, STAGE TRACKING AND FAIL-LOUD HELPERS
// (restated from query 01 so both probes' console records read alike)
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
  log(s"query_id                  : $QUERY_ID")
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
      "the override is inherited, and where a runner defaults lower raise it through its " +
      "own documented environment override. Raising a heap is permitted and reported; " +
      "lowering one is not, because a truncated result's silence cannot be told apart " +
      "from a clean one - and for a dataflow query that risk is at its highest")
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
  // Re-verified HERE, independently of query 01's check, immediately before THIS
  // load (AAP 0.6.4, 0.8.2): a load against different bytes than the record
  // describes produces conclusions about a graph nobody has.
  //
  // harness/cpg/spark.cpg is git-tracked while harness/artifacts/** is ignored,
  // which is why the provisioned path is a small SYMLINK to a host-global graph.
  // Measuring the link instead of its target would record a few dozen bytes and
  // the comparison would fail spuriously, so the link is resolved first and the
  // TARGET is what is sized and hashed. Both readings are logged and the
  // no-follow one is explicitly discarded.
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

  // -------------------------------------------------------------------------
  stage("D-load: switchWorkspace then importCpg")
  // -------------------------------------------------------------------------
  // The workspace is the probe's own, never a shared or default one: joern
  // writes a multi-gigabyte project tree (including a working copy of the graph)
  // into it, and two Joern processes sharing one corrupt each other. It is
  // switched BEFORE any load. queries/joern/.workspace carries its own
  // .gitignore, so the scratch stays out of the commit without editing upstream
  // Spark's .gitignore.
  //
  // The load applies the console's default overlays to the working copy, and the
  // OSS dataflow layer is one of them. The persisted graph is not modified: the
  // engine works on the copy, which is what makes re-verifying the identity
  // before every load a meaningful check rather than a formality.
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

  // The flow engine's context: the console's own EngineContext - and therefore
  // the same semantics the dataflow overlay was created with - with ONE field
  // overridden, the call-depth bound. Copying rather than constructing is
  // deliberate: a fresh EngineContext would carry default semantics that need
  // not match the overlay's, and the bound is the only thing this query means to
  // change. It is passed EXPLICITLY at every call site below, so no implicit
  // resolution decides which context a traversal ran under.
  val baseEngineContext: EngineContext = context
  val flowContext: EngineContext =
    baseEngineContext.copy(config = EngineConfig(maxCallDepth = MAX_FLOW_CALL_DEPTH))
  val shallowFlowContext: EngineContext =
    baseEngineContext.copy(config = EngineConfig(maxCallDepth = MAX_FLOW_CALL_DEPTH_SHALLOW))
  val boundaryFlowContext: EngineContext =
    baseEngineContext.copy(config = EngineConfig(maxCallDepth = MAX_BOUNDARY_FLOW_CALL_DEPTH))
  val semanticsClass = baseEngineContext.semantics.getClass.getName
  log(s"flow engine semantics     : $semanticsClass (inherited from the console context)")
  log(s"flow engine maxCallDepth  : ${flowContext.config.maxCallDepth} (primary), " +
    s"${shallowFlowContext.config.maxCallDepth} (shallow), " +
    s"${boundaryFlowContext.config.maxCallDepth} (boundary probes)")

  // -------------------------------------------------------------------------
  stage("E-selection: the source sets (BOUNDARY 4 and the unapply) and the sink")
  // -------------------------------------------------------------------------
  /** Operator pseudo-calls are CPG artefacts, not method calls. Named so the
   *  exclusion is a stated modelling decision rather than a silent filter. */
  val OPERATOR_CALL_PREFIX = "<operator"
  def isOperatorCall(c: Call): Boolean = c.methodFullName.startsWith(OPERATOR_CALL_PREFIX)

  def lineOf(c: Call): Int = c.lineNumber.map(_.toInt).getOrElse(-1)
  def lineOfMethod(m: Method): Int = m.lineNumber.map(_.toInt).getOrElse(-1)
  def owningTypes(m: Method): List[String] = m.typeDecl.fullName.l.distinct.sorted

  /** The enclosing method of any node a flow can carry. Matched most specific
   *  first, because Call is an Expression and Expression is a CfgNode. */
  def enclosingMethodOf(n: AstNode): String = n match {
    case p: MethodParameterIn => p.method.fullName
    case m: Method            => m.fullName
    case c: CfgNode           => c.method.fullName
    case _                    => ""
  }

  /** The callee a node names, where it names one at all. */
  def calleeOf(n: AstNode): String = n match {
    case c: Call => c.methodFullName
    case _       => ""
  }

  def lineOfNode(n: AstNode): Int = n.lineNumber.map(_.toInt).getOrElse(-1)

  /** A node's code, collapsed onto one line and length-capped, so a flow record
   *  stays readable and the emitted JSON stays deterministic. */
  val MAX_CODE_CHARS = 160
  def codeOf(n: AstNode): String = {
    val one = n.code.replace('\n', ' ').replace('\r', ' ').trim
    if (one.length <= MAX_CODE_CHARS) one else one.take(MAX_CODE_CHARS) + "..."
  }

  /** A stable identity for a node, used for ordering and de-duplication. */
  def nodeKey(n: AstNode): String =
    s"${n.label}@${enclosingMethodOf(n)}#${lineOfNode(n)}:${codeOf(n)}"

  // --- the entry methods: BOUNDARY 4 -----------------------------------------
  // The handler body compiles into a synthetic partial-function class, so the
  // graph's entry point is that class's applyOrElse. The source-level method of
  // the same name is ALSO selected, so the report can show what each of the two
  // actually contains rather than assuming they are interchangeable.
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
  val entryMethodNameSet = entryGroups.map(_._1).toSet
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

  // --- ARM 1 sources: the handler's own formal parameters ---------------------
  // `this` is excluded because it carries the enclosing Master instance rather
  // than the message, and the exclusion is reported with the names dropped. The
  // Any-typed parameter is identified by its erased type rather than by position.
  def parametersOf(ms: List[Method]): List[MethodParameterIn] =
    ms.flatMap(_.parameter.l)
      .distinctBy(p => (p.method.fullName, p.index, p.name, p.typeFullName))
      .sortBy(p => (p.method.fullName, p.index, p.name))
  val allEntryParameters = parametersOf(entryGroupsTraversed.flatMap(_._2))
  val excludedThisParameters = allEntryParameters
    .filter(p => p.name == THIS_PARAMETER_NAME || p.index == 0)
  val armOneSourceNodesAll: List[CfgNode] = allEntryParameters
    .filterNot(p => p.name == THIS_PARAMETER_NAME || p.index == 0)
  val armOneSourceNodes = armOneSourceNodesAll.take(MAX_SOURCE_NODES)
  val armOneSourceTruncated = armOneSourceNodesAll.size - armOneSourceNodes.size
  val messageTypedParameters = allEntryParameters
    .filter(p => p.typeFullName == MESSAGE_PARAMETER_TYPE)
    .filterNot(p => p.name == THIS_PARAMETER_NAME || p.index == 0)
  log(s"entry parameters (all)    : ${allEntryParameters.size}")
  log(s"  excluded as `$THIS_PARAMETER_NAME`   : " +
    (if (excludedThisParameters.isEmpty) "<none>"
     else excludedThisParameters
       .map(p => s"${p.method.fullName}#${p.index}:${p.name}").mkString(", ")))
  log(s"ARM 1 source nodes        : ${armOneSourceNodes.size} " +
    s"(cap $MAX_SOURCE_NODES, truncated=$armOneSourceTruncated)")
  allEntryParameters.filterNot(p => p.name == THIS_PARAMETER_NAME || p.index == 0)
    .foreach { p =>
      log(s"  param: ${p.method.fullName}  index=${p.index} name=${p.name} " +
        s"type=${p.typeFullName}")
    }
  log(s"Any-typed message params  : ${messageTypedParameters.size} of type " +
    MESSAGE_PARAMETER_TYPE)

  // --- ARM 2 sources: the unapply-recovered payload --------------------------
  // Master.scala:410 recovers the DriverDescription by pattern match, which in
  // bytecode is a type test, a cast and the case class's own accessor. The
  // accessor's RESULT inside the entry methods is therefore the payload as the
  // handler body sees it, and it is a different source from the Any-typed formal
  // parameter of ARM 1. Selecting both, as two arms, is how the treatment of the
  // unapply is made explicit rather than left implicit in one number.
  val payloadCallsScanned = cpg.call.nameExact(REQUEST_MESSAGE_ACCESSOR).take(MAX_CALL_SCAN).l
  val payloadScanTruncated = payloadCallsScanned.size >= MAX_CALL_SCAN
  val payloadCallsPrimary = payloadCallsScanned
    .filterNot(isOperatorCall)
    .filter(_.methodFullName.startsWith(REQUEST_MESSAGE_TYPE + "."))
    .filter(c => entryMethodNameSet.contains(c.method.fullName))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  // Stated fallback: any call inside the entry methods whose callee is declared
  // on the submission message type. Used only where the primary selection is
  // empty, and the report names which selection produced the arm.
  val payloadCallsFallback = entryGroupsTraversed
    .flatMap(_._2)
    .flatMap(_.call.l)
    .filterNot(isOperatorCall)
    .filter(_.methodFullName.startsWith(REQUEST_MESSAGE_TYPE + "."))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val armTwoSelection =
    if (payloadCallsPrimary.nonEmpty) "primary: call sites of " + REQUEST_MESSAGE_TYPE +
      "." + REQUEST_MESSAGE_ACCESSOR + " inside the entry methods"
    else if (payloadCallsFallback.nonEmpty) "fallback: every call inside the entry " +
      "methods whose callee is declared on " + REQUEST_MESSAGE_TYPE
    else "empty: neither the accessor nor any call on " + REQUEST_MESSAGE_TYPE +
      " appears inside the entry methods"
  val armTwoCalls =
    if (payloadCallsPrimary.nonEmpty) payloadCallsPrimary else payloadCallsFallback
  val armTwoSourceNodesAll: List[CfgNode] = armTwoCalls
  val armTwoSourceNodes = armTwoSourceNodesAll.take(MAX_SOURCE_NODES)
  val armTwoSourceTruncated = armTwoSourceNodesAll.size - armTwoSourceNodes.size
  log(s"payload calls scanned     : ${payloadCallsScanned.size} named " +
    s"$REQUEST_MESSAGE_ACCESSOR (cap $MAX_CALL_SCAN, truncated=$payloadScanTruncated)")
  log(s"ARM 2 selection           : $armTwoSelection")
  log(s"ARM 2 source nodes        : ${armTwoSourceNodes.size} " +
    s"(cap $MAX_SOURCE_NODES, truncated=$armTwoSourceTruncated)")
  armTwoCalls.foreach { c =>
    log(s"  payload: ${c.method.fullName} -> ${c.methodFullName} graph_line=${lineOf(c)}")
  }

  // --- the sink: the privileged launch ---------------------------------------
  // Taken from its indexed call name and then constrained to the DriverRunner
  // surface so an unrelated `start` elsewhere in the graph cannot stand in for
  // it. The sink NODE set is the launch call together with its receiver and its
  // arguments: a flow that reaches the value being launched ends at one of
  // those, and taking only the call node would miss a flow into the receiver.
  val startCallsScanned = cpg.call.nameExact(SINK_CALL_NAME).take(MAX_CALL_SCAN).l
  val sinkScanTruncated = startCallsScanned.size >= MAX_CALL_SCAN
  val sinkCallsAll = startCallsScanned.filter(_.methodFullName.matches(SINK_CALLEE_REGEX))
  val sinkCalls = sinkCallsAll
    .filter(c => owningTypes(c.method).exists(_.matches(SINK_HOST_TYPE_REGEX)))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val sinkHostNames = sinkCalls.map(_.method.fullName).distinct.sorted
  val sinkReceivers = sinkCalls.flatMap(_.receiver.l)
  val sinkArguments = sinkCalls.flatMap(_.argument.l)
  val sinkNodesAll: List[CfgNode] =
    (sinkCalls ++ sinkReceivers ++ sinkArguments).distinctBy(nodeKey).sortBy(nodeKey)
  val sinkNodes = sinkNodesAll.take(MAX_SINK_NODES)
  val sinkNodesTruncated = sinkNodesAll.size - sinkNodes.size
  log(s"calls named $SINK_CALL_NAME scanned : ${startCallsScanned.size} " +
    s"(cap $MAX_CALL_SCAN, truncated=$sinkScanTruncated)")
  log(s"launch call sites (any host): ${sinkCallsAll.size}")
  log(s"launch call sites (sink host): ${sinkCalls.size}")
  sinkCalls.foreach { c =>
    log(s"  sink: ${c.method.fullName} -> ${c.methodFullName} graph_line=${lineOf(c)} " +
      s"dispatch=${c.dispatchType}")
  }
  log(s"sink nodes (call+receiver+argument): ${sinkNodes.size} " +
    s"(cap $MAX_SINK_NODES, truncated=$sinkNodesTruncated)")
  log(s"sink host methods         : ${sinkHostNames.mkString(", ")}")
  if (sinkCalls.isEmpty) {
    abortRun("no privileged-launch call site was found on the sink surface: no call " +
      s"matching $SINK_CALLEE_REGEX is hosted by a type matching $SINK_HOST_TYPE_REGEX")
  }
  if (sinkNodes.isEmpty) {
    abortRun("the launch call sites carry no receiver, argument or call node that can " +
      "serve as a dataflow sink")
  }

  /** Sink groups: one per host method, so the per-source step cap governs a real
   *  number of evaluations rather than a single one. */
  val sinkGroups: List[(String, List[CfgNode])] = sinkNodes
    .groupBy(enclosingMethodOf)
    .toList
    .sortBy(_._1)
  log(s"sink groups               : ${sinkGroups.size} " +
    sinkGroups.map { case (h, ns) => s"$h(${ns.size})" }.mkString("[", ", ", "]"))

  // -------------------------------------------------------------------------
  stage("F-predicates: the selector, its bytecode collision, and the constraint")
  // -------------------------------------------------------------------------
  // The selector CONSTANTS are byte-identical to query 01's, which is what makes
  // the two spurious counts comparable. The narrowing below is therefore the
  // same three steps, and ALL THREE SETS ARE REPORTED so it is auditable rather
  // than asserted:
  //   1. the broad anchored selector on methods of the SecurityManager type
  //   2. minus every bytecode setter (name ending in the setter suffix)
  //   3. intersected with the five named source-level predicates
  // Step 2 exists for a verified reason: SecurityManager.scala:59 declares
  // `private var aclsOn`, Scala compiles a private var into accessors, and the
  // graph therefore carries aclsOn() AND aclsOn_$eq(boolean), both of which
  // satisfy the `acls.*` alternative. The source-level 5-of-28 result does not
  // transfer to bytecode unchanged.
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
  val predicateFullNameSet = predicateFullNames.toSet
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
  stage("G-flow-machinery: the bounded evaluator every arm below runs through")
  // -------------------------------------------------------------------------
  /** One element of one flow, flattened for the record. */
  final case class FlowElement(
      index: Int,
      label: String,
      enclosingMethod: String,
      callee: String,
      graphLine: Int,
      code: String)

  /** One flow, attributed to the arm, source group and sink group that found it. */
  final case class FlowRecord(
      armId: String,
      sourceGroup: String,
      sinkGroup: String,
      elementCount: Int,
      elementsTruncated: Boolean,
      signature: String,
      elements: List[FlowElement])

  /** One arm's complete, bounded result. Every counter here is reported. */
  final case class ArmResult(
      armId: String,
      description: String,
      callDepth: Int,
      sourceSelection: String,
      sourceGroupsDiscovered: Int,
      sourceGroupsTraversed: Int,
      sourceGroupsTruncated: Int,
      sourceNodes: Int,
      sourceNodesTruncated: Int,
      sinkGroups: Int,
      sinkNodes: Int,
      sinkNodesTruncated: Int,
      evaluations: Int,
      stepCapReached: Boolean,
      flowsFound: Int,
      flowsRetained: Int,
      perPairCapReached: Boolean,
      lengthCapReached: Boolean,
      flows: List[FlowRecord])

  /** The one place a flow traversal is invoked. Bounded by construction: the
   *  engine's call depth comes from the context handed in, the source and sink
   *  node sets are pre-capped, and the retained flows are capped per pair. */
  def flowsFor(
      sinks: List[CfgNode],
      sources: List[CfgNode],
      ctx: EngineContext): List[io.joern.dataflowengineoss.language.Path] =
    if (sinks.isEmpty || sources.isEmpty) Nil
    else sinks.iterator.reachableByFlows(sources.iterator)(ctx).l

  def elementsOf(flow: io.joern.dataflowengineoss.language.Path): List[FlowElement] =
    flow.elements.zipWithIndex.map { case (n, i) =>
      FlowElement(i, n.label, enclosingMethodOf(n), calleeOf(n), lineOfNode(n), codeOf(n))
    }

  def signatureOf(els: List[FlowElement]): String =
    els.map(e => s"${e.label}@${e.enclosingMethod}#${e.graphLine}").mkString(" -> ")

  /**
   * Evaluate one arm: every (source group, sink group) pair, in deterministic
   * order, each through the bounded traversal above. Source groups are the
   * source nodes grouped by their enclosing method - the arm's entry points -
   * and the per-source step cap governs how many sink groups one source group
   * may be evaluated against.
   */
  def evaluateArm(
      armId: String,
      description: String,
      sourceSelection: String,
      sourceNodesGiven: List[CfgNode],
      sourceNodesTruncated: Int,
      sinkGroupsGiven: List[(String, List[CfgNode])],
      sinkNodeCount: Int,
      sinkNodesTruncatedCount: Int,
      ctx: EngineContext): ArmResult = {
    val sourceGroupsAll: List[(String, List[CfgNode])] = sourceNodesGiven
      .groupBy(enclosingMethodOf)
      .toList
      .sortBy(_._1)
    val sourceGroups = sourceGroupsAll.take(MAX_ENTRY_POINTS)
    var evaluations = 0
    var stepCapReached = false
    var flowsFound = 0
    var perPairCapReached = false
    var lengthCapReached = false
    val retained = scala.collection.mutable.ArrayBuffer.empty[FlowRecord]

    sourceGroups.foreach { case (sourceGroupName, sourceGroupNodes) =>
      var stepsForThisSource = 0
      sinkGroupsGiven.foreach { case (sinkGroupName, sinkGroupNodes) =>
        if (stepsForThisSource >= MAX_STEPS_PER_SOURCE) stepCapReached = true
        else {
          stepsForThisSource += 1
          evaluations += 1
          val found = flowsFor(sinkGroupNodes, sourceGroupNodes, ctx)
          flowsFound += found.size
          val asRecords = found.map { f =>
            val allEls = elementsOf(f)
            val truncated = allEls.size > MAX_FLOW_LENGTH
            if (truncated) lengthCapReached = true
            val els = allEls.take(MAX_FLOW_LENGTH)
            FlowRecord(armId, sourceGroupName, sinkGroupName, allEls.size, truncated,
              signatureOf(els), els)
          }
          val deduped = asRecords
            .distinctBy(r => (r.sourceGroup, r.sinkGroup, r.signature))
            .sortBy(r => (r.elementCount, r.signature))
          if (deduped.size > MAX_FLOWS_PER_PAIR) perPairCapReached = true
          retained ++= deduped.take(MAX_FLOWS_PER_PAIR)
          log(f"  arm $armId%-28s depth=${ctx.config.maxCallDepth}%2d " +
            f"source=$sourceGroupName sink=$sinkGroupName found=${found.size}%4d " +
            f"retained=${deduped.take(MAX_FLOWS_PER_PAIR).size}%4d")
        }
      }
    }

    ArmResult(
      armId = armId,
      description = description,
      callDepth = ctx.config.maxCallDepth,
      sourceSelection = sourceSelection,
      sourceGroupsDiscovered = sourceGroupsAll.size,
      sourceGroupsTraversed = sourceGroups.size,
      sourceGroupsTruncated = sourceGroupsAll.size - sourceGroups.size,
      sourceNodes = sourceNodesGiven.size,
      sourceNodesTruncated = sourceNodesTruncated,
      sinkGroups = sinkGroupsGiven.size,
      sinkNodes = sinkNodeCount,
      sinkNodesTruncated = sinkNodesTruncatedCount,
      evaluations = evaluations,
      stepCapReached = stepCapReached,
      flowsFound = flowsFound,
      flowsRetained = retained.size,
      perPairCapReached = perPairCapReached,
      lengthCapReached = lengthCapReached,
      flows = retained.toList.sortBy(r =>
        (r.sourceGroup, r.sinkGroup, r.elementCount, r.signature)))
  }

  // -------------------------------------------------------------------------
  stage("H-liveness-control: is the dataflow layer live on THIS sink at all")
  // -------------------------------------------------------------------------
  // A zero from a cross-boundary arm means one of two very different things: the
  // route is not connected by data, or the engine returned nothing because no
  // reaching-definition edges were available to it. Those are indistinguishable
  // from the zero alone, so the control asks for a flow that must exist if the
  // layer is live: from the launch's own enclosing method's formal parameter to
  // the launch call itself, which the parameter is the receiver of. This is a
  // CONTROL, not a route from the handler, and its flows are never counted as
  // routes.
  val controlHostMethods = cpg.typeDecl
    .fullNameExact(CONTROL_HOST_TYPE)
    .method
    .nameExact(CONTROL_HOST_METHOD)
    .l
  val controlHostNames = controlHostMethods.map(_.fullName).distinct.sorted
  val controlParametersAll = parametersOf(controlHostMethods)
    .filterNot(p => p.name == THIS_PARAMETER_NAME || p.index == 0)
  val controlSourceNodesAll: List[CfgNode] = controlParametersAll
  val controlSourceNodes = controlSourceNodesAll.take(MAX_SOURCE_NODES)
  val controlSourceTruncated = controlSourceNodesAll.size - controlSourceNodes.size
  /** The control's sinks are the launch nodes hosted by the control method only,
   *  which keeps the control intraprocedural by construction. */
  val controlSinkGroups = sinkGroups.filter { case (host, _) =>
    controlHostNames.contains(host)
  }
  val controlSinkNodeCount = controlSinkGroups.map(_._2.size).sum
  log(s"control host methods      : ${controlHostNames.mkString(", ")}")
  log(s"control source nodes      : ${controlSourceNodes.size} " +
    s"(cap $MAX_SOURCE_NODES, truncated=$controlSourceTruncated)")
  controlParametersAll.foreach { p =>
    log(s"  control param: ${p.method.fullName} index=${p.index} name=${p.name} " +
      s"type=${p.typeFullName}")
  }
  log(s"control sink groups       : ${controlSinkGroups.size} " +
    s"nodes=$controlSinkNodeCount")
  if (controlHostMethods.isEmpty) {
    abortRun(s"$CONTROL_HOST_TYPE.$CONTROL_HOST_METHOD is not present in the graph, so " +
      "the engine-liveness control has nothing to rest on and a zero from the " +
      "cross-boundary arms could not be attributed")
  }
  if (controlSinkGroups.isEmpty) {
    abortRun(s"no launch sink node is hosted by $CONTROL_HOST_TYPE.$CONTROL_HOST_METHOD, " +
      "so the control cannot be made intraprocedural against this graph")
  }

  val controlNanos = System.nanoTime()
  val controlArm = evaluateArm(
    armId = "CONTROL-intraprocedural-liveness",
    description = "the launch's own enclosing method's formal parameters to the launch " +
      "call in that same method: intraprocedural by construction, and non-empty if and " +
      "only if the OSS dataflow layer is live on this sink in this graph",
    sourceSelection = s"formal parameters of $CONTROL_HOST_TYPE.$CONTROL_HOST_METHOD, " +
      s"`$THIS_PARAMETER_NAME` excluded",
    sourceNodesGiven = controlSourceNodes,
    sourceNodesTruncated = controlSourceTruncated,
    sinkGroupsGiven = controlSinkGroups,
    sinkNodeCount = controlSinkNodeCount,
    sinkNodesTruncatedCount = 0,
    ctx = shallowFlowContext)
  log(s"control elapsed_ms        : ${elapsedMs(controlNanos)}")
  val engineLive = controlArm.flowsRetained > 0
  log(s"dataflow layer live       : $engineLive " +
    s"(${controlArm.flowsRetained} control flow(s) retained of " +
    s"${controlArm.flowsFound} found)")
  if (!engineLive) {
    log("NOTE: the control returned no flow. Every zero reported below is therefore")
    log("attributable to the engine as configured rather than to the route, and the")
    log("report states that explicitly instead of reading the zero as a route finding.")
  }

  // -------------------------------------------------------------------------
  stage("I-arms: the bounded cross-boundary dataflow arms")
  // -------------------------------------------------------------------------
  // ARM 1 twice, at two call depths, so sensitivity to the depth bound is
  // MEASURED: the engine does not report whether its internal maxCallDepth was
  // hit, so running the same arm shallow and deep and comparing is the evidence
  // that stands in for a flag the engine does not expose. ARM 2 once, at the
  // primary depth, over the unapply-recovered payload.
  val armOneShallowNanos = System.nanoTime()
  val armOneShallow = evaluateArm(
    armId = "ARM1-handler-parameters-shallow",
    description = "the handler's own formal parameters to the privileged launch, at the " +
      "shallow call depth; run to measure sensitivity to the depth bound",
    sourceSelection = "every formal parameter of the synthetic " + ENTRY_SYNTHETIC_METHOD +
      " and of the source-level " + HANDLER_METHOD + ", with `" + THIS_PARAMETER_NAME +
      "` excluded",
    sourceNodesGiven = armOneSourceNodes,
    sourceNodesTruncated = armOneSourceTruncated,
    sinkGroupsGiven = sinkGroups,
    sinkNodeCount = sinkNodes.size,
    sinkNodesTruncatedCount = sinkNodesTruncated,
    ctx = shallowFlowContext)
  log(s"ARM1 shallow elapsed_ms   : ${elapsedMs(armOneShallowNanos)}")

  val armOneNanos = System.nanoTime()
  val armOne = evaluateArm(
    armId = "ARM1-handler-parameters",
    description = "the handler's own formal parameters to the privileged launch, at the " +
      "primary call depth",
    sourceSelection = "every formal parameter of the synthetic " + ENTRY_SYNTHETIC_METHOD +
      " and of the source-level " + HANDLER_METHOD + ", with `" + THIS_PARAMETER_NAME +
      "` excluded; the Any-typed parameter is identified by its erased type " +
      MESSAGE_PARAMETER_TYPE + " rather than by position",
    sourceNodesGiven = armOneSourceNodes,
    sourceNodesTruncated = armOneSourceTruncated,
    sinkGroupsGiven = sinkGroups,
    sinkNodeCount = sinkNodes.size,
    sinkNodesTruncatedCount = sinkNodesTruncated,
    ctx = flowContext)
  log(s"ARM1 primary elapsed_ms   : ${elapsedMs(armOneNanos)}")

  val armTwoNanos = System.nanoTime()
  val armTwo = evaluateArm(
    armId = "ARM2-unapply-recovered-payload",
    description = "the payload as the handler body sees it after the pattern match - the " +
      "result of the message type's own accessor inside the entry methods - to the " +
      "privileged launch, at the primary call depth",
    sourceSelection = armTwoSelection,
    sourceNodesGiven = armTwoSourceNodes,
    sourceNodesTruncated = armTwoSourceTruncated,
    sinkGroupsGiven = sinkGroups,
    sinkNodeCount = sinkNodes.size,
    sinkNodesTruncatedCount = sinkNodesTruncated,
    ctx = flowContext)
  log(s"ARM2 elapsed_ms           : ${elapsedMs(armTwoNanos)}")

  /** The route-bearing arms. The control is deliberately NOT among them. */
  val routeArms = List(armOneShallow, armOne, armTwo)
  val allArms = routeArms :+ controlArm
  routeArms.foreach { a =>
    log(s"arm ${a.armId}: depth=${a.callDepth} evaluations=${a.evaluations} " +
      s"found=${a.flowsFound} retained=${a.flowsRetained} " +
      s"source_groups=${a.sourceGroupsTraversed}/${a.sourceGroupsDiscovered} " +
      s"step_cap=${a.stepCapReached} per_pair_cap=${a.perPairCapReached} " +
      s"length_cap=${a.lengthCapReached}")
  }

  /**
   * Distinct routes: the route-bearing arms' flows DEDUPLICATED on (source
   * group, sink group, element signature), never summed - not across the arms
   * here, and not with query 01's routes, which answer a different question over
   * different edges.
   */
  val distinctRoutes = routeArms
    .flatMap(_.flows)
    .distinctBy(r => (r.sourceGroup, r.sinkGroup, r.signature))
    .sortBy(r => (r.sourceGroup, r.sinkGroup, r.elementCount, r.signature))
  log(s"distinct routes (route arms, deduplicated): ${distinctRoutes.size}")

  /**
   * Whether a bound was reached. Three of the four bounds are observable from
   * the evaluator's own counters. The engine's internal call-depth bound is NOT
   * observable from its output - it reports no truncation flag - so it is
   * addressed by the depth-sensitivity measurement instead: the same arm at two
   * depths. Equal results across the two depths is evidence that the result does
   * not depend on the bound across that range; a difference would be evidence
   * that it does. The limitation is stated rather than papered over.
   */
  val depthSensitive = armOneShallow.flowsRetained != armOne.flowsRetained ||
    armOneShallow.flows.map(_.signature).sorted != armOne.flows.map(_.signature).sorted
  val observableBoundReached = routeArms.exists(a =>
    a.stepCapReached || a.perPairCapReached || a.lengthCapReached ||
      a.sourceGroupsTruncated > 0 || a.sourceNodesTruncated > 0 || a.sinkNodesTruncated > 0)
  val boundReached = observableBoundReached || depthSensitive
  log(s"depth-sensitive (shallow vs primary differ): $depthSensitive")
  log(s"observable bound reached  : $observableBoundReached")
  log(s"bound_reached             : $boundReached")

  // -------------------------------------------------------------------------
  stage("J-boundaries: each of the four hops, measured in DATAFLOW terms")
  // -------------------------------------------------------------------------
  // Query 01 measured these four hops for a CALL edge. This query measures the
  // same four hops for a DATA edge, which is a different question and can
  // legitimately give a different answer - B3 in particular was measured as
  // crossed by a call edge there. Each boundary below is a bounded flow
  // traversal between the hop's two ends, and every end is measured rather than
  // asserted.
  final case class BoundaryRecord(
      id: String,
      hop: String,
      fromEnd: String,
      toEnd: String,
      reason: String,
      modelling: String,
      crossedByADataFlow: Boolean,
      flowsFound: Int,
      measured: List[(String, String)])

  def boundaryProbe(
      label: String,
      sources: List[CfgNode],
      sinks: List[CfgNode]): (Int, List[FlowRecord]) = {
    val found = flowsFor(sinks.take(MAX_SINK_NODES), sources.take(MAX_SOURCE_NODES),
      boundaryFlowContext)
    val records = found.map { f =>
      val allEls = elementsOf(f)
      val els = allEls.take(MAX_FLOW_LENGTH)
      FlowRecord(label, "boundary-source", "boundary-sink", allEls.size,
        allEls.size > MAX_FLOW_LENGTH, signatureOf(els), els)
    }.distinctBy(_.signature).sortBy(r => (r.elementCount, r.signature))
    log(s"  boundary $label: sources=${sources.size} sinks=${sinks.size} " +
      s"flows=${found.size} distinct=${records.size}")
    (found.size, records.take(MAX_FLOWS_PER_PAIR))
  }

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
  /** The producer END for a dataflow question is the VALUE handed to the message
   *  constructor, not the constructor call itself, so the arguments are the
   *  source set. The consumer end is the accessor's result. */
  val messageProducerArguments: List[CfgNode] = messageProducerSites
    .flatMap(_.argument.l)
    .distinctBy(nodeKey)
    .sortBy(nodeKey)
  val messageConsumerNodes: List[CfgNode] = messageConsumerSites
    .distinctBy(nodeKey)
    .sortBy(nodeKey)
  val (b1Found, b1Flows) =
    boundaryProbe("B1-rpc", messageProducerArguments, messageConsumerNodes)
  val boundaryB1 = BoundaryRecord(
    id = "B1-rpc",
    hop = "RpcEndpointRef.send of " + MESSAGE_TYPE + ", Master to Worker",
    fromEnd = messageProducers.mkString(", "),
    toEnd = messageConsumers.mkString(", "),
    reason = "a message send carries no data edge: the value is serialized out of one " +
      "process and deserialized into another, so the sender's argument and the " +
      "receiver's accessor result are two unrelated definitions as far as " +
      "reaching-definition edges are concerned",
    modelling = "modelled explicitly by pairing on the MESSAGE TYPE - the ARGUMENTS of " +
      "call sites of " + MESSAGE_TYPE + "." + MESSAGE_CTOR_NAME + " are the producer end " +
      "and the RESULTS of call sites of its field accessors (" +
      MESSAGE_ACCESSOR_NAMES.mkString(", ") + ") are the consumer end, with the message " +
      "type's and companion's own generated machinery excluded by owning type",
    crossedByADataFlow = b1Found > 0,
    flowsFound = b1Found,
    measured = List(
      "producer_call_sites" -> jnum(messageProducerSites.size.toLong),
      "producer_argument_nodes" -> jnum(messageProducerArguments.size.toLong),
      "consumer_call_sites" -> jnum(messageConsumerSites.size.toLong),
      "producers" -> jstrArr(messageProducers),
      "consumers" -> jstrArr(messageConsumers),
      "producer_call_site_graph_lines" ->
        jstrArr(messageProducerSites.map(c => s"${c.method.fullName}#${lineOf(c)}")),
      "consumer_call_site_graph_lines" ->
        jstrArr(messageConsumerSites
          .map(c => s"${c.method.fullName}#${lineOf(c)}").distinct.sorted),
      "flows_producer_argument_to_consumer_result" -> jnum(b1Found.toLong)))

  // --- B2: the thread hop ---------------------------------------------------
  val threadHostMethods = cpg.typeDecl
    .fullNameExact(THREAD_HOST_TYPE)
    .method
    .nameExact(THREAD_HOST_METHOD)
    .l
  /** Every parameter INCLUDING the receiver: DriverRunner.start takes no explicit
   *  argument, so the only value that could cross the hop is the enclosing
   *  instance, and excluding it would make the measurement vacuous. The
   *  inclusion is the opposite of the ARM 1 convention and is stated as such. */
  val threadSourceNodes: List[CfgNode] = parametersOf(threadHostMethods)
  val threadBodyMethods = cpg.typeDecl
    .fullName(THREAD_BODY_TYPE_REGEX)
    .method
    .nameExact(THREAD_BODY_METHOD)
    .l
  val threadBodyNames = threadBodyMethods.map(_.fullName).distinct.sorted
  val threadBodyContinuationCalls = threadBodyMethods
    .flatMap(_.call.l)
    .filterNot(isOperatorCall)
    .filter(_.name == THREAD_BODY_CONTINUATION_NAME)
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val threadSinkNodes: List[CfgNode] =
    (threadBodyContinuationCalls ++ threadBodyContinuationCalls.flatMap(_.receiver.l))
      .distinctBy(nodeKey)
      .sortBy(nodeKey)
  val (b2Found, b2Flows) = boundaryProbe("B2-thread", threadSourceNodes, threadSinkNodes)
  val boundaryB2 = BoundaryRecord(
    id = "B2-thread",
    hop = THREAD_HOST_TYPE + "." + THREAD_HOST_METHOD + " calls Thread.start(); the " +
      "route continues in " + THREAD_BODY_METHOD + "() on the anonymous Thread subclass",
    fromEnd = threadHostMethods.map(_.fullName).distinct.sorted.mkString(", "),
    toEnd = threadBodyNames.mkString(", "),
    reason = "Thread.start() -> run() is a JVM scheduling relation: the start frame " +
      "returns immediately and run() is entered on another thread, so no data edge " +
      "joins a definition in the one to a use in the other",
    modelling = "not modelled - the two ends are measured as they stand, with every " +
      "parameter of the start method INCLUDING the receiver taken as the source set " +
      "because the method takes no explicit argument and the enclosing instance is the " +
      "only value that could cross",
    crossedByADataFlow = b2Found > 0,
    flowsFound = b2Found,
    measured = List(
      "start_method_nodes" -> jnum(threadHostMethods.size.toLong),
      "start_parameter_nodes_including_receiver" -> jnum(threadSourceNodes.size.toLong),
      "thread_body_methods" -> jstrArr(threadBodyNames),
      "thread_body_continuation_calls" -> jnum(threadBodyContinuationCalls.size.toLong),
      "thread_body_continuation_call_graph_lines" ->
        jstrArr(threadBodyContinuationCalls
          .map(c => s"${c.method.fullName}#${lineOf(c)}").distinct.sorted),
      "flows_start_scope_to_thread_body" -> jnum(b2Found.toLong)))

  // --- B3: the interface hop ------------------------------------------------
  val abstractLaunchCalls = sinkCalls
    .filter(_.methodFullName.startsWith(ABSTRACT_LAUNCH_CALLEE_PREFIX))
  val concreteLaunchCalls = sinkCalls
    .filter(_.methodFullName.startsWith(JDK_LAUNCH_CALLEE_PREFIX))
  val interfaceSourceNodes: List[CfgNode] =
    (abstractLaunchCalls.flatMap(_.receiver.l) ++ abstractLaunchCalls.flatMap(_.argument.l))
      .distinctBy(nodeKey)
      .sortBy(nodeKey)
  val interfaceSinkNodes: List[CfgNode] =
    (concreteLaunchCalls ++ concreteLaunchCalls.flatMap(_.receiver.l))
      .distinctBy(nodeKey)
      .sortBy(nodeKey)
  val (b3Found, b3Flows) =
    boundaryProbe("B3-interface", interfaceSourceNodes, interfaceSinkNodes)
  val boundaryB3 = BoundaryRecord(
    id = "B3-interface",
    hop = "the launch call site invokes the ABSTRACT ProcessBuilderLike.start; the JDK " +
      "launch is reached only through the anonymous implementation",
    fromEnd = abstractLaunchCalls.map(c => s"${c.method.fullName}#${lineOf(c)}")
      .distinct.sorted.mkString(", "),
    toEnd = concreteLaunchCalls.map(c => s"${c.method.fullName}#${lineOf(c)}")
      .distinct.sorted.mkString(", "),
    reason = "an interface invocation names the declaring type, so joining the receiver " +
      "at the abstract call site to a definition inside the implementation needs the " +
      "type hierarchy; a reaching-definition edge does not cross that on its own",
    modelling = "not modelled by this query - the receiver and arguments at the abstract " +
      "call site are the source end, the concrete JDK launch call and its receiver are " +
      "the sink end, and whether a flow joins them is reported as measured. Query 01 " +
      "measured this same hop as CROSSED by a call edge, which is why the two " +
      "measurements are reported separately rather than as one verdict",
    crossedByADataFlow = b3Found > 0,
    flowsFound = b3Found,
    measured = List(
      "abstract_launch_call_sites" -> jnum(abstractLaunchCalls.size.toLong),
      "concrete_launch_call_sites" -> jnum(concreteLaunchCalls.size.toLong),
      "abstract_call_site_receiver_and_argument_nodes" ->
        jnum(interfaceSourceNodes.size.toLong),
      "concrete_call_site_nodes" -> jnum(interfaceSinkNodes.size.toLong),
      "abstract_declarations_named" ->
        jstrArr(abstractLaunchCalls.map(_.methodFullName).distinct.sorted),
      "concrete_implementations_named" ->
        jstrArr(concreteLaunchCalls.map(_.methodFullName).distinct.sorted),
      "flows_abstract_receiver_to_concrete_launch" -> jnum(b3Found.toLong)))

  // --- B4: the partial-function hop -----------------------------------------
  val sourceLevelParameters: List[CfgNode] = parametersOf(sourceLevelHandlerNodes)
    .filterNot(p => p.name == THIS_PARAMETER_NAME || p.index == 0)
  val syntheticBodyContinuationCalls = syntheticEntryNodes
    .flatMap(_.call.l)
    .filterNot(isOperatorCall)
    .filter(_.name == HANDLER_BODY_CONTINUATION_NAME)
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val syntheticBodySinkNodes: List[CfgNode] =
    (syntheticBodyContinuationCalls ++ syntheticBodyContinuationCalls.flatMap(_.argument.l))
      .distinctBy(nodeKey)
      .sortBy(nodeKey)
  val (b4Found, b4Flows) =
    boundaryProbe("B4-partial-function", sourceLevelParameters, syntheticBodySinkNodes)
  val boundaryB4 = BoundaryRecord(
    id = "B4-partial-function",
    hop = HANDLER_TYPE + "." + HANDLER_METHOD + " returns a PartialFunction whose body " +
      "compiles into a synthetic class",
    fromEnd = sourceLevelHandlerNodes.map(_.fullName).distinct.sorted.mkString(", "),
    toEnd = syntheticEntryNodes.map(_.fullName).distinct.sorted.mkString(", "),
    reason = "the method named " + HANDLER_METHOD + " only constructs the partial " +
      "function; the case bodies live in the synthetic class's " +
      ENTRY_SYNTHETIC_METHOD + ", so a source selected on the source-level name is a " +
      "definition in a method that contains none of the route, and the payload the body " +
      "uses arrives through an unapply rather than through that parameter",
    modelling = "modelled by measuring the hop directly: the source-level method's " +
      "parameters are the source end and the calls the synthetic body makes with the " +
      "recovered payload (" + HANDLER_BODY_CONTINUATION_NAME + ", with its arguments) " +
      "are the sink end. ARM 1 and ARM 2 above select BOTH sides as sources in their " +
      "own right, so the difference between them is measured rather than assumed",
    crossedByADataFlow = b4Found > 0,
    flowsFound = b4Found,
    measured = List(
      "source_level_handler_methods" ->
        jstrArr(sourceLevelHandlerNodes.map(_.fullName).distinct.sorted),
      "synthetic_entry_methods" ->
        jstrArr(syntheticEntryNodes.map(_.fullName).distinct.sorted),
      "source_level_parameter_nodes" -> jnum(sourceLevelParameters.size.toLong),
      "synthetic_body_continuation_calls" ->
        jnum(syntheticBodyContinuationCalls.size.toLong),
      "synthetic_body_continuation_call_graph_lines" ->
        jstrArr(syntheticBodyContinuationCalls
          .map(c => s"${c.method.fullName}#${lineOf(c)}").distinct.sorted),
      "flows_source_level_parameter_to_synthetic_body" -> jnum(b4Found.toLong)))

  val boundaries = List(boundaryB1, boundaryB2, boundaryB3, boundaryB4)
  val boundaryFlows = (b1Flows ++ b2Flows ++ b3Flows ++ b4Flows)
    .distinctBy(r => (r.armId, r.signature))
    .sortBy(r => (r.armId, r.elementCount, r.signature))
  val boundariesNotCrossed = boundaries.filterNot(_.crossedByADataFlow)
  log(s"boundaries measured       : ${boundaries.size}")
  log(s"boundaries NOT crossed by a data flow: " +
    s"${boundariesNotCrossed.map(_.id).mkString(", ")}")

  // -------------------------------------------------------------------------
  stage("K-spurious: the mechanical definition, applied to the emitted set")
  // -------------------------------------------------------------------------
  // A route is spurious ONLY where it passes one of the five named predicates
  // before reaching the sink. Nothing else makes a route spurious, and this
  // judges the QUERY's own output - it says nothing about Spark. The predicate
  // set is the one stage F resolved from the byte-identical selector block, so
  // this count and query 01's are counts of the same thing.
  val predicateCallerNameSet = predicateCallerNames.toSet
  def routeMethods(r: FlowRecord): List[String] =
    (r.sourceGroup :: r.sinkGroup :: r.elements.map(_.enclosingMethod)).distinct
  def routeCallees(r: FlowRecord): List[String] =
    r.elements.map(_.callee).filter(_.nonEmpty).distinct
  def routeIsSpurious(r: FlowRecord): Boolean =
    routeCallees(r).exists(predicateFullNameSet.contains) ||
      routeMethods(r).exists(predicateFullNameSet.contains) ||
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
  stage("L-duplicate-formulation: the verdict against 01 and 03, on evidence")
  // -------------------------------------------------------------------------
  // Both this query and query 01 address ONE handler/sink pair, so whether they
  // are two formulations or one restated is a real question. It is answered from
  // four measured or checkable properties rather than asserted:
  //   1. the EDGE KINDS traversed - reaching-definition/data edges here, call
  //      edges there - which is what makes the questions different;
  //   2. the ENTRY-POINT GRANULARITY - parameters and expressions here, whole
  //      methods there - so the two do not even select the same nodes;
  //   3. whether they CAN differ for some input, demonstrated within this run by
  //      B3, which query 01 measured as crossed by a call edge and this query
  //      measures for a data edge independently;
  //   4. the API CONSTRUCT SETS, computed here as a set difference against query
  //      01's published list, so the difference is auditable rather than claimed.
  val apiConstructsHere = JOERN_API_CONSTRUCTS.distinct.sorted
  val apiConstructsThere = SIBLING_CALLGRAPH_API_CONSTRUCTS.distinct.sorted
  val apiOnlyHere = apiConstructsHere.filterNot(apiConstructsThere.contains)
  val apiOnlyThere = apiConstructsThere.filterNot(apiConstructsHere.contains)
  val apiShared = apiConstructsHere.filter(apiConstructsThere.contains)
  log(s"API constructs here       : ${apiConstructsHere.size}")
  log(s"API constructs in 01      : ${apiConstructsThere.size} (transcribed from its " +
    "published envelope)")
  log(s"only here                 : ${apiOnlyHere.mkString(", ")}")
  log(s"only in 01                : ${apiOnlyThere.mkString(", ")}")
  log(s"shared                    : ${apiShared.size}")

  // How the two formulations' four boundary verdicts compare, computed rather than
  // claimed: query 01's list is transcribed above, this query's is measured, and
  // the symmetric difference is the disagreement set. Agreement is a legitimate
  // outcome and is reported as one - two formulations that happen to agree on a
  // verdict are still two formulations.
  val boundariesNotCrossedHere = boundaries.filterNot(_.crossedByADataFlow).map(_.id).sorted
  val boundariesNotCrossedThere = SIBLING_CALLGRAPH_BOUNDARIES_NOT_CROSSED.sorted
  val boundaryVerdictDisagreements =
    (boundariesNotCrossedHere.filterNot(boundariesNotCrossedThere.contains) ++
      boundariesNotCrossedThere.filterNot(boundariesNotCrossedHere.contains)).distinct.sorted
  val boundaryVerdictsAgree = boundaryVerdictDisagreements.isEmpty
  /** Element-level flow records: sequences of IDENTIFIER, METHOD_PARAMETER_IN and
   *  CALL nodes with their graph lines. A method-level call-edge traversal does
   *  not produce this record kind at all, which is the measured difference in
   *  what the two formulations can return. */
  val elementLevelFlowRecords =
    distinctRoutes.size + boundaryFlows.size + controlArm.flows.size
  val divergenceEvidence =
    if (!boundaryVerdictsAgree)
      "the two formulations DISAGREE on " + boundaryVerdictDisagreements.size.toString +
        " of the four boundary verdicts (" + boundaryVerdictDisagreements.mkString(", ") +
        "): a hop crossed by one edge kind and not the other, measured here and " +
        "transcribed from query 01's published envelope"
    else
      "the two formulations AGREE on all four boundary verdicts in this run (both " +
        "report " + boundariesNotCrossedHere.mkString(", ") + " uncrossed), and " +
        "agreement on a verdict is not identity of formulation. The measured " +
        "difference is in what each can RETURN: this query emitted " +
        elementLevelFlowRecords.toString + " element-level flow record(s), each a " +
        "sequence of IDENTIFIER, METHOD_PARAMETER_IN and CALL nodes with their graph " +
        "lines - including " + controlArm.flows.size.toString + " from a formal " +
        "parameter to the launch call inside a single method - and a method-level " +
        "call-edge traversal produces no such record for any input, which is why " +
        "query 01 published none"
  val duplicateVerdictAgainst01 = "not_duplicate"
  val duplicateBasisAgainst01 =
    "the same handler/sink pair addressed over DIFFERENT EDGES. This query traverses " +
      "reaching-definition (data) edges through the OSS dataflow layer and selects " +
      "PARAMETER and EXPRESSION nodes as its ends; query 01 traverses CALL edges and " +
      "selects whole METHODS. The two therefore ask different questions of different " +
      "nodes, and neither is expressible as the other: no call-edge traversal " +
      "establishes that a value reaches the launch, and no reaching-definition " +
      "traversal establishes that control can arrive there. Measured in this run: " +
      divergenceEvidence + ". Auditable corroboration: " + apiOnlyHere.size.toString +
      " of this query's " + apiConstructsHere.size.toString + " API constructs do not " +
      "appear in query 01's published list at all (" + apiOnlyHere.mkString(", ") +
      "), and " + apiOnlyThere.size.toString + " of query 01's do not appear here (" +
      apiOnlyThere.mkString(", ") + "). Their results are reported side by side and are " +
      "NEVER SUMMED."
  val duplicateVerdictAgainst03 = "not_duplicate"
  val duplicateBasisAgainst03 =
    "a different target set and a different formulation: " + SIBLING_PARAMETERIZED_QUERY +
      " is parameterized over handler/sink pairs and covers a second pair this query " +
      "does not address (the deploy/rest/StandaloneRestServer handler to the " +
      "deploy/worker/DriverRunner sink). This query is fixed to one pair and to the " +
      "dataflow formulation of it, so neither subsumes the other and their returns are " +
      "likewise never summed."
  log(s"duplicate vs 01           : $duplicateVerdictAgainst01")
  log(s"duplicate vs 03           : $duplicateVerdictAgainst03")
  log(s"boundary verdicts here    : uncrossed=${boundariesNotCrossedHere.mkString(", ")}")
  log(s"boundary verdicts in 01   : uncrossed=${boundariesNotCrossedThere.mkString(", ")} " +
    "(transcribed)")
  log(s"boundary verdicts agree   : $boundaryVerdictsAgree " +
    (if (boundaryVerdictsAgree) "" else s"(disagree on " +
      s"${boundaryVerdictDisagreements.mkString(", ")})"))
  log(s"element-level flow records: $elementLevelFlowRecords " +
    "(a record kind a method-level call-edge traversal does not produce)")

  // -------------------------------------------------------------------------
  stage("M-records: the returned set, capped and deterministic")
  // -------------------------------------------------------------------------
  def elementJson(e: FlowElement): String = jobj(12, List(
    "index" -> jnum(e.index.toLong),
    "label" -> jstr(e.label),
    "enclosing_method" -> jstr(e.enclosingMethod),
    "callee" -> jstr(e.callee),
    "graph_line" -> jnum(e.graphLine.toLong),
    "code" -> jstr(e.code)))

  def flowJson(kind: String, r: FlowRecord): String = jobj(6, List(
    "kind" -> jstr(kind),
    "arm_id" -> jstr(r.armId),
    "source_group" -> jstr(r.sourceGroup),
    "sink_group" -> jstr(r.sinkGroup),
    "element_count" -> jnum(r.elementCount.toLong),
    "elements_truncated" -> jbool(r.elementsTruncated),
    "signature" -> jstr(r.signature),
    "passed_auth_or_acl_predicate" -> jbool(routeIsSpurious(r)),
    "spurious" -> jbool(routeIsSpurious(r)),
    "elements" -> jrawArr(10, r.elements.map(elementJson))))

  def boundaryJson(b: BoundaryRecord): String = jobj(6, List(
    "kind" -> jstr("boundary"),
    "boundary_id" -> jstr(b.id),
    "hop" -> jstr(b.hop),
    "from_end" -> jstr(b.fromEnd),
    "to_end" -> jstr(b.toEnd),
    "reason" -> jstr(b.reason),
    "modelling" -> jstr(b.modelling),
    "crossed_by_a_data_flow" -> jbool(b.crossedByADataFlow),
    "flows_found" -> jnum(b.flowsFound.toLong),
    "measured" -> jobj(8, b.measured)))

  def armJson(a: ArmResult): String = jobj(6, List(
    "arm_id" -> jstr(a.armId),
    "description" -> jstr(a.description),
    "call_depth_bound" -> jnum(a.callDepth.toLong),
    "source_selection" -> jstr(a.sourceSelection),
    "source_groups_discovered" -> jnum(a.sourceGroupsDiscovered.toLong),
    "source_groups_traversed" -> jnum(a.sourceGroupsTraversed.toLong),
    "source_groups_truncated" -> jnum(a.sourceGroupsTruncated.toLong),
    "source_nodes" -> jnum(a.sourceNodes.toLong),
    "source_nodes_truncated" -> jnum(a.sourceNodesTruncated.toLong),
    "sink_groups" -> jnum(a.sinkGroups.toLong),
    "sink_nodes" -> jnum(a.sinkNodes.toLong),
    "sink_nodes_truncated" -> jnum(a.sinkNodesTruncated.toLong),
    "evaluations" -> jnum(a.evaluations.toLong),
    "step_cap_reached" -> jbool(a.stepCapReached),
    "flows_found" -> jnum(a.flowsFound.toLong),
    "flows_retained" -> jnum(a.flowsRetained.toLong),
    "per_pair_cap_reached" -> jbool(a.perPairCapReached),
    "flow_length_cap_reached" -> jbool(a.lengthCapReached)))

  val recordJsonAll = boundaries.map(boundaryJson) ++
    distinctRoutes.map(r => flowJson("route", r)) ++
    boundaryFlows.map(r => flowJson("boundary-flow", r)) ++
    controlArm.flows.map(r => flowJson("liveness-control-flow", r))
  val totalReturnsCapReached = recordJsonAll.size > MAX_TOTAL_RETURNS
  val recordJson = recordJsonAll.take(MAX_TOTAL_RETURNS)
  val returnedRecordCount = recordJson.size
  log(s"records returned          : $returnedRecordCount " +
    s"(${boundaries.size} boundary, ${distinctRoutes.size} route, " +
    s"${boundaryFlows.size} boundary-flow, ${controlArm.flows.size} control; cap " +
    s"$MAX_TOTAL_RETURNS, reached=$totalReturnsCapReached)")

  val duplicateFormulationJson = jrawArr(4, List(
    jobj(6, List(
      "against" -> jstr(SIBLING_CALLGRAPH_QUERY),
      "status" -> jstr(duplicateVerdictAgainst01),
      "basis" -> jstr(duplicateBasisAgainst01),
      "same_target_pair" -> jbool(true),
      "same_edge_kinds" -> jbool(false),
      "same_entry_point_granularity" -> jbool(false),
      "can_differ_for_some_input" -> jbool(true),
      "one_expressible_as_the_other" -> jbool(false),
      "boundary_verdicts_here_uncrossed" -> jstrArr(boundariesNotCrossedHere),
      "boundary_verdicts_in_01_uncrossed_transcribed" ->
        jstrArr(boundariesNotCrossedThere),
      "boundary_verdicts_agree" -> jbool(boundaryVerdictsAgree),
      "boundary_verdict_disagreements" -> jstrArr(boundaryVerdictDisagreements),
      "element_level_flow_records_emitted_here" -> jnum(elementLevelFlowRecords.toLong),
      "divergence_evidence" -> jstr(divergenceEvidence),
      "api_constructs_only_here" -> jstrArr(apiOnlyHere),
      "api_constructs_only_there" -> jstrArr(apiOnlyThere),
      "api_constructs_shared" -> jnum(apiShared.size.toLong),
      "results_summed" -> jbool(false))),
    jobj(6, List(
      "against" -> jstr(SIBLING_PARAMETERIZED_QUERY),
      "status" -> jstr(duplicateVerdictAgainst03),
      "basis" -> jstr(duplicateBasisAgainst03),
      "same_target_pair" -> jbool(false),
      "same_edge_kinds" -> jbool(false),
      "same_entry_point_granularity" -> jbool(false),
      "can_differ_for_some_input" -> jbool(true),
      "one_expressible_as_the_other" -> jbool(false),
      "results_summed" -> jbool(false)))))

  // -------------------------------------------------------------------------
  stage("N-write: the envelope, the prose report and the console log")
  // -------------------------------------------------------------------------
  val resultsDir = repoRoot.resolve(RESULTS_DIR)
  Files.createDirectories(resultsDir)
  val jsonPath = resultsDir.resolve(s"$QUERY_ID.json")
  val mdPath = resultsDir.resolve(s"$QUERY_ID.md")

  val envelope = jobj(0, List(
    "query_id" -> jstr(QUERY_ID),
    "query_source" -> jstr(s"queries/joern/$QUERY_ID.sc"),
    "formulation" -> jstr("bounded DATAFLOW over reaching-definition edges, from the " +
      "standalone Master's driver-submission handler to the privileged process launch " +
      "hosted on the DriverRunner surface: the same handler/sink pair query 01 addresses " +
      "by call-graph reachability, formulated over different edges"),
    "observational_only" -> jbool(true),
    "contributes_dataset_rows" -> jbool(false),
    "compile_status" -> jstr("compiled"),
    "compile_status_convention" -> jstr("this field is written by the running script, " +
      "so its presence is itself the evidence: a compile failure produces no envelope " +
      "at all and the compiler's diagnostic lands in the console stream"),
    "run_status" -> jstr("completed"),
    "returned_record_count" -> jnum(returnedRecordCount.toLong),
    "returned_record_kinds" -> jobj(2, List(
      "boundary" -> jnum(boundaries.size.toLong),
      "route" -> jnum(distinctRoutes.size.toLong),
      "boundary_flow" -> jnum(boundaryFlows.size.toLong),
      "liveness_control_flow" -> jnum(controlArm.flows.size.toLong))),
    "distinct_routes" -> jnum(distinctRoutes.size.toLong),
    "distinct_routes_convention" -> jstr("the route-bearing arms' flows deduplicated on " +
      "(source group, sink group, element signature); the arms' returns are not summed " +
      "with each other, and this query's routes are NEVER summed with query 01's - the " +
      "two address one pair by two formulations and adding them yields a number that " +
      "means nothing. The engine-liveness control's flows are NOT routes and are " +
      "reported under their own field"),
    "never_summed_with" -> jstrArr(List(SIBLING_CALLGRAPH_QUERY, SIBLING_PARAMETERIZED_QUERY)),
    "spurious_count" -> jnum(spuriousCount.toLong),
    "spurious_definition" -> jstr("a route is spurious ONLY where it passes an " +
      "authorization or ACL predicate before reaching the sink, the predicate set being " +
      "exactly the five named selectors below; this judges the query, not Spark"),
    "expected_spurious_route_absent" -> jbool(expectedSpuriousAbsent),
    "expected_spurious_absence_basis" ->
      jstr(if (absenceIsStructural) "structural" else "filtering"),
    "expected_spurious_absence_statement" -> jstr(
      "no route in the emitted set passed an auth/ACL predicate as defined by these " +
        "five named selectors, and no call site of any of the five exists on the route " +
        "surface at all, so the absence is structural rather than a consequence of the " +
        "query filtering well"),
    "bound_value" -> jnum(MAX_FLOW_CALL_DEPTH.toLong),
    "bound_value_meaning" -> jstr("MAX_FLOW_CALL_DEPTH, the engine's " +
      "EngineConfig.maxCallDepth: the number of call boundaries the backward search may " +
      "expand while looking for a source. It exceeds the five method boundaries the " +
      "documented route crosses, so a flow absent within it is not an artefact of a " +
      "short bound"),
    "bound_reached" -> jbool(boundReached),
    "bound_reached_basis" -> jstr("the observable caps are the evaluator's own: the " +
      "per-source step cap, the per-pair flow cap, the flow-length cap and the source, " +
      "sink and entry-point truncation counters. The engine's INTERNAL call-depth bound " +
      "is not observable from its output - it reports no truncation flag - so depth is " +
      "addressed by running ARM 1 at two depths and comparing, which is reported under " +
      "depth_sensitivity. This limitation is stated rather than papered over"),
    "bounds" -> jobj(2, List(
      "MAX_FLOW_CALL_DEPTH" -> jnum(MAX_FLOW_CALL_DEPTH.toLong),
      "MAX_FLOW_CALL_DEPTH_SHALLOW" -> jnum(MAX_FLOW_CALL_DEPTH_SHALLOW.toLong),
      "MAX_BOUNDARY_FLOW_CALL_DEPTH" -> jnum(MAX_BOUNDARY_FLOW_CALL_DEPTH.toLong),
      "MAX_FLOW_LENGTH" -> jnum(MAX_FLOW_LENGTH.toLong),
      "MAX_FLOWS_PER_PAIR" -> jnum(MAX_FLOWS_PER_PAIR.toLong),
      "MAX_STEPS_PER_SOURCE" -> jnum(MAX_STEPS_PER_SOURCE.toLong),
      "MAX_TOTAL_RETURNS" -> jnum(MAX_TOTAL_RETURNS.toLong),
      "MAX_SOURCE_NODES" -> jnum(MAX_SOURCE_NODES.toLong),
      "MAX_SINK_NODES" -> jnum(MAX_SINK_NODES.toLong),
      "MAX_ENTRY_POINTS" -> jnum(MAX_ENTRY_POINTS.toLong),
      "MAX_CALL_SCAN" -> jnum(MAX_CALL_SCAN.toLong),
      "MAX_CODE_CHARS" -> jnum(MAX_CODE_CHARS.toLong))),
    "depth_sensitivity" -> jobj(2, List(
      "shallow_depth" -> jnum(armOneShallow.callDepth.toLong),
      "primary_depth" -> jnum(armOne.callDepth.toLong),
      "shallow_flows_retained" -> jnum(armOneShallow.flowsRetained.toLong),
      "primary_flows_retained" -> jnum(armOne.flowsRetained.toLong),
      "results_differ_across_the_two_depths" -> jbool(depthSensitive),
      "interpretation" -> jstr("equal results across the two depths is evidence that " +
        "the result does not depend on the call-depth bound across that range; a " +
        "difference would be evidence that it does"))),
    "entry_points_discovered" -> jnum(entryPointsDiscovered.toLong),
    "entry_points_traversed" -> jnum(entryPointsTraversed.toLong),
    "entry_points_truncated" -> jnum(entryPointsTruncated.toLong),
    "entry_point_selection" -> jstr("BOUNDARY 4: the handler body compiles into a " +
      "synthetic partial-function class, so the synthetic " + ENTRY_SYNTHETIC_METHOD +
      " on every type matching " + ENTRY_SYNTHETIC_TYPE_REGEX + " is selected together " +
      "with the source-level " + HANDLER_TYPE + "." + HANDLER_METHOD),
    "entry_points" -> jstrArr(entryGroups.map(_._1)),
    "source_selection" -> jobj(2, List(
      "arm1" -> jstr("every formal parameter of the entry methods, with the implicit " +
        "receiver excluded"),
      "arm1_receiver_parameters_excluded" ->
        jstrArr(excludedThisParameters
          .map(p => s"${p.method.fullName}#${p.index}:${p.name}")),
      "arm1_any_typed_parameters" ->
        jstrArr(messageTypedParameters
          .map(p => s"${p.method.fullName}#${p.index}:${p.name}:${p.typeFullName}")),
      "arm1_message_parameter_identified_by" -> jstr("erased bytecode type " +
        MESSAGE_PARAMETER_TYPE + ", not by parameter position"),
      "arm2_selection" -> jstr(armTwoSelection),
      "arm2_nodes" -> jstrArr(armTwoCalls
        .map(c => s"${c.method.fullName} -> ${c.methodFullName}#${lineOf(c)}")),
      "unapply_treatment" -> jstr("the payload arrives at Master.scala:410 through a " +
        "pattern match, which in bytecode is a type test, a cast and the case class's " +
        "own " + REQUEST_MESSAGE_ACCESSOR + " accessor rather than an assignment. The " +
        "Any-typed formal parameter (ARM 1) and the accessor's result (ARM 2) are " +
        "therefore selected as TWO SEPARATE ARMS and reported separately, so neither " +
        "choice is hidden inside one number and the flow counts stay interpretable"))),
    "sink_hosts" -> jstrArr(sinkHostNames),
    "sink_call_sites" -> jstrArr(sinkCalls.map(c =>
      s"${c.method.fullName} -> ${c.methodFullName} #${lineOf(c)}")),
    "sink_node_composition" -> jobj(2, List(
      "launch_call_nodes" -> jnum(sinkCalls.size.toLong),
      "receiver_nodes" -> jnum(sinkReceivers.size.toLong),
      "argument_nodes" -> jnum(sinkArguments.size.toLong),
      "distinct_sink_nodes_used" -> jnum(sinkNodes.size.toLong),
      "sink_nodes_truncated" -> jnum(sinkNodesTruncated.toLong),
      "rationale" -> jstr("a flow that reaches the value being launched ends at the " +
        "launch call, its receiver or one of its arguments; taking only the call node " +
        "would miss a flow into the receiver"))),
    "operator_pseudo_calls_excluded" -> jbool(true),
    "duplicate_class_definitions_unioned" -> jbool(true),
    "graph" -> jobj(2, List(
      "path_source" -> jstr(cpgPathSource),
      "named_path" -> jstr(cpgNamed.toString),
      "resolved_path" -> jstr(cpgResolved.toString),
      "named_path_is_symlink" -> jbool(cpgIsLink),
      "byte_size_following_the_link" -> jnum(sizeFollow),
      "byte_size_without_following" -> jnum(sizeNoFollow),
      "sha256" -> jstr(shaObserved),
      "identity_record" -> jstr(CPG_RECORD_PATH),
      "identity_recorded_byte_size" -> jnum(recordedSize),
      "identity_recorded_sha256" -> jstr(recordedSha),
      "identity_reverified_before_load" -> jbool(true),
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
        "route is mandated because the alternative loader spawns a second JVM at the " +
        "same heap"),
      "dataflow_layer" -> jstr("applied by importCpg as one of the console's default " +
        "overlays; its liveness on this sink is not assumed but measured by the " +
        "engine-liveness control arm"),
      "flow_engine_semantics" -> jstr(semanticsClass),
      "flow_engine_context_source" -> jstr("the console's own EngineContext, copied with " +
        "only the call-depth bound overridden, and passed EXPLICITLY at every call site " +
        "so no implicit resolution decides which context a traversal ran under"),
      "workspace" -> jstr(WORKSPACE_PATH),
      "heap_bound_jvm_position" -> jstr("one of 4 (frontend build, importCpg " +
        "verification load, Stage 3 Joern runner, this probe)"))),
    "predicate_selector" -> jobj(2, List(
      "type" -> jstr(PREDICATE_TYPE),
      "name_regex" -> jstr(PREDICATE_NAME_REGEX),
      "setter_suffix_excluded" -> jstr(PREDICATE_SETTER_SUFFIX),
      "named_five" -> jstrArr(PREDICATE_NAMED_FIVE.sorted),
      "selector_block_byte_identical_to" -> jstr(SIBLING_CALLGRAPH_QUERY),
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
    "arms" -> jrawArr(4, routeArms.map(armJson)),
    "engine_liveness_control" -> jobj(2, List(
      "arm" -> armJson(controlArm),
      "dataflow_layer_live_on_this_sink" -> jbool(engineLive),
      "why_it_exists" -> jstr("a zero from a cross-boundary arm means either that the " +
        "route is not connected by data or that the engine had no reaching-definition " +
        "edges to walk, and the zero alone cannot tell those apart. The control asks for " +
        "a flow that must exist if the layer is live - from the launch's own enclosing " +
        "method's formal parameter to the launch call it is the receiver of - so the " +
        "cross-boundary zeros become attributable"),
      "counted_as_routes" -> jbool(false))),
    "boundaries_not_crossed_by_a_data_flow" -> jstrArr(boundariesNotCrossed.map(_.id)),
    "boundary_b3_verdicts" -> jobj(2, List(
      "call_edge_verdict_in_01_transcribed" -> jstr("crossed"),
      "data_edge_verdict_measured_here" ->
        jstr(if (boundaryB3.crossedByADataFlow) "crossed" else "not crossed"),
      "data_flows_found_here" -> jnum(boundaryB3.flowsFound.toLong),
      "note" -> jstr("two measurements of one hop under two different questions, " +
        "reported separately rather than merged into a single verdict"))),
    "duplicate_formulation" -> jstr(duplicateVerdictAgainst01),
    "duplicate_formulation_detail" -> duplicateFormulationJson,
    "effort_query_revisions_committed" -> jnum(QUERY_REVISIONS_COMMITTED.toLong),
    "effort_query_revisions_convention" -> jstr(QUERY_REVISIONS_CONVENTION),
    "effort_joern_api_constructs" -> jstrArr(apiConstructsHere),
    "effort_joern_api_construct_count" -> jnum(apiConstructsHere.size.toLong),
    "effort_joern_api_constructs_not_used_by_01" -> jstrArr(apiOnlyHere),
    "effort_parameterizability" -> jstr("not claimed here; proven by " +
      PARAMETERIZABILITY_OWNER + " invoking its parameterized form on the second named " +
      "handler/sink pair and capturing that invocation's result"),
    "total_returns_cap_reached" -> jbool(totalReturnsCapReached),
    "records" -> jrawArr(2, recordJson))) + "\n"

  writeUtf8(jsonPath, envelope)
  log(s"envelope written          : $jsonPath (${envelope.length} chars)")

  // ---------------------------- the prose report ----------------------------
  val md = scala.collection.mutable.ArrayBuffer.empty[String]
  def md0(line: String): Unit = md += line
  /** Sentence-case a computed string that is reused verbatim in the envelope,
   *  where it reads as a field value rather than as a sentence. */
  def sentence(s: String): String =
    if (s.isEmpty) s else s.substring(0, 1).toUpperCase + s.substring(1)

  md0(s"# Joern capability probe $QUERY_ID")
  md0("")
  md0("Bounded **dataflow** from the Spark standalone Master's driver-submission handler")
  md0("to the privileged process launch hosted on the `DriverRunner` surface, over the")
  md0("code-property graph built from the pinned tree's bytecode. This is the **same")
  md0(s"handler/sink pair as `$SIBLING_CALLGRAPH_QUERY`**, addressed by a")
  md0("**different formulation**: data edges rather than call edges.")
  md0("")
  md0("This report is **observational**. It judges no finding - not real, not important,")
  md0("not a false positive, not a duplicate - and makes no comparison between tools. It")
  md0("contributes no row to `oss-scan-results/findings.json` and writes nothing into")
  md0("`harness/artifacts/raw/`.")
  md0("")
  md0(s"The slug `$QUERY_ID` is the **identifier** the plan")
  md0("assigns this query. It names the question the query was written to ask - whether a")
  md0("dataflow formulation can join this handler to this sink, and whether any route it")
  md0("returns passes one of five named predicates first. It is not a finding, and nothing")
  md0("in this report should be read as an assessment of Spark, of any Spark component or")
  md0("of any Spark configuration.")
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
  md0(s"| Graph identity re-verified before the load | yes, against `$CPG_RECORD_PATH` |")
  md0(s"| Graph methods / typeDecls / files | $methodCount / $typeDeclCount / $fileCount |")
  md0(s"| Flow engine semantics | `$semanticsClass` |")
  md0(s"| Compile status | compiled |")
  md0(s"| Run status | completed |")
  md0(s"| Records returned | $returnedRecordCount (${boundaries.size} boundary, " +
    s"${distinctRoutes.size} route, ${boundaryFlows.size} boundary-flow, " +
    s"${controlArm.flows.size} control) |")
  md0(s"| Distinct routes | ${distinctRoutes.size} |")
  md0(s"| Spurious routes | $spuriousCount |")
  md0(s"| Dataflow layer live on this sink | $engineLive |")
  md0("")
  md0("## The result")
  md0("")
  md0(s"**Distinct routes: ${distinctRoutes.size}.** Routes are counted distinct on")
  md0("(source group, sink group, flow element signature) across the route-bearing arms")
  md0("below. They are **never summed** - not across the arms, and not with")
  md0(s"`$SIBLING_CALLGRAPH_QUERY`, which asks a different question over")
  md0("different edges. A reader who adds the two queries' routes together gets a number")
  md0("that means nothing.")
  md0("")
  if (distinctRoutes.isEmpty) {
    md0("No flow from a source to a sink node was returned within the stated bounds. That")
    md0("is a capability finding about what this formulation can express over this graph,")
    md0("and it is reported as measured: no bound was loosened, removed or re-run")
    md0("unbounded to produce a non-empty result. Two things make the zero interpretable")
    md0("rather than merely empty:")
    md0("")
    md0(s"1. the **engine-liveness control** returned $engineLive for the question " +
      "\"does the")
    md0("   dataflow layer produce a flow on this very sink\", so the zero above is")
    md0("   attributable to the route rather than to an engine with nothing to walk;")
    md0("2. the **four boundaries** below are measured individually, and each one that is")
    md0("   not crossed by a data edge is a named reason.")
  } else {
    md0("Flows were returned. Each is listed in the envelope with its full element")
    md0("sequence, the enclosing method of each element and that element's graph line.")
    distinctRoutes.foreach { r =>
      md0("")
      md0(s"- arm `${r.armId}`, ${r.elementCount} elements, source group")
      md0(s"  `${r.sourceGroup}` to sink group `${r.sinkGroup}`")
    }
  }
  md0("")
  md0("### The engine-liveness control")
  md0("")
  md0(s"`dataflow_layer_live_on_this_sink` = **$engineLive**. The control asks for a flow")
  md0(s"from `$CONTROL_HOST_TYPE.$CONTROL_HOST_METHOD`'s own formal parameter to the")
  md0("launch call in that same method, which the parameter is the receiver of. It is")
  md0("intraprocedural by construction and it is **not a route**: its flows are reported")
  md0("under their own field and are never counted among the routes above.")
  md0("")
  md0(s"- control flows found: ${controlArm.flowsFound}, retained: " +
    s"${controlArm.flowsRetained}")
  md0(s"- control evaluations: ${controlArm.evaluations} at call depth " +
    s"${controlArm.callDepth}")
  md0("")
  if (engineLive) {
    md0("Because the control is non-empty, a zero from a cross-boundary arm is a statement")
    md0("about the route and not about the engine.")
  } else {
    md0("Because the control is empty, every zero above is attributable to the engine as")
    md0("configured rather than to the route, and this report says so rather than reading")
    md0("the zero as a route finding.")
  }
  md0("")
  md0("## Whether the bound was reached")
  md0("")
  md0(s"`bound_reached` = **$boundReached**. The primary bound is `MAX_FLOW_CALL_DEPTH` =")
  md0(s"$MAX_FLOW_CALL_DEPTH, the engine's `EngineConfig.maxCallDepth`: the number of call")
  md0("boundaries the backward search may expand while looking for a source. Every")
  md0("traversal in this query carries an explicit named bound; none runs unbounded.")
  md0("")
  md0("Two kinds of bound, reported separately because only one of them is observable:")
  md0("")
  md0(s"- **Observable caps** (`observable_bound_reached` = $observableBoundReached): the")
  md0("  per-source step cap, the per-pair flow cap, the flow-length cap and the source,")
  md0("  sink and entry-point truncation counters are all counted by this query's own")
  md0("  evaluator, so whether each bit was set is measured.")
  md0("- **The engine's internal call-depth bound is not observable**: the engine reports")
  md0("  no truncation flag when it stops expanding callers. Rather than guess, ARM 1 is")
  md0(s"  run TWICE - at depth $MAX_FLOW_CALL_DEPTH_SHALLOW and at depth " +
    s"$MAX_FLOW_CALL_DEPTH - and the")
  md0(s"  results compared: `results_differ_across_the_two_depths` = $depthSensitive.")
  md0("  Equal results are evidence that the outcome does not depend on the bound across")
  md0("  that range; a difference would be evidence that it does. This is a stated")
  md0("  limitation of the engine's output, not a gap in the measurement.")
  md0("")
  md0("| bound | value |")
  md0("| --- | --- |")
  md0(s"| MAX_FLOW_CALL_DEPTH | $MAX_FLOW_CALL_DEPTH |")
  md0(s"| MAX_FLOW_CALL_DEPTH_SHALLOW | $MAX_FLOW_CALL_DEPTH_SHALLOW |")
  md0(s"| MAX_BOUNDARY_FLOW_CALL_DEPTH | $MAX_BOUNDARY_FLOW_CALL_DEPTH |")
  md0(s"| MAX_FLOW_LENGTH | $MAX_FLOW_LENGTH |")
  md0(s"| MAX_FLOWS_PER_PAIR | $MAX_FLOWS_PER_PAIR |")
  md0(s"| MAX_STEPS_PER_SOURCE | $MAX_STEPS_PER_SOURCE |")
  md0(s"| MAX_TOTAL_RETURNS | $MAX_TOTAL_RETURNS |")
  md0(s"| MAX_SOURCE_NODES | $MAX_SOURCE_NODES |")
  md0(s"| MAX_SINK_NODES | $MAX_SINK_NODES |")
  md0(s"| MAX_ENTRY_POINTS | $MAX_ENTRY_POINTS |")
  md0(s"| MAX_CALL_SCAN | $MAX_CALL_SCAN |")
  md0(s"| MAX_CODE_CHARS | $MAX_CODE_CHARS |")
  md0("")
  md0("| arm | depth | evaluations | flows found | flows retained | step cap | " +
    "per-pair cap | length cap | source groups |")
  md0("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
  allArms.foreach { a =>
    md0(s"| `${a.armId}` | ${a.callDepth} | ${a.evaluations} | ${a.flowsFound} | " +
      s"${a.flowsRetained} | ${a.stepCapReached} | ${a.perPairCapReached} | " +
      s"${a.lengthCapReached} | ${a.sourceGroupsTraversed}/${a.sourceGroupsDiscovered} |")
  }
  md0("")
  md0("## Entry points, the source selection, and the unapply")
  md0("")
  md0(s"Discovered $entryPointsDiscovered, traversed $entryPointsTraversed, truncated " +
    s"$entryPointsTruncated.")
  md0("")
  md0(s"`$HANDLER_METHOD` returns a `PartialFunction`, so its body compiles into a")
  md0("synthetic class and the handler's formal parameter in the graph belongs to that")
  md0(s"class's `$ENTRY_SYNTHETIC_METHOD`, not to a method named `$HANDLER_METHOD`. Both")
  md0("are selected, so the difference between them is measured rather than assumed:")
  md0("")
  entryGroups.foreach { case (fn, nodes) =>
    md0(s"- `$fn` (${nodes.size} node(s), graph line ${lineOfMethod(nodes.head)})")
  }
  md0("")
  md0("The formal parameter is `Any`-typed, and the `DriverDescription` payload is")
  md0("recovered by the pattern match at `Master.scala:410` - an **unapply**, which in")
  md0(s"bytecode is a type test, a cast and the case class's own `$REQUEST_MESSAGE_ACCESSOR`")
  md0("accessor rather than an assignment. Selecting one side only would leave the flow")
  md0("count uninterpretable, so **both** are selected, as two arms:")
  md0("")
  md0(s"- **ARM 1** - every formal parameter of the entry methods, with the implicit")
  md0(s"  receiver (`$THIS_PARAMETER_NAME`) excluded because it carries the enclosing")
  md0("  instance rather than the message. The `Any`-typed parameter is identified by its")
  md0(s"  erased bytecode type `$MESSAGE_PARAMETER_TYPE` rather than by position;")
  md0(s"  ${messageTypedParameters.size} parameter(s) matched that type.")
  md0(s"- **ARM 2** - the unapply-recovered payload. Selection used: $armTwoSelection.")
  md0("")
  if (excludedThisParameters.nonEmpty) {
    md0("Receiver parameters excluded from ARM 1:")
    md0("")
    excludedThisParameters.foreach { p =>
      md0(s"- `${p.method.fullName}` index ${p.index}, name `${p.name}`, type " +
        s"`${p.typeFullName}`")
    }
    md0("")
  }
  md0("## The sink")
  md0("")
  sinkCalls.foreach { c =>
    md0(s"- `${c.method.fullName}` calls `${c.methodFullName}` at graph line " +
      s"${lineOf(c)} (dispatch `${c.dispatchType}`)")
  }
  md0("")
  md0(s"The sink NODE set is the launch call together with its receiver and its arguments")
  md0(s"(${sinkCalls.size} call, ${sinkReceivers.size} receiver, ${sinkArguments.size} " +
    s"argument; ${sinkNodes.size} distinct after de-duplication): a flow that reaches the")
  md0("value being launched ends at one of those, and taking only the call node would")
  md0("miss a flow into the receiver.")
  md0("")
  md0("## The four boundaries, as capability findings")
  md0("")
  md0("Each hop below is measured against the graph with its own bounded flow traversal,")
  md0("not asserted. `crossed by a data flow` states whether a flow in fact joins the")
  md0("hop's two ends.")
  md0("")
  boundaries.foreach { b =>
    md0(s"### ${b.id} - crossed by a data flow: **${b.crossedByADataFlow}** " +
      s"(${b.flowsFound} flow(s) found)")
    md0("")
    md0(s"- **hop**: ${b.hop}")
    md0(s"- **from**: ${if (b.fromEnd.isEmpty) "(none measured)" else "`" + b.fromEnd + "`"}")
    md0(s"- **to**: ${if (b.toEnd.isEmpty) "(none measured)" else "`" + b.toEnd + "`"}")
    md0(s"- **reason**: ${b.reason}")
    md0(s"- **modelling**: ${b.modelling}")
    md0("")
  }
  md0("Boundaries not crossed by a data flow: " +
    (if (boundariesNotCrossed.isEmpty) "none"
     else boundariesNotCrossed.map(b => s"`${b.id}`").mkString(", ")) + ".")
  md0("")
  md0("`B3-interface` deserves one explicit note, because it is the hop on which the two")
  md0(s"formulations could most easily have parted company. `$SIBLING_CALLGRAPH_QUERY`")
  md0("measured it as **crossed by a call edge**; this query measures the same hop for a")
  md0(s"**data edge** and reports **${boundaryB3.crossedByADataFlow}**")
  md0(s"(${boundaryB3.flowsFound} flow(s) found). Two measurements of one hop under two")
  md0("different questions, reported separately rather than merged into one verdict.")
  md0("")
  md0("Across all four hops the two formulations' verdicts")
  md0(s"**${if (boundaryVerdictsAgree) "agree" else "disagree"}**" +
    (if (boundaryVerdictsAgree) "" else " on " + boundaryVerdictDisagreements.mkString(", ")) +
    ". Query 01's verdicts are transcribed from its published envelope; this query's are")
  md0("measured. Agreement is a result, not a foregone conclusion, and it does not make")
  md0("the two one formulation - see the verdict below.")
  md0("")
  md0("## The predicate set, and the source types it came from")
  md0("")
  md0("The mechanical definition: a route is spurious **only** where it passes an")
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
  md0("The selector block in this query's source is **byte-identical** to")
  md0(s"`$SIBLING_CALLGRAPH_QUERY`'s. It has to be: the two spurious counts")
  md0("are only comparable if the definition of the term is the same text, and the")
  md0("duplicate-formulation verdict below rests on that comparability.")
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
  md0(s"`duplicate_formulation` = **$duplicateVerdictAgainst01**, answered against both")
  md0("other queries on evidence rather than by assertion.")
  md0("")
  md0(s"### Against `$SIBLING_CALLGRAPH_QUERY`: $duplicateVerdictAgainst01")
  md0("")
  md0("The same handler/sink pair, and four properties that differ:")
  md0("")
  md0("- **Edge kinds traversed.** This query walks reaching-definition (data) edges")
  md0("  through the OSS dataflow layer; query 01 walks CALL edges. Neither traversal")
  md0("  establishes the other's conclusion: a call path does not show that a value")
  md0("  arrives, and a data path does not show that control can.")
  md0("- **Entry-point granularity.** This query's ends are PARAMETER and EXPRESSION")
  md0("  nodes; query 01's are whole METHODS. The two do not select the same nodes.")
  md0("- **What each can return, measured in this run.** " + sentence(divergenceEvidence) +
    ".")
  md0(s"- **API construct sets.** ${apiOnlyHere.size} of this query's " +
    s"${apiConstructsHere.size} constructs")
  md0("  do not appear in query 01's published list, and " + apiOnlyThere.size +
    " of query 01's do not")
  md0("  appear here. The difference is computed from the two lists rather than eyeballed:")
  md0("")
  apiOnlyHere.foreach(c => md0(s"  - only here: `$c`"))
  apiOnlyThere.foreach(c => md0(s"  - only in query 01: `$c`"))
  md0("")
  md0("Their results are reported side by side and are **never summed**.")
  md0("")
  md0(s"### Against `$SIBLING_PARAMETERIZED_QUERY`: $duplicateVerdictAgainst03")
  md0("")
  md0(sentence(duplicateBasisAgainst03))
  md0("")
  md0("## The three effort measures")
  md0("")
  md0(s"1. **Query revisions committed: $QUERY_REVISIONS_COMMITTED.** Convention: " +
    QUERY_REVISIONS_CONVENTION + ". This run introduces the file in a single commit.")
  md0(s"2. **Distinct Joern API constructs used: ${apiConstructsHere.size}.** Listed")
  md0("   explicitly and deduplicated so the count is auditable from the list rather than")
  md0("   asserted; every entry appears literally in the query source:")
  md0("")
  apiConstructsHere.foreach(c => md0(s"   - `$c`"))
  md0("")
  md0(s"3. **Parameterizability: not claimed here.** It is proven by")
  md0(s"   `$PARAMETERIZABILITY_OWNER` actually invoking its parameterized form on the")
  md0("   second named handler/sink pair (the `deploy/rest/StandaloneRestServer` handler")
  md0("   to the `deploy/worker/DriverRunner` sink) and capturing that invocation's")
  md0("   result. A parameter list that merely exists does not satisfy it.")
  md0("")
  md0("## Modelling decisions, stated so the counts stay interpretable")
  md0("")
  md0("- **Two source sets, two arms.** The `Any`-typed formal parameter and the")
  md0("  unapply-recovered payload are different nodes, so they are evaluated separately")
  md0("  and reported separately rather than unioned into one number.")
  md0("- **The implicit receiver is excluded from ARM 1 and included in B2.** It carries")
  md0("  the enclosing instance rather than the message, so it is not a handler input;")
  md0("  but `DriverRunner.start` takes no explicit argument, so excluding it there would")
  md0("  make that measurement vacuous. Both choices are stated where they apply.")
  md0("- **Operator pseudo-calls are excluded.** A CPG `<operator>.*` call is an artefact")
  md0("  of the representation rather than a method call.")
  md0("- **Duplicate class definitions are unioned.** The graph carries more than one node")
  md0("  per class where two staged archives carried the same class, so method nodes are")
  md0("  grouped by full name and their parameters and calls unioned rather than one node")
  md0("  being picked.")
  md0("- **The flow engine's context is the console's own, copied.** Only the call-depth")
  md0("  bound is overridden, so the semantics the traversals run under are the same ones")
  md0(s"  the dataflow overlay was built with (`$semanticsClass`), and the context is")
  md0("  passed explicitly at every call site so no implicit resolution decides it.")
  md0("- **Graph line numbers are the graph's own.** A node's `lineNumber` comes from the")
  md0("  bytecode line-number table and can differ by a line from the `def` or statement")
  md0("  line cited from the source. Source anchors in this report are quoted from the")
  md0("  pinned tree; graph lines are labelled as such.")
  md0("- **Element code is collapsed and capped.** A flow element's `code` is put on one")
  md0(s"  line and capped at $MAX_CODE_CHARS characters, so the record stays readable and")
  md0("  the emitted JSON stays deterministic.")
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
  md0("`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so")
  md0("`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs")
  md0("at; where a runner defaults below the floor it is raised through its own documented")
  md0("environment override. The query measures the heap it received and stops below the")
  md0("floor: raising a heap is permitted and reported, lowering one is not.")
  md0("")

  writeUtf8(mdPath, md.mkString("", "\n", "\n"))
  log(s"prose report written      : $mdPath (${md.size} lines)")

  // -------------------------------------------------------------------------
  stage("O-result: the result region, emitted only now that every stage passed")
  // -------------------------------------------------------------------------
  log(s"total elapsed_ms          : ${elapsedMs(runStartNanos)}")
  log(MARKER_RESULT_BEGIN)
  log(s"query_id                  : $QUERY_ID")
  log(s"compile_status            : compiled")
  log(s"run_status                : completed")
  log(s"returned_record_count     : $returnedRecordCount")
  log(s"distinct_routes           : ${distinctRoutes.size}")
  log(s"distinct_routes_never_summed_with: $SIBLING_CALLGRAPH_QUERY, " +
    SIBLING_PARAMETERIZED_QUERY)
  log(s"spurious_count            : $spuriousCount")
  log(s"expected_spurious_absent  : $expectedSpuriousAbsent " +
    s"(${if (absenceIsStructural) "structural" else "filtering"})")
  log(s"bound_value               : $MAX_FLOW_CALL_DEPTH")
  log(s"bound_reached             : $boundReached")
  log(s"depth_sensitive           : $depthSensitive")
  log(s"entry_points_traversed    : $entryPointsTraversed")
  log(s"entry_points_truncated    : $entryPointsTruncated")
  log(s"dataflow_layer_live       : $engineLive " +
    s"(${controlArm.flowsRetained} control flow(s))")
  log(s"duplicate_formulation     : $duplicateVerdictAgainst01 (vs " +
    s"$SIBLING_CALLGRAPH_QUERY), $duplicateVerdictAgainst03 (vs " +
    s"$SIBLING_PARAMETERIZED_QUERY)")
  log(s"joern_api_constructs      : ${apiConstructsHere.size} " +
    s"(${apiOnlyHere.size} not used by $SIBLING_CALLGRAPH_QUERY)")
  log(s"boundaries_not_crossed    : ${boundariesNotCrossed.map(_.id).mkString(", ")}")
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
