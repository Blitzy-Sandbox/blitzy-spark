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
// NAMED CONSTANTS. No inline literal selects a node, an edge, a type, a
// method, a call site, a source, a sink or a route anywhere below: every
// selector the arms, the boundary measurements and the predicate search use is
// a named constant declared in this block, so what the query looks for can be
// read off in one place and cited in the result without reading the traversal.
//
// Three kinds of literal do appear below, and none of them selects anything in
// the graph. First, the lexical tokens of the two text readers - the Scala
// string-literal scanner that extracts a sibling query's declared constants,
// and the identity-record reader - where the literal describes the syntax of
// the file being read. Second, the two group names a boundary probe stamps on
// the records it publishes ("boundary-source" and "boundary-sink"), which name
// a record field rather than choose a node. Third, the field names and prose of
// the envelope and the prose report, which are output rather than selection.
// ===========================================================================

/** The slug. Both result filenames and the console log name derive from it. */
val QUERY_ID = "02-dataflow-unguarded-driver-launch"

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
 * matches is not a check.
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
 * after the load.
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

/** The sibling queries this one reports a duplicate-formulation verdict against. */
val SIBLING_CALLGRAPH_QUERY = "01-callgraph-unguarded-driver-launch"
val SIBLING_PARAMETERIZED_QUERY = "03-parameterized-handler-sink-pairs"
val SIBLING_QUERY_IDS = List(SIBLING_CALLGRAPH_QUERY, SIBLING_PARAMETERIZED_QUERY)
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
 *     query loads. Omitting it published a command whose most consequential
 *     input was invisible: a reader who had it set to another graph would
 *     reproduce a different load and read this envelope as describing it;
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

/** Maximum flows RETAINED and reported per (source group, sink group) pair. This
 *  governs output, not search: it is applied to flows the engine has already
 *  returned and this query has already materialized. */
val MAX_FLOWS_PER_PAIR = 8

/** Maximum flows MATERIALIZED from one engine evaluation - the cap on the engine's
 *  own return, applied as `take(cap + 1)` BEFORE the traversal is converted to a
 *  list, so the number of paths this query brings into memory is bounded and its
 *  truncation is observable rather than assumed absent.
 *
 *  It is deliberately a DIFFERENT quantity from MAX_FLOWS_PER_PAIR and
 *  MAX_FLOW_LENGTH. Those two cap what is retained and how much of each retained
 *  flow is reported, and a retention cap applied after a full materialization
 *  bounds the output rather than the traversal - so presenting either as the bound
 *  on the engine's return would overstate what this query controls. This cap is
 *  the one that governs the materialization itself.
 */
val MAX_ENGINE_FLOWS_PER_EVALUATION = 64

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

/** Cap on the indexed call-name sweeps: the sink call-site sweep, the ARM 2
 *  request-message accessor sweep, the message constructor and accessor call-site
 *  sweeps, the predicate call-site sweep, and the node-local call and operand
 *  materializations taken over those call sites. Every sweep this cap governs
 *  publishes its own observed count and its own truncation flag, and the cap's
 *  reported reached flag is the DISJUNCTION over all of them - so a truncation in
 *  any one sweep is visible in the cap's reached flag rather than being hidden by
 *  a sweep that completed. */
val MAX_CALL_SCAN = 200000
/** Cap on the keyed type, method and formal-parameter lookups - the entry-point,
 *  predicate, message, liveness-control, thread, JDK-launch and route-surface
 *  selections. Each is a lookup on an exact name or a type regex rather than a
 *  graph sweep, so it returns a handful of nodes at this pin; the cap exists so
 *  that "no traversal this query materializes runs without a cap" is literally
 *  true rather than true of the big ones. Like MAX_CALL_SCAN, its reported
 *  reached flag is the disjunction over every sweep it governs. */
val MAX_TYPE_SCAN = 100000

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
 *
 * The four constants below, and nothing else, are the predicate block: they are
 * intended to be the same text in all three queries of the probe, because the
 * three spurious counts are only comparable while the definition of "spurious"
 * is the same text. That sameness is not asserted here - it is MEASURED: the
 * four are named in the formulation identity block, stage L reads each sibling
 * source's own literals by those names, and the outcome is published per sibling
 * as `predicate_selector_literals_identical`. The route surface that used to sit
 * inside this block is a property of one query's own route rather than of the
 * shared definition, so it is declared separately below.
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
// exactly the four predicate constants - PREDICATE_TYPE, PREDICATE_NAME_REGEX,
// PREDICATE_SETTER_SUFFIX and PREDICATE_NAMED_FIVE - and nothing else. It is
// intended to be the same text as the corresponding block of the sibling
// queries: the three queries' spurious counts are only comparable if the
// definition of "spurious" is the same text. That is no longer ASSERTED here.
// Stage L reads each sibling source's own predicate constants at run time and
// publishes predicate_selector_literals_identical as a MEASURED boolean, so a
// divergence is reported rather than silently invalidating both counts. The
// route surface is NOT part of this block: it is a property of this query's own
// route and is declared below.

// -------------------------------------------------- the route surface, DERIVED
/**
 * The types whose methods carry THIS query's route, used to establish the
 * structural basis for the expected-spurious absence. Synthetic `$$anonfun$` and
 * `$$anon$` classes of these types are included by prefix.
 *
 * DERIVED from this query's own route rather than declared as a set that merely
 * looks plausible, and every entry names the role it plays:
 *
 *   - `HANDLER_TYPE` owns the entry points, which is where every arm's sources
 *     are selected from (stage E);
 *   - `MESSAGE_RELAY_HOST_TYPE` owns the consumer end of the RPC hop measured as
 *     boundary B1 - the worker that receives the launch message and creates the
 *     runner - so it is the relay this route passes through;
 *   - `SINK_HOST_RUNNER_TYPE` and `SINK_HOST_LAUNCHER_TYPE` are the two hosts
 *     `SINK_HOST_TYPE_REGEX` admits, and are where the privileged launch call
 *     sites actually sit (stage E and boundary B3). The runner is also the host
 *     of the engine-liveness control arm.
 *
 * This is deliberately NOT part of the byte-identical predicate block above. The
 * predicate constants define the term "spurious" and are compared across the
 * three sources under the names the formulation identity block declares, so they
 * must be the same text in all three. A route surface is a property of one
 * query's own route: query 03's second pair enters at
 * `org.apache.spark.deploy.rest.StandaloneRestServer`, which is not on this
 * query's route at all, and carrying that type here would search a surface this
 * route does not cross while omitting the two hosts it does. Stage J asserts
 * that every route end this query MEASURED is covered by the surface below, and
 * publishes per-prefix reach evidence, so the derivation is checked rather than
 * claimed.
 */
val MESSAGE_RELAY_HOST_TYPE = "org.apache.spark.deploy.worker.Worker"
val SINK_HOST_RUNNER_TYPE = "org.apache.spark.deploy.worker.DriverRunner"
val SINK_HOST_LAUNCHER_TYPE = "org.apache.spark.deploy.worker.ProcessBuilderLike"
val ROUTE_SURFACE_TYPE_PREFIXES = List(
  HANDLER_TYPE,
  MESSAGE_RELAY_HOST_TYPE,
  SINK_HOST_RUNNER_TYPE,
  SINK_HOST_LAUNCHER_TYPE)
/** The role each prefix plays on this route, published beside the reach
 *  evidence so a reader sees why each one is in the surface. */
val ROUTE_SURFACE_TYPE_ROLES = List(
  HANDLER_TYPE -> "entry-point owner: the handler every arm's sources are selected from",
  MESSAGE_RELAY_HOST_TYPE ->
    "relay: the consumer end of the RPC hop measured as boundary B1",
  SINK_HOST_RUNNER_TYPE ->
    ("sink host: the runner that holds the launch call site, and the host of the " +
      "engine-liveness control arm"),
  SINK_HOST_LAUNCHER_TYPE ->
    "sink host: the launcher surface the privileged start is declared on")

// ------------------------------------------- the formulation identity block
/**
 * What makes this query's FORMULATION what it is, declared in one place and
 * under names that all three queries of the probe share, so that the
 * duplicate-formulation comparison in stage L extracts the SIBLING sources'
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
 * query's the same way. It is asserted against MAX_FLOW_CALL_DEPTH at run time,
 * so the repetition cannot drift unnoticed.
 */
val FORMULATION_EDGE_KINDS = List("REACHING_DEF")
val FORMULATION_END_NODE_KINDS = List("METHOD_PARAMETER_IN", "EXPRESSION")
val FORMULATION_PAIR_IDS = List("pair-one")
val FORMULATION_BOUND_NAME = "MAX_FLOW_CALL_DEPTH"
val FORMULATION_BOUND_KIND =
  "call boundaries the backward data-flow search may expand"
val FORMULATION_BOUND_VALUE = 6
val FORMULATION_TRAVERSAL_SEMANTICS =
  "interprocedural data-flow reachability over the dataflow layer's " +
    "reaching-definition edges, selecting PARAMETER and EXPRESSION nodes as its ends"
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
/** Where a DATA-flow formulation's source end actually is, which is not where a
 *  call-graph formulation's is. Stated as part of the route surface because it
 *  is the reason the two formulations select different nodes. */
val ROUTE_SURFACE_SOURCE_NODE_SELECTION =
  "this query's source end is the SYNTHETIC partial-function method behind " +
    MASTER_SOURCE_FILE + ":409 and its " + MESSAGE_PARAMETER_TYPE +
    "-typed formal parameter, which the bytecode carries erased. A call-graph " +
    "formulation starts from the enclosing method instead, which is part of what the " +
    "duplicate_formulation entry against " + SIBLING_CALLGRAPH_QUERY + " rests on"
val ROUTE_SURFACE_UNAPPLY =
  MASTER_SOURCE_FILE + ":410 case RequestSubmitDriver(description) - the payload " +
    "arrives through a pattern match, which in bytecode is a type test, a cast and the " +
    "case class's own " + REQUEST_MESSAGE_ACCESSOR + " accessor rather than an assignment"
val ROUTE_SURFACE_ADDITIONAL_DATAFLOW_OBSTACLE =
  "beyond the four boundaries, a dataflow formulation faces the erasure of the message " +
    "payload: the handler's formal parameter is typed " + MESSAGE_PARAMETER_TYPE + ", so " +
    "the parameter carries no message type for a reaching-definition search to follow, " +
    "and the payload the body uses is recovered by the unapply above rather than " +
    "assigned from that parameter. This query answers that by selecting BOTH sides as " +
    "sources in their own right - the erased parameter in ARM 1 and the accessor result " +
    "in ARM 2 - and reporting the two separately, so neither choice is hidden inside " +
    "one number. It is reported as a property of the formulation, not worked around"
/** The four hops the route crosses that no reaching-definition edge can join,
 *  in boundary-id order, each stated as the reason rather than as an assertion.
 *  These are the same structural hops the call-graph formulation names, stated
 *  for the edge kind THIS query traverses. */
val ROUTE_SURFACE_NON_CONNECTIVITY_HOPS_FOR_A_DATA_FLOW = List(
  "B1-rpc: a message send carries no data edge: the value is serialized out of one " +
    "process at " + MASTER_SOURCE_FILE + ":1367 and deserialized into another at " +
    WORKER_SOURCE_FILE + ":687, so the sender's argument and the receiver's accessor " +
    "result are two unrelated definitions as far as reaching-definition edges are " +
    "concerned",
  "B2-thread: " + DRIVER_RUNNER_SOURCE_FILE + ":123 Thread.start() -> run() at :90 is " +
    "a JVM scheduling relation: the start frame returns immediately and run() is " +
    "entered on another thread, so no data edge joins a definition in the one to a use " +
    "in the other",
  "B3-interface: an interface invocation names the declaring type (" +
    DRIVER_RUNNER_SOURCE_FILE + ":270), so joining the receiver at the abstract call " +
    "site to a definition inside the implementation at :276 needs the type hierarchy; " +
    "a reaching-definition edge does not cross that on its own",
  "B4-partial-function: the method named " + HANDLER_METHOD + " at " +
    MASTER_SOURCE_FILE + ":409 only constructs the partial function; the case bodies " +
    "live in the synthetic class's " + ENTRY_SYNTHETIC_METHOD + ", so a source selected " +
    "on the source-level name is a definition in a method that contains none of the " +
    "route, and the payload the body uses arrives through an unapply rather than " +
    "through that parameter")
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
 * probe. The number is MEASURED at run time from the repository's own history
 * rather than written down here, because a written-down count is a claim that
 * drifts the moment the file is committed again. The commit that publishes this
 * query's own result files is necessarily NOT among the commits counted: it
 * cannot exist while the run that writes those files is still in progress.
 */
val QUERY_REVISIONS_CONVENTION =
  "commits touching " + QUERY_SOURCE_DIR + "/" + QUERY_ID +
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

/** Invoking git as a measurement, not as a shell: no shell, closed stdin and a
 *  bounded wait. */
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
  "EngineConfig.copy",
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
 * Nothing about a sibling query is transcribed here. Two constants used to
 * live at this point - query 01's published boundary verdicts and its published
 * API construct list, both copied out of its envelope - and both are gone. A
 * copied value can drift from the envelope it was copied from with nothing
 * recording it, which is the failure this probe must not contain. What replaces
 * them: the API construct list is read out of the SIBLING SOURCE at run time in
 * stage L, by the same extractor that reads this query's own; and a sibling's
 * boundary verdicts are that sibling's own measurement, published in that
 * sibling's envelope, so this query publishes its own and cites no other.
 */

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
  log(s"query_id                  : $QUERY_ID")
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
      "the override is inherited, and where a runner defaults lower raise it through its " +
      "own documented environment override. Raising a heap is permitted and reported; " +
      "lowering one is not, because a truncated result's silence cannot be told apart " +
      "from a clean one - and for a dataflow query that risk is at its highest")
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
  if (!declaredBoundValue.contains(MAX_FLOW_CALL_DEPTH.toLong)) {
    abortRun("the formulation identity block declares FORMULATION_BOUND_VALUE=" +
      declaredBoundValue.map(_.toString).getOrElse("<absent>") + " while the traversal " +
      s"runs at $FORMULATION_BOUND_NAME=$MAX_FLOW_CALL_DEPTH. The declared block is what " +
      "the duplicate-formulation comparison reads, so a declaration that has drifted " +
      "from the behaviour it describes would make that comparison wrong")
  }
  log(s"query source              : $sourceRepoRelative")
  log(s"query source bytes        : $sourceByteSize")
  log(s"query source sha256       : $sourceSha256")
  log(s"alternative loader        : absent (measured: $alternativeLoaderOccurrences " +
    "occurrences in the source text)")
  log(s"declared bound            : $FORMULATION_BOUND_NAME=" +
    s"${declaredBoundValue.map(_.toString).getOrElse("<absent>")} (agrees with the " +
    "traversal)")
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
  //   - importCpg is given THAT path and no other;
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
  // against. Every candidate that exists is read; ambiguity inside one record and
  // disagreement between two are both fatal.
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
  val recordCorroborators =
    identityCandidates.toList.tail.map(_.label)
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
  // The workspace is the probe's own, never a shared or default one: joern writes
  // a multi-gigabyte project tree (including a working copy of the graph) into
  // it, removes and rewrites project state inside it, and two Joern processes
  // sharing one corrupt each other. It is switched BEFORE any load.
  // queries/joern/.workspace is the AAP-named location and carries its own
  // .gitignore, so the scratch stays out of the commit without editing upstream
  // Spark's .gitignore.
  //
  // The load applies the console's default overlays to the working copy, and the
  // OSS dataflow layer is one of them. The persisted graph is not modified: the
  // engine works on the copy, which is what makes re-verifying the identity
  // before every load a meaningful check rather than a formality.
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
  // account above.
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
  log("import binding            : PASS - the imported copy is byte-for-byte and " +
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

  // The flow engine's context: the console's own EngineContext - and therefore
  // the same semantics the dataflow overlay was created with - with ONE FIELD
  // overridden, the call-depth bound. The override is taken on the BASE
  // CONTEXT'S OWN CONFIG (`baseEngineContext.config.copy(...)`) rather than on a
  // freshly constructed EngineConfig, and the difference is not cosmetic:
  // EngineConfig carries five fields, so constructing one would silently reset
  // the four this query does not name - the shared cache, the initial table
  // capacity, the max-arguments-to-allow bound and the semantics-only flag - to
  // whatever the class's defaults happen to be, while the comment here and the
  // published evidence both say only the depth changed. Copying the overlay's
  // own config preserves every other field it was created with, so the bound is
  // the only difference between the base context and the three below. Each is
  // passed EXPLICITLY at every call site, so no implicit resolution decides
  // which context a traversal ran under.
  val baseEngineContext: EngineContext = context
  val flowContext: EngineContext =
    baseEngineContext.copy(
      config = baseEngineContext.config.copy(maxCallDepth = MAX_FLOW_CALL_DEPTH))
  val shallowFlowContext: EngineContext =
    baseEngineContext.copy(
      config = baseEngineContext.config.copy(maxCallDepth = MAX_FLOW_CALL_DEPTH_SHALLOW))
  val boundaryFlowContext: EngineContext =
    baseEngineContext.copy(
      config = baseEngineContext.config.copy(maxCallDepth = MAX_BOUNDARY_FLOW_CALL_DEPTH))
  val semanticsClass = baseEngineContext.semantics.getClass.getName
  log(s"flow engine semantics     : $semanticsClass (inherited from the console context)")
  log(s"flow engine context       : the console's own, copied with only " +
    "EngineConfig.maxCallDepth overridden on the base context's own config")
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
  /** The marker a code string carries when MAX_CODE_CHARS truncated it. Named
   *  rather than inline because the cap's "reached" flag is MEASURED from the
   *  emitted records by looking for exactly this marker. */
  val CODE_TRUNCATION_MARKER = "..."
  def codeOf(n: AstNode): String = {
    val one = n.code.replace('\n', ' ').replace('\r', ' ').trim
    if (one.length <= MAX_CODE_CHARS) one
    else one.take(MAX_CODE_CHARS) + CODE_TRUNCATION_MARKER
  }

  /** A stable identity for a node, used for ordering and de-duplication. */
  def nodeKey(n: AstNode): String =
    s"${n.label}@${enclosingMethodOf(n)}#${lineOfNode(n)}:${codeOf(n)}"

  // --- the entry methods: BOUNDARY 4 -----------------------------------------
  // The handler body compiles into a synthetic partial-function class, so the
  // graph's entry point is that class's applyOrElse. The source-level method of
  // the same name is ALSO selected, so the report can show what each of the two
  // actually contains rather than assuming they are interchangeable.
  val syntheticTypeDecls = boundedList(
    "entry: synthetic partial-function type declarations", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"type declarations whose full name matches $ENTRY_SYNTHETIC_TYPE_REGEX")(
    cpg.typeDecl.fullName(ENTRY_SYNTHETIC_TYPE_REGEX))
  val syntheticEntryNodes = boundedList(
    "entry: methods on those synthetic types", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"methods declared on those type declarations, before the $ENTRY_SYNTHETIC_METHOD " +
      "name filter")(syntheticTypeDecls.iterator.flatMap(_.method))
    .filter(_.name == ENTRY_SYNTHETIC_METHOD)
  val sourceLevelHandlerNodes = boundedList(
    "entry: source-level handler methods", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"methods named $HANDLER_METHOD declared on $HANDLER_TYPE")(
    cpg.typeDecl.fullNameExact(HANDLER_TYPE).method.nameExact(HANDLER_METHOD))
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
  // BOUNDED, and labelled by the caller, because this def serves four different
  // selections (the entry methods, the liveness control host, the B2 thread host
  // and the B4 source-level handler) and a shared label would publish four sweeps
  // a reader could not tell apart.
  def parametersOf(label: String)(ms: List[Method]): List[MethodParameterIn] =
    boundedList(s"parameters: $label", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"every formal parameter declared on the $label method(s), before any receiver " +
        "exclusion or de-duplication")(ms.iterator.flatMap(_.parameter))
      .distinctBy(p => (p.method.fullName, p.index, p.name, p.typeFullName))
      .sortBy(p => (p.method.fullName, p.index, p.name))
  val allEntryParameters =
    parametersOf("entry")(entryGroupsTraversed.flatMap(_._2))
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
  val payloadCallsScanned = boundedList(
    s"ARM 2: calls named $REQUEST_MESSAGE_ACCESSOR", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    s"the indexed sweep over every call named $REQUEST_MESSAGE_ACCESSOR in the graph, " +
      "before the operator, declaring-type and entry-method constraints")(
    cpg.call.nameExact(REQUEST_MESSAGE_ACCESSOR))
  val payloadScanTruncated = boundedSweeps
    .find(_.label == s"ARM 2: calls named $REQUEST_MESSAGE_ACCESSOR").exists(_.truncated)
  val payloadCallsPrimary = payloadCallsScanned
    .filterNot(isOperatorCall)
    .filter(_.methodFullName.startsWith(REQUEST_MESSAGE_TYPE + "."))
    .filter(c => entryMethodNameSet.contains(c.method.fullName))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  // Stated fallback: any call inside the entry methods whose callee is declared
  // on the submission message type. Used only where the primary selection is
  // empty, and the report names which selection produced the arm.
  val payloadCallsFallback = boundedList(
    "ARM 2 fallback: calls inside the entry methods", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "every call node inside the traversed entry methods, before the operator and " +
      "declaring-type constraints")(
    entryGroupsTraversed.iterator.flatMap(_._2).flatMap(_.call))
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
  val startCallsScanned = boundedList(
    s"sink: calls named $SINK_CALL_NAME", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    s"the indexed sweep over every call named $SINK_CALL_NAME in the graph, before the " +
      "callee-regex and host-type constraints")(cpg.call.nameExact(SINK_CALL_NAME))
  val sinkScanTruncated =
    boundedSweeps.find(_.label == s"sink: calls named $SINK_CALL_NAME").exists(_.truncated)
  val sinkCallsAll = startCallsScanned.filter(_.methodFullName.matches(SINK_CALLEE_REGEX))
  val sinkCalls = sinkCallsAll
    .filter(c => owningTypes(c.method).exists(_.matches(SINK_HOST_TYPE_REGEX)))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val sinkHostNames = sinkCalls.map(_.method.fullName).distinct.sorted
  val sinkReceivers = boundedList(
    "sink: receiver operands of the launch call sites", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "the receiver node of each launch call site retained above")(
    sinkCalls.iterator.flatMap(_.receiver))
  val sinkArguments = boundedList(
    "sink: argument operands of the launch call sites", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "the argument nodes of each launch call site retained above")(
    sinkCalls.iterator.flatMap(_.argument))
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
  val predicateTypeDecls = boundedList(
    "predicate: type declarations", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"type declarations whose full name is exactly $PREDICATE_TYPE")(
    cpg.typeDecl.fullNameExact(PREDICATE_TYPE))
  if (predicateTypeDecls.isEmpty) {
    abortRun(s"$PREDICATE_TYPE is not present in the graph, so the mechanical " +
      "definition of a spurious route has no predicate set to rest on")
  }
  val predicateTypeMethods = boundedList(
    "predicate: methods on that type", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"every method declared on $PREDICATE_TYPE, before the three selector steps")(
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
  val predicateFullNameSet = predicateFullNames.toSet
  log("predicate selector        : PASS (asserted against the graph, not the source)")

  // Where those predicates are actually CALLED. This is the structural basis for
  // the expected-spurious question and it is measured, never inferred.
  // BOUNDED. This is a graph-wide sweep: the call sites of five methods, over
  // every caller in the graph. Capped under MAX_CALL_SCAN like the sink sweep,
  // and its truncation flag published beside that cap's, so the claim that no
  // traversal materializes without a cap is true of this one too.
  val predicateCallSites = boundedList(
    "predicate: call sites of the five named predicates", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "incoming call sites of the five resolved predicate methods, graph-wide, before " +
      "the route-surface filter")(predicateFinal.iterator.flatMap(_.callIn))
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

  /** The one place a flow traversal is invoked, and the one place the engine's
   *  return is materialized. THREE mechanisms bound it and they are named
   *  separately because they bound different things:
   *
   *    - the engine's own backward search is governed by the
   *      `EngineConfig.maxCallDepth` on the context handed in;
   *    - the node sets it is given are pre-capped by MAX_SOURCE_NODES and
   *      MAX_SINK_NODES, each with its own truncation flag;
   *    - the number of paths brought into memory from the result is capped HERE by
   *      MAX_ENGINE_FLOWS_PER_EVALUATION, taken as cap+1 before the list is built,
   *      so this sweep reports whether it was truncated like every other.
   *
   *  What this query CANNOT bound is how much work the engine performs internally
   *  before it yields a path: the dataflow API exposes no control over that and
   *  reports no counter for it. That limitation is published rather than implied,
   *  and it is why the retention caps are never described as bounding the search.
   */
  def flowsFor(
      label: String,
      sinks: List[CfgNode],
      sources: List[CfgNode],
      ctx: EngineContext): List[io.joern.dataflowengineoss.language.Path] =
    if (sinks.isEmpty || sources.isEmpty) Nil
    else boundedList(s"engine flows: $label", "MAX_ENGINE_FLOWS_PER_EVALUATION",
      MAX_ENGINE_FLOWS_PER_EVALUATION,
      "paths materialized from one reachableByFlows evaluation, taken as cap+1 " +
        "before the engine's return was converted to a list, so a truncation is " +
        "measured at the materialization rather than inferred from what was retained")(
      sinks.iterator.reachableByFlows(sources.iterator)(ctx))

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
          val found = flowsFor(s"$armId $sourceGroupName -> $sinkGroupName",
            sinkGroupNodes, sourceGroupNodes, ctx)
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
  val controlHostMethods = boundedList(
    "liveness control: host methods", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"methods named $CONTROL_HOST_METHOD declared on $CONTROL_HOST_TYPE")(
    cpg.typeDecl.fullNameExact(CONTROL_HOST_TYPE).method.nameExact(CONTROL_HOST_METHOD))
  val controlHostNames = controlHostMethods.map(_.fullName).distinct.sorted
  val controlParametersAll = parametersOf("liveness control host")(controlHostMethods)
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
    .sortBy(r => (r.sourceGroup, r.sinkGroup, r.elementCount, r.signature, r.armId))
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
    val found = flowsFor(s"boundary $label", sinks.take(MAX_SINK_NODES),
      sources.take(MAX_SOURCE_NODES), boundaryFlowContext)
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
  val messageTypeDecls = boundedList(
    "B1: message type declarations", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"type declarations whose full name is exactly $MESSAGE_TYPE")(
    cpg.typeDecl.fullNameExact(MESSAGE_TYPE))
  val messageMethods = boundedList(
    "B1: methods on the message type", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"every method declared on $MESSAGE_TYPE, before the constructor and accessor " +
      "name filters")(messageTypeDecls.iterator.flatMap(_.method))
  val messageCtors = messageMethods.filter(_.name == MESSAGE_CTOR_NAME)
  val messageAccessors = messageMethods.filter(m => MESSAGE_ACCESSOR_NAMES.contains(m.name))
  /** Call sites inside the message type or its companion are the case class's own
   *  generated machinery (apply, copy, unapply, equals, productElement), not a
   *  send or a receive. They are excluded by owning type so the producer and
   *  consumer sets are the two real ends of the hop. */
  val messageOwnTypes = Set(MESSAGE_TYPE, MESSAGE_TYPE + "$")
  def outsideMessageType(m: Method): Boolean =
    !owningTypes(m).exists(messageOwnTypes.contains)
  // BOUNDED, both of them, for the same reason as the predicate sweep above:
  // these are the call sites of the message constructor and of its accessors over
  // every caller in the graph.
  val messageProducerSites = boundedList(
    "B1: producer call sites of the message constructor", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    s"incoming call sites of $MESSAGE_TYPE.$MESSAGE_CTOR_NAME, graph-wide, before the " +
      "own-type exclusion")(messageCtors.iterator.flatMap(_.callIn))
    .filter(c => outsideMessageType(c.method))
    .distinctBy(c => (c.method.fullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, lineOf(c)))
  val messageConsumerSites = boundedList(
    "B1: consumer call sites of the message accessors", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "incoming call sites of the message type's field accessors (" +
      MESSAGE_ACCESSOR_NAMES.mkString(", ") + "), graph-wide, before the own-type " +
      "exclusion")(messageAccessors.iterator.flatMap(_.callIn))
    .filter(c => outsideMessageType(c.method))
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val messageProducers = messageProducerSites.map(_.method.fullName).distinct.sorted
  val messageConsumers = messageConsumerSites.map(_.method.fullName).distinct.sorted
  /** The producer END for a dataflow question is the VALUE handed to the message
   *  constructor, not the constructor call itself, so the arguments are the
   *  source set. The consumer end is the accessor's result. */
  val messageProducerArguments: List[CfgNode] = boundedList(
    "B1: arguments of the message constructor call sites", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "the argument nodes of the producer call sites selected above, which are the VALUES " +
      "handed to the constructor")(messageProducerSites.iterator.flatMap(_.argument))
    .distinctBy(nodeKey)
    .sortBy(nodeKey)
  val messageConsumerNodes: List[CfgNode] = messageConsumerSites
    .distinctBy(nodeKey)
    .sortBy(nodeKey)
  val (b1Found, b1Flows) =
    boundaryProbe("B1-rpc", messageProducerArguments, messageConsumerNodes)

  // ---------------- the route surface, CHECKED against the measured route -----
  // The surface is derived from this query's own route (see its declaration), and
  // here that derivation is verified rather than trusted: every end this query
  // actually measured - the entry points, the B1 consumer end, and the sink hosts
  // - must be owned by one of the prefixes. A surface that omitted a host the
  // route crosses would search for the predicate everywhere except where the
  // route goes, and the absence it then reported would be an artefact of the
  // surface rather than a property of the route.
  val measuredRouteEnds: List[(String, String)] =
    entryGroups.map(g => ("entry point", g._1)) :::
      messageConsumers.map(c => ("B1 consumer end (the relay)", c)) :::
      sinkHostNames.toList.sorted.map(h => ("sink host", h))
  val routeEndsNotCovered = measuredRouteEnds.filterNot { case (_, fullName) =>
    ROUTE_SURFACE_TYPE_PREFIXES.exists(p => fullName.startsWith(p))
  }
  measuredRouteEnds.foreach { case (role, fullName) =>
    log(s"  route end ($role): $fullName")
  }
  if (routeEndsNotCovered.nonEmpty) {
    abortRun("the route surface does not cover every end this query measured: " +
      routeEndsNotCovered.map { case (role, fn) => s"$role $fn" }.mkString("; ") +
      s". The surface is ${ROUTE_SURFACE_TYPE_PREFIXES.mkString(", ")}, and a predicate " +
      "search over a surface the route leaves would report an absence that is a " +
      "property of the surface rather than of the route")
  }
  log(s"route surface coverage    : PASS - all ${measuredRouteEnds.size} measured route " +
    s"end(s) are owned by one of ${ROUTE_SURFACE_TYPE_PREFIXES.size} prefix(es)")

  /** Per-prefix reach evidence: what the prefix owns in the graph, what role it
   *  plays on the route, and which measured ends it accounts for. Published so a
   *  reader can see that each prefix earns its place rather than taking the
   *  derivation on trust. */
  val routeSurfaceReach: List[String] = ROUTE_SURFACE_TYPE_PREFIXES.map { prefix =>
    val role = ROUTE_SURFACE_TYPE_ROLES.toMap.getOrElse(prefix, "unstated")
    val typeDeclsOwned = boundedList(
      s"route surface: type declarations under $prefix", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
      s"type declarations whose full name starts with $prefix, including synthetic " +
        "closure and anonymous classes")(
      cpg.typeDecl.fullName(java.util.regex.Pattern.quote(prefix) + ".*"))
    val endsAccountedFor = measuredRouteEnds.filter(_._2.startsWith(prefix))
    val predicateCallsHere =
      predicateCallSitesOnRoute.count(c => owningTypes(c.method).exists(_.startsWith(prefix)))
    log(s"  surface $prefix: typeDecls=${typeDeclsOwned.size} " +
      s"measured_ends=${endsAccountedFor.size} predicate_call_sites=$predicateCallsHere")
    jobj(6, List(
      "type_prefix" -> jstr(prefix),
      "role_on_this_route" -> jstr(role),
      "type_declarations_owned" -> jnum(typeDeclsOwned.size.toLong),
      "measured_route_ends_accounted_for" -> jnum(endsAccountedFor.size.toLong),
      "measured_route_ends" -> jstrArr(endsAccountedFor.map(e => s"${e._1}: ${e._2}")),
      "predicate_call_sites_here" -> jnum(predicateCallsHere.toLong),
      "reached_by_the_route" -> jbool(endsAccountedFor.nonEmpty)))
  }
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
  val threadHostMethods = boundedList(
    "B2: thread host methods", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"methods named $THREAD_HOST_METHOD declared on $THREAD_HOST_TYPE")(
    cpg.typeDecl.fullNameExact(THREAD_HOST_TYPE).method.nameExact(THREAD_HOST_METHOD))
  /** Every parameter INCLUDING the receiver: DriverRunner.start takes no explicit
   *  argument, so the only value that could cross the hop is the enclosing
   *  instance, and excluding it would make the measurement vacuous. The
   *  inclusion is the opposite of the ARM 1 convention and is stated as such. */
  val threadSourceNodes: List[CfgNode] =
    parametersOf("B2 thread host")(threadHostMethods)
  val threadBodyMethods = boundedList(
    "B2: thread body methods", "MAX_TYPE_SCAN", MAX_TYPE_SCAN,
    s"methods named $THREAD_BODY_METHOD declared on types matching " +
      THREAD_BODY_TYPE_REGEX)(
    cpg.typeDecl.fullName(THREAD_BODY_TYPE_REGEX).method.nameExact(THREAD_BODY_METHOD))
  val threadBodyNames = threadBodyMethods.map(_.fullName).distinct.sorted
  val threadBodyContinuationCalls = boundedList(
    "B2: calls in the thread body methods", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "every call node in the bodies of the thread-body methods retained above, before " +
      s"the operator filter and the $THREAD_BODY_CONTINUATION_NAME name filter")(
    threadBodyMethods.iterator.flatMap(_.call))
    .filterNot(isOperatorCall)
    .filter(_.name == THREAD_BODY_CONTINUATION_NAME)
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val threadBodyContinuationReceivers = boundedList(
    "B2: receiver operands of the thread-body continuation calls", "MAX_CALL_SCAN",
    MAX_CALL_SCAN,
    "the receiver node of each thread-body continuation call retained above")(
    threadBodyContinuationCalls.iterator.flatMap(_.receiver))
  val threadSinkNodes: List[CfgNode] =
    (threadBodyContinuationCalls ++ threadBodyContinuationReceivers)
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
  val abstractLaunchReceivers = boundedList(
    "B3: receiver operands of the abstract launch call sites", "MAX_CALL_SCAN",
    MAX_CALL_SCAN,
    "the receiver node of each abstract launch call site")(
    abstractLaunchCalls.iterator.flatMap(_.receiver))
  val abstractLaunchArguments = boundedList(
    "B3: argument operands of the abstract launch call sites", "MAX_CALL_SCAN",
    MAX_CALL_SCAN,
    "the argument nodes of each abstract launch call site")(
    abstractLaunchCalls.iterator.flatMap(_.argument))
  val concreteLaunchReceivers = boundedList(
    "B3: receiver operands of the concrete launch call sites", "MAX_CALL_SCAN",
    MAX_CALL_SCAN,
    "the receiver node of each concrete JDK launch call site")(
    concreteLaunchCalls.iterator.flatMap(_.receiver))
  val interfaceSourceNodes: List[CfgNode] =
    (abstractLaunchReceivers ++ abstractLaunchArguments)
      .distinctBy(nodeKey)
      .sortBy(nodeKey)
  val interfaceSinkNodes: List[CfgNode] =
    (concreteLaunchCalls ++ concreteLaunchReceivers)
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
  val sourceLevelParameters: List[CfgNode] =
    parametersOf("B4 source-level handler")(sourceLevelHandlerNodes)
    .filterNot(p => p.name == THIS_PARAMETER_NAME || p.index == 0)
  val syntheticBodyContinuationCalls = boundedList(
    "B4: calls in the synthetic handler bodies", "MAX_CALL_SCAN", MAX_CALL_SCAN,
    "every call node in the bodies of the synthetic entry methods, before the operator " +
      s"filter and the $HANDLER_BODY_CONTINUATION_NAME name filter")(
    syntheticEntryNodes.iterator.flatMap(_.call))
    .filterNot(isOperatorCall)
    .filter(_.name == HANDLER_BODY_CONTINUATION_NAME)
    .distinctBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
    .sortBy(c => (c.method.fullName, c.methodFullName, lineOf(c)))
  val syntheticBodyContinuationArguments = boundedList(
    "B4: argument operands of the synthetic-body continuation calls", "MAX_CALL_SCAN",
    MAX_CALL_SCAN,
    "the argument nodes of each synthetic-body continuation call retained above")(
    syntheticBodyContinuationCalls.iterator.flatMap(_.argument))
  val syntheticBodySinkNodes: List[CfgNode] =
    (syntheticBodyContinuationCalls ++ syntheticBodyContinuationArguments)
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
    .sortBy(r => (r.armId, r.sourceGroup, r.sinkGroup, r.elementCount, r.signature))
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
  // Whether two queries are the same formulation is a property of the two
  // QUERIES rather than of either run's numbers, so it is answered from the two
  // sources' declared formulation identity blocks and NOT from any figure
  // transcribed out of a sibling's published envelope. What this query still
  // measures for itself, and publishes as its own, is what its OWN traversal
  // returned: its boundary verdicts, its element-level flow records and its own
  // API construct list. A sibling's verdicts are that sibling's measurement,
  // published in that sibling's envelope, and are not copied here.
  val apiConstructsHere = JOERN_API_CONSTRUCTS.distinct.sorted
  val boundariesNotCrossedHere =
    boundaries.filterNot(_.crossedByADataFlow).map(_.id).sorted
  /** Element-level flow records: sequences of IDENTIFIER, METHOD_PARAMETER_IN and
   *  CALL nodes with their graph lines. A method-level call-edge traversal does
   *  not produce this record kind at all, which is a measured difference in
   *  what this formulation can return - measured HERE, on this query's own
   *  output, rather than inferred from another query's. */
  val elementLevelFlowRecords =
    distinctRoutes.size + boundaryFlows.size + controlArm.flows.size
  val divergenceEvidence =
    "measured on this query's own output: " + elementLevelFlowRecords.toString +
      " element-level flow record(s), each a sequence of IDENTIFIER, " +
      "METHOD_PARAMETER_IN and CALL nodes with their graph lines - including " +
      controlArm.flows.size.toString + " from a formal parameter to the launch call " +
      "inside a single method - and " + boundariesNotCrossedHere.size.toString +
      " of the " + boundaries.size.toString + " measured hops not crossed by a data " +
      "flow (" + boundariesNotCrossedHere.mkString(", ") + "). A record of this kind " +
      "is a shape a method-level call-edge traversal does not produce for any input"
  log(s"API constructs here       : ${apiConstructsHere.size}")
  log(s"boundary verdicts here    : uncrossed=${boundariesNotCrossedHere.mkString(", ")}")
  log(s"element-level flow records: $elementLevelFlowRecords " +
    "(a record kind a method-level call-edge traversal does not produce)")

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
      // asserted agreement - "at the same value", "empty in BOTH directions",
      // "byte for byte" - and then listed the FALSE ones after the words "the
      // formulations differ on". The published sentence therefore said that two
      // formulations differed on the bound being the same value, which reverses
      // the structured verdict sitting a few fields below it in the same
      // envelope. A reader trusting the prose read the opposite of the
      // measurement.
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

  // -------------------------------------------------------------------------
  stage("N-write: the envelope, the prose report and the console log")
  // -------------------------------------------------------------------------
  val resultsDir = repoRoot.resolve(RESULTS_DIR)
  Files.createDirectories(resultsDir)
  val jsonPath = resultsDir.resolve(s"$QUERY_ID.json")
  val mdPath = resultsDir.resolve(s"$QUERY_ID.md")

  // The publication identifier. DERIVED, never a timestamp: every member of one
  // publication carries it, so a consumer holding two members whose identifiers
  // DIFFER knows it holds two generations. The converse does not hold: equal
  // identifiers mean equal (query, source, graph), which two separate invocations
  // share, so the member-set identifier is what settles sameness of generation.
  // A nanotime would distinguish invocations and would
  // break the byte-identity contract this envelope states, because an unchanged
  // source over an unchanged graph would then emit different bytes on every
  // run. Deriving it from the source digest and the graph's identity gives the
  // same guarantee for free.
  val publicationId = sha256OfBytes(
    List(QUERY_ID, sourceSha256, shaObserved, sizeFollow.toString,
      methodCount.toString).mkString("\n").getBytes(StandardCharsets.UTF_8))
  log(s"publication id            : $publicationId")
  publicationIdOfRecord = Some(publicationId)

  /** What the route set MEANS, computed from the route set rather than written
   *  down. Neither form says anything about Spark. */
  val routeOutcomeStatement =
    if (distinctRoutes.isEmpty)
      "zero routes returned: records carries no record of kind route. The pair is not " +
        "joined by a reaching-definition path across the hops above, so a bounded " +
        "dataflow traversal over data edges returns none. That is the capability " +
        "finding this query exists to report - what this formulation can and cannot " +
        "express over this graph - and it is reported as measured. The bound was not " +
        "loosened or removed, the query was not widened, no arm was added to obtain a " +
        "route, and no flow was manufactured. It is not a statement about Spark, about " +
        "any Spark component or about any configuration"
    else
      distinctRoutes.size + " distinct route(s) returned, each published in full under " +
        "records with the arm that returned it, its ordered element sequence, the " +
        "signature its identity was taken on and whether it passed one of the named " +
        "predicates. A route is a traversal result and carries no judgement of the code " +
        "it traverses"

  /**
   * Effort measure 2, audited by measurement rather than by assertion. Every
   * entry in the declared construct list names a member this source invokes,
   * and that is checked here against the source text with the list's OWN
   * declaration excised first, so no entry can satisfy itself by appearing in
   * the list.
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

  /** The relation against the call-graph sibling, which two fields below report
   *  on specifically: the API constructs this formulation uses and that one does
   *  not, and whether the two sources' predicate selector literals are the same
   *  text. Both are taken from the ONE comparison stage L computed; neither is
   *  transcribed, and where the sibling source could not be read both say so. */
  /**
   * Every cap this query counts, measured at the scope its own name states,
   * across every arm that ran: the three route-bearing arms, the engine-liveness
   * control and the four boundary probes. The engine's INTERNAL call-depth
   * bounds are deliberately reported as not observable rather than as false -
   * the engine exposes no truncation flag for them - and depth sensitivity is
   * the substitute evidence, reported under its own field.
   */
  val allFlowRecords =
    routeArms.flatMap(_.flows) ++ controlArm.flows ++ boundaryFlows
  val lengthCapReachedAnywhere =
    allArms.exists(_.lengthCapReached) || allFlowRecords.exists(_.elementsTruncated)
  val codeCapReachedAnywhere = allFlowRecords
    .exists(_.elements.exists(_.code.endsWith(CODE_TRUNCATION_MARKER)))
  val stepCapReachedAnywhere = allArms.exists(_.stepCapReached)
  val perPairCapReachedAnywhere = allArms.exists(_.perPairCapReached)
  val sourceNodeCapReachedAnywhere = allArms.exists(_.sourceNodesTruncated > 0)
  val sinkNodeCapReachedAnywhere =
    sinkNodesTruncated > 0 || allArms.exists(_.sinkNodesTruncated > 0)
  val sourceGroupCapReachedAnywhere = allArms.exists(_.sourceGroupsTruncated > 0)
  val BOUND_REACHED_YES = "reached"
  val BOUND_REACHED_NO = "not reached"
  val BOUND_REACHED_UNOBSERVABLE = "not observable from the engine's output"
  def boundReachedLiteral(reached: Boolean): String =
    if (reached) BOUND_REACHED_YES else BOUND_REACHED_NO

  val callgraphRelation = relations.find(_.theirs.queryId == SIBLING_CALLGRAPH_QUERY)
  val callgraphRelationEstablished = callgraphRelation.exists(_.theirs.established)
  val apiConstructsOnlyHereVsCallgraph =
    callgraphRelation.map(_.apiOnlyHere).getOrElse(Nil)

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
    "distinct_routes_identity_function" -> jstr("a route identity is the triple " +
      "(source group, sink group, element signature), the signature being the ordered " +
      "sequence of LABEL@enclosing-method#graph-line over the flow's elements. Two " +
      "returns with equal triples are ONE route however many traversal orders produced " +
      "them, and the three route-bearing arms are deduplicated against each other on " +
      "it rather than added together. The unit is the distinct FLOW, which is what " +
      "makes the count auditable from the records themselves: every record below " +
      "publishes the signature its identity was taken on"),
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
    "observable_bound_reached" -> jbool(observableBoundReached),
    "observable_bound_reached_convention" -> jstr("the conjunction of every cap this " +
      "query's own evaluator counts: the per-source step cap, the per-pair flow cap, " +
      "the flow-length cap and the source, sink and entry-point truncation counters. " +
      "It is " + observableBoundReached + " because each of those is " +
      (if (observableBoundReached) "not uniformly false" else "false or zero") +
      " in all three route-bearing arms and in the control. It deliberately excludes " +
      "the engine's internal call-depth bound, which the engine reports no truncation " +
      "flag for; bounds_reached states each constant separately"),
    "bounds" -> jobj(2, List(
      "MAX_FLOW_CALL_DEPTH" -> jnum(MAX_FLOW_CALL_DEPTH.toLong),
      "MAX_FLOW_CALL_DEPTH_SHALLOW" -> jnum(MAX_FLOW_CALL_DEPTH_SHALLOW.toLong),
      "MAX_BOUNDARY_FLOW_CALL_DEPTH" -> jnum(MAX_BOUNDARY_FLOW_CALL_DEPTH.toLong),
      "MAX_FLOW_LENGTH" -> jnum(MAX_FLOW_LENGTH.toLong),
      "MAX_FLOWS_PER_PAIR" -> jnum(MAX_FLOWS_PER_PAIR.toLong),
      "MAX_ENGINE_FLOWS_PER_EVALUATION" -> jnum(MAX_ENGINE_FLOWS_PER_EVALUATION.toLong),
      "MAX_STEPS_PER_SOURCE" -> jnum(MAX_STEPS_PER_SOURCE.toLong),
      "MAX_TOTAL_RETURNS" -> jnum(MAX_TOTAL_RETURNS.toLong),
      "MAX_SOURCE_NODES" -> jnum(MAX_SOURCE_NODES.toLong),
      "MAX_SINK_NODES" -> jnum(MAX_SINK_NODES.toLong),
      "MAX_ENTRY_POINTS" -> jnum(MAX_ENTRY_POINTS.toLong),
      "MAX_CALL_SCAN" -> jnum(MAX_CALL_SCAN.toLong),
      "MAX_TYPE_SCAN" -> jnum(MAX_TYPE_SCAN.toLong),
      "MAX_CODE_CHARS" -> jnum(MAX_CODE_CHARS.toLong))),
    "bounds_meaning" -> jobj(2, List(
      "MAX_FLOW_CALL_DEPTH" -> jstr("the engine's EngineConfig.maxCallDepth for the " +
        "three route-bearing arms: the number of call boundaries the backward search " +
        "may expand while looking for a source"),
      "MAX_FLOW_CALL_DEPTH_SHALLOW" -> jstr("the same engine bound, at the shallower " +
        "value ARM 1 is additionally run at so that depth sensitivity can be measured " +
        "rather than assumed"),
      "MAX_BOUNDARY_FLOW_CALL_DEPTH" -> jstr("the same engine bound, at the value the " +
        "four boundary probes run at; a boundary probe asks about ONE hop, so it needs " +
        "no depth beyond it"),
      "MAX_FLOW_LENGTH" -> jstr("maximum flow elements PUBLISHED per record; a longer " +
        "flow is published truncated and flagged rather than dropped. It governs the " +
        "report, not the traversal: it is applied to a flow the engine has already " +
        "returned"),
      "MAX_FLOWS_PER_PAIR" -> jstr("maximum RETAINED flows per (source group, sink " +
        "group) pair, applied after deduplication to flows already materialized. It " +
        "governs output rather than search, which is why it is not the bound on the " +
        "engine's return - MAX_ENGINE_FLOWS_PER_EVALUATION is"),
      "MAX_ENGINE_FLOWS_PER_EVALUATION" -> jstr("maximum paths MATERIALIZED from one " +
        "reachableByFlows evaluation, taken as cap+1 before the engine's return is " +
        "converted to a list. This is the only cap of the three that bounds the " +
        "materialization itself; the other two bound what is kept and what is printed"),
      "MAX_STEPS_PER_SOURCE" -> jstr("the per-source-group step cap: how many sink " +
        "groups one source group may be evaluated against, reset at each source group"),
      "MAX_TOTAL_RETURNS" -> jstr("the total-returns cap, across every record kind " +
        "this query emits"),
      "MAX_SOURCE_NODES" -> jstr("maximum source nodes taken into an arm; the " +
        "remainder are counted as truncated rather than dropped silently"),
      "MAX_SINK_NODES" -> jstr("maximum sink nodes taken into an arm, counted the same " +
        "way"),
      "MAX_ENTRY_POINTS" -> jstr("maximum source groups traversed per arm, which for " +
        "the entry-point arms is the entry-point cap; the remainder are counted as " +
        "truncated"),
      "MAX_CALL_SCAN" -> jstr("cap on every sweep whose elements are CALL nodes or the " +
        "operand nodes of a call site: the two graph-wide indexed sweeps (the sink name " +
        "sweep and the ARM 2 accessor sweep), the message constructor and accessor " +
        "call-site sweeps, the predicate call-site sweep, and the node-local call and " +
        "operand materializations the boundary probes select their ends from. Every " +
        "sweep it governs is listed with its own observed count and truncation flag " +
        "under bounded_sweeps, and the reached flag for this cap is the disjunction over " +
        "all of them - a cap governing several sweeps cannot report the state of only " +
        "one of them"),
      "MAX_TYPE_SCAN" -> jstr("cap on the keyed type, method and formal-parameter " +
        "lookups - the entry-point, predicate, message, thread, JDK-launch, liveness " +
        "control and route-surface selections - so that no traversal this query " +
        "materializes outside the flow engine runs without a cap"),
      "MAX_CODE_CHARS" -> jstr("maximum characters of a node's code published on a " +
        "flow element; a longer string is published truncated with a marker"))),
    "bounds_reached" -> jobj(2, List(
      "MAX_FLOW_CALL_DEPTH" -> jstr(BOUND_REACHED_UNOBSERVABLE),
      "MAX_FLOW_CALL_DEPTH_SHALLOW" -> jstr(BOUND_REACHED_UNOBSERVABLE),
      "MAX_BOUNDARY_FLOW_CALL_DEPTH" -> jstr(BOUND_REACHED_UNOBSERVABLE),
      "MAX_FLOW_LENGTH" -> jstr(boundReachedLiteral(lengthCapReachedAnywhere)),
      "MAX_FLOWS_PER_PAIR" -> jstr(boundReachedLiteral(perPairCapReachedAnywhere)),
      "MAX_ENGINE_FLOWS_PER_EVALUATION" ->
        jstr(boundReachedLiteral(sweepCapReached("MAX_ENGINE_FLOWS_PER_EVALUATION"))),
      "MAX_STEPS_PER_SOURCE" -> jstr(boundReachedLiteral(stepCapReachedAnywhere)),
      "MAX_TOTAL_RETURNS" -> jstr(boundReachedLiteral(totalReturnsCapReached)),
      "MAX_SOURCE_NODES" -> jstr(boundReachedLiteral(sourceNodeCapReachedAnywhere)),
      "MAX_SINK_NODES" -> jstr(boundReachedLiteral(sinkNodeCapReachedAnywhere)),
      "MAX_ENTRY_POINTS" -> jstr(boundReachedLiteral(
        entryPointsTruncated > 0 || sourceGroupCapReachedAnywhere)),
      "MAX_CALL_SCAN" -> jstr(boundReachedLiteral(sweepCapReached("MAX_CALL_SCAN"))),
      "MAX_TYPE_SCAN" -> jstr(boundReachedLiteral(sweepCapReached("MAX_TYPE_SCAN"))),
      "MAX_CODE_CHARS" -> jstr(boundReachedLiteral(codeCapReachedAnywhere)))),
    "bounds_reached_vocabulary" -> jstrArr(List(
      BOUND_REACHED_YES, BOUND_REACHED_NO, BOUND_REACHED_UNOBSERVABLE)),
    "bounds_reached_basis" -> jobj(2, List(
      "MAX_FLOW_CALL_DEPTH" -> jstr("the engine exposes no truncation flag for its " +
        "internal call-depth bound, so no honest reached/not-reached value exists to " +
        "publish. Depth is addressed instead by running ARM 1 at " +
        MAX_FLOW_CALL_DEPTH_SHALLOW + " and at " + MAX_FLOW_CALL_DEPTH +
        " and comparing, reported under depth_sensitivity"),
      "MAX_FLOW_CALL_DEPTH_SHALLOW" -> jstr("the same limitation, for the same reason"),
      "MAX_BOUNDARY_FLOW_CALL_DEPTH" -> jstr("the same limitation; the boundary probes " +
        "ask about one hop each, so the value is chosen to exceed a single hop rather " +
        "than measured against a frontier"),
      "MAX_FLOW_LENGTH" -> jstr(
        boundReachedLiteral(lengthCapReachedAnywhere) + ": " +
          allFlowRecords.count(_.elementsTruncated) + " of " + allFlowRecords.size +
          " published flow record(s) exceeded " + MAX_FLOW_LENGTH + " elements"),
      "MAX_FLOWS_PER_PAIR" -> jstr(
        boundReachedLiteral(perPairCapReachedAnywhere) + ": " +
          allArms.map(a => a.armId + " retained " + a.flowsRetained + " flow(s) of " +
            a.flowsFound + " found, cap " + MAX_FLOWS_PER_PAIR + " per pair")
            .mkString("; ")),
      "MAX_ENGINE_FLOWS_PER_EVALUATION" -> jstr(
        boundReachedLiteral(sweepCapReached("MAX_ENGINE_FLOWS_PER_EVALUATION")) + ": " +
          sweepBasisFor("MAX_ENGINE_FLOWS_PER_EVALUATION")),
      "MAX_STEPS_PER_SOURCE" -> jstr(
        boundReachedLiteral(stepCapReachedAnywhere) + ": " +
          allArms.map(a => a.armId + " made " + a.evaluations + " evaluation(s) over " +
            a.sourceGroupsTraversed + " source group(s), cap " + MAX_STEPS_PER_SOURCE +
            " sink groups per source group").mkString("; ")),
      "MAX_TOTAL_RETURNS" -> jstr(
        boundReachedLiteral(totalReturnsCapReached) + ": " + returnedRecordCount +
          " records of " + MAX_TOTAL_RETURNS),
      "MAX_SOURCE_NODES" -> jstr(
        boundReachedLiteral(sourceNodeCapReachedAnywhere) + ": " +
          allArms.map(a => a.armId + " took " + a.sourceNodes + " source node(s), " +
            a.sourceNodesTruncated + " truncated").mkString("; ")),
      "MAX_SINK_NODES" -> jstr(
        boundReachedLiteral(sinkNodeCapReachedAnywhere) + ": " + sinkNodesTruncated +
          " sink node(s) truncated at selection, cap " + MAX_SINK_NODES + "; " +
          allArms.map(a => a.armId + " took " + a.sinkNodes + " sink node(s), " +
            a.sinkNodesTruncated + " truncated").mkString("; ")),
      "MAX_ENTRY_POINTS" -> jstr(
        boundReachedLiteral(entryPointsTruncated > 0 || sourceGroupCapReachedAnywhere) +
          ": " + entryPointsDiscovered + " entry point(s) discovered, " +
          entryPointsTraversed + " traversed, " + entryPointsTruncated +
          " truncated, cap " + MAX_ENTRY_POINTS + "; " +
          allArms.map(a => a.armId + " traversed " + a.sourceGroupsTraversed + " of " +
            a.sourceGroupsDiscovered + " source group(s)").mkString("; ")),
      "MAX_CALL_SCAN" -> jstr(
        boundReachedLiteral(sweepCapReached("MAX_CALL_SCAN")) + ": " +
          sweepBasisFor("MAX_CALL_SCAN")),
      "MAX_TYPE_SCAN" -> jstr(
        boundReachedLiteral(sweepCapReached("MAX_TYPE_SCAN")) + ": " +
          sweepBasisFor("MAX_TYPE_SCAN")),
      "MAX_CODE_CHARS" -> jstr(
        boundReachedLiteral(codeCapReachedAnywhere) + ": measured by looking for the " +
          "truncation marker on every code string this run published, over " +
          allFlowRecords.map(_.elements.size).sum + " flow element(s)"))),
    "bounded_sweeps" -> jrawArr(4, boundedSweeps.toList.map(sw => jobj(6, List(
      "sweep" -> jstr(sw.label),
      "cap_name" -> jstr(sw.capName),
      "cap" -> jnum(sw.cap.toLong),
      "observed" -> jnum(sw.observed.toLong),
      "truncated" -> jbool(sw.truncated),
      "basis" -> jstr(sw.basis))))),
    "bounded_sweeps_completeness" -> jstr("every traversal this query materializes " +
      "OUTSIDE THE FLOW ENGINE is listed above, and each one goes through a single " +
      "bounded-materialization helper that takes cap+1 elements and reports truncation " +
      "when it saw more than cap - so a cap applied at one site and forgotten at the " +
      "next is not expressible, and a sweep absent from that list did not run. TWO " +
      "MECHANISMS, NAMED SEPARATELY, because one helper does not cover both. The helper " +
      "covers the selection: the graph-wide indexed call sweeps, the call-site sweeps of " +
      "the message constructor, the message accessors and the five predicates, the " +
      "keyed type, method and formal-parameter lookups, and the node-local call and " +
      "operand materializations the boundary probes take their ends from - each " +
      "governed by MAX_CALL_SCAN or MAX_TYPE_SCAN and published above with its own " +
      "reached flag and per-sweep basis. The helper ALSO covers the flow engine's " +
      "RETURN: the iterator reachableByFlows yields is materialized through the same " +
      "cap+1 helper under MAX_ENGINE_FLOWS_PER_EVALUATION, one registered sweep per " +
      "evaluation, so the number of paths this query materializes from any single " +
      "engine evaluation is bounded and its truncation is reported like every other " +
      "sweep's. What the helper does NOT and CANNOT cover is the engine's own backward " +
      "search BEFORE it yields an element: that work happens inside reachableByFlows, " +
      "the API exposes no counter for it and no cap over it, so it is neither bounded " +
      "nor counted here and this record does not claim otherwise. It is influenced only " +
      "indirectly, by the EngineConfig.maxCallDepth this query overrides on the " +
      "console's context (MAX_FLOW_CALL_DEPTH, MAX_FLOW_CALL_DEPTH_SHALLOW and " +
      "MAX_BOUNDARY_FLOW_CALL_DEPTH), by MAX_STEPS_PER_SOURCE on how many sink groups " +
      "one source group is evaluated against, by MAX_SOURCE_NODES and MAX_SINK_NODES on " +
      "the node sets handed to it, and by MAX_ENTRY_POINTS on the source groups " +
      "traversed. MAX_FLOWS_PER_PAIR and MAX_FLOW_LENGTH govern NEITHER the search nor " +
      "the materialization: the first bounds how many already-materialized flows a pair " +
      "RETAINS, the second how many elements of a retained flow are REPORTED. Two " +
      "further reads are stated " +
      "rather than counted as sweeps, because neither selects a set from the graph: the " +
      "per-node accessors this query calls inside filters and record builders (a " +
      "method's declaring type names, a node's own code and line) are node-local and " +
      "bounded by the already-bounded list they are called over; and the three graph " +
      "totals published under graph.methods, graph.type_declarations and graph.files " +
      "are counts taken by the engine over its own indexes, traversed without being " +
      "retained. A reader checking the boundedness claim needs to know which mechanism " +
      "governs what, so the division is published rather than collapsed into one " +
      "sentence about a single helper"),
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
    "entry_points_truncated_meaning" -> jstr("the two counters exist so that a sweep " +
      "cannot run unbounded and so that a trimmed traversal cannot pass for a complete " +
      "one. A truncated count above zero is a measured property of the traversal, to " +
      "be reported rather than hidden; here " + entryPointsDiscovered + " entry " +
      "point(s) were discovered against a cap of " + MAX_ENTRY_POINTS + ", of which " +
      entryPointsTraversed + " were traversed and " + entryPointsTruncated +
      " truncated"),
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
        "source_node_selection" -> jstr(ROUTE_SURFACE_SOURCE_NODE_SELECTION),
        "unapply" -> jstr(ROUTE_SURFACE_UNAPPLY),
        "additional_dataflow_obstacle" ->
          jstr(ROUTE_SURFACE_ADDITIONAL_DATAFLOW_OBSTACLE),
        "non_connectivity_hops_for_a_data_flow" ->
          jstrArr(ROUTE_SURFACE_NON_CONNECTIVITY_HOPS_FOR_A_DATA_FLOW),
        "zero_route_meaning" -> jstr("a zero route count would mean that records " +
          "carries no record of kind route: that the pair is not joined by a " +
          "reaching-definition path across the hops above, so a bounded dataflow " +
          "traversal over data edges returns none. It would be the capability finding " +
          "this query exists to report - what this formulation can and cannot express " +
          "over this graph - and it would be reported as measured, with the bound " +
          "neither loosened nor removed, the query not widened, no arm added to obtain " +
          "a route and no flow manufactured. It would not be a statement about Spark, " +
          "about any Spark component or about any configuration"),
        "route_outcome" -> jstr(routeOutcomeStatement))),
    "route_record_schema" -> jstrArr(List(
      "records holds two shapes, and kind names which one a record is",
      "a BOUNDARY record carries boundary_id (one of B1-rpc, B2-thread, B3-interface, " +
        "B4-partial-function), hop, from_end and to_end, what was measured, how the " +
        "hop is modelled, crossed_by_a_data_flow with flows_found, and the reason",
      "a FLOW record - kind boundary-flow or liveness-control-flow - carries arm_id, " +
        "source_group and sink_group, element_count, elements_truncated, signature, " +
        "passed_auth_or_acl_predicate with spurious, and elements each carrying index, " +
        "label, enclosing_method, callee, graph_line and code",
      "a flow record is checkable end to end from its own fields: the signature is the " +
        "identity the deduplication was taken on, and the elements are what the " +
        "signature was computed over",
      "no record carries a file path; a record locates itself by method full name and " +
        "by the graph's own line number, and every source path in this envelope is " +
        "expressed relative to the SPARK_SRC root",
      "graph_line is the GRAPH's line number, read from the bytecode line-number " +
        "table, and can differ by a line from the def or statement line cited from the " +
        "pinned source")),
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
      "resolved_target_path_literal_lives_in" -> jstr(LOG_DIR + "/probe-" + QUERY_ID +
        ".log, which is where this run's host-specific paths are recorded; they are " +
        "deliberately not carried into this envelope"),
      "measurement_semantics" -> jstr("symlink-FOLLOWING. Where the named path is a " +
        "symlink, measuring the link itself records a few dozen bytes rather than the " +
        "graph: byte_size_without_following is recorded only to be discarded. The size " +
        "below is the resolved target's; the sha256 below is taken from the bytes as " +
        "they were copied into the private input this run imported, and the copy's size " +
        "is asserted equal to the target's, so the pair describes one set of bytes " +
        "rather than two measurements of two files"),
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
      "identity_record_role" -> jstr("the declared owner of this pair, which computed " +
        "it at write time with the same symlink-following semantics; this envelope " +
        "cites that measurement rather than establishing a second one"),
      "identity_recorded_byte_size" -> jnum(recordedSize),
      "identity_recorded_sha256" -> jstr(recordedSha),
      "identity_comparison_result" -> jstr("match - the observed byte size and sha256 " +
        "equal the pair the identity record owns, on both values; a mismatch halts the " +
        "run in the identity stage and publishes no envelope"),
      "identity_reverified_before_load" -> jbool(true),
      "loaded_bytes_are_a_private_copy" -> jbool(true),
      "private_copy_protocol" -> jstr("the digest above is taken FROM THE BYTES BEING " +
        "COPIED, in one pass, into a directory this run created under the clone-private " +
        "scratch root with a random name and mode 0700; the copy is then set read-only " +
        "in a non-writable directory, and importCpg is given that copy and nothing else. " +
        "Measuring the host-shared path and then importing it would be two opens of one " +
        "name, so the pair compared would not be the pair the engine read"),
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
      "jdk_major_required" -> jstr(REQUIRED_JDK_MAJOR),
      "jvm_version" -> jstr(jvmVersion),
      "command" -> jstr(REPRODUCTION_COMMAND),
      "command_precondition" -> jstr(REPRODUCTION_COMMAND_PRECONDITION),
      "command_working_directory" -> jstr("$" + SCRATCH_ROOT_ENV_VAR + ", outside the " +
        "repository, because joern creates ./workspace in its own working directory and " +
        "exposes no flag to move it"),
      "command_graph_selector" -> jstr("$" + CPG_ENV_VAR + ", named explicitly in the " +
        "command because it selects the graph bytes the query loads"),
      "command_completeness" -> jstr("the command above is COMPLETE and runnable as " +
        "written: the working directory, the repository root, the graph selector, the " +
        "JDK, the heap override, the log level, the script path and the closed stdin. " +
        "Every environment value this query reads appears in it - $" +
        REPO_ROOT_ENV_VAR + ", $" + CPG_ENV_VAR + " and $" + SCRATCH_ROOT_ENV_VAR + " - " +
        "and it reads no other. The three are written as variable references rather " +
        "than as literal paths because an absolute path is a property of a checkout " +
        "rather than of the measurement, and this envelope is held to byte-identity " +
        "across checkouts; sourcing harness/env.sh exports all three, which is what the " +
        "precondition above states. There is still no variable that selects the identity " +
        "record: it is resolved by provenance from records reached through the values " +
        "named here, so a reader running this command reproduces the run this envelope " +
        "describes rather than a differently adjudicated one"),
      "jvm_arguments_kept" -> jstrArr(jvmArgsKept),
      "jvm_arguments_redacted_count" -> jnum(jvmArgsRedacted.size.toLong),
      "jvm_arguments_redaction_policy" -> jstr(JVM_ARG_REDACTION_POLICY),
      "heap_actually_used_bytes" -> jnum(heapMaxBytes),
      "heap_floor_bytes" -> jnum(HEAP_FLOOR_BYTES),
      "heap_at_or_above_floor" -> jbool(heapMaxBytes >= HEAP_FLOOR_BYTES),
      "heap_above_floor" -> jbool(heapMaxBytes > HEAP_FLOOR_BYTES),
      "heap_pre_touch_proof" -> jstr("the floor value's own commit proof is the gate's " +
        "java -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version, which exited 0. A heap above " +
        "the floor is permitted and reported, and is proven committable by that same " +
        "pre-touch test before use; a heap below it is not, which is why this query " +
        "measures Runtime.maxMemory() and halts rather than trusting the flag it was " +
        "given"),
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
        "route is mandated because the alternative loader spawns a second JVM at the " +
        "same heap"),
      "loader_is_importcpg_only" -> jbool(true),
      "loader_alternative_absent_from_the_source" ->
        jbool(alternativeLoaderOccurrences == 0),
      "dataflow_layer" -> jstr("applied by importCpg as one of the console's default " +
        "overlays; its liveness on this sink is not assumed but measured by the " +
        "engine-liveness control arm"),
      "flow_engine_semantics" -> jstr(semanticsClass),
      "flow_engine_context_source" -> jstr("the console's own EngineContext, copied with " +
        "only the call-depth bound overridden, and passed EXPLICITLY at every call site " +
        "so no implicit resolution decides which context a traversal ran under"),
      "workspace" -> jstr(WORKSPACE_PATH),
      "workspace_run_directory" -> jstr(workspaceRunRepoRelative),
      "workspace_safety" -> jstr("the AAP-named root is reached by a descriptor descent " +
        "from the resolved repository root that refuses a symbolic link at any " +
        "component; the directory actually switched to is created by this run with " +
        "createDirectory under a random name, so an existing name fails rather than " +
        "being reused and no previous run's project state is inherited; an exclusive " +
        "file lock is held inside it for the rest of the run, because two Joern " +
        "processes in one clone are what corrupts a workspace; and it is its verified " +
        "real path that is handed to switchWorkspace. It is removed by a shutdown hook " +
        "confined to that directory, since the engine holds project state in it until " +
        "the JVM exits"),
      "workspace_lock_held" -> jbool(true),
      "heap_bound_jvm_position" -> jstr("one of 4 (frontend build, importCpg " +
        "verification load, Stage 3 Joern runner, this probe)"))),
    "predicate_selector" -> jobj(2, List(
      "type" -> jstr(PREDICATE_TYPE),
      "name_regex" -> jstr(PREDICATE_NAME_REGEX),
      "setter_suffix_excluded" -> jstr(PREDICATE_SETTER_SUFFIX),
      "named_five" -> jstrArr(PREDICATE_NAMED_FIVE.sorted),
      "predicate_selector_literals_compared_against" -> jstr(SIBLING_CALLGRAPH_QUERY),
      "predicate_selector_literals_identical" ->
        (if (callgraphRelationEstablished)
           jbool(callgraphRelation.exists(_.samePredicateSelectors))
         else jstr("not established")),
      "predicate_selector_literals_comparison" -> jstr(
        if (callgraphRelationEstablished)
          "MEASURED in the duplicate-formulation stage: the two sources' declared " +
            "predicate selector literals were read from the two source files and " +
            "compared as literal text. The per-sibling entry under " +
            "duplicate_formulation_detail carries both lists"
        else "the sibling source could not be read, so no comparison was made and none " +
          "is asserted: " + callgraphRelation.map(_.theirs.note).getOrElse(
            "no relation was computed against " + SIBLING_CALLGRAPH_QUERY)),
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
      "route_surface_type_prefixes" -> jstrArr(ROUTE_SURFACE_TYPE_PREFIXES),
      "route_surface_derivation" -> jstr("DERIVED from THIS query's own route rather " +
        "than declared: the entry-point owner, the relay that is the consumer end of " +
        "the RPC hop measured as boundary B1, and the two sink hosts the launch call " +
        "sites actually sit on. Query 03's second pair enters at a REST server type " +
        "that is not on this route at all, so carrying it here would search a surface " +
        "this route does not cross while omitting a host it does. Stage J asserts that " +
        "every route end this query measured is covered by one of these prefixes, and " +
        "publishes the reach evidence below, so the surface is checked against the " +
        "route rather than asserted alongside it"),
      "route_surface_reach_evidence" -> jrawArr(4, routeSurfaceReach),
      "route_surface_covers_every_measured_end" -> jbool(true))),
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
      "data_edge_verdict_measured_here" ->
        jstr(if (boundaryB3.crossedByADataFlow) "crossed" else "not crossed"),
      "data_flows_found_here" -> jnum(boundaryB3.flowsFound.toLong),
      "call_edge_verdict_owner" -> jstr(SIBLING_CALLGRAPH_QUERY),
      "call_edge_verdict_here" -> jstr("not measured by this query and deliberately " +
        "not transcribed from the sibling's envelope: the same hop under the other " +
        "question is that query's own measurement, and copying it here would create a " +
        "second copy of a number that can drift from its owner"),
      "note" -> jstr("one hop, two different questions. Each query publishes the " +
        "verdict it measured and no other, so the two are read side by side rather " +
        "than merged into a single verdict"))),
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
    "effort_query_revisions_measured_at_head" ->
      (revisionHead.map(jstr).getOrElse("null")),
    "effort_query_revisions_measured_on_branch" ->
      (revisionBranch.map(jstr).getOrElse("null")),
    "effort_query_revisions_ancestry_verified" -> jbool(revisionsEstablished),
    "effort_joern_api_constructs" -> jstrArr(apiConstructsHere),
    "effort_joern_api_construct_count" -> jnum(apiConstructsHere.size.toLong),
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
    "effort_joern_api_constructs_not_used_by_01" ->
      jstrArr(apiConstructsOnlyHereVsCallgraph),
    "effort_joern_api_constructs_not_used_by_01_basis" -> jstr(
      if (callgraphRelationEstablished)
        "MEASURED: the set difference between this query's declared construct list and " +
          SIBLING_CALLGRAPH_QUERY + "'s, both read from the two SOURCES at run time in " +
          "the duplicate-formulation stage. The difference in the other direction is " +
          "published in that stage's per-sibling entry"
      else "not established: the sibling source could not be read, so the difference " +
        "is published empty rather than guessed - " +
        callgraphRelation.map(_.theirs.note).getOrElse(
          "no relation was computed against " + SIBLING_CALLGRAPH_QUERY)),
    "effort_parameterizability" -> jstr("not claimed here; proven by " +
      PARAMETERIZABILITY_OWNER + " invoking its parameterized form on the second named " +
      "handler/sink pair and capturing that invocation's result"),
    "total_returns_cap_reached" -> jbool(totalReturnsCapReached),
    "records_order" -> jstr("an explicit TOTAL sort key, applied per record kind in a " +
      "fixed kind order. Boundary records come first, in the fixed boundary order " +
      boundaries.map(_.id).mkString(", ") + ". Then the route records, ordered by " +
      "(source_group, sink_group, element_count, signature, arm_id). Then the " +
      "boundary-flow records, ordered by (arm_id, source_group, sink_group, " +
      "element_count, signature). Then the liveness-control-flow records, ordered by " +
      "(source_group, sink_group, element_count, signature) within the one arm that " +
      "produces them. Each key is the COMPLETE published identity of the record it " +
      "orders, so no two records can share a whole key and the order is TOTAL rather " +
      "than merely stable. arm_id is the last component of the route key rather than " +
      "the first because a route's identity is (source group, sink group, signature) " +
      "and the arm is the provenance of the surviving copy: which arm's copy survives " +
      "deduplication is decided by the arm order declared in the source, which is not " +
      "data-dependent. The whole set is then truncated to MAX_TOTAL_RETURNS"),
    "collection_order" -> jstr("every list this envelope publishes is ordered " +
      "explicitly rather than left in traversal order. Source-construct lists ascend " +
      "by their position on the route surface, which is the order the route surface " +
      "block itself declares (route_surface, launch_driver_calls, " +
      "non_connectivity_hops_for_a_data_flow). Graph and query identifier lists ascend " +
      "lexicographically (entry_points, sink_hosts, sink_call_sites, named_five, " +
      "step1_broad_matches, step2_setters_excluded, step2_remaining, " +
      "step3_non_predicate_residue_dropped, final_names, final_full_names, " +
      "route_surface_type_prefixes, never_summed_with, effort_joern_api_constructs, " +
      "effort_joern_api_constructs_not_used_by_01, api_constructs_only_here, " +
      "api_constructs_only_there, non_deterministic_quantities_excluded). Boundary " +
      "lists follow the fixed boundary order. Bounds objects follow the order the " +
      "bounds are declared in the source. Arms follow the order the arms are run in, " +
      "which is declared in the source and not data-dependent. Per-sibling duplicate " +
      "entries follow the sibling order declared in the source. Effort commit lists " +
      "are newest first, as git reports them, which is a stated order rather than an " +
      "incidental one"))

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
    "graph_identity_owner" -> jstr(recordOfAccountLabel),
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
      "marker file. The converse does NOT hold and is not claimed: equal publication " +
      "identifiers mean equal query, source and graph, which two separate invocations " +
      "share, so sameness of generation is settled by the completion manifest's " +
      "member_set_id over MEMBER BYTES and not by this identifier")))

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
  md0(s"| Query source | `$sourceRepoRelative` |")
  md0(s"| Query source sha256 | `$sourceSha256` ($sourceByteSize bytes) |")
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
    "load |")
  md0(s"| Graph methods / typeDecls / files | $methodCount / $typeDeclCount / $fileCount |")
  md0(s"| Flow engine semantics | `${mdSafe(semanticsClass)}` |")
  md0(s"| Compile status | compiled |")
  md0(s"| Run status | completed |")
  md0(s"| Records returned | $returnedRecordCount (${boundaries.size} boundary, " +
    s"${distinctRoutes.size} route, ${boundaryFlows.size} boundary-flow, " +
    s"${controlArm.flows.size} control) |")
  md0(s"| Distinct routes | ${distinctRoutes.size} |")
  md0(s"| Spurious routes | $spuriousCount |")
  md0(s"| Dataflow layer live on this sink | $engineLive |")
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
  md0(s"| MAX_ENGINE_FLOWS_PER_EVALUATION | $MAX_ENGINE_FLOWS_PER_EVALUATION |")
  md0(s"| MAX_FLOW_LENGTH | $MAX_FLOW_LENGTH |")
  md0(s"| MAX_FLOWS_PER_PAIR | $MAX_FLOWS_PER_PAIR |")
  md0(s"| MAX_STEPS_PER_SOURCE | $MAX_STEPS_PER_SOURCE |")
  md0(s"| MAX_TOTAL_RETURNS | $MAX_TOTAL_RETURNS |")
  md0(s"| MAX_SOURCE_NODES | $MAX_SOURCE_NODES |")
  md0(s"| MAX_SINK_NODES | $MAX_SINK_NODES |")
  md0(s"| MAX_ENTRY_POINTS | $MAX_ENTRY_POINTS |")
  md0(s"| MAX_CALL_SCAN | $MAX_CALL_SCAN |")
  md0(s"| MAX_TYPE_SCAN | $MAX_TYPE_SCAN |")
  md0(s"| MAX_CODE_CHARS | $MAX_CODE_CHARS |")
  md0("")
  md0("Every bound above is published with its reached flag and its basis in the")
  md0("envelope's `bounds_reached` and `bounds_reached_basis`. For the two sweep caps")
  md0("the reached flag is the **disjunction over every sweep that cap governs**, and")
  md0("the basis names each governed sweep with its own observed count and flag, so a")
  md0("cap that governs several sweeps cannot report the state of only one of them.")
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
  md0("Every materialization **outside the flow engine** goes through one bounded helper")
  md0("that takes `cap + 1` elements and reports truncation when it saw more than `cap`,")
  md0("so a cap applied at one site and forgotten at the next is not expressible: a")
  md0("sweep absent from this table did not run.")
  md0("")
  md0("The flow engine's **return** goes through that same helper. The iterator")
  md0("`reachableByFlows` yields is materialized under")
  md0("`MAX_ENGINE_FLOWS_PER_EVALUATION`, taken as `cap + 1` before the iterator becomes")
  md0("a list, registered as one sweep per evaluation and published above with its own")
  md0("reached flag and per-evaluation basis. So the paths this query materializes from")
  md0("any one engine evaluation are bounded on the same terms as every other sweep.")
  md0("")
  md0("The engine's own backward search **before** it yields an element is the one thing")
  md0("neither mechanism reaches, and that is stated rather than papered over. It runs")
  md0("inside `reachableByFlows`, the API exposes no counter for it and no cap over it,")
  md0("so this query neither bounds nor counts it and claims nothing about its cost. It")
  md0("is influenced only indirectly: by the `EngineConfig.maxCallDepth` this query")
  md0("overrides on the console's own context (`MAX_FLOW_CALL_DEPTH`,")
  md0("`MAX_FLOW_CALL_DEPTH_SHALLOW`, `MAX_BOUNDARY_FLOW_CALL_DEPTH`), by")
  md0("`MAX_STEPS_PER_SOURCE` on how many sink groups one source group is evaluated")
  md0("against, by `MAX_SOURCE_NODES` and `MAX_SINK_NODES` on the node sets handed to")
  md0("it, and by `MAX_ENTRY_POINTS` on the source groups traversed. Each of those is")
  md0("published above with its own reached flag and basis.")
  md0("")
  md0("`MAX_FLOWS_PER_PAIR` and `MAX_FLOW_LENGTH` govern **neither** the search nor the")
  md0("materialization: the first bounds how many already-materialized flows a pair")
  md0("**retains**, the second how many elements of a retained flow are **reported**.")
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
      md0(s"- `${mdSafe(p.method.fullName)}` index ${p.index}, name `${mdSafe(p.name)}`, type " +
        s"`${mdSafe(p.typeFullName)}`")
    }
    md0("")
  }
  md0("## The sink")
  md0("")
  sinkCalls.foreach { c =>
    md0(s"- `${mdSafe(c.method.fullName)}` calls `${mdSafe(c.methodFullName)}` at graph line " +
      s"${lineOf(c)} (dispatch `${mdSafe(c.dispatchType)}`)")
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
    md0(s"- **from**: ${if (b.fromEnd.isEmpty) "(none measured)" else "`" + mdSafe(b.fromEnd) + "`"}")
    md0(s"- **to**: ${if (b.toEnd.isEmpty) "(none measured)" else "`" + mdSafe(b.toEnd) + "`"}")
    md0(s"- **reason**: ${b.reason}")
    md0(s"- **modelling**: ${b.modelling}")
    md0("")
  }
  md0("Boundaries not crossed by a data flow: " +
    (if (boundariesNotCrossed.isEmpty) "none"
     else boundariesNotCrossed.map(b => s"`${b.id}`").mkString(", ")) + ".")
  md0("")
  md0("`B3-interface` deserves one explicit note, because it is the hop on which the two")
  md0("formulations could most easily have parted company. This query measures that hop")
  md0(s"for a **data edge** and reports **${boundaryB3.crossedByADataFlow}**")
  md0(s"(${boundaryB3.flowsFound} flow(s) found). The same hop under the call-edge")
  md0(s"question belongs to `$SIBLING_CALLGRAPH_QUERY` and is **deliberately not")
  md0("transcribed here**: that verdict is that query's own measurement, and a second")
  md0("copy of a number is a number that can drift from its owner. Each query publishes")
  md0("the verdict it measured, and a reader comparing the two reads them side by side")
  md0("rather than merged into one verdict.")
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
  md0(mdSafe(duplicateFormulationAggregation) + ".")
  md0("")
  relations.foreach { r =>
    val siblingDigest =
      if (r.theirs.established) s"source sha256 `${r.theirs.sourceSha256}`"
      else "source not read"
    md0(s"### Against `${mdSafe(r.theirs.queryId)}` ($siblingDigest): ${mdSafe(r.status)}")
    md0("")
    md0(s"- **Scope.** ${r.scope}.")
    md0(s"- **Basis.** ${r.basis}.")
    md0(s"- **Edge kinds.** here ${r.theirs.edgeKinds.size} vs " +
      s"${myFormulation.edgeKinds.mkString(", ")} - same kinds: ${r.sameEdgeKinds}. " +
      s"A call path does not show that a value arrives, and a data path does not show " +
      s"that control can, so neither traversal establishes the other's conclusion " +
      s"where these differ.")
    md0(s"- **Route-end node kinds.** here ${mdSafe(myFormulation.endNodeKinds.mkString(", "))}" +
      s"; there ${mdSafe(r.theirs.endNodeKinds.mkString(", "))} - same kinds: " +
      s"${r.sameEndNodeKinds}.")
    md0(s"- **API construct sets.** ${r.apiOnlyHere.size} of this query's " +
      s"${apiConstructsHere.size} constructs do not appear in that query's declared " +
      s"list, and ${r.apiOnlyThere.size} of that query's do not appear here; " +
      s"${r.apiShared.size} are shared. The difference is computed from the two " +
      s"sources' own lists rather than eyeballed.")
    md0(s"- **Predicate selector literals identical.** ${r.samePredicateSelectors}.")
    md0("")
    if (r.apiOnlyHere.nonEmpty || r.apiOnlyThere.nonEmpty) {
      r.apiOnlyHere.foreach(c => md0(s"  - only here: `$c`"))
      r.apiOnlyThere.foreach(c => md0(s"  - only in ${mdSafe(r.theirs.queryId)}: `${mdSafe(c)}`"))
      md0("")
    }
  }
  md0("**What each formulation can return, measured in this run.** " +
    sentence(divergenceEvidence) + ".")
  md0("")
  md0(duplicateFormulationRelation + ".")
  md0("")
  md0("Their results are reported side by side and are **never summed**.")
  md0("")
  md0("## The three effort measures")
  md0("")
  md0(s"1. **Query revisions committed: " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established") +
    ".** Convention: " + QUERY_REVISIONS_CONVENTION + ".")
  md0(s"   Measurement: ${mdSafe(revisionsNote)}.")
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
  md0(s"2. **Distinct Joern API constructs used: ${apiConstructsHere.size}.** Listed")
  md0("   explicitly and deduplicated so the count is auditable from the list rather")
  md0("   than asserted. Each entry was searched for in this query's own source text")
  md0("   with the list's own declaration excised first, so no entry can satisfy")
  md0(s"   itself: ${apiConstructsConfirmed.size} of ${apiConstructsHere.size} were")
  md0("   confirmed" +
    (if (apiConstructsNotConfirmed.isEmpty) "."
     else ", and the entries the search could not confirm are listed in the envelope " +
       "rather than dropped from the count."))
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
  md0(s"  the dataflow overlay was built with (`${mdSafe(semanticsClass)}`), and the context is")
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
  md0(s"Precondition: $REPRODUCTION_COMMAND_PRECONDITION.")
  md0("")
  val reproductionFence = mdFence(REPRODUCTION_COMMAND)
  md0(reproductionFence)
  md0(REPRODUCTION_COMMAND)
  md0(reproductionFence)
  md0("")
  md0("That is the **whole** command and it is runnable as written: the working")
  md0("directory, the repository root, **the graph selector**, the JDK, the heap")
  md0("override, the log level, the script path and the closed stdin. Every environment")
  md0(s"value this query reads appears in it - `$$$REPO_ROOT_ENV_VAR`, " +
    s"`$$$CPG_ENV_VAR` and")
  md0(s"`$$$SCRATCH_ROOT_ENV_VAR` - and it reads no other. They are written as variable")
  md0("references rather than as literal paths because an absolute path is a property of")
  md0("a checkout rather than of the measurement, and this report is held to")
  md0("byte-identity across checkouts; sourcing `harness/env.sh` exports all three.")
  md0("")
  md0(s"`$$$CPG_ENV_VAR` is named explicitly because it selects the graph bytes the")
  md0("query loads: a reader with it pointing at another graph reproduces a different")
  md0("load, and a command that omitted it would leave its most consequential input")
  md0("invisible. There is still no variable that selects the identity record. The")
  md0("record of account is resolved by provenance - the in-checkout frontend log where")
  md0("it carries a write-time `bytes:`/`sha256:` pair, and otherwise the provisioning")
  md0("record beside the resolved graph - and both are reached through values this")
  md0(s"command names. For this run it was `$recordOfAccountLabel`.")
  md0("")
  md0("`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so")
  md0("`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs")
  md0("at; where a runner defaults below the floor it is raised through its own documented")
  md0("environment override. The query measures the heap it received and stops below the")
  md0("floor: raising a heap is permitted and reported, lowering one is not.")
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
    s"(the console log is staged last, because it names the other two)")

  // -------------------------------------------------------------------------
  stage("O-result: the result region, emitted only now that every stage passed")
  // -------------------------------------------------------------------------
  log(s"total elapsed_ms          : ${elapsedMs(runStartNanos)}")
  logMarker(MARKER_RESULT_BEGIN)
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
  log(s"duplicate_formulation     : $duplicateFormulationAggregate")
  relations.foreach(r =>
    log(s"  vs ${r.theirs.queryId}: ${r.status} (scope: ${r.scope})"))
  log(s"query_revisions_committed : " +
    (if (revisionsEstablished) revisionCommits.size.toString else "not established"))
  log(s"joern_api_constructs      : ${apiConstructsHere.size} " +
    s"(${apiConstructsOnlyHereVsCallgraph.size} not declared by " +
    s"$SIBLING_CALLGRAPH_QUERY)")
  log(s"boundaries_not_crossed    : ${boundariesNotCrossed.map(_.id).mkString(", ")}")
  log(s"query_source_sha256       : $sourceSha256")
  log(s"publication_id            : $publicationId")
  log(s"envelope                  : $jsonPath")
  log(s"prose report              : $mdPath")
  logMarker(MARKER_RESULT_END)
  logMarker(MARKER_OK)

  // The publication, all three members at once. The two staged members are on
  // disk and fsynced; the console log is staged now, as the last member,
  // because its content names the other two - and only then is anything moved
  // onto a published path. A failure anywhere above this line leaves all three
  // targets holding their previous generation rather than a mixed one.
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
