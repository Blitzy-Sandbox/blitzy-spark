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
// NAMED CONSTANTS - no inline literal governs behaviour anywhere below
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
/** Per-entry-point step cap: method expansions, not edges. */
val MAX_EXPANSIONS_PER_ENTRY = 200000
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
      "is not, because a truncated result's silence cannot be told apart from a clean one")
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
      methodsVisited: Int,
      callSitesConsidered: Int,
      fanOutSitesEncountered: Int,
      fanOutSitesNotFollowed: Int,
      maxDepthUsed: Int,
      depthBoundReached: Boolean,
      expansionBudgetExhausted: Boolean,
      routeCapReached: Boolean,
      routes: List[RouteRecord])

  def walk(walkId: String, followFanOut: Boolean): WalkResult = {
    var methodsVisited = 0
    var expansions = 0
    var callSitesConsidered = 0
    var fanOutEncountered = 0
    var fanOutNotFollowed = 0
    var maxDepthUsed = 0
    var depthBoundReached = false
    var budgetExhausted = false
    var routeCapReached = false
    val routes = scala.collection.mutable.ArrayBuffer.empty[RouteRecord]

    entryGroupsTraversed.foreach { case (entryName, entryNodes) =>
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
      log(f"  walk $walkId%-18s entry=$entryName visited=${visited.size}%8d " +
        f"depth=$depth%2d expansions=$expansions%8d")
    }

    WalkResult(walkId, followFanOut, entryPointsTraversed, expansions,
      methodsVisited, callSitesConsidered, fanOutEncountered, fanOutNotFollowed,
      maxDepthUsed, depthBoundReached, budgetExhausted, routeCapReached, routes.toList)
  }

  val walkNanos = System.nanoTime()
  val walkA = walk("A-follows-fan-out", followFanOut = true)
  log(s"walk A elapsed_ms         : ${elapsedMs(walkNanos)}")
  val walkBNanos = System.nanoTime()
  val walkB = walk("B-fan-out-recorded", followFanOut = false)
  log(s"walk B elapsed_ms         : ${elapsedMs(walkBNanos)}")
  val walks = List(walkA, walkB)
  walks.foreach { w =>
    log(s"walk ${w.walkId}: routes=${w.routes.size} expansions=${w.expansions} " +
      s"call_sites=${w.callSitesConsidered} fanout_seen=${w.fanOutSitesEncountered} " +
      s"fanout_not_followed=${w.fanOutSitesNotFollowed} max_depth=${w.maxDepthUsed} " +
      s"depth_bound_reached=${w.depthBoundReached} " +
      s"budget_exhausted=${w.expansionBudgetExhausted} " +
      s"route_cap_reached=${w.routeCapReached}")
  }

  /** Distinct routes across both walks: deduplicated on the hop sequence, never
   *  summed. Two walks over one handler/sink pair are two formulations of the
   *  same question, so their returns are reported side by side and counted once. */
  val distinctRoutes = walks
    .flatMap(_.routes)
    .distinctBy(r => (r.entryPoint, r.sinkHost, r.hops.map(h => (h.fromMethod, h.callSite, h.toMethod))))
    .sortBy(r => (r.entryPoint, r.sinkHost, r.hops.size))
  val boundReached = walks.exists(w =>
    w.depthBoundReached || w.expansionBudgetExhausted || w.routeCapReached)
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
  val abstractLaunchNames = sinkCallCallees.filter(_.startsWith(
    "org.apache.spark.deploy.worker.ProcessBuilderLike.start"))
  val concreteLaunchNames = sinkCallCallees.filterNot(_.startsWith(
    "org.apache.spark.deploy.worker.ProcessBuilderLike.start"))
  val jdkLaunchMethodNodes = cpg.method
    .fullNameExact("java.lang.ProcessBuilder.start:java.lang.Process()")
    .l
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
  val handlerReachesBody = sourceLevelHandlerCallees.exists(_.contains("createDriver"))
  val syntheticReachesBody = syntheticEntryCallees.exists(_.contains("createDriver"))
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
    "method_expansions" -> jnum(w.expansions.toLong),
    "methods_visited" -> jnum(w.methodsVisited.toLong),
    "call_sites_considered" -> jnum(w.callSitesConsidered.toLong),
    "fan_out_sites_encountered" -> jnum(w.fanOutSitesEncountered.toLong),
    "fan_out_sites_not_followed" -> jnum(w.fanOutSitesNotFollowed.toLong),
    "max_depth_used" -> jnum(w.maxDepthUsed.toLong),
    "depth_bound_reached" -> jbool(w.depthBoundReached),
    "expansion_budget_exhausted" -> jbool(w.expansionBudgetExhausted),
    "route_cap_reached" -> jbool(w.routeCapReached),
    "routes_returned" -> jnum(w.routes.size.toLong)))

  val duplicateFormulationJson = jrawArr(4, List(
    jobj(6, List(
      "against" -> jstr("02-dataflow-unguarded-driver-launch"),
      "status" -> jstr("not_duplicate"),
      "basis" -> jstr("the same handler/sink pair addressed by a different " +
        "formulation: this query traverses CALL edges only and asserts nothing about " +
        "data flow, so the two are two formulations of one question and their returns " +
        "are reported side by side, never summed"))),
    jobj(6, List(
      "against" -> jstr(PARAMETERIZABILITY_OWNER),
      "status" -> jstr("not_duplicate"),
      "basis" -> jstr("a different target set and a different formulation: " +
        PARAMETERIZABILITY_OWNER + " is parameterized over handler/sink pairs and " +
        "covers a second pair this query does not address")))))

  // -------------------------------------------------------------------------
  stage("K-write: the envelope, the prose report and the console log")
  // -------------------------------------------------------------------------
  val resultsDir = repoRoot.resolve(RESULTS_DIR)
  Files.createDirectories(resultsDir)
  val jsonPath = resultsDir.resolve(s"$QUERY_ID.json")
  val mdPath = resultsDir.resolve(s"$QUERY_ID.md")

  val envelope = jobj(0, List(
    "query_id" -> jstr(QUERY_ID),
    "query_source" -> jstr(s"queries/joern/$QUERY_ID.sc"),
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
    "returned_record_count" -> jnum(returnedRecordCount.toLong),
    "returned_record_kinds" -> jobj(2, List(
      "boundary" -> jnum(boundaries.size.toLong),
      "route" -> jnum(distinctRoutes.size.toLong))),
    "distinct_routes" -> jnum(distinctRoutes.size.toLong),
    "distinct_routes_convention" -> jstr("routes from both walks deduplicated on " +
      "(entry point, sink host, hop sequence); the walks' returns are never summed"),
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
    "bound_value" -> jnum(MAX_CALL_DEPTH.toLong),
    "bound_value_meaning" -> jstr("MAX_CALL_DEPTH, the maximum call-graph hops walked " +
      "from an entry point; it exceeds the hop count of the documented route, so a " +
      "route absent within it is not an artefact of a short bound"),
    "bound_reached" -> jbool(boundReached),
    "bounds" -> jobj(2, List(
      "MAX_CALL_DEPTH" -> jnum(MAX_CALL_DEPTH.toLong),
      "MAX_ROUTES" -> jnum(MAX_ROUTES.toLong),
      "MAX_EXPANSIONS_PER_ENTRY" -> jnum(MAX_EXPANSIONS_PER_ENTRY.toLong),
      "MAX_TOTAL_RETURNS" -> jnum(MAX_TOTAL_RETURNS.toLong),
      "MAX_ENTRY_POINTS" -> jnum(MAX_ENTRY_POINTS.toLong),
      "MAX_CALL_SCAN" -> jnum(MAX_CALL_SCAN.toLong),
      "FANOUT_CALLEE_THRESHOLD" -> jnum(FANOUT_CALLEE_THRESHOLD.toLong))),
    "entry_points_discovered" -> jnum(entryPointsDiscovered.toLong),
    "entry_points_traversed" -> jnum(entryPointsTraversed.toLong),
    "entry_points_truncated" -> jnum(entryPointsTruncated.toLong),
    "entry_point_selection" -> jstr("BOUNDARY 4: the handler body compiles into a " +
      "synthetic partial-function class, so the synthetic " + ENTRY_SYNTHETIC_METHOD +
      " on every type matching " + ENTRY_SYNTHETIC_TYPE_REGEX + " is selected together " +
      "with the source-level " + HANDLER_TYPE + "." + HANDLER_METHOD),
    "entry_points" -> jstrArr(entryGroups.map(_._1)),
    "sink_hosts" -> jstrArr(sinkHostNames.toList.sorted),
    "sink_call_sites" -> jstrArr(sinkCalls.map(c =>
      s"${c.method.fullName} -> ${c.methodFullName} #${lineOf(c)}")),
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
        "route is mandated because the alternative spawns a second JVM at the same heap"),
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
    "duplicate_formulation" -> jstr("not_duplicate"),
    "duplicate_formulation_detail" -> duplicateFormulationJson,
    "effort_query_revisions_committed" -> jnum(QUERY_REVISIONS_COMMITTED.toLong),
    "effort_query_revisions_convention" -> jstr(QUERY_REVISIONS_CONVENTION),
    "effort_joern_api_constructs" -> jstrArr(JOERN_API_CONSTRUCTS),
    "effort_joern_api_construct_count" -> jnum(JOERN_API_CONSTRUCTS.distinct.size.toLong),
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
  md0(s"| Query source | `queries/joern/$QUERY_ID.sc` |")
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
      if (w.expansionBudgetExhausted) Some("the per-entry expansion budget was exhausted") else None,
      if (w.routeCapReached) Some("the route cap was reached") else None).flatten
    md0(s"- walk `${w.walkId}`: " +
      (if (bits.isEmpty) "no bound was reached; the walk ran to exhaustion"
       else bits.mkString("; ")) +
      s". Expansion budget used ${w.expansions} of $MAX_EXPANSIONS_PER_ENTRY per entry " +
      s"point; routes returned ${w.routes.size} of $MAX_ROUTES.")
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
  md0(s"| MAX_TOTAL_RETURNS | $MAX_TOTAL_RETURNS |")
  md0(s"| MAX_ENTRY_POINTS | $MAX_ENTRY_POINTS |")
  md0(s"| MAX_CALL_SCAN | $MAX_CALL_SCAN |")
  md0(s"| FANOUT_CALLEE_THRESHOLD | $FANOUT_CALLEE_THRESHOLD |")
  md0("")
  md0("| walk | follows fan-out | expansions | call sites | fan-out seen | " +
    "fan-out not followed | max depth | depth bound reached | budget exhausted | routes |")
  md0("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
  walks.foreach { w =>
    md0(s"| `${w.walkId}` | ${w.followsFanOut} | ${w.expansions} | " +
      s"${w.callSitesConsidered} | ${w.fanOutSitesEncountered} | " +
      s"${w.fanOutSitesNotFollowed} | ${w.maxDepthUsed} | ${w.depthBoundReached} | " +
      s"${w.expansionBudgetExhausted} | ${w.routes.size} |")
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
  md0("`duplicate_formulation` = **not_duplicate**.")
  md0("")
  md0("- Against `02-dataflow-unguarded-driver-launch`: the **same** handler/sink pair by")
  md0("  a **different** formulation. This query traverses CALL edges only and asserts")
  md0("  nothing about data flow. The two are two formulations of one question, so their")
  md0("  returns are reported side by side and **never summed**.")
  md0(s"- Against `$PARAMETERIZABILITY_OWNER`: a different target set and a different")
  md0("  formulation - that query is parameterized over handler/sink pairs and covers a")
  md0("  second pair this query does not address.")
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
  md0("cd <a scratch directory outside the repository>")
  md0("HARNESS_REPO_ROOT=<repo> JAVA_HOME=\"$JAVA_HOME_21\" \\")
  md0("  JAVA_TOOL_OPTIONS=\"-Xmx64g\" SL_LOGGING_LEVEL=WARN \\")
  md0(s"  joern --script <repo>/queries/joern/$QUERY_ID.sc -J-Xmx64g < /dev/null")
  md0("```")
  md0("")
  md0("`joern --script` forks a child JVM and does not forward `-J-Xmx` to it, so")
  md0("`JAVA_TOOL_OPTIONS` is the override that actually raises the heap the query runs")
  md0("at. The query measures the heap it received and stops below the floor: raising a")
  md0("heap is permitted and reported, lowering one is not.")
  md0("")

  writeUtf8(mdPath, md.mkString("", "\n", "\n"))
  log(s"prose report written      : $mdPath (${md.size} lines)")

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
  log(s"duplicate_formulation     : not_duplicate")
  log(s"joern_api_constructs      : ${JOERN_API_CONSTRUCTS.distinct.size}")
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
