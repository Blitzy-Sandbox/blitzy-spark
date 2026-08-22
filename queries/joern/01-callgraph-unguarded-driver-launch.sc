// queries/joern/01-callgraph-unguarded-driver-launch.sc
// ===========================================================================================
// Phase 3 capability probe — query 01 of three, the CALL-GRAPH formulation.
//
// WHAT THIS QUERY ATTEMPTS
// ------------------------
// One reachability class, expressed over the call graph and over nothing else:
//
//     an RPC entry point named `receive` or `receiveAndReply`, enclosed in a type whose full
//     name lies under `org.apache.spark.deploy.`, reaching a privileged sink — `createDriver`,
//     a `DriverRunner` construction, or a process launch — along a path on which no derived
//     authentication or ACL predicate appears.
//
// Those three sinks are the three the probe's worked example names, and they are labelled with
// that vocabulary below. A fourth anchor, an `ExecutorRunner` construction, is carried
// separately and is labelled `additional`; it never substitutes for one of the three.
//
// Queries 02 and 03 attempt the same whole class by two other formulations — data flow, and a
// parameterized form. A class can be expressible in one view and not another, so no single
// query's outcome settles the question this probe asks, and this script therefore draws no
// conclusion and orders nothing. It emits what its traversal found, and, equally, what its
// traversal could not express, in `diagnostics`.
//
// THE GRAPH IS READ, NEVER BUILT
// ------------------------------
// The persisted code-property graph at `harness/cpg/spark.cpg` is loaded with `importCpg`, or
// opened from the workspace when a previous script in this workspace already loaded it. No
// graph-construction command appears anywhere in this file: this script cannot build a graph,
// and nothing under `harness/` is written, edited or removed by it. The only path it writes to
// is the Joern workspace named below, which is scratch and is never cleaned up here.
//
// INVOCATION
// ----------
//     joern --script queries/joern/01-callgraph-unguarded-driver-launch.sc
//
// Exactly that, run from the directory that contains `harness/` — the two paths below are
// relative to it. `exec` takes no parameters, so no `--param` is required or accepted, no
// environment variable is read, and nothing is prompted for: a reader can run the line above
// by hand and get precisely what the Phase 3 driver gets.
//
// LOAD ORDERING IS A CORRECTNESS CONSTRAINT, NOT A STYLE CHOICE
// ------------------------------------------------------------
//  1. `---BLITZY-START---` is printed as the very first action, before the workspace is
//     switched and before anything is loaded. It is the only thing that distinguishes a script
//     that never compiled (no start marker) from one that compiled and then threw while
//     loading (start marker, no result region). Printing it after the load would misreport the
//     second case as the first.
//  2. `switchWorkspace` is called BEFORE any load. It closes the current workspace and opens
//     another, so a load performed first would be discarded by it.
//  3. The load is idempotent AND provenance-checked. The workspace is persistent scratch shared
//     with the other Phase 3 queries, so a project of the same name may already be present —
//     but a project's NAME is derived from its input path's last segment and is therefore not
//     evidence of its contents. Before an existing project is opened, the input path Joern
//     recorded for it when it was created is canonicalized and compared with the canonical
//     path of the graph this script is contracted to read; a mismatch, or a recorded path that
//     no longer canonicalizes, FAILS THE RUN CLOSED through the failure protocol below rather
//     than substituting a stale graph for the pinned one. On a match the verified identity —
//     the canonical graph path, its size in bytes, and the project's own recorded input path —
//     is recorded in `diagnostics.graph_identity`, so a reader can check what was read rather
//     than take it on trust. Which of import or open happened is recorded in
//     `diagnostics.load_mode`. Opening an existing project is still reading.
//
// RESULT CONTRACT, AND THE FAILURE PROTOCOL THAT IS ITS OTHER HALF
// ---------------------------------------------------------------
// On success: one JSON object, printed strictly between `---BLITZY-RESULT-BEGIN---` and
// `---BLITZY-RESULT-END---`, with nothing else in that region — the driver slices it and parses
// it, so a single stray line there would be read as a runtime failure that did not happen. All
// graph work completes, and the whole document is built as one string, BEFORE the BEGIN marker
// is printed: the markers are emitted only once a complete result exists.
//
// On failure: NO result region is printed at all. Any failure — a provenance mismatch, an empty
// graph, an exception from any traversal — is written to STDERR as one `---BLITZY-FAILURE---`
// line naming the stage, the exception type and its message, followed by the stack trace, and
// the exception is then re-raised so the process terminates with a non-zero exit status. That
// combination — start marker present, result region absent, exit status non-zero — is what tells
// the driver a run compiled and did not complete. Emitting a result region after a caught
// failure, as an earlier revision of this script did, would have the driver classify a failed or
// partial run as a successful one, so no error path here produces a payload of any kind.
//
// The success object has exactly two top-level keys:
//
//   {
//     "returns": [
//       {
//         "handler":            "<method full name of the RPC entry point>",
//         "sink":               "<method full name of the sink reached>",
//         "path":               ["<method full name>", "..."],  // handler first, sink last
//         "predicates_on_path": ["<predicate full name>", "..."]  // [] when none was found
//       }
//     ],
//     "diagnostics": { ... }
//   }
//
// Each member of `returns` carries exactly those four keys and no others. `diagnostics` carries
// what the traversal resolved and where it stopped, because a recorded limit is part of the
// answer this probe produces, and a query that returns nothing while recording why it returned
// nothing is a result, whereas one that returns nothing silently is a defect. Its keys:
//
//   load_mode           whether the project was imported or opened (see above).
//   graph_identity      the canonical path of the graph read, its size in bytes, the input path
//                       the workspace project recorded, and how the two were compared.
//   cpg_method_count    method count read from the loaded graph; evidence it loaded non-empty.
//   derived_predicates  the authentication/ACL predicate set, derived from the graph at
//                       execution time and never hardcoded, with every exclusion rule that
//                       fired and exactly what each removed.
//   handler_selection   the entry-point discriminator, every candidate it accepted, and every
//                       candidate it excluded with the rule that excluded it and the evidence.
//   handler_anchors     each entry-point anchor with the node count it resolved to and the
//                       names resolved — zero counts included.
//   sink_anchors        the same, per sink anchor, with each anchor's label and kind.
//   bridges             per boundary the traversal has to cross: whether the rule fired, what
//                       it connected, and whether an emitted path actually needed it.
//   route_enumeration   every bound the route enumeration applies, and, per bound, whether it
//                       was reached and what it left out.
//   traversal           the depth bound and whether it was reached, what the frontier was
//                       allowed to expand through, how routes are enumerated, and the reach of
//                       the predicate check.
//
// Output is deterministic: every collection printed is sorted or built in a fixed order, so
// re-running an unchanged source produces byte-identical output and a diff between revisions
// means something. The one field that legitimately differs between a run against a fresh
// workspace and a run against a warm one is `load_mode`, whose whole purpose is to record that
// difference. Every value emitted is read from the graph; nothing is estimated, inferred or
// filled in.
//
// THE PREDICATE SET, AND THE TWO TRAPS IN DERIVING IT
// ---------------------------------------------------
// The set is derived at execution time so that a predicate added or renamed in the tree the
// graph was built over is not missed. Members of `SecurityManager` whose names match
// `check.*Permissions`, `acls.*` or `isAuthenticationEnabled` are selected by ANCHORED FULL
// MATCH — the difference between a small set and a large one, since a substring test would
// admit every `set…Acls` / `get…Acls` accessor. Two mechanical exclusion rules then run, in
// order: names ending in the Scala setter suffix are dropped, and names that coincide with a
// field/member name on the type declaration are dropped — which removes a matching field
// accessor by construction rather than by naming it. A third rule stands in for the second
// where the frontend populates no member nodes: a name is dropped when a setter for it exists
// on the same type declaration, which is the same evidence obtained a different way. Which
// rules fired, and what each removed, is recorded. The count is whatever the derivation
// produces on the graph in front of it and is never adjusted toward an expected number.
//
// WHICH METHODS ARE ENTRY ANCHORS, AND WHY NAME AND PACKAGE ARE NOT ENOUGH
// -----------------------------------------------------------------------
// The class names its entry point as `receive` / `receiveAndReply` in the `deploy` package, and
// selecting on exactly that — a name and an enclosing-type prefix — resolves methods that are
// not standalone-mode RPC handler declarations at all: a driver-plugin `receive` that happens to
// share the name, a YARN endpoint outside standalone mode, and a trait's inherited default that
// has no declaration of its own and whose body is a forwarder. Each of those becomes a return
// whose handler never received the message the class is about, so the entry set is qualified
// STRUCTURALLY, by three mechanical tests over the graph, applied in this order and each
// recorded with what it removed:
//
//   1. SIGNATURE. An `RpcEndpoint` handler declares `scala.PartialFunction()` (for `receive`) or
//      `scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` (for `receiveAndReply`). A
//      method of the same name with any other signature implements a different interface.
//   2. ENCLOSING TYPE. Under `org.apache.spark.deploy.` and NOT under
//      `org.apache.spark.deploy.yarn.`, because the class the probe was asked about is
//      standalone deploy mode and a YARN endpoint is a different deployment.
//   3. ITS OWN CASE BODIES. A declared partial-function handler allocates the synthetic
//      partial-function class that carries its case bodies, so one of its outgoing calls names a
//      type whose full name is its OWN enclosing type followed by the frontend's partial-function
//      infix and its OWN method name. An inherited trait default allocates nothing of the kind:
//      its only call is the trait's static forwarder. This test is what tells a declaration from
//      an inherited default without reading a line number or naming a type.
//
// Nothing is hardcoded: the set is whatever those three tests accept on the graph in front of
// them, and every candidate they reject is reported in `diagnostics.handler_selection` with the
// rule and the evidence, because the exclusions are the evidence that the discriminator did its
// work. A trait's static forwarder remains a legitimate TRAVERSAL BRIDGE — a path may still run
// through one, and `diagnostics.traversal` reports when one did; it simply stops being a place a
// path may START.
//
// THE TWO BOUNDARIES THIS TRAVERSAL HAS TO CROSS
// ----------------------------------------------
// Over a JVM-bytecode frontend, a pure callee chase stops twice on the way from an entry point
// to a sink, and both stops are properties of Scala compilation rather than of the code being
// analysed:
//
//   * At the entry-point end, a handler returning a `PartialFunction` does not contain its own
//     case bodies: it allocates a synthetic partial-function class, and the bodies live on that
//     class. A traversal that stops there reaches no sink at all, and would report zero for a
//     reason that has nothing to do with what it was asked to find.
//   * At the sink end, a method that hands work to a thread does not call the work: it
//     allocates an anonymous class and the runtime invokes its `run`.
//
// Both are bridged mechanically, by relating a frontier method to the anonymous type it
// allocates — the partial-function class named after that same method, and the anonymous class
// whose `run` carries the deferred body. Neither bridge invents an edge: each one connects a
// method to a type that method demonstrably allocates. Whether each fired, what it connected,
// and whether an emitted path needed it, is recorded.
//
// ONE RETURN PER ROUTE, NOT ONE PER SINK
// --------------------------------------
// A sink is commonly reachable from an entry point by more than one route, and the routes differ
// in exactly the thing this probe is about: which methods lie between the two ends, and which
// predicates lie on them. A traversal that keeps one predecessor per method — the first one it
// happened to discover — emits one route per (entry point, sink) pair and silently discards the
// rest, so a route through one construction can hide a route through another, and a route with
// no predicate on it can hide one that has a predicate (or the reverse). This script therefore
// enumerates routes in two passes:
//
//   * FORWARD, once per entry point: a level-synchronous frontier that expands each method at
//     most once but records EVERY edge it observes, as a set of predecessors per method rather
//     than a single one. Each method is expanded once, so the enumeration is over the edge set
//     that bounded frontier observed; a route needing a method expanded a second time, deeper
//     than its first discovery, is outside what this pass observes, and that is stated here
//     rather than left to be discovered.
//   * BACKWARD, once per (entry point, sink) pair: every distinct SIMPLE ordered route over
//     those edges, within the depth bound, enumerated in predecessor-name order so the output
//     is reproducible. Deduplication is on the exact emitted tuple — entry point, sink, ordered
//     path and predicates found on it — never on the sink method, so two genuinely different
//     routes to one sink are two returns and two identical ones are one.
//
// Enumeration is BOUNDED, and every bound is reported in `diagnostics.route_enumeration` with
// whether it was reached: a cap on routes per pair (with an exact `routes_beyond_the_cap_exist`
// flag, established by looking for one route past the cap), a cap on the backward-search steps a
// single pair may take, and a cap on the total returns one run may emit. A bound that fires is
// part of the answer, so it is named with the pairs it fired on rather than smoothed over.
//
// THE PREDICATE FILTER AND `predicates_on_path` ARE TWO DIFFERENT THINGS
// ---------------------------------------------------------------------
// The traversal filter never expands through a derived predicate, so no predicate is ever an
// intermediate node of an emitted path; that filter is what makes this a query for the
// unguarded class. `predicates_on_path` is then computed separately over the emitted path, as
// found, and its reach is wider: a path node that is itself a predicate, plus the predicates
// each path node calls directly. So the second pass can find what the filter did not, and where
// it does the return is emitted anyway with `predicates_on_path` populated — a return is never
// dropped for carrying a predicate, and a predicate is never added to make one look as though
// it does. Whether a predicate lies on a path is a property of this formulation: a path that
// stops at a construction does not traverse what a path continuing past it traverses, and both
// are correct answers about their own path.
// ===========================================================================================

import io.shiftleft.codepropertygraph.generated.nodes.Method

// --- Paths. Both are relative to the directory that contains `harness/`. -------------------
val WORKSPACE_PATH = "queries/joern/.workspace"
val CPG_PATH       = "harness/cpg/spark.cpg"

// --- The markers of the query-to-driver contract. -------------------------------------------
// The three stdout markers, and the stderr marker that carries a failure. A failure never
// produces a result region, so BEGIN and END are printed on exactly one path through the script.
val MARKER_START   = "---BLITZY-START---"
val MARKER_BEGIN   = "---BLITZY-RESULT-BEGIN---"
val MARKER_END     = "---BLITZY-RESULT-END---"
val MARKER_FAILURE = "---BLITZY-FAILURE---"

// --- Selectors. Every one is an anchored full-match regex over a graph property. -----------
val DEPLOY_TYPE_SELECTOR      = "org\\.apache\\.spark\\.deploy\\..*"
val DEPLOY_YARN_TYPE_SELECTOR = "org\\.apache\\.spark\\.deploy\\.yarn\\..*"
val SECURITY_MANAGER_SELECTOR = "org\\.apache\\.spark\\.SecurityManager"
val PREDICATE_NAME_SELECTOR   = "^(check.*Permissions|acls.*|isAuthenticationEnabled)$"
val SCALA_SETTER_SUFFIX       = "_$eq"

// --- The entry-point discriminator. See WHICH METHODS ARE ENTRY ANCHORS in the header. -----
// The two names the class asks for, the two signatures an `RpcEndpoint` handler declares, and
// the frontend's infix for the synthetic class that carries a partial function's case bodies.
val HANDLER_NAMES = List("receive", "receiveAndReply")
val HANDLER_SIGNATURES = List(
  "scala.PartialFunction()",
  "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)")
val PARTIAL_FUNCTION_INFIX = "$$anonfun$"

val CREATE_DRIVER_SELECTOR   = "(.*\\$\\$)?createDriver"
val DRIVER_RUNNER_SELECTOR   = "org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>.*"
val PROCESS_LAUNCH_SELECTOR =
  "(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec).*"
val EXECUTOR_RUNNER_SELECTOR = "org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>.*"

// --- Traversal bounds. --------------------------------------------------------------------
// The deepest sink in this class is reached through the thread bridge and a launch chain below
// it, so a shallow bound cannot see it. This bound leaves substantial headroom over that, and
// whether it was reached is recorded rather than passed over in silence.
val MAX_CALL_DEPTH   = 20
val EXPANSION_PREFIX = "org.apache.spark."
val OPERATOR_PREFIX  = "<operator>"

// --- Route-enumeration bounds. See ONE RETURN PER ROUTE in the header. ----------------------
// Enumerating simple routes over a call graph is exponential in the depth bound, so the
// enumeration is capped in three independent places and every cap is reported with whether it
// fired. The caps are set so that the class this probe was asked about is answered well inside
// them: its routes run five to seven edges, one entry point reaches at most a handful of sinks,
// and the whole run emits returns in the tens. They exist to bound a pathological graph, not to
// shape this answer.
//
//   MAX_ROUTES_PER_PAIR                  distinct routes emitted per (entry point, sink) pair.
//   MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR predecessor steps the backward search may take for one
//                                        pair before it stops and says so.
//   MAX_RETURNS_TOTAL                    returns one run may emit, over every pair.
val MAX_ROUTES_PER_PAIR                  = 8
val MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR = 200000
val MAX_RETURNS_TOTAL                    = 500

// --- JSON serialization. Deterministic, and escaping is explicit. --------------------------
// Method full names carry `$`, `<`, `>`, `:`, `(`, `)`, `,`, `[`, `]`, `.` and `/`, so nothing
// here is concatenated without escaping and an empty array is emitted as `[]`.

def jsonEscape(raw: String): String = {
  val sb = new StringBuilder
  raw.foreach {
    case '"'  => sb.append('\\'); sb.append('"')
    case '\\' => sb.append('\\'); sb.append('\\')
    case '\n' => sb.append('\\'); sb.append('n')
    case '\r' => sb.append('\\'); sb.append('r')
    case '\t' => sb.append('\\'); sb.append('t')
    case '\b' => sb.append('\\'); sb.append('b')
    case '\f' => sb.append('\\'); sb.append('f')
    case c if c.toInt < 0x20 || c.toInt == 0x7f =>
      sb.append('\\'); sb.append('u'); sb.append("%04x".format(c.toInt))
    case c => sb.append(c)
  }
  sb.toString
}

def jsonString(raw: String): String = "\"" + jsonEscape(raw) + "\""

def jsonStringArray(values: Seq[String]): String =
  if (values.isEmpty) "[]" else values.map(jsonString).mkString("[", ", ", "]")

def jsonBool(value: Boolean): String = if (value) "true" else "false"

def jsonInt(value: Int): String = value.toString

/** An object on one line, in the order the fields are given. */
def jsonObject(fields: Seq[(String, String)]): String =
  fields.map { case (k, v) => jsonString(k) + ": " + v }.mkString("{", ", ", "}")

/** An array of already-rendered values, one per line at the given indent. */
def jsonBlockArray(rendered: Seq[String], indent: String): String =
  if (rendered.isEmpty) "[]"
  else rendered.map(indent + "  " + _).mkString("[\n", ",\n", "\n" + indent + "]")

/** An object of already-rendered values, one field per line at the given indent. */
def jsonBlockObject(fields: Seq[(String, String)], indent: String): String =
  if (fields.isEmpty) "{}"
  else
    fields
      .map { case (k, v) => indent + "  " + jsonString(k) + ": " + v }
      .mkString("{\n", ",\n", "\n" + indent + "}")

// ===========================================================================================
// The failure protocol. See RESULT CONTRACT in the header.
//
// One function, used by every error path in this script, so that a failure cannot be reported
// two ways. It writes the stage, the exception type and the message to stderr as a single
// marked line, follows it with the stack trace for a human reading the captured log, and then
// re-raises: the run therefore ends with the start marker printed on stdout, NO result region,
// and a non-zero exit status, which is the shape the driver classifies as "compiled, did not
// complete". stdout is flushed first so that whatever the script had already printed cannot be
// interleaved into the stderr report.
// ===========================================================================================

def reportFailureAndRaise(stage: String, failure: Throwable): Nothing = {
  System.out.flush()
  System.err.println(
    MARKER_FAILURE + " stage=" + stage + " type=" + failure.getClass.getName +
      " message=" + Option(failure.getMessage).getOrElse(""))
  failure.printStackTrace(System.err)
  System.err.flush()
  throw failure
}

/**
 * The canonical, symlink-resolved path of a file, or None where it does not resolve. Used on
 * both sides of the graph-provenance comparison, so that a clone-relative path, an absolute
 * path and a symlink to the same graph compare equal while a different graph does not.
 */
def canonicalPathOf(rawPath: String): Option[String] =
  scala.util
    .Try(java.nio.file.Paths.get(rawPath).toRealPath().toString)
    .toOption

// ===========================================================================================
// The query. `exec` takes no parameters: see INVOCATION in the header.
// ===========================================================================================

@main def exec(): Unit = {

  // (1) The start marker is the very first action, before the workspace switch and before any
  //     load — it is what tells a compile failure apart from an early runtime failure.
  println(MARKER_START)

  // Rendered JSON fragments, accumulated in a fixed order so the output is deterministic.
  val diagnostics     = scala.collection.mutable.ListBuffer.empty[(String, String)]
  val renderedReturns = scala.collection.mutable.ListBuffer.empty[String]

  // Names the stage the run reached, so a failure below is reported against the step that
  // failed rather than as an unattributed error.
  var stage = "start"

  try {
    // -------------------------------------------------------------------------------------
    // (2) Workspace first. It closes the current workspace and opens another, so switching
    //     after a load would discard the loaded project.
    // -------------------------------------------------------------------------------------
    stage = "switch_workspace"
    switchWorkspace(WORKSPACE_PATH)

    // -------------------------------------------------------------------------------------
    // (3) Idempotent, provenance-checked load. The workspace is persistent scratch shared with
    //     the other Phase 3 queries, so a project of this name may already be present — and a
    //     project name is only the last segment of the input path it was created from, so it is
    //     not evidence of what the project holds. An existing project is therefore opened only
    //     when the input path Joern recorded for it canonicalizes to the same file as the graph
    //     this script reads; anything else fails the run closed.
    // -------------------------------------------------------------------------------------
    stage = "load_graph"
    val projectName = CPG_PATH.split('/').last

    val cpgCanonicalPath = canonicalPathOf(CPG_PATH).getOrElse(
      throw new RuntimeException(
        "the graph this script is contracted to read does not resolve to a file: " + CPG_PATH +
          " (relative to the working directory, which must be the directory containing " +
          "`harness/`)"))
    val cpgSizeBytes = java.nio.file.Files.size(java.nio.file.Paths.get(cpgCanonicalPath))

    val existingProject   = workspace.projects.find(_.name == projectName)
    val recordedInputPath = existingProject.map(_.inputPath)
    val recordedCanonical = recordedInputPath.flatMap(canonicalPathOf)
    val provenanceOutcome =
      existingProject match {
        case None => "no_existing_project_the_pinned_graph_was_imported"
        case Some(_) if recordedCanonical.contains(cpgCanonicalPath) =>
          "existing_project_recorded_input_path_canonicalizes_to_the_pinned_graph"
        case Some(_) =>
          throw new RuntimeException(
            "the workspace already holds a project named `" + projectName + "` whose recorded " +
              "input path is not the graph this script reads: recorded `" +
              recordedInputPath.getOrElse("") + "` canonicalizing to `" +
              recordedCanonical.getOrElse("<does not resolve>") + "`, expected `" +
              cpgCanonicalPath + "`. Opening it would substitute a stale or foreign graph for " +
              "the pinned one, so the run fails closed. Remove the stale project from " +
              WORKSPACE_PATH + " (it is scratch) and re-run")
      }

    val loadMode =
      if (existingProject.isDefined) {
        val opened = open(projectName)
        if (opened.isEmpty) {
          throw new RuntimeException(
            "project is present in the workspace and its provenance was verified, but it could " +
              "not be opened: " + projectName)
        }
        "opened_existing_project"
      } else {
        importCpg(CPG_PATH)
        "imported_persisted_cpg"
      }

    // Read back the project the run is actually working against, so the recorded identity is
    // the loaded project's own and not the pre-load observation.
    val loadedProject = workspace.projects.find(_.name == projectName)

    val methodCount = cpg.method.size
    diagnostics.append("load_mode"        -> jsonString(loadMode))
    diagnostics.append("workspace"        -> jsonString(WORKSPACE_PATH))
    diagnostics.append("cpg_source"       -> jsonString(CPG_PATH))
    diagnostics.append("cpg_project_name" -> jsonString(projectName))
    diagnostics.append(
      "graph_identity" -> jsonBlockObject(
        Seq(
          "declared_relative_path" -> jsonString(CPG_PATH),
          "canonical_path"         -> jsonString(cpgCanonicalPath),
          "size_bytes"             -> jsonString(cpgSizeBytes.toString),
          "project_recorded_input_path" ->
            (recordedInputPath.orElse(loadedProject.map(_.inputPath)) match {
              case Some(path) => jsonString(path)
              case None       => "null"
            }),
          "project_recorded_input_path_canonical" ->
            (recordedCanonical
              .orElse(loadedProject.map(_.inputPath).flatMap(canonicalPathOf)) match {
              case Some(path) => jsonString(path)
              case None       => "null"
            }),
          "project_directory" ->
            (loadedProject.map(_.path.toString) match {
              case Some(path) => jsonString(path)
              case None       => "null"
            }),
          "project_applied_overlays" ->
            jsonStringArray(loadedProject.map(_.appliedOverlays.toList).getOrElse(Nil).sorted),
          "verification_rule" -> jsonString(
            "an existing workspace project is opened only when the input path it recorded at " +
              "creation canonicalizes — symlinks resolved — to the same file as the graph path " +
              "above; a mismatch, or a recorded path that no longer resolves, fails the run " +
              "closed rather than reading a stale graph. Size is recorded as evidence and is " +
              "deliberately not the test: applying an overlay legitimately changes the copy the " +
              "project holds without changing which graph it came from"),
          "outcome" -> jsonString(provenanceOutcome)),
        "    "))
    diagnostics.append("cpg_method_count" -> jsonInt(methodCount))
    if (methodCount == 0) {
      throw new RuntimeException("the loaded graph reports zero methods: " + CPG_PATH)
    }

    // -------------------------------------------------------------------------------------
    // (4) The authentication / ACL predicate set, derived from the graph — never hardcoded.
    //     Anchored full match, then the exclusion rules in order, each recorded with what it
    //     removed. The resulting count is whatever the derivation produces.
    // -------------------------------------------------------------------------------------
    stage = "derive_predicates"
    val securityManagerTypes =
      cpg.typeDecl.fullName(SECURITY_MANAGER_SELECTOR).fullName.l.distinct.sorted
    val securityManagerMembers =
      cpg.typeDecl.fullName(SECURITY_MANAGER_SELECTOR).member.name.l.distinct.sorted
    val securityManagerMethods =
      cpg.typeDecl.fullName(SECURITY_MANAGER_SELECTOR).method.l.distinctBy(_.fullName)

    val patternMatched = securityManagerMethods.filter(_.name.matches(PREDICATE_NAME_SELECTOR))

    // Rule 1 — drop the Scala setter accessors.
    val afterSetterRule     = patternMatched.filterNot(_.name.endsWith(SCALA_SETTER_SUFFIX))
    val removedBySetterRule =
      patternMatched.map(_.name).diff(afterSetterRule.map(_.name)).distinct.sorted

    // Rule 2 — drop names that coincide with a field/member name on the type declaration.
    //          This removes a matching field accessor by construction.
    val memberNames          = securityManagerMembers.toSet
    val memberRuleApplicable = securityManagerMembers.nonEmpty
    val afterMemberRule =
      if (memberRuleApplicable) afterSetterRule.filterNot(m => memberNames.contains(m.name))
      else afterSetterRule
    val removedByMemberRule =
      afterSetterRule.map(_.name).diff(afterMemberRule.map(_.name)).distinct.sorted

    // Rule 3 — the same evidence obtained another way, for a frontend that populates no member
    //          nodes: a name is a field accessor when a setter for it exists on the same type.
    //          Applied only where rule 2 could not be.
    val settersPresent = securityManagerMethods
      .map(_.name)
      .filter(_.endsWith(SCALA_SETTER_SUFFIX))
      .map(_.dropRight(SCALA_SETTER_SUFFIX.length))
      .toSet
    val accessorRuleApplicable = !memberRuleApplicable
    val afterAccessorRule =
      if (accessorRuleApplicable) afterMemberRule.filterNot(m => settersPresent.contains(m.name))
      else afterMemberRule
    val removedByAccessorRule =
      afterMemberRule.map(_.name).diff(afterAccessorRule.map(_.name)).distinct.sorted

    val predicateFullNames = afterAccessorRule.map(_.fullName).distinct.sorted
    val predicateSet       = predicateFullNames.toSet

    diagnostics.append(
      "derived_predicates" -> jsonBlockObject(
        Seq(
          "type_declaration_selector" -> jsonString(SECURITY_MANAGER_SELECTOR),
          "resolved_type_declarations" -> jsonStringArray(securityManagerTypes),
          "member_names_on_type_declaration" -> jsonStringArray(securityManagerMembers),
          "name_selector" -> jsonString(PREDICATE_NAME_SELECTOR),
          "match_mode" -> jsonString("anchored full match"),
          "methods_considered" -> jsonInt(securityManagerMethods.size),
          "pattern_matched" -> jsonStringArray(patternMatched.map(_.name).distinct.sorted),
          "exclusion_rules" -> jsonBlockArray(
            Seq(
              jsonObject(Seq(
                "rule"     -> jsonString("scala_setter_suffix"),
                "applied"  -> jsonBool(true),
                "removed"  -> jsonStringArray(removedBySetterRule))),
              jsonObject(Seq(
                "rule"     -> jsonString("field_member_name_collision"),
                "applied"  -> jsonBool(memberRuleApplicable),
                "removed"  -> jsonStringArray(removedByMemberRule))),
              jsonObject(Seq(
                "rule"     -> jsonString("field_accessor_setter_evidence"),
                "applied"  -> jsonBool(accessorRuleApplicable),
                "removed"  -> jsonStringArray(removedByAccessorRule)))),
            "      "),
          "resolved" -> jsonStringArray(predicateFullNames),
          "count"    -> jsonInt(predicateFullNames.size)),
        "    "))

    // -------------------------------------------------------------------------------------
    // (5) Anchors. The entry points are selected by name AND by the full name of the type that
    //     encloses them — so an endpoint declared inside another type is reached too — and are
    //     then qualified structurally by the three tests in WHICH METHODS ARE ENTRY ANCHORS,
    //     which is what separates a declared standalone RPC handler from a same-named plugin
    //     method, a YARN endpoint and an inherited trait default. Every anchor is reported with
    //     the node count it resolved to, zero counts included, and every candidate the
    //     discriminator rejected is reported with the rule that rejected it.
    // -------------------------------------------------------------------------------------
    stage = "resolve_anchors"

    case class Anchor(label: String, kind: String, selector: String, methods: List[Method])

    def resolved(methods: List[Method]): List[Method] =
      methods.distinctBy(_.fullName).sortBy(_.fullName)

    /** A candidate rejected by the discriminator, with the rule and the evidence it was read on. */
    final case class HandlerExclusion(fullName: String, rule: String, evidence: String)

    val handlerCandidates = resolved(
      HANDLER_NAMES.flatMap(handlerName =>
        cpg.method
          .nameExact(handlerName)
          .where(_.typeDecl.fullName(DEPLOY_TYPE_SELECTOR))
          .l))

    val handlerExclusions = scala.collection.mutable.ListBuffer.empty[HandlerExclusion]

    /**
     * The full names of the synthetic partial-function types a method allocates for its own case
     * bodies: an outgoing call into a type named for this method's own enclosing type, the
     * frontend's partial-function infix and this method's own name. A declared partial-function
     * handler has at least one; an inherited trait default has none, and its calls name the
     * trait's static forwarder instead.
     */
    def ownPartialFunctionBodyAllocations(method: Method): List[String] = {
      val owner  = method.typeDecl.fullName.headOption.getOrElse("")
      val prefix = owner + PARTIAL_FUNCTION_INFIX + method.name + "$"
      method.call.methodFullName.l.filter(_.startsWith(prefix)).distinct.sorted
    }

    val handlerMethodsAccepted = handlerCandidates.filter { candidate =>
      val owner      = candidate.typeDecl.fullName.headOption.getOrElse("")
      val signature  = candidate.signature
      val ownBodies  = ownPartialFunctionBodyAllocations(candidate)
      val otherCalls =
        candidate.call.methodFullName.l
          .filterNot(_.startsWith(OPERATOR_PREFIX))
          .distinct
          .sorted

      if (!HANDLER_SIGNATURES.contains(signature)) {
        handlerExclusions.append(
          HandlerExclusion(
            candidate.fullName,
            "signature_is_not_an_rpc_endpoint_handler_signature",
            "signature is `" + signature + "`, and an RpcEndpoint handler declares one of " +
              HANDLER_SIGNATURES.mkString("`", "`, `", "`")))
        false
      } else if (owner.matches(DEPLOY_YARN_TYPE_SELECTOR)) {
        handlerExclusions.append(
          HandlerExclusion(
            candidate.fullName,
            "enclosing_type_is_outside_standalone_deploy",
            "enclosing type `" + owner + "` matches " + DEPLOY_YARN_TYPE_SELECTOR +
              ", and the class this query attempts is standalone deploy mode"))
        false
      } else if (ownBodies.isEmpty) {
        handlerExclusions.append(
          HandlerExclusion(
            candidate.fullName,
            "declares_no_partial_function_body_class_of_its_own",
            "no outgoing call names a type beginning `" + owner + PARTIAL_FUNCTION_INFIX +
              candidate.name + "$`, so this is an inherited trait default rather than a " +
              "declaration; its non-operator calls are " +
              (if (otherCalls.isEmpty) "none" else otherCalls.mkString("`", "`, `", "`"))))
        false
      } else true
    }

    val handlerAnchors = HANDLER_NAMES.map { handlerName =>
      Anchor(
        handlerName,
        "user_named",
        "method name is exactly `" + handlerName + "`, the enclosing type full name matches " +
          DEPLOY_TYPE_SELECTOR + " and not " + DEPLOY_YARN_TYPE_SELECTOR + ", the signature is " +
          "one an RpcEndpoint handler declares, and the method allocates the partial-function " +
          "body class of its own name",
        handlerMethodsAccepted.filter(_.name == handlerName))
    }

    diagnostics.append(
      "handler_selection" -> jsonBlockObject(
        Seq(
          "name_selector"                    -> jsonStringArray(HANDLER_NAMES),
          "enclosing_type_selector"          -> jsonString(DEPLOY_TYPE_SELECTOR),
          "excluded_enclosing_type_selector" -> jsonString(DEPLOY_YARN_TYPE_SELECTOR),
          "signature_selector"               -> jsonStringArray(HANDLER_SIGNATURES),
          "own_body_class_rule" -> jsonString(
            "an outgoing call whose method full name begins with the candidate's own enclosing " +
              "type, `" + PARTIAL_FUNCTION_INFIX + "` and the candidate's own name — the " +
              "evidence that the candidate DECLARES the partial function rather than inheriting " +
              "a trait default"),
          "rule_order" -> jsonStringArray(
            List(
              "signature_is_not_an_rpc_endpoint_handler_signature",
              "enclosing_type_is_outside_standalone_deploy",
              "declares_no_partial_function_body_class_of_its_own")),
          "candidates_considered" -> jsonInt(handlerCandidates.size),
          "candidates"            -> jsonStringArray(handlerCandidates.map(_.fullName)),
          "accepted_count"        -> jsonInt(handlerMethodsAccepted.size),
          "accepted"              -> jsonStringArray(handlerMethodsAccepted.map(_.fullName)),
          "excluded_count"        -> jsonInt(handlerExclusions.size),
          "excluded" -> jsonBlockArray(
            handlerExclusions.toList
              .sortBy(_.fullName)
              .map(exclusion =>
                jsonObject(Seq(
                  "full_name" -> jsonString(exclusion.fullName),
                  "rule"      -> jsonString(exclusion.rule),
                  "evidence"  -> jsonString(exclusion.evidence)))),
            "      "),
          "trait_forwarder_note" -> jsonString(
            "a trait's static forwarder remains a traversal bridge — a route may run through " +
              "one, and the traversal block reports when one did — it is only barred from being " +
              "a place a route STARTS")),
        "    "))

    val sinkAnchors = List(
      Anchor(
        "createDriver",
        "user_named",
        "method name matches " + CREATE_DRIVER_SELECTOR +
          " (which admits the Scala-mangled form of a private method) and the enclosing type " +
          "full name matches " + DEPLOY_TYPE_SELECTOR,
        resolved(
          cpg.method
            .name(CREATE_DRIVER_SELECTOR)
            .where(_.typeDecl.fullName(DEPLOY_TYPE_SELECTOR))
            .l)),
      Anchor(
        "DriverRunner",
        "user_named",
        "method full name matches " + DRIVER_RUNNER_SELECTOR +
          " (the construction of a DriverRunner)",
        resolved(cpg.method.fullName(DRIVER_RUNNER_SELECTOR).l)),
      Anchor(
        "process_launch",
        "user_named",
        "method full name matches " + PROCESS_LAUNCH_SELECTOR,
        resolved(cpg.method.fullName(PROCESS_LAUNCH_SELECTOR).l)),
      Anchor(
        "ExecutorRunner",
        "additional",
        "method full name matches " + EXECUTOR_RUNNER_SELECTOR + " (the construction of an " +
          "ExecutorRunner) — carried in addition to the three above and never in place of one",
        resolved(cpg.method.fullName(EXECUTOR_RUNNER_SELECTOR).l)))

    def renderAnchor(anchor: Anchor): String =
      jsonObject(Seq(
        "label"          -> jsonString(anchor.label),
        "kind"           -> jsonString(anchor.kind),
        "selector"       -> jsonString(anchor.selector),
        "resolved_count" -> jsonInt(anchor.methods.size),
        "resolved"       -> jsonStringArray(anchor.methods.map(_.fullName))))

    diagnostics.append(
      "handler_anchors" -> jsonBlockArray(handlerAnchors.map(renderAnchor), "    "))
    diagnostics.append(
      "sink_anchors" -> jsonBlockArray(sinkAnchors.map(renderAnchor), "    "))

    // -------------------------------------------------------------------------------------
    // (6) The traversal. Callee edges, plus the two bridges the header describes. Nothing is
    //     expanded through a derived predicate, which is what makes this the unguarded class.
    //     Two passes, as ONE RETURN PER ROUTE in the header sets out: a forward pass that
    //     observes the edge set, and a bounded backward pass that enumerates every distinct
    //     ordered route over it.
    // -------------------------------------------------------------------------------------
    stage = "traverse"

    val threadBridgeTypes       = scala.collection.mutable.TreeSet.empty[String]
    val threadBridgeConnections = scala.collection.mutable.TreeSet.empty[String]
    val pfBridgeTypes           = scala.collection.mutable.TreeSet.empty[String]
    val pfBridgeConnections     = scala.collection.mutable.TreeSet.empty[String]

    def ownerOf(method: Method): String = method.typeDecl.fullName.headOption.getOrElse("")

    def methodsOfType(typeFullName: String): List[Method] =
      resolved(cpg.typeDecl.fullNameExact(typeFullName).method.l)

    /**
     * Of the types owning the callees of some method, those whose full name is that method's own
     * enclosing type followed by `infix` and a number — i.e. the anonymous types that method
     * demonstrably allocates. Returned in full-name order.
     */
    def allocatedAnonymousTypes(
        owner: String,
        infix: String,
        calleeOwners: List[String]): List[String] = {
      val prefix = owner + infix
      calleeOwners
        .filter(t => t.startsWith(prefix) && t.stripPrefix(prefix).forall(_.isDigit))
        .sorted
    }

    def successorsOf(method: Method): List[(Method, String)] = {
      val owner        = ownerOf(method)
      val direct       = resolved(method.callee.l).map(callee => (callee, "call"))
      val calleeOwners = direct.map(entry => ownerOf(entry._1)).distinct

      // Bridge 1 — the thread boundary: the deferred body lives on the `run` of an anonymous
      // type this method allocates, and the runtime, not this method, invokes it.
      val threadTypes = allocatedAnonymousTypes(owner, "$$anon$", calleeOwners)
      threadTypes.foreach(threadBridgeTypes.add)
      val threadSuccessors = threadTypes.flatMap(t => methodsOfType(t).filter(_.name == "run"))
      threadSuccessors.foreach(r =>
        threadBridgeConnections.add(method.fullName + " ==> " + r.fullName))

      // Bridge 2 — the partial-function boundary at the entry-point end: the case bodies live
      // on the synthetic partial-function type this method allocates and which is named after
      // it, not in the method itself.
      val pfTypes = allocatedAnonymousTypes(owner, "$$anonfun$" + method.name + "$", calleeOwners)
      pfTypes.foreach(pfBridgeTypes.add)
      val pfTypeMethods = pfTypes.map(t => (t, methodsOfType(t)))
      pfTypeMethods.foreach { case (t, members) =>
        pfBridgeConnections.add(
          method.fullName + " ==> " + t + " [" + members.size + " methods]")
      }
      val pfSuccessors = pfTypeMethods.flatMap { case (_, members) => members }

      direct ++
        threadSuccessors.map(m => (m, "bridge_thread")) ++
        pfSuccessors.map(m => (m, "bridge_partialfunction"))
    }

    val handlerMethods = resolved(handlerAnchors.flatMap(_.methods))
    val sinkMethods    = resolved(sinkAnchors.flatMap(_.methods))
    val sinkFullNames  = sinkMethods.map(_.fullName).toSet

    /** One emitted return, before rendering: the four fields the contract fixes and no others. */
    final case class Emitted(
        handler: String,
        sink: String,
        path: List[String],
        predicates: List[String])

    // Graph reads that repeat across routes are memoized, so a method appearing on many routes
    // is read once. Both caches are keyed by method full name and hold graph-derived values
    // only, so memoizing cannot change an answer — only how often the graph is asked for it.
    val predicateCallCache = scala.collection.mutable.HashMap.empty[String, List[String]]
    def predicatesCalledBy(methodFullName: String): List[String] =
      predicateCallCache.getOrElseUpdate(
        methodFullName,
        cpg.method
          .fullNameExact(methodFullName)
          .l
          .flatMap(_.call.methodFullName.l)
          .filter(predicateSet.contains)
          .distinct
          .sorted)

    val traitForwarderCache = scala.collection.mutable.HashMap.empty[String, Boolean]
    def isTraitForwarder(methodFullName: String): Boolean =
      traitForwarderCache.getOrElseUpdate(
        methodFullName,
        cpg.method.fullNameExact(methodFullName).l.exists(_.name.endsWith("$")))

    val emitted                    = scala.collection.mutable.ListBuffer.empty[Emitted]
    var boundReached               = false
    var truncatedHandlers          = 0
    var methodsSeenSummed          = 0
    var edgesObservedSummed        = 0
    var longestRouteEdges          = -1
    var shortestRouteEdges         = -1
    var pairsEnumerated            = 0
    var routesEnumerated           = 0
    var returnsDiscardedByTotalCap = 0
    val pairsAtRouteCap            = scala.collection.mutable.TreeSet.empty[String]
    val pairsAtStepCap             = scala.collection.mutable.TreeSet.empty[String]
    val forwarderReturns           = scala.collection.mutable.TreeSet.empty[String]
    val threadBridgeOnPaths        = scala.collection.mutable.TreeSet.empty[String]
    val pfBridgeOnPaths            = scala.collection.mutable.TreeSet.empty[String]

    handlerMethods.foreach { handler =>
      // --- Forward pass. Each method is expanded at most once, and EVERY edge observed is
      //     recorded as a predecessor of its target — a set, not a single one, which is what
      //     keeps an alternate route and its predicate history from being discarded.
      val predecessorsOf =
        scala.collection.mutable.HashMap.empty[String, scala.collection.mutable.TreeSet[
          (String, String)]]
      val firstDepthOf = scala.collection.mutable.HashMap.empty[String, Int]
      val reachedSinks = scala.collection.mutable.TreeSet.empty[String]

      firstDepthOf.put(handler.fullName, 0)
      var frontier: List[Method] = List(handler)
      var depth                  = 0

      while (depth < MAX_CALL_DEPTH && frontier.nonEmpty) {
        val next = scala.collection.mutable.ListBuffer.empty[Method]
        frontier.foreach { current =>
          successorsOf(current).foreach { case (successor, edgeKind) =>
            val fullName = successor.fullName
            // A self edge can never lie on a simple route, so it is not recorded; recording it
            // would add a predecessor the backward pass must then reject on every route.
            if (fullName != current.fullName) {
              predecessorsOf
                .getOrElseUpdate(
                  fullName,
                  scala.collection.mutable.TreeSet.empty[(String, String)])
                .add((current.fullName, edgeKind))
            }
            if (!firstDepthOf.contains(fullName)) {
              firstDepthOf.put(fullName, depth + 1)
              if (sinkFullNames.contains(fullName)) reachedSinks.add(fullName)
              // The frontier expands only through the analysed code base, never through an
              // operator pseudo-method, and never through a derived predicate.
              val expandable =
                fullName.startsWith(EXPANSION_PREFIX) &&
                  !fullName.startsWith(OPERATOR_PREFIX) &&
                  !predicateSet.contains(fullName)
              if (expandable) next.append(successor)
            }
          }
        }
        depth += 1
        frontier = next.toList
      }

      methodsSeenSummed += firstDepthOf.size
      edgesObservedSummed += predecessorsOf.valuesIterator.map(_.size).sum
      if (depth >= MAX_CALL_DEPTH && frontier.nonEmpty) {
        boundReached = true
        truncatedHandlers += 1
      }

      /** The edge kinds recorded for one ordered pair, in a fixed order. */
      def edgeKindsBetween(from: String, to: String): List[String] =
        predecessorsOf
          .get(to)
          .map(_.toList.filter(_._1 == from).map(_._2).distinct.sorted)
          .getOrElse(Nil)

      // --- Backward pass, once per (entry point, sink) pair: every distinct simple ordered
      //     route over the observed edges, within the depth bound, in predecessor-name order.
      reachedSinks.toList.foreach { sinkFullName =>
        pairsEnumerated += 1
        val pairLabel = handler.fullName + " ==> " + sinkFullName
        val routes    = scala.collection.mutable.ListBuffer.empty[List[String]]
        var steps     = 0
        var stepCapHit = false

        // One route past the cap is deliberately searched for, so that "more routes exist than
        // the cap emits" is an exact fact rather than an inference from having reached the cap.
        val routeSearchLimit = MAX_ROUTES_PER_PAIR + 1

        /**
         * Extends one route suffix backwards. `suffix` runs from `node` (its head) to the sink
         * (its last element); `onSuffix` is the set of methods already on it, which keeps the
         * route simple and the recursion finite. A predecessor is followed only when the depth
         * bound still leaves it room to reach the entry point, which is what keeps the search
         * from walking the whole reachable set.
         */
        def extendRoute(node: String, suffix: List[String], onSuffix: Set[String]): Unit =
          if (routes.size < routeSearchLimit && !stepCapHit) {
            if (node == handler.fullName) {
              routes.append(suffix)
            } else {
              val edgesRemaining = MAX_CALL_DEPTH - (suffix.size - 1)
              val predecessors =
                predecessorsOf
                  .get(node)
                  .map(_.toList.map(_._1).distinct.sorted)
                  .getOrElse(Nil)
              predecessors.foreach { predecessor =>
                if (routes.size < routeSearchLimit && !stepCapHit) {
                  steps += 1
                  if (steps >= MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR) {
                    stepCapHit = true
                    pairsAtStepCap.add(pairLabel)
                  } else if (
                    !onSuffix.contains(predecessor) &&
                    edgesRemaining >= 1 &&
                    firstDepthOf.getOrElse(predecessor, Int.MaxValue) <= edgesRemaining - 1
                  ) {
                    extendRoute(predecessor, predecessor :: suffix, onSuffix + predecessor)
                  }
                }
              }
            }
          }

        extendRoute(sinkFullName, List(sinkFullName), Set(sinkFullName))

        val routesBeyondCapExist = routes.size > MAX_ROUTES_PER_PAIR
        if (routesBeyondCapExist) pairsAtRouteCap.add(pairLabel)
        val routesKept = routes.toList.take(MAX_ROUTES_PER_PAIR)
        routesEnumerated += routesKept.size

        routesKept.foreach { path =>
          // The second pass, computed over the emitted route as found: a path node that is
          // itself a predicate, plus the predicates each path node calls directly. A return is
          // emitted whatever this finds, and nothing is added to it that the graph does not
          // carry.
          val predicatesOnPath = path.flatMap { fullName =>
            val itself = if (predicateSet.contains(fullName)) List(fullName) else Nil
            itself ++ predicatesCalledBy(fullName)
          }

          // A Scala trait's static forwarder is linked to every implementation of the method it
          // forwards, so a route may hop from one endpoint to another through one. Such a return
          // is emitted as the call graph carries it and is listed here, not filtered out.
          if (path.exists(isTraitForwarder)) forwarderReturns.add(pairLabel)

          val routeEdges = path.size - 1
          if (routeEdges > longestRouteEdges) longestRouteEdges = routeEdges
          if (shortestRouteEdges < 0 || routeEdges < shortestRouteEdges) {
            shortestRouteEdges = routeEdges
          }

          path.zip(path.drop(1)).foreach { case (from, to) =>
            val kinds = edgeKindsBetween(from, to)
            if (kinds.contains("bridge_thread")) threadBridgeOnPaths.add(from + " ==> " + to)
            if (kinds.contains("bridge_partialfunction")) pfBridgeOnPaths.add(from + " ==> " + to)
          }

          if (emitted.size < MAX_RETURNS_TOTAL) {
            emitted.append(Emitted(handler.fullName, sinkFullName, path, predicatesOnPath))
          } else {
            returnsDiscardedByTotalCap += 1
          }
        }
      }
    }

    // One return per distinct EXACT tuple — entry point, sink, ordered path, predicates found on
    // it — never one per sink, and in a fixed order, so an unchanged source emits the same bytes.
    val distinctReturns = emitted.toList.distinct.sortBy(entry =>
      (entry.handler, entry.sink, entry.path.mkString("|"), entry.predicates.mkString("|")))
    distinctReturns.foreach { entry =>
      renderedReturns.append(
        jsonObject(Seq(
          "handler"            -> jsonString(entry.handler),
          "sink"               -> jsonString(entry.sink),
          "path"               -> jsonStringArray(entry.path),
          "predicates_on_path" -> jsonStringArray(entry.predicates))))
    }

    // -------------------------------------------------------------------------------------
    // (7) What the traversal could and could not express.
    // -------------------------------------------------------------------------------------
    stage = "record_diagnostics"

    def renderBridge(
        rule: String,
        matchedTypes: scala.collection.mutable.TreeSet[String],
        connections: scala.collection.mutable.TreeSet[String],
        onPaths: scala.collection.mutable.TreeSet[String]): String =
      jsonBlockObject(
        Seq(
          "rule"                         -> jsonString(rule),
          "fired"                        -> jsonBool(matchedTypes.nonEmpty),
          "succeeded"                    -> jsonBool(connections.nonEmpty),
          "matched_types"                -> jsonInt(matchedTypes.size),
          "distinct_connections"         -> jsonInt(connections.size),
          "needed_by_an_emitted_path"    -> jsonBool(onPaths.nonEmpty),
          "connections_on_emitted_paths" -> jsonStringArray(onPaths.toList),
          "connections"                  -> jsonStringArray(connections.toList)),
        "      ")

    diagnostics.append(
      "bridges" -> jsonBlockObject(
        Seq(
          "thread_boundary" -> renderBridge(
            "from a frontier method to the `run` of an anonymous type it allocates, whose full " +
              "name is the enclosing type followed by `$$anon$` and a number",
            threadBridgeTypes,
            threadBridgeConnections,
            threadBridgeOnPaths),
          "partialfunction_boundary" -> renderBridge(
            "from a frontier method to every method of the synthetic partial-function type it " +
              "allocates, whose full name is the enclosing type followed by `$$anonfun$`, the " +
              "method's own name and a number",
            pfBridgeTypes,
            pfBridgeConnections,
            pfBridgeOnPaths)),
        "    "))

    diagnostics.append(
      "route_enumeration" -> jsonBlockObject(
        Seq(
          "method" -> jsonString(
            "a forward pass per entry point that expands each method at most once and records " +
              "every edge it observes as a SET of predecessors per method, then a backward pass " +
              "per (entry point, sink) pair that enumerates every distinct simple ordered route " +
              "over those edges within the depth bound, in predecessor-name order"),
          "deduplication" -> jsonString(
            "on the exact emitted tuple — entry point, sink, ordered path, predicates found on " +
              "the path — and never on the sink method, so two different routes to one sink are " +
              "two returns"),
          "stated_scope" -> jsonString(
            "each method is expanded once, so the routes enumerated are those lying in the edge " +
              "set the bounded frontier observed; a route requiring a method to be expanded a " +
              "second time deeper than its first discovery is outside this pass"),
          "max_routes_per_pair"                   -> jsonInt(MAX_ROUTES_PER_PAIR),
          "max_enumeration_steps_per_pair"        -> jsonInt(MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR),
          "max_returns_total"                     -> jsonInt(MAX_RETURNS_TOTAL),
          "pairs_enumerated"                      -> jsonInt(pairsEnumerated),
          "routes_kept"                           -> jsonInt(routesEnumerated),
          "pairs_with_routes_beyond_the_cap"      -> jsonInt(pairsAtRouteCap.size),
          "pairs_with_routes_beyond_the_cap_list" -> jsonStringArray(pairsAtRouteCap.toList),
          "pairs_stopped_at_the_step_cap"         -> jsonInt(pairsAtStepCap.size),
          "pairs_stopped_at_the_step_cap_list"    -> jsonStringArray(pairsAtStepCap.toList),
          "returns_discarded_by_the_total_cap"    -> jsonInt(returnsDiscardedByTotalCap),
          "what_a_reached_bound_discarded" -> jsonString(
            "a pair listed against the route cap has at least one further route this run did " +
              "not emit — the search looks one route past the cap, so the flag is exact — and " +
              "the number of further routes is not enumerated. A pair listed against the step " +
              "cap stopped its backward search there, so further routes for it are unknown " +
              "rather than absent. The total cap discards whole returns, and the count above is " +
              "exactly how many")),
        "    "))

    diagnostics.append(
      "traversal" -> jsonBlockObject(
        Seq(
          "direction" -> jsonString(
            "forward over callee edges, one traversal per resolved entry point, then a bounded " +
              "backward enumeration of routes over the edges that traversal observed"),
          "max_call_depth" -> jsonInt(MAX_CALL_DEPTH),
          "bound_reached" -> jsonBool(boundReached),
          "entry_points_truncated_at_bound" -> jsonInt(truncatedHandlers),
          "entry_points_traversed" -> jsonInt(handlerMethods.size),
          "sink_methods_resolved" -> jsonInt(sinkMethods.size),
          "methods_seen_summed_over_entry_points" -> jsonInt(methodsSeenSummed),
          "edges_observed_summed_over_entry_points" -> jsonInt(edgesObservedSummed),
          "expansion_restriction" -> jsonString(
            "the frontier expands only through methods whose full name begins with `" +
              EXPANSION_PREFIX + "`; an operator pseudo-method and a derived predicate are " +
              "never expanded, so no emitted path has a predicate as an intermediate node. A " +
              "sink or a predicate is still recognised wherever it is reached, including " +
              "outside that prefix"),
          "path_selection" -> jsonString(
            "one return per distinct ordered route from an entry point to a sink, up to the " +
              "bounds in `route_enumeration`; predecessors are followed in full-name order and " +
              "returns are sorted, so the output is reproducible"),
          "predicate_check_reach" -> jsonString(
            "the emitted path nodes, plus one outgoing call step from each of them"),
          "returns_emitted"                  -> jsonInt(distinctReturns.size),
          "returns_removed_by_deduplication" -> jsonInt(emitted.size - distinctReturns.size),
          "returns_whose_emitted_path_carries_a_derived_predicate" ->
            jsonInt(distinctReturns.count(_.predicates.nonEmpty)),
          "longest_emitted_route_edge_count" ->
            (if (longestRouteEdges < 0) "null" else jsonInt(longestRouteEdges)),
          "shortest_emitted_route_edge_count" ->
            (if (shortestRouteEdges < 0) "null" else jsonInt(shortestRouteEdges)),
          "returns_whose_path_traverses_a_trait_default_method_forwarder" ->
            jsonInt(forwarderReturns.size),
          "returns_traversing_a_trait_default_method_forwarder" ->
            jsonStringArray(forwarderReturns.toList)),
        "    "))

    // -------------------------------------------------------------------------------------
    // (8) The result region: the BEGIN marker, one JSON object, the END marker, and nothing
    //     else between them. It is inside this block, and last, on purpose — the whole document
    //     is built as one string first, so the markers are printed only once a complete result
    //     exists, and any failure above leaves this unreached.
    // -------------------------------------------------------------------------------------
    stage = "emit_result"
    val document =
      "{\n" +
        "  " + jsonString("returns") + ": " + jsonBlockArray(renderedReturns.toList, "  ") +
        ",\n" +
        "  " + jsonString("diagnostics") + ": " + jsonBlockObject(diagnostics.toList, "  ") +
        "\n" +
        "}"

    println(MARKER_BEGIN)
    println(document)
    println(MARKER_END)

  } catch {
    case scala.util.control.NonFatal(failure) =>
      // No result region on a failure: the stage, the exception type and the message go to
      // stderr and the exception is re-raised, so the run exits non-zero with the start marker
      // printed and nothing that could be read as a result. A partial diagnostics buffer is
      // deliberately discarded rather than published as a success-shaped payload.
      reportFailureAndRaise(stage, failure)
  }
}
