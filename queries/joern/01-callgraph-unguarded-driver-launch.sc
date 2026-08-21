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
//  3. The load is idempotent. The workspace is shared with the other Phase 3 queries, so the
//     project may already exist; where it does, it is opened rather than imported again, which
//     avoids a duplicate project. Opening an existing project is still reading. Which of the
//     two happened is recorded in `diagnostics.load_mode`.
//
// RESULT CONTRACT
// ---------------
// One JSON object, printed strictly between `---BLITZY-RESULT-BEGIN---` and
// `---BLITZY-RESULT-END---`, with nothing else in that region — the driver slices it and parses
// it, so a single stray line there would be read as a runtime failure that did not happen. All
// graph work therefore completes before the BEGIN marker is printed. The object has exactly two
// top-level keys:
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
//   cpg_method_count    method count read from the loaded graph; evidence it loaded non-empty.
//   derived_predicates  the authentication/ACL predicate set, derived from the graph at
//                       execution time and never hardcoded, with every exclusion rule that
//                       fired and exactly what each removed.
//   handler_anchors     each entry-point anchor with the node count it resolved to and the
//                       names resolved — zero counts included.
//   sink_anchors        the same, per sink anchor, with each anchor's label and kind.
//   bridges             per boundary the traversal has to cross: whether the rule fired, what
//                       it connected, and whether an emitted path actually needed it.
//   traversal           the depth bound and whether it was reached, what the frontier was
//                       allowed to expand through, how a path is selected, and the reach of
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

// --- The three markers of the query-to-driver contract. ------------------------------------
val MARKER_START = "---BLITZY-START---"
val MARKER_BEGIN = "---BLITZY-RESULT-BEGIN---"
val MARKER_END   = "---BLITZY-RESULT-END---"

// --- Selectors. Every one is an anchored full-match regex over a graph property. -----------
val DEPLOY_TYPE_SELECTOR      = "org\\.apache\\.spark\\.deploy\\..*"
val SECURITY_MANAGER_SELECTOR = "org\\.apache\\.spark\\.SecurityManager"
val PREDICATE_NAME_SELECTOR   = "^(check.*Permissions|acls.*|isAuthenticationEnabled)$"
val SCALA_SETTER_SUFFIX       = "_$eq"

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
    // (3) Idempotent load. The workspace is shared with the other Phase 3 queries, so the
    //     project may already be present; opening it is reading, and it avoids a duplicate.
    // -------------------------------------------------------------------------------------
    stage = "load_graph"
    val projectName = CPG_PATH.split('/').last
    val loadMode =
      if (workspace.projects.exists(_.name == projectName)) {
        val opened = open(projectName)
        if (opened.isEmpty) {
          throw new RuntimeException(
            "project is present in the workspace but could not be opened: " + projectName)
        }
        "opened_existing_project"
      } else {
        importCpg(CPG_PATH)
        "imported_persisted_cpg"
      }

    val methodCount = cpg.method.size
    diagnostics.append("load_mode"        -> jsonString(loadMode))
    diagnostics.append("workspace"        -> jsonString(WORKSPACE_PATH))
    diagnostics.append("cpg_source"       -> jsonString(CPG_PATH))
    diagnostics.append("cpg_project_name" -> jsonString(projectName))
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
    // (5) Anchors. The entry points are selected by name AND by the full name of the type
    //     that encloses them, so an endpoint declared inside another type is reached too.
    //     Every anchor is reported with the node count it resolved to, zero counts included.
    // -------------------------------------------------------------------------------------
    stage = "resolve_anchors"

    case class Anchor(label: String, kind: String, selector: String, methods: List[Method])

    def resolved(methods: List[Method]): List[Method] =
      methods.distinctBy(_.fullName).sortBy(_.fullName)

    val handlerAnchors = List("receive", "receiveAndReply").map { handlerName =>
      Anchor(
        handlerName,
        "user_named",
        "method name is exactly `" + handlerName + "` and the enclosing type full name matches " +
          DEPLOY_TYPE_SELECTOR,
        resolved(
          cpg.method.nameExact(handlerName).where(_.typeDecl.fullName(DEPLOY_TYPE_SELECTOR)).l))
    }

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

    var boundReached          = false
    var truncatedHandlers     = 0
    var methodsSeenSummed     = 0
    var deepestSinkDepth      = -1
    val forwarderReturns      = scala.collection.mutable.TreeSet.empty[String]
    val threadBridgeOnPaths   = scala.collection.mutable.TreeSet.empty[String]
    val pfBridgeOnPaths       = scala.collection.mutable.TreeSet.empty[String]

    handlerMethods.foreach { handler =>
      val predecessor = scala.collection.mutable.HashMap.empty[String, (String, String)]
      val depthOf     = scala.collection.mutable.HashMap.empty[String, Int]
      val reached     = scala.collection.mutable.ListBuffer.empty[Method]

      depthOf.put(handler.fullName, 0)
      var frontier: List[Method] = List(handler)
      var depth                  = 0

      while (depth < MAX_CALL_DEPTH && frontier.nonEmpty) {
        val next = scala.collection.mutable.ListBuffer.empty[Method]
        frontier.foreach { current =>
          successorsOf(current).foreach { case (successor, edgeKind) =>
            val fullName = successor.fullName
            if (!depthOf.contains(fullName)) {
              depthOf.put(fullName, depth + 1)
              predecessor.put(fullName, (current.fullName, edgeKind))
              if (sinkFullNames.contains(fullName)) reached.append(successor)
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

      methodsSeenSummed += depthOf.size
      if (depth >= MAX_CALL_DEPTH && frontier.nonEmpty) {
        boundReached = true
        truncatedHandlers += 1
      }

      resolved(reached.toList).foreach { sink =>
        // Reconstruct the ordered path, handler first and sink last, together with the kind of
        // each edge so a bridge an emitted path depended on can be reported as such.
        var path   = List(sink.fullName)
        var edges  = List.empty[(String, String, String)]
        var cursor = sink.fullName
        while (predecessor.contains(cursor)) {
          val (parent, edgeKind) = predecessor(cursor)
          edges = (parent, cursor, edgeKind) :: edges
          path = parent :: path
          cursor = parent
        }

        // The second pass, computed over the emitted path as found: a path node that is itself
        // a predicate, plus the predicates each path node calls directly. A return is emitted
        // whatever this finds, and nothing is added to it that the graph does not carry.
        val pathNodes = path.map(fullName => (fullName, cpg.method.fullNameExact(fullName).l))
        val predicatesOnPath = pathNodes.flatMap { case (fullName, nodes) =>
          val itself = if (predicateSet.contains(fullName)) List(fullName) else Nil
          val called =
            nodes.flatMap(_.call.methodFullName.l).filter(predicateSet.contains).distinct.sorted
          itself ++ called
        }

        // A Scala trait's static forwarder is linked to every implementation of the method it
        // forwards, so a path may hop from one endpoint to another through one. Such a return is
        // emitted as the call graph carries it and is listed here, not filtered out.
        if (pathNodes.exists { case (_, nodes) => nodes.exists(_.name.endsWith("$")) }) {
          forwarderReturns.add(handler.fullName + " ==> " + sink.fullName)
        }
        if (depthOf(sink.fullName) > deepestSinkDepth) deepestSinkDepth = depthOf(sink.fullName)
        edges.foreach {
          case (from, to, "bridge_thread")         => threadBridgeOnPaths.add(from + " ==> " + to)
          case (from, to, "bridge_partialfunction") => pfBridgeOnPaths.add(from + " ==> " + to)
          case _                                    => ()
        }

        renderedReturns.append(
          jsonObject(Seq(
            "handler"            -> jsonString(handler.fullName),
            "sink"               -> jsonString(sink.fullName),
            "path"               -> jsonStringArray(path),
            "predicates_on_path" -> jsonStringArray(predicatesOnPath))))
      }
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
      "traversal" -> jsonBlockObject(
        Seq(
          "direction" -> jsonString(
            "forward over callee edges, one traversal per resolved entry point"),
          "max_call_depth" -> jsonInt(MAX_CALL_DEPTH),
          "bound_reached" -> jsonBool(boundReached),
          "entry_points_truncated_at_bound" -> jsonInt(truncatedHandlers),
          "entry_points_traversed" -> jsonInt(handlerMethods.size),
          "sink_methods_resolved" -> jsonInt(sinkMethods.size),
          "methods_seen_summed_over_entry_points" -> jsonInt(methodsSeenSummed),
          "expansion_restriction" -> jsonString(
            "the frontier expands only through methods whose full name begins with `" +
              EXPANSION_PREFIX + "`; an operator pseudo-method and a derived predicate are " +
              "never expanded, so no emitted path has a predicate as an intermediate node. A " +
              "sink or a predicate is still recognised wherever it is reached, including " +
              "outside that prefix"),
          "path_selection" -> jsonString(
            "one return per (entry point, sink) pair, whose path is the breadth-first discovery " +
              "path; successors are visited in full-name order, so the path is reproducible"),
          "predicate_check_reach" -> jsonString(
            "the emitted path nodes, plus one outgoing call step from each of them"),
          "deepest_emitted_sink_depth" ->
            (if (deepestSinkDepth < 0) "null" else jsonInt(deepestSinkDepth)),
          "returns_whose_path_traverses_a_trait_default_method_forwarder" ->
            jsonInt(forwarderReturns.size),
          "returns_traversing_a_trait_default_method_forwarder" ->
            jsonStringArray(forwarderReturns.toList)),
        "    "))

  } catch {
    case scala.util.control.NonFatal(failure) =>
      // A failure is recorded against the stage that produced it and the result region is still
      // emitted, so the driver reads a parseable result rather than an unexplained silence.
      diagnostics.append(
        "error" -> jsonObject(Seq(
          "stage"   -> jsonString(stage),
          "type"    -> jsonString(failure.getClass.getName),
          "message" -> jsonString(Option(failure.getMessage).getOrElse("")))))
  }

  // (8) The result region: the BEGIN marker, one JSON object, the END marker, and nothing else
  //     between them. Every graph read above has already completed.
  val document =
    "{\n" +
      "  " + jsonString("returns") + ": " + jsonBlockArray(renderedReturns.toList, "  ") + ",\n" +
      "  " + jsonString("diagnostics") + ": " + jsonBlockObject(diagnostics.toList, "  ") + "\n" +
      "}"

  println(MARKER_BEGIN)
  println(document)
  println(MARKER_END)
}
