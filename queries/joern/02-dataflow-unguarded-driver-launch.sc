// queries/joern/02-dataflow-unguarded-driver-launch.sc
// ===========================================================================================
// Phase 3 capability probe — query 02 of three, the DATA-FLOW formulation.
//
// WHAT THIS QUERY ATTEMPTS
// ------------------------
// One reachability class, expressed over data flow and over nothing else:
//
//     an RPC entry point named `receive` or `receiveAndReply`, enclosed in a type whose full
//     name lies under `org.apache.spark.deploy.`, whose driver-submission message reaches a
//     privileged sink's command- or jar-bearing argument — `createDriver`, a `DriverRunner`
//     construction, or a process launch — along a flow on which no derived authentication or
//     ACL predicate appears.
//
// Those three sinks are the three the probe's worked example names, and they are labelled with
// that vocabulary below. A fourth anchor, an `ExecutorRunner` construction, is carried
// separately and is labelled `additional`; it never substitutes for one of the three.
//
// This is the whole class, not a component of it. The other two committed queries attempt the
// same whole class by two other formulations — the call graph, and a parameterized form. A class
// can be expressible in one view of a graph and not another, so no single query's outcome
// settles the question this probe asks. This script therefore draws no conclusion, orders
// nothing, and reconciles its outcome against no other formulation's: it emits what its flow
// queries found and, equally, what they could not express, in `diagnostics`.
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
//     joern --script queries/joern/02-dataflow-unguarded-driver-launch.sc
//
// Exactly that, run from the directory that contains `harness/` — the two paths below are
// relative to it. `exec` takes no parameters, so no `--param` is required or accepted, no
// environment variable is read, and nothing is prompted for: a reader can run the line above
// by hand and get precisely what the Phase 3 driver gets.
//
// No time limit is imposed anywhere in this script. Engaging the data-flow layer and answering
// a reachability question over a graph this size can take a long time; the query is never
// narrowed, bounded in wall-clock terms, or abandoned to make it finish sooner.
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
//  3. The load is idempotent. The workspace is shared with the other Phase 3 queries and with
//     the environment gate's own coverage check, so by the time this script runs the project
//     will very likely already exist; where it does, it is opened rather than imported again,
//     which avoids a duplicate project. Opening an existing project is still reading. Which of
//     the two happened is recorded in `diagnostics.load_mode`.
//  4. The data-flow layer is engaged AFTER the load, because it operates on the loaded graph.
//     Its outcome is read back from the graph it returns rather than assumed.
//
// RESULT CONTRACT
// ---------------
// One JSON object, printed strictly between `---BLITZY-RESULT-BEGIN---` and
// `---BLITZY-RESULT-END---`, with nothing else in that region — the driver slices it and parses
// it, so a single stray line there would be read as a runtime failure that did not happen. All
// graph work therefore completes before the BEGIN marker is printed, which also keeps the
// data-flow engine's own progress and warning output well clear of the region. The object has
// exactly two top-level keys:
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
// what the flow queries resolved and where they stopped, because a recorded limit is part of
// the answer this probe produces, and a query that returns nothing while recording why it
// returned nothing is a result, whereas one that returns nothing silently is a defect. Its
// keys:
//
//   load_mode           whether the project was imported or opened (see above).
//   cpg_method_count    method count read from the loaded graph; evidence it loaded non-empty.
//   dataflow_layer      the layer command that was run, the overlay state read from the graph
//                       before and after running it, the reachability step used, and the call
//                       depth the engine defaults to beside the depth this script configures.
//   derived_predicates  the authentication/ACL predicate set, derived from the graph at
//                       execution time and never hardcoded, with every exclusion rule that
//                       fired and exactly what each removed.
//   handler_anchors     each entry-point anchor with the node count it resolved to and the
//                       names resolved — zero counts included.
//   source_nodes        what was resolved as the driver-submission message, per entry point,
//                       by which of the two rules, and which entry points yielded none.
//   sink_anchors        each sink anchor with its label and kind, the argument-selection rule
//                       that fired per sink method, the node counts — zeros included — the
//                       flow counts at both call depths, and, where an anchor returned no
//                       flow, the expression each of its sink nodes arrives through.
//   bridges             per boundary this formulation has to deal with: whether it resolved,
//                       whether it was applied, what it connected, and whether an emitted path
//                       needed it.
//   traversal           the call-depth bound and what is known about reaching it, how a path is
//                       composed, how returns are selected, and the reach of both the flow
//                       filter and the separate predicate check.
//
// Output is deterministic: every collection printed is sorted or built in a fixed order, and
// returns are de-duplicated and sorted, so re-running an unchanged source produces
// byte-identical output and a diff between revisions means something. The one field that
// legitimately differs between a run against a fresh workspace and a run against a warm one is
// `load_mode`, whose whole purpose is to record that difference. Every value emitted is read
// from the graph; nothing is estimated, inferred or filled in, and no line number of any source
// file appears in this script or in anything it prints.
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
// Transport-level switches are excluded by that construction rather than by exception: a switch
// governing encryption or SSL is not a check on the identity of a caller, and it matches none
// of the three name patterns. The same holds for a private helper of a matching method, and for
// the channel-setup constructors of an authentication or SASL bootstrap.
//
// THE TWO BOUNDARIES THIS FORMULATION HAS TO DEAL WITH
// ---------------------------------------------------
// Over a JVM-bytecode frontend, two properties of Scala compilation — not of the code being
// analysed — stand between an entry point and a sink, and a data-flow formulation meets them in
// different places from a call-graph one:
//
//   * At the entry-point end the boundary bears on SOURCE SELECTION, not on traversal. A handler
//     returning a partial function does not contain its own case bodies: it allocates a
//     synthetic partial-function class and the bodies live on that class. The named handler
//     therefore carries no message parameter at all, so selecting "the handler's parameter"
//     naively resolves the wrong node or none, and the flow query then returns empty for a
//     reason that has nothing to do with the code under analysis. The message is resolved
//     inside the synthetic body instead — the parameter of the body the handler's own partial
//     function class declares — and, where the handler is not a partial function and carries a
//     message parameter directly, that parameter is used and no bridge is needed. Which of the
//     two applied is recorded per entry point.
//   * At the sink end a method that hands work to a thread does not call the work: it allocates
//     an anonymous class and the runtime invokes its `run`. A data-flow relation follows data
//     dependence, and an allocation-to-runtime handoff is not one, so a flow does not continue
//     past it. This is why the class names three alternative sinks rather than requiring the
//     deepest: reaching the process launch is not required for the class to be attempted, and
//     this formulation anchors at the sinks above the boundary. The boundary itself is resolved
//     from the graph and recorded, so the limit is evidence rather than an assertion.
//
// THE FLOW FILTER AND `predicates_on_path` ARE TWO DIFFERENT THINGS
// ----------------------------------------------------------------
// A flow is discarded when the query's own predicate check finds a derived predicate on the
// flow itself — an element that is a call to a predicate, or an element sitting inside one.
// That filter is what makes this a query for the unguarded class, and it is why no predicate
// appears among the flow elements of an emitted return.
//
// `predicates_on_path` is then computed separately over the emitted path, as found, and its
// reach is wider in two ways: the path carries the named entry point and the sink method, which
// no flow element covered, and the check looks at each path node plus one outgoing call step
// from it. So the second pass can find what the filter did not, and where it does the return is
// emitted anyway with `predicates_on_path` populated — a return is never dropped for carrying a
// predicate, and a predicate is never added to make one look as though it does. Whether a
// predicate lies on a path is a property of this formulation and of the path it emitted: a flow
// that ends at a construction does not traverse what a flow continuing past it traverses, and
// both are correct answers about their own path.
// ===========================================================================================

import io.shiftleft.codepropertygraph.generated.nodes.{AstNode, Call, CfgNode, Expression, Method}
import io.joern.dataflowengineoss.queryengine.EngineContext

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

// --- Source selection: the driver-submission message. --------------------------------------
// A partial function's case bodies live on the synthetic class the handler allocates, whose
// full name is the handler's own enclosing type followed by this infix, the handler's name and
// a number. The message is the parameter at this index of the body that class declares, whose
// erased type over a bytecode frontend is the universal object type.
val PARTIAL_FUNCTION_INFIX     = "$$anonfun$"
val PARTIAL_FUNCTION_BODY_NAME = "applyOrElse"
val MESSAGE_PARAMETER_INDEX    = 1
val MESSAGE_PARAMETER_TYPE     = "java.lang.Object"

// A destructured driver-submission field is a read of the driver description off the message,
// so the accessors are derived from the graph: a method on a deploy-message type whose return
// type is the driver description. A default-argument supplier carries that same return type
// without being a field read, and is excluded by its name prefix.
val DEPLOY_MESSAGE_SELECTOR = "org\\.apache\\.spark\\.deploy\\.DeployMessages\\$.*"
val DRIVER_DESCRIPTION_TYPE = "org.apache.spark.deploy.DriverDescription"
val DEFAULT_ARGUMENT_PREFIX = "copy$default$"

// --- Sink selection: the command- or jar-bearing argument. ---------------------------------
// A sink's command or jar argument is identified from the sink method's own formal parameters:
// by parameter name, or by a parameter type that carries a command or a jar. Where a sink
// declares no such parameter — a launch performed on an object that already carries the
// command — the receiver of the call is the sink node instead.
val COMMAND_BEARING_NAME_SELECTOR = "(?i).*(command|jar|desc).*"
val COMMAND_BEARING_TYPE_SELECTOR =
  "org\\.apache\\.spark\\.deploy\\.(ApplicationDescription|Command|DriverDescription)"
val RECEIVER_ARGUMENT_INDEX = 0

// --- The thread boundary, and the operator whose left side names a definition. --------------
val THREAD_ANONYMOUS_INFIX = "$$anon$"
val THREAD_BODY_NAME       = "run"
val ASSIGNMENT_OPERATOR    = "<operator>.assignment"

// --- Call-depth bound. ---------------------------------------------------------------------
// The engine's own default is read from it at execution time and recorded beside this value.
// This bound is set well above that default so that the chain running below the construction
// sinks is not excluded by the bound itself; what is known about reaching it is recorded rather
// than passed over in silence, and each anchor is also answered at the engine default so the
// bound's effect on the outcome is measured rather than assumed.
val MAX_CALL_DEPTH = 12

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
    // (3) Idempotent load. The workspace is shared with the other Phase 3 queries and with
    //     the gate's coverage check, so the project is likely to be present already; opening
    //     it is reading, and it avoids a duplicate.
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
    // (4) The open-source data-flow layer, engaged after the load because it operates on the
    //     loaded graph. Its outcome is read back from the graph it returns — the overlay set
    //     before and after — rather than assumed from the call having returned. The engine's
    //     own default call depth is read from it and a deeper context is derived from it, so
    //     the bound this script uses is explicit and the default it replaces is on the record.
    // -------------------------------------------------------------------------------------
    stage = "engage_dataflow_layer"
    val overlaysBeforeLayer = cpg.metaData.overlays.l.distinct.sorted
    val dataflowCpg         = run.ossdataflow
    val overlaysAfterLayer  = dataflowCpg.metaData.overlays.l.distinct.sorted
    val ambientContext      = summon[EngineContext]
    val engineDefaultDepth  = ambientContext.config.maxCallDepth
    val deepContext = EngineContext(
      ambientContext.semantics, ambientContext.config.copy(maxCallDepth = MAX_CALL_DEPTH))

    diagnostics.append(
      "dataflow_layer" -> jsonBlockObject(
        Seq(
          "command"                  -> jsonString("run.ossdataflow"),
          "engaged"                  -> jsonBool(true),
          "overlays_before_engaging" -> jsonStringArray(overlaysBeforeLayer),
          "overlays_after_engaging"  -> jsonStringArray(overlaysAfterLayer),
          "outcome" -> jsonString(
            if (overlaysBeforeLayer == overlaysAfterLayer)
              "the layer ran and the overlay set read from the graph is unchanged, so the " +
                "persisted graph already carried it and nothing was added"
            else
              "the layer ran and added the overlays that appear in the after set but not the " +
                "before set"),
          "reachability_step"                -> jsonString("sink.reachableByFlows(source)"),
          "engine_default_max_call_depth"    -> jsonInt(engineDefaultDepth),
          "configured_max_call_depth"        -> jsonInt(MAX_CALL_DEPTH),
          "time_limit_imposed_by_this_script" -> jsonBool(false)),
        "    "))

    // -------------------------------------------------------------------------------------
    // (5) The authentication / ACL predicate set, derived from the graph — never hardcoded.
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
                "rule"    -> jsonString("scala_setter_suffix"),
                "applied" -> jsonBool(true),
                "removed" -> jsonStringArray(removedBySetterRule))),
              jsonObject(Seq(
                "rule"    -> jsonString("field_member_name_collision"),
                "applied" -> jsonBool(memberRuleApplicable),
                "removed" -> jsonStringArray(removedByMemberRule))),
              jsonObject(Seq(
                "rule"    -> jsonString("field_accessor_setter_evidence"),
                "applied" -> jsonBool(accessorRuleApplicable),
                "removed" -> jsonStringArray(removedByAccessorRule)))),
            "      "),
          "resolved" -> jsonStringArray(predicateFullNames),
          "count"    -> jsonInt(predicateFullNames.size)),
        "    "))


    // -------------------------------------------------------------------------------------
    // (6) Entry-point anchors. Selected by name AND by the full name of the type that encloses
    //     them, so an endpoint declared inside another type is reached too — a selector keyed
    //     to outer classes alone would silently miss the endpoints that sit on inner classes.
    //     Every anchor is reported with the node count it resolved to, zero counts included.
    // -------------------------------------------------------------------------------------
    stage = "resolve_handler_anchors"

    def resolved(methods: List[Method]): List[Method] =
      methods.distinctBy(_.fullName).sortBy(_.fullName)

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

    final case class Anchor(label: String, kind: String, selector: String, methods: List[Method])

    def renderAnchor(anchor: Anchor): String =
      jsonObject(Seq(
        "label"          -> jsonString(anchor.label),
        "kind"           -> jsonString(anchor.kind),
        "selector"       -> jsonString(anchor.selector),
        "resolved_count" -> jsonInt(anchor.methods.size),
        "resolved"       -> jsonStringArray(anchor.methods.map(_.fullName))))

    val handlerAnchors = List("receive", "receiveAndReply").map { handlerName =>
      Anchor(
        handlerName,
        "user_named",
        "method name is exactly `" + handlerName + "` and the enclosing type full name matches " +
          DEPLOY_TYPE_SELECTOR,
        resolved(
          cpg.method.nameExact(handlerName).where(_.typeDecl.fullName(DEPLOY_TYPE_SELECTOR)).l))
    }

    diagnostics.append(
      "handler_anchors" -> jsonBlockArray(handlerAnchors.map(renderAnchor), "    "))

    val handlerMethods = resolved(handlerAnchors.flatMap(_.methods))

    // -------------------------------------------------------------------------------------
    // (7) Source nodes: the driver-submission message, per entry point. This is where the
    //     partial-function boundary bites, so both shapes are resolved from the graph rather
    //     than assumed, and which one applied is recorded per entry point.
    //
    //     Rule A — the entry point declares a message parameter itself. That happens where the
    //              handler is not a partial function, and no bridge is needed.
    //     Rule B — the entry point allocates a synthetic partial-function class and the message
    //              is the parameter of the body that class declares. This is the bridge.
    //     Rule C — in addition, a read of the driver description off the message inside such a
    //              body is carried as a source, because the destructured field is the value the
    //              class is about and a flow may begin at the read rather than at the parameter.
    // -------------------------------------------------------------------------------------
    stage = "resolve_source_nodes"

    val messageAccessors = cpg.method
      .where(_.typeDecl.fullName(DEPLOY_MESSAGE_SELECTOR))
      .l
      .filter(_.methodReturn.typeFullName == DRIVER_DESCRIPTION_TYPE)
      .filterNot(_.name.startsWith(DEFAULT_ARGUMENT_PREFIX))
      .map(_.fullName)
      .distinct
      .sorted
    val messageAccessorSet = messageAccessors.toSet

    val sourceNodeHandler   = scala.collection.mutable.HashMap.empty[Long, String]
    val sourceNodes         = scala.collection.mutable.ListBuffer.empty[CfgNode]
    val partialFunctionTypes = scala.collection.mutable.TreeSet.empty[String]
    val partialFunctionConnections = scala.collection.mutable.TreeSet.empty[String]
    val partialFunctionBodyNames   = scala.collection.mutable.TreeSet.empty[String]
    val renderedSources = scala.collection.mutable.ListBuffer.empty[String]
    val handlersWithoutSource = scala.collection.mutable.TreeSet.empty[String]

    handlerMethods.foreach { handler =>
      // Rule A — a message parameter declared by the entry point itself.
      val directParameters = handler.parameter
        .index(MESSAGE_PARAMETER_INDEX)
        .l
        .filter(_.typeFullName == MESSAGE_PARAMETER_TYPE)

      // Rule B — the synthetic partial-function class this entry point allocates, and the body
      //          it declares. The body is selected by name; where the frontend names it
      //          otherwise, the fallback selects, on the same class, a method whose parameter at
      //          the message index carries the erased message type. Which applied is recorded.
      val calleeOwners = handler.callee.l.map(ownerOf).distinct
      val partialFunctionTypesHere = allocatedAnonymousTypes(
        ownerOf(handler), PARTIAL_FUNCTION_INFIX + handler.name + "$", calleeOwners)
      partialFunctionTypesHere.foreach(partialFunctionTypes.add)

      val bodiesByName = partialFunctionTypesHere.flatMap(t =>
        methodsOfType(t).filter(_.name == PARTIAL_FUNCTION_BODY_NAME))
      val bodyRule = if (bodiesByName.nonEmpty) "named_body" else "erased_message_parameter"
      val bodies =
        if (bodiesByName.nonEmpty) bodiesByName
        else
          partialFunctionTypesHere.flatMap(t =>
            methodsOfType(t).filter(m =>
              m.parameter
                .index(MESSAGE_PARAMETER_INDEX)
                .l
                .exists(_.typeFullName == MESSAGE_PARAMETER_TYPE)))
      bodies.foreach { body =>
        partialFunctionBodyNames.add(body.fullName)
        partialFunctionConnections.add(handler.fullName + " ==> " + body.fullName)
      }

      val bridgedParameters = bodies.flatMap(
        _.parameter
          .index(MESSAGE_PARAMETER_INDEX)
          .l
          .filter(_.typeFullName == MESSAGE_PARAMETER_TYPE))

      // Rule C — a read of the driver description off the message, inside such a body.
      val destructuredReads =
        bodies.flatMap(_.call.l).filter(c => messageAccessorSet.contains(c.methodFullName))

      val here: List[CfgNode] =
        directParameters.map(node => node: CfgNode) ++
          bridgedParameters.map(node => node: CfgNode) ++
          destructuredReads.map(node => node: CfgNode)

      here.foreach { node =>
        sourceNodeHandler.put(node.id, handler.fullName)
        sourceNodes.append(node)
      }
      if (here.isEmpty) handlersWithoutSource.add(handler.fullName)

      renderedSources.append(
        jsonObject(Seq(
          "handler"                     -> jsonString(handler.fullName),
          "direct_message_parameters"   -> jsonInt(directParameters.size),
          "partial_function_types"      -> jsonStringArray(partialFunctionTypesHere),
          "partial_function_body_rule"  -> jsonString(bodyRule),
          "partial_function_bodies"     -> jsonStringArray(bodies.map(_.fullName).distinct.sorted),
          "bridged_message_parameters"  -> jsonInt(bridgedParameters.size),
          "destructured_field_reads"    -> jsonStringArray(
            destructuredReads.map(_.methodFullName).distinct.sorted),
          "bridge_needed"               -> jsonBool(directParameters.isEmpty),
          "bridge_succeeded"            -> jsonBool(bridgedParameters.nonEmpty),
          "resolved_source_nodes"       -> jsonInt(here.size))))
    }

    val sourceNodeList = sourceNodes.toList

    diagnostics.append(
      "source_nodes" -> jsonBlockObject(
        Seq(
          "rule_a_direct_message_parameter" -> jsonString(
            "the parameter at the message index of the entry point itself, where its type is " +
              "the erased message type — used where the entry point is not a partial function"),
          "rule_b_partial_function_body_parameter" -> jsonString(
            "the parameter at the message index of the body declared by the synthetic " +
              "partial-function type the entry point allocates, whose full name is the entry " +
              "point's enclosing type followed by `" + PARTIAL_FUNCTION_INFIX + "`, the entry " +
              "point's own name and a number — the named entry point declares no message " +
              "parameter, so this is the bridge that makes a source resolvable at all"),
          "rule_c_destructured_driver_submission_field" -> jsonString(
            "a call, inside such a body, to an accessor on a deploy-message type whose return " +
              "type is `" + DRIVER_DESCRIPTION_TYPE + "`, excluding a default-argument " +
              "supplier by its `" + DEFAULT_ARGUMENT_PREFIX + "` name prefix"),
          "message_parameter_index"     -> jsonInt(MESSAGE_PARAMETER_INDEX),
          "message_parameter_type"      -> jsonString(MESSAGE_PARAMETER_TYPE),
          "message_accessor_selector"   -> jsonString(
            "enclosing type matches " + DEPLOY_MESSAGE_SELECTOR + " and return type is " +
              DRIVER_DESCRIPTION_TYPE),
          "resolved_message_accessors"  -> jsonStringArray(messageAccessors),
          "per_handler"                 -> jsonBlockArray(renderedSources.toList, "      "),
          "total_resolved_source_nodes" -> jsonInt(sourceNodeList.size),
          "handlers_with_no_resolved_source" -> jsonStringArray(handlersWithoutSource.toList)),
        "    "))


    // -------------------------------------------------------------------------------------
    // (8) Sink anchors, and the command- or jar-bearing argument of each. The three the class
    //     names are labelled `user_named`; the fourth is labelled `additional` and never
    //     substitutes for one of them. Every anchor is reported with the node count it resolved
    //     to, zero counts included.
    // -------------------------------------------------------------------------------------
    stage = "resolve_sink_anchors"

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

    /**
     * The parameter indices of a sink method that carry a command or a jar, by parameter name or
     * by parameter type. The receiver index is never among them: a receiver is selected by the
     * fallback rule below, not by this one.
     */
    def commandBearingIndices(sinkMethod: Method): List[Int] =
      sinkMethod.parameter.l
        .filter(_.index > RECEIVER_ARGUMENT_INDEX)
        .filter(parameter =>
          parameter.name.matches(COMMAND_BEARING_NAME_SELECTOR) ||
            parameter.typeFullName.matches(COMMAND_BEARING_TYPE_SELECTOR))
        .map(_.index)
        .distinct
        .sorted

    /**
     * The expression a definition of `code` inside `methodFullName` arrives through, read off the
     * assignments in that method. Recorded for a sink node no flow reached, so that "no flow" is
     * accompanied by the expression the value arrives through rather than left unexplained.
     */
    def definingExpressions(methodFullName: String, code: String): List[String] =
      cpg.method
        .fullNameExact(methodFullName)
        .ast
        .isCall
        .nameExact(ASSIGNMENT_OPERATOR)
        .l
        .filter(_.argument.argumentIndex(1).code.l.contains(code))
        .map(_.code)
        .distinct
        .sorted

    // -------------------------------------------------------------------------------------
    // (9) The flow queries. One per sink anchor, over the whole source set, because the work
    //     the engine does is the backward exploration from that anchor's sink nodes and is
    //     shared across sources — asking once per anchor rather than once per pair changes how
    //     the same question is asked, never which question. Each flow is attributed back to the
    //     entry point its source belongs to and the sink method its terminal node feeds, by
    //     node identity, so a flow is never credited to the wrong end.
    //
    //     Each anchor is answered twice: at the configured bound, which produces the returns,
    //     and at the engine's own default bound, which is recorded beside it so the bound's
    //     effect on the outcome is measured rather than asserted.
    // -------------------------------------------------------------------------------------
    stage = "resolve_flows"

    final case class Emitted(
        handler: String,
        sink: String,
        path: List[String],
        predicates: List[String])

    val sinkNodeMethod       = scala.collection.mutable.HashMap.empty[Long, String]
    val emitted              = scala.collection.mutable.ListBuffer.empty[Emitted]
    val renderedSinkAnchors  = scala.collection.mutable.ListBuffer.empty[String]
    var flowsAtConfigured    = 0
    var flowsAtDefault       = 0
    var flowsFiltered        = 0
    var flowsUnattributed    = 0
    var deepestFlowMethods   = 0
    var boundChangedOutcome  = false

    /** The derived predicates the flow itself carries: a call to one, or an element inside one. */
    def predicatesOnFlow(elements: List[AstNode]): List[String] = {
      val insidePredicate = elements.collect { case node: CfgNode => node.method.fullName }
      val callsPredicate  = elements.collect { case node: Call => node.methodFullName }
      (insidePredicate ++ callsPredicate).filter(predicateSet.contains).distinct.sorted
    }

    /**
     * The emitted path: the named entry point first, then the enclosing methods of the flow's
     * elements in flow order — which begins in the synthetic partial-function body, the bridge
     * the header describes — then the sink method last. An occurrence of either end in the
     * middle is dropped so that the ordering the contract requires holds exactly.
     */
    def composePath(handler: String, sink: String, elements: List[AstNode]): List[String] = {
      val flowMethods = elements.collect { case node: CfgNode => node.method.fullName }.distinct
      handler :: (flowMethods.filterNot(m => m == handler || m == sink) ++ List(sink))
    }

    sinkAnchors.foreach { anchor =>
      val renderedSelections = scala.collection.mutable.ListBuffer.empty[String]
      var anchorSinkNodes    = List.empty[Expression]

      anchor.methods.foreach { sinkMethod =>
        val bearingIndices = commandBearingIndices(sinkMethod)
        val selectionRule =
          if (bearingIndices.nonEmpty) "command_or_jar_bearing_formal_parameter"
          else "receiver_of_the_launch"
        val selectedIndices =
          if (bearingIndices.nonEmpty) bearingIndices else List(RECEIVER_ARGUMENT_INDEX)
        val callSites = cpg.call.methodFullNameExact(sinkMethod.fullName).l
        val selected = callSites.flatMap(callSite =>
          callSite.argument.l.filter(argument => selectedIndices.contains(argument.argumentIndex)))
        selected.foreach(node => sinkNodeMethod.put(node.id, sinkMethod.fullName))
        anchorSinkNodes = anchorSinkNodes ++ selected

        renderedSelections.append(
          jsonObject(Seq(
            "sink_method"               -> jsonString(sinkMethod.fullName),
            "argument_selection_rule"   -> jsonString(selectionRule),
            "selected_argument_indices" -> jsonStringArray(selectedIndices.map(_.toString)),
            "selected_parameters" -> jsonStringArray(
              sinkMethod.parameter.l
                .filter(parameter => selectedIndices.contains(parameter.index))
                .sortBy(_.index)
                .map(parameter =>
                  parameter.index + ":" + parameter.name + ":" + parameter.typeFullName)),
            "call_sites"      -> jsonInt(callSites.size),
            "sink_node_count" -> jsonInt(selected.size))))
      }

      // The configured bound produces the returns; the engine default is asked afterwards and
      // recorded beside it. Both iterate fresh over the same materialized node lists, because a
      // traversal is consumed by the query that reads it.
      val configuredFlows =
        anchorSinkNodes.iterator.reachableByFlows(sourceNodeList.iterator)(using deepContext).l
      val defaultFlows =
        anchorSinkNodes.iterator.reachableByFlows(sourceNodeList.iterator)(using ambientContext).l

      flowsAtConfigured += configuredFlows.size
      flowsAtDefault += defaultFlows.size
      if (configuredFlows.size != defaultFlows.size) boundChangedOutcome = true

      var anchorFiltered     = 0
      var anchorUnattributed = 0
      var anchorContributed  = 0

      configuredFlows.foreach { flow =>
        val elements    = flow.elements
        val flowMethods = elements.collect { case node: CfgNode => node.method.fullName }.distinct
        if (flowMethods.size > deepestFlowMethods) deepestFlowMethods = flowMethods.size

        val handlerName = elements.flatMap(node => sourceNodeHandler.get(node.id)).headOption
        val sinkName    = elements.reverse.flatMap(node => sinkNodeMethod.get(node.id)).headOption

        (handlerName, sinkName) match {
          case (Some(handlerFullName), Some(sinkFullName)) =>
            // The flow filter. A flow carrying a derived predicate is not part of the unguarded
            // class this query asks for, so it is discarded here and counted.
            if (predicatesOnFlow(elements).nonEmpty) {
              anchorFiltered += 1
              flowsFiltered += 1
            } else {
              val path = composePath(handlerFullName, sinkFullName, elements)

              // The second pass, computed over the emitted path as found: a path node that is
              // itself a predicate, plus the predicates each path node calls directly. A return
              // is emitted whatever this finds, and nothing is added to it that the graph does
              // not carry.
              val pathNodes =
                path.map(fullName => (fullName, cpg.method.fullNameExact(fullName).l))
              val predicatesOnPath = pathNodes.flatMap { case (fullName, nodes) =>
                val itself = if (predicateSet.contains(fullName)) List(fullName) else Nil
                val called = nodes
                  .flatMap(_.call.methodFullName.l)
                  .filter(predicateSet.contains)
                  .distinct
                  .sorted
                itself ++ called
              }
              emitted.append(Emitted(handlerFullName, sinkFullName, path, predicatesOnPath))
              anchorContributed += 1
            }
          case _ =>
            anchorUnattributed += 1
            flowsUnattributed += 1
        }
      }

      // Where an anchor returned no flow, the sink nodes it did resolve are recorded together
      // with the expression each arrives through, so that a zero is accompanied by what was
      // asked and what the value passes through rather than left as an absence.
      val zeroFlowEvidence =
        if (configuredFlows.nonEmpty) List.empty[String]
        else
          anchorSinkNodes
            .map { node =>
              val enclosing = node.method.fullName
              jsonObject(Seq(
                "sink_node_code" -> jsonString(node.code),
                "in_method"      -> jsonString(enclosing),
                "arrives_through" -> jsonStringArray(definingExpressions(enclosing, node.code))))
            }
            .distinct
            .sorted

      renderedSinkAnchors.append(
        jsonBlockObject(
          Seq(
            "label"                          -> jsonString(anchor.label),
            "kind"                           -> jsonString(anchor.kind),
            "selector"                       -> jsonString(anchor.selector),
            "resolved_count"                 -> jsonInt(anchor.methods.size),
            "resolved"                       -> jsonStringArray(anchor.methods.map(_.fullName)),
            "argument_selection" -> jsonBlockArray(renderedSelections.toList, "        "),
            "sink_node_count"                -> jsonInt(anchorSinkNodes.size),
            "flows_at_configured_bound"      -> jsonInt(configuredFlows.size),
            "flows_at_engine_default_bound"  -> jsonInt(defaultFlows.size),
            "flows_filtered_by_flow_filter"  -> jsonInt(anchorFiltered),
            "flows_not_attributable"         -> jsonInt(anchorUnattributed),
            "returns_contributed"            -> jsonInt(anchorContributed),
            "sink_nodes_when_no_flow_returned" -> jsonBlockArray(zeroFlowEvidence, "        ")),
          "      "))
    }

    diagnostics.append("sink_anchors" -> jsonBlockArray(renderedSinkAnchors.toList, "    "))

    // Returns are de-duplicated and sorted, because two flows that differ only below the method
    // level project to one path in a method-level schema and would otherwise be emitted twice.
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
    // (10) What this formulation could and could not express.
    // -------------------------------------------------------------------------------------
    stage = "record_diagnostics"

    // The boundary at the sink end, resolved from the graph rather than asserted: for each type
    // that owns a resolved sink method, the anonymous types its methods allocate and the
    // deferred bodies those types carry. A data-flow relation follows data dependence, and an
    // allocation whose body the runtime invokes is not one, so this formulation does not follow
    // the boundary; it anchors at the sinks above it, which the class names as alternatives.
    val sinkOwnerTypes =
      sinkAnchors.flatMap(_.methods).map(ownerOf).filter(_.nonEmpty).distinct.sorted
    val threadBoundaryTypes       = scala.collection.mutable.TreeSet.empty[String]
    val threadBoundaryConnections = scala.collection.mutable.TreeSet.empty[String]
    val threadBoundaryBodyNames   = scala.collection.mutable.TreeSet.empty[String]
    sinkOwnerTypes.foreach { ownerType =>
      methodsOfType(ownerType).foreach { method =>
        val anonymousTypes = allocatedAnonymousTypes(
          ownerType, THREAD_ANONYMOUS_INFIX, method.callee.l.map(ownerOf).distinct)
        anonymousTypes.foreach(threadBoundaryTypes.add)
        anonymousTypes
          .flatMap(t => methodsOfType(t).filter(_.name == THREAD_BODY_NAME))
          .foreach { body =>
            threadBoundaryBodyNames.add(body.fullName)
            threadBoundaryConnections.add(method.fullName + " ==> " + body.fullName)
          }
      }
    }

    val partialFunctionNeeded =
      distinctReturns.exists(_.path.exists(partialFunctionBodyNames.contains))
    val threadBoundaryNeeded =
      distinctReturns.exists(_.path.exists(threadBoundaryBodyNames.contains))

    def renderBridge(
        rule: String,
        applied: Boolean,
        appliedNote: String,
        matchedTypes: scala.collection.mutable.TreeSet[String],
        connections: scala.collection.mutable.TreeSet[String],
        neededByEmittedPath: Boolean): String =
      jsonBlockObject(
        Seq(
          "rule"                      -> jsonString(rule),
          "boundary_resolved"         -> jsonBool(matchedTypes.nonEmpty),
          "applied_by_this_query"     -> jsonBool(applied),
          "applied_note"              -> jsonString(appliedNote),
          "succeeded"                 -> jsonBool(applied && connections.nonEmpty),
          "matched_types"             -> jsonInt(matchedTypes.size),
          "distinct_connections"      -> jsonInt(connections.size),
          "needed_by_an_emitted_path" -> jsonBool(neededByEmittedPath),
          "connections"               -> jsonStringArray(connections.toList)),
        "      ")

    diagnostics.append(
      "bridges" -> jsonBlockObject(
        Seq(
          "partialfunction_boundary" -> renderBridge(
            "from an entry point to the body declared by the synthetic partial-function type it " +
              "allocates, whose full name is the enclosing type followed by `" +
              PARTIAL_FUNCTION_INFIX + "`, the entry point's own name and a number — the " +
              "message parameter lives on that body and not on the named entry point, so " +
              "without this " +
              "the source set would be empty and the query would return nothing for a reason " +
              "that has nothing to do with the code under analysis",
            true,
            "applied at source selection: it is what makes a message source resolvable, and it " +
              "is not needed for an entry point that declares a message parameter itself",
            partialFunctionTypes,
            partialFunctionConnections,
            partialFunctionNeeded),
          "thread_boundary" -> renderBridge(
            "from a method owning a resolved sink to the deferred body of an anonymous type it " +
              "allocates, whose full name is the enclosing type followed by `" +
              THREAD_ANONYMOUS_INFIX + "` and a number and whose body the runtime invokes",
            false,
            "not applied: a flow follows data dependence, and an allocation whose body the " +
              "runtime invokes is not one, so this formulation does not continue past the " +
              "boundary and anchors instead at the sinks above it, which the class names as " +
              "alternatives to the deepest one. The boundary is resolved and recorded here so " +
              "the limit is evidence rather than an assertion",
            threadBoundaryTypes,
            threadBoundaryConnections,
            threadBoundaryNeeded)),
        "    "))

    diagnostics.append(
      "traversal" -> jsonBlockObject(
        Seq(
          "direction" -> jsonString(
            "data flow, asked backward from each sink anchor's command- or jar-bearing nodes to " +
              "the resolved driver-submission message sources, one query per sink anchor over " +
              "the whole source set"),
          "reachability_step"             -> jsonString("sink.reachableByFlows(source)"),
          "max_call_depth"                -> jsonInt(MAX_CALL_DEPTH),
          "engine_default_max_call_depth" -> jsonInt(engineDefaultDepth),
          "bound_reached" -> jsonString(
            "the data-flow engine exposes no signal for having truncated at its call-depth " +
              "bound, so reaching the bound cannot be read off a single query. What is measured " +
              "instead is the bound's effect: every anchor was answered a second time at the " +
              "engine default depth, and the two counts are recorded per anchor and summed " +
              "below. Nothing was truncated silently — where an anchor returned no flow, its " +
              "resolved sink nodes and the expression each arrives through are recorded"),
          "bound_changed_outcome_versus_engine_default" -> jsonBool(boundChangedOutcome),
          "flows_at_configured_bound"                   -> jsonInt(flowsAtConfigured),
          "flows_at_engine_default_bound"               -> jsonInt(flowsAtDefault),
          "flows_filtered_by_flow_filter"               -> jsonInt(flowsFiltered),
          "flows_not_attributable"                      -> jsonInt(flowsUnattributed),
          "deepest_emitted_flow_distinct_methods" ->
            (if (deepestFlowMethods == 0) "null" else jsonInt(deepestFlowMethods)),
          "entry_points_resolved"      -> jsonInt(handlerMethods.size),
          "entry_points_with_a_source" ->
            jsonInt(handlerMethods.size - handlersWithoutSource.size),
          "source_nodes_queried"        -> jsonInt(sourceNodeList.size),
          "sink_nodes_queried_distinct" -> jsonInt(sinkNodeMethod.size),
          "returns_emitted"             -> jsonInt(distinctReturns.size),
          "returns_removed_by_deduplication" -> jsonInt(emitted.size - distinctReturns.size),
          "path_composition" -> jsonString(
            "the named entry point first, then the enclosing methods of the flow's elements in " +
              "flow order — which begins in the synthetic partial-function body — then the sink " +
              "method last; an occurrence of either end in the middle is dropped so the " +
              "ordering holds exactly"),
          "return_selection" -> jsonString(
            "one return per distinct (entry point, sink, path, predicates) tuple, sorted, so a " +
              "flow that differs from another only below the method level is emitted once"),
          "flow_filter_reach" -> jsonString(
            "the flow's own elements: an element that is a call to a derived predicate, or an " +
              "element sitting inside one. A flow carrying either is discarded and counted, so " +
              "no emitted path has a predicate among its flow elements"),
          "predicate_check_reach" -> jsonString(
            "the emitted path nodes, plus one outgoing call step from each of them — wider than " +
              "the filter, and over a path that carries the entry point and the sink method " +
              "which no flow element covered")),
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

  // (11) The result region: the BEGIN marker, one JSON object, the END marker, and nothing else
  //      between them. Every graph read above has already completed.
  val document =
    "{\n" +
      "  " + jsonString("returns") + ": " + jsonBlockArray(renderedReturns.toList, "  ") + ",\n" +
      "  " + jsonString("diagnostics") + ": " + jsonBlockObject(diagnostics.toList, "  ") + "\n" +
      "}"

  println(MARKER_BEGIN)
  println(document)
  println(MARKER_END)
}

