// queries/joern/03-parameterized-unguarded-handler-sink.sc
// ===========================================================================================
// Phase 3 capability probe — query 03 of three, the PARAMETERIZED formulation.
//
// WHAT THIS QUERY ATTEMPTS
// ------------------------
// One reachability class, the same whole class the other two committed queries attempt, with
// the identity of both ends lifted into parameters:
//
//     an RPC entry point selected by `handlerPattern` reaching a privileged sink selected by
//     `sinkPattern`, along a path on which no derived authentication or ACL predicate appears.
//
// With no parameter supplied, the two defaults select exactly the ends the class names: entry
// points named `receive` or `receiveAndReply` enclosed in a type under `org.apache.spark.deploy.`,
// and the three sinks — `createDriver`, a `DriverRunner` construction, and a process launch —
// together with a fourth anchor for an `ExecutorRunner` construction, which is carried in
// addition to those three and never in place of one of them.
//
// This is the whole class, not a component of it, and not a wrapper that merely demonstrates
// that a parameter can be passed. The traversal below is written against the node sets the two
// patterns RESOLVE TO: it nowhere assumes that an entry point is named `receive`, that a sink is
// named `createDriver`, or that either end lies in any particular package. Every piece of
// knowledge specific to the class this probe was asked about lives in a default parameter value,
// where a caller can replace it, and in nothing else.
//
// A class can be expressible in one view of a graph and not another, so no single query's
// outcome settles the question this probe asks. This script therefore draws no conclusion,
// orders nothing, and reconciles its outcome against no other formulation's: it emits what its
// traversal found and, equally, what its traversal could not express, in `diagnostics`.
//
// WHICH FORMULATION THIS PARAMETERIZES, AND WHY
// ---------------------------------------------
// It parameterizes the CALL-GRAPH formulation — the relation the first of the three committed
// queries expresses — and not the data-flow one. The reason is what the parameters have to do.
//
// A call-graph formulation takes exactly two inputs: a set of entry-point methods and a set of
// sink methods. Lifting the identity of both ends into parameters therefore needs no further
// selection rule, and the traversal can be written against the resolved sets alone. A data-flow
// formulation needs two more rules, and both are properties of the particular pair it was
// written for: which node carries the message at the entry-point end, and which argument of the
// sink carries the value that arrives there. Generalizing those would mean either two more
// parameters — inflating a list that is itself one of this probe's reported measures — or
// leaving them as they stand, which would let this query answer that it generalizes while still
// assuming the ends it was written for. The class asked for is the whole class attempted in
// parameterized form, not a third distinct engine, so the formulation whose inputs already are
// the two ends is the one to parameterize.
//
// What that choice costs is stated rather than left to be inferred: the relation this query
// expresses is reachability over call edges, and `diagnostics.traversal.relation_expressed`
// records both that and the relation it does not express.
//
// THE PARAMETER LIST
// ------------------
// Three parameters, each a string at the invocation boundary — Joern passes `--param k=v` as a
// string — and each with a default that reproduces the class above, so that the script runs with
// no `--param` at all and generalizes only where a caller overrides it.
//
//   handlerPattern : string
//       Default: `receive=org\.apache\.spark\.deploy\..*\.receive:.*;`
//                `receiveAndReply=org\.apache\.spark\.deploy\..*\.receiveAndReply:.*`
//       Generalizes over: the identity of the entry point. One or more labelled alternatives
//       (format below), each an anchored full-match regex over a method's full name, so an
//       entry point on an inner or anonymous type is selected by the same rule as one on a
//       top-level type. Replacing this value asks the same question of a different handler.
//
//   sinkPattern : string
//       Default, as four alternatives of one value:
//         `createDriver=org\.apache\.spark\.deploy\..*createDriver:.*;`
//         `DriverRunner=org\.apache\.spark\.deploy\.worker\.DriverRunner\.<init>:.*;`
//         `process_launch=(java\.lang\.ProcessBuilder\.start|java\.lang\.Runtime\.exec):.*;`
//         `ExecutorRunner_additional=`
//           `org\.apache\.spark\.deploy\.worker\.ExecutorRunner\.<init>:.*`
//       Generalizes over: the identity of the privileged sink. Same format and same matching
//       rule as `handlerPattern`. Each alternative is resolved and reported separately, so a
//       caller sees which of their alternatives reached the graph and which resolved to nothing.
//       The fourth alternative's label carries the suffix `_additional`, which records in the
//       data that it is carried in addition to the three the class names; the script attaches no
//       meaning to that suffix and treats every alternative identically.
//
//   maxDepth : string, parsed here as a positive integer
//       Default: `20`
//       Generalizes over: how far the traversal may follow call edges from an entry point. A
//       handler/sink pair further apart than the pair above needs a larger bound, and a caller
//       measuring the effect of the bound needs a smaller one. The default leaves substantial
//       headroom over the deepest path this formulation can compose for the default ends, which
//       runs through a construction, a deferred body and a launch chain below it.
//
// Nothing else is a parameter, and two omissions are deliberate. The graph path and the
// workspace path are fixed constants: they are what this script reads and where it reads it,
// not something to generalize across. The authentication/ACL predicate set is derived from the
// graph at execution time and is deliberately not a parameter, so that a predicate added or
// renamed in the tree the graph was built over cannot be missed and a caller cannot narrow the
// check that makes this a query for the UNGUARDED class.
//
// THE PATTERN FORMAT
// ------------------
// `handlerPattern` and `sinkPattern` each carry one or more alternatives separated by `;`:
//
//     <label>=<regex>;<label>=<regex>;...
//
// The label is everything before the first `=` in an alternative, trimmed; the regex is
// everything after it, taken verbatim. An alternative with no `=` is accepted and labelled with
// its own pattern text. Labels must be distinct, because each one names a reported anchor.
//
// Each regex is matched as an ANCHORED FULL MATCH against a method's full name as the frontend
// records it — owner type, method name, then a signature — for example:
//
//     org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()
//
// Two consequences follow, and both are the caller's to work with rather than something this
// script guesses around. A regex metacharacter in a literal name — `.`, `$`, `(`, `)` — must be
// escaped to match itself. And because the match is anchored, a pattern must cover the signature
// too, which `.*` at the end does. A pattern that resolves to no node is reported with a count
// of zero rather than passed over, so a pattern that did not do what its author expected is
// visible in the output rather than indistinguishable from a graph that holds nothing.
//
// One limitation of the format, stated because it is real: a `;` cannot appear inside a pattern,
// since `;` separates alternatives and this format defines no escape for it. It is recorded in
// `diagnostics` beside the parsed alternatives.
//
// INVOCATION
// ----------
// Both forms work, and the script must be run from the directory that contains `harness/`, since
// the two paths below are relative to it. Nothing is prompted for, no environment variable is
// read, and no interactive console state is required, so a reader gets by hand precisely what
// the Phase 3 driver gets.
//
//   With the defaults, which attempt the class the probe was asked about:
//
//     joern --script queries/joern/03-parameterized-unguarded-handler-sink.sc
//
//   With a different handler/sink pair — here entry points in the storage package reaching a
//   process launch, which is a pair the defaults do not describe:
//
//     joern --script queries/joern/03-parameterized-unguarded-handler-sink.sc \
//       --param 'handlerPattern=storage=org\.apache\.spark\.storage\..*\.receiveAndReply:.*' \
//       --param 'sinkPattern=process_launch=java\.lang\.ProcessBuilder\.start:.*' \
//       --param maxDepth=12
//
// A parameter value is a single shell argument, so quote it: the patterns contain characters a
// shell would otherwise interpret. Overriding one parameter leaves the others at their defaults.
//
// No time limit is imposed anywhere in this script, and a caller's `maxDepth` bounds call edges
// rather than wall-clock time: the traversal is never narrowed or abandoned to make it finish
// sooner.
//
// THE GRAPH IS READ, NEVER BUILT
// ------------------------------
// The persisted code-property graph at `harness/cpg/spark.cpg` is loaded with `importCpg`, or
// opened from the workspace when a previous script in this workspace already loaded it. No
// graph-construction command appears anywhere in this file: this script cannot build a graph,
// and nothing under `harness/` is written, edited or removed by it. The only path it writes to
// is the Joern workspace named below, which is scratch and is never cleaned up here.
//
// LOAD ORDERING AND VALIDATION ORDERING ARE CORRECTNESS CONSTRAINTS, NOT STYLE CHOICES
// ------------------------------------------------------------------------------------
//  1. `---BLITZY-START---` is printed as the very first action — before the parameters are
//     validated, before the workspace is switched, and before anything is loaded. It is the only
//     thing that distinguishes a script that never compiled (no start marker) from one that
//     compiled and then failed (start marker, no result region). That distinction matters more
//     for this script than for the other two, because a parameter value is the one input a
//     caller supplies and a bad one fails early: printed first, the marker records that the
//     script compiled and ran and that the value was rejected.
//  2. Parameter validation runs next, and OUTSIDE the block that catches a failure and still
//     emits a result region. A rejected parameter therefore ends the run with the start marker
//     printed and no result region — the shape that says the script compiled and did not
//     complete — rather than a parseable result that would claim it ran to an answer. The
//     message names the parameter, the alternative and what was wrong with it.
//  3. `switchWorkspace` is called BEFORE any load. It closes the current workspace and opens
//     another, so a load performed first would be discarded by it.
//  4. The load is idempotent. The workspace is shared with the other Phase 3 queries and with
//     the environment gate's own coverage check, so by the time this script runs the project
//     will very likely already exist; where it does, it is opened rather than imported again,
//     which avoids a duplicate project. Opening an existing project is still reading. Which of
//     the two happened is recorded in `diagnostics.load_mode`.
//
// RESULT CONTRACT
// ---------------
// One JSON object, printed strictly between `---BLITZY-RESULT-BEGIN---` and
// `---BLITZY-RESULT-END---`, with nothing else in that region — the driver slices it and parses
// it, so a single stray line there would be read as a runtime failure that did not happen. All
// graph work therefore completes before the BEGIN marker is printed. The object has exactly two
// top-level keys, and they are the two the other committed queries emit:
//
//   {
//     "returns": [
//       {
//         "handler":            "<method full name of the entry point>",
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
//   parameters          every parameter, with the value actually used, the declared default, the
//                       alternatives the value parsed into, and what the parameter generalizes
//                       over — which is what makes a parameterized run reproducible from its
//                       own output.
//   load_mode           whether the project was imported or opened (see above).
//   workspace           the workspace path selected, the graph path read, and the project name
//   cpg_source          derived from it — carried as `workspace`, `cpg_source` and
//   cpg_project_name    `cpg_project_name`, so a result names what it was produced from.
//   cpg_method_count    method count read from the loaded graph; evidence it loaded non-empty.
//   derived_predicates  the authentication/ACL predicate set, derived from the graph at
//                       execution time and never hardcoded, with every exclusion rule that
//                       fired and exactly what each removed.
//   handler_anchors     each entry-point alternative with the node count it resolved to, the
//                       names resolved and the returns it contributed — zero counts included.
//   sink_anchors        the same, per sink alternative.
//   bridges             per boundary the traversal has to cross: whether the rule fired, what it
//                       connected, and whether an emitted path actually needed it.
//   traversal           the depth bound a caller asked for and whether it was reached, what the
//                       frontier was allowed to expand through and the measurement behind that
//                       restriction, how a path is selected, the reach of the predicate check,
//                       and — where nothing was returned — what in the run accounts for that.
//
// Output is deterministic: every collection printed is sorted or built in a fixed order, so
// re-running an unchanged source with unchanged parameters produces byte-identical output and a
// diff between revisions means something. Two fields legitimately differ between runs, and each
// records the difference it reflects: `load_mode`, between a cold workspace and a warm one, and
// `parameters`, between one caller's invocation and another's. Every value emitted is read from
// the graph or from the parameter binding actually in force; nothing is estimated, inferred or
// filled in, and no line number of any source file appears in this script or in anything it
// prints.
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
// governing encryption or SSL is not a check on the identity of a caller, and it matches none of
// the three name patterns. The same holds for a private helper of a matching method, for the
// channel-setup constructors of an authentication or SASL bootstrap, and for a liveness or
// recovery-state guard, none of which is a check on a caller's identity either.
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
//   * At the sink end, a method that hands work to a thread does not call the work: it allocates
//     an anonymous class and the runtime invokes its `run`.
//
// Both are bridged mechanically, and — because the ends of this query are whatever a caller
// asked for — both rules are keyed off the frontier method the traversal is standing on rather
// than off any name this file knows: an anonymous type is a bridge target when its full name is
// that method's OWN enclosing type followed by the frontend's infix for the construct, and, for
// the partial-function case, that method's OWN name. Neither bridge invents an edge: each one
// connects a method to a type that method demonstrably allocates. Whether each fired, what it
// connected, and whether an emitted path needed it, is recorded.
//
// THE PREDICATE FILTER AND `predicates_on_path` ARE TWO DIFFERENT THINGS
// ---------------------------------------------------------------------
// The traversal filter never expands through a derived predicate, so no predicate is ever an
// intermediate node of an emitted path; that filter is what makes this a query for the
// unguarded class. `predicates_on_path` is then computed separately over the emitted path, as
// found, and its reach is wider: a path node that is itself a predicate, plus the predicates
// each path node calls directly. So the second pass can find what the filter did not, and where
// it does the return is emitted anyway with `predicates_on_path` populated — a return is never
// dropped for carrying a predicate, and a predicate is never added to make one look as though it
// does. Whether a predicate lies on a path is a property of this formulation and of the path it
// emitted: a path that stops at a construction does not traverse what a path continuing past it
// traverses, and both are correct answers about their own path. For this script there is one
// further dependence, and it is recorded rather than smoothed over: the paths a run emits depend
// on the bound the caller set, so the predicates found on them do too.
// ===========================================================================================

import io.shiftleft.codepropertygraph.generated.nodes.Method

// --- Paths. Both are relative to the directory that contains `harness/`, and neither is a ---
// --- parameter: they are what this script reads and where it reads it. ---------------------
val WORKSPACE_PATH = "queries/joern/.workspace"
val CPG_PATH       = "harness/cpg/spark.cpg"

// --- The three markers of the query-to-driver contract. ------------------------------------
val MARKER_START = "---BLITZY-START---"
val MARKER_BEGIN = "---BLITZY-RESULT-BEGIN---"
val MARKER_END   = "---BLITZY-RESULT-END---"

// --- The pattern-list format: alternatives separated by one character, each optionally -----
// --- carrying a label ahead of the first occurrence of another. ----------------------------
val ALTERNATIVE_SEPARATOR = ";"
val LABEL_SEPARATOR       = '='
val ADDITIONAL_LABEL_NOTE =
  "a label is a name and nothing more: the script resolves and reports every alternative " +
    "identically, whatever its label says"

// --- Parameter defaults. Each reproduces the ends of the class the probe was asked about, --
// --- and each is a pattern rather than a location. -----------------------------------------
val HANDLER_PATTERN_DEFAULT =
  "receive=org\\.apache\\.spark\\.deploy\\..*\\.receive:.*" +
    ALTERNATIVE_SEPARATOR +
    "receiveAndReply=org\\.apache\\.spark\\.deploy\\..*\\.receiveAndReply:.*"

val SINK_PATTERN_DEFAULT =
  "createDriver=org\\.apache\\.spark\\.deploy\\..*createDriver:.*" +
    ALTERNATIVE_SEPARATOR +
    "DriverRunner=org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>:.*" +
    ALTERNATIVE_SEPARATOR +
    "process_launch=(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec):.*" +
    ALTERNATIVE_SEPARATOR +
    "ExecutorRunner_additional=org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>:.*"

val MAX_DEPTH_DEFAULT = "20"

// --- What each parameter generalizes over, carried into the output so that a reader of the -
// --- result knows what the value in force was free to vary. --------------------------------
val HANDLER_PATTERN_GENERALIZES_OVER =
  "the identity of the entry point: any method the graph holds, selected by an anchored " +
    "full-match regex over its full name, whatever its name, its enclosing type or its package"
val SINK_PATTERN_GENERALIZES_OVER =
  "the identity of the privileged sink: any method the graph holds, selected the same way, " +
    "including a constructor and a method with no body of its own"
val MAX_DEPTH_GENERALIZES_OVER =
  "how far the traversal may follow call edges from an entry point, so that a handler and a " +
    "sink further apart than the default pair can still be related"

// --- Selectors that are NOT parameters. ----------------------------------------------------
// The predicate set is derived from the graph so that a predicate added or renamed in the tree
// the graph was built over is not missed, and it is deliberately not exposed as a parameter: a
// caller who could narrow it could quietly turn this into a query for a different class.
val SECURITY_MANAGER_SELECTOR = "org\\.apache\\.spark\\.SecurityManager"
val PREDICATE_NAME_SELECTOR   = "^(check.*Permissions|acls.*|isAuthenticationEnabled)$"
val SCALA_SETTER_SUFFIX       = "_$eq"

// --- Frontend constructs the traversal has to work with. -----------------------------------
// Both infixes and the thread body name are properties of how a JVM-bytecode frontend records
// Scala compilation output, not of any particular code base, so neither is a parameter.
val THREAD_ANONYMOUS_INFIX  = "$$anon$"
val THREAD_BODY_NAME        = "run"
val PARTIAL_FUNCTION_INFIX  = "$$anonfun$"
val OPERATOR_PREFIX         = "<operator>"
val TRAIT_FORWARDER_SUFFIX  = "$"

// ===========================================================================================
// JSON serialization. Deterministic, and escaping is explicit.
// Method full names carry `$`, `<`, `>`, `:`, `(`, `)`, `,`, `[`, `]`, `.` and `/`, and a
// caller's pattern carries `\` besides, so nothing here is concatenated without escaping and an
// empty array is emitted as `[]`.
// ===========================================================================================

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
// Parameter parsing and validation.
//
// Joern hands every `--param` value over as a string, so a value that is not a string here is
// parsed and checked by this script rather than by the invocation. Each failure raises with a
// message naming the parameter, the alternative and what was wrong with it, and — because these
// run before the block that catches a failure and still emits a result region — a rejected
// value ends the run with the start marker printed and no result region.
// ===========================================================================================

final case class PatternAlternative(label: String, pattern: String)

/**
 * The alternatives a pattern-list parameter carries, in the order the caller wrote them. The
 * label is everything before the first label separator, trimmed; the pattern is everything after
 * it, taken verbatim so that no trimming can alter a regex. An alternative with no label
 * separator is labelled with its own pattern text.
 */
def parsePatternList(parameterName: String, raw: String): List[PatternAlternative] = {
  if (raw.trim.isEmpty) {
    throw new IllegalArgumentException(
      "parameter `" + parameterName + "` is empty: it must carry at least one alternative of " +
        "the form <label>" + LABEL_SEPARATOR + "<regex>, with alternatives separated by `" +
        ALTERNATIVE_SEPARATOR + "`")
  }

  // The separator is kept in the split so that an empty alternative is rejected rather than
  // silently dropped: a stray separator is a typo, and a query whose ends were not what its
  // caller wrote is worse than one that refused to run.
  val pieces = raw.split(ALTERNATIVE_SEPARATOR, -1).toList

  val alternatives = pieces.zipWithIndex.map { case (piece, position) =>
    if (piece.trim.isEmpty) {
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` has an empty alternative at position " +
          (position + 1) + " of " + pieces.size + ": remove the stray `" +
          ALTERNATIVE_SEPARATOR + "` or fill the alternative in")
    }

    val separatorAt = piece.indexOf(LABEL_SEPARATOR.toInt)
    val (label, pattern) =
      if (separatorAt < 0) (piece.trim, piece)
      else (piece.take(separatorAt).trim, piece.drop(separatorAt + 1))

    if (label.isEmpty) {
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` has an alternative with an empty label at position " +
          (position + 1) + ": write <label>" + LABEL_SEPARATOR + "<regex>, or omit the label " +
          "entirely to have the pattern name itself")
    }
    if (pattern.isEmpty) {
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` has an alternative with an empty pattern at " +
          "position " + (position + 1) + " (label `" + label + "`): a pattern selects the " +
          "methods this end of the query resolves to and cannot be empty")
    }

    // Compiled here so that a malformed regex is rejected before the graph is loaded, which is
    // both faster to report and unambiguous about what failed.
    scala.util.Try(java.util.regex.Pattern.compile(pattern)) match {
      case scala.util.Failure(syntaxFailure) =>
        throw new IllegalArgumentException(
          "parameter `" + parameterName + "` carries a pattern that is not a valid regular " +
            "expression at position " + (position + 1) + " (label `" + label + "`): " +
            Option(syntaxFailure.getMessage).getOrElse(syntaxFailure.getClass.getName))
      case scala.util.Success(_) => ()
    }

    PatternAlternative(label, pattern)
  }

  val duplicateLabels =
    alternatives.groupBy(_.label).filter(_._2.size > 1).keys.toList.sorted
  if (duplicateLabels.nonEmpty) {
    throw new IllegalArgumentException(
      "parameter `" + parameterName + "` repeats the label(s) " +
        duplicateLabels.mkString(", ") + ": each label names one reported anchor, so labels " +
        "must be distinct")
  }

  alternatives
}

/** A bound on call edges: an integer, and at least one, so that a traversal can take a step. */
def parsePositiveInt(parameterName: String, raw: String): Int =
  raw.trim.toIntOption match {
    case None =>
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` is not an integer: `" + raw + "`")
    case Some(value) if value < 1 =>
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` is not a positive integer: `" + raw + "`. A bound " +
          "below one would let no traversal take a step")
    case Some(value) => value
  }

/**
 * Whether a value in force is the declared default. Joern passes a parameter as a string and
 * offers no provenance for it, so a caller who passes exactly the declared default cannot be
 * told apart from one who passed nothing; this is value equality, and the output says so.
 */
def originOf(valueUsed: String, declaredDefault: String): String =
  if (valueUsed == declaredDefault) "default_value" else "override"


// ===========================================================================================
// The query. Three parameters, each with a default that reproduces the class the probe was
// asked about: see THE PARAMETER LIST and INVOCATION in the header.
// ===========================================================================================

@main def exec(
    handlerPattern: String = HANDLER_PATTERN_DEFAULT,
    sinkPattern: String = SINK_PATTERN_DEFAULT,
    maxDepth: String = MAX_DEPTH_DEFAULT): Unit = {

  // (1) The start marker is the very first action — before the parameters are validated, before
  //     the workspace switch and before any load. It is what tells a script that never compiled
  //     apart from one that compiled and then rejected a parameter value.
  println(MARKER_START)

  // (2) Parameter validation, and deliberately outside the block below that catches a failure
  //     and still emits a result region. A rejected value therefore leaves the start marker
  //     printed and no result region, which is the shape that says the script compiled and did
  //     not complete — never a parseable result claiming an answer it did not reach.
  val handlerAlternatives = parsePatternList("handlerPattern", handlerPattern)
  val sinkAlternatives    = parsePatternList("sinkPattern", sinkPattern)
  val maxCallDepth        = parsePositiveInt("maxDepth", maxDepth)

  // Rendered JSON fragments, accumulated in a fixed order so the output is deterministic.
  val diagnostics     = scala.collection.mutable.ListBuffer.empty[(String, String)]
  val renderedReturns = scala.collection.mutable.ListBuffer.empty[String]

  // Names the stage the run reached, so a failure below is reported against the step that
  // failed rather than as an unattributed error.
  var stage = "start"

  // (3) The parameter binding actually in force, recorded first, because every count below is a
  //     count under these values and a result that did not carry them could not be reproduced.
  def renderAlternatives(alternatives: List[PatternAlternative], indent: String): String =
    jsonBlockArray(
      alternatives.map(alternative =>
        jsonObject(Seq(
          "label"   -> jsonString(alternative.label),
          "pattern" -> jsonString(alternative.pattern)))),
      indent)

  diagnostics.append(
    "parameters" -> jsonBlockObject(
      Seq(
        "declared" -> jsonBlockArray(
          Seq(
            jsonBlockObject(
              Seq(
                "name"                -> jsonString("handlerPattern"),
                "type"                -> jsonString("string"),
                "value_used"          -> jsonString(handlerPattern),
                "declared_default"    -> jsonString(HANDLER_PATTERN_DEFAULT),
                "origin"              ->
                  jsonString(originOf(handlerPattern, HANDLER_PATTERN_DEFAULT)),
                "generalizes_over"    -> jsonString(HANDLER_PATTERN_GENERALIZES_OVER),
                "parsed_alternatives" -> renderAlternatives(handlerAlternatives, "          ")),
              "        "),
            jsonBlockObject(
              Seq(
                "name"                -> jsonString("sinkPattern"),
                "type"                -> jsonString("string"),
                "value_used"          -> jsonString(sinkPattern),
                "declared_default"    -> jsonString(SINK_PATTERN_DEFAULT),
                "origin"              -> jsonString(originOf(sinkPattern, SINK_PATTERN_DEFAULT)),
                "generalizes_over"    -> jsonString(SINK_PATTERN_GENERALIZES_OVER),
                "parsed_alternatives" -> renderAlternatives(sinkAlternatives, "          ")),
              "        "),
            jsonBlockObject(
              Seq(
                "name" -> jsonString("maxDepth"),
                "type" -> jsonString(
                  "string at the invocation boundary, parsed by this script as a positive " +
                    "integer"),
                "value_used"       -> jsonString(maxDepth),
                "value_parsed"     -> jsonInt(maxCallDepth),
                "declared_default" -> jsonString(MAX_DEPTH_DEFAULT),
                "origin"           -> jsonString(originOf(maxDepth, MAX_DEPTH_DEFAULT)),
                "generalizes_over" -> jsonString(MAX_DEPTH_GENERALIZES_OVER)),
              "        ")),
          "      "),
        "origin_rule" -> jsonString(
          "the value in force is compared with the declared default: `default_value` where they " +
            "are equal and `override` where they differ. A parameter arrives as a string with " +
            "no provenance, so a caller who passed exactly the declared default is recorded the " +
            "same way as one who passed nothing"),
        "pattern_list_format" -> jsonString(
          "one or more alternatives separated by `" + ALTERNATIVE_SEPARATOR + "`, each of the " +
            "form <label>" + LABEL_SEPARATOR + "<regex> — the label is everything before the " +
            "first `" + LABEL_SEPARATOR + "`, trimmed, and the regex is everything after it, " +
            "taken verbatim; an alternative with no `" + LABEL_SEPARATOR + "` is labelled with " +
            "its own pattern text"),
        "pattern_list_format_limitation" -> jsonString(
          "the format defines no escape for its own separator, so a `" + ALTERNATIVE_SEPARATOR +
            "` cannot appear inside a pattern"),
        "label_note" -> jsonString(ADDITIONAL_LABEL_NOTE),
        "match_mode" -> jsonString(
          "anchored full match against a method full name as the frontend records it — owner " +
            "type, method name, then a signature")),
      "    "))

  try {
    // -------------------------------------------------------------------------------------
    // (4) Workspace first. It closes the current workspace and opens another, so switching
    //     after a load would discard the loaded project.
    // -------------------------------------------------------------------------------------
    stage = "switch_workspace"
    switchWorkspace(WORKSPACE_PATH)

    // -------------------------------------------------------------------------------------
    // (5) Idempotent load. The workspace is shared with the other Phase 3 queries and with the
    //     gate's coverage check, so the project is likely to be present already; opening it is
    //     reading, and it avoids a duplicate.
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
    // (6) The authentication / ACL predicate set, derived from the graph — never hardcoded and
    //     never a parameter. Anchored full match, then the exclusion rules in order, each
    //     recorded with what it removed. The resulting count is whatever the derivation
    //     produces on the graph in front of it.
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
          "type_declaration_selector"        -> jsonString(SECURITY_MANAGER_SELECTOR),
          "resolved_type_declarations"       -> jsonStringArray(securityManagerTypes),
          "member_names_on_type_declaration" -> jsonStringArray(securityManagerMembers),
          "name_selector"                    -> jsonString(PREDICATE_NAME_SELECTOR),
          "match_mode"                       -> jsonString("anchored full match"),
          "exposed_as_a_parameter"           -> jsonBool(false),
          "methods_considered"               -> jsonInt(securityManagerMethods.size),
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
    // (7) The two ends, resolved from the parameters. Each alternative is resolved and
    //     reported on its own, so a caller learns which of their alternatives reached the
    //     graph; an alternative that resolved to nothing is reported with a count of zero
    //     rather than passed over.
    // -------------------------------------------------------------------------------------
    stage = "resolve_anchors"

    final case class Anchor(label: String, selector: String, methods: List[Method])

    def resolved(methods: List[Method]): List[Method] =
      methods.distinctBy(_.fullName).sortBy(_.fullName)

    def resolveAnchor(alternative: PatternAlternative): Anchor =
      Anchor(
        alternative.label,
        alternative.pattern,
        resolved(cpg.method.fullName(alternative.pattern).l))

    val handlerAnchors = handlerAlternatives.map(resolveAnchor)
    val sinkAnchors    = sinkAlternatives.map(resolveAnchor)

    val handlerMethods = resolved(handlerAnchors.flatMap(_.methods))
    val sinkMethods    = resolved(sinkAnchors.flatMap(_.methods))
    val sinkFullNames  = sinkMethods.map(_.fullName).toSet

    // -------------------------------------------------------------------------------------
    // (8) What the frontier may expand through, established by measurement rather than by a
    //     name this file chose. A method the graph carries no body for cannot be expanded
    //     through at all — the count below is the evidence — so the bodied methods are the
    //     maximal expandable set, and taking it as the restriction keeps a code base out of
    //     the traversal: whatever pair a caller names, the frontier follows the graph's own
    //     bodies rather than a package this script knows about.
    // -------------------------------------------------------------------------------------
    stage = "measure_expansion_criterion"
    val methodsWithABody    = cpg.method.isExternal(false).size
    val methodsWithoutABody = cpg.method.isExternal(true).l
    val methodsWithoutABodyCarryingACalleeEdge = methodsWithoutABody.count(_.callee.nonEmpty)

    // -------------------------------------------------------------------------------------
    // (9) The traversal. Callee edges, plus the two bridges the header describes, both keyed
    //     off the frontier method the traversal is standing on rather than off any name this
    //     file knows. Nothing is expanded through a derived predicate, which is what makes
    //     this the unguarded class.
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
      val threadTypes = allocatedAnonymousTypes(owner, THREAD_ANONYMOUS_INFIX, calleeOwners)
      threadTypes.foreach(threadBridgeTypes.add)
      val threadSuccessors =
        threadTypes.flatMap(t => methodsOfType(t).filter(_.name == THREAD_BODY_NAME))
      threadSuccessors.foreach(r =>
        threadBridgeConnections.add(method.fullName + " ==> " + r.fullName))

      // Bridge 2 — the partial-function boundary: the case bodies live on the synthetic
      // partial-function type this method allocates and which is named after it, not in the
      // method itself. The infix carries THIS method's own name, so the rule follows whatever
      // entry point a caller's pattern resolved rather than a name written here.
      val pfTypes =
        allocatedAnonymousTypes(owner, PARTIAL_FUNCTION_INFIX + method.name + "$", calleeOwners)
      pfTypes.foreach(pfBridgeTypes.add)
      val pfTypeMethods = pfTypes.map(t => (t, methodsOfType(t)))
      pfTypeMethods.foreach { case (t, members) =>
        pfBridgeConnections.add(method.fullName + " ==> " + t + " [" + members.size + " methods]")
      }
      val pfSuccessors = pfTypeMethods.flatMap { case (_, members) => members }

      direct ++
        threadSuccessors.map(m => (m, "bridge_thread")) ++
        pfSuccessors.map(m => (m, "bridge_partialfunction"))
    }

    final case class Emitted(
        handler: String,
        sink: String,
        path: List[String],
        predicates: List[String])

    val emitted               = scala.collection.mutable.ListBuffer.empty[Emitted]
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

      while (depth < maxCallDepth && frontier.nonEmpty) {
        val next = scala.collection.mutable.ListBuffer.empty[Method]
        frontier.foreach { current =>
          successorsOf(current).foreach { case (successor, edgeKind) =>
            val fullName = successor.fullName
            if (!depthOf.contains(fullName)) {
              depthOf.put(fullName, depth + 1)
              predecessor.put(fullName, (current.fullName, edgeKind))
              if (sinkFullNames.contains(fullName)) reached.append(successor)
              // The frontier expands only through a method the graph carries a body for, never
              // through an operator pseudo-method, and never through a derived predicate.
              val expandable =
                !successor.isExternal &&
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
      if (depth >= maxCallDepth && frontier.nonEmpty) {
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
        // forwards, so a path may hop from one entry point to another through one. Such a return
        // is emitted as the call graph carries it and is listed here, not filtered out.
        if (pathNodes.exists { case (_, nodes) =>
              nodes.exists(_.name.endsWith(TRAIT_FORWARDER_SUFFIX))
            }) {
          forwarderReturns.add(handler.fullName + " ==> " + sink.fullName)
        }
        if (depthOf(sink.fullName) > deepestSinkDepth) deepestSinkDepth = depthOf(sink.fullName)
        edges.foreach {
          case (from, to, "bridge_thread")          => threadBridgeOnPaths.add(from + " ==> " + to)
          case (from, to, "bridge_partialfunction") => pfBridgeOnPaths.add(from + " ==> " + to)
          case _                                    => ()
        }

        emitted.append(Emitted(handler.fullName, sink.fullName, path, predicatesOnPath))
      }
    }

    // One return per distinct tuple, in a fixed order, so an unchanged source run with unchanged
    // parameters emits the same bytes.
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
    // (10) What the traversal could and could not express, under the parameters in force.
    // -------------------------------------------------------------------------------------
    stage = "record_diagnostics"

    def returnsFrom(anchor: Anchor, endOf: Emitted => String): Int = {
      val names = anchor.methods.map(_.fullName).toSet
      distinctReturns.count(entry => names.contains(endOf(entry)))
    }

    def renderAnchor(anchor: Anchor, contributed: Int): String =
      jsonObject(Seq(
        "label"               -> jsonString(anchor.label),
        "selector"            -> jsonString(anchor.selector),
        "resolved_count"      -> jsonInt(anchor.methods.size),
        "resolved"            -> jsonStringArray(anchor.methods.map(_.fullName)),
        "returns_contributed" -> jsonInt(contributed)))

    diagnostics.append(
      "handler_anchors" -> jsonBlockArray(
        handlerAnchors.map(anchor => renderAnchor(anchor, returnsFrom(anchor, _.handler))),
        "    "))
    diagnostics.append(
      "sink_anchors" -> jsonBlockArray(
        sinkAnchors.map(anchor => renderAnchor(anchor, returnsFrom(anchor, _.sink))),
        "    "))

    def renderBridge(
        rule: String,
        matchedTypes: scala.collection.mutable.TreeSet[String],
        connections: scala.collection.mutable.TreeSet[String],
        onPaths: scala.collection.mutable.TreeSet[String]): String =
      jsonBlockObject(
        Seq(
          "rule"                         -> jsonString(rule),
          "keyed_off" -> jsonString(
            "the frontier method the traversal is standing on — its own enclosing type and, " +
              "for the partial-function rule, its own name — so the rule follows whatever a " +
              "caller's pattern resolved rather than a name written into this script"),
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
            "from a frontier method to the `" + THREAD_BODY_NAME + "` of an anonymous type it " +
              "allocates, whose full name is the enclosing type followed by `" +
              THREAD_ANONYMOUS_INFIX + "` and a number",
            threadBridgeTypes,
            threadBridgeConnections,
            threadBridgeOnPaths),
          "partialfunction_boundary" -> renderBridge(
            "from a frontier method to every method of the synthetic partial-function type it " +
              "allocates, whose full name is the enclosing type followed by `" +
              PARTIAL_FUNCTION_INFIX + "`, the method's own name and a number",
            pfBridgeTypes,
            pfBridgeConnections,
            pfBridgeOnPaths)),
        "    "))

    // Where nothing was returned, what in this run accounts for it — read from the run rather
    // than guessed at, so that a zero is a result and not an absence.
    val handlerAnchorsWithNoNodes = handlerAnchors.filter(_.methods.isEmpty).map(_.label).sorted
    val sinkAnchorsWithNoNodes    = sinkAnchors.filter(_.methods.isEmpty).map(_.label).sorted
    val noReturnsReasons: List[String] =
      if (distinctReturns.nonEmpty) List.empty[String]
      else
        List(
          if (handlerMethods.isEmpty)
            Some("no entry point resolved, so no traversal was started")
          else None,
          if (sinkMethods.isEmpty)
            Some("no sink method resolved, so no traversal could terminate at one")
          else None,
          if (handlerAnchorsWithNoNodes.nonEmpty)
            Some(
              "these handlerPattern alternatives resolved to no node: " +
                handlerAnchorsWithNoNodes.mkString(", "))
          else None,
          if (sinkAnchorsWithNoNodes.nonEmpty)
            Some(
              "these sinkPattern alternatives resolved to no node: " +
                sinkAnchorsWithNoNodes.mkString(", "))
          else None,
          if (pfBridgeConnections.isEmpty)
            Some(
              "the partial-function bridge connected nothing, so no case body was entered from " +
                "an entry point")
          else None,
          if (threadBridgeConnections.isEmpty)
            Some("the thread bridge connected nothing, so no deferred body was entered")
          else None,
          if (boundReached)
            Some(
              "the bound of " + maxCallDepth + " call edges was reached for " +
                truncatedHandlers + " entry point(s), so the frontier was still expanding when " +
                "the traversal stopped there")
          else None,
          if (handlerMethods.nonEmpty && sinkMethods.nonEmpty)
            Some(
              "no path from any of the " + handlerMethods.size + " resolved entry points " +
                "reached any of the " + sinkMethods.size + " resolved sink methods")
          else None).flatten

    diagnostics.append(
      "traversal" -> jsonBlockObject(
        Seq(
          "direction" -> jsonString(
            "forward over callee edges, one traversal per resolved entry point, from the node " +
              "set `handlerPattern` resolved to the node set `sinkPattern` resolved"),
          "generality" -> jsonString(
            "the traversal reads both ends from the resolved node sets and tests membership by " +
              "method full name, so no step of it depends on an entry point's name, a sink's " +
              "name, or the package either lies in"),
          "relation_expressed" -> jsonString(
            "reachability over call edges: a return says a chain of calls, plus the bridges " +
              "recorded above, runs from the entry point to the sink. Whether a value the entry " +
              "point received arrives at an argument of the sink is a different relation, over " +
              "data dependence, which this formulation does not express and does not claim to"),
          "max_call_depth"                       -> jsonInt(maxCallDepth),
          "max_call_depth_source"                -> jsonString("the `maxDepth` parameter"),
          "bound_reached"                        -> jsonBool(boundReached),
          "entry_points_truncated_at_bound"      -> jsonInt(truncatedHandlers),
          "entry_points_traversed"               -> jsonInt(handlerMethods.size),
          "sink_methods_resolved"                -> jsonInt(sinkMethods.size),
          "methods_seen_summed_over_entry_points" -> jsonInt(methodsSeenSummed),
          "expansion_restriction" -> jsonString(
            "the frontier expands only through a method the graph carries a body for; an " +
              "operator pseudo-method and a derived predicate are never expanded, so no emitted " +
              "path has a predicate as an intermediate node. A sink or a predicate is still " +
              "recognised wherever it is reached, including where the graph carries no body for " +
              "it"),
          "expansion_restriction_evidence" -> jsonBlockObject(
            Seq(
              "methods_the_graph_carries_a_body_for" -> jsonInt(methodsWithABody),
              "methods_the_graph_carries_no_body_for" -> jsonInt(methodsWithoutABody.size),
              "methods_carrying_no_body_that_hold_a_callee_edge" ->
                jsonInt(methodsWithoutABodyCarryingACalleeEdge),
              "reading" -> jsonString(
                "a method carrying no body holds no callee edge to follow, so the bodied " +
                  "methods are the whole of what any callee traversal could expand through; " +
                  "the restriction is therefore a property of this graph rather than a choice " +
                  "of code base, and it is measured here rather than asserted")),
            "      "),
          "path_selection" -> jsonString(
            "one return per (entry point, sink) pair, whose path is the breadth-first discovery " +
              "path; successors are visited in full-name order and returns are sorted, so the " +
              "output is reproducible"),
          "predicate_check_reach" -> jsonString(
            "the emitted path nodes, plus one outgoing call step from each of them"),
          "predicates_on_path_dependence_on_the_bound" -> jsonString(
            "the paths this run emitted are the paths reachable within the bound above, so the " +
              "predicates found on them are a property of that bound as well as of the graph: a " +
              "path stopping at a construction does not traverse what a path continuing past it " +
              "traverses. A caller changing `maxDepth` changes which paths exist to be checked, " +
              "and this field records that rather than smoothing it over"),
          "returns_emitted"                  -> jsonInt(distinctReturns.size),
          "returns_removed_by_deduplication" -> jsonInt(emitted.size - distinctReturns.size),
          "returns_whose_emitted_path_carries_a_derived_predicate" ->
            jsonInt(distinctReturns.count(_.predicates.nonEmpty)),
          "deepest_emitted_sink_depth" ->
            (if (deepestSinkDepth < 0) "null" else jsonInt(deepestSinkDepth)),
          "returns_whose_path_traverses_a_trait_default_method_forwarder" ->
            jsonInt(forwarderReturns.size),
          "returns_traversing_a_trait_default_method_forwarder" ->
            jsonStringArray(forwarderReturns.toList),
          "no_returns_explanation" ->
            (if (distinctReturns.nonEmpty) "null"
             else if (noReturnsReasons.isEmpty)
               jsonString(
                 "nothing was returned and the run recorded no resolvable cause; the anchor " +
                   "counts and bridge state above are the whole of what was observed")
             else jsonString(noReturnsReasons.mkString("; ")))),
        "    "))

  } catch {
    case scala.util.control.NonFatal(failure) =>
      // A failure inside the graph work is recorded against the stage that produced it and the
      // result region is still emitted, so the driver reads a parseable result rather than an
      // unexplained silence. A rejected parameter is the other case and never reaches here: it
      // is raised above this block, on purpose.
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

