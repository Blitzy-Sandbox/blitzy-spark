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
//  3. The load is idempotent AND provenance-checked. The workspace is persistent scratch shared
//     with the other Phase 3 queries and with the environment gate's own coverage check, so by
//     the time this script runs the project will very likely already exist — but a project's
//     NAME is derived from its input path's last segment and is therefore not evidence of its
//     contents. Before an existing project is opened, the input path Joern recorded for it when
//     it was created is canonicalized and compared with the canonical path of the graph this
//     script is contracted to read; a mismatch, or a recorded path that no longer canonicalizes,
//     FAILS THE RUN CLOSED through the failure protocol below rather than substituting a stale
//     graph for the pinned one. On a match the verified identity — the canonical graph path, its
//     size in bytes, its content digest, and the project's own recorded input path — is recorded
//     in `diagnostics.graph_identity`. Which of import or open happened is recorded in
//     `diagnostics.load_mode`. Opening an existing project is still reading.
//  4. The data-flow layer is engaged AFTER the load, because it operates on the loaded graph.
//     Its outcome is read back from the graph it returns rather than assumed.
//
// THE CHECK AND THE LOAD ARE TIED TOGETHER
// ----------------------------------------
// A path and a size are properties of a NAME, not of the bytes behind it, so a provenance check
// that finishes before the load leaves a window in which a different file could be loaded than
// the one that passed (CWE-367). This script closes that window rather than narrowing it: the
// SHA-256 of the file at the canonical path is taken BEFORE the load and taken again AFTER it,
// and a difference in the digest OR the size fails the run closed through the failure protocol,
// naming the before and after values. Both digests are computed by streaming the file in
// bounded chunks, so a half-gigabyte graph is never held in memory.
//
// The digest is recorded in `diagnostics.graph_identity` beside the size, together with
// `content_digest_reverified_after_load` and a `digest_verification_rule` that states what the
// digest proves on each load branch — the file `importCpg` read, on an import; the pinned source
// file the project's recorded input path ties the project to, on an open, the project's own copy
// being a separate artifact that applying an overlay legitimately changes. No expected digest is
// hardcoded here and none is compared against any record: the digest exists to detect a change
// across the window and to record what was read.
//
// RESULT CONTRACT, AND THE FAILURE PROTOCOL THAT IS ITS OTHER HALF
// ---------------------------------------------------------------
// On success: one JSON object, printed strictly between `---BLITZY-RESULT-BEGIN---` and
// `---BLITZY-RESULT-END---`, with nothing else in that region — the driver slices it and parses
// it, so a single stray line there would be read as a runtime failure that did not happen. All
// graph work completes, and the whole document is built as one string, BEFORE the BEGIN marker
// is printed, which also keeps the data-flow engine's own progress and warning output well clear
// of the region: the markers are emitted only once a complete result exists.
//
// On failure: NO result region is printed at all. Any failure — a provenance mismatch, an empty
// graph, an exception from the data-flow engine — is written to STDERR as one
// `---BLITZY-FAILURE---` line naming the stage, the exception type and its message, followed by
// the stack trace, and the exception is then re-raised so the process terminates with a non-zero
// exit status. That combination — start marker present, result region absent, exit status
// non-zero — is what tells the driver a run compiled and did not complete. Emitting a result
// region after a caught failure, as an earlier revision of this script did, would have the
// driver classify a failed or partial run as a successful one, so no error path here produces a
// payload of any kind.
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
// what the flow queries resolved and where they stopped, because a recorded limit is part of
// the answer this probe produces, and a query that returns nothing while recording why it
// returned nothing is a result, whereas one that returns nothing silently is a defect. Its
// keys:
//
//   load_mode           whether the project was imported or opened (see above).
//   graph_identity      the canonical path of the graph read, its size in bytes, its content
//                       digest taken before the load and re-verified after it, the input path
//                       the workspace project recorded, and how each was compared.
//   cpg_method_count    method count read from the loaded graph; evidence it loaded non-empty.
//   dataflow_layer      the layer command that was run, the overlay state read from the graph
//                       before and after running it, the reachability step used, and the call
//                       depth the engine defaults to beside the depth this script configures.
//   derived_predicates  the authentication/ACL predicate set, derived from the graph at
//                       execution time and never hardcoded, with every exclusion rule that
//                       fired and exactly what each removed.
//   handler_selection   the entry-point discriminator, every candidate it accepted, and every
//                       candidate it excluded with the rule that excluded it and the evidence.
//   handler_anchors     each entry-point anchor with the node count it resolved to and the
//                       names resolved — zero counts included.
//   source_nodes        what was resolved as the driver-submission message and its
//                       command-bearing value, per entry point and per source class, the type
//                       evidence each node was selected on, and — named explicitly — every
//                       entry point that yielded no qualifying source at all.
//   sink_anchors        each sink anchor with its label and kind, the argument-selection rule
//                       that fired per sink method, the node counts — zeros included — the
//                       flow counts at both call depths, and, where an anchor returned no
//                       flow, the expression each of its sink nodes arrives through.
//   bridges             per boundary this formulation has to deal with: whether it resolved,
//                       whether it was applied, what it connected, and whether an emitted path
//                       needed it.
//   traversal           the call-depth bound and what is known about reaching it, how a path is
//                       composed, how returns are selected, the flows emitted per source class,
//                       how many flows carried a predicate on their own elements, and the reach
//                       of the predicate check — together with the statement that no flow is
//                       filtered out for carrying one.
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
// WHICH METHODS ARE ENTRY ANCHORS, AND WHY NAME AND PACKAGE ARE NOT ENOUGH
// -----------------------------------------------------------------------
// The class names its entry point as `receive` / `receiveAndReply` in the `deploy` package, and
// selecting on exactly that — a name and an enclosing-type prefix — resolves methods that are
// not standalone-mode RPC handler declarations at all: a driver-plugin `receive` that happens to
// share the name, a YARN endpoint outside standalone mode, and a trait's inherited default that
// has no declaration of its own and whose body is a forwarder. Each of those becomes an entry
// point that never received the message the class is about, so the entry set is qualified
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
// rule and the evidence. For this formulation the third test does double duty — the partial-
// function class it looks for is also where this query's sources live, as the next section sets
// out — so an entry point that fails it has no source to select in any case.
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
//     therefore carries no message parameter at all, and the parameter the synthetic body does
//     carry is the erased universal object type, which is evidence of nothing: every partial
//     function ever compiled has one, whatever message it handles. Sources are therefore
//     selected by TYPE EVIDENCE inside that body, as the next section sets out, and never from
//     the erased parameter. Which body class carried a handler's sources is recorded per entry
//     point.
//   * At the sink end a method that hands work to a thread does not call the work: it allocates
//     an anonymous class and the runtime invokes its `run`. A data-flow relation follows data
//     dependence, and an allocation-to-runtime handoff is not one, so a flow does not continue
//     past it. This is why the class names three alternative sinks rather than requiring the
//     deepest: reaching the process launch is not required for the class to be attempted, and
//     this formulation anchors at the sinks above the boundary. The boundary itself is resolved
//     from the graph and recorded, so the limit is evidence rather than an assertion.
//
// WHAT A SOURCE IS, AND WHY THE ERASED PARAMETER IS NOT ONE
// --------------------------------------------------------
// The class names the driver-submission message and the command it carries, so sources are bound
// to that message by TYPE EVIDENCE the handler's own body carries, and to nothing weaker. Two
// type names are pinned in this file — the submission message case class and the driver
// description it carries — exactly as the three sinks are pinned by name. Everything else about
// the source set is DERIVED from the graph at execution time:
//
//   * the accessor that reads a description off a deploy message is derived, as a method on a
//     deploy-message type whose return type is the driver-description type, with a
//     default-argument supplier excluded by its name prefix;
//   * the command- and jar-bearing members of the description are derived from that type's own
//     members, by return type and by name;
//   * a deploy message OTHER than the pinned submission message that also carries a description
//     accessor is a DIFFERENT source class — the internal hand-off from one of these endpoints
//     to another — and is resolved, labelled and counted separately. It is never folded into the
//     submission-message set, because a value that arrived on an internal message did not arrive
//     from a submitter, and the two answer different questions.
//
// Within an entry point's own body scope — the entry point itself plus the methods of the
// synthetic partial-function classes it allocates, transitively — a source node is then one of:
//
//   A  a value whose static type IS the message: the cast the pattern match compiles to, and any
//      identifier or local carrying that type;
//   B  a call to the derived description accessor OF that message — the command-bearing value
//      read off it;
//   C  a call to a derived command- or jar-bearing member of the description reached from B.
//
// What is deliberately NOT a source is the erased `java.lang.Object` parameter of a synthetic
// partial-function body. Every partial function compiled over a bytecode frontend has one,
// whatever messages it handles, so admitting it makes every handler a driver-submission source
// and lets an unrelated message's accessors into the flows. An entry point with no qualifying
// source is REPORTED, by name, in `diagnostics.source_nodes.handlers_with_no_qualifying_source`
// — a named absence is evidence; a generic parameter standing in for evidence is not.
//
// EVERY ATTRIBUTABLE FLOW IS EMITTED; `predicates_on_path` IS WHERE PREDICATES ARE REPORTED
// -----------------------------------------------------------------------------------------
// A flow whose elements carry a derived predicate is NOT discarded. Discarding it would remove
// from the result set precisely the returns the mechanical spurious test exists to classify, and
// would make a spurious count of zero a property of the filter rather than a measurement. So
// every flow that can be attributed to an entry point and a sink is emitted as a return, with
// `predicates_on_path` populated from what the graph carries, and the number of flows whose own
// elements carried a predicate is reported in `diagnostics` beside it as a separate measure.
//
// `predicates_on_path` is computed over the emitted path, as found, and its reach is wider than
// the flow elements in two ways: the path carries the named entry point and the sink method,
// which no flow element covered, and the check looks at each path node plus one outgoing call
// step from it. A predicate is never added to make a return look as though it has one, and a
// return is never dropped for having one. Whether a predicate lies on a path is a property of
// this formulation and of the path it emitted: a flow that ends at a construction does not
// traverse what a flow continuing past it traverses, and both are correct answers about their own
// path. Applying the spurious definition to those lists is the driver's step, not this script's.
// ===========================================================================================

import io.shiftleft.codepropertygraph.generated.nodes.{AstNode, Call, CfgNode, Expression, Method}
import io.joern.dataflowengineoss.queryengine.EngineContext

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

val CREATE_DRIVER_SELECTOR   = "(.*\\$\\$)?createDriver"
val DRIVER_RUNNER_SELECTOR   = "org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>.*"
val PROCESS_LAUNCH_SELECTOR =
  "(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec).*"
val EXECUTOR_RUNNER_SELECTOR = "org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>.*"

// --- The entry-point discriminator. See WHICH METHODS ARE ENTRY ANCHORS in the header. -----
// The two names the class asks for, the two signatures an `RpcEndpoint` handler declares, and
// the frontend's infix for the synthetic class that carries a partial function's case bodies.
val HANDLER_NAMES = List("receive", "receiveAndReply")
val HANDLER_SIGNATURES = List(
  "scala.PartialFunction()",
  "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)")
val PARTIAL_FUNCTION_INFIX = "$$anonfun$"

// The prefix the frontend gives an operator pseudo-method. Such a call is never evidence about
// which interface a method implements, so it is left out of the evidence the discriminator
// reports, and it is never followed as a source-scope edge.
val OPERATOR_PREFIX = "<operator>"

// --- Source selection: the driver-submission message and the value it carries. --------------
// See WHAT A SOURCE IS in the header. Two type names are pinned here, exactly as the three
// sinks are pinned by name: the driver-submission message the class names, and the driver
// description it carries. Everything else — which accessor reads the description off a message,
// which members of the description carry the command and the jar, and which other deploy
// message carries a description as an internal hand-off — is DERIVED from the graph below.
val SUBMISSION_MESSAGE_TYPE = "org.apache.spark.deploy.DeployMessages$RequestSubmitDriver"
val DRIVER_DESCRIPTION_TYPE = "org.apache.spark.deploy.DriverDescription"

// A method on a deploy-message type whose return type is the driver description is the accessor
// that reads it off the message. A default-argument supplier carries that same return type
// without being a field read, and is excluded by its name prefix.
val DEPLOY_MESSAGE_SELECTOR = "org\\.apache\\.spark\\.deploy\\.DeployMessages\\$.*"
val DEFAULT_ARGUMENT_PREFIX = "copy$default$"

// The command- and jar-bearing members of the description, derived from its own members: a
// method returning a type that carries a command, or a method whose name names a command or a
// jar. A synthetic accessor and a default-argument supplier are excluded.
val DESCRIPTION_COMMAND_TYPE_SELECTOR = "org\\.apache\\.spark\\.deploy\\.Command"
val DESCRIPTION_COMMAND_NAME_SELECTOR = "(?i)(command|jar|jarurl|mainclass|arguments)"

// The two labelled source classes. The first is a value that arrived from a submitter; the
// second is the internal hand-off between two of these endpoints. They are resolved, reported
// and counted separately, and never merged.
val SOURCE_CLASS_SUBMISSION = "driver_submission_message_from_a_submitter"
val SOURCE_CLASS_INTERNAL   = "internal_endpoint_to_endpoint_driver_handoff"

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
// The graph content digest. See THE CHECK AND THE LOAD ARE TIED TOGETHER in the header.
//
// A path and a size are properties of a name; a digest is a property of the bytes. This is what
// closes the window between checking the graph and loading it (CWE-367): the digest is taken at
// the canonical path BEFORE the load and taken again AFTER it, and a difference fails the run
// closed rather than reporting a graph that was checked and a graph that was read as one thing.
//
// The file is half a gigabyte, so it is read in bounded chunks and never held in memory: the
// digest is updated per chunk and only the 32-byte result is retained.
// ===========================================================================================

val GRAPH_DIGEST_ALGORITHM   = "SHA-256"
val GRAPH_DIGEST_CHUNK_BYTES = 8 * 1024 * 1024

/**
 * The lowercase hex digest of a file's CONTENT, prefixed with the algorithm that made it, read
 * in `GRAPH_DIGEST_CHUNK_BYTES` chunks. Any I/O failure propagates to the caller, which reports
 * it through the failure protocol: a digest that could not be taken is never reported as one
 * that matched.
 */
def graphContentDigestOf(filePath: String): String = {
  val digest = java.security.MessageDigest.getInstance(GRAPH_DIGEST_ALGORITHM)
  val source = java.nio.file.Files.newInputStream(java.nio.file.Paths.get(filePath))
  try {
    val buffer    = new Array[Byte](GRAPH_DIGEST_CHUNK_BYTES)
    var bytesRead = source.read(buffer)
    while (bytesRead > 0) {
      digest.update(buffer, 0, bytesRead)
      bytesRead = source.read(buffer)
    }
  } finally source.close()
  GRAPH_DIGEST_ALGORITHM.toLowerCase + ":" +
    digest.digest().map(byte => "%02x".format(byte & 0xff)).mkString
}

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
    //     the other Phase 3 queries and with the gate's coverage check, so a project of this
    //     name is likely to be present already — and a project name is only the last segment of
    //     the input path it was created from, so it is not evidence of what the project holds.
    //     An existing project is therefore opened only when the input path Joern recorded for it
    //     canonicalizes to the same file as the graph this script reads; anything else fails the
    //     run closed.
    // -------------------------------------------------------------------------------------
    stage = "load_graph"
    val projectName = CPG_PATH.split('/').last

    val cpgCanonicalPath = canonicalPathOf(CPG_PATH).getOrElse(
      throw new RuntimeException(
        "the graph this script is contracted to read does not resolve to a file: " + CPG_PATH +
          " (relative to the working directory, which must be the directory containing " +
          "`harness/`)"))
    val cpgSizeBytes = java.nio.file.Files.size(java.nio.file.Paths.get(cpgCanonicalPath))

    // The content digest is taken BEFORE the load and re-taken after it, so the file that was
    // checked and the file that was read are established to be the same bytes rather than the
    // same name. See THE CHECK AND THE LOAD ARE TIED TOGETHER in the header.
    val cpgDigestBeforeLoad = graphContentDigestOf(cpgCanonicalPath)

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

    // The other half of the provenance test. Re-taking the digest and the size at the same
    // canonical path after the load is what makes the check apply to the bytes that were read:
    // a swap in the window between them would otherwise pass a check on one file and load
    // another. A difference in either fails the run closed, naming both values.
    val cpgDigestAfterLoad = graphContentDigestOf(cpgCanonicalPath)
    val cpgSizeAfterLoad = java.nio.file.Files.size(java.nio.file.Paths.get(cpgCanonicalPath))
    if (cpgDigestAfterLoad != cpgDigestBeforeLoad || cpgSizeAfterLoad != cpgSizeBytes) {
      throw new RuntimeException(
        "the graph file changed across the check-then-load window, so the graph checked is not " +
          "established to be the graph loaded: " + cpgCanonicalPath + " was digest `" +
          cpgDigestBeforeLoad + "` at " + cpgSizeBytes + " bytes before the load and digest `" +
          cpgDigestAfterLoad + "` at " + cpgSizeAfterLoad + " bytes after it. The run fails " +
          "closed rather than reporting a result against a graph whose identity is unverified")
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
          "content_digest"         -> jsonString(cpgDigestBeforeLoad),
          "content_digest_reverified_after_load" ->
            jsonBool(cpgDigestAfterLoad == cpgDigestBeforeLoad && cpgSizeAfterLoad == cpgSizeBytes),
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
          "digest_verification_rule" -> jsonString(
            "the content digest above is taken at the canonical path before the load and taken " +
              "again after it, and a difference in either the digest or the size fails the run " +
              "closed: that is what ties the graph checked to the graph read across the " +
              "check-then-load window. What the digest proves depends on which branch ran, so " +
              "both are stated. On `imported_persisted_cpg` it is the digest of the file " +
              "`importCpg` read. On `opened_existing_project` it is the digest of the pinned " +
              "source file whose identity the project's recorded input path ties the project to " +
              "— not of the project's own copy, which is a separate artifact that applying an " +
              "overlay legitimately changes, exactly as the size rule above states. No expected " +
              "digest is hardcoded here and none is compared against any record: the digest " +
              "detects a change across the window and records what was read, and nothing else"),
          "outcome" -> jsonString(provenanceOutcome)),
        "    "))
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
    //     to outer classes alone would silently miss the endpoints that sit on inner classes —
    //     and then qualified structurally by the three tests in WHICH METHODS ARE ENTRY ANCHORS,
    //     which is what separates a declared standalone RPC handler from a same-named plugin
    //     method, a YARN endpoint and an inherited trait default. Every anchor is reported with
    //     the node count it resolved to, zero counts included, and every candidate the
    //     discriminator rejected is reported with the rule that rejected it.
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
            "a trait's static forwarder remains a traversal bridge — a flow may run through " +
              "one, and the traversal block reports when one did — it is only barred from being " +
              "a place a flow STARTS"),
          "why_it_matters_to_this_formulation" -> jsonString(
            "the third test looks for the very partial-function class this query's sources live " +
              "inside, so an entry point that fails it has no source to select in any case")),
        "    "))

    diagnostics.append(
      "handler_anchors" -> jsonBlockArray(handlerAnchors.map(renderAnchor), "    "))

    val handlerMethods = resolved(handlerAnchors.flatMap(_.methods))

    // -------------------------------------------------------------------------------------
    // (7) Source nodes: the driver-submission message and the command-bearing value it carries,
    //     per entry point. See WHAT A SOURCE IS in the header. Sources are bound to the pinned
    //     submission message by type evidence the handler's own body scope carries; the erased
    //     `java.lang.Object` parameter of a synthetic partial-function body is never a source,
    //     because every partial function has one whatever it handles.
    //
    //     Rule A — a value whose static type IS a qualifying message: the cast the pattern match
    //              compiles to, and any identifier carrying that type.
    //     Rule B — a call to the derived description accessor OF a qualifying message.
    //     Rule C — a call to a derived command- or jar-bearing member of the description.
    //
    //     Two source classes are kept apart and never merged: the pinned submission message,
    //     which is a value that arrived from a submitter, and any other deploy message carrying
    //     a description accessor, which is the internal hand-off from one of these endpoints to
    //     another. A rule-C node is attributed to whichever class the handler's scope carries
    //     evidence for; where a scope carries both, the rule-C nodes there are not admitted and
    //     the scope is named, because attributing them either way would be a guess.
    // -------------------------------------------------------------------------------------
    stage = "resolve_source_nodes"

    // The accessor that reads a driver description off a deploy message, derived from the graph:
    // a method on a deploy-message type whose return type is the description. A default-argument
    // supplier carries that same return type without being a field read, and is excluded by its
    // name prefix.
    val descriptionAccessorMethods = cpg.method
      .where(_.typeDecl.fullName(DEPLOY_MESSAGE_SELECTOR))
      .l
      .filter(_.methodReturn.typeFullName == DRIVER_DESCRIPTION_TYPE)
      .filterNot(_.name.startsWith(DEFAULT_ARGUMENT_PREFIX))
      .distinctBy(_.fullName)
      .sortBy(_.fullName)

    val submissionAccessors = descriptionAccessorMethods
      .filter(m => ownerOf(m) == SUBMISSION_MESSAGE_TYPE)
      .map(_.fullName)
    val internalAccessors = descriptionAccessorMethods
      .filterNot(m => ownerOf(m) == SUBMISSION_MESSAGE_TYPE)
      .map(_.fullName)
    val submissionAccessorSet = submissionAccessors.toSet
    val internalAccessorSet   = internalAccessors.toSet

    // The message types each class covers: the pinned submission message, and every OTHER
    // deploy-message type that carries a description accessor.
    val submissionMessageTypes = List(SUBMISSION_MESSAGE_TYPE)
    val internalMessageTypes = descriptionAccessorMethods
      .map(ownerOf)
      .filter(_.nonEmpty)
      .filterNot(_ == SUBMISSION_MESSAGE_TYPE)
      .distinct
      .sorted

    // The command- and jar-bearing members of the description, derived from its own members by
    // return type or by name, with a default-argument supplier excluded the same way.
    val descriptionMembers = cpg.typeDecl
      .fullNameExact(DRIVER_DESCRIPTION_TYPE)
      .method
      .l
      .filterNot(_.name.startsWith(DEFAULT_ARGUMENT_PREFIX))
      .filter(m =>
        m.methodReturn.typeFullName.matches(DESCRIPTION_COMMAND_TYPE_SELECTOR) ||
          m.name.matches(DESCRIPTION_COMMAND_NAME_SELECTOR))
      .map(_.fullName)
      .distinct
      .sorted
    val descriptionMemberSet = descriptionMembers.toSet

    if (submissionAccessors.isEmpty) {
      throw new RuntimeException(
        "no accessor returning `" + DRIVER_DESCRIPTION_TYPE + "` was found on the pinned " +
          "driver-submission message type `" + SUBMISSION_MESSAGE_TYPE + "` in the loaded " +
          "graph, so this formulation has no source to bind to and a zero result would be a " +
          "property of the selector rather than of the code. Either the pinned type name does " +
          "not exist in this graph or the module carrying it produced no bytecode")
    }

    // The type declaration full names of the graph, materialized once. A handler's body scope is
    // every type whose full name begins with that handler's own partial-function prefix, which
    // reaches nested case bodies too because their names extend the same prefix.
    val allTypeDeclFullNames = cpg.typeDecl.fullName.l.distinct

    val sourceNodeHandler          = scala.collection.mutable.HashMap.empty[Long, String]
    val sourceNodeClass            = scala.collection.mutable.HashMap.empty[Long, String]
    val sourceNodes                = scala.collection.mutable.ListBuffer.empty[CfgNode]
    val partialFunctionTypes       = scala.collection.mutable.TreeSet.empty[String]
    val partialFunctionConnections = scala.collection.mutable.TreeSet.empty[String]
    val partialFunctionBodyNames   = scala.collection.mutable.TreeSet.empty[String]
    val renderedSources            = scala.collection.mutable.ListBuffer.empty[String]
    val handlersWithoutSource      = scala.collection.mutable.TreeSet.empty[String]
    val scopesWithBothClasses      = scala.collection.mutable.TreeSet.empty[String]
    var submissionSourceNodeCount  = 0
    var internalSourceNodeCount    = 0
    var descriptionMemberNodesHeld = 0

    handlerMethods.foreach { handler =>
      // The handler's own body scope: the handler itself, plus every method of every type whose
      // full name begins with the handler's own partial-function prefix. A nested case body's
      // type name extends that same prefix, so the scope is transitive by construction.
      val scopePrefix = ownerOf(handler) + PARTIAL_FUNCTION_INFIX + handler.name + "$"
      val scopeTypes  = allTypeDeclFullNames.filter(_.startsWith(scopePrefix)).sorted
      val scopeBodies = scopeTypes.flatMap(methodsOfType).distinctBy(_.fullName).sortBy(_.fullName)
      val scopeMethods = (handler :: scopeBodies).distinctBy(_.fullName).sortBy(_.fullName)

      scopeTypes.foreach(partialFunctionTypes.add)
      scopeBodies.foreach { body =>
        partialFunctionBodyNames.add(body.fullName)
        partialFunctionConnections.add(handler.fullName + " ==> " + body.fullName)
      }

      // Rule A — values whose static type IS a qualifying message.
      def typedValuesOf(messageTypes: List[String]): List[Expression] =
        messageTypes.flatMap { messageType =>
          scopeMethods.flatMap { scopeMethod =>
            val casts = scopeMethod.ast.isCall.l
              .filter(_.typeFullName == messageType)
              .filter(_.name.startsWith(OPERATOR_PREFIX))
            val identifiers = scopeMethod.ast.isIdentifier.l.filter(_.typeFullName == messageType)
            casts.map(node => node: Expression) ++ identifiers.map(node => node: Expression)
          }
        }

      // Rule B — calls to the derived description accessor of a qualifying message.
      def accessorCallsOf(accessorSet: Set[String]): List[Call] =
        scopeMethods.flatMap(_.call.l).filter(c => accessorSet.contains(c.methodFullName))

      val submissionTypedValues  = typedValuesOf(submissionMessageTypes)
      val submissionAccessorHits = accessorCallsOf(submissionAccessorSet)
      val internalTypedValues    = typedValuesOf(internalMessageTypes)
      val internalAccessorHits   = accessorCallsOf(internalAccessorSet)

      val submissionEvidence = submissionTypedValues.nonEmpty || submissionAccessorHits.nonEmpty
      val internalEvidence   = internalTypedValues.nonEmpty || internalAccessorHits.nonEmpty

      // Rule C — calls to a derived command- or jar-bearing member of the description. Held only
      // where exactly one of the two classes has evidence in this scope, so the attribution is a
      // fact about the scope rather than a choice; where both do, they are named and dropped.
      val descriptionMemberHits =
        scopeMethods.flatMap(_.call.l).filter(c => descriptionMemberSet.contains(c.methodFullName))
      val descriptionMemberClass =
        if (submissionEvidence && !internalEvidence) Some(SOURCE_CLASS_SUBMISSION)
        else if (internalEvidence && !submissionEvidence) Some(SOURCE_CLASS_INTERNAL)
        else None
      if (descriptionMemberHits.nonEmpty && submissionEvidence && internalEvidence) {
        scopesWithBothClasses.add(handler.fullName)
      }

      val classified: List[(CfgNode, String)] =
        submissionTypedValues.map(node => (node: CfgNode, SOURCE_CLASS_SUBMISSION)) ++
          submissionAccessorHits.map(node => (node: CfgNode, SOURCE_CLASS_SUBMISSION)) ++
          internalTypedValues.map(node => (node: CfgNode, SOURCE_CLASS_INTERNAL)) ++
          internalAccessorHits.map(node => (node: CfgNode, SOURCE_CLASS_INTERNAL)) ++
          (descriptionMemberClass match {
            case Some(sourceClass) =>
              descriptionMemberHits.map(node => (node: CfgNode, sourceClass))
            case None => Nil
          })

      classified.foreach { case (node, sourceClass) =>
        // A node reached by two rules is one source node; the first rule that claimed it in the
        // fixed order above owns it, so the attribution does not depend on iteration order.
        if (!sourceNodeHandler.contains(node.id)) {
          sourceNodeHandler.put(node.id, handler.fullName)
          sourceNodeClass.put(node.id, sourceClass)
          sourceNodes.append(node)
          if (sourceClass == SOURCE_CLASS_SUBMISSION) submissionSourceNodeCount += 1
          else internalSourceNodeCount += 1
        }
      }
      if (descriptionMemberClass.isDefined) descriptionMemberNodesHeld += descriptionMemberHits.size

      if (classified.isEmpty) handlersWithoutSource.add(handler.fullName)

      renderedSources.append(
        jsonObject(Seq(
          "handler"                        -> jsonString(handler.fullName),
          "scope_prefix"                   -> jsonString(scopePrefix),
          "scope_types"                    -> jsonStringArray(scopeTypes),
          "scope_methods"                  -> jsonInt(scopeMethods.size),
          "rule_a_submission_typed_values" -> jsonInt(submissionTypedValues.size),
          "rule_b_submission_accessor_calls" -> jsonInt(submissionAccessorHits.size),
          "rule_a_internal_typed_values"   -> jsonInt(internalTypedValues.size),
          "rule_b_internal_accessor_calls" -> jsonInt(internalAccessorHits.size),
          "rule_c_description_member_calls" -> jsonInt(descriptionMemberHits.size),
          "rule_c_attributed_to" -> jsonString(
            descriptionMemberClass.getOrElse(
              if (descriptionMemberHits.isEmpty) "not_applicable_no_description_member_call"
              else "not_attributable_both_or_neither_class_has_evidence_in_this_scope")),
          "source_classes_present" -> jsonStringArray(
            (if (submissionEvidence) List(SOURCE_CLASS_SUBMISSION) else Nil) ++
              (if (internalEvidence) List(SOURCE_CLASS_INTERNAL) else Nil)),
          "resolved_source_nodes" -> jsonInt(classified.map(_._1.id).distinct.size),
          "erased_object_parameter_admitted" -> jsonBool(false))))
    }

    val sourceNodeList = sourceNodes.toList

    diagnostics.append(
      "source_nodes" -> jsonBlockObject(
        Seq(
          "binding" -> jsonString(
            "sources are bound to the pinned driver-submission message type and the pinned " +
              "driver-description type by type evidence inside each entry point's own body " +
              "scope; the erased `java.lang.Object` parameter of a synthetic partial-function " +
              "body is NEVER a source, because every partial function compiled over a bytecode " +
              "frontend has one whatever messages it handles, so admitting it would make every " +
              "entry point a driver-submission source"),
          "pinned_submission_message_type" -> jsonString(SUBMISSION_MESSAGE_TYPE),
          "pinned_driver_description_type" -> jsonString(DRIVER_DESCRIPTION_TYPE),
          "body_scope_rule" -> jsonString(
            "the entry point itself, plus every method of every type whose full name begins " +
              "with the entry point's own enclosing type, `" + PARTIAL_FUNCTION_INFIX + "` and " +
              "the entry point's own name — which reaches a nested case body too, because its " +
              "type name extends that same prefix"),
          "rule_a_message_typed_value" -> jsonString(
            "an operator call in that scope whose static type is a qualifying message type — the " +
              "cast a pattern match compiles to — or an identifier carrying that type"),
          "rule_b_description_accessor_call" -> jsonString(
            "a call in that scope to a method on a deploy-message type whose return type is `" +
              DRIVER_DESCRIPTION_TYPE + "`, excluding a default-argument supplier by its `" +
              DEFAULT_ARGUMENT_PREFIX + "` name prefix"),
          "rule_c_description_member_call" -> jsonString(
            "a call in that scope to a member of `" + DRIVER_DESCRIPTION_TYPE + "` whose return " +
              "type matches " + DESCRIPTION_COMMAND_TYPE_SELECTOR + " or whose name matches " +
              DESCRIPTION_COMMAND_NAME_SELECTOR + " — the command- and jar-bearing values, " +
              "admitted only where exactly one source class has evidence in the same scope"),
          "message_accessor_selector" -> jsonString(
            "enclosing type matches " + DEPLOY_MESSAGE_SELECTOR + " and return type is " +
              DRIVER_DESCRIPTION_TYPE),
          "source_class_submission" -> jsonBlockObject(
            Seq(
              "label" -> jsonString(SOURCE_CLASS_SUBMISSION),
              "meaning" -> jsonString(
                "a value carried by the pinned driver-submission message — the message a " +
                  "submitter sends, which is the class the probe was asked about"),
              "message_types"      -> jsonStringArray(submissionMessageTypes),
              "resolved_accessors" -> jsonStringArray(submissionAccessors),
              "source_nodes"       -> jsonInt(submissionSourceNodeCount)),
            "      "),
          "source_class_internal" -> jsonBlockObject(
            Seq(
              "label" -> jsonString(SOURCE_CLASS_INTERNAL),
              "meaning" -> jsonString(
                "a value carried by a deploy message OTHER than the submission message that " +
                  "also carries a driver description — the internal hand-off from one of these " +
                  "endpoints to another. Reported and counted separately and never folded into " +
                  "the submission set, because a value that arrived on an internal message did " +
                  "not arrive from a submitter"),
              "message_types"      -> jsonStringArray(internalMessageTypes),
              "resolved_accessors" -> jsonStringArray(internalAccessors),
              "source_nodes"       -> jsonInt(internalSourceNodeCount)),
            "      "),
          "resolved_description_members" -> jsonStringArray(descriptionMembers),
          "description_member_source_nodes_held" -> jsonInt(descriptionMemberNodesHeld),
          "scopes_where_a_description_member_read_was_not_attributable" ->
            jsonStringArray(scopesWithBothClasses.toList),
          "per_handler"                 -> jsonBlockArray(renderedSources.toList, "      "),
          "total_resolved_source_nodes" -> jsonInt(sourceNodeList.size),
          "handlers_with_no_qualifying_source" -> jsonStringArray(handlersWithoutSource.toList),
          "handlers_with_no_qualifying_source_note" -> jsonString(
            "an entry point named here carries no value of the pinned message type and no call " +
              "to a derived description accessor anywhere in its body scope, so this " +
              "formulation has no source for it. It is named rather than given the erased " +
              "partial-function parameter as a stand-in")),
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
    var flowsWithPredicate   = 0
    var flowsUnattributed    = 0
    var deepestFlowMethods   = 0
    var boundChangedOutcome  = false
    val flowsBySourceClass   = scala.collection.mutable.HashMap.empty[String, Int]

    /**
     * The derived predicates the flow itself carries: a call to one, or an element inside one.
     * This is a MEASURE, not a filter — a flow carrying one is emitted like any other, and the
     * count of such flows is reported beside the returns. Filtering them out here would remove
     * from the result set exactly the returns the mechanical spurious test exists to classify.
     */
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

      var anchorWithPredicate = 0
      var anchorUnattributed  = 0
      var anchorContributed   = 0

      configuredFlows.foreach { flow =>
        val elements    = flow.elements
        val flowMethods = elements.collect { case node: CfgNode => node.method.fullName }.distinct
        if (flowMethods.size > deepestFlowMethods) deepestFlowMethods = flowMethods.size

        val handlerName = elements.flatMap(node => sourceNodeHandler.get(node.id)).headOption
        val sinkName    = elements.reverse.flatMap(node => sinkNodeMethod.get(node.id)).headOption
        val sourceClass = elements.flatMap(node => sourceNodeClass.get(node.id)).headOption

        (handlerName, sinkName) match {
          case (Some(handlerFullName), Some(sinkFullName)) =>
            // Every attributable flow is emitted. A flow whose own elements carry a derived
            // predicate is counted here as a measure and emitted like any other — the mechanical
            // spurious test is applied by the driver to `predicates_on_path`, and it cannot run
            // on a return this query removed.
            if (predicatesOnFlow(elements).nonEmpty) {
              anchorWithPredicate += 1
              flowsWithPredicate += 1
            }
            sourceClass.foreach(label =>
              flowsBySourceClass.put(label, flowsBySourceClass.getOrElse(label, 0) + 1))

            val path = composePath(handlerFullName, sinkFullName, elements)

            // The second pass, computed over the emitted path as found: a path node that is
            // itself a predicate, plus the predicates each path node calls directly. A return is
            // emitted whatever this finds, and nothing is added to it that the graph does not
            // carry.
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
            "flows_whose_elements_carry_a_predicate" -> jsonInt(anchorWithPredicate),
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
          "flows_whose_elements_carry_a_predicate"       -> jsonInt(flowsWithPredicate),
          "flows_emitted_by_source_class" -> jsonBlockObject(
            List(SOURCE_CLASS_SUBMISSION, SOURCE_CLASS_INTERNAL).map(label =>
              label -> jsonInt(flowsBySourceClass.getOrElse(label, 0))),
            "      "),
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
          "no_flow_filter" -> jsonString(
            "no flow is discarded for carrying a predicate. `flows_whose_elements_carry_a_" +
              "predicate` above is a measure over the flow's own elements — an element that is a " +
              "call to a derived predicate, or an element sitting inside one — and every one of " +
              "those flows is emitted as a return like any other. Filtering them would remove " +
              "from the result set exactly the returns the mechanical spurious test exists to " +
              "classify, and would make a spurious count of zero a property of this query rather " +
              "than a measurement"),
          "predicate_check_reach" -> jsonString(
            "the emitted path nodes, plus one outgoing call step from each of them — wider than " +
              "the element-level measure, and over a path that carries the entry point and the " +
              "sink method which no flow element covered"),
          "spurious_determination" -> jsonString(
            "not made here. `predicates_on_path` is reported per return and the mechanical " +
              "on-path test is applied downstream: a return is spurious when an authentication " +
              "or ACL predicate lies on the path from the entry point to the sink, and for no " +
              "other reason")),
        "    "))

    // -------------------------------------------------------------------------------------
    // (11) The result region: the BEGIN marker, one JSON object, the END marker, and nothing
    //      else between them. It is inside this block, and last, on purpose — the whole document
    //      is built as one string first, so the markers are printed only once a complete result
    //      exists, and any failure above leaves this unreached.
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

