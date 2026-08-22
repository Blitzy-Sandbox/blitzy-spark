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
// With no parameter supplied, the two defaults select exactly the ends the class names: the
// declared standalone-deploy RPC handlers named `receive` or `receiveAndReply`, and the three
// sinks — `createDriver`, a `DriverRunner` construction, and a process launch — together with a
// fourth anchor for an `ExecutorRunner` construction, which is carried in addition to those three
// and never in place of one of them.
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
// Each is declared with an EMPTY default at the invocation boundary, and the declared default
// below is substituted internally when the caller supplied nothing. That is what makes "supplied"
// and "not supplied" two distinguishable facts rather than a guess from value equality: a caller
// who passes exactly the declared default is recorded as having supplied it, which a comparison
// against the default could not do. See PARAMETER PROVENANCE below.
//
//   handlerPattern : string
//       Default: `receive=org\.apache\.spark\.deploy\.(?!yarn\.).*\.receive:`
//                `scala\.PartialFunction\(\);`
//                `receiveAndReply=org\.apache\.spark\.deploy\.(?!yarn\.).*\.receiveAndReply:`
//                `scala\.PartialFunction\(org\.apache\.spark\.rpc\.RpcCallContext\)`
//       Generalizes over: the identity of the entry point. One or more labelled alternatives
//       (format below), each an anchored full-match regex over a method's full name, so an
//       entry point on an inner or anonymous type is selected by the same rule as one on a
//       top-level type. Replacing this value asks the same question of a different handler.
//       The default carries the SIGNATURE an `RpcEndpoint` handler declares and excludes the
//       YARN sub-package, because the class this probe was asked about is standalone deploy mode
//       and a same-named driver-plugin method or YARN endpoint is a different interface. See
//       WHICH ENTRY POINTS THE DEFAULT SELECTS.
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
//       runs through a construction, a deferred body and a launch chain below it. It is bounded
//       above as well as below: see THE INPUT LIMITS.
//
// Nothing else is a parameter, and two omissions are deliberate. The graph path and the
// workspace path are fixed constants: they are what this script reads and where it reads it,
// not something to generalize across. The authentication/ACL predicate set is derived from the
// graph at execution time and is deliberately not a parameter, so that a predicate added or
// renamed in the tree the graph was built over cannot be missed and a caller cannot narrow the
// check that makes this a query for the UNGUARDED class.
//
// PARAMETER PROVENANCE, AND WHY THE DECLARED DEFAULT IS NOT THE SENTINEL
// ---------------------------------------------------------------------
// Joern hands a `--param` value over as a string and exposes no metadata saying whether one was
// passed, so provenance has to be built rather than read. Comparing the value in force against
// the declared default does NOT establish it: a caller who passes the declared default verbatim
// is indistinguishable from one who passes nothing, and the earlier revision of this script
// reported such a caller as `default_value`, which was false. Instead each parameter is DECLARED
// with a SENTINEL no invocation can deliver, and the declared default is substituted internally
// when that sentinel arrives. `supplied_by_the_caller` and `declared_default_not_supplied` are
// therefore facts about the invocation.
//
// The sentinel is a NUL-bearing string rather than the empty string, and the difference is not
// cosmetic. An empty `--param handlerPattern=` arrives here as an empty string, so an empty
// sentinel would report a caller who passed one as not having supplied anything — the same class
// of falsehood as inferring provenance from equality, just rarer. A process argument cannot
// contain a NUL, so this sentinel is unsuppliable by construction: every value a caller can pass
// is recorded as supplied, and an empty pattern or an empty depth is then refused on its merits
// rather than being silently replaced by a default. `provenance_rule` in the output states this.
//
// CALLER-SUPPLIED VALUES ARE NEVER ECHOED (CWE-200, CWE-532)
// ---------------------------------------------------------
// A parameter value is the one input that does not come from this file or from the graph, so it
// is the one value that could carry something a caller did not intend to publish. The result
// object and every failure message therefore carry, for a caller-supplied value, only its
// PARAMETER NAME, its length in characters, and a SHA-256 digest of its UTF-8 bytes — never the
// text. A literal is echoed only where the value in force is the declared default authored in
// this file, which carries nothing of the caller's.
//
// That reaches every place a value would otherwise surface, and each is closed here rather than
// left to the reader to check: the parameter block; the parsed alternatives, whose labels and
// regexes are the caller's text; the per-anchor `label` and `selector`; the explanation of a run
// that returned nothing, which names alternatives; and every validation failure message. Where a
// label is withheld, a positional stand-in of the form `<parameter>#<position>` names the
// alternative instead, so a reader can still tell which alternative a count belongs to and can
// prove a claimed invocation by digest. `redaction` in the output states the policy in force.
//
// One value is exempt and says so in the output: the numeric bound in force is emitted as an
// integer even when a caller supplied it, because every count in the result is a count under that
// bound and a result that hid it could not be read. The caller's raw text for it is still withheld
// and digested, and no pattern value is emitted under any circumstances.
//
// One boundary is outside this script and is stated rather than implied, because a report written
// against this policy would otherwise promise more than the code can deliver: the Joern script
// runner prints `executing <script> with params=Map(...)` to stderr BEFORE this script's first
// statement, so a captured stderr log holds the invocation as the runner echoed it. Nothing this
// script emits carries a caller's value, and a script cannot suppress its interpreter's banner —
// so a consumer that must not disclose a parameter withholds or filters that captured line.
// `redaction.outside_this_scripts_control` says the same thing in the output.
//
// THE INPUT LIMITS (CWE-400, CWE-1333)
// ------------------------------------
// A parameter is caller-controlled and this graph holds hundreds of thousands of methods, so an
// unbounded pattern, an unbounded alternative count, an unbounded resolved node set or an
// unbounded depth is a denial-of-service surface, and an ambiguously quantified regex is a
// catastrophic-backtracking surface. Every limit is a named constant below, is stated in
// `diagnostics.input_limits`, and is ENFORCED BY REJECTION: an out-of-bounds value ends the run
// through the failure protocol with a message naming the parameter and the limit it exceeded.
// Nothing is silently clamped, because a clamped run answers a question the caller did not ask
// while looking like one that answered theirs.
//
// The resolved-cardinality limits are checked after the anchors resolve and BEFORE any traversal
// starts, since the traversal is the expensive part and a pattern resolving to tens of thousands
// of methods is the case that has to be refused rather than attempted.
//
// WHICH ENTRY POINTS THE DEFAULT SELECTS, AND THE ONE STRUCTURAL RULE THAT APPLIES TO ANY PAIR
// --------------------------------------------------------------------------------------------
// Selecting an entry point by name and package alone resolves methods that are not declared RPC
// handlers: a driver-plugin method that happens to share the name, a YARN endpoint outside
// standalone mode, and a trait's inherited default that has no declaration of its own and whose
// body is a forwarder. The first two are excluded by the DEFAULT PATTERN, which carries the
// handler signature and excludes the YARN sub-package — they are properties of the pair a caller
// names, so they belong in the parameter where a caller can change them.
//
// The third cannot be expressed as a regex over a method's full name, because an inherited
// default and a declaration are indistinguishable by name and signature. It is therefore a
// STRUCTURAL rule, and it is written so that it generalizes rather than hardcoding this pair: a
// resolved entry point whose signature says it IS a Scala partial function is additionally
// required to allocate the synthetic partial-function class that carries its own case bodies —
// an outgoing call into a type named for its own enclosing type, the frontend's partial-function
// infix and its own name. An inherited default allocates nothing of the kind; its only call is
// the trait's static forwarder. A resolved entry point of any OTHER signature is admitted
// untouched, so a caller naming a method that is not a partial function is unaffected by this
// rule.
//
// Every candidate the rule removes is reported in `diagnostics.handler_qualification` with the
// evidence it was read on, because the exclusions are the evidence that the rule did its work. A
// trait's static forwarder remains a legitimate TRAVERSAL BRIDGE — a path may still run through
// one, and the traversal block reports when one did; it simply stops being a place a path may
// START.
//
// The two kinds of narrowing leave their evidence in two different places, and both are recorded:
// what the STRUCTURAL rule removed is in `handler_qualification`, and what the PATTERN never
// nominated is in `pinned_class_handler_census`, which lists every method of either name the
// pinned class uses, under its package, that the graph holds — nominated or not — with its
// signature and enclosing type as the graph reports them. So a reader can see that a same-named
// driver-plugin method or a YARN endpoint was present and was not selected, and can compare the
// graph facts against the pattern in force rather than inferring the difference from a count. The
// census selects nothing; it is evidence about the class, not part of the query.
//
// ONE RETURN PER ROUTE, NOT ONE PER SINK
// --------------------------------------
// A sink is commonly reachable from an entry point by more than one route, and the routes differ
// in exactly the thing this probe is about: which methods lie between the two ends, and which
// predicates lie on them. A traversal that keeps one predecessor per method — the first one it
// happened to discover — emits one route per (entry point, sink) pair and silently discards the
// rest, so a route through one construction can hide a route through another, and a route with no
// predicate on it can hide one that has a predicate (or the reverse). This script therefore
// enumerates routes in two passes:
//
//   * FORWARD, once per entry point: a level-synchronous frontier that expands each method at
//     most once but records EVERY edge it observes, as a set of predecessors per method rather
//     than a single one. Each method is expanded once, so the enumeration is over the edge set
//     that bounded frontier observed; a route needing a method expanded a second time, deeper
//     than its first discovery, is outside what this pass observes, and that is stated here
//     rather than left to be discovered.
//   * BACKWARD, once per (entry point, sink) pair: every distinct SIMPLE ordered route over those
//     edges, within the caller's depth bound, enumerated in predecessor-name order so the output
//     is reproducible. Deduplication is on the exact emitted tuple — entry point, sink, ordered
//     path and predicates found on it — never on the sink method, so two genuinely different
//     routes to one sink are two returns and two identical ones are one.
//
// Enumeration is BOUNDED by the same named constants as every other limit, and every bound is
// reported in `diagnostics.route_enumeration` with whether it was reached: a cap on routes per
// pair (with an exact `routes_beyond_the_cap_exist` flag, established by looking for one route
// past the cap), a cap on the backward-search steps a single pair may take, and a cap on the
// total returns one run may emit. A bound that fires is part of the answer, so it is named with
// the pairs it fired on rather than smoothed over.
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
// A label is bounded in length and an alternative's regex is bounded in length, in count and in
// quantifier structure: see THE INPUT LIMITS. A pattern is compiled before the graph is loaded,
// so a malformed one is refused without any graph work.
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
//  2. Parameter validation runs next, BEFORE the workspace is switched and before anything is
//     loaded, so a rejected value costs no graph work and is reported against its own stage. A
//     rejected parameter ends the run through the failure protocol below: start marker printed,
//     no result region, non-zero exit. The message names the parameter, the alternative's
//     position and the limit or rule it violated — never the value, which is reported by digest.
//  3. `switchWorkspace` is called BEFORE any load. It closes the current workspace and opens
//     another, so a load performed first would be discarded by it.
//  4. The load is idempotent AND provenance-checked. The workspace is persistent scratch shared
//     with the other Phase 3 queries and with the environment gate's own coverage check, so by
//     the time this script runs a project of this name will very likely already exist — but a
//     project's NAME is derived from its input path's last segment and is therefore not evidence
//     of its contents. Before an existing project is opened, the input path Joern recorded for it
//     when it was created is canonicalized and compared with the canonical path of the graph this
//     script is contracted to read; a mismatch, or a recorded path that no longer canonicalizes,
//     FAILS THE RUN CLOSED rather than substituting a stale graph for the pinned one. On a match
//     the verified identity — the canonical graph path, its size in bytes, its content digest,
//     and the project's own recorded input path — is recorded in `diagnostics.graph_identity`.
//     Which of import or open happened is recorded in `diagnostics.load_mode`. Opening an
//     existing project is still reading.
//  5. The resolved-cardinality limits are checked after the anchors resolve and before the
//     traversal starts, so an over-broad pattern is refused rather than attempted.
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
// graph work completes, and the whole document is built as one string, BEFORE the BEGIN marker is
// printed: the markers are emitted only once a complete result exists.
//
// On failure: NO result region is printed at all. Any failure — a rejected parameter, a
// provenance mismatch, an over-broad pattern, an empty graph, an exception from any traversal —
// is written to STDERR as one `---BLITZY-FAILURE---` line naming the stage, the exception type
// and its message, followed by the stack trace, and the exception is then re-raised so the
// process terminates with a non-zero exit status. That combination — start marker present, result
// region absent, exit status non-zero — is what tells the driver a run compiled and did not
// complete. Emitting a result region after a caught failure, as an earlier revision of this
// script did, would have the driver classify a failed or partial run as a successful one, so no
// error path here produces a payload of any kind.
//
// The success object has exactly two top-level keys, and they are the two the other committed
// queries emit:
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
//   parameters          every parameter, with its provenance as a fact about the invocation, its
//                       declared default, a redaction-safe account of the value in force, the
//                       alternatives it parsed into, and what it generalizes over — which is what
//                       makes a parameterized run provable from its own output.
//   redaction           the redaction policy in force, and which fields it governs.
//   input_limits        every limit on a caller's input, with the value enforced, so the policy
//                       is auditable from the output rather than from this file.
//   load_mode           whether the project was imported or opened (see above).
//   workspace           the workspace path selected, the graph path read, and the project name
//   cpg_source          derived from it — carried as `workspace`, `cpg_source` and
//   cpg_project_name    `cpg_project_name`, so a result names what it was produced from.
//   graph_identity      the canonical path of the graph read, its size in bytes, its content
//                       digest taken before the load and re-verified after it, the input path
//                       the workspace project recorded, and how each was compared.
//   cpg_method_count    method count read from the loaded graph; evidence it loaded non-empty.
//   derived_predicates  the authentication/ACL predicate set, derived from the graph at
//                       execution time and never hardcoded, with every exclusion rule that
//                       fired and exactly what each removed.
//   handler_qualification  the structural rule applied to resolved entry points, every candidate
//                       it accepted, and every candidate it excluded with the evidence.
//   pinned_class_handler_census  every method of either name the pinned class uses, under its
//                       package, that the graph holds — with whether the patterns in force
//                       nominated it, whether it was accepted, and its signature and enclosing
//                       type. Evidence only: it selects nothing (see the census constants).
//   handler_anchors     each entry-point alternative with the node count it resolved to, the
//                       names resolved and the returns it contributed — zero counts included.
//   handler_anchors_before_qualification  the same per alternative as the pattern resolved it,
//                       before the structural rule narrowed it, so the rule's effect on each
//                       alternative is readable without subtracting one count from another.
//   sink_anchors        the same, per sink alternative.
//   resolved_cardinality  what each end resolved to against the limits, checked before any
//                       traversal ran.
//   bridges             per boundary the traversal has to cross: whether the rule fired, what it
//                       connected, and whether an emitted path actually needed it.
//   route_enumeration   every bound the route enumeration applies, and, per bound, whether it was
//                       reached and what it left out.
//   traversal           the depth bound a caller asked for and whether it was reached, what the
//                       frontier was allowed to expand through and the measurement behind that
//                       restriction, how routes are enumerated, the reach of the predicate check,
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
//
// Applying the spurious definition to those lists is the driver's step, not this script's: a
// return is spurious when an authentication or ACL predicate lies on the path from the entry
// point to the sink, and for no other reason.
// ===========================================================================================

import io.shiftleft.codepropertygraph.generated.nodes.Method

// --- Paths. Both are relative to the directory that contains `harness/`, and neither is a ---
// --- parameter: they are what this script reads and where it reads it. ---------------------
val WORKSPACE_PATH = "queries/joern/.workspace"
val CPG_PATH       = "harness/cpg/spark.cpg"

// --- The markers of the query-to-driver contract. -------------------------------------------
// The three stdout markers, and the stderr marker that carries a failure. A failure never
// produces a result region, so BEGIN and END are printed on exactly one path through the script.
val MARKER_START   = "---BLITZY-START---"
val MARKER_BEGIN   = "---BLITZY-RESULT-BEGIN---"
val MARKER_END     = "---BLITZY-RESULT-END---"
val MARKER_FAILURE = "---BLITZY-FAILURE---"

// --- The pattern-list format: alternatives separated by one character, each optionally -----
// --- carrying a label ahead of the first occurrence of another. ----------------------------
val ALTERNATIVE_SEPARATOR = ";"
val LABEL_SEPARATOR       = '='
val ADDITIONAL_LABEL_NOTE =
  "a label is a name and nothing more: the script resolves and reports every alternative " +
    "identically, whatever its label says"

// --- The sentinel that makes parameter provenance a fact. See PARAMETER PROVENANCE. ---------
// Every parameter is DECLARED with this value, and the declared default below is substituted
// internally when it arrives. A value that arrives as anything else was supplied by the caller —
// including one equal to the declared default, which a comparison against the default could not
// tell apart from an absent value.
//
// It carries a NUL character, which is what makes it UNSUPPLIABLE rather than merely unlikely: a
// process argument is NUL-terminated, so no `--param` value reaching this script can contain one.
// The empty string was the obvious candidate and is measurably wrong for the job — `--param
// handlerPattern=` delivers an empty string, so an empty sentinel would record a caller who
// passed one as not having supplied anything. With this sentinel every value a caller can pass,
// the empty one included, is recorded as supplied and is then validated on its merits.
val PARAMETER_NOT_SUPPLIED = "\u0000<parameter-not-supplied>"

// --- Parameter defaults. Each reproduces the ends of the class the probe was asked about, --
// --- and each is a pattern rather than a location. -----------------------------------------
// The handler default carries the SIGNATURE an `RpcEndpoint` handler declares and excludes the
// YARN sub-package by negative lookahead, so a same-named driver-plugin method — whose signature
// is the erased universal object type — and a YARN endpoint outside standalone mode are not
// selected. The one remaining distinction, between a declaration and an inherited trait default,
// cannot be drawn by a regex over a full name and is drawn structurally: see WHICH ENTRY POINTS
// THE DEFAULT SELECTS.
val HANDLER_PATTERN_DEFAULT =
  "receive=org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receive:" +
    "scala\\.PartialFunction\\(\\)" +
    ALTERNATIVE_SEPARATOR +
    "receiveAndReply=org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receiveAndReply:" +
    "scala\\.PartialFunction\\(org\\.apache\\.spark\\.rpc\\.RpcCallContext\\)"

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

// --- The signatures a Scala `RpcEndpoint` handler declares. ---------------------------------
// Not a parameter and not a selector: the structural rule in WHICH ENTRY POINTS THE DEFAULT
// SELECTS applies to a resolved entry point whose signature is one of these — a method that IS a
// Scala partial function — and leaves every other resolved entry point untouched, which is what
// keeps the rule general across whatever pair a caller names.
val HANDLER_SIGNATURES = List(
  "scala.PartialFunction()",
  "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)")

// --- The census of the pinned class, which is EVIDENCE AND NEVER A SELECTOR. -----------------
// The default patterns select fewer methods than a name-and-package reading of the class would,
// and the difference is the point: a driver-plugin `receive` and a YARN endpoint share the name
// without being standalone-mode RPC handler declarations. So that the narrowing is auditable
// rather than merely asserted, `diagnostics.pinned_class_handler_census` lists every method of
// either name under the pinned class's package that the graph holds, says whether the patterns in
// force nominated it, and carries its signature and enclosing type as the graph reports them. The
// two names and the package below are used for that listing ONLY. No traversal, no anchor and no
// return depends on them, so a caller pointing the parameters at an entirely different pair still
// gets the same census — read then as "what this run did NOT ask about", which is equally useful.
val CENSUS_HANDLER_NAMES   = List("receive", "receiveAndReply")
val CENSUS_PACKAGE_PREFIX  = "org.apache.spark.deploy."

// ===========================================================================================
// THE INPUT LIMITS. Every one is enforced by rejection, never by a silent clamp, and every one
// is reported in `diagnostics.input_limits` so the policy is auditable from a result rather than
// from this file. See THE INPUT LIMITS in the header for why each exists.
//
// The values are set so that the class this probe was asked about is answered well inside them —
// its default patterns are two and four alternatives of a few dozen characters, resolving to
// single-digit and low-double-digit node counts — and so that a pathological input is refused
// rather than attempted on a graph of hundreds of thousands of methods.
// ===========================================================================================

// Length of a whole parameter value, and of one alternative's regex and label within it.
val MAX_PARAMETER_VALUE_LENGTH   = 4096
val MAX_ALTERNATIVE_REGEX_LENGTH = 512
val MAX_ALTERNATIVE_LABEL_LENGTH = 64

// Alternatives one pattern-list parameter may carry.
val MAX_ALTERNATIVES_PER_PARAMETER = 32

// Regex complexity. A repeating quantifier applied to a group whose body already quantifies is
// the catastrophic-backtracking vector (CWE-1333) and is refused outright — including a `?` inside
// the body, as in `(a?)+`; the count bound refuses a pattern whose complexity is unbounded in the
// other direction.
val MAX_QUANTIFIERS_PER_REGEX = 32

// The depth a caller may ask the traversal to follow. Bounded above as well as below: the work a
// callee traversal does grows with the bound, and an unbounded bound on this graph is a
// denial-of-service surface (CWE-400).
val MIN_MAX_DEPTH = 1
val MAX_MAX_DEPTH = 32

// What one alternative, and one whole end, may resolve to. Checked after the anchors resolve and
// BEFORE any traversal starts, because the traversal is the expensive part.
val MAX_RESOLVED_METHODS_PER_ALTERNATIVE = 256
val MAX_RESOLVED_METHODS_PER_END         = 512

// --- Route-enumeration bounds. See ONE RETURN PER ROUTE in the header. ----------------------
//   MAX_ROUTES_PER_PAIR                  distinct routes emitted per (entry point, sink) pair.
//   MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR predecessor steps the backward search may take for one
//                                        pair before it stops and says so.
//   MAX_RETURNS_TOTAL                    returns one run may emit, over every pair.
val MAX_ROUTES_PER_PAIR                  = 8
val MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR = 200000
val MAX_RETURNS_TOTAL                    = 500

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
//
// This is NOT the redaction digest defined below. The two uses share an algorithm and nothing
// else, and they are deliberately named apart: `graphContentDigestOf` digests a FILE for
// provenance and is reported in full, while `digestOf` digests a caller-supplied STRING so that
// the value itself need never be echoed. Neither is ever substituted for the other.
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
// Redaction of caller-supplied values. See CALLER-SUPPLIED VALUES ARE NEVER ECHOED.
//
// A caller's parameter value never reaches the result object or a failure message as text. What
// reaches them is the parameter's name, the value's length in characters, and a SHA-256 digest of
// its UTF-8 bytes — enough to prove that a claimed invocation is the one that ran, and not enough
// to publish anything the caller did not intend to. A literal is echoed only for a value authored
// in this file, which is what a declared default is.
// ===========================================================================================

val REDACTION_DIGEST_ALGORITHM = "SHA-256"

/** The lowercase hex digest of a value's UTF-8 bytes, prefixed with the algorithm that made it. */
def digestOf(raw: String): String = {
  val bytes = java.security.MessageDigest
    .getInstance(REDACTION_DIGEST_ALGORITHM)
    .digest(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  REDACTION_DIGEST_ALGORITHM.toLowerCase + ":" +
    bytes.map(byte => "%02x".format(byte & 0xff)).mkString
}

/**
 * A caller's value named for a failure message: the parameter, the position within it, and the
 * digest and length of the offending text — never the text. A message assembled this way can be
 * read from a captured stderr log without leaking what the caller passed.
 */
def redactedReference(parameterName: String, position: Int, value: String): String =
  "parameter `" + parameterName + "` alternative " + position + " [length=" +
    value.length + " digest=" + digestOf(value) + "]"

// ===========================================================================================
// Regex safety. See THE INPUT LIMITS.
//
// A caller's regex is matched against every method full name in a graph of hundreds of thousands
// of methods, so a pattern whose matching time is not linear in its input is a denial-of-service
// surface (CWE-1333). The vector is AMBIGUOUS NESTED QUANTIFICATION — a REPEATING quantifier
// applied to a group whose body already contains a quantifier of any kind, as in `(a+)+`,
// `(?:.*)*` or `(a?)+` — where the number of ways to split the input among the repetitions grows
// exponentially. Such a pattern is refused, and so is one carrying more quantifiers than the
// bound above.
//
// The two roles `?` plays are why this scan reads the pattern rather than searching it for
// characters. As a quantifier it makes a group's body ambiguous, so `(a?)+` is as unsafe as
// `(a+)+` and a `?` counts as a quantifier for that test. Directly after `(` it is not a
// quantifier at all but a group prefix — `(?:…)`, `(?=…)`, `(?!…)`, `(?<…` — and the default
// handler pattern's own `(?!yarn\.)` is one of those, so counting it would refuse this script's
// own default. As an OUTER quantifier it is harmless: a group repeating at most once cannot
// backtrack ambiguously against itself, so `(a+)?` is accepted where `(a+)+` is not. The outer
// test therefore reads `*`, `+` and `{`, and the body test reads those plus a `?` that is not a
// group prefix.
//
// The scan is a single pass that tracks escaping, character classes, group nesting and whether a
// group was just opened, because a quantifier inside a character class is a literal, an escaped
// one is not a quantifier at all, and a `?` directly after `(` is a prefix rather than either.
// It is deliberately conservative: it refuses a shape that CAN backtrack catastrophically without
// proving that a given input would make it, since the alternative is to attempt it on this graph.
// ===========================================================================================

/** One reason a regex was refused: the rule, and where in the pattern it fired. */
final case class RegexRefusal(rule: String, detail: String)

/**
 * The unsafe constructs a regex carries, in the order they were found; empty when it is safe.
 *
 * Examples: `unsafeRegexConstructs("(a+)+")` and `unsafeRegexConstructs("(a?)+")` each return one
 * `nested_quantifier` refusal, because both apply a repeating quantifier to a group whose body
 * quantifies. `unsafeRegexConstructs("org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receive:.*")`
 * returns none: the `?` is a group prefix rather than a quantifier, and each `.*` is a single
 * quantifier over a non-group, which cannot backtrack ambiguously against itself.
 */
def unsafeRegexConstructs(pattern: String): List[RegexRefusal] = {
  val refusals    = scala.collection.mutable.ListBuffer.empty[RegexRefusal]
  // For each currently-open group, whether a quantifier has been seen inside it.
  val openGroups  = scala.collection.mutable.Stack.empty[Boolean]
  var quantifiers = 0
  var index       = 0
  var escaped     = false
  var inClass     = false
  // True only while standing on the character directly after an unescaped `(`, where a `?` opens
  // a group prefix — `(?:`, `(?=`, `(?!`, `(?<` — rather than quantifying anything.
  var justOpenedGroup = false

  /** A quantifier that may repeat a group many times, which is what makes nesting ambiguous. */
  def isRepeatingQuantifierChar(c: Char): Boolean = c == '*' || c == '+' || c == '{'

  while (index < pattern.length) {
    val c = pattern.charAt(index)
    if (escaped) {
      escaped = false
      justOpenedGroup = false
    } else if (c == '\\') {
      escaped = true
      justOpenedGroup = false
    } else if (inClass) {
      if (c == ']') inClass = false
      justOpenedGroup = false
    } else {
      c match {
        case '[' =>
          inClass = true
          justOpenedGroup = false
        case '(' =>
          openGroups.push(false)
          justOpenedGroup = true
        case ')' =>
          val bodyHadQuantifier = if (openGroups.nonEmpty) openGroups.pop() else false
          val nextChar          = if (index + 1 < pattern.length) pattern.charAt(index + 1) else ' '
          // Only a REPEATING quantifier on the group is the unsafe shape: `(a+)?` repeats the
          // group at most once and cannot backtrack ambiguously against itself.
          if (bodyHadQuantifier && isRepeatingQuantifierChar(nextChar)) {
            refusals.append(
              RegexRefusal(
                "nested_quantifier",
                "a repeating quantifier `" + nextChar + "` at character " + (index + 2) +
                  " is applied to a group whose body already quantifies, which is the " +
                  "catastrophic-backtracking shape"))
          }
          justOpenedGroup = false
        case '?' if justOpenedGroup =>
          // A group prefix, not a quantifier: `(?:`, `(?=`, `(?!`, `(?<`. Counting it would
          // refuse this script's own default handler pattern, whose `(?!yarn\.)` is one.
          justOpenedGroup = false
        case q if isRepeatingQuantifierChar(q) || q == '?' =>
          quantifiers += 1
          // Every enclosing group now contains a quantifier, so a repeating quantifier applied to
          // any of them is ambiguous, not only one applied to the innermost.
          val marked = openGroups.map(_ => true).toList
          openGroups.clear()
          marked.reverse.foreach(openGroups.push)
          justOpenedGroup = false
        case _ => justOpenedGroup = false
      }
    }
    index += 1
  }

  if (quantifiers > MAX_QUANTIFIERS_PER_REGEX) {
    refusals.append(
      RegexRefusal(
        "too_many_quantifiers",
        "the pattern carries " + quantifiers + " quantifiers and the limit is " +
          MAX_QUANTIFIERS_PER_REGEX))
  }

  refusals.toList
}

// ===========================================================================================
// Parameter parsing and validation.
//
// Joern hands every `--param` value over as a string, so a value that is not a string here is
// parsed and checked by this script rather than by the invocation. Each failure raises with a
// message naming the parameter, the alternative's position and the limit or rule it violated —
// never the value, which is referenced by digest — and every one of them ends the run through the
// failure protocol above: start marker printed, no result region, non-zero exit.
// ===========================================================================================

/**
 * One alternative of a pattern-list parameter, with everything needed to report it without
 * echoing a caller's text.
 *
 * `label` and `pattern` are the real values, used to name an anchor internally and to query the
 * graph. `redacted` is true when the owning parameter's value came from the caller, in which case
 * `reportedLabel` substitutes a positional stand-in and `reportedFields` carries digests rather
 * than text.
 */
final case class PatternAlternative(
    parameterName: String,
    position: Int,
    label: String,
    pattern: String,
    redacted: Boolean) {

  /** The name this alternative is reported under: its own label, or a positional stand-in. */
  def reportedLabel: String =
    if (redacted) parameterName + "#" + position else label

  /** The evidence for this alternative: literal where authored in this file, digests otherwise. */
  def reportedFields: Seq[(String, String)] =
    if (redacted)
      Seq(
        "label"          -> jsonString(reportedLabel),
        "label_withheld" -> jsonBool(true),
        "label_length"   -> jsonInt(label.length),
        "label_digest"   -> jsonString(digestOf(label)),
        "pattern_withheld" -> jsonBool(true),
        "pattern_length"   -> jsonInt(pattern.length),
        "pattern_digest"   -> jsonString(digestOf(pattern)))
    else
      Seq(
        "label"            -> jsonString(label),
        "label_withheld"   -> jsonBool(false),
        "pattern"          -> jsonString(pattern),
        "pattern_withheld" -> jsonBool(false))
}

/**
 * The alternatives a pattern-list parameter carries, in the order the caller wrote them. The
 * label is everything before the first label separator, trimmed; the pattern is everything after
 * it, taken verbatim so that no trimming can alter a regex. An alternative with no label
 * separator is labelled with its own pattern text.
 *
 * Every limit in THE INPUT LIMITS that bears on a pattern list is enforced here, before the graph
 * is loaded, and a violation is refused rather than clamped. `redacted` records whether the value
 * came from the caller, which determines whether the alternatives may be echoed at all.
 */
def parsePatternList(
    parameterName: String,
    raw: String,
    redacted: Boolean): List[PatternAlternative] = {
  if (raw.trim.isEmpty) {
    throw new IllegalArgumentException(
      "parameter `" + parameterName + "` is empty: it must carry at least one alternative of " +
        "the form <label>" + LABEL_SEPARATOR + "<regex>, with alternatives separated by `" +
        ALTERNATIVE_SEPARATOR + "`")
  }
  if (raw.length > MAX_PARAMETER_VALUE_LENGTH) {
    throw new IllegalArgumentException(
      "parameter `" + parameterName + "` is longer than the limit: length " + raw.length +
        " exceeds MAX_PARAMETER_VALUE_LENGTH=" + MAX_PARAMETER_VALUE_LENGTH +
        ". The value is refused rather than truncated, because a truncated pattern would select " +
        "an end the caller did not ask for")
  }

  // The separator is kept in the split so that an empty alternative is rejected rather than
  // silently dropped: a stray separator is a typo, and a query whose ends were not what its
  // caller wrote is worse than one that refused to run.
  val pieces = raw.split(ALTERNATIVE_SEPARATOR, -1).toList

  if (pieces.size > MAX_ALTERNATIVES_PER_PARAMETER) {
    throw new IllegalArgumentException(
      "parameter `" + parameterName + "` carries " + pieces.size + " alternatives and the " +
        "limit is MAX_ALTERNATIVES_PER_PARAMETER=" + MAX_ALTERNATIVES_PER_PARAMETER +
        ". Each alternative is a separate regex matched against every method full name in the " +
        "graph, so the count is bounded; the value is refused rather than truncated")
  }

  val alternatives = pieces.zipWithIndex.map { case (piece, index) =>
    val position = index + 1
    if (piece.trim.isEmpty) {
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` has an empty alternative at position " + position +
          " of " + pieces.size + ": remove the stray `" + ALTERNATIVE_SEPARATOR +
          "` or fill the alternative in")
    }

    val separatorAt = piece.indexOf(LABEL_SEPARATOR.toInt)
    val (label, pattern) =
      if (separatorAt < 0) (piece.trim, piece)
      else (piece.take(separatorAt).trim, piece.drop(separatorAt + 1))

    if (label.isEmpty) {
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` has an alternative with an empty label at position " +
          position + ": write <label>" + LABEL_SEPARATOR + "<regex>, or omit the label " +
          "entirely to have the pattern name itself")
    }
    if (label.length > MAX_ALTERNATIVE_LABEL_LENGTH) {
      throw new IllegalArgumentException(
        redactedReference(parameterName, position, label) + " has a label longer than the " +
          "limit: length " + label.length + " exceeds MAX_ALTERNATIVE_LABEL_LENGTH=" +
          MAX_ALTERNATIVE_LABEL_LENGTH + ". A label names one reported anchor, so it is bounded")
    }
    if (pattern.isEmpty) {
      throw new IllegalArgumentException(
        "parameter `" + parameterName + "` has an alternative with an empty pattern at " +
          "position " + position + ": a pattern selects the methods this end of the query " +
          "resolves to and cannot be empty")
    }
    if (pattern.length > MAX_ALTERNATIVE_REGEX_LENGTH) {
      throw new IllegalArgumentException(
        redactedReference(parameterName, position, pattern) + " carries a regex longer than " +
          "the limit: length " + pattern.length + " exceeds MAX_ALTERNATIVE_REGEX_LENGTH=" +
          MAX_ALTERNATIVE_REGEX_LENGTH + ". The value is refused rather than truncated")
    }

    // Refused before compilation, because the shape is the hazard rather than the syntax: a
    // pattern with ambiguous nested quantification compiles perfectly and then backtracks.
    val refusals = unsafeRegexConstructs(pattern)
    if (refusals.nonEmpty) {
      throw new IllegalArgumentException(
        redactedReference(parameterName, position, pattern) + " carries a regex construct this " +
          "script refuses, because it is matched against every method full name in the graph: " +
          refusals.map(r => r.rule + " (" + r.detail + ")").mkString("; ") +
          ". Rewrite the pattern without it")
    }

    // Compiled here so that a malformed regex is rejected before the graph is loaded, which is
    // both faster to report and unambiguous about what failed.
    //
    // The exception's own message is deliberately NOT used: `PatternSyntaxException.getMessage`
    // embeds the offending pattern, so reporting it would publish the caller's text through the
    // failure path that the rest of this script withholds it from. The description and the index
    // say what is wrong and where, and carry none of the pattern.
    scala.util.Try(java.util.regex.Pattern.compile(pattern)) match {
      case scala.util.Failure(syntaxFailure) =>
        val explanation = syntaxFailure match {
          case syntax: java.util.regex.PatternSyntaxException =>
            Option(syntax.getDescription).getOrElse("malformed pattern") +
              " at index " + syntax.getIndex
          case other => other.getClass.getName
        }
        throw new IllegalArgumentException(
          redactedReference(parameterName, position, pattern) + " is not a valid regular " +
            "expression: " + explanation + ". The pattern itself is withheld and referenced by " +
            "digest above")
      case scala.util.Success(_) => ()
    }

    PatternAlternative(parameterName, position, label, pattern, redacted)
  }

  val duplicateLabels = alternatives.groupBy(_.label).filter(_._2.size > 1).values.toList
  if (duplicateLabels.nonEmpty) {
    throw new IllegalArgumentException(
      "parameter `" + parameterName + "` repeats a label at position(s) " +
        duplicateLabels.flatMap(_.map(_.position)).sorted.mkString(", ") +
        ": each label names one reported anchor, so labels must be distinct. The labels " +
        "themselves are withheld and referenced by digest: " +
        duplicateLabels
          .map(group => redactedReference(parameterName, group.head.position, group.head.label))
          .mkString("; "))
  }

  alternatives
}

/**
 * A bound on call edges: an integer within the limits in THE INPUT LIMITS. Bounded above as well
 * as below, because the work a callee traversal does grows with the bound and this graph holds
 * hundreds of thousands of methods. Out of range is refused, never clamped.
 *
 * A rejected value is referenced by length and digest and never echoed, including when it parsed
 * as an integer. The redaction policy makes no exception for a value that looks harmless: a caller
 * knows what they passed, so echoing it buys nothing, and one message that echoes is enough to
 * make the policy untrue.
 */
def parseBoundedDepth(parameterName: String, raw: String): Int = {
  def reference: String =
    "parameter `" + parameterName + "` [length=" + raw.length + " digest=" + digestOf(raw) + "]"
  raw.trim.toIntOption match {
    case None =>
      throw new IllegalArgumentException(reference + " is not an integer")
    case Some(value) if value < MIN_MAX_DEPTH =>
      throw new IllegalArgumentException(
        reference + " is below the limit MIN_MAX_DEPTH=" + MIN_MAX_DEPTH + ". A bound below one " +
          "would let no traversal take a step")
    case Some(value) if value > MAX_MAX_DEPTH =>
      throw new IllegalArgumentException(
        reference + " is above the limit MAX_MAX_DEPTH=" + MAX_MAX_DEPTH + ". The bound is " +
          "refused rather than clamped, because a clamped run would answer a shallower question " +
          "than the caller asked while reporting the depth they asked for")
    case Some(value) => value
  }
}

/**
 * The value in force for a parameter, and whether the caller supplied it. Every parameter is
 * declared with `PARAMETER_NOT_SUPPLIED`, whose NUL no process argument can carry, so the
 * sentinel arriving means the caller passed nothing and the declared default authored in this file
 * applies. See PARAMETER PROVENANCE: this is a fact about the invocation, not a comparison against
 * the default, so a caller who passes the declared default verbatim is correctly recorded as
 * having supplied it, and one who passes an empty value is recorded as having supplied that.
 */
def resolveParameter(arrived: String, declaredDefault: String): (String, Boolean) =
  if (arrived == PARAMETER_NOT_SUPPLIED) (declaredDefault, false) else (arrived, true)

/** How a value in force is reported: literal where authored here, digest where the caller's. */
def parameterValueFields(
    valueInForce: String,
    suppliedByCaller: Boolean): Seq[(String, String)] =
  if (suppliedByCaller)
    Seq(
      "value_supplied_by_the_caller" -> jsonBool(true),
      "value_withheld"               -> jsonBool(true),
      "value_length"                 -> jsonInt(valueInForce.length),
      "value_digest"                 -> jsonString(digestOf(valueInForce)))
  else
    Seq(
      "value_supplied_by_the_caller" -> jsonBool(false),
      "value_withheld"               -> jsonBool(false),
      "value_used"                   -> jsonString(valueInForce))


// ===========================================================================================
// The query. Three parameters, each with a default that reproduces the class the probe was
// asked about: see THE PARAMETER LIST and INVOCATION in the header.
// ===========================================================================================

@main def exec(
    handlerPattern: String = PARAMETER_NOT_SUPPLIED,
    sinkPattern: String = PARAMETER_NOT_SUPPLIED,
    maxDepth: String = PARAMETER_NOT_SUPPLIED): Unit = {

  // (1) The start marker is the very first action — before the parameters are validated, before
  //     the workspace switch and before any load. It is what tells a script that never compiled
  //     apart from one that compiled and then rejected a parameter value.
  println(MARKER_START)

  // Rendered JSON fragments, accumulated in a fixed order so the output is deterministic.
  val diagnostics     = scala.collection.mutable.ListBuffer.empty[(String, String)]
  val renderedReturns = scala.collection.mutable.ListBuffer.empty[String]

  // Names the stage the run reached, so a failure below is reported against the step that
  // failed rather than as an unattributed error.
  var stage = "start"

  try {
    // -------------------------------------------------------------------------------------
    // (2) Parameter provenance, then validation. Both run before the workspace is switched and
    //     before anything is loaded, so a rejected value costs no graph work. Every limit in THE
    //     INPUT LIMITS is enforced by refusal, and every failure message names the parameter and
    //     the limit while referencing the caller's text only by digest.
    // -------------------------------------------------------------------------------------
    stage = "resolve_parameters"
    val (handlerPatternInForce, handlerPatternSupplied) =
      resolveParameter(handlerPattern, HANDLER_PATTERN_DEFAULT)
    val (sinkPatternInForce, sinkPatternSupplied) =
      resolveParameter(sinkPattern, SINK_PATTERN_DEFAULT)
    val (maxDepthInForce, maxDepthSupplied) =
      resolveParameter(maxDepth, MAX_DEPTH_DEFAULT)

    stage = "validate_parameters"
    val handlerAlternatives =
      parsePatternList("handlerPattern", handlerPatternInForce, handlerPatternSupplied)
    val sinkAlternatives =
      parsePatternList("sinkPattern", sinkPatternInForce, sinkPatternSupplied)
    val maxCallDepth = parseBoundedDepth("maxDepth", maxDepthInForce)

    // -------------------------------------------------------------------------------------
    // (3) The parameter binding actually in force, recorded first, because every count below is
    //     a count under these values and a result that did not carry them could not be proved.
    // -------------------------------------------------------------------------------------
    stage = "record_parameters"

    def renderAlternatives(alternatives: List[PatternAlternative], indent: String): String =
      jsonBlockArray(
        alternatives.map(alternative =>
          jsonObject(
            Seq("position" -> jsonInt(alternative.position)) ++ alternative.reportedFields)),
        indent)

    diagnostics.append(
      "parameters" -> jsonBlockObject(
        Seq(
          "declared" -> jsonBlockArray(
            Seq(
              jsonBlockObject(
                Seq("name" -> jsonString("handlerPattern"), "type" -> jsonString("string")) ++
                  parameterValueFields(handlerPatternInForce, handlerPatternSupplied) ++
                  Seq(
                    "declared_default" -> jsonString(HANDLER_PATTERN_DEFAULT),
                    "provenance" -> jsonString(
                      if (handlerPatternSupplied) "supplied_by_the_caller"
                      else "declared_default_not_supplied"),
                    "generalizes_over" -> jsonString(HANDLER_PATTERN_GENERALIZES_OVER),
                    "parsed_alternatives" ->
                      renderAlternatives(handlerAlternatives, "          ")),
                "        "),
              jsonBlockObject(
                Seq("name" -> jsonString("sinkPattern"), "type" -> jsonString("string")) ++
                  parameterValueFields(sinkPatternInForce, sinkPatternSupplied) ++
                  Seq(
                    "declared_default" -> jsonString(SINK_PATTERN_DEFAULT),
                    "provenance" -> jsonString(
                      if (sinkPatternSupplied) "supplied_by_the_caller"
                      else "declared_default_not_supplied"),
                    "generalizes_over" -> jsonString(SINK_PATTERN_GENERALIZES_OVER),
                    "parsed_alternatives" -> renderAlternatives(sinkAlternatives, "          ")),
                "        "),
              jsonBlockObject(
                Seq(
                  "name" -> jsonString("maxDepth"),
                  "type" -> jsonString(
                    "string at the invocation boundary, parsed by this script as an integer " +
                      "within MIN_MAX_DEPTH..MAX_MAX_DEPTH")) ++
                  parameterValueFields(maxDepthInForce, maxDepthSupplied) ++
                  Seq(
                    "value_parsed"     -> jsonInt(maxCallDepth),
                    "declared_default" -> jsonString(MAX_DEPTH_DEFAULT),
                    "provenance" -> jsonString(
                      if (maxDepthSupplied) "supplied_by_the_caller"
                      else "declared_default_not_supplied"),
                    "generalizes_over" -> jsonString(MAX_DEPTH_GENERALIZES_OVER)),
                "        ")),
            "      "),
          "provenance_rule" -> jsonString(
            "every parameter is DECLARED with a NUL-bearing sentinel no process argument can " +
              "carry, and the declared default authored in this file is substituted when that " +
              "sentinel arrives. `supplied_by_the_caller` and `declared_default_not_supplied` " +
              "are therefore facts about the invocation rather than a comparison against the " +
              "default: a caller who passes the declared default verbatim is recorded as having " +
              "supplied it, and a caller who passes an explicitly empty value is recorded as " +
              "having supplied that — it is then refused as an empty pattern or an empty depth " +
              "rather than silently replaced by a default"),
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

    diagnostics.append(
      "redaction" -> jsonBlockObject(
        Seq(
          "policy" -> jsonString(
            "a value the caller supplied is never echoed. What is emitted for one is its " +
              "parameter name, its length in characters and a " + REDACTION_DIGEST_ALGORITHM +
              " digest of its UTF-8 bytes — enough to prove that a claimed invocation is the one " +
              "that ran, and not enough to publish anything the caller did not intend to. A " +
              "literal is emitted only where the value in force is the declared default authored " +
              "in this file, which carries nothing of the caller's"),
          "digest_algorithm" -> jsonString(REDACTION_DIGEST_ALGORITHM),
          "governed_fields" -> jsonStringArray(
            List(
              "parameters.declared[].value_used — replaced by value_length and value_digest",
              "parameters.declared[].parsed_alternatives[].label — replaced by a positional " +
                "stand-in of the form <parameter>#<position>, with label_length and label_digest",
              "parameters.declared[].parsed_alternatives[].pattern — replaced by pattern_length " +
                "and pattern_digest",
              "handler_anchors[].label and sink_anchors[].label — the same positional stand-in",
              "handler_anchors[].selector and sink_anchors[].selector — replaced by " +
                "selector_length and selector_digest",
              "handler_qualification.excluded[] — method full names come from the graph and are " +
                "emitted in full; no caller text appears here",
              "traversal.no_returns_explanation — names alternatives by their reported label only",
              "every validation failure message on stderr — names the parameter, the " +
                "alternative's position and the limit, and references the text by length and " +
                "digest, including a value that parsed as an integer and a regex the JDK " +
                "refused: `PatternSyntaxException.getMessage` embeds the pattern, so its " +
                "description and index are reported instead of its message")),
          "fields_always_literal" -> jsonStringArray(
            List(
              "parameters.declared[].declared_default — authored in this file",
              "every method full name, type name and predicate name — read from the graph",
              "every limit in input_limits — authored in this file")),
          "stated_exception" -> jsonString(
            "the numeric bound in force is emitted as an integer — `parameters.declared[]." +
              "value_parsed` and `traversal.max_call_depth` — even when the caller supplied it, " +
              "because every count in this result is a count under that bound and a result that " +
              "hid it could not be interpreted. The caller's raw TEXT for it is still withheld " +
              "and referenced by length and digest, and no pattern value is emitted under any " +
              "circumstances. This is stated rather than left to be noticed, so that a report " +
              "written against this policy claims exactly what the code enforces"),
          "outside_this_scripts_control" -> jsonString(
            "the Joern script runner prints `executing <script> with params=Map(...)` to stderr " +
              "before this script's first statement runs, so a captured stderr log carries the " +
              "invocation as the RUNNER echoed it. That line is the interpreter's and cannot be " +
              "suppressed from inside a script. Everything this script emits — the result " +
              "object, every diagnostics field and every failure message — withholds a " +
              "caller-supplied value and references it by length and digest, so a report that " +
              "must not disclose one has to withhold or filter that captured line rather than " +
              "rely on this script for it"),
          "values_withheld_in_this_run" -> jsonStringArray(
            List(
              (if (handlerPatternSupplied) Some("handlerPattern") else None),
              (if (sinkPatternSupplied) Some("sinkPattern") else None),
              (if (maxDepthSupplied) Some("maxDepth") else None)).flatten)),
        "    "))

    diagnostics.append(
      "input_limits" -> jsonBlockObject(
        Seq(
          "enforcement" -> jsonString(
            "by refusal, never by a silent clamp: an out-of-bounds value ends the run through " +
              "the failure protocol with a message naming the parameter and the limit, because a " +
              "clamped run answers a question the caller did not ask while looking like one that " +
              "answered theirs"),
          "max_parameter_value_length"           -> jsonInt(MAX_PARAMETER_VALUE_LENGTH),
          "max_alternative_regex_length"         -> jsonInt(MAX_ALTERNATIVE_REGEX_LENGTH),
          "max_alternative_label_length"         -> jsonInt(MAX_ALTERNATIVE_LABEL_LENGTH),
          "max_alternatives_per_parameter"       -> jsonInt(MAX_ALTERNATIVES_PER_PARAMETER),
          "max_quantifiers_per_regex"            -> jsonInt(MAX_QUANTIFIERS_PER_REGEX),
          "min_max_depth"                        -> jsonInt(MIN_MAX_DEPTH),
          "max_max_depth"                        -> jsonInt(MAX_MAX_DEPTH),
          "max_resolved_methods_per_alternative" -> jsonInt(MAX_RESOLVED_METHODS_PER_ALTERNATIVE),
          "max_resolved_methods_per_end"         -> jsonInt(MAX_RESOLVED_METHODS_PER_END),
          "max_routes_per_pair"                  -> jsonInt(MAX_ROUTES_PER_PAIR),
          "max_route_enumeration_steps_per_pair" ->
            jsonInt(MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR),
          "max_returns_total"                    -> jsonInt(MAX_RETURNS_TOTAL),
          "refused_regex_constructs" -> jsonString(
            "a REPEATING quantifier (`*`, `+`, `{`) applied to a group whose body already " +
              "quantifies — the ambiguous nested quantification that backtracks catastrophically " +
              "(CWE-1333). A `?` in the body counts, so `(a?)+` is refused as `(a+)+` is; a `?` " +
              "as the OUTER quantifier does not, since a group repeating at most once cannot " +
              "backtrack against itself. The scan tracks escaping, character classes, group " +
              "nesting and group prefixes, so a quantifier inside a character class or behind a " +
              "backslash is read as a literal and a `?` directly after `(` is read as the group " +
              "prefix it is"),
          "cardinality_check_point" -> jsonString(
            "after the anchors resolve and before any traversal starts, because the traversal is " +
              "the expensive part and an over-broad pattern has to be refused rather than " +
              "attempted (CWE-400)")),
        "    "))

    // -------------------------------------------------------------------------------------
    // (4) Workspace first. It closes the current workspace and opens another, so switching
    //     after a load would discard the loaded project.
    // -------------------------------------------------------------------------------------
    stage = "switch_workspace"
    switchWorkspace(WORKSPACE_PATH)

    // -------------------------------------------------------------------------------------
    // (5) Idempotent, provenance-checked load. The workspace is persistent scratch shared with
    //     the other Phase 3 queries and with the gate's coverage check, so a project of this name
    //     is likely to be present already — and a project name is only the last segment of the
    //     input path it was created from, so it is not evidence of what the project holds. An
    //     existing project is therefore opened only when the input path Joern recorded for it
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

    final case class Anchor(
        alternative: PatternAlternative,
        methods: List[Method]) {

      /** The name this anchor is reported under: see the redaction policy. */
      def label: String = alternative.reportedLabel

      /** The selector's evidence: literal where authored in this file, digest where the caller's. */
      def selectorFields: Seq[(String, String)] =
        if (alternative.redacted)
          Seq(
            "selector_withheld" -> jsonBool(true),
            "selector_length"   -> jsonInt(alternative.pattern.length),
            "selector_digest"   -> jsonString(digestOf(alternative.pattern)))
        else
          Seq(
            "selector"          -> jsonString(alternative.pattern),
            "selector_withheld" -> jsonBool(false))
    }

    def resolved(methods: List[Method]): List[Method] =
      methods.distinctBy(_.fullName).sortBy(_.fullName)

    def resolveAnchor(alternative: PatternAlternative): Anchor =
      Anchor(alternative, resolved(cpg.method.fullName(alternative.pattern).l))

    val rawHandlerAnchors = handlerAlternatives.map(resolveAnchor)
    val sinkAnchors       = sinkAlternatives.map(resolveAnchor)

    // -------------------------------------------------------------------------------------
    // (7a) The resolved-cardinality limits. Checked here — after the anchors resolve, before any
    //      traversal starts — because the traversal is the expensive part and a pattern that
    //      resolved to tens of thousands of methods is the case that has to be refused rather
    //      than attempted on a graph of this size (CWE-400). Refused, never clamped: silently
    //      dropping resolved methods would answer a narrower question than the caller asked.
    //      The check runs on the RAW resolved sets, before the structural rule below narrows the
    //      entry-point set, so a caller cannot slip an over-broad pattern past it.
    // -------------------------------------------------------------------------------------
    stage = "check_resolved_cardinality"

    def enforceCardinality(parameterName: String, anchors: List[Anchor]): Unit = {
      anchors.foreach { anchor =>
        if (anchor.methods.size > MAX_RESOLVED_METHODS_PER_ALTERNATIVE) {
          throw new RuntimeException(
            "parameter `" + parameterName + "` alternative " + anchor.alternative.position +
              " (reported as `" + anchor.label + "`) resolved to " + anchor.methods.size +
              " methods and the limit is MAX_RESOLVED_METHODS_PER_ALTERNATIVE=" +
              MAX_RESOLVED_METHODS_PER_ALTERNATIVE + ". Each resolved method starts or " +
              "terminates a traversal over a graph of " + methodCount + " methods, so the count " +
              "is bounded; narrow the pattern rather than expecting this run to attempt it")
        }
      }
      val total = resolved(anchors.flatMap(_.methods)).size
      if (total > MAX_RESOLVED_METHODS_PER_END) {
        throw new RuntimeException(
          "parameter `" + parameterName + "` resolved to " + total + " distinct methods across " +
            "its " + anchors.size + " alternatives and the limit is " +
            "MAX_RESOLVED_METHODS_PER_END=" + MAX_RESOLVED_METHODS_PER_END +
            ". Narrow the pattern rather than expecting this run to attempt it")
      }
    }

    enforceCardinality("handlerPattern", rawHandlerAnchors)
    enforceCardinality("sinkPattern", sinkAnchors)

    // -------------------------------------------------------------------------------------
    // (7b) The one structural rule on resolved entry points. See WHICH ENTRY POINTS THE DEFAULT
    //      SELECTS: a resolved entry point whose SIGNATURE says it is a Scala partial function is
    //      additionally required to allocate the synthetic partial-function class carrying its own
    //      case bodies, which is what tells a declaration from a trait's inherited default — a
    //      distinction no regex over a full name can draw. An entry point of any other signature
    //      is admitted untouched, so the rule generalizes across whatever pair a caller names.
    // -------------------------------------------------------------------------------------
    stage = "qualify_handlers"

    /** A resolved entry point the structural rule removed, with the evidence it was read on. */
    final case class HandlerExclusion(fullName: String, rule: String, evidence: String)

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

    def qualifies(candidate: Method): Boolean =
      if (!HANDLER_SIGNATURES.contains(candidate.signature)) true
      else {
        val owner     = candidate.typeDecl.fullName.headOption.getOrElse("")
        val ownBodies = ownPartialFunctionBodyAllocations(candidate)
        if (ownBodies.nonEmpty) true
        else {
          val otherCalls = candidate.call.methodFullName.l
            .filterNot(_.startsWith(OPERATOR_PREFIX))
            .distinct
            .sorted
          handlerExclusions.append(
            HandlerExclusion(
              candidate.fullName,
              "declares_no_partial_function_body_class_of_its_own",
              "the signature `" + candidate.signature + "` says this method IS a Scala partial " +
                "function, but no outgoing call names a type beginning `" + owner +
                PARTIAL_FUNCTION_INFIX + candidate.name + "$`, so it is an inherited trait " +
                "default rather than a declaration; its non-operator calls are " +
                (if (otherCalls.isEmpty) "none" else otherCalls.mkString("`", "`, `", "`"))))
          false
        }
      }

    val handlerCandidates = resolved(rawHandlerAnchors.flatMap(_.methods))
    val handlerQualified  = handlerCandidates.filter(qualifies)
    val qualifiedFullNames = handlerQualified.map(_.fullName).toSet

    val handlerAnchors =
      rawHandlerAnchors.map(anchor =>
        anchor.copy(methods = anchor.methods.filter(m => qualifiedFullNames.contains(m.fullName))))

    diagnostics.append(
      "handler_qualification" -> jsonBlockObject(
        Seq(
          "rule" -> jsonString(
            "a resolved entry point whose signature is one an `RpcEndpoint` handler declares — " +
              HANDLER_SIGNATURES.mkString("`", "`, `", "`") + " — must also allocate the " +
              "synthetic partial-function class carrying its own case bodies: an outgoing call " +
              "whose method full name begins with its own enclosing type, `" +
              PARTIAL_FUNCTION_INFIX + "` and its own name. That is the evidence it DECLARES the " +
              "partial function rather than inheriting a trait default, and it is a distinction " +
              "no regex over a method full name can draw"),
          "applies_to" -> jsonString(
            "only a resolved entry point whose signature says it IS a Scala partial function. An " +
              "entry point of any other signature is admitted untouched, which is what keeps " +
              "this rule general across whatever pair a caller names"),
          "why_it_is_not_a_parameter" -> jsonString(
            "an inherited trait default has no declaration of its own, so a return anchored at " +
              "one names an entry point that never received a message. That is a defect in the " +
              "return rather than a choice about which pair to ask about, so it is not something " +
              "a caller narrows or widens"),
          "candidates_considered" -> jsonInt(handlerCandidates.size),
          "candidates"            -> jsonStringArray(handlerCandidates.map(_.fullName)),
          "accepted_count"        -> jsonInt(handlerQualified.size),
          "accepted"              -> jsonStringArray(handlerQualified.map(_.fullName)),
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

    // The census of the pinned class: every method of either handler name under the pinned
    // package the graph holds, and what became of it under the patterns in force. It selects
    // nothing — see the constants' comment — and exists so that what a pattern did NOT nominate
    // is visible with the graph facts a reader would compare it against, rather than having to be
    // inferred from a candidate count. Every value here is read from the graph, so a
    // caller-supplied pattern is never echoed through it.
    val censusExclusionRules =
      handlerExclusions.toList.map(exclusion => exclusion.fullName -> exclusion.rule).toMap
    val nominatedFullNames = handlerCandidates.map(_.fullName).toSet
    val censusMethods = resolved(
      CENSUS_HANDLER_NAMES.flatMap(handlerName =>
        cpg.method
          .nameExact(handlerName)
          .l
          .filter(_.fullName.startsWith(CENSUS_PACKAGE_PREFIX))))

    diagnostics.append(
      "pinned_class_handler_census" -> jsonBlockObject(
        Seq(
          "purpose" -> jsonString(
            "evidence, never a selector: what the patterns in force did and did not nominate " +
              "among the methods the pinned class names, with the graph facts a reader compares " +
              "against a pattern. Nothing in the traversal depends on this listing"),
          "names_listed"     -> jsonStringArray(CENSUS_HANDLER_NAMES),
          "package_prefix"   -> jsonString(CENSUS_PACKAGE_PREFIX),
          "methods_in_graph" -> jsonInt(censusMethods.size),
          "nominated_by_the_patterns_in_force" ->
            jsonInt(censusMethods.count(m => nominatedFullNames.contains(m.fullName))),
          "accepted_as_entry_points" ->
            jsonInt(censusMethods.count(m => qualifiedFullNames.contains(m.fullName))),
          "methods" -> jsonBlockArray(
            censusMethods.map { method =>
              val fullName  = method.fullName
              val nominated = nominatedFullNames.contains(fullName)
              val accepted  = qualifiedFullNames.contains(fullName)
              val outcome =
                if (accepted) "accepted_as_an_entry_point"
                else
                  censusExclusionRules.getOrElse(
                    fullName,
                    "not_matched_by_any_handler_pattern_alternative_in_force")
              jsonObject(
                Seq(
                  "full_name" -> jsonString(fullName),
                  "signature" -> jsonString(method.signature),
                  "enclosing_type" ->
                    jsonString(method.typeDecl.fullName.headOption.getOrElse("")),
                  "nominated_by_a_pattern" -> jsonBool(nominated),
                  "accepted_as_an_entry_point" -> jsonBool(accepted),
                  "outcome" -> jsonString(outcome)))
            },
            "      ")),
        "    "))

    val handlerMethods = handlerQualified
    val sinkMethods    = resolved(sinkAnchors.flatMap(_.methods))
    val sinkFullNames  = sinkMethods.map(_.fullName).toSet

    diagnostics.append(
      "resolved_cardinality" -> jsonBlockObject(
        Seq(
          "checked_before_any_traversal" -> jsonBool(true),
          "handler_pattern_resolved_distinct_before_qualification" ->
            jsonInt(handlerCandidates.size),
          "handler_pattern_resolved_distinct_after_qualification" -> jsonInt(handlerMethods.size),
          "sink_pattern_resolved_distinct"                        -> jsonInt(sinkMethods.size),
          "largest_handler_alternative" ->
            jsonInt(if (rawHandlerAnchors.isEmpty) 0 else rawHandlerAnchors.map(_.methods.size).max),
          "largest_sink_alternative" ->
            jsonInt(if (sinkAnchors.isEmpty) 0 else sinkAnchors.map(_.methods.size).max),
          "limit_per_alternative" -> jsonInt(MAX_RESOLVED_METHODS_PER_ALTERNATIVE),
          "limit_per_end"         -> jsonInt(MAX_RESOLVED_METHODS_PER_END),
          "reading" -> jsonString(
            "the limits were applied to the RAW resolved sets, before the structural rule " +
              "narrowed the entry-point set, so a caller cannot slip an over-broad pattern past " +
              "them by relying on the rule to shrink it afterwards")),
        "    "))

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
    //     this the unguarded class. Two passes, as ONE RETURN PER ROUTE in the header sets out:
    //     a forward pass that observes the edge set, and a bounded backward pass that enumerates
    //     every distinct ordered route over it.
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
        cpg.method.fullNameExact(methodFullName).l.exists(_.name.endsWith(TRAIT_FORWARDER_SUFFIX)))

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

      while (depth < maxCallDepth && frontier.nonEmpty) {
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

      methodsSeenSummed += firstDepthOf.size
      edgesObservedSummed += predecessorsOf.valuesIterator.map(_.size).sum
      if (depth >= maxCallDepth && frontier.nonEmpty) {
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
      //     route over those edges, within the caller's depth bound, in predecessor-name order.
      reachedSinks.toList.foreach { sinkFullName =>
        pairsEnumerated += 1
        val pairLabel = handler.fullName + " ==> " + sinkFullName
        val routes    = scala.collection.mutable.ListBuffer.empty[List[String]]
        var steps      = 0
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
              val edgesRemaining = maxCallDepth - (suffix.size - 1)
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
          // The second pass, computed over the emitted route as found: a path node that is itself
          // a predicate, plus the predicates each path node calls directly. A return is emitted
          // whatever this finds, and nothing is added to it that the graph does not carry.
          val predicatesOnPath = path.flatMap { fullName =>
            val itself = if (predicateSet.contains(fullName)) List(fullName) else Nil
            itself ++ predicatesCalledBy(fullName)
          }

          // A Scala trait's static forwarder is linked to every implementation of the method it
          // forwards, so a route may hop from one entry point to another through one. Such a
          // return is emitted as the call graph carries it and is listed here, not filtered out.
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
    // it — never one per sink, and in a fixed order, so an unchanged source run with unchanged
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
      jsonObject(
        Seq(
          "label"    -> jsonString(anchor.label),
          "position" -> jsonInt(anchor.alternative.position)) ++
          anchor.selectorFields ++
          Seq(
            "resolved_count"      -> jsonInt(anchor.methods.size),
            "resolved"            -> jsonStringArray(anchor.methods.map(_.fullName)),
            "returns_contributed" -> jsonInt(contributed)))

    // The entry-point anchors are reported twice: as the pattern resolved them, and as the
    // structural rule left them, so a caller can see what their pattern reached and what the rule
    // removed from it without having to subtract one count from another.
    diagnostics.append(
      "handler_anchors" -> jsonBlockArray(
        handlerAnchors.map(anchor => renderAnchor(anchor, returnsFrom(anchor, _.handler))),
        "    "))
    diagnostics.append(
      "handler_anchors_before_qualification" -> jsonBlockArray(
        rawHandlerAnchors.map(anchor =>
          jsonObject(
            Seq(
              "label"    -> jsonString(anchor.label),
              "position" -> jsonInt(anchor.alternative.position)) ++
              anchor.selectorFields ++
              Seq(
                "resolved_count" -> jsonInt(anchor.methods.size),
                "resolved"       -> jsonStringArray(anchor.methods.map(_.fullName))))),
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
          if (pairsAtStepCap.nonEmpty)
            Some(
              pairsAtStepCap.size + " (entry point, sink) pair(s) stopped at the backward-search " +
                "step cap of " + MAX_ROUTE_ENUMERATION_STEPS_PER_PAIR + ", so whether a route " +
                "exists for them is unknown rather than answered")
          else None,
          if (handlerMethods.nonEmpty && sinkMethods.nonEmpty)
            Some(
              "no path from any of the " + handlerMethods.size + " resolved entry points " +
                "reached any of the " + sinkMethods.size + " resolved sink methods")
          else None).flatten

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
              "exactly how many"),
          "pair_labels_are_graph_derived" -> jsonString(
            "the pair labels listed above are method full names read from the graph, never " +
              "caller-supplied pattern text, so a bound can be audited without echoing an input")),
        "    "))

    diagnostics.append(
      "traversal" -> jsonBlockObject(
        Seq(
          "direction" -> jsonString(
            "forward over callee edges, one traversal per resolved entry point, from the node " +
              "set `handlerPattern` resolved to the node set `sinkPattern` resolved, then a " +
              "bounded backward enumeration of routes over the edges that traversal observed"),
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
          "entry_points_traversed"                  -> jsonInt(handlerMethods.size),
          "sink_methods_resolved"                   -> jsonInt(sinkMethods.size),
          "methods_seen_summed_over_entry_points"   -> jsonInt(methodsSeenSummed),
          "edges_observed_summed_over_entry_points" -> jsonInt(edgesObservedSummed),
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
            "one return per distinct ordered route from an entry point to a sink, up to the " +
              "bounds in `route_enumeration`; predecessors are followed in full-name order and " +
              "returns are sorted, so the output is reproducible"),
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
          "longest_emitted_route_edge_count" ->
            (if (longestRouteEdges < 0) "null" else jsonInt(longestRouteEdges)),
          "shortest_emitted_route_edge_count" ->
            (if (shortestRouteEdges < 0) "null" else jsonInt(shortestRouteEdges)),
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

    // -------------------------------------------------------------------------------------
    // (11) The result region: the BEGIN marker, one JSON object, the END marker, and nothing
    //      else between them. It is inside this block, and last, on purpose — the whole
    //      document is built as one string first, so the markers are printed only once a
    //      complete result exists, and any failure above leaves this unreached.
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
      // printed and nothing that could be read as a result. A rejected parameter arrives here
      // too — validation runs inside this block — and is reported the same way, by name and
      // limit, with no caller-supplied value in the message. A partial diagnostics buffer is
      // deliberately discarded rather than published as a success-shaped payload.
      reportFailureAndRaise(stage, failure)
  }
}

