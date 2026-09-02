// harness/lib/joern-scan.sc — the BOUNDED query set baked into run-joern.sh.
//
// Why bounded: a whole-graph sweep of Joern's default 59-query bundle over a
// ~1.4 M-method graph ran 2 h 26 m wall / 9 h 35 m CPU on 4 vCPU WITHOUT finishing.
// The scanning run imposes no time limit, so an unbounded sweep does not fail there —
// it hangs, and the run never produces a dataset. This set is six structural queries,
// each keyed on an indexed call name and each capped by an explicit traversal bound,
// so the runner completes over the full graph. The bound applies to how much work is
// asked of the query engine, never to which files or modules are in scope.
//
// Inputs (environment variables set by run-joern.sh; JVM system properties of the
// same name, with dots instead of underscores, are honoured as a fallback):
//   HARNESS_SCAN_CPG    absolute path of the CPG to load with importCpg
//   HARNESS_SCAN_OUT    absolute path of the JSON artifact to write
//   HARNESS_SCAN_BOUND  per-query traversal bound (integer)
//   HARNESS_SCAN_HEAP_FLOOR_BYTES  the heap floor this JVM must satisfy, in bytes
//                                  (default 64 GiB); below it the run aborts before
//                                  importCpg
//   HARNESS_SCAN_HEAP_RECORD       absolute path of the JSON record carrying the heap
//                                  THIS JVM measured, written before the floor test
//
// Why this script measures its own heap: `joern --script` runs the script in a CHILD
// JVM (replpp.scripting.NonForkingScriptRunner) and does NOT forward the launcher's
// -J-Xmx to it, so the parent ReplBridge JVM's heap says nothing about the heap the
// queries actually run at. The only place that value can be OBSERVED rather than
// requested is inside this script, which is why the check is here and why it precedes
// importCpg: AAP 0.5.4/0.9.1 require the Stage 3 Joern runner at a heap of at least
// 64 GiB, and 0.8.2 makes the rule one-way — raising is permitted and reported,
// lowering is not, because a truncated result's silence cannot be told from a clean
// one. The measured value goes to its own record file, never into the artifact, whose
// shape is fixed by oss-scan-results/tool-status.md and the adapter tests.

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

def cfg(envName: String, propName: String): Option[String] =
  sys.env.get(envName).filter(_.nonEmpty).orElse(sys.props.get(propName).filter(_.nonEmpty))

val cpgPath = cfg("HARNESS_SCAN_CPG", "harness.cpg")
  .getOrElse(throw new RuntimeException("HARNESS_SCAN_CPG is not set"))
val outPath = cfg("HARNESS_SCAN_OUT", "harness.out")
  .getOrElse(throw new RuntimeException("HARNESS_SCAN_OUT is not set"))
val bound   = cfg("HARNESS_SCAN_BOUND", "harness.bound").getOrElse("2000").toInt

def jesc(s: String): String =
  s.flatMap {
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case c if c.isControl => "?"
    case c    => c.toString
  }

// ------------------------------------------------------ the child JVM heap floor
// Measured here, in the JVM that will actually hold the graph, and checked BEFORE
// importCpg so a sub-floor JVM never reaches the engine and never writes an
// artifact. The record is written before the test, so a refused run still leaves the
// evidence run-joern.sh reads back: a run that only printed its verdict and then died
// would leave the runner unable to tell "below the floor" from "never measured".

/** 64 GiB in bytes — AAP 0.5.4/0.9.1's floor for the Stage 3 Joern JVM, and the
 *  default when HARNESS_SCAN_HEAP_FLOOR_BYTES is not supplied. A default that had to
 *  be passed in would make the floor optional. */
val DEFAULT_HEAP_FLOOR_BYTES = 64L * 1024L * 1024L * 1024L

/** The only JVM arguments whose VALUES may be published. These console streams and
 *  the record below are preserved verbatim as evidence, so an argument value nobody
 *  whitelisted must never reach one: a memory or stack flag is the evidence this
 *  check reports, and every other argument is left out entirely rather than being
 *  logged with a value that could carry a token or a connection string. Same policy
 *  and same prefix list as the Stage 5 probe queries. */
val HEAP_ARG_PREFIXES = List(
  "-Xms", "-Xmx", "-Xmn", "-Xss",
  "-XX:MaxMetaspaceSize", "-XX:MetaspaceSize", "-XX:MaxDirectMemorySize",
  "-XX:MaxRAMPercentage", "-XX:ThreadStackSize")

val heapFloorBytes: Long =
  cfg("HARNESS_SCAN_HEAP_FLOOR_BYTES", "harness.heap.floor.bytes") match {
    case None => DEFAULT_HEAP_FLOOR_BYTES
    case Some(raw) =>
      try raw.trim.toLong
      catch {
        case _: NumberFormatException =>
          throw new RuntimeException(
            "CONFIGURATION FAULT: HARNESS_SCAN_HEAP_FLOOR_BYTES must be a whole number " +
              s"of bytes, got '${jesc(raw)}'. A floor that cannot be read is not a floor, " +
              "so nothing is loaded.")
      }
  }
val heapRecordPath = cfg("HARNESS_SCAN_HEAP_RECORD", "harness.heap.record")
val heapMaxBytes   = Runtime.getRuntime.maxMemory()
val heapFloorOk    = heapMaxBytes >= heapFloorBytes
val jdkMajor       = System.getProperty("java.specification.version")
val jvmVersion     = System.getProperty("java.vm.version")
val jvmMemoryArgs  = {
  import scala.jdk.CollectionConverters._
  java.lang.management.ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toList
    .filter(arg => HEAP_ARG_PREFIXES.exists(p => arg == p || arg.startsWith(p)))
}
def gib(bytes: Long): String = f"${bytes.toDouble / (1024L * 1024L * 1024L)}%.5f"

val heapRecordJson = s"""{
 "measured_by": "harness/lib/joern-scan.sc, inside the JVM that runs importCpg and the queries",
 "measured_at_utc": "${jesc(java.time.Instant.now().toString)}",
 "jvm_role": "the child JVM joern --script forks (replpp.scripting.NonForkingScriptRunner)",
 "heap_max_bytes": $heapMaxBytes,
 "heap_max_gib": "${gib(heapMaxBytes)}",
 "heap_floor_bytes": $heapFloorBytes,
 "heap_floor_gib": "${gib(heapFloorBytes)}",
 "at_or_above_floor": $heapFloorOk,
 "jdk_specification_version": "${jesc(jdkMajor)}",
 "java_vm_version": "${jesc(jvmVersion)}",
 "jvm_memory_stack_args": [${jvmMemoryArgs.map(a => "\"" + jesc(a) + "\"").mkString(", ")}],
 "jvm_arg_policy": "memory and stack flags only; every other JVM argument is omitted rather than logged, because this record is preserved verbatim",
 "mechanism": "the floor reaches this JVM through JAVA_TOOL_OPTIONS=-Xmx<heap>, appended last by harness/bin/run-joern.sh; joern --script does not forward the launcher's -J-Xmx to this child",
 "measurement": "Runtime.getRuntime.maxMemory() in this JVM -- observed, not requested"
}
"""

heapRecordPath.foreach { path =>
  try {
    val target = Paths.get(path)
    val parent = target.getParent
    if (parent != null) Files.createDirectories(parent)
    Files.write(target, heapRecordJson.getBytes(StandardCharsets.UTF_8))
    println(s"heap record       : $path")
  } catch {
    case e: Exception =>
      throw new RuntimeException(
        s"CONFIGURATION FAULT: cannot write the child-JVM heap record to $path: " +
          s"${e.getClass.getName}: ${e.getMessage}. harness/bin/run-joern.sh treats an " +
          "absent record as a fault, so the run stops here rather than loading a graph " +
          "at a heap nobody can establish.")
  }
}

println(s"jdk (in-JVM)      : specification=$jdkMajor vm=$jvmVersion")
println(s"heap measured     : $heapMaxBytes bytes (${gib(heapMaxBytes)} GiB) -- this JVM, observed")
println(s"heap floor        : $heapFloorBytes bytes (${gib(heapFloorBytes)} GiB)")
println(s"heap jvm args     : ${if (jvmMemoryArgs.isEmpty) "<none>" else jvmMemoryArgs.mkString(" ")}")
println(s"heap floor verdict: ${if (heapFloorOk) "PASS (measured, not requested)" else "FAIL (below the floor)"}")

if (!heapFloorOk) {
  val reason =
    s"the heap ACTUALLY available to the JVM that would hold the graph is $heapMaxBytes " +
      s"bytes (${gib(heapMaxBytes)} GiB), below the floor of $heapFloorBytes bytes " +
      s"(${gib(heapFloorBytes)} GiB). `joern --script` forks a child JVM and does NOT " +
      "forward the launcher's -J-Xmx to it, so the child's heap comes from " +
      "JAVA_TOOL_OPTIONS: raise HARNESS_JOERN_HEAP (which harness/bin/run-joern.sh " +
      "floor-checks and appends as -Xmx to JAVA_TOOL_OPTIONS) rather than lowering it. " +
      "AAP 0.8.2's rule has a direction -- raising a heap is permitted and reported, " +
      "lowering one is not, because a truncated result's silence cannot be told apart " +
      "from a clean one."
  println(s"CONFIGURATION FAULT: $reason")
  println("importCpg was NOT reached; no graph was loaded and no artifact was written.")
  throw new RuntimeException(s"CONFIGURATION FAULT: $reason")
}

println(s"loading CPG with importCpg: $cpgPath")
importCpg(cpgPath)

val methodCount   = cpg.method.size
val typeDeclCount = cpg.typeDecl.size
val fileCount     = cpg.file.size
println(s"graph loaded: methods=$methodCount typeDecls=$typeDeclCount files=$fileCount")

case class Q(id: String, severity: String, message: String, callNames: List[String], calleeMatch: List[String])

val queries = List(
  Q("joern-process-exec", "HIGH",
    "external process launch reachable in bytecode",
    List("exec", "start"),
    List("java.lang.Runtime.exec", "java.lang.ProcessBuilder.start")),
  Q("joern-unsafe-deserialization", "HIGH",
    "java deserialization entry point (ObjectInputStream.readObject)",
    List("readObject"),
    List("java.io.ObjectInputStream.readObject")),
  Q("joern-reflection-forname", "MEDIUM",
    "reflective class loading (Class.forName)",
    List("forName"),
    List("java.lang.Class.forName")),
  Q("joern-message-digest", "MEDIUM",
    "message digest construction (algorithm chosen at this call site)",
    List("getInstance"),
    List("java.security.MessageDigest.getInstance")),
  Q("joern-cipher-getinstance", "MEDIUM",
    "cipher construction (transformation chosen at this call site)",
    List("getInstance"),
    List("javax.crypto.Cipher.getInstance")),
  Q("joern-xml-factory", "MEDIUM",
    "XML parser factory construction (external entity posture set at this call site)",
    List("newInstance"),
    List("javax.xml.parsers.DocumentBuilderFactory.newInstance",
         "javax.xml.parsers.SAXParserFactory.newInstance",
         "javax.xml.stream.XMLInputFactory.newInstance",
         "javax.xml.transform.TransformerFactory.newInstance"))
)

val findingBuf = scala.collection.mutable.ArrayBuffer.empty[String]
val queryBuf   = scala.collection.mutable.ArrayBuffer.empty[String]

queries.foreach { q =>
  val t0 = System.nanoTime()
  // Keyed on the indexed call NAME, then filtered on the callee's full name; capped at
  // bound+1 so truncation is observed rather than assumed.
  val hits = cpg.call
    .nameExact(q.callNames: _*)
    .filter(c => q.calleeMatch.exists(m => c.methodFullName.startsWith(m)))
    .take(bound + 1)
    .l
  val truncated = hits.size > bound
  val kept = hits.take(bound)
  kept.foreach { c =>
    val m = c.method
    val cls = m.typeDecl.fullName.headOption.getOrElse("<unknown>")
    val file = m.filename
    val line = c.lineNumber.map(_.toString).getOrElse("null")
    findingBuf += s"""  {"query_id": "${jesc(q.id)}", "severity": "${q.severity}", "message": "${jesc(q.message)}", "callee": "${jesc(c.methodFullName)}", "class": "${jesc(cls)}", "method": "${jesc(m.fullName)}", "file": "${jesc(file)}", "line": $line}"""
  }
  val ms = (System.nanoTime() - t0) / 1000000L
  queryBuf += s"""  {"id": "${jesc(q.id)}", "bound": $bound, "returned": ${kept.size}, "bound_reached": $truncated, "elapsed_ms": $ms, "callee_prefixes": [${q.calleeMatch.map(m => "\"" + jesc(m) + "\"").mkString(", ")}]}"""
  println(f"query ${q.id}%-32s returned ${kept.size}%6d bound_reached=$truncated elapsed_ms=$ms")
}

val json = s"""{
 "tool": "joern",
 "tool_version": "4.0.607",
 "cpg": "${jesc(cpgPath)}",
 "graph": {"methods": $methodCount, "type_declarations": $typeDeclCount, "files": $fileCount},
 "query_set": "harness/lib/joern-scan.sc (6 bounded structural queries)",
 "queries": [
${queryBuf.mkString(",\n")}
 ],
 "findings": [
${findingBuf.mkString(",\n")}
 ]
}
"""

Files.write(Paths.get(outPath), json.getBytes(StandardCharsets.UTF_8))
println(s"wrote ${findingBuf.size} findings to $outPath")
