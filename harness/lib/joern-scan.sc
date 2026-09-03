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
