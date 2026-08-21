// harness/lib/joern-baked-queries.sc
// The baked query set for harness/bin/run-joern.sh. NOT a Phase-3 probe script: this is the
// Joern tool's own scanner pass over the persisted code-property graph.
//
// It LOADS the persisted graph with importCpg and never calls importCode: no graph is built here.
// Output: one JSON object per line on stdout between the markers below, so the runner can collect
// them without depending on Joern's own pretty-printer.
//
//   ---HARNESS-JOERN-BEGIN---
//   {"rule_id":..,"message":..,"class_file":..,"start_line":..,"method_full_name":..}
//   ---HARNESS-JOERN-END---
//
// Paths in `class_file` are the graph's own FILE names, i.e. bytecode class paths from the CPG
// build, NOT $SPARK_SRC-relative source paths. run-joern.sh maps them to source paths.

def esc(s: String): String =
  s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ").replace("\r", " ").replace("\t", " ")

@main def exec(cpgPath: String) = {
  importCpg(cpgPath)
  println("HARNESS_JOERN_CPG=" + cpgPath)
  println("HARNESS_JOERN_METHODS=" + cpg.method.size)
  println("HARNESS_JOERN_TYPEDECLS=" + cpg.typeDecl.size)

  val rows = scala.collection.mutable.ListBuffer[String]()

  def emit(ruleId: String, message: String, file: String, line: Option[Int], method: String): Unit =
    rows += s"""{"rule_id":"${esc(ruleId)}","message":"${esc(message)}","class_file":"${esc(file)}","start_line":${line.map(_.toString).getOrElse("null")},"method_full_name":"${esc(method)}"}"""

  // 1. Process launch sites: java.lang.ProcessBuilder.start / Runtime.exec
  val q1 = cpg.call.methodFullName("(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec).*").l
  q1.foreach { c =>
    emit("joern.process-launch-site",
         "Call to " + c.methodFullName + " (external process launch)",
         c.method.filename, c.lineNumber, c.method.fullName)
  }

  // 2. Java deserialization entry points: ObjectInputStream.readObject
  val q2 = cpg.call.methodFullName("java\\.io\\.ObjectInputStream\\.readObject.*").l
  q2.foreach { c =>
    emit("joern.java-deserialization-site",
         "Call to " + c.methodFullName + " (Java object deserialization)",
         c.method.filename, c.lineNumber, c.method.fullName)
  }

  // 3. Reflective class loading: Class.forName / ClassLoader.loadClass
  val q3 = cpg.call.methodFullName("(java\\.lang\\.Class\\.forName|java\\.lang\\.ClassLoader\\.loadClass).*").l
  q3.foreach { c =>
    emit("joern.reflective-class-load",
         "Call to " + c.methodFullName + " (reflective class loading)",
         c.method.filename, c.lineNumber, c.method.fullName)
  }

  // 4. Weak hash construction: MessageDigest.getInstance with an MD5/SHA-1 literal argument
  val q4 = cpg.call.methodFullName("java\\.security\\.MessageDigest\\.getInstance.*")
             .where(_.argument.isLiteral.code("(?i).*\"?(md5|md2|sha-?1)\"?.*")).l
  q4.foreach { c =>
    emit("joern.weak-hash-algorithm",
         "MessageDigest.getInstance with a weak algorithm literal: " + c.argument.isLiteral.code.l.mkString(","),
         c.method.filename, c.lineNumber, c.method.fullName)
  }

  // 5. deploy-package RPC handler that reaches a process launch over the call graph.
  //    Bounded to two call steps so the pass stays linear on a 445k-method graph.
  val launchers = cpg.method.fullName("(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec).*").l
  val handlers  = cpg.method.name("receive|receiveAndReply").where(_.fullName("org\\.apache\\.spark\\.deploy\\..*")).l
  handlers.foreach { h =>
    val reached = h.callee.l.flatMap(c1 => c1 :: c1.callee.l).filter(m => launchers.exists(_.fullName == m.fullName))
    if (reached.nonEmpty) {
      emit("joern.rpc-handler-reaches-process-launch",
           "deploy-package RPC handler " + h.fullName + " reaches " + reached.map(_.fullName).distinct.mkString(","),
           h.filename, h.lineNumber, h.fullName)
    }
  }

  println("HARNESS_JOERN_QUERY_COUNTS=process-launch-site=" + q1.size +
          ",java-deserialization-site=" + q2.size +
          ",reflective-class-load=" + q3.size +
          ",weak-hash-algorithm=" + q4.size +
          ",rpc-handler-reaches-process-launch=" + rows.count(_.contains("rpc-handler-reaches-process-launch")))
  println("---HARNESS-JOERN-BEGIN---")
  rows.foreach(println)
  println("---HARNESS-JOERN-END---")
}
