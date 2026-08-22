# `03-parameterized-unguarded-handler-sink` — query outcome

Written by the Phase 3 driver from its own capture of this query's invocation. The query
did not write this file, and nothing in it is asserted that the driver did not observe.

| | |
|---|---|
| Query source | `queries/joern/03-parameterized-unguarded-handler-sink.sc` |
| Source sha256 | `49ce08f0d7e592827eb6241857fb934dc1290ecbb372109e1f8c711c2fad79b2` |
| Invoked | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/03-parameterized-unguarded-handler-sink.sc` |
| Parameters | none — the script's declared defaults are in force |
| JVM | `openjdk version "21.0.12.1" 2026-08-18 LTS` (`JAVA_HOME=/opt/blitzy-harness/tools/jdk-21.0.12.1+1`) |
| `JAVA_OPTS` | `-Xmx48g -Xss64m` |
| Started / ended (UTC) | `2026-08-22T10:31:43Z` / `2026-08-22T10:32:00Z` |
| Elapsed | 16.4 s |
| Exit code | `0` |
| Start marker seen | True |
| Result region parsed | True |
| `compiled` | True |
| `ran` | True |
| Returns | 10 |
| Spurious under the on-path test | 0 |
| Clean positive | True |
| Graph | `harness/cpg/spark.cpg`, loaded with `importCpg`; built: False |

## 1. The precondition, as the driver observed it

The driver runs only after the controller has published the dataset. This is what it
observed at `2026-08-22T10:28:10Z`, immediately before invoking this query — not an assumption:

| Published output | Present | Rows | Bytes | sha256 |
|---|---|---|---|---|
| `oss-scan-results/findings.json` | True | 10178 | 5806988 | `2b3fb2dbb5c2f30c711524a5a0be141aab8445e00814a7fdf6f8ba6c6f664f51` |
| `oss-scan-results/findings.csv` | True | 10178 | 3309257 | `68ae2e4ed1b0f9197a4e813c4e73f9d9c2a9864143d9f56c8173af9aa5f25e13` |
| `oss-scan-results/severity-map.md` | True | — | 6049 | `ebf11a85342c7e62c3a2ad1f403ea13672dd1bd579746f85969ac47798a8207f` |

## 2. Outcome

**The query compiled, ran, and returned 10 result(s)**, of which 0 are spurious under
the on-path test and 10 are not.

### 2.1 Returns

| # | Handler | Sink | Path length | Predicates on path | Spurious |
|---|---|---|---|---|---|
| 1 | `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 3 | none | no |
| 2 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 8 | none | no |
| 3 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 10 | none | no |
| 4 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 7 | none | no |
| 5 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 9 | none | no |
| 6 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 10 | none | no |
| 7 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 9 | none | no |
| 8 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `java.lang.ProcessBuilder.start:java.lang.Process()` | 6 | none | no |
| 9 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)` | 3 | none | no |
| 10 | `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` | `org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)` | 3 | none | no |

### 2.2 Paths as emitted

Each path is the ordered list of method full names the query emitted, handler first
and sink last.

**Return 1** — `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` ⇒ `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

1. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

**Return 2** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `java.lang.ProcessBuilder.start:java.lang.Process()`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5$adapted:java.lang.Object(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)`
3. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)`
4. `org.apache.spark.util.SparkFileUtils.deleteRecursively:void(java.io.File)`
5. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File)`
6. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File,java.io.FilenameFilter)`
7. `org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingUnixNative:void(java.io.File)`
8. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 3** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `java.lang.ProcessBuilder.start:java.lang.Process()`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5$adapted:java.lang.Object(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)`
3. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)`
4. `org.apache.spark.util.Utils$.deleteRecursively:void(java.io.File)`
5. `org.apache.spark.util.SparkFileUtils.deleteRecursively$:void(org.apache.spark.util.SparkFileUtils,java.io.File)`
6. `org.apache.spark.util.SparkFileUtils.deleteRecursively:void(java.io.File)`
7. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File)`
8. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File,java.io.FilenameFilter)`
9. `org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingUnixNative:void(java.io.File)`
10. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 4** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `java.lang.ProcessBuilder.start:java.lang.Process()`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)`
3. `org.apache.spark.util.SparkFileUtils.deleteRecursively:void(java.io.File)`
4. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File)`
5. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File,java.io.FilenameFilter)`
6. `org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingUnixNative:void(java.io.File)`
7. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 5** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `java.lang.ProcessBuilder.start:java.lang.Process()`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)`
3. `org.apache.spark.util.Utils$.deleteRecursively:void(java.io.File)`
4. `org.apache.spark.util.SparkFileUtils.deleteRecursively$:void(org.apache.spark.util.SparkFileUtils,java.io.File)`
5. `org.apache.spark.util.SparkFileUtils.deleteRecursively:void(java.io.File)`
6. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File)`
7. `org.apache.spark.network.util.JavaUtils.deleteRecursively:void(java.io.File,java.io.FilenameFilter)`
8. `org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingUnixNative:void(java.io.File)`
9. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 6** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `java.lang.ProcessBuilder.start:java.lang.Process()`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.DriverRunner.start:void()`
4. `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
5. `org.apache.spark.deploy.worker.DriverRunner.prepareAndRunDriver:int()`
6. `org.apache.spark.deploy.worker.DriverRunner.downloadUserJar:java.lang.String(java.io.File)`
7. `org.apache.spark.util.Utils$.fetchFile:java.io.File(java.lang.String,java.io.File,org.apache.spark.ReadOnlySparkConf,org.apache.hadoop.conf.Configuration,long,boolean,boolean)`
8. `org.apache.spark.util.Utils$.executeAndGetOutput:java.lang.String(scala.collection.immutable.Seq,java.io.File,scala.collection.Map,boolean)`
9. `org.apache.spark.util.Utils$.executeCommand:java.lang.Process(scala.collection.immutable.Seq,java.io.File,scala.collection.Map,boolean)`
10. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 7** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `java.lang.ProcessBuilder.start:java.lang.Process()`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.DriverRunner.start:void()`
4. `org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()`
5. `org.apache.spark.deploy.worker.DriverRunner.prepareAndRunDriver:int()`
6. `org.apache.spark.deploy.worker.DriverRunner.runDriver:int(java.lang.ProcessBuilder,java.io.File,boolean)`
7. `org.apache.spark.deploy.worker.DriverRunner.runCommandWithRetry:int(org.apache.spark.deploy.worker.ProcessBuilderLike,scala.Function1,boolean)`
8. `org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()`
9. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 8** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `java.lang.ProcessBuilder.start:java.lang.Process()`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.ExecutorRunner.start:void()`
4. `org.apache.spark.deploy.worker.ExecutorRunner$$anon$1.run:void()`
5. `org.apache.spark.deploy.worker.ExecutorRunner.org$apache$spark$deploy$worker$ExecutorRunner$$fetchAndRunExecutor:void()`
6. `java.lang.ProcessBuilder.start:java.lang.Process()`

**Return 9** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)`

**Return 10** — `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()` ⇒ `org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)`

1. `org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()`
2. `org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)`

## 3. Spurious returns under the on-path test

A return is spurious when an authentication or ACL predicate from the set the query derived at execution time lies on the path from the handler to the sink, and for no other reason. The test is applied mechanically to `predicates_on_path`: non-empty means spurious, empty means not spurious. No broader judgement is applied, and the determination is a property of the query rather than of Spark.

| | |
|---|---|
| Returns emitted | 10 |
| Spurious | 0 |
| Not spurious | 10 |

### 3.1 The predicate set, derived at execution time

Derived from the graph rather than hardcoded, so a predicate added or renamed since
any earlier measurement is not missed.

| | |
|---|---|
| Type-declaration selector | `org\.apache\.spark\.SecurityManager` |
| Name selector | `^(check.*Permissions\|acls.*\|isAuthenticationEnabled)$` |
| Match mode | anchored full match |
| Methods considered | 126 |
| Resolved | `org.apache.spark.SecurityManager.aclsEnabled:boolean()`, `org.apache.spark.SecurityManager.checkAdminPermissions:boolean(java.lang.String)`, `org.apache.spark.SecurityManager.checkModifyPermissions:boolean(java.lang.String)`, `org.apache.spark.SecurityManager.checkUIViewPermissions:boolean(java.lang.String)`, `org.apache.spark.SecurityManager.isAuthenticationEnabled:boolean()` |
| Count | 5 |

Exclusion rules applied in order, each with what it removed:

* {"applied": true, "removed": ["aclsOn_$eq"], "rule": "scala_setter_suffix"}
* {"applied": true, "removed": ["aclsOn"], "rule": "field_member_name_collision"}
* {"applied": false, "removed": [], "rule": "field_accessor_setter_evidence"}

## 4. Diagnostics the query recorded

* **`bridges`**:

```json
{
  "partialfunction_boundary": {
    "connections": [
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1 [6 methods]",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.org$apache$spark$deploy$client$StandaloneAppClient$ClientEndpoint$$askAndReplyAsync:void(org.apache.spark.rpc.RpcEndpointRef,org.apache.spark.rpc.RpcCallContext,java.lang.Object) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$org$apache$spark$deploy$client$StandaloneAppClient$ClientEndpoint$$askAndReplyAsync$1 [5 methods]",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1 [12 methods]",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1 [6 methods]",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1 [44 methods]",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1 [30 methods]",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1 [33 methods]",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1 [5 methods]",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1 [5 methods]",
      "org.apache.spark.rpc.RpcTimeout.addMessageIfTimeout:scala.PartialFunction() ==> org.apache.spark.rpc.RpcTimeout$$anonfun$addMessageIfTimeout$1 [5 methods]"
    ],
    "connections_on_emitted_paths": [
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5$adapted:java.lang.Object(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)"
    ],
    "distinct_connections": 10,
    "fired": true,
    "keyed_off": "the frontier method the traversal is standing on \u2014 its own enclosing type and, for the partial-function rule, its own name \u2014 so the rule follows whatever a caller's pattern resolved rather than a name written into this script",
    "matched_types": 10,
    "needed_by_an_emitted_path": true,
    "rule": "from a frontier method to every method of the synthetic partial-function type it allocates, whose full name is the enclosing type followed by `$$anonfun$`, the method's own name and a number",
    "succeeded": true
  },
  "thread_boundary": {
    "connections": [
      "org.apache.spark.SparkContext.stopInNewThread:void() ==> org.apache.spark.SparkContext$$anon$3.run:void()",
      "org.apache.spark.deploy.master.Master$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1) ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1.run:void()",
      "org.apache.spark.deploy.worker.DriverRunner.start:void() ==> org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()",
      "org.apache.spark.deploy.worker.ExecutorRunner.start:void() ==> org.apache.spark.deploy.worker.ExecutorRunner$$anon$1.run:void()",
      "org.apache.spark.rpc.netty.NettyRpcEnv.askAbortable:org.apache.spark.rpc.AbortableRpcFuture(org.apache.spark.rpc.netty.RequestMessage,org.apache.spark.rpc.RpcTimeout,scala.reflect.ClassTag) ==> org.apache.spark.rpc.netty.NettyRpcEnv$$anon$1.run:void()",
      "org.apache.spark.util.SparkShutdownHookManager.install:void() ==> org.apache.spark.util.SparkShutdownHookManager$$anon$2.run:void()",
      "org.apache.spark.util.logging.FileAppender.<init>:void(java.io.InputStream,java.io.File,int,boolean) ==> org.apache.spark.util.logging.FileAppender$$anon$1.run:void()"
    ],
    "connections_on_emitted_paths": [
      "org.apache.spark.deploy.worker.DriverRunner.start:void() ==> org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()",
      "org.apache.spark.deploy.worker.ExecutorRunner.start:void() ==> org.apache.spark.deploy.worker.ExecutorRunner$$anon$1.run:void()"
    ],
    "distinct_connections": 7,
    "fired": true,
    "keyed_off": "the frontier method the traversal is standing on \u2014 its own enclosing type and, for the partial-function rule, its own name \u2014 so the rule follows whatever a caller's pattern resolved rather than a name written into this script",
    "matched_types": 13,
    "needed_by_an_emitted_path": true,
    "rule": "from a frontier method to the `run` of an anonymous type it allocates, whose full name is the enclosing type followed by `$$anon$` and a number",
    "succeeded": true
  }
}
```

* **`cpg_method_count`** — 445568
* **`cpg_project_name`** — spark.cpg
* **`cpg_source`** — harness/cpg/spark.cpg
* **`graph_identity`**:

```json
{
  "canonical_path": "/opt/blitzy-harness/cpg/spark.cpg",
  "content_digest": "sha-256:6b3b135ee79f67778918804e7ed46badb8716875b581e8726bb98ba7f1c5330b",
  "content_digest_reverified_after_load": true,
  "declared_relative_path": "harness/cpg/spark.cpg",
  "digest_verification_rule": "the content digest above is taken at the canonical path before the load and taken again after it, and a difference in either the digest or the size fails the run closed: that is what ties the graph checked to the graph read across the check-then-load window. What the digest proves depends on which branch ran, so both are stated. On `imported_persisted_cpg` it is the digest of the file `importCpg` read. On `opened_existing_project` it is the digest of the pinned source file whose identity the project's recorded input path ties the project to \u2014 not of the project's own copy, which is a separate artifact that applying an overlay legitimately changes, exactly as the size rule above states. No expected digest is hardcoded here and none is compared against any record: the digest detects a change across the window and records what was read, and nothing else",
  "outcome": "existing_project_recorded_input_path_canonicalizes_to_the_pinned_graph",
  "project_applied_overlays": [
    "base",
    "callgraph",
    "controlflow",
    "dataflowOss",
    "typerel"
  ],
  "project_directory": "/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/queries/joern/.workspace/spark.cpg",
  "project_recorded_input_path": "/tmp/blitzy/blitzy-spark/blitzy-bc24581f-42e0-4f34-85a4-3a2e1121945d_343ca4/harness/cpg/spark.cpg",
  "project_recorded_input_path_canonical": "/opt/blitzy-harness/cpg/spark.cpg",
  "size_bytes": "509105796",
  "verification_rule": "an existing workspace project is opened only when the input path it recorded at creation canonicalizes \u2014 symlinks resolved \u2014 to the same file as the graph path above; a mismatch, or a recorded path that no longer resolves, fails the run closed rather than reading a stale graph. Size is recorded as evidence and is deliberately not the test: applying an overlay legitimately changes the copy the project holds without changing which graph it came from"
}
```

* **`handler_anchors`**:

```json
[
  {
    "label": "receive",
    "position": 1,
    "resolved": [
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()"
    ],
    "resolved_count": 5,
    "returns_contributed": 9,
    "selector": "org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receive:scala\\.PartialFunction\\(\\)",
    "selector_withheld": false
  },
  {
    "label": "receiveAndReply",
    "position": 2,
    "resolved": [
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    ],
    "resolved_count": 3,
    "returns_contributed": 1,
    "selector": "org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receiveAndReply:scala\\.PartialFunction\\(org\\.apache\\.spark\\.rpc\\.RpcCallContext\\)",
    "selector_withheld": false
  }
]
```

* **`handler_anchors_before_qualification`**:

```json
[
  {
    "label": "receive",
    "position": 1,
    "resolved": [
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()"
    ],
    "resolved_count": 5,
    "selector": "org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receive:scala\\.PartialFunction\\(\\)",
    "selector_withheld": false
  },
  {
    "label": "receiveAndReply",
    "position": 2,
    "resolved": [
      "org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    ],
    "resolved_count": 5,
    "selector": "org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receiveAndReply:scala\\.PartialFunction\\(org\\.apache\\.spark\\.rpc\\.RpcCallContext\\)",
    "selector_withheld": false
  }
]
```

* **`handler_qualification`**:

```json
{
  "accepted": [
    "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()"
  ],
  "accepted_count": 8,
  "applies_to": "only a resolved entry point whose signature says it IS a Scala partial function. An entry point of any other signature is admitted untouched, which is what keeps this rule general across whatever pair a caller names",
  "candidates": [
    "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
  ],
  "candidates_considered": 10,
  "excluded": [
    {
      "evidence": "the signature `scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` says this method IS a Scala partial function, but no outgoing call names a type beginning `org.apache.spark.deploy.ClientEndpoint$$anonfun$receiveAndReply$`, so it is an inherited trait default rather than a declaration; its non-operator calls are `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`",
      "full_name": "org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "rule": "declares_no_partial_function_body_class_of_its_own"
    },
    {
      "evidence": "the signature `scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` says this method IS a Scala partial function, but no outgoing call names a type beginning `org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receiveAndReply$`, so it is an inherited trait default rather than a declaration; its non-operator calls are `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`",
      "full_name": "org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "rule": "declares_no_partial_function_body_class_of_its_own"
    }
  ],
  "excluded_count": 2,
  "rule": "a resolved entry point whose signature is one an `RpcEndpoint` handler declares \u2014 `scala.PartialFunction()`, `scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` \u2014 must also allocate the synthetic partial-function class carrying its own case bodies: an outgoing call whose method full name begins with its own enclosing type, `$$anonfun$` and its own name. That is the evidence it DECLARES the partial function rather than inheriting a trait default, and it is a distinction no regex over a method full name can draw",
  "trait_forwarder_note": "a trait's static forwarder remains a traversal bridge \u2014 a route may run through one, and the traversal block reports when one did \u2014 it is only barred from being a place a route STARTS",
  "why_it_is_not_a_parameter": "an inherited trait default has no declaration of its own, so a return anchored at one names an entry point that never received a message. That is a defect in the return rather than a choice about which pair to ask about, so it is not something a caller narrows or widens"
}
```

* **`input_limits`**:

```json
{
  "cardinality_check_point": "after the anchors resolve and before any traversal starts, because the traversal is the expensive part and an over-broad pattern has to be refused rather than attempted (CWE-400)",
  "enforcement": "by refusal, never by a silent clamp: an out-of-bounds value ends the run through the failure protocol with a message naming the parameter and the limit, because a clamped run answers a question the caller did not ask while looking like one that answered theirs",
  "max_alternative_label_length": 64,
  "max_alternative_regex_length": 512,
  "max_alternatives_per_parameter": 32,
  "max_max_depth": 32,
  "max_parameter_value_length": 4096,
  "max_quantifiers_per_regex": 32,
  "max_resolved_methods_per_alternative": 256,
  "max_resolved_methods_per_end": 512,
  "max_returns_total": 500,
  "max_route_enumeration_steps_per_pair": 200000,
  "max_routes_per_pair": 8,
  "min_max_depth": 1,
  "refused_regex_constructs": "a REPEATING quantifier (`*`, `+`, `{`) applied to a group whose body already quantifies \u2014 the ambiguous nested quantification that backtracks catastrophically (CWE-1333). A `?` in the body counts, so `(a?)+` is refused as `(a+)+` is; a `?` as the OUTER quantifier does not, since a group repeating at most once cannot backtrack against itself. The scan tracks escaping, character classes, group nesting and group prefixes, so a quantifier inside a character class or behind a backslash is read as a literal and a `?` directly after `(` is read as the group prefix it is"
}
```

* **`load_mode`** — opened_existing_project
* **`parameters`**:

```json
{
  "declared": [
    {
      "declared_default": "receive=org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receive:scala\\.PartialFunction\\(\\);receiveAndReply=org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receiveAndReply:scala\\.PartialFunction\\(org\\.apache\\.spark\\.rpc\\.RpcCallContext\\)",
      "generalizes_over": "the identity of the entry point: any method the graph holds, selected by an anchored full-match regex over its full name, whatever its name, its enclosing type or its package",
      "name": "handlerPattern",
      "parsed_alternatives": [
        {
          "label": "receive",
          "label_withheld": false,
          "pattern": "org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receive:scala\\.PartialFunction\\(\\)",
          "pattern_withheld": false,
          "position": 1
        },
        {
          "label": "receiveAndReply",
          "label_withheld": false,
          "pattern": "org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receiveAndReply:scala\\.PartialFunction\\(org\\.apache\\.spark\\.rpc\\.RpcCallContext\\)",
          "pattern_withheld": false,
          "position": 2
        }
      ],
      "provenance": "declared_default_not_supplied",
      "type": "string",
      "value_supplied_by_the_caller": false,
      "value_used": "receive=org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receive:scala\\.PartialFunction\\(\\);receiveAndReply=org\\.apache\\.spark\\.deploy\\.(?!yarn\\.).*\\.receiveAndReply:scala\\.PartialFunction\\(org\\.apache\\.spark\\.rpc\\.RpcCallContext\\)",
      "value_withheld": false
    },
    {
      "declared_default": "createDriver=org\\.apache\\.spark\\.deploy\\..*createDriver:.*;DriverRunner=org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>:.*;process_launch=(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec):.*;ExecutorRunner_additional=org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>:.*",
      "generalizes_over": "the identity of the privileged sink: any method the graph holds, selected the same way, including a constructor and a method with no body of its own",
      "name": "sinkPattern",
      "parsed_alternatives": [
        {
          "label": "createDriver",
          "label_withheld": false,
          "pattern": "org\\.apache\\.spark\\.deploy\\..*createDriver:.*",
          "pattern_withheld": false,
          "position": 1
        },
        {
          "label": "DriverRunner",
          "label_withheld": false,
          "pattern": "org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>:.*",
          "pattern_withheld": false,
          "position": 2
        },
        {
          "label": "process_launch",
          "label_withheld": false,
          "pattern": "(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec):.*",
          "pattern_withheld": false,
          "position": 3
        },
        {
          "label": "ExecutorRunner_additional",
          "label_withheld": false,
          "pattern": "org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>:.*",
          "pattern_withheld": false,
          "position": 4
        }
      ],
      "provenance": "declared_default_not_supplied",
      "type": "string",
      "value_supplied_by_the_caller": false,
      "value_used": "createDriver=org\\.apache\\.spark\\.deploy\\..*createDriver:.*;DriverRunner=org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>:.*;process_launch=(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec):.*;ExecutorRunner_additional=org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>:.*",
      "value_withheld": false
    },
    {
      "declared_default": "20",
      "generalizes_over": "how far the traversal may follow call edges from an entry point, so that a handler and a sink further apart than the default pair can still be related",
      "name": "maxDepth",
      "provenance": "declared_default_not_supplied",
      "type": "string at the invocation boundary, parsed by this script as an integer within MIN_MAX_DEPTH..MAX_MAX_DEPTH",
      "value_parsed": 20,
      "value_supplied_by_the_caller": false,
      "value_used": "20",
      "value_withheld": false
    }
  ],
  "label_note": "a label is a name and nothing more: the script resolves and reports every alternative identically, whatever its label says",
  "match_mode": "anchored full match against a method full name as the frontend records it \u2014 owner type, method name, then a signature",
  "pattern_list_format": "one or more alternatives separated by `;`, each of the form <label>=<regex> \u2014 the label is everything before the first `=`, trimmed, and the regex is everything after it, taken verbatim; an alternative with no `=` is labelled with its own pattern text",
  "pattern_list_format_limitation": "the format defines no escape for its own separator, so a `;` cannot appear inside a pattern",
  "provenance_rule": "every parameter is DECLARED with a NUL-bearing sentinel no process argument can carry, and the declared default authored in this file is substituted when that sentinel arrives. `supplied_by_the_caller` and `declared_default_not_supplied` are therefore facts about the invocation rather than a comparison against the default: a caller who passes the declared default verbatim is recorded as having supplied it, and a caller who passes an explicitly empty value is recorded as having supplied that \u2014 it is then refused as an empty pattern or an empty depth rather than silently replaced by a default"
}
```

* **`pinned_class_handler_census`**:

```json
{
  "accepted_as_entry_points": 8,
  "methods": [
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.ClientEndpoint",
      "full_name": "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction()"
    },
    {
      "accepted_as_an_entry_point": false,
      "enclosing_type": "org.apache.spark.deploy.ClientEndpoint",
      "full_name": "org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "nominated_by_a_pattern": true,
      "outcome": "declares_no_partial_function_body_class_of_its_own",
      "signature": "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    },
    {
      "accepted_as_an_entry_point": false,
      "enclosing_type": "org.apache.spark.deploy.DriverRedirectConsolePlugin",
      "full_name": "org.apache.spark.deploy.DriverRedirectConsolePlugin.receive:java.lang.Object(java.lang.Object)",
      "nominated_by_a_pattern": false,
      "outcome": "not_matched_by_any_handler_pattern_alternative_in_force",
      "signature": "java.lang.Object(java.lang.Object)"
    },
    {
      "accepted_as_an_entry_point": false,
      "enclosing_type": "org.apache.spark.deploy.DriverTimeoutDriverPlugin",
      "full_name": "org.apache.spark.deploy.DriverTimeoutDriverPlugin.receive:java.lang.Object(java.lang.Object)",
      "nominated_by_a_pattern": false,
      "outcome": "not_matched_by_any_handler_pattern_alternative_in_force",
      "signature": "java.lang.Object(java.lang.Object)"
    },
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint",
      "full_name": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction()"
    },
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint",
      "full_name": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    },
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.master.Master",
      "full_name": "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction()"
    },
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.master.Master",
      "full_name": "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    },
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.worker.Worker",
      "full_name": "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction()"
    },
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.worker.Worker",
      "full_name": "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    },
    {
      "accepted_as_an_entry_point": true,
      "enclosing_type": "org.apache.spark.deploy.worker.WorkerWatcher",
      "full_name": "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()",
      "nominated_by_a_pattern": true,
      "outcome": "accepted_as_an_entry_point",
      "signature": "scala.PartialFunction()"
    },
    {
      "accepted_as_an_entry_point": false,
      "enclosing_type": "org.apache.spark.deploy.worker.WorkerWatcher",
      "full_name": "org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "nominated_by_a_pattern": true,
      "outcome": "declares_no_partial_function_body_class_of_its_own",
      "signature": "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    },
    {
      "accepted_as_an_entry_point": false,
      "enclosing_type": "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint",
      "full_name": "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receive:scala.PartialFunction()",
      "nominated_by_a_pattern": false,
      "outcome": "not_matched_by_any_handler_pattern_alternative_in_force",
      "signature": "scala.PartialFunction()"
    },
    {
      "accepted_as_an_entry_point": false,
      "enclosing_type": "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint",
      "full_name": "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "nominated_by_a_pattern": false,
      "outcome": "not_matched_by_any_handler_pattern_alternative_in_force",
      "signature": "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    }
  ],
  "methods_in_graph": 14,
  "names_listed": [
    "receive",
    "receiveAndReply"
  ],
  "nominated_by_the_patterns_in_force": 10,
  "package_prefix": "org.apache.spark.deploy.",
  "purpose": "evidence, never a selector: what the patterns in force did and did not nominate among the methods the pinned class names, with the graph facts a reader compares against a pattern. Nothing in the traversal depends on this listing"
}
```

* **`redaction`**:

```json
{
  "digest_algorithm": "SHA-256",
  "fields_always_literal": [
    "parameters.declared[].declared_default \u2014 authored in this file",
    "every method full name, type name and predicate name \u2014 read from the graph",
    "every limit in input_limits \u2014 authored in this file"
  ],
  "governed_fields": [
    "parameters.declared[].value_used \u2014 replaced by value_length and value_digest",
    "parameters.declared[].parsed_alternatives[].label \u2014 replaced by a positional stand-in of the form <parameter>#<position>, with label_length and label_digest",
    "parameters.declared[].parsed_alternatives[].pattern \u2014 replaced by pattern_length and pattern_digest",
    "handler_anchors[].label and sink_anchors[].label \u2014 the same positional stand-in",
    "handler_anchors[].selector and sink_anchors[].selector \u2014 replaced by selector_length and selector_digest",
    "handler_qualification.excluded[] \u2014 method full names come from the graph and are emitted in full; no caller text appears here",
    "traversal.no_returns_explanation \u2014 names alternatives by their reported label only",
    "every validation failure message on stderr \u2014 names the parameter, the alternative's position and the limit, and references the text by length and digest, including a value that parsed as an integer and a regex the JDK refused: `PatternSyntaxException.getMessage` embeds the pattern, so its description and index are reported instead of its message"
  ],
  "outside_this_scripts_control": "the Joern script runner prints `executing <script> with params=Map(...)` to stderr before this script's first statement runs, so a captured stderr log carries the invocation as the RUNNER echoed it. That line is the interpreter's and cannot be suppressed from inside a script. Everything this script emits \u2014 the result object, every diagnostics field and every failure message \u2014 withholds a caller-supplied value and references it by length and digest, so a report that must not disclose one has to withhold or filter that captured line rather than rely on this script for it",
  "policy": "a value the caller supplied is never echoed. What is emitted for one is its parameter name, its length in characters and a SHA-256 digest of its UTF-8 bytes \u2014 enough to prove that a claimed invocation is the one that ran, and not enough to publish anything the caller did not intend to. A literal is emitted only where the value in force is the declared default authored in this file, which carries nothing of the caller's",
  "stated_exception": "the numeric bound in force is emitted as an integer \u2014 `parameters.declared[].value_parsed` and `traversal.max_call_depth` \u2014 even when the caller supplied it, because every count in this result is a count under that bound and a result that hid it could not be interpreted. The caller's raw TEXT for it is still withheld and referenced by length and digest, and no pattern value is emitted under any circumstances. This is stated rather than left to be noticed, so that a report written against this policy claims exactly what the code enforces",
  "values_withheld_in_this_run": []
}
```

* **`resolved_cardinality`**:

```json
{
  "checked_before_any_traversal": true,
  "handler_pattern_resolved_distinct_after_qualification": 8,
  "handler_pattern_resolved_distinct_before_qualification": 10,
  "largest_handler_alternative": 5,
  "largest_sink_alternative": 1,
  "limit_per_alternative": 256,
  "limit_per_end": 512,
  "reading": "the limits were applied to the RAW resolved sets, before the structural rule narrowed the entry-point set, so a caller cannot slip an over-broad pattern past them by relying on the rule to shrink it afterwards",
  "sink_pattern_resolved_distinct": 4
}
```

* **`route_enumeration`**:

```json
{
  "deduplication": "on the exact emitted tuple \u2014 entry point, sink, ordered path, predicates found on the path \u2014 and never on the sink method, so two different routes to one sink are two returns",
  "max_enumeration_steps_per_pair": 200000,
  "max_returns_total": 500,
  "max_routes_per_pair": 8,
  "method": "a forward pass per entry point that expands each method at most once and records every edge it observes as a SET of predecessors per method, then a backward pass per (entry point, sink) pair that enumerates every distinct simple ordered route over those edges within the depth bound, in predecessor-name order",
  "pair_labels_are_graph_derived": "the pair labels listed above are method full names read from the graph, never caller-supplied pattern text, so a bound can be audited without echoing an input",
  "pairs_enumerated": 4,
  "pairs_stopped_at_the_step_cap": 0,
  "pairs_stopped_at_the_step_cap_list": [],
  "pairs_with_routes_beyond_the_cap": 0,
  "pairs_with_routes_beyond_the_cap_list": [],
  "returns_discarded_by_the_total_cap": 0,
  "routes_kept": 10,
  "stated_scope": "each method is expanded once, so the routes enumerated are those lying in the edge set the bounded frontier observed; a route requiring a method to be expanded a second time deeper than its first discovery is outside this pass",
  "what_a_reached_bound_discarded": "a pair listed against the route cap has at least one further route this run did not emit \u2014 the search looks one route past the cap, so the flag is exact \u2014 and the number of further routes is not enumerated. A pair listed against the step cap stopped its backward search there, so further routes for it are unknown rather than absent. The total cap discards whole returns, and the count above is exactly how many"
}
```

* **`sink_anchors`**:

```json
[
  {
    "label": "createDriver",
    "position": 1,
    "resolved": [
      "org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)"
    ],
    "resolved_count": 1,
    "returns_contributed": 1,
    "selector": "org\\.apache\\.spark\\.deploy\\..*createDriver:.*",
    "selector_withheld": false
  },
  {
    "label": "DriverRunner",
    "position": 2,
    "resolved": [
      "org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)"
    ],
    "resolved_count": 1,
    "returns_contributed": 1,
    "selector": "org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>:.*",
    "selector_withheld": false
  },
  {
    "label": "process_launch",
    "position": 3,
    "resolved": [
      "java.lang.ProcessBuilder.start:java.lang.Process()"
    ],
    "resolved_count": 1,
    "returns_contributed": 7,
    "selector": "(java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec):.*",
    "selector_withheld": false
  },
  {
    "label": "ExecutorRunner_additional",
    "position": 4,
    "resolved": [
      "org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)"
    ],
    "resolved_count": 1,
    "returns_contributed": 1,
    "selector": "org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>:.*",
    "selector_withheld": false
  }
]
```

* **`traversal`**:

```json
{
  "bound_reached": true,
  "direction": "forward over callee edges, one traversal per resolved entry point, from the node set `handlerPattern` resolved to the node set `sinkPattern` resolved, then a bounded backward enumeration of routes over the edges that traversal observed",
  "edges_observed_summed_over_entry_points": 408730,
  "entry_points_traversed": 8,
  "entry_points_truncated_at_bound": 4,
  "expansion_restriction": "the frontier expands only through a method the graph carries a body for; an operator pseudo-method and a derived predicate are never expanded, so no emitted path has a predicate as an intermediate node. A sink or a predicate is still recognised wherever it is reached, including where the graph carries no body for it",
  "expansion_restriction_evidence": {
    "methods_carrying_no_body_that_hold_a_callee_edge": 0,
    "methods_the_graph_carries_a_body_for": 380024,
    "methods_the_graph_carries_no_body_for": 65544,
    "reading": "a method carrying no body holds no callee edge to follow, so the bodied methods are the whole of what any callee traversal could expand through; the restriction is therefore a property of this graph rather than a choice of code base, and it is measured here rather than asserted"
  },
  "generality": "the traversal reads both ends from the resolved node sets and tests membership by method full name, so no step of it depends on an entry point's name, a sink's name, or the package either lies in",
  "longest_emitted_route_edge_count": 9,
  "max_call_depth": 20,
  "max_call_depth_source": "the `maxDepth` parameter",
  "methods_seen_summed_over_entry_points": 119556,
  "no_returns_explanation": null,
  "path_selection": "one return per distinct ordered route from an entry point to a sink, up to the bounds in `route_enumeration`; predecessors are followed in full-name order and returns are sorted, so the output is reproducible",
  "predicate_check_reach": "the emitted path nodes, plus one outgoing call step from each of them",
  "predicates_on_path_dependence_on_the_bound": "the paths this run emitted are the paths reachable within the bound above, so the predicates found on them are a property of that bound as well as of the graph: a path stopping at a construction does not traverse what a path continuing past it traverses. A caller changing `maxDepth` changes which paths exist to be checked, and this field records that rather than smoothing it over",
  "relation_expressed": "reachability over call edges: a return says a chain of calls, plus the bridges recorded above, runs from the entry point to the sink. Whether a value the entry point received arrives at an argument of the sink is a different relation, over data dependence, which this formulation does not express and does not claim to",
  "returns_emitted": 10,
  "returns_removed_by_deduplication": 0,
  "returns_traversing_a_trait_default_method_forwarder": [
    "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> java.lang.ProcessBuilder.start:java.lang.Process()"
  ],
  "returns_whose_emitted_path_carries_a_derived_predicate": 0,
  "returns_whose_path_traverses_a_trait_default_method_forwarder": 1,
  "shortest_emitted_route_edge_count": 2,
  "sink_methods_resolved": 4
}
```

* **`workspace`** — queries/joern/.workspace

## 5. Failure capture and the stderr reference

| | |
|---|---|
| Captured stderr | `stderr_ref: null` — the invocation succeeded, so no failure diagnostic is owed or cited. The envelope's contract makes this a reference on failure |
| Failure-marker lines | none — the run emitted no failure marker |
| What was on that stream | Joern's own load and save lines. Its script runner also prints an `executing <script> with params=Map(...)` line before the script's first statement, so where a query is invoked with parameters that line is the runner's echo of them, not the script's — which is why the redaction the script performs cannot reach it |

`queries/joern/.workspace/` is Joern scratch: unbounded, uncounted, not a deliverable and
not committed — which is why no deliverable cites a file inside it as evidence.

## 6. Revision log

A revision is one recorded execution of a distinct source text for this query. The driver
hashes the source before running it and appends; the log is append-only across driver
invocations, and the count below is the number of distinct hashes in it.

| # | Executed at (UTC) | Source sha256 | compiled | ran | Returns | Spurious |
|---|---|---|---|---|---|---|
| 1 | `2026-08-22T06:12:36Z` | `489074c782c5deb6d3443a05834d09335c3bbda0142b547a273dc87dc37f934c` | True | True | 10 | 0 |
| 2 | `2026-08-22T06:18:16Z` | `489074c782c5deb6d3443a05834d09335c3bbda0142b547a273dc87dc37f934c` | True | True | 10 | 0 |
| 3 | `2026-08-22T06:23:37Z` | `489074c782c5deb6d3443a05834d09335c3bbda0142b547a273dc87dc37f934c` | True | True | 10 | 0 |
| 4 | `2026-08-22T06:33:14Z` | `489074c782c5deb6d3443a05834d09335c3bbda0142b547a273dc87dc37f934c` | True | True | 10 | 0 |
| 5 | `2026-08-22T06:52:32Z` | `489074c782c5deb6d3443a05834d09335c3bbda0142b547a273dc87dc37f934c` | True | True | 10 | 0 |
| 6 | `2026-08-22T10:12:30Z` | `98acda311bdb4f84cfa4a68d302720a3186c0617244b2dad7f11d9d817d4f18a` | True | True | 10 | 0 |
| 7 | `2026-08-22T10:31:43Z` | `49ce08f0d7e592827eb6241857fb934dc1290ecbb372109e1f8c711c2fad79b2` | True | True | 10 | 0 |

Distinct source texts executed by the driver: **3**. Executions recorded: **7**.

## 7. What this file does not claim

It does not claim that anything this query returned is a vulnerability, a real bug or a
false positive, and it compares nothing against any other tool. The spurious count is a
property of the query — whether it matched what it was asked to match — reached by the
mechanical on-path test above and by nothing else. The graph was **read**, with
`importCpg`, and not built.
