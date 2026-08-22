# `01-callgraph-unguarded-driver-launch` — query outcome

Written by the Phase 3 driver from its own capture of this query's invocation. The query
did not write this file, and nothing in it is asserted that the driver did not observe.
The envelope beside it, `queries/joern/results/01-callgraph-unguarded-driver-launch.json`,
carries 22 top-level keys: the 18 this contract requires, present and in order, plus four
supplementary keys, each the only record of a fact the AAP requires reported. They are
named one by one, with the fact each carries, in `oss-scan-results/joern-probe.md` under
*The envelope these statements are written from*.

| | |
|---|---|
| Query source | `queries/joern/01-callgraph-unguarded-driver-launch.sc` |
| Source sha256 | `535237eeef30e07b7f7a8f8f27c361e9173944094e05685394e67d32d7575ff8` |
| Invoked | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/01-callgraph-unguarded-driver-launch.sc` |
| Parameters | none — the script's declared defaults are in force |
| JVM | `openjdk version "21.0.12.1" 2026-08-18 LTS` (`JAVA_HOME=/opt/blitzy-harness/tools/jdk-21.0.12.1+1`) |
| `JAVA_OPTS` | `-Xmx48g -Xss64m` |
| Started / ended (UTC) | `2026-08-22T10:08:29Z` / `2026-08-22T10:08:45Z` |
| Elapsed | 15.7 s |
| Exit code | `0` |
| Start marker seen | True |
| Result region parsed | True |
| `compiled` | True |
| `ran` | True |
| Returns | 10 |
| Spurious under the on-path test | 0 |
| Clean positive | True |
| Graph | `harness/cpg/spark.cpg`, **read and not built**: loaded with `importCpg`, because no project for it was present in the shared workspace at this invocation — `load_mode: imported_persisted_cpg`. Where one is present the script opens that project instead, after verifying its recorded input path canonicalizes to this same file; `built: False` on either branch |

## 1. The precondition, as the driver observed it

The driver runs only after the controller has published the dataset. This is what it
observed at `2026-08-22T10:08:29Z`, immediately before invoking this query — not an assumption:

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
  "outcome": "no_existing_project_the_pinned_graph_was_imported",
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
    "kind": "user_named",
    "label": "receive",
    "resolved": [
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()"
    ],
    "resolved_count": 5,
    "selector": "method name is exactly `receive`, the enclosing type full name matches org\\.apache\\.spark\\.deploy\\..* and not org\\.apache\\.spark\\.deploy\\.yarn\\..*, the signature is one an RpcEndpoint handler declares, and the method allocates the partial-function body class of its own name"
  },
  {
    "kind": "user_named",
    "label": "receiveAndReply",
    "resolved": [
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
    ],
    "resolved_count": 3,
    "selector": "method name is exactly `receiveAndReply`, the enclosing type full name matches org\\.apache\\.spark\\.deploy\\..* and not org\\.apache\\.spark\\.deploy\\.yarn\\..*, the signature is one an RpcEndpoint handler declares, and the method allocates the partial-function body class of its own name"
  }
]
```

* **`handler_selection`**:

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
  "candidates": [
    "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.DriverRedirectConsolePlugin.receive:java.lang.Object(java.lang.Object)",
    "org.apache.spark.deploy.DriverTimeoutDriverPlugin.receive:java.lang.Object(java.lang.Object)",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
  ],
  "candidates_considered": 14,
  "enclosing_type_selector": "org\\.apache\\.spark\\.deploy\\..*",
  "excluded": [
    {
      "evidence": "no outgoing call names a type beginning `org.apache.spark.deploy.ClientEndpoint$$anonfun$receiveAndReply$`, so this is an inherited trait default rather than a declaration; its non-operator calls are `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`",
      "full_name": "org.apache.spark.deploy.ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "rule": "declares_no_partial_function_body_class_of_its_own"
    },
    {
      "evidence": "signature is `java.lang.Object(java.lang.Object)`, and an RpcEndpoint handler declares one of `scala.PartialFunction()`, `scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`",
      "full_name": "org.apache.spark.deploy.DriverRedirectConsolePlugin.receive:java.lang.Object(java.lang.Object)",
      "rule": "signature_is_not_an_rpc_endpoint_handler_signature"
    },
    {
      "evidence": "signature is `java.lang.Object(java.lang.Object)`, and an RpcEndpoint handler declares one of `scala.PartialFunction()`, `scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`",
      "full_name": "org.apache.spark.deploy.DriverTimeoutDriverPlugin.receive:java.lang.Object(java.lang.Object)",
      "rule": "signature_is_not_an_rpc_endpoint_handler_signature"
    },
    {
      "evidence": "no outgoing call names a type beginning `org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receiveAndReply$`, so this is an inherited trait default rather than a declaration; its non-operator calls are `org.apache.spark.rpc.RpcEndpoint.receiveAndReply$:scala.PartialFunction(org.apache.spark.rpc.RpcEndpoint,org.apache.spark.rpc.RpcCallContext)`",
      "full_name": "org.apache.spark.deploy.worker.WorkerWatcher.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "rule": "declares_no_partial_function_body_class_of_its_own"
    },
    {
      "evidence": "enclosing type `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint` matches org\\.apache\\.spark\\.deploy\\.yarn\\..*, and the class this query attempts is standalone deploy mode",
      "full_name": "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receive:scala.PartialFunction()",
      "rule": "enclosing_type_is_outside_standalone_deploy"
    },
    {
      "evidence": "enclosing type `org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint` matches org\\.apache\\.spark\\.deploy\\.yarn\\..*, and the class this query attempts is standalone deploy mode",
      "full_name": "org.apache.spark.deploy.yarn.ApplicationMaster$AMEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "rule": "enclosing_type_is_outside_standalone_deploy"
    }
  ],
  "excluded_count": 6,
  "excluded_enclosing_type_selector": "org\\.apache\\.spark\\.deploy\\.yarn\\..*",
  "name_selector": [
    "receive",
    "receiveAndReply"
  ],
  "own_body_class_rule": "an outgoing call whose method full name begins with the candidate's own enclosing type, `$$anonfun$` and the candidate's own name \u2014 the evidence that the candidate DECLARES the partial function rather than inheriting a trait default",
  "rule_order": [
    "signature_is_not_an_rpc_endpoint_handler_signature",
    "enclosing_type_is_outside_standalone_deploy",
    "declares_no_partial_function_body_class_of_its_own"
  ],
  "signature_selector": [
    "scala.PartialFunction()",
    "scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)"
  ],
  "trait_forwarder_note": "a trait's static forwarder remains a traversal bridge \u2014 a route may run through one, and the traversal block reports when one did \u2014 it is only barred from being a place a route STARTS"
}
```

* **`load_mode`** — imported_persisted_cpg
* **`route_enumeration`**:

```json
{
  "deduplication": "on the exact emitted tuple \u2014 entry point, sink, ordered path, predicates found on the path \u2014 and never on the sink method, so two different routes to one sink are two returns",
  "max_enumeration_steps_per_pair": 200000,
  "max_returns_total": 500,
  "max_routes_per_pair": 8,
  "method": "a forward pass per entry point that expands each method at most once and records every edge it observes as a SET of predecessors per method, then a backward pass per (entry point, sink) pair that enumerates every distinct simple ordered route over those edges within the depth bound, in predecessor-name order",
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
    "kind": "user_named",
    "label": "createDriver",
    "resolved": [
      "org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)"
    ],
    "resolved_count": 1,
    "selector": "method name matches (.*\\$\\$)?createDriver (which admits the Scala-mangled form of a private method) and the enclosing type full name matches org\\.apache\\.spark\\.deploy\\..*"
  },
  {
    "kind": "user_named",
    "label": "DriverRunner",
    "resolved": [
      "org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)"
    ],
    "resolved_count": 1,
    "selector": "method full name matches org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>.* (the construction of a DriverRunner)"
  },
  {
    "kind": "user_named",
    "label": "process_launch",
    "resolved": [
      "java.lang.ProcessBuilder.start:java.lang.Process()"
    ],
    "resolved_count": 1,
    "selector": "method full name matches (java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec).*"
  },
  {
    "kind": "additional",
    "label": "ExecutorRunner",
    "resolved": [
      "org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)"
    ],
    "resolved_count": 1,
    "selector": "method full name matches org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>.* (the construction of an ExecutorRunner) \u2014 carried in addition to the three above and never in place of one"
  }
]
```

* **`traversal`**:

```json
{
  "bound_reached": true,
  "direction": "forward over callee edges, one traversal per resolved entry point, then a bounded backward enumeration of routes over the edges that traversal observed",
  "edges_observed_summed_over_entry_points": 408730,
  "entry_points_traversed": 8,
  "entry_points_truncated_at_bound": 4,
  "expansion_restriction": "the frontier expands only through methods whose full name begins with `org.apache.spark.`; an operator pseudo-method and a derived predicate are never expanded, so no emitted path has a predicate as an intermediate node. A sink or a predicate is still recognised wherever it is reached, including outside that prefix",
  "longest_emitted_route_edge_count": 9,
  "max_call_depth": 20,
  "methods_seen_summed_over_entry_points": 119556,
  "path_selection": "one return per distinct ordered route from an entry point to a sink, up to the bounds in `route_enumeration`; predecessors are followed in full-name order and returns are sorted, so the output is reproducible",
  "predicate_check_reach": "the emitted path nodes, plus one outgoing call step from each of them",
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
| Captured stderr | `stderr_ref: null` — the invocation succeeded, so no failure diagnostic is owed or cited. The envelope's contract makes this field a *reference* on failure and never a copy: populated, it is an object naming the captured stream and the lines within it that hold the diagnostic — `{"path": "<the captured stderr stream for this invocation>", "line_range": [<first>, <last>]}` — so the stream is cited by path and line range rather than quoted, which is what keeps anything a tool printed to it from being republished here. `null` is that reference's empty form. This run produced no populated instance: each of its three invocations exited `0` with a start marker printed and a result region parsed, so no failure diagnostic was captured for any of them |
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
| 1 | `2026-08-22T06:08:04Z` | `d387a7f7ba70804f1b0d93136c6997e6dc4e5ec6efaa70c33b55cdd652bff448` | True | True | 10 | 0 |
| 2 | `2026-08-22T06:14:09Z` | `d387a7f7ba70804f1b0d93136c6997e6dc4e5ec6efaa70c33b55cdd652bff448` | True | True | 10 | 0 |
| 3 | `2026-08-22T06:19:13Z` | `d387a7f7ba70804f1b0d93136c6997e6dc4e5ec6efaa70c33b55cdd652bff448` | True | True | 10 | 0 |
| 4 | `2026-08-22T06:29:08Z` | `d387a7f7ba70804f1b0d93136c6997e6dc4e5ec6efaa70c33b55cdd652bff448` | True | True | 10 | 0 |
| 5 | `2026-08-22T06:48:45Z` | `d387a7f7ba70804f1b0d93136c6997e6dc4e5ec6efaa70c33b55cdd652bff448` | True | True | 10 | 0 |
| 6 | `2026-08-22T10:08:29Z` | `535237eeef30e07b7f7a8f8f27c361e9173944094e05685394e67d32d7575ff8` | True | True | 10 | 0 |

Distinct source texts executed by the driver: **2**. Executions recorded: **6**.

## 7. What this file does not claim

It does not claim that anything this query returned is a vulnerability, a real bug or a
false positive, and it compares nothing against any other tool. The spurious count is a
property of the query — whether it matched what it was asked to match — reached by the
mechanical on-path test above and by nothing else. The graph was **read**, with
`importCpg`, and not built.
