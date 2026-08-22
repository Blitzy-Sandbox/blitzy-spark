# `02-dataflow-unguarded-driver-launch` — query outcome

Written by the Phase 3 driver from its own capture of this query's invocation. The query
did not write this file, and nothing in it is asserted that the driver did not observe.
The envelope beside it, `queries/joern/results/02-dataflow-unguarded-driver-launch.json`,
carries 22 top-level keys: the 18 this contract requires, present and in order, plus four
supplementary keys, each the only record of a fact the AAP requires reported. They are
named one by one, with the fact each carries, in `oss-scan-results/joern-probe.md` under
*The envelope these statements are written from*.

| | |
|---|---|
| Query source | `queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| Source sha256 | `045b5df31ff41bb03abe92020421e3a432dd711a96880bf0d3f48e3d50363edd` |
| Invoked | `/opt/blitzy-harness/tools/joern-cli/joern --script queries/joern/02-dataflow-unguarded-driver-launch.sc` |
| Parameters | none — the script's declared defaults are in force |
| JVM | `openjdk version "21.0.12.1" 2026-08-18 LTS` (`JAVA_HOME=/opt/blitzy-harness/tools/jdk-21.0.12.1+1`) |
| `JAVA_OPTS` | `-Xmx48g -Xss64m` |
| Started / ended (UTC) | `2026-08-22T10:28:10Z` / `2026-08-22T10:31:43Z` |
| Elapsed | 213.4 s |
| Exit code | `0` |
| Start marker seen | True |
| Result region parsed | True |
| `compiled` | True |
| `ran` | True |
| Returns | 1 |
| Spurious under the on-path test | 0 |
| Clean positive | True |
| Graph | `harness/cpg/spark.cpg`, **read and not built**: this invocation opened the project a previous `importCpg` of that same file had created in the shared workspace, after verifying the input path the project recorded canonicalizes to it — `load_mode: opened_existing_project`. The script calls `importCpg` itself where no such project is present; `built: False` on either branch |

## 1. The precondition, as the driver observed it

The driver runs only after the controller has published the dataset. This is what it
observed at `2026-08-22T10:28:10Z`, immediately before invoking this query — not an assumption:

| Published output | Present | Rows | Bytes | sha256 |
|---|---|---|---|---|
| `oss-scan-results/findings.json` | True | 10178 | 5806988 | `2b3fb2dbb5c2f30c711524a5a0be141aab8445e00814a7fdf6f8ba6c6f664f51` |
| `oss-scan-results/findings.csv` | True | 10178 | 3309257 | `68ae2e4ed1b0f9197a4e813c4e73f9d9c2a9864143d9f56c8173af9aa5f25e13` |
| `oss-scan-results/severity-map.md` | True | — | 6049 | `ebf11a85342c7e62c3a2ad1f403ea13672dd1bd579746f85969ac47798a8207f` |

## 2. Outcome

**The query compiled, ran, and returned 1 result(s)**, of which 0 are spurious under
the on-path test and 1 is not.

### 2.1 Returns

| # | Handler | Sink | Path length | Predicates on path | Spurious |
|---|---|---|---|---|---|
| 1 | `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` | `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)` | 3 | none | no |

### 2.2 Paths as emitted

Each path is the ordered list of method full names the query emitted, handler first
and sink last.

**Return 1** — `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)` ⇒ `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

1. `org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)`
2. `org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)`
3. `org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)`

## 3. Spurious returns under the on-path test

A return is spurious when an authentication or ACL predicate from the set the query derived at execution time lies on the path from the handler to the sink, and for no other reason. The test is applied mechanically to `predicates_on_path`: non-empty means spurious, empty means not spurious. No broader judgement is applied, and the determination is a property of the query rather than of Spark.

| | |
|---|---|
| Returns emitted | 1 |
| Spurious | 0 |
| Not spurious | 1 |

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
    "applied_by_this_query": true,
    "applied_note": "applied at source selection: it is what makes a message source resolvable, and it is not needed for an entry point that declares a message parameter itself",
    "boundary_resolved": true,
    "connections": [
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$1:java.lang.String(java.lang.String)",
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$2:java.lang.String(java.lang.String)",
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1.<init>:void(org.apache.spark.deploy.ClientEndpoint)",
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1.isDefinedAt:boolean(java.lang.Object)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$1:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1,java.lang.String,java.lang.String,java.lang.String,int)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$2:java.lang.String(java.lang.String)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$3:java.lang.String()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$4:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1,java.lang.String,scala.Enumeration$Value,java.lang.String)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$5:java.lang.String()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$6:java.lang.String()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$7:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1,java.lang.String,java.lang.String)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$anonfun$applyOrElse$8:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1,org.apache.spark.rpc.RpcEndpointRef)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.<init>:void(org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction() ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1.isDefinedAt:boolean(java.lang.Object)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$10:java.lang.String()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$9:java.lang.String()",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1.<init>:void(org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint,org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1.isDefinedAt:boolean(java.lang.Object)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1.$anonfun$run$1:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1.<init>:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1.run:void()",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$10:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$11:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$12:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$13:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$14$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.DeployMessages$WorkerExecutorStateResponse)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$14:boolean(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.DeployMessages$WorkerExecutorStateResponse)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$15$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.DeployMessages$WorkerExecutorStateResponse)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$15:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.DeployMessages$WorkerExecutorStateResponse)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$16$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.DeployMessages$WorkerDriverStateResponse)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$16:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.DeployMessages$WorkerDriverStateResponse)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$17$adapted:java.lang.Object(java.lang.String,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$17:boolean(java.lang.String,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$18$adapted:java.lang.Object(org.apache.spark.deploy.master.WorkerInfo,scala.collection.immutable.Map,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$18:void(org.apache.spark.deploy.master.WorkerInfo,scala.collection.immutable.Map,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$19:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$1:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$20$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.ExecutorDescription)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$20:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo,org.apache.spark.deploy.ExecutorDescription)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$21$adapted:java.lang.Object(org.apache.spark.deploy.ExecutorDescription,scala.Tuple2)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$21:boolean(org.apache.spark.deploy.ExecutorDescription,scala.Tuple2)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$22$adapted:java.lang.Object(org.apache.spark.deploy.master.WorkerInfo,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$22:void(org.apache.spark.deploy.master.WorkerInfo,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$23$adapted:java.lang.Object(java.lang.String,scala.Tuple2)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$23:boolean(java.lang.String,scala.Tuple2)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$24:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$25:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$26$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.ApplicationInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$26:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.ApplicationInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$2:java.lang.String()",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$3$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$3:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$4$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$4:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$5$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$5:void(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$6:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.ApplicationDescription)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$7:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,org.apache.spark.deploy.ApplicationDescription,org.apache.spark.deploy.master.ApplicationInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$8:java.lang.String(org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$anonfun$applyOrElse$9:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.<init>:void(org.apache.spark.deploy.master.Master)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.isDefinedAt:boolean(java.lang.Object)",
      "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction() ==> org.apache.spark.deploy.master.Master$$anonfun$receive$1.org$apache$spark$deploy$master$Master$$anonfun$$$outer:org.apache.spark.deploy.master.Master()",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$27:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,org.apache.spark.deploy.DriverDescription)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$28:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$29$adapted:java.lang.Object(java.lang.String,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$29:boolean(java.lang.String,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$30$adapted:java.lang.Object(java.lang.String,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$30:void(java.lang.String,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$31:org.apache.spark.internal.MessageWithContext(org.apache.spark.internal.MessageWithContext)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$32:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$33:java.lang.String()",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$34$adapted:java.lang.Object(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$34:void(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$35$adapted:java.lang.Object(java.lang.String,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$35:void(java.lang.String,org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$36:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,java.lang.String)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$37:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,int,int)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$38$adapted:java.lang.Object(java.lang.String,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$38:boolean(java.lang.String,org.apache.spark.deploy.master.DriverInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$39:java.lang.String(org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$40:java.lang.String(org.apache.spark.deploy.master.WorkerInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$41:scala.Option(int,org.apache.spark.deploy.master.ApplicationInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$42:java.lang.String(int,scala.Enumeration$Value)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$43:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,org.apache.spark.deploy.master.ExecutorDesc,scala.Enumeration$Value)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$44$adapted:java.lang.Object(org.apache.spark.deploy.master.ExecutorDesc)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$44:boolean(org.apache.spark.deploy.master.ExecutorDesc)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$45:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,org.apache.spark.deploy.master.ApplicationInfo)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$46:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1,java.lang.String,int)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.<init>:void(org.apache.spark.deploy.master.Master,org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1.isDefinedAt:boolean(java.lang.Object)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$10:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,org.apache.spark.rpc.RpcEndpointRef)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$11:org.apache.spark.deploy.DeployMessages$WorkerExecutorStateResponse(org.apache.spark.deploy.worker.ExecutorRunner)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$12:org.apache.spark.deploy.DeployMessages$WorkerDriverStateResponse(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$13:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$14:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$15:java.lang.String()",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$16:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String,int,org.apache.spark.deploy.ApplicationDescription)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$17:scala.collection.immutable.ArraySeq(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$18:scala.Option(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$19:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.IOException)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$1:java.lang.String(org.apache.spark.deploy.worker.ExecutorRunner)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$20:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String,int,org.apache.spark.deploy.ApplicationDescription)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$21:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String,int)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$22:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$23:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$24:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$25:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$26:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.String)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$2:java.lang.String(org.apache.spark.deploy.worker.DriverRunner)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$3:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,scala.collection.immutable.Set)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$4$adapted:java.lang.Object(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,scala.collection.immutable.Set,java.io.File)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$4:boolean(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,scala.collection.immutable.Set,java.io.File)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5$adapted:java.lang.Object(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$5:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$6:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.io.File)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$7$adapted:java.lang.Object(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.Throwable)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$7:void(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.Throwable)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$8:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.Worker$$anonfun$receive$1,java.lang.Throwable)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$anonfun$applyOrElse$9:java.lang.String()",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.<init>:void(org.apache.spark.deploy.worker.Worker)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.isDefinedAt:boolean(java.lang.Object)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1.$anonfun$applyOrElse$27:scala.Tuple2(scala.Tuple2)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1.<init>:void(org.apache.spark.deploy.worker.Worker,org.apache.spark.rpc.RpcCallContext)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext) ==> org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1.isDefinedAt:boolean(java.lang.Object)",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1.$anonfun$applyOrElse$1:org.apache.spark.internal.MessageWithContext(org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1,java.lang.Object)",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1.$deserializeLambda$:java.lang.Object(java.lang.invoke.SerializedLambda)",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1.<init>:void(org.apache.spark.deploy.worker.WorkerWatcher)",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
      "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction() ==> org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1.isDefinedAt:boolean(java.lang.Object)"
    ],
    "distinct_connections": 145,
    "matched_types": 96,
    "needed_by_an_emitted_path": true,
    "rule": "from an entry point to the body declared by the synthetic partial-function type it allocates, whose full name is the enclosing type followed by `$$anonfun$`, the entry point's own name and a number \u2014 the message parameter lives on that body and not on the named entry point, so without this the source set would be empty and the query would return nothing for a reason that has nothing to do with the code under analysis",
    "succeeded": true
  },
  "thread_boundary": {
    "applied_by_this_query": false,
    "applied_note": "not applied: a flow follows data dependence, and an allocation whose body the runtime invokes is not one, so this formulation does not continue past the boundary and anchors instead at the sinks above it, which the class names as alternatives to the deepest one. The boundary is resolved and recorded here so the limit is evidence rather than an assertion",
    "boundary_resolved": true,
    "connections": [
      "org.apache.spark.deploy.worker.DriverRunner.start:void() ==> org.apache.spark.deploy.worker.DriverRunner$$anon$2.run:void()",
      "org.apache.spark.deploy.worker.ExecutorRunner.start:void() ==> org.apache.spark.deploy.worker.ExecutorRunner$$anon$1.run:void()"
    ],
    "distinct_connections": 2,
    "matched_types": 3,
    "needed_by_an_emitted_path": false,
    "rule": "from a method owning a resolved sink to the deferred body of an anonymous type it allocates, whose full name is the enclosing type followed by `$$anon$` and a number and whose body the runtime invokes",
    "succeeded": false
  }
}
```

* **`cpg_method_count`** — 445568
* **`cpg_project_name`** — spark.cpg
* **`cpg_source`** — harness/cpg/spark.cpg
* **`dataflow_layer`**:

```json
{
  "command": "run.ossdataflow",
  "configured_max_call_depth": 12,
  "engaged": true,
  "engine_default_max_call_depth": 4,
  "outcome": "the layer ran and the overlay set read from the graph is unchanged, so the persisted graph already carried it and nothing was added",
  "overlays_after_engaging": [
    "base",
    "callgraph",
    "controlflow",
    "dataflowOss",
    "typerel"
  ],
  "overlays_before_engaging": [
    "base",
    "callgraph",
    "controlflow",
    "dataflowOss",
    "typerel"
  ],
  "reachability_step": "sink.reachableByFlows(source)",
  "time_limit_imposed_by_this_script": false
}
```

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
  "trait_forwarder_note": "a trait's static forwarder remains a traversal bridge \u2014 a flow may run through one, and the traversal block reports when one did \u2014 it is only barred from being a place a flow STARTS",
  "why_it_matters_to_this_formulation": "the third test looks for the very partial-function class this query's sources live inside, so an entry point that fails it has no source to select in any case"
}
```

* **`load_mode`** — opened_existing_project
* **`sink_anchors`**:

```json
[
  {
    "argument_selection": [
      {
        "argument_selection_rule": "command_or_jar_bearing_formal_parameter",
        "call_sites": 2,
        "selected_argument_indices": [
          "1"
        ],
        "selected_parameters": [
          "1:desc:org.apache.spark.deploy.DriverDescription"
        ],
        "sink_method": "org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)",
        "sink_node_count": 2
      }
    ],
    "flows_at_configured_bound": 4,
    "flows_at_engine_default_bound": 4,
    "flows_not_attributable": 0,
    "flows_whose_elements_carry_a_predicate": 0,
    "kind": "user_named",
    "label": "createDriver",
    "resolved": [
      "org.apache.spark.deploy.master.Master.org$apache$spark$deploy$master$Master$$createDriver:org.apache.spark.deploy.master.DriverInfo(org.apache.spark.deploy.DriverDescription)"
    ],
    "resolved_count": 1,
    "returns_contributed": 4,
    "selector": "method name matches (.*\\$\\$)?createDriver (which admits the Scala-mangled form of a private method) and the enclosing type full name matches org\\.apache\\.spark\\.deploy\\..*",
    "sink_node_count": 2,
    "sink_nodes_when_no_flow_returned": []
  },
  {
    "argument_selection": [
      {
        "argument_selection_rule": "command_or_jar_bearing_formal_parameter",
        "call_sites": 1,
        "selected_argument_indices": [
          "5"
        ],
        "selected_parameters": [
          "5:driverDesc:org.apache.spark.deploy.DriverDescription"
        ],
        "sink_method": "org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)",
        "sink_node_count": 1
      }
    ],
    "flows_at_configured_bound": 0,
    "flows_at_engine_default_bound": 0,
    "flows_not_attributable": 0,
    "flows_whose_elements_carry_a_predicate": 0,
    "kind": "user_named",
    "label": "DriverRunner",
    "resolved": [
      "org.apache.spark.deploy.worker.DriverRunner.<init>:void(org.apache.spark.SparkConf,java.lang.String,java.io.File,java.io.File,org.apache.spark.deploy.DriverDescription,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,org.apache.spark.SecurityManager,scala.collection.immutable.Map)"
    ],
    "resolved_count": 1,
    "returns_contributed": 0,
    "selector": "method full name matches org\\.apache\\.spark\\.deploy\\.worker\\.DriverRunner\\.<init>.* (the construction of a DriverRunner)",
    "sink_node_count": 1,
    "sink_nodes_when_no_flow_returned": [
      {
        "arrives_through": [
          "$stack298 = driverDesc.copy(x$11, x$12, x$13, x$14, x$10, x$15)"
        ],
        "in_method": "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
        "sink_node_code": "$stack298"
      }
    ]
  },
  {
    "argument_selection": [
      {
        "argument_selection_rule": "receiver_of_the_launch",
        "call_sites": 19,
        "selected_argument_indices": [
          "0"
        ],
        "selected_parameters": [
          "0:p0:ANY"
        ],
        "sink_method": "java.lang.ProcessBuilder.start:java.lang.Process()",
        "sink_node_count": 19
      }
    ],
    "flows_at_configured_bound": 0,
    "flows_at_engine_default_bound": 0,
    "flows_not_attributable": 0,
    "flows_whose_elements_carry_a_predicate": 0,
    "kind": "user_named",
    "label": "process_launch",
    "resolved": [
      "java.lang.ProcessBuilder.start:java.lang.Process()"
    ],
    "resolved_count": 1,
    "returns_contributed": 0,
    "selector": "method full name matches (java\\.lang\\.ProcessBuilder\\.start|java\\.lang\\.Runtime\\.exec).*",
    "sink_node_count": 19,
    "sink_nodes_when_no_flow_returned": [
      {
        "arrives_through": [
          "$stack1 = this.processBuilder$1"
        ],
        "in_method": "org.apache.spark.deploy.worker.ProcessBuilderLike$$anon$3.start:java.lang.Process()",
        "sink_node_code": "$stack1"
      },
      {
        "arrives_through": [
          "$stack23 = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.sql.connect.SparkSession.close:void()",
        "sink_node_code": "$stack23"
      },
      {
        "arrives_through": [
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.api.r.RUtils$.isRInstalled:boolean()",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.deploy.PythonRunner$.main:void(java.lang.String[])",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.deploy.RPackageUtils$.rPackageBuilder:boolean(java.io.File,java.io.PrintStream,boolean,java.lang.String)",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.deploy.RRunner$.main:void(java.lang.String[])",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = $stack66.buildProcessBuilder(subsCommand, $stack64, exitCode, message, $stack42, arguments, x$4)"
        ],
        "in_method": "org.apache.spark.deploy.worker.ExecutorRunner.org$apache$spark$deploy$worker$ExecutorRunner$$fetchAndRunExecutor:void()",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.network.util.JavaUtils.deleteRecursivelyUsingUnixNative:void(java.io.File)",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.sql.connect.SparkSession$$anon$2.run:void()",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = builder.directory($stack18)",
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.sql.execution.BaseScriptTransformationExec.initProc:scala.Tuple4()",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = builder.directory(workingDir)",
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.util.Utils$.executeCommand:java.lang.Process(scala.collection.immutable.Seq,java.io.File,scala.collection.Map,boolean)",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "builder = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.util.Utils$.getHeapHistogram:java.lang.String[]()",
        "sink_node_code": "builder"
      },
      {
        "arrives_through": [
          "pb = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.api.python.PythonWorkerFactory.createSimpleWorker:scala.Tuple2(boolean)",
        "sink_node_code": "pb"
      },
      {
        "arrives_through": [
          "pb = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.api.python.PythonWorkerFactory.startDaemon:void()",
        "sink_node_code": "pb"
      },
      {
        "arrives_through": [
          "pb = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.api.r.BaseRRunner$.createRProcess:org.apache.spark.api.r.BufferedStreamThread(int,java.lang.String)",
        "sink_node_code": "pb"
      },
      {
        "arrives_through": [
          "pb = this.createBuilder()"
        ],
        "in_method": "org.apache.spark.launcher.SparkLauncher.launch:java.lang.Process()",
        "sink_node_code": "pb"
      },
      {
        "arrives_through": [
          "pb = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.rdd.PipedRDD.compute:scala.collection.Iterator(org.apache.spark.Partition,org.apache.spark.TaskContext)",
        "sink_node_code": "pb"
      },
      {
        "arrives_through": [
          "pb = new java.lang.ProcessBuilder"
        ],
        "in_method": "org.apache.spark.sql.connect.SparkSession$.withLocalConnectServer:java.lang.Object(scala.Function0)",
        "sink_node_code": "pb"
      },
      {
        "arrives_through": [
          "pb#7 = this.createBuilder()"
        ],
        "in_method": "org.apache.spark.launcher.SparkLauncher.startApplication:org.apache.spark.launcher.SparkAppHandle(org.apache.spark.launcher.SparkAppHandle$Listener[])",
        "sink_node_code": "pb#7"
      }
    ]
  },
  {
    "argument_selection": [
      {
        "argument_selection_rule": "command_or_jar_bearing_formal_parameter",
        "call_sites": 1,
        "selected_argument_indices": [
          "3"
        ],
        "selected_parameters": [
          "3:appDesc:org.apache.spark.deploy.ApplicationDescription"
        ],
        "sink_method": "org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)",
        "sink_node_count": 1
      }
    ],
    "flows_at_configured_bound": 0,
    "flows_at_engine_default_bound": 0,
    "flows_not_attributable": 0,
    "flows_whose_elements_carry_a_predicate": 0,
    "kind": "additional",
    "label": "ExecutorRunner",
    "resolved": [
      "org.apache.spark.deploy.worker.ExecutorRunner.<init>:void(java.lang.String,int,org.apache.spark.deploy.ApplicationDescription,int,int,org.apache.spark.rpc.RpcEndpointRef,java.lang.String,java.lang.String,java.lang.String,int,java.lang.String,java.io.File,java.io.File,java.lang.String,org.apache.spark.SparkConf,scala.collection.immutable.Seq,scala.Enumeration$Value,int,scala.collection.immutable.Map)"
    ],
    "resolved_count": 1,
    "returns_contributed": 0,
    "selector": "method full name matches org\\.apache\\.spark\\.deploy\\.worker\\.ExecutorRunner\\.<init>.* (the construction of an ExecutorRunner) \u2014 carried in addition to the three above and never in place of one",
    "sink_node_count": 1,
    "sink_nodes_when_no_flow_returned": [
      {
        "arrives_through": [
          "$stack247 = appDesc.copy(x$2, x$3, x$10, x$4, x$5, x$6, x$7, x$8, x$9)"
        ],
        "in_method": "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1.applyOrElse:java.lang.Object(java.lang.Object,scala.Function1)",
        "sink_node_code": "$stack247"
      }
    ]
  }
]
```

* **`source_nodes`**:

```json
{
  "binding": "sources are bound to the pinned driver-submission message type and the pinned driver-description type by type evidence inside each entry point's own body scope; the erased `java.lang.Object` parameter of a synthetic partial-function body is NEVER a source, because every partial function compiled over a bytecode frontend has one whatever messages it handles, so admitting it would make every entry point a driver-submission source",
  "body_scope_rule": "the entry point itself, plus every method of every type whose full name begins with the entry point's own enclosing type, `$$anonfun$` and the entry point's own name \u2014 which reaches a nested case body too, because its type name extends that same prefix",
  "description_member_source_nodes_held": 2,
  "handlers_with_no_qualifying_source": [
    "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
    "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
    "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()"
  ],
  "handlers_with_no_qualifying_source_note": "an entry point named here carries no value of the pinned message type and no call to a derived description accessor anywhere in its body scope, so this formulation has no source for it. It is named rather than given the erased partial-function parameter as a stand-in",
  "message_accessor_selector": "enclosing type matches org\\.apache\\.spark\\.deploy\\.DeployMessages\\$.* and return type is org.apache.spark.deploy.DriverDescription",
  "per_handler": [
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.ClientEndpoint.receive:scala.PartialFunction()",
      "resolved_source_nodes": 0,
      "rule_a_internal_typed_values": 0,
      "rule_a_submission_typed_values": 0,
      "rule_b_internal_accessor_calls": 0,
      "rule_b_submission_accessor_calls": 0,
      "rule_c_attributed_to": "not_applicable_no_description_member_call",
      "rule_c_description_member_calls": 0,
      "scope_methods": 7,
      "scope_prefix": "org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$",
      "scope_types": [
        "org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1",
        "org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_1__15598",
        "org.apache.spark.deploy.ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_2__15599"
      ],
      "source_classes_present": []
    },
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receive:scala.PartialFunction()",
      "resolved_source_nodes": 0,
      "rule_a_internal_typed_values": 0,
      "rule_a_submission_typed_values": 0,
      "rule_b_internal_accessor_calls": 0,
      "rule_b_submission_accessor_calls": 0,
      "rule_c_attributed_to": "not_applicable_no_description_member_call",
      "rule_c_description_member_calls": 0,
      "scope_methods": 13,
      "scope_prefix": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$",
      "scope_types": [
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_1__14084",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_2__14079",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_3__14080",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_4__14081",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_5__14082",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_6__14085",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_7__14083",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receive$1$_anonfun_applyOrElse_8__14086"
      ],
      "source_classes_present": []
    },
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "resolved_source_nodes": 0,
      "rule_a_internal_typed_values": 0,
      "rule_a_submission_typed_values": 0,
      "rule_b_internal_accessor_calls": 0,
      "rule_b_submission_accessor_calls": 0,
      "rule_c_attributed_to": "not_applicable_no_description_member_call",
      "rule_c_description_member_calls": 0,
      "scope_methods": 7,
      "scope_prefix": "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$",
      "scope_types": [
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_10__14102",
        "org.apache.spark.deploy.client.StandaloneAppClient$ClientEndpoint$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_9__14101"
      ],
      "source_classes_present": []
    },
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.master.Master.receive:scala.PartialFunction()",
      "resolved_source_nodes": 0,
      "rule_a_internal_typed_values": 0,
      "rule_a_submission_typed_values": 0,
      "rule_b_internal_accessor_calls": 0,
      "rule_b_submission_accessor_calls": 0,
      "rule_c_attributed_to": "not_applicable_no_description_member_call",
      "rule_c_description_member_calls": 0,
      "scope_methods": 49,
      "scope_prefix": "org.apache.spark.deploy.master.Master$$anonfun$receive$",
      "scope_types": [
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$$anon$1$_anonfun_run_1__16538",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_10__16169",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_11__16167",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_12__16170",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_13__16171",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_14_adapted__16172",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_15_adapted__16173",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_16_adapted__16174",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_17_adapted__16194",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_18_adapted__16195",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_19__16175",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_1__16157",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_20_adapted__16176",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_21_adapted__16196",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_22_adapted__16178",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_23_adapted__16197",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_24__16182",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_25__16179",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_26_adapted__16181",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_2__16158",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_3_adapted__16159",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_4_adapted__16161",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_5_adapted__16188",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_6__16162",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_7__16164",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_8__16166",
        "org.apache.spark.deploy.master.Master$$anonfun$receive$1$_anonfun_applyOrElse_9__16168"
      ],
      "source_classes_present": []
    },
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.master.Master.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "resolved_source_nodes": 6,
      "rule_a_internal_typed_values": 0,
      "rule_a_submission_typed_values": 4,
      "rule_b_internal_accessor_calls": 0,
      "rule_b_submission_accessor_calls": 1,
      "rule_c_attributed_to": "driver_submission_message_from_a_submitter",
      "rule_c_description_member_calls": 1,
      "scope_methods": 31,
      "scope_prefix": "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$",
      "scope_types": [
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_27__16302",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_28__16303",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_29_adapted__16304",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_30_adapted__16305",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_31__16306",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_32__16310",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_33__16307",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_34_adapted__16308",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_35_adapted__16327",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_36__16329",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_37__16309",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_38_adapted__16311",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_39__16312",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_40__16313",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_41__16314",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_42__16317",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_43__16316",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_44_adapted__16318",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_45__16319",
        "org.apache.spark.deploy.master.Master$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_46__16315"
      ],
      "source_classes_present": [
        "driver_submission_message_from_a_submitter"
      ]
    },
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.worker.Worker.receive:scala.PartialFunction()",
      "resolved_source_nodes": 8,
      "rule_a_internal_typed_values": 6,
      "rule_a_submission_typed_values": 0,
      "rule_b_internal_accessor_calls": 1,
      "rule_b_submission_accessor_calls": 0,
      "rule_c_attributed_to": "internal_endpoint_to_endpoint_driver_handoff",
      "rule_c_description_member_calls": 1,
      "scope_methods": 34,
      "scope_prefix": "org.apache.spark.deploy.worker.Worker$$anonfun$receive$",
      "scope_types": [
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_10__15023",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_11__15024",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_12__15025",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_13__15026",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_14__15027",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_15__15028",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_16__15029",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_17__15030",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_18__15060",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_19__15059",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_1__15019",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_20__15016",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_21__15031",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_22__15033",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_23__15035",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_24__15032",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_25__15034",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_26__15036",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_2__15020",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_3__15021",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_4_adapted__15056",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_5_adapted__15057",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_6__15054",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_7_adapted__15022",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_8__15058",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receive$1$_anonfun_applyOrElse_9__15018"
      ],
      "source_classes_present": [
        "internal_endpoint_to_endpoint_driver_handoff"
      ]
    },
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.worker.Worker.receiveAndReply:scala.PartialFunction(org.apache.spark.rpc.RpcCallContext)",
      "resolved_source_nodes": 0,
      "rule_a_internal_typed_values": 0,
      "rule_a_submission_typed_values": 0,
      "rule_b_internal_accessor_calls": 0,
      "rule_b_submission_accessor_calls": 0,
      "rule_c_attributed_to": "not_applicable_no_description_member_call",
      "rule_c_description_member_calls": 0,
      "scope_methods": 6,
      "scope_prefix": "org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$",
      "scope_types": [
        "org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1",
        "org.apache.spark.deploy.worker.Worker$$anonfun$receiveAndReply$1$_anonfun_applyOrElse_27__14998"
      ],
      "source_classes_present": []
    },
    {
      "erased_object_parameter_admitted": false,
      "handler": "org.apache.spark.deploy.worker.WorkerWatcher.receive:scala.PartialFunction()",
      "resolved_source_nodes": 0,
      "rule_a_internal_typed_values": 0,
      "rule_a_submission_typed_values": 0,
      "rule_b_internal_accessor_calls": 0,
      "rule_b_submission_accessor_calls": 0,
      "rule_c_attributed_to": "not_applicable_no_description_member_call",
      "rule_c_description_member_calls": 0,
      "scope_methods": 6,
      "scope_prefix": "org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$",
      "scope_types": [
        "org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1",
        "org.apache.spark.deploy.worker.WorkerWatcher$$anonfun$receive$1$_anonfun_applyOrElse_1__15164"
      ],
      "source_classes_present": []
    }
  ],
  "pinned_driver_description_type": "org.apache.spark.deploy.DriverDescription",
  "pinned_submission_message_type": "org.apache.spark.deploy.DeployMessages$RequestSubmitDriver",
  "resolved_description_members": [
    "org.apache.spark.deploy.DriverDescription.command:org.apache.spark.deploy.Command()",
    "org.apache.spark.deploy.DriverDescription.jarUrl:java.lang.String()"
  ],
  "rule_a_message_typed_value": "an operator call in that scope whose static type is a qualifying message type \u2014 the cast a pattern match compiles to \u2014 or an identifier carrying that type",
  "rule_b_description_accessor_call": "a call in that scope to a method on a deploy-message type whose return type is `org.apache.spark.deploy.DriverDescription`, excluding a default-argument supplier by its `copy$default$` name prefix",
  "rule_c_description_member_call": "a call in that scope to a member of `org.apache.spark.deploy.DriverDescription` whose return type matches org\\.apache\\.spark\\.deploy\\.Command or whose name matches (?i)(command|jar|jarurl|mainclass|arguments) \u2014 the command- and jar-bearing values, admitted only where exactly one source class has evidence in the same scope",
  "scopes_where_a_description_member_read_was_not_attributable": [],
  "source_class_internal": {
    "label": "internal_endpoint_to_endpoint_driver_handoff",
    "meaning": "a value carried by a deploy message OTHER than the submission message that also carries a driver description \u2014 the internal hand-off from one of these endpoints to another. Reported and counted separately and never folded into the submission set, because a value that arrived on an internal message did not arrive from a submitter",
    "message_types": [
      "org.apache.spark.deploy.DeployMessages$LaunchDriver"
    ],
    "resolved_accessors": [
      "org.apache.spark.deploy.DeployMessages$LaunchDriver.driverDesc:org.apache.spark.deploy.DriverDescription()"
    ],
    "source_nodes": 8
  },
  "source_class_submission": {
    "label": "driver_submission_message_from_a_submitter",
    "meaning": "a value carried by the pinned driver-submission message \u2014 the message a submitter sends, which is the class the probe was asked about",
    "message_types": [
      "org.apache.spark.deploy.DeployMessages$RequestSubmitDriver"
    ],
    "resolved_accessors": [
      "org.apache.spark.deploy.DeployMessages$RequestSubmitDriver.driverDescription:org.apache.spark.deploy.DriverDescription()"
    ],
    "source_nodes": 6
  },
  "total_resolved_source_nodes": 14
}
```

* **`traversal`**:

```json
{
  "bound_changed_outcome_versus_engine_default": false,
  "bound_reached": "the data-flow engine exposes no signal for having truncated at its call-depth bound, so reaching the bound cannot be read off a single query. What is measured instead is the bound's effect: every anchor was answered a second time at the engine default depth, and the two counts are recorded per anchor and summed below. Nothing was truncated silently \u2014 where an anchor returned no flow, its resolved sink nodes and the expression each arrives through are recorded",
  "deepest_emitted_flow_distinct_methods": 1,
  "direction": "data flow, asked backward from each sink anchor's command- or jar-bearing nodes to the resolved driver-submission message sources, one query per sink anchor over the whole source set",
  "engine_default_max_call_depth": 4,
  "entry_points_resolved": 8,
  "entry_points_with_a_source": 2,
  "flows_at_configured_bound": 4,
  "flows_at_engine_default_bound": 4,
  "flows_emitted_by_source_class": {
    "driver_submission_message_from_a_submitter": 4,
    "internal_endpoint_to_endpoint_driver_handoff": 0
  },
  "flows_not_attributable": 0,
  "flows_whose_elements_carry_a_predicate": 0,
  "max_call_depth": 12,
  "no_flow_filter": "no flow is discarded for carrying a predicate. `flows_whose_elements_carry_a_predicate` above is a measure over the flow's own elements \u2014 an element that is a call to a derived predicate, or an element sitting inside one \u2014 and every one of those flows is emitted as a return like any other. Filtering them would remove from the result set exactly the returns the mechanical spurious test exists to classify, and would make a spurious count of zero a property of this query rather than a measurement",
  "path_composition": "the named entry point first, then the enclosing methods of the flow's elements in flow order \u2014 which begins in the synthetic partial-function body \u2014 then the sink method last; an occurrence of either end in the middle is dropped so the ordering holds exactly",
  "predicate_check_reach": "the emitted path nodes, plus one outgoing call step from each of them \u2014 wider than the element-level measure, and over a path that carries the entry point and the sink method which no flow element covered",
  "reachability_step": "sink.reachableByFlows(source)",
  "return_selection": "one return per distinct (entry point, sink, path, predicates) tuple, sorted, so a flow that differs from another only below the method level is emitted once",
  "returns_emitted": 1,
  "returns_removed_by_deduplication": 3,
  "sink_nodes_queried_distinct": 23,
  "source_nodes_queried": 14,
  "spurious_determination": "not made here. `predicates_on_path` is reported per return and the mechanical on-path test is applied downstream: a return is spurious when an authentication or ACL predicate lies on the path from the entry point to the sink, and for no other reason"
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
| 1 | `2026-08-22T06:08:18Z` | `831b37459372921dabcaca89d19d4435a85814030e12986fd0ed2d6e41416b8e` | True | True | 1 | 0 |
| 2 | `2026-08-22T06:14:23Z` | `831b37459372921dabcaca89d19d4435a85814030e12986fd0ed2d6e41416b8e` | True | True | 1 | 0 |
| 3 | `2026-08-22T06:19:27Z` | `831b37459372921dabcaca89d19d4435a85814030e12986fd0ed2d6e41416b8e` | True | True | 1 | 0 |
| 4 | `2026-08-22T06:29:23Z` | `831b37459372921dabcaca89d19d4435a85814030e12986fd0ed2d6e41416b8e` | True | True | 1 | 0 |
| 5 | `2026-08-22T06:49:00Z` | `831b37459372921dabcaca89d19d4435a85814030e12986fd0ed2d6e41416b8e` | True | True | 1 | 0 |
| 6 | `2026-08-22T10:08:45Z` | `3adf06420af2203240768263fb84923efa1ef6f5acb5c54f241daf1ddd5e5b62` | True | True | 1 | 0 |
| 7 | `2026-08-22T10:28:10Z` | `045b5df31ff41bb03abe92020421e3a432dd711a96880bf0d3f48e3d50363edd` | True | True | 1 | 0 |

Distinct source texts executed by the driver: **3**. Executions recorded: **7**.

## 7. What this file does not claim

It does not claim that anything this query returned is a vulnerability, a real bug or a
false positive, and it compares nothing against any other tool. The spurious count is a
property of the query — whether it matched what it was asked to match — reached by the
mechanical on-path test above and by nothing else. The graph was **read**, with
`importCpg`, and not built.
