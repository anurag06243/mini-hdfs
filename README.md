# Mini HDFS

[![CI](https://github.com/anurag06243/mini-hdfs/actions/workflows/ci.yml/badge.svg)](https://github.com/anurag06243/mini-hdfs/actions/workflows/ci.yml)

A distributed file system in the style of HDFS, written from scratch in Java 21 with **zero
runtime dependencies**. The NameNode, DataNodes, and clients are separate processes that
talk over TCP: a hand-built binary RPC protocol for metadata and a streaming protocol for
block data.

Files are split into blocks and replicated 3× through a DataNode write pipeline. The
cluster detects dead nodes and corrupt replicas and repairs itself. NameNode metadata
survives crashes through a write-ahead log and checkpoints.

Not a Hadoop clone: a ground-up implementation of the mechanisms that make HDFS work, built
to understand them end to end.

---

## Try it (2 minutes)

```powershell
mvn -q compile
bin/start-cluster.ps1          # NameNode + 3 DataNodes, each in its own window
bin/hdfs.ps1 dfs               # interactive client
```
Linux/macOS: `bin/start-cluster.sh` and `bin/hdfs dfs`.

```
hdfs:/> mkdir -p /docs/2026
hdfs:/> put README.md /docs                  # upload a local file
hdfs:/> cd /docs
hdfs:/docs> stat README.md                   # which DataNodes hold each block
hdfs:/docs> report                           # DataNode liveness as the NameNode sees it
```

Then break things:

| Do this | Watch this |
|---|---|
| Close a DataNode window | NameNode log: `STALE` → `DEAD` → `block N under-replicated: dn-2 -> dn-3`; `stat` shows 3 replicas again |
| Restart that DataNode | It re-registers; its now-extra copies get `DELETE`d |
| Close the NameNode, start it again (`bin/hdfs.ps1 namenode --dir data/namenode`) | Safe mode until DataNodes reconnect and report on their own; every file intact |
| Flip a byte in `data/dn-1/current/blk_1` | Reads skip the bad copy; it is quarantined to `corrupt/` and re-replicated |
| `rm` a file | Its blocks disappear from every DataNode's disk |

Stop everything: `bin/stop-cluster.ps1` / `bin/stop-cluster.sh`.

---

## Architecture

```
                      ┌─────────────────────────── NameNode process ───────────────────────────┐
  client process      │  NameNodeServer: thread per connection, one read/write lock            │
 ┌───────────────┐ RPC│  ┌───────────────────────┐  ┌────────────────┐  ┌──────────────────┐  │
 │ hdfs dfs ...  │───▶│  │ namespace tree (INode)│  │ BlockMap       │  │ edit log (fsync) │  │
 │ HdfsClient    │    │  │ file -> [block ids]   │  │ block -> nodes │  │ + fsimage        │  │
 └──────┬────────┘    │  └───────────────────────┘  └────────────────┘  └──────────────────┘  │
        │             │  monitor: liveness (NORMAL/STALE/DEAD), re-replication, checkpoints    │
        │             └───────────────────────────────▲────────────────────────────────────────┘
        │ block bytes (never through the NameNode)    │ heartbeat (commands ride the reply),
        ▼                                             │ full block reports — DataNode -> NameNode only
 ┌────────────┐   pipeline   ┌────────────┐   ┌────────────┐
 │ DataNode 1 │─────────────▶│ DataNode 2 │──▶│ DataNode 3 │   each hop: verify CRC, fsync to disk,
 │ blk_*.meta │◀── ACK ──────│            │◀──│            │   forward, ACK after downstream ACK
 └────────────┘              └────────────┘   └────────────┘
```

| Concern | How it works |
|---|---|
| **Metadata vs data** | The NameNode holds only the namespace and block locations; bytes flow client ↔ DataNode |
| **Control plane** | Length-prefixed binary frames over persistent TCP connections; 14 operations ([ADR-0003](docs/adr/0003-control-plane-rpc.md)) |
| **Write path** | `addBlock` → pipeline client → DN → DN → DN; on failure abandon the block, exclude the failed node, retry |
| **Read path** | Locations from the NameNode (shuffled to spread load); bytes from any replica, checksum-verified, falling back to the next replica |
| **Failure detection** | Heartbeats every second; STALE nodes get no new blocks; DEAD nodes' blocks are re-replicated from survivors |
| **Self-healing** | Commands piggyback on heartbeat replies (the NameNode never connects to DataNodes); full block reports reconcile lost, extra, and orphaned replicas |
| **Durability** | Edit log fsync'd before every mutation is applied; atomic fsimage checkpoints; replay tolerates a torn last entry |
| **Restart** | Safe mode until ≥ 99.9% of blocks are reported; DataNodes reconnect and re-register on their own |
| **Integrity** | CRC32 verified by every pipeline hop, on every disk read, and by the client; a background block scanner catches cold-data rot ([ADR-0004](docs/adr/0004-datanode-storage-and-integrity.md)) |
| **Safety** | A cluster ID handshake stops a DataNode from joining the wrong NameNode (whose block report would otherwise get all its blocks deleted as orphans) |

---

## Modules

```
hdfs-common/     wire protocol (RpcOps, Wire, NameNodeRpcClient), protocol interfaces, DTOs, namespace model
hdfs-namenode/   NameNodeImpl (namespace, BlockMap, liveness, safe mode), NameNodeServer, EditLog, FsImage
hdfs-datanode/   DataNode (heartbeat loop, commands, block scanner), DiskDataNodeStorage, TcpDataNodeServer
hdfs-client/     HdfsClient (pipeline write with recovery, fallback read), HdfsCli
bin/             hdfs / hdfs.ps1, start-cluster, stop-cluster
```

About 2,700 lines of main code and 1,000 lines of tests.

---

## Tests

```bash
mvn clean test
```

42 tests, run in CI on every push:

| Suite | What it proves |
|---|---|
| `ClusterIntegrationTest` | Real NameNode RPC server + 4 on-disk DataNodes over sockets: multi-block round trip; DataNode crash → re-replication; NameNode restart → safe mode → DataNodes rejoin; delete frees disk; corrupt replica detected and repaired; block scanner catches unread corruption; pipeline failure → exclude and retry; foreign-cluster DataNode refused |
| `NameNodeCorrectnessTest` | Block invalidation on delete (incl. directories); orphan and over-replication handling; abandon + exclusion; namespace edge cases; move survives checkpoint; checkpoint mid-write; lease recovery logged; cluster ID |
| `NameNodePersistenceTest` | fsimage + edit log replay, rename/delete across restart, ID generators never regress, safe mode |
| `DiskDataNodeStorageTest` | Blocks survive restart, bit-flip detection + quarantine, half-written block from a crash ignored |
| `HeartbeatReplicationTest`, `TcpPipelineTest`, `EndToEndTest` | State machine, block report reconciliation, pipeline and read fallback |

---

## Deliberate simplifications

| Here | Real HDFS | Why it's fine here |
|---|---|---|
| Single NameNode | Active/standby + JournalNode quorum + ZooKeeper failover | Data survives a NameNode crash; HA is the next tier |
| Whole-block store-and-forward | 64 KB packets, cut-through forwarding | Same protocol shape, simpler buffering |
| Restart the block on pipeline failure | Repair the pipeline in place, generation stamps | Correct, just re-sends bytes |
| One CRC per block | CRC per 512-byte chunk | We always read whole blocks |
| Random placement | Rack-aware placement | Single-rack setup |
| No retry cache, no auth | Retry cache, Kerberos, block tokens | Out of scope |

---

## Design docs

| | |
|---|---|
| [Requirements & capacity math](docs/requirement.md) | Why 128 MB blocks, small-files problem, NameNode memory |
| [HLD](docs/uml/hld-architecture.md) | Components and control/data plane split |
| [ADRs](docs/adr/) | Language, communication, RPC protocol, storage & integrity |
| [Learning logs](docs/learning/) | Phase-by-phase deep dives; [phase 8](docs/learning/phase8-production-hardening.md) covers the move to a real cluster |
| [Interview question bank](docs/interview/) | 70+ questions with key points, by phase |
