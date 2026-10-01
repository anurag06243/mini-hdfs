# Mini HDFS

A from-scratch implementation of an HDFS-like distributed filesystem in Java, built as a deep-learning vehicle for distributed systems design. This is **not** a Hadoop clone — it is a ground-up implementation of the core concepts that makes HDFS interesting: metadata/data separation, block replication, failure detection, self-healing, write pipelines, and durable recovery.

> **Goal:** Be able to answer any HLD/LLD, architecture, scaling, or trade-off question about this system in a FAANG-level interview — because I designed and wrote it.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         HDFS Client                              │
│  put(data, path)  get(path)  mkdir  ls  rename  delete           │
└────────────────────────┬────────────────────────────────────────┘
                         │ Control plane (method calls → gRPC in prod)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                        NameNode                                   │
│                                                                   │
│  Namespace tree           BlockMap              Persistence       │
│  INode (abstract)         block → [DN, DN, DN]  EditLog (WAL)    │
│  ├─ DirectoryNode         DN → [block, block]   FsImage snapshot │
│  └─ FileNode                                                      │
│       └─ [blockId, ...]   DatanodeInfo          pendingCommands  │
│                           NORMAL/STALE/DEAD     (piggybacked)    │
└────────────────────────┬────────────────────────────────────────┘
                         │ Data plane: raw TCP
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   ┌────────────┐  ┌────────────┐  ┌────────────┐
   │ DataNode 1 │  │ DataNode 2 │  │ DataNode 3 │
   │ TCP server │  │ TCP server │  │ TCP server │
   │ blocks:    │  │ blocks:    │  │ blocks:    │
   │ {1,4,7,…} │  │ {1,2,5,…} │  │ {1,3,6,…} │
   └────────────┘  └────────────┘  └────────────┘

Write pipeline:  Client ──► DN-1 ──► DN-2 ──► DN-3
                               ◄─────────────────  ACK
```

**Control plane** (metadata): client ↔ NameNode via `INameNodeService`; DataNode ↔ NameNode via `IDatanodeProtocol`. In this implementation both are direct method calls (seams designed for gRPC in a real deployment).

**Data plane** (block bytes): raw TCP with a simple binary protocol. Writes use a chained pipeline; reads stream directly from the closest DataNode with automatic fallback.

---

## What Each Phase Demonstrates

| Phase | Feature | Core concept |
|-------|---------|--------------|
| 0 | Requirements + capacity math | Problem framing, why 128 MB blocks |
| 1 | HLD — component decomposition | Metadata/data separation, control vs data plane |
| 2 | LLD — classes, contracts, state machines | Composite pattern (`INode`), `BlockMap`, `DatanodeInfo` |
| 3 | Walking skeleton — write + read in memory | End-to-end path before depth, interface-as-seam |
| 4+5 | Heartbeats, failure detection, re-replication | Pull model, state machine (NORMAL→STALE→DEAD), command piggybacking |
| 6 | TCP write pipeline + read path | Chained pipeline, binary protocol, fallback reads |
| 7 | Edit log + fsimage + restart recovery | WAL, checkpoint, ID generator safety, lease recovery |

---

## Key Design Decisions

### Composite pattern for the namespace
`INode` (abstract) → `FileNode` (leaf) or `DirectoryNode` (composite). Rename is O(1) — only a namespace pointer moves; block IDs and the BlockMap are untouched.

### Interface-as-seam
`INameNodeService` and `IDataNodeStorage` decouple the client from the transport. Phase 3–5 use in-memory implementations; Phase 6 swaps in TCP without changing `HdfsClient`.

### Pull model for commands
The NameNode never opens a connection to a DataNode. Commands (`REPLICATE`, `DELETE`) are queued in `pendingCommands` and piggybacked on heartbeat replies. This keeps the NameNode stateless with respect to outbound connections.

### Full block reports (not deltas)
Full block reports are idempotent — the NameNode always converges to the correct view regardless of restart history. Delta reports would require both sides to perfectly track "what the other already knows," which is fragile after any restart or partial outage.

### Write-ahead logging
Every mutating namespace operation writes a log entry to disk **before** updating in-memory state. On restart: load the last fsimage snapshot, replay only the edits written since the snapshot. The BlockMap is not persisted — it is rebuilt from DataNode block reports (the same self-healing mechanism used for failure recovery).

---

## Module Layout

```
mini-hdfs/
├── hdfs-common/          DTOs, model classes (INode, BlockMap, DatanodeInfo, …)
├── hdfs-namenode/        NameNodeImpl, INameNodeService, IDatanodeProtocol,
│                         EditLog, FsImage
├── hdfs-datanode/        InMemoryDataNode, TcpDataNodeServer, BlockProtocol
└── hdfs-client/          HdfsClient, TcpBlockWriter, TcpBlockReader
```

---

## Try it: interactive shell

```powershell
./shell.ps1
```

Starts an in-process cluster (1 NameNode + 4 DataNodes on real TCP ports, 64 KB blocks, RF=3) and drops you at an `hdfs>` prompt:

```
hdfs> mkdir /docs
hdfs> put README.md /docs                 # upload a real file from your disk
hdfs> write /hello.txt hi there           # or create one from text
hdfs> mv /hello.txt /docs
hdfs> cd /docs                            # relative paths and .. work everywhere
hdfs> ls
hdfs> stat /docs/README.md                # blocks + which DataNodes hold them
hdfs> kill dn-1                           # crash a DataNode
hdfs> wait                                # watch NORMAL -> STALE -> DEAD -> REPLICATE
hdfs> revive dn-1                         # block report -> DELETE of the extra replica
hdfs> restart-nn                          # NameNode crash; fsimage + edit log replay
hdfs> get /docs/README.md out.md          # download it back
```

`help` lists every command. Each launch starts from an empty cluster (DataNode storage is in memory), and NameNode metadata is written to `shell-data/`.

## Scripted demo

```powershell
./demo.ps1
```

Builds the project and runs [`Demo.java`](hdfs-client/src/main/java/dev/anurag/hdfs/client/Demo.java): one NameNode plus four DataNodes on real TCP ports, walked through six scenes: namespace ops + write pipeline, read path, read fallback after a DataNode is killed, NameNode crash + restart (edit-log replay, BlockMap rebuilt from block reports, checkpoint), and failure detection + re-replication. NameNode metadata lands in `demo-data/` so you can inspect `edits.log` and `fsimage.bin`.

---

## Build & Test

**Requirements:** Java 22+, Maven 3.9+

```bash
# Build and run all 19 tests
mvn clean test
```

```bash
# Build only (no tests)
mvn clean compile
```

Expected output:
```
Tests run: 1   (NamespaceTreeTest)     — INode composite tree
Tests run: 4   (EndToEndTest)          — namespace ops, multi-block, fault tolerance
Tests run: 4   (HeartbeatReplicationTest) — state machine, block report, re-replication
Tests run: 4   (TcpPipelineTest)       — real TCP write pipeline, read fallback
Tests run: 6   (NameNodePersistenceTest) — restart recovery, edit log, fsimage

BUILD SUCCESS — Tests run: 19, Failures: 0, Errors: 0
```

---

## What This Is Not

- **Not a Hadoop/HDFS clone.** No Protobuf, no YARN, no MapReduce, no Zookeeper.
- **Not production storage.** No checksums, no lease recovery, no safe mode, no HA/standby NameNode, no rack-awareness, no short-circuit reads, no erasure coding.
- **Not a performance benchmark.** Blocks are buffered fully in memory; no streaming I/O, no NIO, no off-heap buffers.

All simplifications are deliberate and documented. The goal is to demonstrate understanding of the core mechanisms — not to build a deployable system.

---

## Deferred (Tier 2)

NameNode HA (standby + QJM + ZKFC + fencing) · Federation · Write leases + pipeline recovery · Erasure coding · Snapshots · Quotas · Security (Kerberos + block tokens) · Short-circuit reads · Safe mode

---

## Docs

| Document | Contents |
|----------|----------|
| [`docs/requirement.md`](docs/requirement.md) | Functional + non-functional requirements, capacity math |
| [`docs/uml/hld-architecture.md`](docs/uml/hld-architecture.md) | Component architecture, control/data plane separation |
| [`docs/adr/`](docs/adr/) | Architecture decision records |
| [`docs/learning/`](docs/learning/) | Phase-by-phase deep dives with diagrams |
| [`docs/interview/`](docs/interview/) | 30+ interview questions across all phases |
