# Phase 2 — LLD Learning Log

> Self-study doc. Read linearly, or jump to the section you got wrong.
> Questions are posed first — try to answer before reading. Diagrams at end of each section.

---

## LLD-1 — Namespace model (files & directories as Java classes)

### The question
Model the HDFS namespace — a directory tree like:
```
/
├── logs/
│   ├── day1.txt   (3 blocks)
│   └── day2.txt   (2 blocks)
└── data/
    └── sales.csv  (1 block)
```
as Java classes. What classes/interfaces? What does a file hold vs a directory? How does the tree work?

### Anurag's answer (paraphrased)
1. It is a class — ✅ (but needs refinement: two classes, one shared abstraction)
2. A file cannot hold a directory; a directory can hold files and directories — ✅ **this is the key insight**
3. Tree traversal: `cd logs/day1.txt` — ✅ (path split, walk down)

### The correct design — Composite pattern

The well-known pattern here is **Composite** (GoF). It solves exactly one problem: *treat individual objects (files) and compositions of objects (directories) uniformly through a common interface.* You noticed this yourself: both have a name, both are "things in the tree," but a directory can contain more of either.

```
INode (abstract class or interface)
  ├── FileNode   (leaf — holds block list, cannot have children)
  └── DirectoryNode  (composite — holds children: Map<name → INode>)
```

#### INode — the shared abstraction
```java
public abstract class INode {
    private final long id;          // unique inode number (like Linux inode)
    private String name;            // just the last segment ("day1.txt", not full path)
    private INode parent;           // null only for root
    private long createdAt;
    private long modifiedAt;
    // permissions: owner, group, mode (MVP: skip or stub)

    public abstract boolean isDirectory();
    public abstract long getSize();  // file = bytes; dir = sum of children (or 0 for MVP)
}
```

Why `parent` pointer? So you can reconstruct the full path by walking up:
`day1.txt → logs → /` = `/logs/day1.txt`. No need to store the full path string anywhere.

Why `id` (inode number)? The path is mutable (rename changes it), but the inode id never changes. Block-to-file mapping uses the inode id, not the path — so `rename /a/b → /c/b` costs O(1) and zero block data moves.

#### FileNode — the leaf
```java
public class FileNode extends INode {
    private List<Long> blockIds;      // ordered list of block IDs (sequence matters)
    private long fileSize;            // total bytes (not always = blocks × blockSize, last block partial)
    private short replicationFactor;  // default 3, configurable per-file

    @Override public boolean isDirectory() { return false; }
    @Override public long getSize() { return fileSize; }
}
```

A FileNode **cannot hold children**. If you call `fileNode.addChild(...)` it should throw, not silently do nothing.

#### DirectoryNode — the composite
```java
public class DirectoryNode extends INode {
    private final Map<String, INode> children;  // key = child name (not full path)

    public void addChild(INode child) { children.put(child.getName(), child); }
    public void removeChild(String name) { children.remove(name); }
    public INode getChild(String name) { return children.get(name); }
    public Collection<INode> listChildren() { return children.values(); }

    @Override public boolean isDirectory() { return true; }
    @Override public long getSize() { return 0; } // or sum children recursively
}
```

The key: `getChild()` returns **INode, not FileNode or DirectoryNode**. The caller gets back the abstraction, so traversal code works identically for both.

#### Tree traversal — resolving `/logs/day1.txt`
```java
public INode resolve(String absolutePath) {
    String[] parts = absolutePath.split("/");  // ["", "logs", "day1.txt"]
    INode current = root;
    for (String part : parts) {
        if (part.isEmpty()) continue;  // leading slash produces empty string
        if (!current.isDirectory()) return null;  // can't walk into a file
        current = ((DirectoryNode) current).getChild(part);
        if (current == null) return null;  // path doesn't exist
    }
    return current;
}
```

This is a standard trie/prefix-tree walk. The namespace IS a trie, but with full names (not chars) at each node.

#### Why Composite fits so well
| Composite role | Our class | Why |
|---|---|---|
| Component | INode | Shared interface for tree operations |
| Leaf | FileNode | No children; holds actual data reference |
| Composite | DirectoryNode | Holds children; delegates size/ops recursively |

Interview one-liner: *"The HDFS namespace is a Composite pattern — files are leaves, directories are composites, both implement INode, so ls/rm/rename work the same regardless of whether the target is a file or a directory."*

#### What gets stored where (NameNode RAM)
- One `INode` object per file or directory — ~150 bytes each (that's the small-files problem number)
- The root is a `DirectoryNode` whose parent is null
- Total objects in RAM = total files + total directories (NOT proportional to data volume)

---

## LLD-2 — Block model

### The question
A file is split into fixed-size blocks. Each block has an ID and lives on DataNodes. How do you model a block?

### The correct design

```java
public class Block {
    private final long blockId;        // unique, globally; NameNode assigns it
    private final long generationStamp; // version number; incremented on recovery (Production)
    private long numBytes;             // actual bytes (last block may be < blockSize)
}
```

A `Block` is a **descriptor**, not the bytes. It is stored in the NameNode's memory. The bytes are on DataNode disks.

Why `generationStamp`? If a DataNode crashes mid-write and comes back, how do you know if its copy of block 42 is the correct version or a stale partial write? The generationStamp (incremented by NameNode on recovery) lets you reject stale replicas. **MVP: skip it. Note it as production-level.**

### Where blocks live — BlockMap

```java
public class BlockMap {
    // For each block: which DataNodes hold a replica
    private final Map<Long, Set<DatanodeInfo>> blockToDatanodes;

    // For each DataNode: which blocks it reports holding
    private final Map<String, Set<Long>> datanodeToBlocks;

    public void addReplica(long blockId, DatanodeInfo dn) { ... }
    public void removeReplica(long blockId, String dnId) { ... }
    public Set<DatanodeInfo> getLocations(long blockId) { return blockToDatanodes.get(blockId); }
}
```

Both directions are needed:
- `blockToDatanodes` → used on every client read/write (getBlockLocations)
- `datanodeToBlocks` → used when a DataNode dies (find all its blocks, check under-replication)

#### FileNode ↔ Block relationship
```
FileNode.blockIds: [101, 102, 103]
                         ↓
BlockMap.blockToDatanodes:
  101 → {DN1, DN2, DN3}
  102 → {DN1, DN2, DN3}
  103 → {DN2, DN3, DN4}
```

The FileNode holds `blockIds` (ordered). The BlockMap holds the location mapping. The NameNode joins them on every metadata operation.

---

## LLD-3 — DataNode state model

### The question
What does the NameNode know about each DataNode? What changes when a DN registers, sends a heartbeat, or dies?

### DatanodeInfo — what NameNode tracks per DataNode

```java
public class DatanodeInfo {
    private final String datanodeId;    // hostname:port or UUID
    private final String hostname;
    private final int port;             // data-plane TCP port (block streaming)
    private final int grpcPort;         // control-plane gRPC port (heartbeats)

    // Capacity (from registration + block reports)
    private long totalCapacityBytes;
    private long usedBytes;
    private long remainingBytes;

    // Liveness
    private long lastHeartbeatMs;       // System.currentTimeMillis() when last heartbeat arrived
    private DatanodeState state;        // NORMAL | STALE | DEAD | DECOMMISSIONED

    // Block report
    private Set<Long> reportedBlocks;   // updated on full block report
}

public enum DatanodeState {
    NORMAL,         // heartbeating on time
    STALE,          // missed 2-3 heartbeats (~20s) — avoid for new writes
    DEAD            // missed 10 heartbeats (~90s) — re-replicate its blocks
}
```

### State transitions

```
                   register()
  (not exist) ──────────────────► NORMAL
                                    │
                           heartbeat arrives
                                    │ (reset lastHeartbeat)
                                  NORMAL
                                    │
                           heartbeat missed
                                    ▼
                                  STALE
                                    │
                           still no heartbeat
                                    ▼
                                   DEAD ──► trigger re-replication of its blocks
```

### What DataNode sends at registration
```
DatanodeRegistration {
  datanodeId: "dn-1"
  hostname: "192.168.1.10"
  dataPort: 50010       // raw TCP, for block streaming
  grpcPort: 50020       // gRPC, for control messages
  totalCapacity: 10_000_000_000  // 10 GB
}
```

### What DataNode sends on heartbeat
```
HeartbeatRequest {
  datanodeId: "dn-1"
  usedBytes: 3_000_000_000
  remainingBytes: 7_000_000_000
  xmitsInProgress: 2  // active replication pipelines (load indicator)
}
```

NameNode reply may carry commands:
```
HeartbeatResponse {
  commands: [
    REPLICATE(blockId=101, targetDn="dn-4"),  // under-replicated block
    DELETE(blockId=209)                        // over-replicated or orphan
  ]
}
```

This is the **command-piggybacking** pattern: NameNode never initiates, only replies. Commands ride heartbeat replies.

---

## LLD-4 — gRPC service definitions (.proto)

### NameNode service (control plane)

```protobuf
syntax = "proto3";
package minihdfs;

// ─── Called by Client ───────────────────────────────────────────────

service NameNodeService {
  // Write path
  rpc CreateFile(CreateFileRequest) returns (CreateFileResponse);
  rpc AddBlock(AddBlockRequest) returns (AddBlockResponse);
  rpc CloseFile(CloseFileRequest) returns (CloseFileResponse);

  // Read path
  rpc GetBlockLocations(GetBlockLocationsRequest) returns (GetBlockLocationsResponse);

  // Namespace ops
  rpc MkDir(MkDirRequest) returns (MkDirResponse);
  rpc ListDir(ListDirRequest) returns (ListDirResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Rename(RenameRequest) returns (RenameResponse);
}

// ─── Called by DataNode ──────────────────────────────────────────────

service DatanodeProtocol {
  rpc RegisterDatanode(DatanodeRegistration) returns (RegisterResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc BlockReport(BlockReportRequest) returns (BlockReportResponse);
}
```

### Key message types

```protobuf
message BlockLocation {
  int64 blockId = 1;
  int64 numBytes = 2;
  int64 offset = 3;               // byte offset in the file
  repeated DatanodeDescriptor locations = 4;
}

message DatanodeDescriptor {
  string datanodeId = 1;
  string hostname = 2;
  int32 dataPort = 3;             // used by Client to open raw TCP
}

message GetBlockLocationsRequest { string path = 1; }
message GetBlockLocationsResponse {
  int64 fileSize = 1;
  repeated BlockLocation blocks = 2;
}

message HeartbeatRequest {
  string datanodeId = 1;
  int64 usedBytes = 2;
  int64 remainingBytes = 3;
  int32 xmitsInProgress = 4;
}

message HeartbeatResponse {
  repeated DatanodeCommand commands = 1;
}

message DatanodeCommand {
  enum Action { REPLICATE = 0; DELETE = 1; }
  Action action = 1;
  int64 blockId = 2;
  DatanodeDescriptor targetDn = 3;  // for REPLICATE
}
```

**Why protobuf field numbers?** They are the actual binary keys. `datanodeId = 1` means this field is encoded as key `1` on the wire. Renaming the field is backward-compatible (name is irrelevant at runtime); *changing a field number* is a breaking change (binary format changes).

---

## LLD-5 — Maven module layout

### The problem
The `.proto` files must be compiled to Java stubs. Both NameNode and DataNode use the generated classes. Where do you put the shared proto?

### Wrong design
```
src/main/java/...  (everything in one module)
```
Problem: NameNode and DataNode end up in one JAR. You can't deploy them independently.

### Correct design — Maven multi-module

```
mini-hdfs/
  pom.xml              (parent POM — common deps, plugin versions)
  hdfs-proto/
    pom.xml            (generates Java stubs from .proto files)
    src/main/proto/
      minihdfs.proto
  hdfs-namenode/
    pom.xml            (depends on hdfs-proto)
    src/main/java/...
  hdfs-datanode/
    pom.xml            (depends on hdfs-proto)
    src/main/java/...
  hdfs-client/
    pom.xml            (depends on hdfs-proto)
    src/main/java/...
  hdfs-common/
    pom.xml            (shared utilities: logging, config, constants)
    src/main/java/...
```

### Parent POM declares modules
```xml
<modules>
  <module>hdfs-proto</module>
  <module>hdfs-common</module>
  <module>hdfs-namenode</module>
  <module>hdfs-datanode</module>
  <module>hdfs-client</module>
</modules>
```

### hdfs-proto POM — the protobuf build step
```xml
<plugin>
  <groupId>io.grpc</groupId>
  <artifactId>protoc-gen-grpc-java</artifactId>
  <version>1.65.0</version>
</plugin>
<plugin>
  <groupId>org.xolstice.maven.plugins</groupId>
  <artifactId>protobuf-maven-plugin</artifactId>
  <!-- generates Java sources from .proto into target/generated-sources -->
</plugin>
```

**MVP simplification:** can start with a single module and split later. The split is mechanical (no design thinking) — Claude can scaffold it when asked.

---

## LLD-6 — NameNode internal state summary

What the NameNode holds **in RAM** (and must persist to disk for recovery):

| Data structure | Type | Purpose |
|---|---|---|
| `root` | `DirectoryNode` | Root of namespace tree |
| `inodeMap` | `Map<Long, INode>` | id → INode (fast lookup by inode id) |
| `blockMap` | `BlockMap` | blockId ↔ DatanodeInfo |
| `datanodes` | `Map<String, DatanodeInfo>` | datanodeId → liveness + capacity |
| `blockIdCounter` | `AtomicLong` | Monotonically increasing block IDs |
| `inodeIdCounter` | `AtomicLong` | Monotonically increasing inode IDs |

Everything is in RAM → fast O(1) or O(depth) lookup. Persistence = edit log + fsimage (Phase 7).

---

## Common interview questions for Phase 2

**Q: Why use an abstract class for INode instead of an interface?**
A: Because `INode` has real shared state (id, name, parent, timestamps) that you don't want to duplicate in every subclass. An interface with default methods would work but is unnatural for shared mutable fields. Use abstract class when you have real shared state.

**Q: Why not store the full path in each INode?**
A: Because rename would require updating every descendant's stored path — O(subtree size). With only `name` + `parent`, rename updates exactly one INode's name field — O(1). The full path is computed on demand by walking parent pointers.

**Q: What happens to blockIds when a file is renamed?**
A: Nothing. `FileNode.blockIds` is unchanged. `BlockMap.blockToDatanodes` is unchanged. Only the namespace pointer moves. Rename = O(1) metadata-only.

**Q: Why does DirectoryNode use Map<String, INode> instead of List<INode>?**
A: Map gives O(1) lookup by child name. `ls /logs/day1.txt` splits on `/`, looks up `"logs"` in root's map in O(1), then `"day1.txt"` in logs' map in O(1). With a List you'd scan all children.

**Q: What is the small-files problem? Where does it show up in the LLD?**
A: Each INode object ≈ 150 bytes in NameNode heap. 10 billion 1KB files = 10B INode objects = ~1.5 TB NameNode RAM, but only 10 TB of data. The namespace tree model is the direct cause — one node per file, no compression. Solution: object storage (S3 design) or HBase on HDFS to batch small keys.

**Q: How does the NameNode detect that a DataNode died?**
A: `DatanodeInfo.lastHeartbeatMs` is updated on every heartbeat. A background monitor thread runs every few seconds and checks: `now - lastHeartbeatMs > DEAD_THRESHOLD (e.g. 90s)` → mark DEAD → find all blocks in `datanodeToBlocks`, check each block's replica count in `blockToDatanodes`, queue re-replication for any block below RF.

**Q: Why are block IDs assigned by the NameNode, not the DataNode?**
A: The NameNode is the single source of truth. If DataNodes assigned IDs, two DNs might claim the same ID — collision. Centralized ID assignment via `AtomicLong` is globally unique with no coordination cost.

---

## Diagrams (draw these in Figma/whiteboard)

### Class diagram to draw
```
INode (abstract)
  ├── FileNode
  │     blockIds: List<Long>
  │     fileSize: long
  │     replicationFactor: short
  └── DirectoryNode
        children: Map<String, INode>
        + addChild(INode)
        + getChild(String): INode
        + listChildren(): Collection<INode>

NameNode
  root: DirectoryNode
  inodeMap: Map<Long, INode>
  blockMap: BlockMap
  datanodes: Map<String, DatanodeInfo>

BlockMap
  blockToDatanodes: Map<Long, Set<DatanodeInfo>>
  datanodeToBlocks: Map<String, Set<Long>>
```

### Sequence to draw: mkdir /logs/day1.txt (create file)
```
Client → NameNode.createFile("/logs/day1.txt", rf=3)
NameNode: resolve("/logs") → DirectoryNode logs
NameNode: new FileNode("day1.txt"), attach to logs
NameNode: → return (no blocks yet, client hasn't sent data)
Client → NameNode.addBlock("/logs/day1.txt")
NameNode: pick 3 DataNodes (by remaining capacity, rack awareness later)
NameNode: allocate new blockId=101
NameNode → Client: [DN1, DN2, DN3] for block 101
Client → DN1,DN2,DN3: stream bytes (raw TCP, Phase 6)
Client → NameNode.closeFile("/logs/day1.txt")
NameNode: set FileNode.fileSize, mark file visible (write-once: visible only at close)
```

---

## Phase 2 gaps to watch

- **Composite pattern name** — always use it in an interview, don't just describe it
- **Inode id vs path** — rename is O(1) because block mappings use id, not path
- **Map<String, INode> not List** — state the reason: O(1) child lookup
- **BlockMap is bidirectional** — one direction per use case; explain both
- **Command piggybacking** — NameNode never initiates; commands ride heartbeat replies
- **Protobuf field numbers** — binary keys, changing them = breaking change

