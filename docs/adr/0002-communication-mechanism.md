# ADR-0002: Communication Mechanism (Control Plane vs Data Plane)

- **Status:** Accepted
- **Date:** 2026-09-02
- **Deciders:** Anurag Kumar (reasoned in Phase 1 HLD discussion)
- **Supersedes / relates to:** builds on the HLD component split
  ([hld-architecture.md](../uml/hld-architecture.md)).

## Context

The three components — Client, NameNode, DataNode — run on different machines and
must talk over the network. There are **two fundamentally different kinds of
traffic**, and they have different needs:

- **Control plane** (Client↔NameNode, DataNode↔NameNode): many small, **structured**
  request/response messages — create file, getBlockLocations, heartbeat, block
  report. Small per call but **high frequency** (every DataNode heartbeats every
  few seconds; every metadata op flows here). Needs a clear message contract and
  low per-call overhead.
- **Data plane** (Client↔DataNode): **bulk, unstructured** byte streams — possibly
  gigabytes. Throughput is everything; there is no structure to encode.

A single mechanism is unlikely to be optimal for both.

## Options considered

Per plane, the candidates were REST/HTTP+JSON, gRPC (HTTP/2 + Protocol Buffers),
raw TCP sockets with a custom protocol, and Java RMI.

### Control plane
- **gRPC** — binary Protocol Buffers, strongly-typed service contracts, built-in
  streaming, efficient for high-frequency small structured calls. Cost: protobuf
  toolchain + generated-code build setup.
- **REST/HTTP+JSON** — simplest to build, human-readable. Cost: verbose, text
  serialization overhead on a high-frequency path; weakest signal for "do you
  understand real distributed-systems RPC?"
- **Raw sockets** — deepest wire-level learning, but you hand-build framing and
  serialization for every message type → too slow to reach MVP for the *control*
  plane, where messages are structured and numerous.
- **Java RMI** — Java-native but dated, heavyweight, poor cross-language and
  streaming story. Rejected.

### Data plane
- **Raw TCP streaming** — open a socket, stream bytes with a tiny header. Zero
  serialization overhead, no message-size limits, maximum throughput. Also teaches
  the real mechanism (framing, packets).
- **gRPC** — would wrap opaque bytes in protobuf messages: pure overhead, hits the
  ~4 MB message limit, forces chunking, buys nothing (no structure to encode).
  Rejected for bulk data.
- **REST** — same problems, worse. Rejected.

## Decision

- **Control plane → gRPC** (Protocol Buffers over HTTP/2).
- **Data plane → raw TCP sockets** with a small custom streaming protocol.

Rationale: match the mechanism to the traffic. **Structured, high-frequency
messages** benefit from protobuf's compact binary encoding and typed contracts →
gRPC. **Bulk unstructured bytes** want the *least* machinery in the path → raw TCP
streaming, where any message/serialization layer is pure overhead. Learning value
reinforces the choice: gRPC teaches protobuf, service definitions, and streaming
RPC; raw sockets teach wire formats, framing, and packetized transfer.

Key insight (the one to defend in an interview): **gRPC/protobuf's binary
efficiency is about encoding *structured messages* compactly, not about moving
*bulk opaque bytes* fast.** So the "efficient binary" tool belongs on the control
plane, and raw streaming belongs on the data plane — not the reverse.

## Consequences

**Accepted costs**
- Two mechanisms to build and maintain instead of one.
- gRPC adds a protobuf build step (Maven protobuf plugin, generated stubs) and a
  learning ramp.
- We hand-roll the data-plane protocol (header + byte stream, later packets/acks).

**Benefits**
- Each plane is well-suited to its traffic; the NameNode stays off the data path.
- Conceptually mirrors real HDFS, so the design is defensible.

**Follow-on decisions (each may need its own ADR)**
- Data-plane protocol details: packet size, ack scheme, checksum placement
  (Phase 6 — write pipeline).
- gRPC service/message definitions (`.proto`) for NameNode and DataNode
  (Phase 2 — LLD).
- Maven module layout to hold shared `.proto` and generated code.

## Production vs. our mini-HDFS
- **Real HDFS:** control plane = **Hadoop RPC** (a custom RPC framework over TCP
  using Protocol Buffers); data plane = **DataTransferProtocol** (custom streaming
  over raw TCP, in packets). Same two-plane split, same conceptual mapping.
- **Our mini-HDFS:** gRPC instead of the bespoke Hadoop RPC (same protobuf idea,
  less boilerplate to learn the concept); a simplified custom TCP protocol for the
  data plane. *Simplified for the MVP; production hardens the data protocol with
  packet pipelining, per-chunk checksums, and pipeline recovery — deferred to
  Phase 6.*
