# ADR-0003: Control-Plane RPC — Hand-Rolled Framed Binary Protocol Instead of gRPC

- **Status:** Accepted
- **Date:** 2026-10-01
- **Supersedes:** the *control plane → gRPC* half of [ADR-0002](0002-communication-mechanism.md).
  The data-plane half (raw TCP streaming) stands.

## Context

Until Phase 7 every component ran in one JVM: the client and DataNodes called
`NameNodeImpl` methods directly, and only block bytes crossed a socket. That made the
system impossible to run as separate processes, which is the defining property of a
distributed file system. A real control-plane transport had to be built.

ADR-0002 chose gRPC. Two things changed since then:

1. The project rule "no black boxes — every mechanism explainable down to the wire
   format" (CLAUDE.md, rule 4). gRPC hides framing, connection management, and
   serialization behind generated stubs and HTTP/2.
2. The control surface turned out small and stable: 14 operations
   (`createFile`, `addBlock`, `heartbeat`, `blockReport`, ...), all request/response,
   no streaming, Java on both ends.

## Options considered

| Option | For | Against |
|---|---|---|
| **gRPC + protobuf** | Schema evolution, cross-language, deadlines, HTTP/2 multiplexing | Protobuf plugin + codegen in the build; HTTP/2 framing is opaque to the learner; ~10 MB of dependencies for 14 calls |
| **Java serialization (ObjectStream)** | Zero code | Insecure (deserialization gadgets), brittle across class changes, no wire spec to explain |
| **JSON over HTTP** | Readable, debuggable with curl | Text overhead on the hottest path (heartbeats); needs an HTTP server dependency |
| **Hand-rolled framed binary over TCP** | Every byte explainable; no dependencies; same shape as Hadoop RPC (length-prefixed frames carrying typed calls) | We own framing, error mapping, reconnects, and versioning |

## Decision

A length-prefixed binary protocol over one persistent TCP connection per caller
(`hdfs-common/.../rpc/`):

```
request  frame: [int32 length][byte op][fields...]
response frame: [int32 length][byte status][result... | UTF error message]
status: 0 = OK, 1 = ERROR, 2 = UNREGISTERED (DataNode must re-register)
```

Fields use `DataOutputStream` encoding (big-endian ints/longs, modified-UTF-8
strings, `int count` + items for lists). `Wire` holds every encoder/decoder so both
ends share one definition. Frames are capped at 16 MB so a corrupt length prefix
can't make the server allocate gigabytes.

The server (`NameNodeServer`) is thread-per-connection and serialises access to
the namesystem with one `ReentrantReadWriteLock`: lookups take the read lock and run
in parallel; mutations, heartbeats, and block reports take the write lock.

## Consequences

**Benefits**
- The NameNode, every DataNode, and every client are separate OS processes that
  only share a host:port. The cluster can be spread across machines.
- Nothing to explain away in an interview: the frame layout fits on one line.

**Costs, accepted knowingly**
- **No schema evolution.** Adding a field to a message breaks old peers. gRPC/protobuf
  would tolerate unknown fields. Fine for a cluster deployed from one build.
- **No automatic retries.** A call that times out may or may not have been applied
  (`createFile` is not idempotent), so the client surfaces the error rather than
  retrying blindly. Real HDFS attaches a client ID + call ID and keeps a
  *retry cache* on the NameNode so retried non-idempotent calls return the original
  result. Noted as Tier 2.
- **One lock, one fsync per mutation.** Write throughput is bounded by fsync latency.
  Real HDFS releases the lock before syncing and batches syncs (group commit).

## Production vs. our mini-HDFS
- **Real HDFS:** Hadoop RPC — the same idea (length-prefixed frames over TCP), with
  protobuf-encoded headers and bodies, SASL auth, call IDs, and a retry cache.
- **Ours:** same frame shape, hand-written field encoding, no auth, no retry cache.
