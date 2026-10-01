# ADR-0004: DataNode Block Storage and End-to-End Integrity

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

DataNodes kept blocks in a `HashMap` — a DataNode restart lost all its data, which
defeats the purpose of a storage node. Nothing detected corrupted bytes either: a
flipped bit on disk or on the wire would be served to clients as valid data.

## Decision

**On-disk layout** (`DiskDataNodeStorage`), one pair of files per block:

```
<dir>/current/blk_<id>        raw bytes
<dir>/current/blk_<id>.meta   [int64 CRC32][int64 length]
<dir>/corrupt/                quarantined replicas (kept for inspection)
<dir>/CLUSTER_ID              NameNode cluster this DataNode belongs to
```

**Crash-safe writes:** write `*.tmp`, `fsync`, then atomic rename — meta first, data
last. A block is visible only when both files are complete; leftover `*.tmp` files
are deleted at startup. A block is never half-present.

**Checksums at every hop:**
1. Client computes CRC32 and sends it after the bytes.
2. Every DataNode in the pipeline verifies it before storing (catches network corruption).
3. The stored `.meta` CRC is verified on every read (catches disk corruption).
4. The reply carries the CRC; the client verifies again (catches corruption on the way back).

**Repair:** a replica that fails its checksum is moved to `corrupt/`, drops out of the
next block report, and the NameNode re-replicates the block from a healthy copy. A
background **block scanner** re-reads every block periodically, so cold data that
nobody reads still gets checked.

**Cluster ID:** before registering, a DataNode asks the NameNode for its cluster ID
and refuses to join if it differs from the stored one. Without this, a NameNode
started on an empty or wrong metadata directory would see every reported block as an
orphan and order the whole cluster's data deleted.

## Options considered

- **One CRC per block (chosen)** vs **CRC per 512-byte chunk (real HDFS):** per-chunk
  lets a reader verify and serve a byte range without reading the whole block, and
  pinpoints the bad chunk. We always read whole blocks, so one CRC is enough.
- **CRC32 (chosen)** vs **MD5/SHA-256:** this is error detection, not tamper-proofing.
  CRC32 is cheap and catches all burst errors up to 32 bits. Security against a
  malicious DataNode would need signatures or block tokens — Tier 2.
- **Quarantine (chosen)** vs **delete on corruption:** quarantining keeps evidence for
  debugging, and the copy is still excluded from service.

## Consequences

- DataNodes survive restarts; full block reports after re-registration reconcile the
  NameNode's view with what's actually on disk.
- Each write costs two fsyncs and two renames per replica. Fine for 1 MB+ blocks.
- Store-and-forward pipeline: each DataNode receives the whole block before
  forwarding it, so pipeline latency is roughly 3× a single transfer. Real HDFS
  forwards 64 KB packets as they arrive (cut-through), overlapping the hops.
