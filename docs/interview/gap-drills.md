# Gap Drills — Question → Model Answer

> Flashcards for the weak spots from the Phase 0 gauntlet ([gaps.md](gaps.md)).
> **How to use:** cover the answer, say yours out loud FIRST, then check. Reading
> the answer builds recognition; producing it builds recall. Drill until the two
> HIGH ones are reflexive.

---

## 🔴 HIGH-1 — The single-NameNode advantage (and why weakness = advantage)

**Q:** "Your single NameNode is a SPOF — but you claim it's also an *advantage*.
Explain the advantage, and why the weakness and the advantage are the *same*
design decision."

**Model answer:**
A single NameNode is the **single source of truth** for all metadata (the
namespace tree + the block map). Every metadata operation flows through **one
place, in one order**, which buys two things almost for free:
- **Strong consistency** — one authority means one consistent view; no reconciling
  metadata across masters, no distributed consensus for metadata ops, no split-brain.
- **Simplicity** — metadata ops are in-memory operations under one lock; no
  Paxos/Raft to agree on the namespace.

The SPOF and this advantage are the **same decision** because having *exactly one
authoritative master* is simultaneously (a) one **consistent source of truth**
[strength] and (b) one **thing that can die** and take the cluster down [weakness].
**I chose consistency + simplicity and paid with availability — a CP-leaning
choice** (CAP).

Two things that make it airtight:
- When the NameNode dies, **data is still durable — not lost, just unreachable.**
  *durability ≠ availability.*
- The Tier-2 fix — **HA** (standby NN + quorum journal + ZKFC + fencing) — removes
  the SPOF but **adds back exactly the consensus complexity the single NameNode
  avoided.** No free lunch.

---

## 🔴 HIGH-2 — Replication-factor arithmetic + the bottleneck

**Q:** "100 DataNodes × 10 TB, replication factor 3. How much usable data? What
runs out first — disk or NameNode RAM — and does file size change the answer?"

**Model answer:**
**Replication factor = TOTAL number of copies**, not "original + 3." So:
> usable = raw ÷ RF = (100 × 10 TB) ÷ 3 = 1000 ÷ 3 ≈ **333 TB**.

The bottleneck depends on **file size**, because **NameNode RAM scales with the
number of objects (files + blocks), not with data volume** (~150 B/object, all in
RAM):
- **1 GB files** → ~333k files, ~2.6M blocks → **~1 GB NameNode RAM** → **disk-bound.**
- Same 333 TB as **1 MB files** → ~333M files → **~100 GB NameNode RAM** →
  **NameNode-RAM-bound.**

Same data, ~100× the RAM → the **small-files problem.**

---

## 🟠 MED-1 — Seek time number

**Q:** "Derive 128 MB from disk physics."
**A:** Seek ≈ **10 ms** (not 1 ms). Want seek = ~1% of block read time → read ≈ 1 s
→ at 100 MB/s → block ≈ 100 MB → **128 MB**. (If seek were 1 ms → ~10 MB block.)
The large block exists *because mechanical seeks are slow*; on SSDs (~0 seek) the
argument weakens and only the NameNode-metadata reason for large blocks remains.

## 🟠 MED-2 — Cost of TOO-BIG blocks (e.g. 10 GB)

**Q:** "What breaks at 10 GB blocks?"
**A:** (1) **Lost parallelism** — small/medium files sit in one block on one node.
(2) **Expensive recovery** — the block is the **unit of re-replication**; a dead
10 GB block drags **10 GB** across the network to heal → slow healing + a longer
**under-replicated (vulnerable) window**. (3) Uneven load balancing. (Data stays
durable — replicas exist; it's the *recovery dynamics* that suffer.)

## 🟠 MED-3 — Selling write-once as GOOD design

**Q:** "Isn't write-once a limitation? Convince me it's good."
**A:** Lead with **two pillars**, not "it's simpler":
1. **The workload never needs random writes** (write-once-read-many batch
   analytics) — don't build what the workload doesn't use.
2. **Name the immutability wins:** trivial consistency, simple replication (just
   copy bytes), free caching, simple concurrency, max sequential throughput.
Magic word: **immutability** (Kafka, LSM-trees, Git). What breaks if you allowed
edits: partial multi-replica update → replicas disagree → **non-deterministic
reads / silent corruption**; to do it right needs a **distributed atomic update on
every write.**

## 🟢 LOW-1 — Alternatives when refusing HDFS

**Q:** "Where would you NOT use HDFS, and what instead?"
**A:** NOT "a normal filesystem" (same ceilings). By reason:
- low-latency random access / lookups → **database / KV store** (Cassandra, Redis, DynamoDB)
- transactional frequent updates (OLTP) → **RDBMS** (Postgres, MySQL)
- millions of small files → **object storage** (S3)
Bonus: **HBase runs on top of HDFS** to add random read/write.

## 🟢 LOW-2 — Throughput vs latency wording

**A:** Say **"high throughput,"** not "fast reads." HDFS = **high throughput,
high latency** (great at moving huge volume, slow per-operation).
