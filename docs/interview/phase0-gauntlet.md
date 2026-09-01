# Phase 0 Gauntlet — Full Transcript (2026-08-13)

> Every question, my answer, my reasoning, and the mentor's grading + correct
> reasoning. Closed-book interview drill. Result: **PASS** (growth shown).
> Companion files: [gaps.md](gaps.md) (drill list) · [learning log](../learning/phase0-requirements.md).
>
> Scorecard: Q1 ✅ · Q2 ✅ · Q3 ⚠️ · Q4 ✅/⚠️ · Q5 ⚠️ · Q6 ⚠️ · Q7 ✅ · Q8 ✅/⚠️

---

## Q1 — Why HDFS vs one big server?

**Question:** "I've got a 50 TB dataset to scan repeatedly. My infra guy says buy
one server with 60 TB of disk. Why use HDFS instead?"

**My answer:** Calculate the read time. 50 TB ÷ 100 MB/s = 500,000 s ≈ 150 hrs
just to read the dataset once. That's where a system like HDFS is needed — it
turns 150 hrs into minutes by distributing the dataset into blocks and reading
them in parallel across nodes.

**My reasoning:** Throughput. One disk reads at ~100 MB/s; parallelism across many
nodes multiplies aggregate bandwidth.

**Grade: ✅ strong.** Led with throughput/parallelism — the killer point, and the
one I *missed* the first time we did this (growth). Number fix: 50 TB ÷ 100 MB/s ≈
**139 hrs**, not 150 (rounding fine, but know it's ~139).

### Q1 follow-up — "12 disks in one server = 1.2 GB/s, why still HDFS?"

**My answer:** (1) Availability lost — if that one machine fails, you're stuck.
(2) No replication — if a disk fails, where do you recover the lost data?

**Grade: ✅ two valid walls, but both are *reliability* walls.** The infra guy
bragged about *speed/simplicity*; I attacked reliability instead of his actual
claim. Missed two stronger walls:
- **Capacity / scale-out ceiling** — one chassis holds ~12–24 disks (~200 TB cap);
  datasets grow to petabytes. Scale *up* is finite; HDFS scales *out* linearly.
  1.2 GB/s is a hard ceiling; HDFS aggregate bandwidth isn't.
- **Compute wall (the deep one)** — even if one box *reads* at 1.2 GB/s, one
  machine's CPU must *process* 50 TB. HDFS lets you **bring compute to data**
  across 1,000 machines: 1,000× bandwidth AND 1,000× CPU. *"Move compute to data,
  not data to compute."*

### Side-learning — how "move compute to data" works technically
HDFS is just storage; a compute framework (MapReduce/Spark) does the moving,
enabled by HDFS via: (1) `getBlockLocations(path)` exposes *which machine* holds
each block; (2) DataNode (storage) and compute worker are co-located on the same
box. The scheduler ships the **task (few KB of code)** to the machine that already
holds the block (**data locality**: NODE_LOCAL > RACK_LOCAL > OFF_RACK); the
machine reads its *local* block and computes; only tiny **results** cross the
network — never the 50 TB. We WILL build `getBlockLocations` (the read path uses it).

---

## Q2 — Partition vs replication

**Question:** "Explain partitioning vs replication like I'm a new grad. Which gives
faster reads, which keeps data safe, and can replication alone give the speed?"

**My answer:** Partitioning = divide a file into blocks (128 MB) distributed over
DataNodes → parallel computation, speed via aggregate bandwidth + node compute.
Replication = duplicate blocks so if one doesn't respond, a replica takes its
place → availability + durability, data safe. Speed from replication alone? **No.**

**Grade: ✅ definitions correct, "No" correct.**

### Q2 follow-up — "Why not? (1 GB file, replicated ×3 but not partitioned)"

**My answer:** With replication only, you read the whole file sequentially from
one copy (or one of its replicas) — ~8 s. Even with 3 copies you still read
sequentially from one, so no speedup.

**Grade: ✅ correct justification.** Number fix: 1 GB ÷ 100 MB/s ≈ **10 s** (you
said 8 — likely mixed with "8 blocks"). Polish for a *great* answer: replication
*does* help **aggregate** throughput across *many concurrent readers* (spread
across replicas, pick closest/least-loaded) — just not a *single* read.

---

## Q3 — Why 128 MB blocks (vs 4 KB), and what breaks at 10 GB?

**My answer:** NameNode RAM grows by the *number of objects* created, not file
size — with 4/8 KB blocks a single file makes billions of block objects/addresses.
Also a "sweep time" concept (~1 ms, name unsure): the time for the pointer to move
between nodes should be ~1% of the compute at 100 MB/s → nearest number is 128 MB.

**My reasoning:** Metadata explosion + the seek/transfer ratio.

**Grade: ⚠️ concept ✅, numbers off, half unanswered.**
- Metadata-by-object-count ✅.
- The concept is **seek time** (name it), and it's ~**10 ms**, not 1 ms. Derivation:
  seek = 1% of read → read = 1 s → 100 MB/s × 1 s = ~100 MB → 128 MB. (1 ms → 10 MB.)
- **Did not answer the 10 GB half** initially.

### Side-learning — where 100 MB/s comes from
It's the ~sustained sequential throughput of a commodity HDD (mid-2000s) — a
ballpark, not a standard. Block size *follows hardware*: defaults grew 64 → 128 →
256 MB as disks sped up; SSDs (~0 seek) weaken the seek argument (only the
metadata argument for large blocks remains).

### Q3 second half — "what breaks at 10 GB blocks?"

**My answer:** Parallelism issue — a 9 GB file sits in one block, no partitioning,
no parallelism, no time gain. On the node-death hint: it stays durable (replicas
exist); only parallelism is impacted.

**Grade: parallelism ✅; durability-preserved ✅; missed recovery cost.** The block
is the **unit of re-replication** — a dead 10 GB block drags **10 GB** across the
network to heal → slow healing + a longer under-replicated (vulnerable) window.
(Also: uneven load balancing with giant blocks.)

---

## Q4 — Write-once: sell it as good design; what breaks with mid-file edits?

**My answer:** It's the price paid to gain scan speed. If we allowed mid-file
edits: (1) an edit must propagate to replicas; a network issue mid-update leaves a
replica un-updated → data inconsistency → catastrophic. (2) One write triggers
calls to all replica blocks to override → network congestion, huge for large
edits. The system gets more complex; write-once keeps it simple.

**Grade: ✅ the failure (consistency), ⚠️ the "why it's good."**
- Sharpen the consequence: not just "inconsistency" but **non-deterministic reads /
  silent corruption** — reads come from *one* replica, so different readers see
  different bytes for the same offset, no source of truth.
- Lead the "good design" case with the two strong pillars (I led with "simpler,"
  the weakest): (1) **the workload never needs random writes** (write-once-read-
  many); (2) **name the immutability wins** — trivial consistency, simple
  replication (copy bytes), free caching, simple concurrency, max throughput.
  Magic word: **immutability** (Kafka, LSM-trees, Git).
- Network congestion is minor; the real cost is **coordination/atomicity** (a
  distributed atomic update across replicas on every write).

---

## Q5 — Single NameNode: SPOF, and any advantage?

**My answer (after a scaffold):** Yes, it's a SPOF — if it dies the cluster
collapses, availability becomes 0, metadata unreachable. I forgot the advantage.

**Grade: ⚠️ weakness nailed, advantage forgotten (HIGH-priority gap).**
- Add: when the NameNode dies the **data is still durable — not lost, just
  unreachable.** durability ≠ availability.
- **The advantage:** single NameNode = **single source of truth** for all metadata
  → **strong consistency** (one authority, one order, no distributed consensus for
  metadata) + **simplicity** (in-memory ops under one lock; no Paxos/Raft, no
  split-brain).
- **The one-liner:** *"The SPOF and the strong consistency are the same design
  decision — one authoritative master is one consistent source of truth (strength)
  and one thing that can die (weakness). I chose consistency + simplicity and paid
  with availability — a CP-leaning choice."*
- Full circle: HA (standby NN + QJM + ZKFC + fencing) removes the SPOF but adds
  back exactly the consensus complexity the single NN avoided. No free lunch.

---

## Q6 — Capacity + bottleneck

**Question:** "100 DataNodes × 10 TB, RF 3. Usable data? What runs out first —
disk or NameNode RAM? Does it change for millions of tiny files?"

**My answer:** Total = 1000 TB ÷ 4 (3+1) = 250 TB usable. Disk runs out for big
files; NameNode RAM runs out for small files.

**Grade: ❌ RF arithmetic, ✅ bottleneck concept.**
- **RF = total copies, not "original + 3".** Usable = raw ÷ RF = 1000 ÷ **3** =
  **~333 TB** (not ÷4 = 250). Must fix — saying 250 reveals you don't know what RF
  means.
- Bottleneck ✅: NameNode RAM scales with **object count** (files+blocks), not data
  volume → big files are disk-bound, small files are NameNode-RAM-bound (same
  333 TB as 1 MB files ≈ 100× the RAM). The small-files problem.

---

## Q7 — Read fails though the file exists; two reasons; lost or recoverable?

**My answer:** First said "no idea." After the metadata≠data hint: (1) the
DataNode/data is actually gone/lost but not yet updated in metadata; (2) a network
issue makes the data unreachable.

**Grade: ✅ both correct (with a hint) — good first-principles recovery.**
1. **All replicas of the block are dead → "missing block."** Metadata says the
   file exists; the bytes are gone. → **Data lost.** (metadata/data disagree.)
2. **Network partition → DataNodes unreachable.** Data is fine on healthy disks,
   just unreachable now. → **Recoverable** (retry another replica / heals).
- Bonus 3rd: **block corruption** — verified by per-block **checksum** on read; try
  another replica; all corrupt → lost. (Checksums = a concept we'll build.)

---

## Q8 — What's it good at; one system to choose it, one to refuse + alternative?

**My answer:** Good at write-once-read-many (logs, reports, scanning), large data
volumes loaded quickly, strong consistency, durability, fast reads. Refuse it for
write-heavy workloads / many writes — there I'd use a normal filesystem.

**Grade: ✅ good-at, ⚠️ alternative wrong.**
- Say **"high throughput"** not "fast reads" (HDFS = high throughput, high latency).
- **"Normal filesystem" is the wrong alternative** — it has the same ceilings HDFS
  solves. Name the right one by *reason*:
  - low-latency random access / lookups → **database / KV store** (Cassandra,
    Redis, DynamoDB);
  - transactional frequent updates (OLTP) → **RDBMS** (Postgres, MySQL);
  - millions of small files → **object storage** (S3).
- Bonus: **HBase runs on top of HDFS** to add random read/write.

---

## Overall verdict

**PASS.** Strong conceptual grasp and real growth under pressure. Two HIGH gaps to
drill until reflexive: (1) the **single-NameNode advantage** (single source of
truth → strong consistency; same decision as the SPOF), (2) **RF arithmetic**
(usable = raw ÷ RF = ÷3). Remaining gaps are precision fixes — see
[gaps.md](gaps.md).
