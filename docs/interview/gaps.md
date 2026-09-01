# Interview Gap Log

Tracks weak spots surfaced in gauntlets, to drill before the next phase.
Status: ⬜ open · ✅ closed (answered cleanly in a later drill).

## Phase 0 gauntlet — 2026-08-13

**Overall: PASS.** Strong conceptual grasp; clear growth (Q1 throughput argument,
which was the original weak spot, is now automatic). Gaps are mostly *precise
numbers* and *one signature answer* (the NameNode advantage).

Scorecard: Q1 ✅ · Q2 ✅ · Q3 ⚠️ · Q4 ✅/⚠️ · Q5 ⚠️ · Q6 ⚠️ · Q7 ✅ · Q8 ✅/⚠️

### Gaps to drill

- ⬜ **[HIGH] The single-NameNode ADVANTAGE.** Forgot it entirely (Q5). Must be
  automatic: single NameNode = single source of truth → strong consistency +
  simplicity (no distributed consensus for metadata). SPOF and this advantage are
  the *same* decision. CP-leaning. This is the #1 answer to nail.
- ⬜ **[HIGH] Replication factor arithmetic (Q6).** Said usable = raw ÷ 4. WRONG.
  RF = *total* copies. Usable = raw ÷ RF = raw ÷ 3. RF=3 means 3 copies total,
  not "original + 3".
- ⬜ **[MED] Seek time number (Q3).** Said ~1 ms; it's ~**10 ms**. The 10 ms is
  what makes the derivation land on ~100–128 MB (1 ms → ~10 MB).
- ⬜ **[MED] Too-big-block recovery cost (Q3).** Only cited lost parallelism.
  Missed: block = unit of re-replication → a dead 10 GB block drags 10 GB over
  the network to heal → slow healing + longer under-replicated (vulnerable) window.
- ⬜ **[MED] Selling write-once as GOOD design (Q4).** Led with "simpler." Lead
  instead with (1) workload never needs random writes, (2) name the immutability
  wins (trivial consistency, simple replication, free caching, simple concurrency).
- ⬜ **[LOW] Alternatives when refusing HDFS (Q8).** Said "normal filesystem."
  Wrong — name a **database / KV store** (low-latency, OLTP) or **object store**
  (small files). Bonus: HBase-on-HDFS for random access.
- ⬜ **[LOW] Throughput vs latency wording.** Say "high throughput," not "faster
  read." HDFS = high throughput, high latency.

### Strengths (keep)
- Q1 throughput/parallelism argument — now instinctive.
- Q2 partition vs replicate, incl. why replication alone gives no single-read speedup.
- Q7 metadata≠data reasoning (missing block vs network-unreachable) — reasoned
  from first principles with a hint.
