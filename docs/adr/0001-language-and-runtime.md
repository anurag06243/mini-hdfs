# ADR-0001: Language and Runtime

- **Status:** Accepted
- **Date:** 2026-08-13
- **Deciders:** Anurag Kumar
- **Note:** This ADR is written by Claude as a *worked example of the format*.
  Every subsequent ADR is drafted by Anurag and hardened by Claude.

## Context

We are building a distributed file system from scratch. The implementation
language constrains: the concurrency primitives available to us, how honestly we
can model I/O and threading, how much of our time goes to the language versus
the architecture, and whether we can compare our design against a real
production system when we want ground truth.

Constraints that matter here:

- The learner works in Java/Spring Boot professionally — language friction
  should be near zero so cognitive budget goes to distributed systems.
- The project is a resume artifact; the stack should read as credible for a
  systems role.
- Java 22 and Maven 3.9.8 are already installed on the machine.

## Options considered

### 1. Java 22 + Maven

- **For:** Apache Hadoop is written in Java, so when we want to check our design
  against reality the source is directly comparable. Mature concurrency toolkit
  (`java.util.concurrent`, NIO, virtual threads in 21+) — every concurrency
  lesson is a real lesson. Zero language ramp-up for this learner. Strong
  ecosystem for serialization, testing, and benchmarking. Already installed.
- **Against:** GC pauses are a real concern for a storage system at scale — the
  real NameNode heap is a known operational pain point. Verbose. JVM startup
  cost makes many-node local simulation heavier than a Go equivalent.

### 2. Go

- **For:** Purpose-built for this class of system: goroutines and channels make
  pipelines and fan-out natural, static binaries make multi-node local
  deployment trivial, gRPC is first-class, and the runtime is lighter than the
  JVM for spinning up 20 DataNodes on one laptop.
- **Against:** Learning a language *while* learning distributed systems splits
  attention — the stated goal is depth on architecture, not breadth on syntax.
  Loses the direct comparison line to Hadoop source. Weaker generics story for
  the abstractions we will want in the LLD phase.

### 3. Python

- **For:** Fastest to prototype; the design reads like pseudocode.
- **Against:** Disqualifying. The GIL means the concurrency we build — write
  pipelines, lock striping on the namespace, thread pools on the DataNode —
  would be a simulation of the lesson rather than the lesson. Also the weakest
  resume signal of the four for a systems project.

### 4. C++

- **For:** Maximum control over memory layout and I/O; closest to how a
  production storage engine is actually built today. No GC pauses.
- **Against:** A large fraction of project time would go to the language and its
  build system rather than to architecture. Highest risk of the project stalling
  — and a stalled project teaches nothing.

## Decision

**Java 22, built with Maven.**

The deciding factor is where the learner's time goes. Java 22 costs us
essentially zero language ramp-up and gives us genuine concurrency primitives,
which means nearly all effort lands on distributed systems design — the actual
objective. That the reference implementation is also Java is a significant
secondary benefit: when we disagree with ourselves, there is a real system to
check against.

Go was the strongest alternative and would have been chosen if the learner were
already fluent in it.

## Consequences

**Accepted costs**

- We inherit the JVM's GC characteristics. This is not purely a downside: real
  NameNode heap pressure is a genuine HDFS operational topic, so we will hit and
  must reason about the same problem the real system has. We will need to think
  carefully about the in-memory namespace representation.
- Running many nodes on one machine is memory-heavy. We will need a deliberate
  local multi-node story (separate JVMs vs. threads in one JVM) — this becomes
  its own ADR.

**Follow-on decisions this opens (each needs its own ADR)**

- Build layout: single module vs. multi-module Maven (`common`, `namenode`,
  `datanode`, `client`).
- Concurrency model: platform threads + pools vs. virtual threads.
- RPC and wire format.
- Serialization for on-disk structures (fsimage, edit log).

**Revisit if:** the JVM footprint makes realistic multi-node testing
impractical, or GC behaviour dominates our measurements to the point that it
obscures the design lessons we are trying to observe.
