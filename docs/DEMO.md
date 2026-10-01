# Mini-HDFS - Live Demo

Everything below is real output from `bin/demo.ps1`, generated on 2026-10-01 20:57 in 30 s.
The script starts a NameNode and four DataNodes as **separate JVM processes**, drives them only
through the `hdfs dfs` command-line client, and injects real failures by killing processes and
editing block files on disk. Run it yourself: `bin/demo.ps1` (add `-KeepRunning` to keep the cluster up).

| Setting | Value |
|---|---|
| Block size | 4 KB (small, so ordinary files span several blocks) |
| Replication | 3 |
| Heartbeat | every 1 s; STALE after 3 s, DEAD after 6 s |
| Block scanner | every 3 s; full block report every 10 s |
| Checkpoint | every 20 edits |

## Results: 14 / 14 passed

| # | Feature | Result | Evidence |
|---|---|---|---|
| 1 | Multi-process cluster | PASS | NameNode + 4 DataNode processes up; all 4 registered and heartbeating within 0.8 s |
| 2 | Namespace ops (mkdir -p, put, write, ls, errors) | PASS | files and directories created; duplicate mkdir and ls-on-a-file rejected with clear errors |
| 3 | Block splitting + 3x replication | PASS | 3 blocks, each on 3 distinct DataNodes; meta file is 16 bytes (CRC32 + length) |
| 4 | Read path + checksums | PASS | downloaded copy SHA-256 matches the original |
| 5 | Rename / move | PASS | file moved; it still uses block 1 on the same DataNodes (no data copied) |
| 6 | Failure detection + re-replication | PASS | dn-1 declared DEAD after 6.3 s; all blocks back to 3 live copies 0.6 s later; file still reads correctly |
| 7 | Rejoin + over-replication cleanup | PASS | dn-1 re-registered by itself and deleted its now-redundant copies |
| 8 | Checksums + block scanner + repair | PASS | corruption found by the scanner in 1.6 s without any read; block back to 3 good copies; file still byte-identical |
| 9 | Pipeline failure recovery | PASS | upload succeeded despite the dead node: 1 block(s) abandoned and retried, 0 of 15 blocks placed on dn-4 |
| 10 | Delete + block invalidation | PASS | 15 blocks removed from every DataNode's disk within 1.3 s |
| 11 | Write-ahead log + checkpoints | PASS | edits logged before being applied; automatic checkpoint written after 20 edits |
| 12 | Crash recovery + safe mode | PASS | namespace rebuilt from fsimage + edit log; writes refused until block reports arrived (1.7 s); every file intact |
| 13 | Cluster-ID safety check | PASS | foreign DataNode refused to join and never appears in the NameNode's report |
| 14 | Interactive shell | PASS | cd / pwd / relative paths work |

---

## 1. Start a cluster of separate processes

One NameNode and four DataNodes, each its own JVM, talking only over TCP. Every DataNode
does a cluster-ID handshake, registers, sends a full block report, and then heartbeats
every second. The NameNode runs with a 4 KB block size so small files span several blocks.

**NameNode log**

```
[NameNode] listening on port 18020 | metadata demo-run\namenode | cluster d6d5dc5c-3824-4e67-a4e7-20cf5f88f5b8
[NameNode] block size 4096 B, STALE after 3000 ms, DEAD after 6000 ms without heartbeat
[NameNode] registered dn-1 at 127.0.0.1:19001
[NameNode] registered dn-3 at 127.0.0.1:19003
[NameNode] registered dn-2 at 127.0.0.1:19002
[NameNode] registered dn-4 at 127.0.0.1:19004
```

```
$ hdfs dfs report
  id       address               state    blocks         used    last hb
  dn-1     127.0.0.1:19001       NORMAL        0          0 B      0.7 s
  dn-3     127.0.0.1:19003       NORMAL        0          0 B      0.7 s
  dn-2     127.0.0.1:19002       NORMAL        0          0 B      0.6 s
  dn-4     127.0.0.1:19004       NORMAL        0          0 B      0.6 s
```

> **PASS** - NameNode + 4 DataNode processes up; all 4 registered and heartbeating within 0.8 s

## 2. Namespace operations

Directories and files live only in the NameNode's memory (backed by the edit log). Paths are
validated, parents must exist (or use `mkdir -p`), and a directory's size is the sum of its files.

```
$ hdfs dfs mkdir -p /projects/hdfs/docs
$ hdfs dfs mkdir /tmp
$ hdfs dfs write /projects/hdfs/hello.txt Hello from a real mini-HDFS cluster
wrote /projects/hdfs/hello.txt
$ hdfs dfs put README.md /projects/hdfs/docs
wrote /projects/hdfs/docs/README.md (9,137 B) in 67 ms
$ hdfs dfs ls /
  dir         9,173  2026-10-01 20:57  projects/
  dir             0  2026-10-01 20:57  tmp/
$ hdfs dfs ls /projects/hdfs
  dir         9,137  2026-10-01 20:57  docs/
  file           36  2026-10-01 20:57  hello.txt
$ hdfs dfs ls /projects/hdfs/docs
  file        9,137  2026-10-01 20:57  README.md
$ hdfs dfs mkdir /projects
error: Already exists: /projects
$ hdfs dfs ls /projects/hdfs/hello.txt
error: Not a directory: /projects/hdfs/hello.txt
```

> **PASS** - files and directories created; duplicate mkdir and ls-on-a-file rejected with clear errors

## 3. Blocks, replication and on-disk storage

README.md is split into 4 KB blocks. Each block was written through a pipeline
(client -> DN -> DN -> DN) and stored on 3 different DataNodes as `blk_<id>` plus a 16-byte
`blk_<id>.meta` holding its CRC32 and length. (`blk_1` is the small hello.txt written just before.)

```
$ hdfs dfs stat /projects/hdfs/docs/README.md
/projects/hdfs/docs/README.md: 9,137 B in 3 block(s)
  block 2            4,096 B  replicas 3  [dn-1, dn-3, dn-4]
  block 3            4,096 B  replicas 3  [dn-1, dn-2, dn-3]
  block 4              945 B  replicas 3  [dn-1, dn-3, dn-4]
```

**Block files on each DataNode's disk**

```
dn-1/current: blk_2 blk_3 blk_4
dn-2/current: blk_1 blk_3
dn-3/current: blk_1 blk_2 blk_3 blk_4
dn-4/current: blk_1 blk_2 blk_4
```

> **PASS** - 3 blocks, each on 3 distinct DataNodes; meta file is 16 bytes (CRC32 + length)

## 4. Read path

The client asks the NameNode only for block locations, then fetches each block straight from a
DataNode, verifying its checksum. The downloaded file is compared byte-for-byte with the original.

```
$ hdfs dfs get /projects/hdfs/docs/README.md demo-run\README.copy.md
saved 9,137 B to demo-run\README.copy.md
$ hdfs dfs cat /projects/hdfs/hello.txt
Hello from a real mini-HDFS cluster
```

> **PASS** - downloaded copy SHA-256 matches the original

## 5. Rename and move

A move is a metadata-only change on the NameNode: block IDs and DataNodes are untouched,
so it costs the same for 1 byte or 1 TB.

```
$ hdfs dfs mv /projects/hdfs/hello.txt /tmp/greeting.txt
moved /projects/hdfs/hello.txt -> /tmp/greeting.txt
$ hdfs dfs ls /projects/hdfs
  dir         9,137  2026-10-01 20:57  docs/
$ hdfs dfs ls /tmp
  file           36  2026-10-01 20:57  greeting.txt
$ hdfs dfs cat /tmp/greeting.txt
Hello from a real mini-HDFS cluster
```

> **PASS** - file moved; it still uses block 1 on the same DataNodes (no data copied)

## 6. A DataNode crashes

`dn-1` (which holds README.md's first block) is killed. Its heartbeats stop: after 3 s the
NameNode marks it STALE (no new blocks go there), after 6 s DEAD. Every block it held is now
below 3 copies, so the NameNode tells a surviving holder, through its next heartbeat reply,
to copy the block to another DataNode.

**NameNode log**

```
[NameNode] dn-1 is STALE (no heartbeat for 3369 ms)
[NameNode] dn-1 is DEAD (no heartbeat for 6401 ms)
[NameNode] block 2 under-replicated (2/3): dn-4 -> dn-2
[NameNode] block 3 under-replicated (2/3): dn-2 -> dn-4
[NameNode] block 4 under-replicated (2/3): dn-4 -> dn-2
```

```
$ hdfs dfs report
  id       address               state    blocks         used    last hb
  dn-1     127.0.0.1:19001       DEAD          0      9,137 B      7.3 s
  dn-3     127.0.0.1:19003       NORMAL        4      9,173 B      0.2 s
  dn-2     127.0.0.1:19002       NORMAL        4      4,132 B      0.2 s
  dn-4     127.0.0.1:19004       NORMAL        4      5,077 B      0.2 s
```

```
$ hdfs dfs stat /projects/hdfs/docs/README.md
/projects/hdfs/docs/README.md: 9,137 B in 3 block(s)
  block 2            4,096 B  replicas 3  [dn-2, dn-3, dn-4]
  block 3            4,096 B  replicas 3  [dn-2, dn-3, dn-4]
  block 4              945 B  replicas 3  [dn-2, dn-3, dn-4]
```

```
$ hdfs dfs get /projects/hdfs/docs/README.md demo-run\README.copy2.md
saved 9,137 B to demo-run\README.copy2.md
```

> **PASS** - dn-1 declared DEAD after 6.3 s; all blocks back to 3 live copies 0.6 s later; file still reads correctly

## 7. The crashed DataNode comes back

`dn-1` restarts with its old blocks still on disk. The NameNode had written it off, so it
answers the first heartbeat with UNREGISTERED. The DataNode re-registers and sends a full block
report, and every copy it reports is now a 4th replica, so the NameNode tells it to delete them.

**dn-1 log**

```
[dn-1] serving blocks on 127.0.0.1:19001 | storage demo-run\dn-1 (3 blocks) | NameNode localhost:18020
[dn-1] registered with NameNode, reported 3 blocks
[dn-1] deleted block 2
[dn-1] deleted block 3
[dn-1] deleted block 4
```

**Disks**

```
dn-1/current: (empty)
dn-2/current: blk_1 blk_2 blk_3 blk_4
dn-3/current: blk_1 blk_2 blk_3 blk_4
dn-4/current: blk_1 blk_2 blk_3 blk_4
```

> **PASS** - dn-1 re-registered by itself and deleted its now-redundant copies

## 8. Silent disk corruption

One byte of `blk_2` on `dn-2` is flipped directly on disk, and nobody reads the file.
The DataNode's background block scanner re-checks every block's CRC32, finds the mismatch,
moves the replica to `corrupt/`, and sends a block report right away. The NameNode sees the
block is down to 2 copies and re-replicates it from a healthy one.

**dn-2 log**

```
[dn-2] block scanner: Checksum mismatch for block 2 on dn-2
```

**NameNode log**

```
[NameNode] block 2 under-replicated (2/3): dn-4 -> dn-1
```

**Disks**

```
dn-1/current: blk_2
dn-2/current: blk_1 blk_3 blk_4    dn-2/corrupt: blk_2
dn-3/current: blk_1 blk_2 blk_3 blk_4
dn-4/current: blk_1 blk_2 blk_3 blk_4
```

```
$ hdfs dfs get /projects/hdfs/docs/README.md demo-run\README.copy3.md
saved 9,137 B to demo-run\README.copy3.md
```

> **PASS** - corruption found by the scanner in 1.6 s without any read; block back to 3 good copies; file still byte-identical

## 9. A DataNode dies in the middle of writes

`dn-4` is killed and a 60 KB file (15 blocks) is uploaded immediately, before the NameNode
notices. Any pipeline that includes `dn-4` fails. The pipeline reports which node failed, and
the client abandons that block, asks for a new one excluding `dn-4`, and retries.

```
$ hdfs dfs mkdir /data
$ hdfs dfs put demo-run\big.bin /data
wrote /data/big.bin (61,440 B) in 213 ms
```

**NameNode log**

```
[NameNode] block 5 of /data/big.bin abandoned after a pipeline failure
[NameNode] block 6 of /data/big.bin placed on [dn-1, dn-2, dn-3], excluding [dn-4] at the client's request
[NameNode] block 7 of /data/big.bin placed on [dn-2, dn-1, dn-3], excluding [dn-4] at the client's request
[NameNode] block 8 of /data/big.bin placed on [dn-1, dn-3, dn-2], excluding [dn-4] at the client's request
[NameNode] block 9 of /data/big.bin placed on [dn-1, dn-3, dn-2], excluding [dn-4] at the client's request
[NameNode] block 10 of /data/big.bin placed on [dn-1, dn-2, dn-3], excluding [dn-4] at the client's request
[NameNode] block 11 of /data/big.bin placed on [dn-2, dn-3, dn-1], excluding [dn-4] at the client's request
[NameNode] block 12 of /data/big.bin placed on [dn-1, dn-2, dn-3], excluding [dn-4] at the client's request
[NameNode] block 13 of /data/big.bin placed on [dn-2, dn-3, dn-1], excluding [dn-4] at the client's request
[NameNode] block 14 of /data/big.bin placed on [dn-1, dn-2, dn-3], excluding [dn-4] at the client's request
[NameNode] block 15 of /data/big.bin placed on [dn-2, dn-3, dn-1], excluding [dn-4] at the client's request
[NameNode] block 16 of /data/big.bin placed on [dn-3, dn-2, dn-1], excluding [dn-4] at the client's request
[NameNode] block 17 of /data/big.bin placed on [dn-3, dn-2, dn-1], excluding [dn-4] at the client's request
[NameNode] block 18 of /data/big.bin placed on [dn-3, dn-1, dn-2], excluding [dn-4] at the client's request
[NameNode] block 19 of /data/big.bin placed on [dn-1, dn-2, dn-3], excluding [dn-4] at the client's request
[NameNode] block 20 of /data/big.bin placed on [dn-1, dn-2, dn-3], excluding [dn-4] at the client's request
```

```
$ hdfs dfs get /data/big.bin demo-run\big.copy.bin
saved 61,440 B to demo-run\big.copy.bin
```

> **PASS** - upload succeeded despite the dead node: 1 block(s) abandoned and retried, 0 of 15 blocks placed on dn-4

## 10. Delete frees the blocks on every DataNode

Deleting a file removes it from the namespace and queues a DELETE for every replica of every
block; each DataNode receives its DELETEs in its next heartbeat reply.

**Before**

```
dn-1/current: 16 block file(s)
dn-2/current: 18 block file(s)
dn-3/current: 19 block file(s)
dn-4/current: 4 block file(s)
```

```
$ hdfs dfs rm /data/big.bin
```

**After**

```
dn-1/current: 1 block file(s)
dn-2/current: 3 block file(s)
dn-3/current: 4 block file(s)
dn-4/current: 4 block file(s)
```

**dn-1 log (excerpt)**

```
[dn-1] deleted block 6
[dn-1] deleted block 7
[dn-1] deleted block 8
[dn-1] deleted block 9
[dn-1] deleted block 10
```

> **PASS** - 15 blocks removed from every DataNode's disk within 1.3 s

## 11. Edit log and checkpoints

Every namespace change is appended to `edits.log` and fsync'd *before* it is applied. Every 20
edits the NameNode writes a full snapshot (`fsimage.bin`, via temp file + atomic rename) and
truncates the log, so recovery replays only recent edits. Below, the log holds only what happened since the last checkpoint.

**NameNode log**

```
[NameNode] checkpoint written
```

**Metadata directory**

```
edits.log         36 bytes
fsimage.bin      256 bytes
VERSION           36 bytes
```

**edits.log (op, timestamp, args)**

```
DELETE  1790868455726  /data/big.bin
```

**fsimage.bin (first lines)**

```
NEXT_INODE_ID  10
NEXT_BLOCK_ID  21
DIR  1    -1
DIR  2  projects  1
DIR  3  hdfs  2
DIR  4  docs  3
FILE  7  README.md  4  3  9137  0  2,3,4
DIR  5  tmp  1
```

> **PASS** - edits logged before being applied; automatic checkpoint written after 20 edits

## 12. Whole cluster loses power

Every process is killed at once (no clean shutdown). Then the NameNode alone is started: it loads
the fsimage and replays the edit log, but it knows no block locations yet, so it stays in
**safe mode**: reads of the namespace work, changes are refused. When the DataNodes come back
and report the blocks on their disks, it leaves safe mode on its own.

**Cluster is down**

```
$ hdfs dfs ls /
error: NameNode localhost:18020 unreachable: Connection refused: getsockopt
```

**NameNode up, no DataNodes yet**

```
$ hdfs dfs report
(no DataNodes registered)
  id       address               state    blocks         used    last hb
$ hdfs dfs ls /projects/hdfs/docs
  file        9,137  2026-10-01 20:57  README.md
$ hdfs dfs mkdir /after-restart
error: NameNode is in safe mode (waiting for DataNode block reports)
```

**NameNode log**

```
[NameNode] listening on port 18020 | metadata demo-run\namenode | cluster d6d5dc5c-3824-4e67-a4e7-20cf5f88f5b8
[NameNode] block size 4096 B, STALE after 3000 ms, DEAD after 6000 ms without heartbeat
[NameNode] in safe mode until DataNodes report their blocks
[NameNode] registered dn-2 at 127.0.0.1:19002
[NameNode] registered dn-4 at 127.0.0.1:19004
[NameNode] leaving safe mode: 4/4 blocks reported
[NameNode] registered dn-1 at 127.0.0.1:19001
[NameNode] registered dn-3 at 127.0.0.1:19003
```

**After recovery**

```
$ hdfs dfs ls /
  dir         9,137  2026-10-01 20:57  projects/
  dir            36  2026-10-01 20:57  tmp/
  dir             0  2026-10-01 20:57  data/
  dir             0  2026-10-01 20:57  after-restart/
$ hdfs dfs get /projects/hdfs/docs/README.md demo-run\README.copy5.md
saved 9,137 B to demo-run\README.copy5.md
$ hdfs dfs cat /tmp/greeting.txt
Hello from a real mini-HDFS cluster
```

> **PASS** - namespace rebuilt from fsimage + edit log; writes refused until block reports arrived (1.7 s); every file intact

## 13. A DataNode from another cluster tries to join

A DataNode whose stored cluster ID doesn't match is refused before it registers. Without this
check, a NameNode started on an empty metadata directory would treat every block a DataNode
reports as an orphan and order it deleted.

**intruder log**

```
[intruder] serving blocks on 127.0.0.1:19009 | storage demo-run\intruder (0 blocks) | NameNode localhost:18020
[intruder] FATAL: cluster ID mismatch: this DataNode belongs to some-other-cluster but the NameNode is d6d5dc5c-3824-4e67-a4e7-20cf5f88f5b8. Refusing to join (its block report would get every block deleted as an orphan).
```

```
$ hdfs dfs report
  id       address               state    blocks         used    last hb
  dn-2     127.0.0.1:19002       NORMAL        3      5,077 B      0.4 s
  dn-4     127.0.0.1:19004       NORMAL        4      9,173 B      0.4 s
  dn-1     127.0.0.1:19001       NORMAL        1      4,096 B      0.4 s
  dn-3     127.0.0.1:19003       NORMAL        4      9,173 B      0.4 s
```

> **PASS** - foreign DataNode refused to join and never appears in the NameNode's report

## 14. Interactive shell

The same client also runs as an interactive shell with a current directory, relative paths and `..`.

**Input: cd /projects/hdfs, pwd, ls, cd docs, stat README.md, cd ../.., pwd**

```
mini-HDFS client connected to NameNode localhost:18020. Type 'help' for commands.
hdfs:/> cd /projects/hdfs
hdfs:/projects/hdfs> pwd
/projects/hdfs
hdfs:/projects/hdfs> ls
  dir         9,137  2026-10-01 20:57  docs/
hdfs:/projects/hdfs> cd docs
hdfs:/projects/hdfs/docs> stat README.md
/projects/hdfs/docs/README.md: 9,137 B in 3 block(s)
  block 2            4,096 B  replicas 3  [dn-1, dn-3, dn-4]
  block 3            4,096 B  replicas 3  [dn-2, dn-3, dn-4]
  block 4              945 B  replicas 3  [dn-2, dn-3, dn-4]
hdfs:/projects/hdfs/docs> cd ../..
hdfs:/projects> pwd
/projects
hdfs:/projects> exit
```

> **PASS** - cd / pwd / relative paths work

