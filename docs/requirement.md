1. Problem Statement :   HDFS exist to staore datasets too large for any single machine and to reaad them fast, by spreading data across many commodity machine to get their combined csapacity and disk bandwidth in parallel- with replication added so that cluster tolerated th constant failures that come with haveing tousands of machine. 

2. Goals : 
    1. this system has single namenode 
    2. this system has mutiple datanode. 
    3. it has a CLI
    4. File : it will be break into mutiple fixed size blocks
    5. replication : Each block maintains a configurable replication factor (default 3). If a block is under-replicated the system creates copies; if over-replicated it removes them.
    6. heartbeats: each datanode will send hearbeat to namenode to indiate that the datanode is alive and ready to query. 
    7. failure detection : any datanode failed to send heartbeat. failing this will declare the datanode dead and proceed for recovery. 
    8. Self healing : replication of node will start. 
    9. Client will be able to write a document. a real write pipeline (client → DN → DN → DN);
    10. CLint able to read the file. 
    11. edit log: Every metadata-mutating operation is appended to an edit log (reads are not logged, since they don't change state). 
    12. fsimage : persistence with restart recovery.


3. Non Goals : HA / standby NameNode / QJM / ZKFC / fencing; Federation; write leases + full pipeline-recovery edge cases; erasure coding; snapshots; quotas; security / Kerberos / block tokens; short-circuit reads.


4. Actors: 
    User : Client
       UseCases : 1. give file to store. 
                  2. create ,list, delete, move , creaate directory
                  3. get file
                  4. 
         : Admin: 
       UseCases : 1. starts and stops the NameNode and DataNodes
                  2. decides the replication factor, or changes it
                  3. watches cluster health, adds/removes machines, 
                  4. decommissions a node for planned maintenance/retirement


5. Functional Rq: 
   FR-1: Create file
    - Description: Client writes a new file into the namespace; the system splits it into blocks and replicates each.
    - Inputs: absolute path (e.g. /logs/day1.txt), the file data (byte stream), optional replication factor.
    - Output: success ack once all blocks are written and min-replicated; or an error.
    - Behavior/constraints: write-once (no overwrite of an existing path); file becomes visible/readable only after the write completes (close).
    - Failure & edge cases (the important part):
        -Path already exists → reject with FileAlreadyExists (write-once — we don't overwrite).
        -Parent directory doesn't exist → reject (or auto-create? — a real design choice; HDFS rejects unless -p).
        -Client dies mid-write → the half-written file must not become visible. What state is it in? (This is the lease problem — note it, defer to Phase 2.)
        -A DataNode in the write pipeline dies mid-write → what happens? (Pipeline recovery — Phase 6.)

   FR-2: Read file
    - Description: Client asks the NameNode for the block locations of a path, then reads the blocks directly from DataNodes and reassembles them in order.
    - Inputs: absolute path.
    - Output: file content (byte stream), or error.
    - Failure & edge cases:
        - File/path does not exist → error (FileNotFound).
        - One replica of a block is down → NOT an error; client retries another replica (this is why we replicate).
        - ALL replicas of a block are down → "missing/corrupt block" error; metadata exists but data is gone (metadata and data can disagree).
        - File currently being written → not visible yet → behaves as FileNotFound.

   FR-3: Delete file / directory
    - Description: METADATA operation. NameNode removes the namespace entry immediately; physical block deletion on DataNodes happens lazily/async (piggybacked on heartbeat responses).
    - Inputs: path, recursive flag (for directories).
    - Output: ack (namespace entry removed).
    - Failure & edge cases:
        - Path does not exist → error.
        - Non-empty directory without recursive flag → reject (rm vs rm -r).
        - File currently being read → metadata removed; in-flight reader may continue from DataNodes until blocks are physically cleaned up.

   FR-4: Rename / move
    - Description: PURE METADATA operation. Updates ONLY the namespace/path entry. The file→block and block→DataNode mappings are UNCHANGED; zero file bytes move. O(1) regardless of file size. Must be ATOMIC.
    - Inputs: source path, destination path.
    - Output: ack.
    - Failure & edge cases:
        - Destination already exists → reject (no silent overwrite).
        - Source does not exist → error.
        - Moving a directory into its own subtree → reject (would create a cycle).
    - Note: atomic + O(1) makes rename usable as a "commit" primitive (write to temp path, then atomic rename to final).

   FR-5: Create directory (mkdir)
    - Inputs: path (optional -p to create missing parents).
    - Output: ack. Failure: already exists, or parent missing without -p.

   FR-6: List directory (ls)
    - Inputs: path.
    - Output: entries (names, type, size, replication). Failure: path does not exist.

   FR-7: Admin operations
    - Set/change replication factor (cluster default or per-file).
    - View cluster health report (live/dead DataNodes, capacity, under-replicated blocks).
    - Decommission a node for planned maintenance (drain its blocks first, then retire). Distinct from crash handling, which is automatic (NameNode).


6. Non-Functional Requirements (target SLOs)

   NFR-1: Durability (data is not lost)
    - Every block replicated x3 (configurable). A block is lost only if ALL 3 replica-holding machines fail within the re-replication window (before the system detects and rebuilds the missing replica).
    - Self-healing: under-replicated blocks are automatically re-replicated.
    - Production note: real HDFS places replicas across FAULT DOMAINS (rack awareness) so one rack/switch failure can't take all 3. Our MVP may use naive placement; note the gap.

   NFR-2: Availability (data is reachable right now)
    - DataNode failures are tolerated: reads/writes continue as long as some replica and the NameNode are up.
    - The single NameNode is a SINGLE POINT OF FAILURE: if it dies, the whole cluster is UNAVAILABLE (metadata lives only there). Data is still durable (not lost), just unreachable until the NameNode recovers.
    - durability != availability. This is our biggest weakness; HA (standby NameNode) is the Tier-2 fix.

   NFR-3: Consistency
    - STRONG consistency. Once a write completes (file closed, min-replicated), it is immediately readable and ALL readers see identical bytes.
    - During a write the file is invisible (write-once, visible-at-close).
    - Enabled by the single NameNode = single source of truth for metadata. Trade-off: this same choice is what causes the SPOF in NFR-2. We chose strong consistency and paid for it with availability (a CP-leaning choice).

   NFR-4: Throughput vs latency
    - Optimize for high SEQUENTIAL THROUGHPUT (aggregate bandwidth) on large files; accept high per-operation LATENCY.
    - NOT optimized for many small low-latency operations (small-files problem: NameNode RAM + per-op overhead). Large files, sequential scans.

   NFR-5: Scalability (direction, not a hard number for MVP)
    - Storage/throughput scale horizontally: add DataNodes → more capacity and more aggregate bandwidth.
    - NameNode RAM is the scaling ceiling (all metadata in memory, ~150 B/object). Federation is the Tier-2 answer.

