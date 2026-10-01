package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.HeartbeatResponse;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.common.model.DatanodeState;
import dev.anurag.hdfs.datanode.InMemoryDataNode;
import dev.anurag.hdfs.datanode.IDataNodeStorage;
import dev.anurag.hdfs.namenode.NameNodeImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatReplicationTest {

    private static final long STALE_MS  = 100L;
    private static final long DEAD_MS   = 200L;
    private static final int  BLOCK_SIZE = 1024 * 1024;

    private NameNodeImpl nameNode;
    private InMemoryDataNode dn1, dn2, dn3;
    private DatanodeInfo dn1Info, dn2Info, dn3Info;
    private Map<String, IDataNodeStorage> registry;
    private HdfsClient client;  // RF=2

    @BeforeEach
    void setUp() throws Exception {
        nameNode = new NameNodeImpl(BLOCK_SIZE, STALE_MS, DEAD_MS);

        dn1 = new InMemoryDataNode("dn-1");
        dn2 = new InMemoryDataNode("dn-2");
        dn3 = new InMemoryDataNode("dn-3");

        // Keep references so we can observe state changes (these are the same objects
        // registered with NameNode — NameNodeImpl mutates state on them directly)
        dn1Info = new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L);
        dn2Info = new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L);
        dn3Info = new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L);

        nameNode.registerDatanode(dn1Info);
        nameNode.registerDatanode(dn2Info);
        nameNode.registerDatanode(dn3Info);

        registry = new HashMap<>(Map.of("dn-1", dn1, "dn-2", dn2, "dn-3", dn3));
        client = new HdfsClient(nameNode, registry, BLOCK_SIZE, (short) 2);
    }

    // -------------------------------------------------------------------------
    // Heartbeat resets state to NORMAL; empty heartbeat returns no commands
    // -------------------------------------------------------------------------

    @Test
    void heartbeat_resetsStateToNormal_andReturnsEmptyCommands() throws Exception {
        HeartbeatResponse resp = nameNode.heartbeat(new HeartbeatRequest("dn-1", 0L, 10_000_000L));
        assertNotNull(resp);
        assertTrue(resp.commands().isEmpty(), "No commands on a fresh heartbeat");
        assertEquals(DatanodeState.NORMAL, dn1Info.getState());
    }

    // -------------------------------------------------------------------------
    // Missing heartbeats → DEAD; a heartbeat after that resets to NORMAL
    // -------------------------------------------------------------------------

    @Test
    void missingHeartbeat_transitionsToDeadThenNormalOnResume() throws Exception {
        // Let time elapse past the DEAD threshold without any heartbeats
        Thread.sleep(DEAD_MS + 50);
        nameNode.checkAndReplicateDeadNodes();

        assertEquals(DatanodeState.DEAD, dn1Info.getState(), "dn-1 should be DEAD");

        // DN comes back — heartbeat should reset to NORMAL
        nameNode.heartbeat(new HeartbeatRequest("dn-1", 0L, 10_000_000L));
        assertEquals(DatanodeState.NORMAL, dn1Info.getState(), "dn-1 should be NORMAL after heartbeat");
    }

    // -------------------------------------------------------------------------
    // Block report: a DN losing a block triggers REPLICATE on the remaining DN
    // -------------------------------------------------------------------------

    @Test
    void blockReport_lostReplica_queuesReplicateOnRemainingDn() throws Exception {
        // Write a file; RF=2 means exactly 2 of the 3 DNs hold the block
        client.put("hello replication".getBytes(), "/test.txt");

        // Find the two DNs that hold the block
        String sourceDnId = !dn1.listBlocks().isEmpty() ? "dn-1"
                          : !dn2.listBlocks().isEmpty() ? "dn-2" : "dn-3";
        InMemoryDataNode sourceDn = sourceDnId.equals("dn-1") ? dn1
                                  : sourceDnId.equals("dn-2") ? dn2 : dn3;
        long blockId = sourceDn.listBlocks().get(0);

        // Simulate: source DN still has the block; all others "lost" it
        String emptyDnId1 = sourceDnId.equals("dn-1") ? "dn-2" : "dn-1";
        nameNode.blockReport(new BlockReportRequest(sourceDnId, List.of(blockId)));
        nameNode.blockReport(new BlockReportRequest(emptyDnId1, List.of()));
        nameNode.blockReport(new BlockReportRequest("dn-3", List.of()));

        // Heartbeat the source DN — NN should have queued REPLICATE for under-replication
        HeartbeatResponse resp = nameNode.heartbeat(new HeartbeatRequest(sourceDnId, 0L, 10_000_000L));

        boolean hasReplicate = resp.commands().stream()
                .anyMatch(c -> c.action() == DatanodeCommand.Action.REPLICATE
                            && c.blockId() == blockId);
        assertTrue(hasReplicate, "Source DN should receive REPLICATE command for the under-replicated block");
    }

    // -------------------------------------------------------------------------
    // DN dies → re-replication brings block count back to RF
    // -------------------------------------------------------------------------

    @Test
    void datanodeDeath_reReplicationRestoresReplicaCount() throws Exception {
        // RF=2: write a file — exactly 2 of 3 DNs get the block
        client.put("resilient data".getBytes(), "/resilient.txt");

        // Identify victim (a DN that HAS the block), survivor (also has the block),
        // and the empty DN (currently holds no block — will receive the re-replicated copy)
        List<Long> b1 = dn1.listBlocks(), b2 = dn2.listBlocks(), b3 = dn3.listBlocks();

        String victimId, survivorId, emptyId;
        InMemoryDataNode victimDn, survivorDn, emptyDn;
        DatanodeInfo victimInfo;

        if (!b1.isEmpty() && !b2.isEmpty()) {
            victimId = "dn-1"; victimDn = dn1; victimInfo = dn1Info;
            survivorId = "dn-2"; survivorDn = dn2;
            emptyId = "dn-3"; emptyDn = dn3;
        } else if (!b1.isEmpty()) {
            // b1 + b3 have the block, b2 is empty
            victimId = "dn-1"; victimDn = dn1; victimInfo = dn1Info;
            survivorId = "dn-3"; survivorDn = dn3;
            emptyId = "dn-2"; emptyDn = dn2;
        } else {
            // b2 + b3 have the block, b1 is empty
            victimId = "dn-2"; victimDn = dn2; victimInfo = dn2Info;
            survivorId = "dn-3"; survivorDn = dn3;
            emptyId = "dn-1"; emptyDn = dn1;
        }

        long blockId = victimDn.listBlocks().get(0);

        // Sleep past DEAD threshold — victim never heartbeats during this window
        Thread.sleep(DEAD_MS + 50);

        // Heartbeat survivors immediately before running the monitor so their
        // timestamps are fresh (elapsed ≈ 0) and they stay NORMAL
        nameNode.heartbeat(new HeartbeatRequest(survivorId, 0L, 10_000_000L));
        nameNode.heartbeat(new HeartbeatRequest(emptyId, 0L, 10_000_000L));

        // Run failure detector — only victim (never heartbeated) should go DEAD
        nameNode.checkAndReplicateDeadNodes();

        assertEquals(DatanodeState.DEAD, victimInfo.getState(), "Victim DN should be DEAD");

        // Heartbeat survivor — should get REPLICATE command to copy block to emptyDn
        HeartbeatResponse survResp = nameNode.heartbeat(
                new HeartbeatRequest(survivorId, 0L, 10_000_000L));

        boolean replicateQueued = survResp.commands().stream()
                .anyMatch(c -> c.action() == DatanodeCommand.Action.REPLICATE
                            && c.blockId() == blockId);
        assertTrue(replicateQueued, "Survivor DN should receive REPLICATE command");

        // Execute the re-replication command
        survivorDn.executeCommands(survResp.commands(), registry);

        // Empty DN should now hold the block
        assertTrue(emptyDn.hasBlock(blockId),
                "After re-replication, formerly-empty DN should hold the block");

        // Data is still fully readable even without the victim
        registry.remove(victimId);
        byte[] original = "resilient data".getBytes();
        byte[] readBack = client.get("/resilient.txt");
        assertArrayEquals(original, readBack, "Data must be readable after DN failure + re-replication");
    }
}
