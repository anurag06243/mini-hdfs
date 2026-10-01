package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.DatanodeReport;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.model.DatanodeState;
import dev.anurag.hdfs.common.rpc.NameNodeRpcClient;
import dev.anurag.hdfs.datanode.BlockTransferClient;
import dev.anurag.hdfs.datanode.DataNode;
import dev.anurag.hdfs.datanode.DiskDataNodeStorage;
import dev.anurag.hdfs.namenode.NameNodeImpl;
import dev.anurag.hdfs.namenode.NameNodeServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whole-cluster tests over real sockets: NameNode RPC server, four DataNodes with on-disk
 * storage and their own heartbeat threads, and a client that only knows the NameNode's address.
 * Timings are shortened (100 ms heartbeats, DEAD after 1 s) so failures play out quickly.
 */
class ClusterIntegrationTest {

    private static final int BLOCK_SIZE = 64 * 1024;
    private static final long STALE_MS = 400;
    private static final long DEAD_MS = 1_000;

    @TempDir
    Path root;

    private NameNodeServer nnServer;
    private NameNodeImpl nn;
    private int nnPort;
    private final Map<String, DataNode> dataNodes = new LinkedHashMap<>();
    private final Map<String, DiskDataNodeStorage> storages = new LinkedHashMap<>();
    private final List<NameNodeRpcClient> proxies = new ArrayList<>();
    private HdfsClient client;

    @BeforeEach
    void startCluster() throws Exception {
        startNameNode(0);
        for (int i = 1; i <= 4; i++) startDataNode("dn-" + i);
        NameNodeRpcClient clientProxy = proxy();
        client = new HdfsClient(clientProxy, BLOCK_SIZE, (short) 3);
        waitFor("4 live DataNodes", () -> liveDataNodes() == 4);
    }

    @AfterEach
    void stopCluster() throws Exception {
        for (DataNode dn : dataNodes.values()) dn.close();
        proxies.forEach(NameNodeRpcClient::close);
        nnServer.close();
    }

    private void startNameNode(int port) throws Exception {
        nn = new NameNodeImpl(BLOCK_SIZE, STALE_MS, DEAD_MS, root.resolve("namenode"));
        nnServer = new NameNodeServer(nn, port, 100, 1_000);
        nnServer.start();
        nnPort = nnServer.getPort();
    }

    private void startDataNode(String id) throws Exception {
        Path dir = root.resolve(id);
        DiskDataNodeStorage storage = new DiskDataNodeStorage(id, dir);
        DataNode dn = new DataNode(id, "127.0.0.1", 0, storage, proxy(), dir.resolve("CLUSTER_ID"), 100, 2_000);
        dn.start();
        dataNodes.put(id, dn);
        storages.put(id, storage);
    }

    private NameNodeRpcClient proxy() {
        NameNodeRpcClient p = new NameNodeRpcClient("127.0.0.1", nnPort);
        proxies.add(p);
        return p;
    }

    private long liveDataNodes() {
        try {
            return client == null ? proxy().getDatanodeReport().stream().filter(r -> r.state() == DatanodeState.NORMAL).count()
                    : client.nameNode().getDatanodeReport().stream().filter(r -> r.state() == DatanodeState.NORMAL).count();
        } catch (Exception e) {
            return -1;
        }
    }

    /** DataNodes that physically hold the block on disk, excluding stopped ones. */
    private List<String> holders(long blockId) {
        return storages.entrySet().stream()
                .filter(e -> dataNodes.containsKey(e.getKey()) && e.getValue().hasBlock(blockId))
                .map(Map.Entry::getKey).toList();
    }

    private void kill(String id) throws Exception {
        dataNodes.remove(id).close();
    }

    private static void waitFor(String what, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) fail("Timed out waiting for: " + what);
            Thread.sleep(50);
        }
    }

    private static byte[] randomBytes(int n, long seed) {
        byte[] b = new byte[n];
        new Random(seed).nextBytes(b);
        return b;
    }

    private List<LocatedBlock> blocksOf(String path) throws Exception {
        return client.nameNode().getBlockLocations(path).blocks();
    }

    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void writeAndRead_multiBlockFile_threeReplicasOnDisk() throws Exception {
        byte[] data = randomBytes(3 * BLOCK_SIZE + 123, 1);
        client.put(data, "/big.bin");

        assertArrayEquals(data, client.get("/big.bin"));
        List<LocatedBlock> blocks = blocksOf("/big.bin");
        assertEquals(4, blocks.size());
        for (LocatedBlock b : blocks) {
            assertEquals(3, b.locations().size());
            assertEquals(3, holders(b.blockId()).size(), "block " + b.blockId() + " stored on 3 disks");
        }
    }

    @Test
    void dataNodeCrash_isDetectedAndBlocksAreReReplicatedOverTheNetwork() throws Exception {
        byte[] data = randomBytes(10_000, 2);
        client.put(data, "/f");
        long blockId = blocksOf("/f").get(0).blockId();
        String victim = holders(blockId).get(0);

        kill(victim);

        waitFor(victim + " marked DEAD", () -> {
            try {
                return client.nameNode().getDatanodeReport().stream()
                        .anyMatch(r -> r.datanodeId().equals(victim) && r.state() == DatanodeState.DEAD);
            } catch (Exception e) { return false; }
        });
        waitFor("block back to 3 live replicas", () -> holders(blockId).size() == 3);
        assertFalse(holders(blockId).contains(victim));
        assertArrayEquals(data, client.get("/f"));
    }

    @Test
    void nameNodeRestart_recoversNamespace_andDataNodesRejoinOnTheirOwn() throws Exception {
        byte[] data = randomBytes(BLOCK_SIZE + 10, 3);
        client.nameNode().mkdir("/logs");
        client.put(data, "/logs/app.log");
        client.nameNode().rename("/logs/app.log", "/logs/app-1.log");

        nnServer.close();                 // NameNode process dies; DataNodes keep retrying
        Thread.sleep(300);
        startNameNode(nnPort);            // new process, same metadata directory and port
        assertTrue(nn.isInSafeMode(), "restarted NameNode waits for block reports");

        waitFor("DataNodes re-register and NN leaves safe mode", () -> !nn.isInSafeMode() && liveDataNodes() == 4);
        assertArrayEquals(data, client.get("/logs/app-1.log"));
        client.put("after restart".getBytes(), "/logs/app-2.log");
        assertArrayEquals("after restart".getBytes(), client.get("/logs/app-2.log"));
    }

    @Test
    void delete_freesBlocksOnEveryDataNode() throws Exception {
        client.put(randomBytes(2 * BLOCK_SIZE, 4), "/tmp.bin");
        List<Long> blockIds = blocksOf("/tmp.bin").stream().map(LocatedBlock::blockId).toList();
        assertFalse(holders(blockIds.get(0)).isEmpty());

        client.nameNode().delete("/tmp.bin");

        waitFor("blocks deleted from disk", () -> blockIds.stream().allMatch(b -> holders(b).isEmpty()));
    }

    @Test
    void corruptReplica_isDetectedOnRead_andRepairedFromAHealthyCopy() throws Exception {
        byte[] data = randomBytes(20_000, 5);
        client.put(data, "/precious");
        LocatedBlock block = blocksOf("/precious").get(0);
        String bad = holders(block.blockId()).get(0);
        Path file = storages.get(bad).dataFile(block.blockId());
        byte[] onDisk = Files.readAllBytes(file);
        onDisk[100] ^= 0x7F; // bit rot
        Files.write(file, onDisk);

        var badDescriptor = block.locations().stream().filter(d -> d.datanodeId().equals(bad)).findFirst().orElseThrow();
        assertThrows(Exception.class, () -> BlockTransferClient.readBlock(block.blockId(), badDescriptor),
                "the corrupt replica is refused, never served");
        assertArrayEquals(data, client.get("/precious"), "client falls back to a healthy replica");

        waitFor("corrupt replica replaced: 3 healthy copies again", () -> holders(block.blockId()).size() == 3);
        for (String id : holders(block.blockId()))
            assertArrayEquals(data, storages.get(id).readBlock(block.blockId()), id + " holds a good copy");
    }

    @Test
    void blockScanner_findsSilentCorruptionWithoutAnyRead() throws Exception {
        byte[] data = randomBytes(5_000, 7);
        client.put(data, "/cold");
        long blockId = blocksOf("/cold").get(0).blockId();
        String bad = holders(blockId).get(0);
        Path file = storages.get(bad).dataFile(blockId);
        byte[] onDisk = Files.readAllBytes(file);
        onDisk[0] ^= 0x01;
        Files.write(file, onDisk);

        dataNodes.get(bad).startBlockScanner(200);

        waitFor("scanner quarantines the bad copy", () -> Files.exists(root.resolve(bad).resolve("corrupt").resolve("blk_" + blockId)));
        waitFor("re-replicated back to 3 healthy copies", () -> holders(blockId).size() == 3
                && holders(blockId).stream().allMatch(id -> {
                    try { return java.util.Arrays.equals(data, storages.get(id).readBlock(blockId)); }
                    catch (Exception e) { return false; }
                }));
    }

    @Test
    void pipelineFailure_clientExcludesDeadNodeAndRetries() throws Exception {
        String victim = "dn-4";
        kill(victim); // the NameNode still believes dn-4 is healthy for up to 1 s

        byte[] data = randomBytes(20 * BLOCK_SIZE, 6); // 20 blocks: dn-4 is almost surely picked at least once
        client.put(data, "/resilient.bin");

        assertArrayEquals(data, client.get("/resilient.bin"));
        for (LocatedBlock b : blocksOf("/resilient.bin")) {
            assertEquals(3, holders(b.blockId()).size());
            assertTrue(b.locations().stream().noneMatch(d -> d.datanodeId().equals(victim)));
        }
    }

    @Test
    void dataNodeFromAnotherCluster_refusesToJoin() throws Exception {
        Path dir = root.resolve("intruder");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("CLUSTER_ID"), "some-other-cluster");
        DiskDataNodeStorage storage = new DiskDataNodeStorage("intruder", dir);
        storage.writeBlock(1, "not yours".getBytes());
        try (DataNode intruder = new DataNode("intruder", "127.0.0.1", 0, storage, proxy(),
                dir.resolve("CLUSTER_ID"), 100, 2_000)) {
            intruder.start();
            Thread.sleep(500);
            List<DatanodeReport> report = client.nameNode().getDatanodeReport();
            assertTrue(report.stream().noneMatch(r -> r.datanodeId().equals("intruder")),
                    "it never registers, so it is never chosen as a write target");
            assertTrue(storage.hasBlock(1), "and its blocks are never deleted as orphans");
        }
    }
}
