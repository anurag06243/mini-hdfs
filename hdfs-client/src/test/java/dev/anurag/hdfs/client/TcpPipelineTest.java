package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.datanode.InMemoryDataNode;
import dev.anurag.hdfs.datanode.TcpDataNodeServer;
import dev.anurag.hdfs.namenode.NameNodeImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6: real TCP sockets — client → DN₁ → DN₂ → DN₃ pipeline write,
 * and direct TCP read back from any DN.
 */
class TcpPipelineTest {

    private static final int BLOCK_SIZE = 256 * 1024; // 256 KB

    private NameNodeImpl nameNode;
    private List<InMemoryDataNode> storages;
    private List<TcpDataNodeServer> servers;
    private HdfsClient client;

    @BeforeEach
    void setUp() throws Exception {
        nameNode = new NameNodeImpl(BLOCK_SIZE);
        storages = new ArrayList<>();
        servers  = new ArrayList<>();

        // Start 3 DataNode servers on OS-assigned ports (port 0)
        for (int i = 1; i <= 3; i++) {
            InMemoryDataNode storage = new InMemoryDataNode("dn-" + i);
            TcpDataNodeServer server = new TcpDataNodeServer(storage, 0); // port 0 = OS picks
            server.start();

            storages.add(storage);
            servers.add(server);

            // Register with NameNode using the actual bound port
            nameNode.registerDatanode(new DatanodeInfo(
                    "dn-" + i, "127.0.0.1", server.getPort(),
                    100_000_000L, 100_000_000L));
        }

        // TCP-mode client: no in-memory registry
        client = new HdfsClient(nameNode, BLOCK_SIZE, (short) 3);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (TcpDataNodeServer s : servers) s.close();
    }

    // -------------------------------------------------------------------------
    // Single-block write and read over TCP
    // -------------------------------------------------------------------------

    @Test
    void tcpWriteAndRead_singleBlock() throws Exception {
        byte[] original = "Hello, TCP HDFS!".getBytes();
        client.put(original, "/hello.txt");

        byte[] readBack = client.get("/hello.txt");
        assertArrayEquals(original, readBack, "Round-trip over TCP should return identical bytes");
    }

    // -------------------------------------------------------------------------
    // Multi-block file: pipeline chains correctly for every block
    // -------------------------------------------------------------------------

    @Test
    void tcpWriteAndRead_multiBlock() throws Exception {
        // 3 blocks worth of data
        byte[] original = new byte[3 * BLOCK_SIZE + 1000];
        new Random(99).nextBytes(original);

        client.put(original, "/big.bin");

        byte[] readBack = client.get("/big.bin");
        assertArrayEquals(original, readBack, "Multi-block TCP round-trip should be identical");
    }

    // -------------------------------------------------------------------------
    // All 3 DataNodes hold every block after a pipeline write (RF=3)
    // -------------------------------------------------------------------------

    @Test
    void pipelineWrite_allReplicasHoldBlock() throws Exception {
        byte[] data = "check replicas".getBytes();
        client.put(data, "/replicas.txt");

        // Every DN storage should hold block-1
        for (int i = 0; i < 3; i++) {
            assertFalse(storages.get(i).listBlocks().isEmpty(),
                    "DN-" + (i + 1) + " should hold at least one block after pipeline write");
        }
    }

    // -------------------------------------------------------------------------
    // Read fallback: if first DN in locations is down, read from second
    // -------------------------------------------------------------------------

    @Test
    void tcpRead_fallbackOnFirstDnDown() throws Exception {
        byte[] original = "fallback test".getBytes();
        client.put(original, "/fallback.txt");

        // Close the first server (port closed = connection refused)
        servers.get(0).close();

        // Should still succeed by falling back to DN-2 or DN-3
        byte[] readBack = client.get("/fallback.txt");
        assertArrayEquals(original, readBack, "Read should fall back to a live replica");
    }
}
