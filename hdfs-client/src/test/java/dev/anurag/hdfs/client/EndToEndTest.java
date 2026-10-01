package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.datanode.IDataNodeStorage;
import dev.anurag.hdfs.datanode.InMemoryDataNode;
import dev.anurag.hdfs.namenode.NameNodeImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class EndToEndTest {

    static final int BLOCK_SIZE = 1024 * 1024; // 1 MB — small for fast tests

    private NameNodeImpl nameNode;
    private InMemoryDataNode dn1, dn2, dn3;
    private Map<String, IDataNodeStorage> registry;
    private HdfsClient client;

    @BeforeEach
    void setup() {
        nameNode = new NameNodeImpl(BLOCK_SIZE);

        dn1 = new InMemoryDataNode("dn-1");
        dn2 = new InMemoryDataNode("dn-2");
        dn3 = new InMemoryDataNode("dn-3");

        nameNode.registerDatanode(new DatanodeInfo("dn-1", "localhost", 50010, 10_000_000_000L, 10_000_000_000L));
        nameNode.registerDatanode(new DatanodeInfo("dn-2", "localhost", 50011, 10_000_000_000L, 10_000_000_000L));
        nameNode.registerDatanode(new DatanodeInfo("dn-3", "localhost", 50012, 10_000_000_000L, 10_000_000_000L));

        registry = new HashMap<>();
        registry.put("dn-1", dn1);
        registry.put("dn-2", dn2);
        registry.put("dn-3", dn3);

        client = new HdfsClient(nameNode, registry, BLOCK_SIZE, (short) 3);
    }

    // Test 1: write a small file, read it back, bytes must match exactly
    @Test
    void basicWriteAndRead() throws Exception {
        byte[] original = "Hello, HDFS!".getBytes();

        client.put(original, "/hello.txt");
        byte[] result = client.get("/hello.txt");

        assertArrayEquals(original, result, "Read bytes must equal written bytes");
    }

    // Test 2: 5 MB file → 5 blocks (at 1MB block size); reassembled bytes match
    @Test
    void multiBlockFile() throws Exception {
        byte[] original = new byte[5 * BLOCK_SIZE];
        new Random(42).nextBytes(original); // deterministic random data

        client.put(original, "/big.txt");
        byte[] result = client.get("/big.txt");

        assertArrayEquals(original, result, "Multi-block file must reassemble correctly");
    }

    // Test 3: one DataNode removed from registry → read still succeeds via other replicas
    @Test
    void faultToleranceOneDataNodeDown() throws Exception {
        byte[] original = "Fault tolerant read".getBytes();

        client.put(original, "/fault.txt");

        // Simulate DN1 failure: remove it from the client's registry
        // NameNode still thinks DN1 is alive (no heartbeat detection yet — Phase 5)
        // but readWithFallback will skip it and try DN2 or DN3
        registry.remove("dn-1");

        byte[] result = client.get("/fault.txt");

        assertArrayEquals(original, result, "Read must succeed even with one replica unavailable");
    }

    // Test 4: namespace ops — mkdir, ls, rename, delete
    @Test
    void namespaceOperations() throws Exception {
        nameNode.mkdir("/logs");
        byte[] data = "day one data".getBytes();

        client.put(data, "/logs/day1.txt");
        client.put(data, "/logs/day2.txt");

        var entries = nameNode.listDir("/logs");
        assertEquals(2, entries.size(), "Should list 2 files in /logs");

        nameNode.rename("/logs/day1.txt", "/logs/day1-archived.txt");
        byte[] renamedResult = client.get("/logs/day1-archived.txt");
        assertArrayEquals(data, renamedResult, "Renamed file must still be readable");

        nameNode.delete("/logs/day2.txt");
        assertThrows(Exception.class, () -> client.get("/logs/day2.txt"),
                "Deleted file must not be readable");
    }
}
