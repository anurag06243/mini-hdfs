package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.datanode.InMemoryDataNode;
import dev.anurag.hdfs.datanode.IDataNodeStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 7: edit log + fsimage restart recovery.
 *
 * Pattern for each test:
 *   1. Create NameNodeImpl with a temp dataDir
 *   2. Perform operations (write files, mkdir, rename, …)
 *   3. saveNamespace() or just let the edit log accumulate
 *   4. Simulate restart: create a NEW NameNodeImpl pointing at the same dataDir
 *   5. Re-register DataNodes + send block reports to rebuild BlockMap
 *   6. Assert namespace and data are intact
 */
class NameNodePersistenceTest {

    private static final int BLOCK_SIZE = 1024 * 1024; // 1 MB

    @TempDir
    Path dataDir;

    private InMemoryDataNode dn1, dn2, dn3;
    private Map<String, IDataNodeStorage> registry;

    @BeforeEach
    void setUpDataNodes() {
        dn1 = new InMemoryDataNode("dn-1");
        dn2 = new InMemoryDataNode("dn-2");
        dn3 = new InMemoryDataNode("dn-3");
        registry = Map.of("dn-1", dn1, "dn-2", dn2, "dn-3", dn3);
    }

    // -------------------------------------------------------------------------
    // Helper: write a tiny file via NameNode directly (no HdfsClient)
    // -------------------------------------------------------------------------

    private void writeFile(NameNodeImpl nn, String path, byte[] data) throws Exception {
        nn.createFile(path, (short) 3);
        var resp = nn.addBlock(path);
        // Write to the DN storages (simulating what HdfsClient + TcpBlockWriter do)
        for (var dn : resp.targets()) {
            IDataNodeStorage storage = registry.get(dn.datanodeId());
            if (storage != null) storage.writeBlock(resp.blockId(), data);
        }
        nn.closeFile(path, data.length);
    }

    // Helper: register all 3 DNs and send full block reports to rebuild BlockMap
    private void reregisterAndReport(NameNodeImpl nn) throws Exception {
        nn.registerDatanode(new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L));
        nn.registerDatanode(new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L));
        nn.registerDatanode(new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L));
        nn.blockReport(new BlockReportRequest("dn-1", dn1.listBlocks()));
        nn.blockReport(new BlockReportRequest("dn-2", dn2.listBlocks()));
        nn.blockReport(new BlockReportRequest("dn-3", dn3.listBlocks()));
    }

    // -------------------------------------------------------------------------
    // 1. Namespace survives restart after saveNamespace (fsimage + truncated log)
    // -------------------------------------------------------------------------

    @Test
    void fsimage_namespaceSurvivesRestart() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.registerDatanode(new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L));

        nn1.mkdir("/logs");
        writeFile(nn1, "/logs/day1.txt", "hello world".getBytes());
        writeFile(nn1, "/report.bin",    "data".getBytes());

        nn1.saveNamespace();

        // Restart
        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        reregisterAndReport(nn2);

        // Directories survive
        List<INodeInfo> root = nn2.listDir("/");
        assertTrue(root.stream().anyMatch(n -> n.name().equals("logs") && n.isDirectory()),
                "/logs should exist after restart");
        assertTrue(root.stream().anyMatch(n -> n.name().equals("report.bin")),
                "/report.bin should exist after restart");

        // File is readable (block locations populated from block reports)
        LocatedBlocks lb = nn2.getBlockLocations("/logs/day1.txt");
        assertFalse(lb.blocks().isEmpty(), "Block locations should be non-empty after block reports");
        assertEquals(11L, lb.fileSize(), "File size should be preserved");
    }

    // -------------------------------------------------------------------------
    // 2. Edit log replay: ops between checkpoints survive without saveNamespace
    // -------------------------------------------------------------------------

    @Test
    void editLog_replayWithoutExplicitSave() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.registerDatanode(new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L));

        nn1.mkdir("/archive");
        writeFile(nn1, "/archive/data.bin", "persistent".getBytes());
        // NO saveNamespace — only the edit log persists this

        // Restart (loads empty fsimage + replays edit log)
        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        reregisterAndReport(nn2);

        List<INodeInfo> archive = nn2.listDir("/archive");
        assertEquals(1, archive.size(), "/archive should have 1 file after edit log replay");
        assertEquals("data.bin", archive.get(0).name());

        LocatedBlocks lb = nn2.getBlockLocations("/archive/data.bin");
        assertEquals(10L, lb.fileSize());
    }

    // -------------------------------------------------------------------------
    // 3. Rename survives restart
    // -------------------------------------------------------------------------

    @Test
    void rename_survivesRestart() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.registerDatanode(new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L));

        writeFile(nn1, "/old.txt", "rename me".getBytes());
        nn1.rename("/old.txt", "/new.txt");

        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        reregisterAndReport(nn2);

        assertThrows(Exception.class, () -> nn2.getBlockLocations("/old.txt"),
                "/old.txt should not exist after rename + restart");
        LocatedBlocks lb = nn2.getBlockLocations("/new.txt");
        assertEquals(9L, lb.fileSize(), "/new.txt should be accessible with correct size");
    }

    // -------------------------------------------------------------------------
    // 4. Delete survives restart
    // -------------------------------------------------------------------------

    @Test
    void delete_survivesRestart() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.registerDatanode(new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L));

        writeFile(nn1, "/temp.txt", "delete me".getBytes());
        writeFile(nn1, "/keep.txt", "keep me".getBytes());
        nn1.delete("/temp.txt");

        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        reregisterAndReport(nn2);

        assertThrows(Exception.class, () -> nn2.getBlockLocations("/temp.txt"),
                "/temp.txt should be gone after delete + restart");
        assertDoesNotThrow(() -> nn2.getBlockLocations("/keep.txt"),
                "/keep.txt should still exist");
    }

    // -------------------------------------------------------------------------
    // 5. Under-construction file is discarded on restart (simplified lease recovery)
    // -------------------------------------------------------------------------

    @Test
    void underConstructionFile_discardedOnRestart() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.registerDatanode(new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L));

        writeFile(nn1, "/committed.txt", "safe".getBytes());

        // Simulate crash mid-write: createFile + addBlock logged, but no closeFile
        nn1.createFile("/crashed.txt", (short) 3);
        nn1.addBlock("/crashed.txt");
        // No closeFile — file stays under construction

        // Restart
        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        reregisterAndReport(nn2);

        // Committed file survives
        assertDoesNotThrow(() -> nn2.getBlockLocations("/committed.txt"));
        // Crashed file is gone (lease recovery discarded it)
        assertThrows(Exception.class, () -> nn2.getBlockLocations("/crashed.txt"),
                "Under-construction file should be discarded on restart");
    }

    // -------------------------------------------------------------------------
    // 6. Block ID and inode ID generators do not regress after restart
    //    (no ID collisions when writing new files after recovery)
    // -------------------------------------------------------------------------

    @Test
    void idGenerators_noRegressionAfterRestart() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.registerDatanode(new DatanodeInfo("dn-1", "localhost", 9001, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-2", "localhost", 9002, 10_000_000L, 10_000_000L));
        nn1.registerDatanode(new DatanodeInfo("dn-3", "localhost", 9003, 10_000_000L, 10_000_000L));

        writeFile(nn1, "/file1.txt", "first".getBytes());
        writeFile(nn1, "/file2.txt", "second".getBytes());

        // Restart: the NN stays read-only (safe mode) until the block reports arrive
        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        assertTrue(nn2.isInSafeMode(), "Restarted NN with existing blocks starts in safe mode");
        assertThrows(Exception.class, () -> nn2.mkdir("/blocked"), "Mutations are refused in safe mode");
        reregisterAndReport(nn2);
        assertFalse(nn2.isInSafeMode(), "All blocks reported -> safe mode exits");

        // Writing a new file after restart should not throw "already exists"
        // (which would happen if inode IDs collide with existing ones)
        assertDoesNotThrow(() -> writeFile(nn2, "/file3.txt", "third".getBytes()),
                "New file after restart should not collide with existing IDs");

        // All three files exist
        List<INodeInfo> root = nn2.listDir("/");
        assertEquals(3, root.size(), "Root should have 3 files after restart");
    }
}
