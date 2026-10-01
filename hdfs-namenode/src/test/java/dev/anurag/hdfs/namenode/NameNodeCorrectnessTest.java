package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Block lifecycle, namespace edge cases and recovery invariants of NameNodeImpl. */
class NameNodeCorrectnessTest {

    private static final int BLOCK_SIZE = 1024;

    @TempDir
    Path dataDir;

    private static void registerThree(NameNodeImpl nn) {
        for (int i = 1; i <= 3; i++)
            nn.registerDatanode(new DatanodeInfo("dn-" + i, "localhost", 9000 + i, 1_000_000L, 1_000_000L));
    }

    /** Heartbeats every DN and returns the commands each one received. */
    private static Map<String, List<DatanodeCommand>> drain(NameNodeImpl nn, String... ids) throws IOException {
        Map<String, List<DatanodeCommand>> out = new HashMap<>();
        for (String id : ids) out.put(id, nn.heartbeat(new HeartbeatRequest(id, 0, 1_000_000L)).commands());
        return out;
    }

    private static boolean hasDelete(List<DatanodeCommand> cmds, long blockId) {
        return cmds.stream().anyMatch(c -> c.action() == DatanodeCommand.Action.DELETE && c.blockId() == blockId);
    }

    private static long writeOneBlockFile(NameNodeImpl nn, String path, short rf, long size) throws IOException {
        nn.createFile(path, rf);
        long blockId = nn.addBlock(path).blockId();
        nn.closeFile(path, size);
        return blockId;
    }

    @Test
    void deleteFile_tellsEveryReplicaToDeleteItsBlock() throws Exception {
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE);
        registerThree(nn);
        long blockId = writeOneBlockFile(nn, "/a.txt", (short) 3, 10);

        nn.delete("/a.txt");

        drain(nn, "dn-1", "dn-2", "dn-3").forEach((dn, cmds) ->
                assertTrue(hasDelete(cmds, blockId), dn + " should be told to delete block " + blockId));
        assertTrue(nn.getDatanodeReport().stream().allMatch(r -> r.numBlocks() == 0), "BlockMap forgets the block");
    }

    @Test
    void deleteDirectory_removesWholeSubtreeAndItsBlocks() throws Exception {
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE);
        registerThree(nn);
        nn.mkdir("/d");
        nn.mkdir("/d/e");
        long b1 = writeOneBlockFile(nn, "/d/a", (short) 3, 1);
        long b2 = writeOneBlockFile(nn, "/d/e/b", (short) 3, 1);

        nn.delete("/d");

        Map<String, List<DatanodeCommand>> cmds = drain(nn, "dn-1", "dn-2", "dn-3");
        assertTrue(hasDelete(cmds.get("dn-1"), b1) && hasDelete(cmds.get("dn-1"), b2));
        assertThrows(IOException.class, () -> nn.createFile("/d/e/c", (short) 3), "subtree is gone");
        // A DataNode that missed the DELETE and still reports the block gets told again (orphan)
        nn.blockReport(new BlockReportRequest("dn-1", List.of(b1)));
        assertTrue(hasDelete(drain(nn, "dn-1").get("dn-1"), b1));
    }

    @Test
    void blockReport_unknownBlock_isDeletedAsOrphan() throws Exception {
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE);
        registerThree(nn);
        nn.blockReport(new BlockReportRequest("dn-2", List.of(999L)));
        assertTrue(hasDelete(drain(nn, "dn-2").get("dn-2"), 999L));
        assertEquals(0, nn.getDatanodeReport().stream().filter(r -> r.datanodeId().equals("dn-2")).findFirst().orElseThrow().numBlocks());
    }

    @Test
    void overReplicatedBlock_extraCopyIsDeletedAndForgotten() throws Exception {
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE);
        registerThree(nn);
        nn.createFile("/f", (short) 2);
        AddBlockResponse resp = nn.addBlock("/f");
        nn.closeFile("/f", 5);
        String extra = Set.of("dn-1", "dn-2", "dn-3").stream()
                .filter(id -> resp.targets().stream().noneMatch(t -> t.datanodeId().equals(id))).findFirst().orElseThrow();

        nn.blockReport(new BlockReportRequest(extra, List.of(resp.blockId())));

        assertTrue(hasDelete(drain(nn, extra).get(extra), resp.blockId()));
        List<DatanodeDescriptor> locs = nn.getBlockLocations("/f").blocks().get(0).locations();
        assertEquals(2, locs.size(), "NameNode must not keep advertising the deleted copy");
        assertTrue(locs.stream().noneMatch(d -> d.datanodeId().equals(extra)));
    }

    @Test
    void abandonBlock_dropsBlockAndRetryHonoursExclusions() throws Exception {
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE);
        registerThree(nn);
        nn.registerDatanode(new DatanodeInfo("dn-4", "localhost", 9004, 1_000_000L, 1_000_000L));
        nn.createFile("/p", (short) 3);
        AddBlockResponse first = nn.addBlock("/p");

        nn.abandonBlock("/p", first.blockId());
        AddBlockResponse second = nn.addBlock("/p", Set.of("dn-1"));
        nn.closeFile("/p", 3);

        assertTrue(second.targets().stream().noneMatch(t -> t.datanodeId().equals("dn-1")));
        assertEquals(List.of(second.blockId()), nn.getBlockLocations("/p").blocks().stream().map(b -> b.blockId()).toList());
        String target = first.targets().get(0).datanodeId();
        assertTrue(hasDelete(drain(nn, target).get(target), first.blockId()), "partial copies are cleaned up");
    }

    @Test
    void addBlock_withNoLiveDataNodes_failsCleanly() throws Exception {
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE);
        nn.createFile("/x", (short) 3);
        IOException e = assertThrows(IOException.class, () -> nn.addBlock("/x"));
        assertTrue(e.getMessage().contains("No live DataNodes"));
    }

    @Test
    void namespaceEdgeCases_areRejectedWithIOException() throws Exception {
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE);
        nn.mkdir("/a");
        nn.createFile("/a/f", (short) 1);
        nn.closeFile("/a/f", 0);

        assertThrows(IOException.class, () -> nn.listDir("/a/f"), "listDir on a file");
        assertThrows(IOException.class, () -> nn.delete("/"), "delete root");
        assertThrows(IOException.class, () -> nn.rename("/a", "/a/b"), "move a directory into itself");
        assertThrows(IOException.class, () -> nn.mkdir("relative"), "relative path");
        assertThrows(IOException.class, () -> nn.mkdir("/bad\tname"), "tab would corrupt the edit log");
    }

    @Test
    void moveIntoDirectory_survivesCheckpointAndRestart() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.mkdir("/docs");
        nn1.createFile("/a.txt", (short) 1);
        nn1.closeFile("/a.txt", 0);
        nn1.rename("/a.txt", "/docs/a.txt");
        nn1.saveNamespace(); // fsimage records each node's parent — must be the new one
        nn1.close();

        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        assertEquals(List.of("docs"), nn2.listDir("/").stream().map(INodeInfo::name).toList());
        assertEquals(List.of("a.txt"), nn2.listDir("/docs").stream().map(INodeInfo::name).toList());
    }

    @Test
    void checkpointWhileFileIsBeingWritten_fileSurvivesRestart() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        registerThree(nn1);
        nn1.createFile("/w", (short) 1);
        long blockId = nn1.addBlock("/w").blockId();
        nn1.saveNamespace();       // file is under construction at checkpoint time
        nn1.closeFile("/w", 7);    // completed after the checkpoint (only in the edit log)
        nn1.close();

        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        registerThree(nn2);
        nn2.blockReport(new BlockReportRequest("dn-1", List.of(blockId)));
        assertEquals(7, nn2.getBlockLocations("/w").fileSize());
    }

    @Test
    void discardedUnderConstructionFile_doesNotMergeWithLaterFileAtSamePath() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        nn1.createFile("/x", (short) 1); // writer "crashes": never closed
        nn1.close();

        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir); // discards /x (and logs it)
        registerThree(nn2);
        long blockId = writeOneBlockFile(nn2, "/x", (short) 1, 5);
        nn2.close();

        NameNodeImpl nn3 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        registerThree(nn3);
        nn3.blockReport(new BlockReportRequest("dn-1", List.of(blockId)));
        var lb = nn3.getBlockLocations("/x");
        assertEquals(5, lb.fileSize());
        assertEquals(1, lb.blocks().size());
    }

    @Test
    void clusterId_isStableAcrossRestarts() throws Exception {
        NameNodeImpl nn1 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        String id = nn1.getClusterId();
        nn1.close();
        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, dataDir);
        NameNodeImpl other = new NameNodeImpl(BLOCK_SIZE, dataDir.resolve("other"));
        assertEquals(id, nn2.getClusterId());
        assertNotEquals(id, other.getClusterId());
        nn2.close();
        other.close();
    }
}
