package dev.anurag.hdfs.datanode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DiskDataNodeStorageTest {

    @TempDir
    Path dir;

    @Test
    void blocksSurviveProcessRestart() throws Exception {
        DiskDataNodeStorage s1 = new DiskDataNodeStorage("dn-1", dir);
        s1.writeBlock(7, "seven".getBytes());
        s1.writeBlock(8, new byte[0]);

        DiskDataNodeStorage s2 = new DiskDataNodeStorage("dn-1", dir);
        assertEquals(List.of(7L, 8L), s2.listBlocks().stream().sorted().toList());
        assertArrayEquals("seven".getBytes(), s2.readBlock(7));
        assertArrayEquals(new byte[0], s2.readBlock(8));
        assertEquals(5, s2.usedBytes());
    }

    @Test
    void corruptBlock_isDetectedQuarantinedAndReported() throws Exception {
        DiskDataNodeStorage s = new DiskDataNodeStorage("dn-1", dir);
        s.writeBlock(1, "important data".getBytes());
        byte[] onDisk = Files.readAllBytes(s.dataFile(1));
        onDisk[0] ^= 0x01; // flip one bit
        Files.write(s.dataFile(1), onDisk);

        assertThrows(CorruptBlockException.class, () -> s.readBlock(1));
        assertFalse(s.listBlocks().contains(1L), "corrupt replica must drop out of block reports");
        assertTrue(Files.exists(dir.resolve("corrupt").resolve("blk_1")), "kept for inspection");
        assertTrue(s.consumeBlockReportRequest(), "DataNode should send a report right away");
        assertFalse(s.consumeBlockReportRequest());
    }

    @Test
    void halfWrittenBlockFromACrash_isIgnoredAndCleanedUp() throws Exception {
        Path current = dir.resolve("current");
        Files.createDirectories(current);
        Files.write(current.resolve("blk_5.tmp"), new byte[]{1, 2, 3});
        Files.write(current.resolve("blk_6"), new byte[]{1}); // data without meta: never completed

        DiskDataNodeStorage s = new DiskDataNodeStorage("dn-1", dir);
        assertTrue(s.listBlocks().isEmpty());
        assertFalse(Files.exists(current.resolve("blk_5.tmp")));
    }

    @Test
    void deleteBlock_removesBothFiles() throws Exception {
        DiskDataNodeStorage s = new DiskDataNodeStorage("dn-1", dir);
        s.writeBlock(3, "x".getBytes());
        s.deleteBlock(3);
        assertFalse(s.hasBlock(3));
        try (var files = Files.list(dir.resolve("current"))) {
            assertEquals(0, files.count());
        }
    }
}
