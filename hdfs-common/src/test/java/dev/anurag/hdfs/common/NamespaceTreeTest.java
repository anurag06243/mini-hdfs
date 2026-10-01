package dev.anurag.hdfs.common;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import dev.anurag.hdfs.common.model.DirectoryNode;
import dev.anurag.hdfs.common.model.FileNode;

public class NamespaceTreeTest {

    @Test
    void namespaceTreeworks() {
        DirectoryNode root = new DirectoryNode(1, null, null);
        DirectoryNode logs = new DirectoryNode(2, "logs", root);
        FileNode day1 = new FileNode(3, "day1.txt", logs, (short) 3);
        root.addChild(logs);
        logs.addChild(day1);
        day1.addBlockId(101L);
        day1.addBlockId(102L);

        assertInstanceOf(DirectoryNode.class, root.getChild("logs"));
        assertInstanceOf(FileNode.class, logs.getChild("day1.txt"));
        assertEquals(2, day1.getBlockIds().size());
        assertTrue(root.isDirectory());
        assertFalse(day1.isDirectory());

    }

    /*
     * 1. Create root DirectoryNode (id=1, name="", parent=null)
     * 2. Create DirectoryNode "logs" (id=2, parent=root), add to root
     * 3. Create FileNode "day1.txt" (id=3, parent=logs, rf=3), add to logs
     * 4. Add two blockIds to the FileNode (e.g. 101L, 102L)
     * 5. Assert: root.getChild("logs") is a DirectoryNode
     * 6. Assert: logs.getChild("day1.txt") is a FileNode
     * 7. Assert: the FileNode has exactly 2 blockIds
     * 8. Assert: isDirectory() returns correct values for each node
     */
}
