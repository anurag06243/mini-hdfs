package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.model.DirectoryNode;
import dev.anurag.hdfs.common.model.FileNode;
import dev.anurag.hdfs.common.model.INode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serialises and deserialises the NameNode namespace tree.
 *
 * Format — one record per line, fields separated by TAB:
 *
 *   NEXT_INODE_ID  <n>
 *   NEXT_BLOCK_ID  <n>
 *   DIR   <id>  <name>  <parentId>          (parentId = -1 for root)
 *   FILE  <id>  <name>  <parentId>  <rf>  <fileSize>  <blockIds>
 *
 * Only committed (not-under-construction) files are saved.
 * BlockMap is NOT saved — it is rebuilt from DN block reports after restart.
 */
public final class FsImage {

    private FsImage() {}

    // -------------------------------------------------------------------------
    // Save
    // -------------------------------------------------------------------------

    public static void save(DirectoryNode root, long nextInodeId, long nextBlockId, Path target)
            throws IOException {
        // Write to a temp file first, then atomic rename — safe against crash mid-write
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (BufferedWriter w = Files.newBufferedWriter(tmp)) {
            w.write("NEXT_INODE_ID\t" + nextInodeId); w.newLine();
            w.write("NEXT_BLOCK_ID\t" + nextBlockId); w.newLine();
            saveNode(root, w);
        }
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void saveNode(INode node, BufferedWriter w) throws IOException {
        long parentId = (node.getParent() == null) ? -1L : node.getParent().getId();

        if (node instanceof DirectoryNode dir) {
            w.write("DIR\t" + node.getId() + "\t" + node.getName() + "\t" + parentId);
            w.newLine();
            for (INode child : dir.listChildren()) saveNode(child, w);

        } else if (node instanceof FileNode fn && !fn.isUnderConstruction()) {
            String blocks = fn.getBlockIds().isEmpty() ? ""
                    : fn.getBlockIds().stream().map(String::valueOf).collect(Collectors.joining(","));
            w.write("FILE\t" + node.getId() + "\t" + node.getName() + "\t" + parentId
                    + "\t" + fn.getReplicationFactor() + "\t" + fn.getFileSize() + "\t" + blocks);
            w.newLine();
        }
        // Under-construction files are deliberately skipped (simplified lease recovery)
    }

    // -------------------------------------------------------------------------
    // Load
    // -------------------------------------------------------------------------

    public record LoadResult(DirectoryNode root, Map<Long, INode> inodeMap,
                             long nextInodeId, long nextBlockId) {}

    public static LoadResult load(Path source) throws IOException {
        Map<Long, INode> inodeMap = new HashMap<>();
        DirectoryNode root = null;
        long nextInodeId = 1, nextBlockId = 1;

        for (String line : Files.readAllLines(source)) {
            if (line.isBlank()) continue;
            String[] p = line.split("\t");
            switch (p[0]) {
                case "NEXT_INODE_ID" -> nextInodeId = Long.parseLong(p[1]);
                case "NEXT_BLOCK_ID" -> nextBlockId = Long.parseLong(p[1]);
                case "DIR" -> {
                    long id       = Long.parseLong(p[1]);
                    String name   = p[2];
                    long parentId = Long.parseLong(p[3]);
                    INode parent  = (parentId == -1) ? null : inodeMap.get(parentId);
                    DirectoryNode dir = new DirectoryNode(id, name, parent);
                    inodeMap.put(id, dir);
                    if (parent instanceof DirectoryNode pd) pd.addChild(dir);
                    if (parentId == -1) root = dir;
                }
                case "FILE" -> {
                    long id       = Long.parseLong(p[1]);
                    String name   = p[2];
                    long parentId = Long.parseLong(p[3]);
                    short rf      = Short.parseShort(p[4]);
                    long fileSize = Long.parseLong(p[5]);
                    String blocks = p.length > 6 ? p[6] : "";
                    INode parent  = inodeMap.get(parentId);
                    FileNode fn   = new FileNode(id, name, parent, rf);
                    fn.setFileSize(fileSize);
                    fn.setUnderConstruction(false);
                    if (!blocks.isEmpty()) {
                        for (String bid : blocks.split(","))
                            fn.addBlockId(Long.parseLong(bid));
                    }
                    inodeMap.put(id, fn);
                    if (parent instanceof DirectoryNode pd) pd.addChild(fn);
                }
            }
        }
        return new LoadResult(root, inodeMap, nextInodeId, nextBlockId);
    }
}
