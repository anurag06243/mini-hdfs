package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.model.DirectoryNode;
import dev.anurag.hdfs.common.model.FileNode;
import dev.anurag.hdfs.common.model.INode;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
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
 *   DIR   <id>  <name>  <parentId>                                  (parentId = -1 for root)
 *   FILE  <id>  <name>  <parentId>  <rf>  <fileSize>  <uc 0|1>  <blockIds comma-separated>
 *
 * Parents are always written before children, so loading is a single pass.
 * Under-construction files are saved too: a checkpoint can happen mid-write, and the edit
 * log entries that complete the file come after it. The NameNode discards files that are
 * still under construction once replay finishes.
 * The BlockMap is NOT saved — it is rebuilt from DataNode block reports after restart.
 */
public final class FsImage {

    private FsImage() {}

    public static void save(DirectoryNode root, long nextInodeId, long nextBlockId, Path target)
            throws IOException {
        // Write + fsync a temp file, then atomically rename it over the old image
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp.toFile());
             BufferedWriter w = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8))) {
            w.write("NEXT_INODE_ID\t" + nextInodeId); w.newLine();
            w.write("NEXT_BLOCK_ID\t" + nextBlockId); w.newLine();
            saveNode(root, w);
            w.flush();
            fos.getChannel().force(true);
        }
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void saveNode(INode node, BufferedWriter w) throws IOException {
        long parentId = (node.getParent() == null) ? -1L : node.getParent().getId();
        if (node instanceof DirectoryNode dir) {
            w.write("DIR\t" + node.getId() + "\t" + node.getName() + "\t" + parentId);
            w.newLine();
            for (INode child : dir.listChildren()) saveNode(child, w);
        } else if (node instanceof FileNode fn) {
            String blocks = fn.getBlockIds().stream().map(String::valueOf).collect(Collectors.joining(","));
            w.write("FILE\t" + node.getId() + "\t" + node.getName() + "\t" + parentId
                    + "\t" + fn.getReplicationFactor() + "\t" + fn.getFileSize()
                    + "\t" + (fn.isUnderConstruction() ? 1 : 0) + "\t" + blocks);
            w.newLine();
        }
    }

    public record LoadResult(DirectoryNode root, Map<Long, INode> inodeMap,
                             long nextInodeId, long nextBlockId) {}

    public static LoadResult load(Path source) throws IOException {
        Map<Long, INode> inodeMap = new HashMap<>();
        DirectoryNode root = null;
        long nextInodeId = 1, nextBlockId = 1;

        for (String line : Files.readAllLines(source, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            String[] p = line.split("\t", -1);
            switch (p[0]) {
                case "NEXT_INODE_ID" -> nextInodeId = Long.parseLong(p[1]);
                case "NEXT_BLOCK_ID" -> nextBlockId = Long.parseLong(p[1]);
                case "DIR" -> {
                    long id       = Long.parseLong(p[1]);
                    long parentId = Long.parseLong(p[3]);
                    INode parent  = (parentId == -1) ? null : inodeMap.get(parentId);
                    DirectoryNode dir = new DirectoryNode(id, p[2], parent);
                    inodeMap.put(id, dir);
                    if (parent instanceof DirectoryNode pd) pd.addChild(dir);
                    if (parentId == -1) root = dir;
                }
                case "FILE" -> {
                    long id       = Long.parseLong(p[1]);
                    INode parent  = inodeMap.get(Long.parseLong(p[3]));
                    FileNode fn   = new FileNode(id, p[2], parent, Short.parseShort(p[4]));
                    fn.setFileSize(Long.parseLong(p[5]));
                    fn.setUnderConstruction(p[6].equals("1"));
                    if (!p[7].isEmpty()) {
                        for (String bid : p[7].split(",")) fn.addBlockId(Long.parseLong(bid));
                    }
                    inodeMap.put(id, fn);
                    if (parent instanceof DirectoryNode pd) pd.addChild(fn);
                }
                default -> throw new IOException("Corrupt fsimage line: " + line);
            }
        }
        if (root == null) throw new IOException("fsimage has no root directory: " + source);
        return new LoadResult(root, inodeMap, nextInodeId, nextBlockId);
    }
}
