package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.HeartbeatResponse;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.model.BlockMap;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.common.model.DatanodeState;
import dev.anurag.hdfs.common.model.DirectoryNode;
import dev.anurag.hdfs.common.model.FileNode;
import dev.anurag.hdfs.common.model.INode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class NameNodeImpl implements INameNodeService, IDatanodeProtocol {

    private static final String FSIMAGE_FILE = "fsimage.bin";
    private static final String EDITLOG_FILE = "edits.log";

    // ── failure-detection thresholds ──────────────────────────────────────────
    private final long staleThresholdMs;
    private final long deadThresholdMs;

    // ── namespace state ───────────────────────────────────────────────────────
    private DirectoryNode root;
    private final Map<Long, INode> inodeMap = new HashMap<>();
    private final BlockMap blockMap = new BlockMap();
    private final Map<String, DatanodeInfo> datanodes = new ConcurrentHashMap<>();
    private final Map<String, List<DatanodeCommand>> pendingCommands = new ConcurrentHashMap<>();

    private final AtomicLong inodeIdGen = new AtomicLong(1);
    private final AtomicLong blockIdGen = new AtomicLong(1);
    private final int blockSize;

    // ── persistence (null = in-memory-only mode) ──────────────────────────────
    private final Path dataDir;
    private EditLog editLog;

    // ── constructors ─────────────────────────────────────────────────────────

    /** In-memory mode (no persistence). Used by Phase 3–5 tests. */
    public NameNodeImpl(int blockSize) {
        this(blockSize, 20_000L, 90_000L, null);
    }

    /** In-memory mode with custom failure-detection thresholds. Used by Phase 4–5 tests. */
    public NameNodeImpl(int blockSize, long staleThresholdMs, long deadThresholdMs) {
        this(blockSize, staleThresholdMs, deadThresholdMs, null);
    }

    /**
     * Persistent mode: namespace is saved to / loaded from {@code dataDir}.
     * On construction, loads existing fsimage + replays edit log if present.
     */
    public NameNodeImpl(int blockSize, Path dataDir) {
        this(blockSize, 20_000L, 90_000L, dataDir);
    }

    public NameNodeImpl(int blockSize, long staleThresholdMs, long deadThresholdMs, Path dataDir) {
        this.blockSize        = blockSize;
        this.staleThresholdMs = staleThresholdMs;
        this.deadThresholdMs  = deadThresholdMs;
        this.dataDir          = dataDir;

        if (dataDir != null) {
            try {
                Files.createDirectories(dataDir);
                loadNamespace();
                editLog = new EditLog(dataDir.resolve(EDITLOG_FILE));
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize persistent NameNode", e);
            }
        } else {
            initEmptyRoot();
        }
    }

    // ── persistence: load ─────────────────────────────────────────────────────

    private void loadNamespace() throws IOException {
        Path fsImagePath = dataDir.resolve(FSIMAGE_FILE);
        Path editLogPath = dataDir.resolve(EDITLOG_FILE);

        if (Files.exists(fsImagePath)) {
            FsImage.LoadResult lr = FsImage.load(fsImagePath);
            this.root = lr.root();
            this.inodeMap.putAll(lr.inodeMap());
            this.inodeIdGen.set(lr.nextInodeId());
            this.blockIdGen.set(lr.nextBlockId());
        } else {
            initEmptyRoot();
        }

        if (Files.exists(editLogPath)) {
            replayEditLog(editLogPath);
        }

        // After replay, advance generators past any IDs seen
        long maxInode = inodeMap.keySet().stream().mapToLong(Long::longValue).max().orElse(0L);
        long maxBlock = inodeMap.values().stream()
                .filter(n -> n instanceof FileNode)
                .flatMap(n -> ((FileNode) n).getBlockIds().stream())
                .mapToLong(Long::longValue).max().orElse(0L);
        inodeIdGen.updateAndGet(cur -> Math.max(cur, maxInode + 1));
        blockIdGen.updateAndGet(cur -> Math.max(cur, maxBlock + 1));
    }

    private void initEmptyRoot() {
        this.root = new DirectoryNode(inodeIdGen.getAndIncrement(), "", null);
        inodeMap.put(root.getId(), root);
    }

    private void replayEditLog(Path editLogPath) throws IOException {
        EditLog log = new EditLog(editLogPath);
        for (String[] parts : log.readAll()) {
            String op = parts[0];
            switch (op) {
                case EditLog.CREATE_FILE -> {
                    // parts: op ts path rf inodeId
                    replayCreateFile(parts[2], Short.parseShort(parts[3]), Long.parseLong(parts[4]));
                }
                case EditLog.MKDIR -> {
                    // parts: op ts path inodeId
                    replayMkdir(parts[2], Long.parseLong(parts[3]));
                }
                case EditLog.ADD_BLOCK -> {
                    // parts: op ts path blockId
                    replayAddBlock(parts[2], Long.parseLong(parts[3]));
                }
                case EditLog.CLOSE_FILE -> {
                    // parts: op ts path fileSize
                    try {
                        closeFile(parts[2], Long.parseLong(parts[3]));
                    } catch (IOException e) {
                        // file may have been deleted later — skip
                    }
                }
                case EditLog.RENAME -> {
                    // parts: op ts src dst
                    try {
                        rename(parts[2], parts[3]);
                    } catch (IOException e) { /* skip stale entries */ }
                }
                case EditLog.DELETE -> {
                    // parts: op ts path
                    try {
                        delete(parts[2]);
                    } catch (IOException e) { /* skip stale entries */ }
                }
            }
        }
        log.close();

        // Discard any files still under construction (simplified lease recovery)
        List<String> toDelete = new ArrayList<>();
        collectUnderConstruction(root, "", toDelete);
        for (String path : toDelete) {
            try { delete(path); } catch (IOException ignored) {}
        }
    }

    private void collectUnderConstruction(DirectoryNode dir, String prefix, List<String> result) {
        for (INode child : dir.listChildren()) {
            String path = prefix + "/" + child.getName();
            if (child instanceof FileNode fn && fn.isUnderConstruction()) {
                result.add(path);
            } else if (child instanceof DirectoryNode d) {
                collectUnderConstruction(d, path, result);
            }
        }
    }

    // Replay helpers — use specific IDs instead of allocating new ones

    private void replayCreateFile(String path, short rf, long inodeId) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String[] parts = path.split("/");
        String name = parts[parts.length - 1];
        if (parent.getChild(name) != null) return; // already exists (idempotent)
        FileNode fn = new FileNode(inodeId, name, parent, rf);
        parent.addChild(fn);
        inodeMap.put(inodeId, fn);
    }

    private void replayMkdir(String path, long inodeId) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String[] parts = path.split("/");
        String name = parts[parts.length - 1];
        if (parent.getChild(name) != null) return;
        DirectoryNode dir = new DirectoryNode(inodeId, name, parent);
        parent.addChild(dir);
        inodeMap.put(inodeId, dir);
    }

    private void replayAddBlock(String path, long blockId) throws IOException {
        FileNode fn = resolveFile(path);
        if (!fn.getBlockIds().contains(blockId)) fn.addBlockId(blockId);
        // BlockMap intentionally not updated — rebuilt from block reports
    }

    // ── persistence: save (checkpoint) ────────────────────────────────────────

    /**
     * Saves a full namespace snapshot to disk and truncates the edit log.
     * Callers: tests; in production this would be triggered periodically.
     */
    public void saveNamespace() throws IOException {
        if (dataDir == null) throw new IllegalStateException("NameNode is not in persistent mode");
        FsImage.save(root, inodeIdGen.get(), blockIdGen.get(), dataDir.resolve(FSIMAGE_FILE));
        editLog.truncate();
    }

    // ── DataNode registration ─────────────────────────────────────────────────

    public void registerDatanode(DatanodeInfo info) {
        datanodes.put(info.getDatanodeId(), info);
        pendingCommands.put(info.getDatanodeId(), new ArrayList<>());
    }

    // ── IDatanodeProtocol ─────────────────────────────────────────────────────

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest req) throws IOException {
        DatanodeInfo dn = datanodes.get(req.datanodeId());
        if (dn == null) throw new IOException("Unknown datanode: " + req.datanodeId());
        dn.setLastHeartbeatMs(System.currentTimeMillis());
        dn.setRemainingBytes(req.remainingBytes());
        dn.setState(DatanodeState.NORMAL);
        List<DatanodeCommand> commands = pendingCommands.getOrDefault(req.datanodeId(), List.of());
        List<DatanodeCommand> snapshot = new ArrayList<>(commands);
        commands.clear();
        return new HeartbeatResponse(snapshot);
    }

    @Override
    public void blockReport(BlockReportRequest req) throws IOException {
        DatanodeInfo dn = datanodes.get(req.datanodeId());
        if (dn == null) throw new IOException("Unknown datanode: " + req.datanodeId());
        Set<Long> reported  = new HashSet<>(req.blockIds());
        Set<Long> prevBlocks = new HashSet<>(blockMap.getBlocksForDatanode(req.datanodeId()));
        blockMap.updateDatanodeBlocks(req.datanodeId(), reported);
        for (long blockId : reported) {
            Set<String> replicas = blockMap.getDatanodesForBlock(blockId);
            short rf = replicationFactorForBlock(blockId);
            if (rf > 0 && replicas.size() > rf)
                pendingCommands.get(req.datanodeId()).add(DatanodeCommand.delete(blockId));
        }
        Set<Long> lostBlocks = new HashSet<>(prevBlocks);
        lostBlocks.removeAll(reported);
        for (long blockId : lostBlocks) {
            Set<String> replicas = blockMap.getDatanodesForBlock(blockId);
            short rf = replicationFactorForBlock(blockId);
            if (rf > 0 && replicas.size() < rf) scheduleReplication(blockId, replicas, rf);
        }
    }

    public void checkAndReplicateDeadNodes() {
        long now = System.currentTimeMillis();
        for (DatanodeInfo dn : datanodes.values()) {
            if (dn.getState() == DatanodeState.DEAD) continue;
            long elapsed = now - dn.getLastHeartbeatMs();
            if (elapsed > deadThresholdMs) {
                dn.setState(DatanodeState.DEAD);
                handleDeadDatanode(dn.getDatanodeId());
            } else if (elapsed > staleThresholdMs) {
                dn.setState(DatanodeState.STALE);
            }
        }
    }

    private void handleDeadDatanode(String deadDnId) {
        Set<Long> lostBlocks = blockMap.getBlocksForDatanode(deadDnId);
        if (lostBlocks == null) return;
        for (long blockId : new HashSet<>(lostBlocks)) {
            blockMap.removeReplica(blockId, deadDnId);
            Set<String> remaining = blockMap.getDatanodesForBlock(blockId);
            short rf = replicationFactorForBlock(blockId);
            if (rf > 0 && remaining != null && remaining.size() < rf)
                scheduleReplication(blockId, remaining, rf);
        }
    }

    private void scheduleReplication(long blockId, Set<String> currentReplicas, short rf) {
        List<String> liveSources = currentReplicas.stream()
                .filter(id -> isLive(datanodes.get(id))).collect(Collectors.toList());
        if (liveSources.isEmpty()) return;
        int needed = rf - currentReplicas.size();
        List<DatanodeInfo> liveCandidates = datanodes.values().stream()
                .filter(dn -> isLive(dn) && !currentReplicas.contains(dn.getDatanodeId()))
                .collect(Collectors.toList());
        Collections.shuffle(liveCandidates);
        for (int i = 0; i < Math.min(needed, liveCandidates.size()); i++) {
            DatanodeInfo target = liveCandidates.get(i);
            String sourceId = liveSources.get(i % liveSources.size());
            DatanodeDescriptor desc = new DatanodeDescriptor(
                    target.getDatanodeId(), target.getHostname(), target.getDataPort());
            pendingCommands.computeIfAbsent(sourceId, k -> new ArrayList<>())
                    .add(DatanodeCommand.replicate(blockId, desc));
            blockMap.addBlock(blockId, target.getDatanodeId());
        }
    }

    private boolean isLive(DatanodeInfo dn) {
        return dn != null && dn.getState() != DatanodeState.DEAD;
    }

    private short replicationFactorForBlock(long blockId) {
        for (INode node : inodeMap.values()) {
            if (node instanceof FileNode fn && fn.getBlockIds().contains(blockId))
                return fn.getReplicationFactor();
        }
        return 0;
    }

    // ── INameNodeService ──────────────────────────────────────────────────────

    private DirectoryNode resolveParent(String path) throws IOException {
        String[] parts = path.split("/");
        INode current = root;
        for (int i = 0; i < parts.length - 1; i++) {
            if (parts[i].isEmpty()) continue;
            if (!(current instanceof DirectoryNode dir))
                throw new IOException("Not a directory: " + parts[i]);
            current = dir.getChild(parts[i]);
            if (current == null) throw new IOException("Path does not exist: " + path);
        }
        if (!(current instanceof DirectoryNode))
            throw new IOException("Parent is not a directory: " + path);
        return (DirectoryNode) current;
    }

    private FileNode resolveFile(String path) throws IOException {
        String[] parts = path.split("/");
        String fileName = parts[parts.length - 1];
        INode node = resolveParent(path).getChild(fileName);
        if (node == null) throw new IOException("File not found: " + path);
        if (!(node instanceof FileNode)) throw new IOException("Not a file: " + path);
        return (FileNode) node;
    }

    @Override
    public void createFile(String path, short replicationFactor) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String[] parts = path.split("/");
        String name = parts[parts.length - 1];
        if (parent.getChild(name) != null) throw new IOException("File already exists: " + path);
        long id = inodeIdGen.getAndIncrement();
        logOp(EditLog.CREATE_FILE, path, replicationFactor, id);
        FileNode fn = new FileNode(id, name, parent, replicationFactor);
        parent.addChild(fn);
        inodeMap.put(id, fn);
    }

    @Override
    public AddBlockResponse addBlock(String path) throws IOException {
        FileNode fn = resolveFile(path);
        if (!fn.isUnderConstruction()) throw new IOException("File not under construction: " + path);
        long blockId = blockIdGen.getAndIncrement();
        logOp(EditLog.ADD_BLOCK, path, blockId);
        List<DatanodeInfo> available = datanodes.values().stream()
                .filter(dn -> dn.getState() != DatanodeState.DEAD)
                .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(available);
        int rf = Math.min(fn.getReplicationFactor(), available.size());
        List<DatanodeInfo> picked = available.subList(0, rf);
        for (DatanodeInfo dn : picked) blockMap.addBlock(blockId, dn.getDatanodeId());
        fn.addBlockId(blockId);
        List<DatanodeDescriptor> targets = picked.stream()
                .map(dn -> new DatanodeDescriptor(dn.getDatanodeId(), dn.getHostname(), dn.getDataPort()))
                .toList();
        return new AddBlockResponse(blockId, targets);
    }

    @Override
    public void closeFile(String path, long fileSize) throws IOException {
        FileNode fn = resolveFile(path);
        if (!fn.isUnderConstruction()) throw new IOException("File not under construction: " + path);
        logOp(EditLog.CLOSE_FILE, path, fileSize);
        fn.setFileSize(fileSize);
        fn.setUnderConstruction(false);
    }

    @Override
    public LocatedBlocks getBlockLocations(String path) throws IOException {
        FileNode fn = resolveFile(path);
        if (fn.isUnderConstruction()) throw new IOException("File still being written: " + path);
        List<LocatedBlock> blocks = new ArrayList<>();
        long offset = 0;
        for (long blockId : fn.getBlockIds()) {
            Set<String> dnIds = blockMap.getDatanodesForBlock(blockId);
            List<DatanodeDescriptor> locs = dnIds.stream()
                    .filter(id -> isLive(datanodes.get(id)))
                    .map(id -> {
                        DatanodeInfo dn = datanodes.get(id);
                        return new DatanodeDescriptor(dn.getDatanodeId(), dn.getHostname(), dn.getDataPort());
                    }).toList();
            long numBytes = Math.min(blockSize, fn.getFileSize() - offset);
            blocks.add(new LocatedBlock(blockId, offset, numBytes, locs));
            offset += numBytes;
        }
        return new LocatedBlocks(fn.getFileSize(), blocks);
    }

    @Override
    public void mkdir(String path) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String[] parts = path.split("/");
        String name = parts[parts.length - 1];
        if (parent.getChild(name) != null) throw new IOException("Already exists: " + path);
        long id = inodeIdGen.getAndIncrement();
        logOp(EditLog.MKDIR, path, id);
        DirectoryNode dir = new DirectoryNode(id, name, parent);
        parent.addChild(dir);
        inodeMap.put(id, dir);
    }

    @Override
    public List<INodeInfo> listDir(String path) throws IOException {
        DirectoryNode dir;
        if (path.equals("/")) {
            dir = root;
        } else {
            String[] parts = path.split("/");
            String lastName = parts[parts.length - 1];
            dir = (DirectoryNode) resolveParent(path).getChild(lastName);
        }
        if (dir == null) throw new IOException("Directory not found: " + path);
        List<INodeInfo> result = new ArrayList<>();
        for (INode child : dir.listChildren())
            result.add(new INodeInfo(child.getName(), child.isDirectory(), child.getSize(), child.getModifiedAt()));
        return result;
    }

    @Override
    public void delete(String path) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String[] parts = path.split("/");
        String name = parts[parts.length - 1];
        INode target = parent.getChild(name);
        if (target == null) throw new IOException("Not found: " + path);
        logOp(EditLog.DELETE, path);
        parent.removeChild(name);
        inodeMap.remove(target.getId());
    }

    @Override
    public void rename(String src, String dst) throws IOException {
        DirectoryNode srcParent = resolveParent(src);
        String[] sp = src.split("/");
        INode target = srcParent.getChild(sp[sp.length - 1]);
        if (target == null) throw new IOException("Not found: " + src);
        DirectoryNode dstParent = resolveParent(dst);
        String[] dp = dst.split("/");
        String dstName = dp[dp.length - 1];
        if (dstParent.getChild(dstName) != null) throw new IOException("Destination exists: " + dst);
        logOp(EditLog.RENAME, src, dst);
        srcParent.removeChild(sp[sp.length - 1]);
        target.setName(dstName);
        dstParent.addChild(target);
    }

    // ── helper ────────────────────────────────────────────────────────────────

    private void logOp(String op, Object... fields) {
        if (editLog == null) return;
        try {
            editLog.log(op, fields);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write edit log entry: " + op, e);
        }
    }
}
