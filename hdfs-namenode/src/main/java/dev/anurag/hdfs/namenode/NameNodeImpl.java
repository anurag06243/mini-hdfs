package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.common.dto.DatanodeReport;
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
import dev.anurag.hdfs.common.protocol.IDatanodeProtocol;
import dev.anurag.hdfs.common.protocol.INameNodeService;
import dev.anurag.hdfs.common.protocol.UnregisteredDatanodeException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The NameNode: namespace tree, block -> DataNode map, DataNode liveness, edit log + fsimage.
 *
 * Not thread-safe. In a running cluster every call goes through {@link NameNodeServer},
 * which serialises access with a single read/write lock (the same "global namesystem lock"
 * design real HDFS uses).
 */
public class NameNodeImpl implements INameNodeService, IDatanodeProtocol {

    private static final String FSIMAGE_FILE = "fsimage.bin";
    private static final String EDITLOG_FILE = "edits.log";
    private static final String VERSION_FILE = "VERSION";
    private static final double SAFE_MODE_THRESHOLD = 0.999;
    private static final long DEFAULT_SAFE_MODE_MAX_MS = 30_000L;

    // ── failure-detection thresholds ──────────────────────────────────────────
    private final long staleThresholdMs;
    private final long deadThresholdMs;

    // ── namespace state ───────────────────────────────────────────────────────
    private DirectoryNode root;
    private final Map<Long, INode> inodeMap = new HashMap<>();
    private final Map<Long, FileNode> blockToFile = new HashMap<>();
    private final BlockMap blockMap = new BlockMap();
    private final Map<String, DatanodeInfo> datanodes = new LinkedHashMap<>();
    private final Map<String, List<DatanodeCommand>> pendingCommands = new HashMap<>();

    private final AtomicLong inodeIdGen = new AtomicLong(1);
    private final AtomicLong blockIdGen = new AtomicLong(1);
    private final int blockSize;

    // ── safe mode: read-only until enough blocks have been reported after a restart ──
    private boolean safeMode;
    private long safeModeEnteredMs;
    private long safeModeMaxMs = DEFAULT_SAFE_MODE_MAX_MS;

    // ── persistence (null = in-memory-only mode) ──────────────────────────────
    private final Path dataDir;
    private EditLog editLog;
    private String clusterId;

    // ── constructors ─────────────────────────────────────────────────────────

    /** In-memory mode (no persistence). */
    public NameNodeImpl(int blockSize) {
        this(blockSize, 20_000L, 90_000L, null);
    }

    /** In-memory mode with custom failure-detection thresholds. */
    public NameNodeImpl(int blockSize, long staleThresholdMs, long deadThresholdMs) {
        this(blockSize, staleThresholdMs, deadThresholdMs, null);
    }

    /** Persistent mode: loads fsimage + replays the edit log from {@code dataDir} if present. */
    public NameNodeImpl(int blockSize, Path dataDir) {
        this(blockSize, 20_000L, 90_000L, dataDir);
    }

    public NameNodeImpl(int blockSize, long staleThresholdMs, long deadThresholdMs, Path dataDir) {
        this.blockSize        = blockSize;
        this.staleThresholdMs = staleThresholdMs;
        this.deadThresholdMs  = deadThresholdMs;
        this.dataDir          = dataDir;

        if (dataDir == null) {
            clusterId = UUID.randomUUID().toString();
            initEmptyRoot();
            return;
        }
        try {
            Files.createDirectories(dataDir);
            Path versionFile = dataDir.resolve(VERSION_FILE);
            if (!Files.exists(versionFile)) Files.writeString(versionFile, UUID.randomUUID().toString());
            clusterId = Files.readString(versionFile).trim();
            loadNamespace();
            editLog = new EditLog(dataDir.resolve(EDITLOG_FILE));
            discardUnderConstructionFiles();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize persistent NameNode", e);
        }
        if (!blockToFile.isEmpty()) {
            safeMode = true;
            safeModeEnteredMs = System.currentTimeMillis();
        }
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    public void setSafeModeMaxMs(long ms) {
        this.safeModeMaxMs = ms;
    }

    public boolean isInSafeMode() {
        return safeMode;
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
            for (INode node : inodeMap.values()) {
                if (node instanceof FileNode fn) fn.getBlockIds().forEach(b -> blockToFile.put(b, fn));
            }
        } else {
            initEmptyRoot();
        }

        if (Files.exists(editLogPath)) replayEditLog(editLogPath);

        // Advance generators past every ID seen, so new IDs never collide with replayed ones
        long maxInode = inodeMap.keySet().stream().mapToLong(Long::longValue).max().orElse(0L);
        long maxBlock = blockToFile.keySet().stream().mapToLong(Long::longValue).max().orElse(0L);
        inodeIdGen.updateAndGet(cur -> Math.max(cur, maxInode + 1));
        blockIdGen.updateAndGet(cur -> Math.max(cur, maxBlock + 1));
    }

    private void initEmptyRoot() {
        this.root = new DirectoryNode(inodeIdGen.getAndIncrement(), "", null);
        inodeMap.put(root.getId(), root);
    }

    private void replayEditLog(Path editLogPath) throws IOException {
        List<String[]> entries;
        try (EditLog log = new EditLog(editLogPath)) {
            entries = log.readAll();
        }
        for (int i = 0; i < entries.size(); i++) {
            String[] p = entries.get(i);
            try {
                switch (p[0]) {
                    case EditLog.CREATE_FILE   -> replayCreateFile(p[2], Short.parseShort(p[3]), Long.parseLong(p[4]));
                    case EditLog.MKDIR         -> replayMkdir(p[2], Long.parseLong(p[3]));
                    case EditLog.ADD_BLOCK     -> replayAddBlock(p[2], Long.parseLong(p[3]));
                    case EditLog.ABANDON_BLOCK -> abandonBlock(p[2], Long.parseLong(p[3]));
                    case EditLog.CLOSE_FILE    -> closeFile(p[2], Long.parseLong(p[3]));
                    case EditLog.RENAME        -> rename(p[2], p[3]);
                    case EditLog.DELETE        -> delete(p[2]);
                    default -> throw new IOException("Unknown edit log op: " + p[0]);
                }
            } catch (RuntimeException e) {
                // A torn final line (crash mid-append) is expected; anything earlier is corruption.
                if (i != entries.size() - 1) throw new IOException("Corrupt edit log at entry " + (i + 1), e);
            }
        }
    }

    /**
     * Simplified lease recovery: a file still under construction after replay belongs to a
     * writer that died with the old NameNode, so it is discarded. Runs after the edit log is
     * open so the DELETE is itself logged — otherwise a later file at the same path would be
     * merged with the stale one on the next replay.
     */
    private void discardUnderConstructionFiles() throws IOException {
        List<String> toDelete = new ArrayList<>();
        collectUnderConstruction(root, "", toDelete);
        for (String path : toDelete) delete(path);
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

    private void replayCreateFile(String path, short rf, long inodeId) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String name = lastComponent(path);
        if (parent.getChild(name) != null) return;
        FileNode fn = new FileNode(inodeId, name, parent, rf);
        parent.addChild(fn);
        inodeMap.put(inodeId, fn);
    }

    private void replayMkdir(String path, long inodeId) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String name = lastComponent(path);
        if (parent.getChild(name) != null) return;
        DirectoryNode dir = new DirectoryNode(inodeId, name, parent);
        parent.addChild(dir);
        inodeMap.put(inodeId, dir);
    }

    private void replayAddBlock(String path, long blockId) throws IOException {
        FileNode fn = resolveFile(path);
        if (!fn.getBlockIds().contains(blockId)) fn.addBlockId(blockId);
        blockToFile.put(blockId, fn);
        // BlockMap is not persisted — it is rebuilt from DataNode block reports
    }

    // ── persistence: checkpoint ───────────────────────────────────────────────

    /** Writes a full fsimage and truncates the edit log. */
    public void saveNamespace() throws IOException {
        if (dataDir == null) throw new IllegalStateException("NameNode is not in persistent mode");
        FsImage.save(root, inodeIdGen.get(), blockIdGen.get(), dataDir.resolve(FSIMAGE_FILE));
        editLog.truncate();
    }

    /** Checkpoints if the edit log has grown past {@code threshold} entries. Returns true if it did. */
    public boolean maybeCheckpoint(long threshold) throws IOException {
        if (editLog == null || editLog.entriesSinceCheckpoint() < threshold) return false;
        saveNamespace();
        return true;
    }

    public void close() throws IOException {
        if (editLog != null) editLog.close();
    }

    // ── IDatanodeProtocol ─────────────────────────────────────────────────────

    @Override
    public void registerDatanode(DatanodeInfo info) {
        datanodes.put(info.getDatanodeId(), info);
        pendingCommands.put(info.getDatanodeId(), new ArrayList<>());
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest req) throws IOException {
        DatanodeInfo dn = datanodes.get(req.datanodeId());
        // A DEAD node's replicas were already written off; make it re-register and re-report
        if (dn == null || dn.getState() == DatanodeState.DEAD) throw new UnregisteredDatanodeException(req.datanodeId());
        dn.setLastHeartbeatMs(System.currentTimeMillis());
        dn.setUsedBytes(req.usedBytes());
        dn.setRemainingBytes(req.remainingBytes());
        dn.setState(DatanodeState.NORMAL);
        List<DatanodeCommand> commands = pendingCommands.computeIfAbsent(req.datanodeId(), k -> new ArrayList<>());
        List<DatanodeCommand> snapshot = new ArrayList<>(commands);
        commands.clear();
        return new HeartbeatResponse(snapshot);
    }

    /**
     * Full block report: the DataNode's complete list of blocks replaces what the NameNode
     * believed about it. Reconciles four cases:
     *   orphan        — block belongs to no file (deleted while the DN was away) -> DELETE it
     *   over-replicated — more replicas than RF -> DELETE this extra copy
     *   lost          — NN thought the DN had it, DN says no -> re-replicate if under RF
     *   new           — recorded as a replica
     */
    @Override
    public void blockReport(BlockReportRequest req) throws IOException {
        String dnId = req.datanodeId();
        DatanodeInfo dn = datanodes.get(dnId);
        if (dn == null) throw new UnregisteredDatanodeException(dnId);
        List<DatanodeCommand> queue = pendingCommands.computeIfAbsent(dnId, k -> new ArrayList<>());

        Set<Long> valid = new HashSet<>();
        for (long blockId : req.blockIds()) {
            if (blockToFile.containsKey(blockId)) valid.add(blockId);
            else queue.add(DatanodeCommand.delete(blockId));
        }

        Set<Long> previous = new HashSet<>(blockMap.getBlocksForDatanode(dnId));
        blockMap.updateDatanodeBlocks(dnId, valid);

        for (long blockId : valid) {
            short rf = blockToFile.get(blockId).getReplicationFactor();
            if (blockMap.getDatanodesForBlock(blockId).size() > rf) {
                queue.add(DatanodeCommand.delete(blockId));
                blockMap.removeReplica(blockId, dnId);
            }
        }

        previous.removeAll(valid);
        if (!safeMode) {
            for (long blockId : previous) replicateIfNeeded(blockId);
        }
        checkSafeMode();
    }

    /** Liveness sweep + under-replication scan. The NameNode server runs this periodically. */
    public void checkAndReplicateDeadNodes() {
        long now = System.currentTimeMillis();
        for (DatanodeInfo dn : datanodes.values()) {
            if (dn.getState() == DatanodeState.DEAD) continue;
            long elapsed = now - dn.getLastHeartbeatMs();
            if (elapsed > deadThresholdMs) {
                dn.setState(DatanodeState.DEAD);
                log(dn.getDatanodeId() + " is DEAD (no heartbeat for " + elapsed + " ms)");
                handleDeadDatanode(dn.getDatanodeId());
            } else if (elapsed > staleThresholdMs && dn.getState() != DatanodeState.STALE) {
                dn.setState(DatanodeState.STALE);
                log(dn.getDatanodeId() + " is STALE (no heartbeat for " + elapsed + " ms)");
            }
        }
        checkSafeMode();
        if (safeMode) return;
        for (Map.Entry<Long, FileNode> e : blockToFile.entrySet()) {
            if (!e.getValue().isUnderConstruction()) replicateIfNeeded(e.getKey());
        }
    }

    private void handleDeadDatanode(String deadDnId) {
        Set<Long> lostBlocks = new HashSet<>(blockMap.getBlocksForDatanode(deadDnId));
        for (long blockId : lostBlocks) blockMap.removeReplica(blockId, deadDnId);
        pendingCommands.remove(deadDnId);
        if (safeMode) return;
        for (long blockId : lostBlocks) replicateIfNeeded(blockId);
    }

    private void replicateIfNeeded(long blockId) {
        FileNode file = blockToFile.get(blockId);
        if (file == null) return;
        Set<String> replicas = new HashSet<>(blockMap.getDatanodesForBlock(blockId));
        if (replicas.size() < file.getReplicationFactor()) scheduleReplication(blockId, replicas, file.getReplicationFactor());
    }

    /**
     * Queues REPLICATE commands on a live source and records the targets in the BlockMap
     * immediately ("pending replica"), so the next sweep doesn't schedule the same copy twice.
     * If the copy never happens, the target's next full block report removes the phantom entry
     * and the block is scheduled again.
     */
    private void scheduleReplication(long blockId, Set<String> currentReplicas, short rf) {
        List<String> liveSources = currentReplicas.stream().filter(id -> isLive(datanodes.get(id))).toList();
        if (liveSources.isEmpty()) return;
        int needed = rf - currentReplicas.size();
        List<DatanodeInfo> candidates = chooseTargets(currentReplicas, needed);
        for (int i = 0; i < candidates.size(); i++) {
            DatanodeInfo target = candidates.get(i);
            String sourceId = liveSources.get(i % liveSources.size());
            pendingCommands.computeIfAbsent(sourceId, k -> new ArrayList<>())
                    .add(DatanodeCommand.replicate(blockId, descriptor(target)));
            blockMap.addBlock(blockId, target.getDatanodeId());
            log("block " + blockId + " under-replicated (" + currentReplicas.size() + "/" + rf
                    + "): " + sourceId + " -> " + target.getDatanodeId());
        }
    }

    private static void log(String msg) {
        System.out.println("[NameNode] " + msg);
    }

    /** Random placement over healthy nodes; STALE nodes are used only if there aren't enough NORMAL ones. */
    private List<DatanodeInfo> chooseTargets(Set<String> excluded, int count) {
        List<DatanodeInfo> normal = new ArrayList<>();
        List<DatanodeInfo> stale = new ArrayList<>();
        for (DatanodeInfo dn : datanodes.values()) {
            if (excluded.contains(dn.getDatanodeId())) continue;
            if (dn.getState() == DatanodeState.NORMAL) normal.add(dn);
            else if (dn.getState() == DatanodeState.STALE) stale.add(dn);
        }
        Collections.shuffle(normal);
        Collections.shuffle(stale);
        normal.addAll(stale);
        return normal.subList(0, Math.min(count, normal.size()));
    }

    private boolean isLive(DatanodeInfo dn) {
        return dn != null && dn.getState() != DatanodeState.DEAD;
    }

    private void checkSafeMode() {
        if (!safeMode) return;
        long reported = blockToFile.keySet().stream().filter(b -> !blockMap.getDatanodesForBlock(b).isEmpty()).count();
        boolean enough = blockToFile.isEmpty() || (double) reported / blockToFile.size() >= SAFE_MODE_THRESHOLD;
        if (enough || System.currentTimeMillis() - safeModeEnteredMs > safeModeMaxMs) {
            safeMode = false;
            log("leaving safe mode: " + reported + "/" + blockToFile.size() + " blocks reported");
        }
    }

    private void checkNotSafeMode() throws IOException {
        if (safeMode) throw new IOException("NameNode is in safe mode (waiting for DataNode block reports)");
    }

    // ── path helpers ──────────────────────────────────────────────────────────

    private static List<String> components(String path) throws IOException {
        if (path == null || !path.startsWith("/")) throw new IOException("Path must be absolute: " + path);
        List<String> parts = new ArrayList<>();
        for (String s : path.split("/")) {
            if (s.isEmpty()) continue;
            if (s.equals(".") || s.equals("..") || s.contains("\t") || s.contains("\n") || s.contains("\r"))
                throw new IOException("Invalid path component '" + s + "' in " + path);
            parts.add(s);
        }
        return parts;
    }

    private static String lastComponent(String path) throws IOException {
        List<String> parts = components(path);
        if (parts.isEmpty()) throw new IOException("Operation not allowed on /");
        return parts.get(parts.size() - 1);
    }

    private INode resolve(String path) throws IOException {
        INode current = root;
        for (String part : components(path)) {
            if (!(current instanceof DirectoryNode dir)) return null;
            current = dir.getChild(part);
            if (current == null) return null;
        }
        return current;
    }

    private DirectoryNode resolveParent(String path) throws IOException {
        List<String> parts = components(path);
        if (parts.isEmpty()) throw new IOException("Operation not allowed on /");
        INode current = root;
        for (String part : parts.subList(0, parts.size() - 1)) {
            if (!(current instanceof DirectoryNode dir)) throw new IOException("Not a directory: " + part);
            current = dir.getChild(part);
            if (current == null) throw new IOException("Path does not exist: " + path);
        }
        if (!(current instanceof DirectoryNode d)) throw new IOException("Parent is not a directory: " + path);
        return d;
    }

    private FileNode resolveFile(String path) throws IOException {
        INode node = resolve(path);
        if (node == null) throw new IOException("File not found: " + path);
        if (!(node instanceof FileNode fn)) throw new IOException("Not a file: " + path);
        return fn;
    }

    private static DatanodeDescriptor descriptor(DatanodeInfo dn) {
        return new DatanodeDescriptor(dn.getDatanodeId(), dn.getHostname(), dn.getDataPort());
    }

    // ── INameNodeService ──────────────────────────────────────────────────────

    @Override
    public void createFile(String path, short replicationFactor) throws IOException {
        checkNotSafeMode();
        if (replicationFactor < 1) throw new IOException("Replication factor must be >= 1");
        DirectoryNode parent = resolveParent(path);
        String name = lastComponent(path);
        if (parent.getChild(name) != null) throw new IOException("File already exists: " + path);
        long id = inodeIdGen.getAndIncrement();
        logOp(EditLog.CREATE_FILE, path, replicationFactor, id);
        FileNode fn = new FileNode(id, name, parent, replicationFactor);
        parent.addChild(fn);
        inodeMap.put(id, fn);
    }

    @Override
    public AddBlockResponse addBlock(String path, Set<String> excludedDatanodes) throws IOException {
        checkNotSafeMode();
        FileNode fn = resolveFile(path);
        if (!fn.isUnderConstruction()) throw new IOException("File not under construction: " + path);
        List<DatanodeInfo> picked = chooseTargets(excludedDatanodes, fn.getReplicationFactor());
        if (picked.isEmpty()) throw new IOException("No live DataNodes available for " + path);
        long blockId = blockIdGen.getAndIncrement();
        logOp(EditLog.ADD_BLOCK, path, blockId);
        for (DatanodeInfo dn : picked) blockMap.addBlock(blockId, dn.getDatanodeId());
        fn.addBlockId(blockId);
        blockToFile.put(blockId, fn);
        return new AddBlockResponse(blockId, picked.stream().map(NameNodeImpl::descriptor).toList());
    }

    @Override
    public void abandonBlock(String path, long blockId) throws IOException {
        FileNode fn = resolveFile(path);
        if (!fn.isUnderConstruction()) throw new IOException("File not under construction: " + path);
        if (!fn.getBlockIds().isEmpty() && fn.getBlockIds().get(fn.getBlockIds().size() - 1) != blockId)
            throw new IOException("Block " + blockId + " is not the last block of " + path);
        logOp(EditLog.ABANDON_BLOCK, path, blockId);
        fn.removeLastBlockId(blockId);
        invalidateBlock(blockId);
    }

    @Override
    public void closeFile(String path, long fileSize) throws IOException {
        FileNode fn = resolveFile(path);
        if (!fn.isUnderConstruction()) throw new IOException("File not under construction: " + path);
        long blocks = fn.getBlockIds().size();
        long expectedBlocks = (fileSize + blockSize - 1) / blockSize;
        if (blocks != expectedBlocks)
            throw new IOException(path + ": " + fileSize + " bytes needs " + expectedBlocks + " blocks of "
                    + blockSize + " B, but " + blocks + " were written (client using a different block size?)");
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
            List<DatanodeDescriptor> locs = new ArrayList<>(blockMap.getDatanodesForBlock(blockId).stream()
                    .map(datanodes::get).filter(this::isLive).map(NameNodeImpl::descriptor).toList());
            Collections.shuffle(locs); // spread reads across replicas
            long numBytes = Math.min(blockSize, fn.getFileSize() - offset);
            blocks.add(new LocatedBlock(blockId, offset, numBytes, locs));
            offset += numBytes;
        }
        return new LocatedBlocks(fn.getFileSize(), blocks);
    }

    @Override
    public void mkdir(String path) throws IOException {
        checkNotSafeMode();
        DirectoryNode parent = resolveParent(path);
        String name = lastComponent(path);
        if (parent.getChild(name) != null) throw new IOException("Already exists: " + path);
        long id = inodeIdGen.getAndIncrement();
        logOp(EditLog.MKDIR, path, id);
        DirectoryNode dir = new DirectoryNode(id, name, parent);
        parent.addChild(dir);
        inodeMap.put(id, dir);
    }

    @Override
    public List<INodeInfo> listDir(String path) throws IOException {
        INode node = resolve(path);
        if (node == null) throw new IOException("Directory not found: " + path);
        if (!(node instanceof DirectoryNode dir)) throw new IOException("Not a directory: " + path);
        List<INodeInfo> result = new ArrayList<>();
        for (INode child : dir.listChildren())
            result.add(new INodeInfo(child.getName(), child.isDirectory(), child.getSize(), child.getModifiedAt()));
        return result;
    }

    /** Deletes a file or a whole directory subtree and schedules deletion of every block it owned. */
    @Override
    public void delete(String path) throws IOException {
        DirectoryNode parent = resolveParent(path);
        String name = lastComponent(path);
        INode target = parent.getChild(name);
        if (target == null) throw new IOException("Not found: " + path);
        logOp(EditLog.DELETE, path);
        parent.removeChild(name);
        removeSubtree(target);
    }

    private void removeSubtree(INode node) {
        inodeMap.remove(node.getId());
        if (node instanceof DirectoryNode dir) {
            for (INode child : new ArrayList<>(dir.listChildren())) removeSubtree(child);
        } else if (node instanceof FileNode fn) {
            for (long blockId : fn.getBlockIds()) invalidateBlock(blockId);
        }
    }

    /** Forgets a block and tells every DataNode holding it to delete it on its next heartbeat. */
    private void invalidateBlock(long blockId) {
        for (String dnId : new ArrayList<>(blockMap.getDatanodesForBlock(blockId))) {
            pendingCommands.computeIfAbsent(dnId, k -> new ArrayList<>()).add(DatanodeCommand.delete(blockId));
        }
        blockMap.removeBlock(blockId);
        blockToFile.remove(blockId);
    }

    @Override
    public void rename(String src, String dst) throws IOException {
        checkNotSafeMode();
        List<String> srcParts = components(src);
        List<String> dstParts = components(dst);
        if (srcParts.isEmpty() || dstParts.isEmpty()) throw new IOException("Cannot rename /");
        if (dstParts.size() > srcParts.size() && dstParts.subList(0, srcParts.size()).equals(srcParts))
            throw new IOException("Cannot move " + src + " into itself");
        DirectoryNode srcParent = resolveParent(src);
        String srcName = lastComponent(src);
        INode target = srcParent.getChild(srcName);
        if (target == null) throw new IOException("Not found: " + src);
        DirectoryNode dstParent = resolveParent(dst);
        String dstName = lastComponent(dst);
        if (dstParent.getChild(dstName) != null) throw new IOException("Destination exists: " + dst);
        logOp(EditLog.RENAME, src, dst);
        srcParent.removeChild(srcName);
        target.setName(dstName);
        target.setParent(dstParent);
        dstParent.addChild(target);
    }

    @Override
    public List<DatanodeReport> getDatanodeReport() {
        long now = System.currentTimeMillis();
        List<DatanodeReport> report = new ArrayList<>();
        for (DatanodeInfo dn : datanodes.values()) {
            report.add(new DatanodeReport(dn.getDatanodeId(), dn.getHostname(), dn.getDataPort(), dn.getState(),
                    blockMap.getBlocksForDatanode(dn.getDatanodeId()).size(), dn.getUsedBytes(),
                    dn.getRemainingBytes(), now - dn.getLastHeartbeatMs()));
        }
        return report;
    }

    // ── helper ────────────────────────────────────────────────────────────────

    /** Write-ahead: the entry is durable before the caller mutates in-memory state. */
    private void logOp(String op, Object... fields) throws IOException {
        if (editLog != null) editLog.log(op, fields);
    }
}
