package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.protocol.INameNodeService;
import dev.anurag.hdfs.datanode.BlockTransferClient;
import dev.anurag.hdfs.datanode.IDataNodeStorage;
import dev.anurag.hdfs.datanode.PipelineException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HdfsClient {

    private static final int MAX_BLOCK_ATTEMPTS = 3;

    private final INameNodeService nameNode;
    private final Map<String, IDataNodeStorage> datanodeRegistry; // null in TCP mode
    private final int blockSize;
    private final short replicationFactor;
    private final boolean useTcp;

    /** In-memory mode (unit tests): block bytes go straight to in-process storages, no network. */
    public HdfsClient(INameNodeService nameNode,
                      Map<String, IDataNodeStorage> datanodeRegistry,
                      int blockSize,
                      short replicationFactor) {
        this.nameNode = nameNode;
        this.datanodeRegistry = datanodeRegistry;
        this.blockSize = blockSize;
        this.replicationFactor = replicationFactor;
        this.useTcp = false;
    }

    /** TCP mode: block bytes travel over the DataNode write pipeline / read protocol. */
    public HdfsClient(INameNodeService nameNode, int blockSize, short replicationFactor) {
        this.nameNode = nameNode;
        this.datanodeRegistry = null;
        this.blockSize = blockSize;
        this.replicationFactor = replicationFactor;
        this.useTcp = true;
    }

    public INameNodeService nameNode() {
        return nameNode;
    }

    // ── Write ────────────────────────────────────────────────────────────────

    /**
     * Writes a new file block by block. If anything fails for good, the partially written
     * file is deleted so no half-file is left behind under construction.
     */
    public void put(byte[] data, String hdfsPath) throws IOException {
        nameNode.createFile(hdfsPath, replicationFactor);
        try {
            for (int offset = 0; offset < data.length; offset += blockSize) {
                byte[] chunk = Arrays.copyOfRange(data, offset, Math.min(offset + blockSize, data.length));
                if (useTcp) writeBlockWithRecovery(hdfsPath, chunk);
                else writeBlockInMemory(hdfsPath, chunk);
            }
            nameNode.closeFile(hdfsPath, data.length);
        } catch (IOException | RuntimeException e) {
            try { nameNode.delete(hdfsPath); } catch (IOException ignored) {}
            throw e;
        }
    }

    /**
     * Simplified pipeline recovery: if the pipeline fails, abandon the block, exclude the node
     * the pipeline blamed, and ask the NameNode for a fresh block on other nodes.
     * (Real HDFS repairs the pipeline in place and keeps the bytes already acknowledged.)
     */
    private void writeBlockWithRecovery(String path, byte[] chunk) throws IOException {
        Set<String> excluded = new HashSet<>();
        PipelineException last = null;
        for (int attempt = 1; attempt <= MAX_BLOCK_ATTEMPTS; attempt++) {
            AddBlockResponse resp = nameNode.addBlock(path, excluded);
            try {
                BlockTransferClient.writeBlock(resp.blockId(), resp.targets(), chunk);
                return;
            } catch (PipelineException e) {
                last = e;
                nameNode.abandonBlock(path, resp.blockId());
                resp.targets().stream()
                        .filter(dn -> BlockTransferClient.address(dn).equals(e.badNode()))
                        .forEach(dn -> excluded.add(dn.datanodeId()));
            }
        }
        throw new IOException("Could not write block of " + path + " after " + MAX_BLOCK_ATTEMPTS + " attempts", last);
    }

    private void writeBlockInMemory(String path, byte[] chunk) throws IOException {
        AddBlockResponse resp = nameNode.addBlock(path);
        for (DatanodeDescriptor dn : resp.targets()) {
            IDataNodeStorage storage = datanodeRegistry.get(dn.datanodeId());
            if (storage == null) throw new IOException("DataNode not in registry: " + dn.datanodeId());
            storage.writeBlock(resp.blockId(), chunk);
        }
    }

    // ── Read ─────────────────────────────────────────────────────────────────

    public byte[] get(String hdfsPath) throws IOException {
        LocatedBlocks located = nameNode.getBlockLocations(hdfsPath);
        ByteArrayOutputStream result = new ByteArrayOutputStream((int) located.fileSize());
        for (LocatedBlock block : located.blocks()) {
            byte[] blockData = useTcp ? readWithFallback(block) : readInMemoryWithFallback(block);
            if (blockData.length != block.numBytes())
                throw new IOException("Block " + block.blockId() + " has " + blockData.length
                        + " bytes, expected " + block.numBytes());
            result.write(blockData);
        }
        return result.toByteArray();
    }

    /** Tries each replica in turn; a dead node or a checksum failure just moves on to the next. */
    private static byte[] readWithFallback(LocatedBlock block) throws IOException {
        if (block.locations().isEmpty()) throw new IOException("No live replica of block " + block.blockId());
        IOException lastError = null;
        for (DatanodeDescriptor dn : block.locations()) {
            try {
                return BlockTransferClient.readBlock(block.blockId(), dn);
            } catch (IOException e) {
                lastError = e;
            }
        }
        throw new IOException("All replicas failed for block " + block.blockId(), lastError);
    }

    private byte[] readInMemoryWithFallback(LocatedBlock block) throws IOException {
        for (DatanodeDescriptor dn : block.locations()) {
            IDataNodeStorage storage = datanodeRegistry.get(dn.datanodeId());
            if (storage != null && storage.hasBlock(block.blockId())) {
                try {
                    return storage.readBlock(block.blockId());
                } catch (IOException e) {
                    // try next replica
                }
            }
        }
        throw new IOException("All replicas failed for block " + block.blockId());
    }
}
