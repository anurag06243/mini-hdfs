package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.datanode.IDataNodeStorage;
import dev.anurag.hdfs.namenode.INameNodeService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class HdfsClient {

    private final INameNodeService nameNode;
    private final Map<String, IDataNodeStorage> datanodeRegistry; // null in TCP mode
    private final int blockSize;
    private final short replicationFactor;
    private final boolean useTcp;

    /**
     * In-memory mode (Phase 3–5 tests): reads/writes go through the registry map.
     */
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

    /**
     * TCP mode (Phase 6): reads/writes use real TCP sockets.
     * DataNodes must have TcpDataNodeServer started on the port advertised to NameNode.
     */
    public HdfsClient(INameNodeService nameNode, int blockSize, short replicationFactor) {
        this.nameNode = nameNode;
        this.datanodeRegistry = null;
        this.blockSize = blockSize;
        this.replicationFactor = replicationFactor;
        this.useTcp = true;
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    public void put(byte[] data, String hdfsPath) throws IOException {
        nameNode.createFile(hdfsPath, replicationFactor);

        int offset = 0;
        while (offset < data.length) {
            int end = Math.min(offset + blockSize, data.length);
            byte[] chunk = Arrays.copyOfRange(data, offset, end);

            AddBlockResponse resp = nameNode.addBlock(hdfsPath);

            if (useTcp) {
                // Pipeline: client → DN-1 → DN-2 → DN-3 (chain of TCP connections)
                TcpBlockWriter.write(resp.blockId(), resp.targets(), chunk);
            } else {
                // In-memory scatter: write chunk to every replica directly
                for (DatanodeDescriptor dn : resp.targets()) {
                    IDataNodeStorage storage = datanodeRegistry.get(dn.datanodeId());
                    if (storage == null) throw new IOException("DataNode not in registry: " + dn.datanodeId());
                    storage.writeBlock(resp.blockId(), chunk);
                }
            }

            offset = end;
        }

        nameNode.closeFile(hdfsPath, data.length);
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    public byte[] get(String hdfsPath) throws IOException {
        LocatedBlocks located = nameNode.getBlockLocations(hdfsPath);
        ByteArrayOutputStream result = new ByteArrayOutputStream((int) located.fileSize());

        for (LocatedBlock block : located.blocks()) {
            byte[] blockData = useTcp
                    ? TcpBlockReader.readWithFallback(block.blockId(), block.locations())
                    : readInMemoryWithFallback(block);
            result.write(blockData);
        }

        return result.toByteArray();
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
