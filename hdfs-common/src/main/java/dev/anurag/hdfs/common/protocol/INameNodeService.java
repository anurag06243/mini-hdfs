package dev.anurag.hdfs.common.protocol;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.DatanodeReport;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlocks;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/** Client -> NameNode contract. Implemented in-process by NameNodeImpl and remotely by NameNodeRpcClient. */
public interface INameNodeService {

    // ── Write path ────────────────────────────────────────────────────────────

    void createFile(String path, short replicationFactor) throws IOException;

    default AddBlockResponse addBlock(String path) throws IOException {
        return addBlock(path, Set.of());
    }

    /** Allocates the next block, never placing it on a DataNode in {@code excludedDatanodes}. */
    AddBlockResponse addBlock(String path, Set<String> excludedDatanodes) throws IOException;

    /** Drops the file's last block after a failed pipeline write so the client can retry it. */
    void abandonBlock(String path, long blockId) throws IOException;

    void closeFile(String path, long fileSize) throws IOException;

    // ── Read path ─────────────────────────────────────────────────────────────

    LocatedBlocks getBlockLocations(String path) throws IOException;

    // ── Namespace ops ─────────────────────────────────────────────────────────

    void mkdir(String path) throws IOException;

    List<INodeInfo> listDir(String path) throws IOException;

    void delete(String path) throws IOException;

    void rename(String src, String dst) throws IOException;

    // ── Admin ─────────────────────────────────────────────────────────────────

    List<DatanodeReport> getDatanodeReport() throws IOException;

    /** Block size is a cluster setting owned by the NameNode; clients must split files with it. */
    int getBlockSize() throws IOException;
}
