package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlocks;

import java.io.IOException;
import java.util.List;

public interface INameNodeService {

    // ── Write path ────────────────────────────────────────────────────────────

    void createFile(String path, short replicationFactor) throws IOException;

    AddBlockResponse addBlock(String path) throws IOException;

    void closeFile(String path, long fileSize) throws IOException;

    // ── Read path ─────────────────────────────────────────────────────────────

    LocatedBlocks getBlockLocations(String path) throws IOException;

    // ── Namespace ops ─────────────────────────────────────────────────────────

    void mkdir(String path) throws IOException;

    List<INodeInfo> listDir(String path) throws IOException;

    void delete(String path) throws IOException;

    void rename(String src, String dst) throws IOException;
}
