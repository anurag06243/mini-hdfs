package dev.anurag.hdfs.common.model;

import java.util.ArrayList;
import java.util.List;

public class FileNode extends INode {
    private List<Long> blockIds;
    private long fileSize;
    private short replicationFactor;
    private boolean isUnderConstruction;

    public FileNode(long id, String name, INode parent, short replicationFactor) {
        super(id, name, parent);
        this.blockIds = new ArrayList<>();
        this.replicationFactor = replicationFactor;
        this.isUnderConstruction = true;
    }

    public void addBlockId(Long blockId) {
        this.blockIds.add(blockId);
    }

    public List<Long> getBlockIds() {
        return this.blockIds;
    }

    public boolean removeLastBlockId(long blockId) {
        if (blockIds.isEmpty() || blockIds.get(blockIds.size() - 1) != blockId) return false;
        blockIds.remove(blockIds.size() - 1);
        return true;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getFileSize() {
        return this.fileSize;
    }

    public short getReplicationFactor() {
        return this.replicationFactor;
    }

    public void setUnderConstruction(boolean isUnderConstruction) {
        this.isUnderConstruction = isUnderConstruction;
    }

    public boolean isUnderConstruction() {
        return this.isUnderConstruction;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public long getSize() {
        return this.fileSize;
    }
}
