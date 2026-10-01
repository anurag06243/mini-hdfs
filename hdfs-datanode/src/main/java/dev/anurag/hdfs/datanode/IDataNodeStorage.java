package dev.anurag.hdfs.datanode;

import java.io.IOException;

public interface IDataNodeStorage {

    void writeBlock(long blockId, byte[] data) throws IOException;

    byte[] readBlock(long blockId) throws IOException;

    boolean hasBlock(long blockId);

    String getDatanodeId();
}
