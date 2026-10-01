package dev.anurag.hdfs.datanode;

import java.io.IOException;
import java.util.List;

public interface IDataNodeStorage {

    void writeBlock(long blockId, byte[] data) throws IOException;

    /** @throws CorruptBlockException if the stored bytes fail their checksum. */
    byte[] readBlock(long blockId) throws IOException;

    boolean hasBlock(long blockId);

    void deleteBlock(long blockId) throws IOException;

    List<Long> listBlocks();

    long usedBytes();

    default long remainingBytes() {
        return 100L * 1024 * 1024 * 1024;
    }

    /** True once after the block set changed without the NameNode asking (e.g. a corrupt replica was dropped). */
    default boolean consumeBlockReportRequest() {
        return false;
    }

    String getDatanodeId();
}
