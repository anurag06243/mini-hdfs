package dev.anurag.hdfs.datanode;

import java.util.zip.CRC32;

/**
 * Data-plane wire protocol (client <-> DataNode, DataNode <-> DataNode). One request per connection.
 *
 * WRITE_BLOCK request:  [byte 0x01][long blockId][int n]{[UTF host][int port]} x n [long len][bytes][long crc32]
 *             reply:    [byte status]  then, if ERROR: [UTF badNode]  ("" = the node sending this reply)
 *   n = downstream nodes still to forward to; each hop strips itself, so the list shrinks.
 *   Every hop verifies the CRC before storing, so corruption on any link is caught.
 *
 * READ_BLOCK request:   [byte 0x02][long blockId]
 *            reply:     [byte status] then OK: [long len][bytes][long crc32] | ERROR: [UTF message]
 */
public final class BlockProtocol {

    private BlockProtocol() {}

    public static final byte OP_WRITE_BLOCK = 0x01;
    public static final byte OP_READ_BLOCK  = 0x02;

    public static final byte STATUS_OK    = 0x00;
    public static final byte STATUS_ERROR = 0x01;

    public static final int MAX_BLOCK_BYTES = 256 * 1024 * 1024;
    public static final int CONNECT_TIMEOUT_MS = 3_000;
    public static final int IO_TIMEOUT_MS = 30_000;

    public static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }
}
