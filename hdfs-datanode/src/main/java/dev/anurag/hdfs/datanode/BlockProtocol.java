package dev.anurag.hdfs.datanode;

/**
 * Wire protocol constants for the DataNode TCP data plane.
 * All integers are big-endian (Java DataOutputStream default).
 *
 * WRITE_BLOCK request layout:
 *   byte    opcode       = OP_WRITE_BLOCK
 *   long    blockId
 *   int     pipelineLen  (number of downstream DNs)
 *   repeat pipelineLen:
 *     short  hostLen
 *     bytes  host (UTF-8)
 *     int    port
 *   long    dataLen
 *   bytes   data[dataLen]
 *
 * READ_BLOCK request layout:
 *   byte    opcode  = OP_READ_BLOCK
 *   long    blockId
 *
 * Response (both ops):
 *   byte    status  (STATUS_OK | STATUS_ERROR)
 *   --- only for READ_BLOCK OK ---
 *   long    dataLen
 *   bytes   data[dataLen]
 */
public final class BlockProtocol {

    public static final byte OP_WRITE_BLOCK = 0x01;
    public static final byte OP_READ_BLOCK  = 0x02;

    public static final byte STATUS_OK    = 0x00;
    public static final byte STATUS_ERROR = 0x01;

    private BlockProtocol() {}
}
