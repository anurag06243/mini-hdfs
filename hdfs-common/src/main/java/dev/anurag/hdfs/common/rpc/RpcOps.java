package dev.anurag.hdfs.common.rpc;

/**
 * Control-plane wire protocol (client/DataNode -> NameNode).
 *
 * Every message is a length-prefixed frame: [int length][payload].
 *   request  payload: [byte op][op-specific fields]
 *   response payload: [byte status][result fields | UTF error message]
 * Integers are big-endian (DataOutputStream); strings are modified UTF-8 (writeUTF).
 * One connection carries many sequential request/response pairs.
 */
public final class RpcOps {

    private RpcOps() {}

    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    // client protocol
    public static final byte CREATE_FILE         = 1;
    public static final byte ADD_BLOCK           = 2;
    public static final byte ABANDON_BLOCK       = 3;
    public static final byte CLOSE_FILE          = 4;
    public static final byte GET_BLOCK_LOCATIONS = 5;
    public static final byte MKDIR               = 6;
    public static final byte LIST_DIR            = 7;
    public static final byte DELETE              = 8;
    public static final byte RENAME              = 9;
    public static final byte DATANODE_REPORT     = 10;
    public static final byte GET_BLOCK_SIZE      = 11;

    // datanode protocol
    public static final byte VERSION      = 19;
    public static final byte REGISTER     = 20;
    public static final byte HEARTBEAT    = 21;
    public static final byte BLOCK_REPORT = 22;

    // response status
    public static final byte STATUS_OK           = 0;
    public static final byte STATUS_ERROR        = 1;
    public static final byte STATUS_UNREGISTERED = 2;
}
