package dev.anurag.hdfs.common.protocol;

import java.io.IOException;

/** Tells a DataNode to re-register and send a full block report. */
public class UnregisteredDatanodeException extends IOException {
    public UnregisteredDatanodeException(String datanodeId) {
        super("Unknown datanode: " + datanodeId);
    }
}
