package dev.anurag.hdfs.common.protocol;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.HeartbeatResponse;
import dev.anurag.hdfs.common.model.DatanodeInfo;

import java.io.IOException;

/**
 * DataNode -> NameNode contract. The NameNode never calls a DataNode; it answers
 * heartbeats with commands (pull model).
 */
public interface IDatanodeProtocol {

    /**
     * Handshake before registering. A DataNode refuses to join if this differs from the ID it
     * stored on first contact — otherwise a NameNode with a lost or wrong metadata directory
     * would see every block as an orphan and delete the cluster's data.
     */
    String getClusterId() throws IOException;

    void registerDatanode(DatanodeInfo info) throws IOException;

    /** @throws UnregisteredDatanodeException if the NameNode does not know this DataNode (e.g. after a NN restart). */
    HeartbeatResponse heartbeat(HeartbeatRequest request) throws IOException;

    void blockReport(BlockReportRequest request) throws IOException;
}
