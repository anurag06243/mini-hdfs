package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.HeartbeatResponse;

import java.io.IOException;

/**
 * The NameNode-side contract for DataNode → NameNode calls.
 * Heartbeat and block report travel over this seam; in Phase 6 this
 * becomes a gRPC service endpoint.
 */
public interface IDatanodeProtocol {

    HeartbeatResponse heartbeat(HeartbeatRequest request) throws IOException;

    void blockReport(BlockReportRequest request) throws IOException;
}
