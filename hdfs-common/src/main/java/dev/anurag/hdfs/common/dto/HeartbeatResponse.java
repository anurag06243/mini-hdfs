package dev.anurag.hdfs.common.dto;

import java.util.List;

public record HeartbeatResponse(List<DatanodeCommand> commands) {}
