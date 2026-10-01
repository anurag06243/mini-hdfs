package dev.anurag.hdfs.common.dto;

import dev.anurag.hdfs.common.model.DatanodeState;

public record DatanodeReport(
        String datanodeId,
        String hostname,
        int dataPort,
        DatanodeState state,
        int numBlocks,
        long usedBytes,
        long remainingBytes,
        long lastHeartbeatAgoMs
) {}
