package dev.anurag.hdfs.common.dto;

import java.util.List;

public record AddBlockResponse(
        long blockId,
        List<DatanodeDescriptor> targets
) {}
