package dev.anurag.hdfs.common.dto;

import java.util.List;

public record LocatedBlock(
        long blockId,
        long offset,
        long numBytes,
        List<DatanodeDescriptor> locations
) {}
