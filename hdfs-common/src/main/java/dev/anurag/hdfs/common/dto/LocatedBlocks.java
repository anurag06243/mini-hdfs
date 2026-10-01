package dev.anurag.hdfs.common.dto;

import java.util.List;

public record LocatedBlocks(
        long fileSize,
        List<LocatedBlock> blocks
) {}
