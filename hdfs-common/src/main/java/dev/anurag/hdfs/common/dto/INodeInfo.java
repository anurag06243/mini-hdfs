package dev.anurag.hdfs.common.dto;

public record INodeInfo(
        String name,
        boolean isDirectory,
        long size,
        long modifiedAt
) {}
