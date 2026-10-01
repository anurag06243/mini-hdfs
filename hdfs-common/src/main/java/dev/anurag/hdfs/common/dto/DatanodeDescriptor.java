package dev.anurag.hdfs.common.dto;

public record DatanodeDescriptor(
        String datanodeId,
        String hostname,
        int dataPort
) {}
