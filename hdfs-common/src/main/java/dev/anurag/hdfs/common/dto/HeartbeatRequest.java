package dev.anurag.hdfs.common.dto;

public record HeartbeatRequest(String datanodeId, long usedBytes, long remainingBytes) {}
