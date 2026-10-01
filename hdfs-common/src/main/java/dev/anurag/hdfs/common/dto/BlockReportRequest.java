package dev.anurag.hdfs.common.dto;

import java.util.List;

public record BlockReportRequest(String datanodeId, List<Long> blockIds) {}
