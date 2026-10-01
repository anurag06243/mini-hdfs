package dev.anurag.hdfs.common.dto;

public record DatanodeCommand(Action action, long blockId, DatanodeDescriptor targetDn) {

    public enum Action { REPLICATE, DELETE }

    // Factory helpers
    public static DatanodeCommand replicate(long blockId, DatanodeDescriptor target) {
        return new DatanodeCommand(Action.REPLICATE, blockId, target);
    }

    public static DatanodeCommand delete(long blockId) {
        return new DatanodeCommand(Action.DELETE, blockId, null);
    }
}
