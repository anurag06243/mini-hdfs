package dev.anurag.hdfs.common.model;

import java.util.*;

public class BlockMap {
    private Map<Long, Set<String>> blockToDatanodesMap;
    private Map<String, Set<Long>> datanodeToBlocksMap;

    public BlockMap() {
        this.blockToDatanodesMap = new HashMap<>();
        this.datanodeToBlocksMap = new HashMap<>();
    }

    public void addBlock(long blockId, String datanodeId) {
        blockToDatanodesMap.computeIfAbsent(blockId, k -> new HashSet<>()).add(datanodeId);
        datanodeToBlocksMap.computeIfAbsent(datanodeId, k -> new HashSet<>()).add(blockId);
    }

    public Set<String> getDatanodesForBlock(long blockId) {
        return blockToDatanodesMap.getOrDefault(blockId, Collections.emptySet());
    }

    public Set<Long> getBlocksForDatanode(String datanodeId) {
        return datanodeToBlocksMap.getOrDefault(datanodeId, Collections.emptySet());
    }

    public void removeBlock(long blockId) {
        Set<String> datanodes = blockToDatanodesMap.remove(blockId);
        if (datanodes != null) {
            for (String datanodeId : datanodes) {
                Set<Long> blocks = datanodeToBlocksMap.get(datanodeId);
                if (blocks != null) {
                    blocks.remove(blockId);
                    if (blocks.isEmpty()) {
                        datanodeToBlocksMap.remove(datanodeId);
                    }
                }
            }
        }
    }

    /**
     * Replaces the block set known for a DN with the authoritative reported set.
     * Used during full block reports to reconcile NN state with DN reality.
     */
    public void updateDatanodeBlocks(String datanodeId, Set<Long> reportedBlockIds) {
        Set<Long> current = datanodeToBlocksMap.getOrDefault(datanodeId, Collections.emptySet());

        // Blocks the DN no longer has — remove their replica entry
        Set<Long> removed = new HashSet<>(current);
        removed.removeAll(reportedBlockIds);
        for (long blockId : removed) {
            Set<String> dns = blockToDatanodesMap.get(blockId);
            if (dns != null) dns.remove(datanodeId);
        }

        // Blocks the DN newly reported — add replica entry
        Set<Long> added = new HashSet<>(reportedBlockIds);
        added.removeAll(current);
        for (long blockId : added) {
            blockToDatanodesMap.computeIfAbsent(blockId, k -> new HashSet<>()).add(datanodeId);
        }

        // Replace the DN's block set
        datanodeToBlocksMap.put(datanodeId, new HashSet<>(reportedBlockIds));
    }

    public void removeReplica(long blockId, String datanodeId) {
        Set<String> datanodeIds = blockToDatanodesMap.get(blockId);
        if (datanodeIds != null) {
            datanodeIds.remove(datanodeId);
            if (datanodeIds.isEmpty()) {
                blockToDatanodesMap.remove(blockId);
            }
        }

        Set<Long> blocks = datanodeToBlocksMap.get(datanodeId);
        if (blocks != null) {
            blocks.remove(blockId);
            if (blocks.isEmpty()) {
                datanodeToBlocksMap.remove(datanodeId);
            }
        }
    }
}
