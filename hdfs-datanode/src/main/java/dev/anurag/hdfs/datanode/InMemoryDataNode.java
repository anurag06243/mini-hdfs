package dev.anurag.hdfs.datanode;

import dev.anurag.hdfs.common.dto.DatanodeCommand;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Volatile block store used by unit tests that don't need real disks. */
public class InMemoryDataNode implements IDataNodeStorage {

    private final String datanodeId;
    private final Map<Long, byte[]> blocks = new ConcurrentHashMap<>();

    public InMemoryDataNode(String datanodeId) {
        this.datanodeId = datanodeId;
    }

    @Override
    public void writeBlock(long blockId, byte[] data) {
        blocks.put(blockId, data.clone());
    }

    @Override
    public byte[] readBlock(long blockId) throws IOException {
        byte[] data = blocks.get(blockId);
        if (data == null) throw new IOException("Block not found: " + blockId);
        return data.clone();
    }

    @Override
    public boolean hasBlock(long blockId) {
        return blocks.containsKey(blockId);
    }

    @Override
    public void deleteBlock(long blockId) {
        blocks.remove(blockId);
    }

    @Override
    public String getDatanodeId() {
        return datanodeId;
    }

    @Override
    public List<Long> listBlocks() {
        return List.copyOf(blocks.keySet());
    }

    @Override
    public long usedBytes() {
        return blocks.values().stream().mapToLong(b -> b.length).sum();
    }

    /**
     * Executes heartbeat commands against other in-process storages (no network).
     * REPLICATE copies the block to the target; DELETE removes it locally.
     */
    public void executeCommands(List<DatanodeCommand> commands, Map<String, IDataNodeStorage> registry)
            throws IOException {
        for (DatanodeCommand cmd : commands) {
            switch (cmd.action()) {
                case REPLICATE -> {
                    IDataNodeStorage target = registry.get(cmd.targetDn().datanodeId());
                    if (target == null) throw new IOException(
                            "REPLICATE target not in registry: " + cmd.targetDn().datanodeId());
                    target.writeBlock(cmd.blockId(), readBlock(cmd.blockId()));
                }
                case DELETE -> blocks.remove(cmd.blockId());
            }
        }
    }
}
