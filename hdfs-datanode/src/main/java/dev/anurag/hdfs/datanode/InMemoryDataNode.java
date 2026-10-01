package dev.anurag.hdfs.datanode;

import dev.anurag.hdfs.common.dto.DatanodeCommand;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    public String getDatanodeId() {
        return datanodeId;
    }

    public List<Long> listBlocks() {
        return List.copyOf(blocks.keySet());
    }

    /**
     * Executes commands received in a HeartbeatResponse.
     * REPLICATE: read block from self → write to the target DN.
     * DELETE: remove the block from local storage.
     *
     * @param commands  the list from HeartbeatResponse.commands()
     * @param registry  map of datanodeId → IDataNodeStorage, used to reach the target DN
     */
    public void executeCommands(List<DatanodeCommand> commands, Map<String, IDataNodeStorage> registry)
            throws IOException {
        for (DatanodeCommand cmd : commands) {
            switch (cmd.action()) {
                case REPLICATE -> {
                    byte[] data = readBlock(cmd.blockId());
                    IDataNodeStorage target = registry.get(cmd.targetDn().datanodeId());
                    if (target == null) throw new IOException(
                            "REPLICATE target not in registry: " + cmd.targetDn().datanodeId());
                    target.writeBlock(cmd.blockId(), data);
                }
                case DELETE -> blocks.remove(cmd.blockId());
            }
        }
    }
}
