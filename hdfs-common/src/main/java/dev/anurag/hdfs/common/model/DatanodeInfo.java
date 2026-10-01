package dev.anurag.hdfs.common.model;

public class DatanodeInfo {
    private final String datanodeId;
    private final String hostname;
    private final int dataPort;
    private final long totalCapacityBytes;
    private long usedBytes;
    private long remainingBytes;
    private long lastHeartbeatMs;
    private DatanodeState state;

    public DatanodeInfo(String datanodeId, String hostname, int dataPort,
                        long totalCapacityBytes, long remainingBytes) {
        this.datanodeId = datanodeId;
        this.hostname = hostname;
        this.dataPort = dataPort;
        this.totalCapacityBytes = totalCapacityBytes;
        this.remainingBytes = remainingBytes;
        this.lastHeartbeatMs = System.currentTimeMillis();
        this.state = DatanodeState.NORMAL;
    }

    public String getDatanodeId()        { return datanodeId; }
    public String getHostname()          { return hostname; }
    public int getDataPort()             { return dataPort; }
    public long getTotalCapacityBytes()  { return totalCapacityBytes; }
    public long getUsedBytes()           { return usedBytes; }
    public long getRemainingBytes()      { return remainingBytes; }
    public long getLastHeartbeatMs()     { return lastHeartbeatMs; }
    public DatanodeState getState()      { return state; }

    public void setUsedBytes(long usedBytes)            { this.usedBytes = usedBytes; }
    public void setRemainingBytes(long remainingBytes)  { this.remainingBytes = remainingBytes; }
    public void setLastHeartbeatMs(long lastHeartbeatMs){ this.lastHeartbeatMs = lastHeartbeatMs; }
    public void setState(DatanodeState state)           { this.state = state; }
}
