package dev.anurag.hdfs.datanode;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.common.protocol.IDatanodeProtocol;
import dev.anurag.hdfs.common.protocol.UnregisteredDatanodeException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A DataNode process: serves block I/O over TCP and runs the NameNode handshake loop.
 *
 * Every heartbeat interval it: registers (if needed) and sends a full block report, heartbeats,
 * hands any piggybacked commands to a separate worker thread (so a slow copy never delays the
 * next heartbeat), and sends another full block report every {@code blockReportMs} or right
 * after a corrupt replica was dropped. If the NameNode answers "unregistered" (it restarted, or
 * declared this node dead), the DataNode re-registers and re-reports.
 */
public class DataNode implements AutoCloseable {

    private final String id;
    private final String host;
    private final IDataNodeStorage storage;
    private final IDatanodeProtocol nameNode;
    private final Path clusterIdFile;
    private final long heartbeatMs;
    private final long blockReportMs;
    private final TcpDataNodeServer server;
    private final ScheduledExecutorService heartbeats = Executors.newSingleThreadScheduledExecutor(daemon("dn-heartbeat"));
    private final ExecutorService commands = Executors.newSingleThreadExecutor(daemon("dn-commands"));
    private final ScheduledExecutorService scanner = Executors.newSingleThreadScheduledExecutor(daemon("dn-scanner"));

    private boolean registered;
    private boolean nameNodeDownLogged;
    private long lastBlockReportMs;

    /**
     * @param clusterIdFile where the NameNode's cluster ID is remembered; null to skip the check
     */
    public DataNode(String id, String host, int port, IDataNodeStorage storage, IDatanodeProtocol nameNode,
                    Path clusterIdFile, long heartbeatMs, long blockReportMs) throws IOException {
        this.id = id;
        this.host = host;
        this.storage = storage;
        this.nameNode = nameNode;
        this.clusterIdFile = clusterIdFile;
        this.heartbeatMs = heartbeatMs;
        this.blockReportMs = blockReportMs;
        this.server = new TcpDataNodeServer(storage, port);
    }

    public void start() {
        server.start();
        heartbeats.scheduleWithFixedDelay(this::tick, 0, heartbeatMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Block scanner: re-reads every stored block on a timer so silent corruption is found even
     * if no client ever reads that replica. A corrupt block is quarantined by the storage layer,
     * which triggers an immediate block report and re-replication from a healthy copy.
     */
    public void startBlockScanner(long intervalMs) {
        scanner.scheduleWithFixedDelay(this::scanBlocks, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void scanBlocks() {
        for (long blockId : storage.listBlocks()) {
            try {
                storage.readBlock(blockId);
            } catch (CorruptBlockException e) {
                log("block scanner: " + e.getMessage());
            } catch (IOException ignored) {
                // deleted between listing and reading
            }
        }
    }

    public int getPort() {
        return server.getPort();
    }

    public IDataNodeStorage storage() {
        return storage;
    }

    private void tick() {
        try {
            if (!registered) register();
            var resp = nameNode.heartbeat(new HeartbeatRequest(id, storage.usedBytes(), storage.remainingBytes()));
            nameNodeDownLogged = false;
            if (!resp.commands().isEmpty()) {
                List<DatanodeCommand> cmds = resp.commands();
                commands.submit(() -> cmds.forEach(this::execute));
            }
            if (storage.consumeBlockReportRequest() || System.currentTimeMillis() - lastBlockReportMs >= blockReportMs) {
                blockReport();
            }
        } catch (UnregisteredDatanodeException e) {
            log("NameNode does not know this node; re-registering");
            registered = false;
        } catch (IllegalStateException e) {
            log("FATAL: " + e.getMessage());
            heartbeats.shutdown();
        } catch (IOException e) {
            if (!nameNodeDownLogged) log("NameNode unreachable, will keep retrying: " + e.getMessage());
            nameNodeDownLogged = true;
            registered = false;
        } catch (RuntimeException e) {
            log("heartbeat loop error: " + e);
        }
    }

    private void register() throws IOException {
        checkClusterId(nameNode.getClusterId());
        nameNode.registerDatanode(new DatanodeInfo(id, host, server.getPort(),
                storage.usedBytes() + storage.remainingBytes(), storage.remainingBytes()));
        registered = true;
        blockReport();
        log("registered with NameNode, reported " + storage.listBlocks().size() + " blocks");
    }

    private void checkClusterId(String clusterId) throws IOException {
        if (clusterIdFile == null) return;
        if (!Files.exists(clusterIdFile)) {
            Files.writeString(clusterIdFile, clusterId);
            return;
        }
        String mine = Files.readString(clusterIdFile).trim();
        if (!mine.equals(clusterId)) {
            throw new IllegalStateException("cluster ID mismatch: this DataNode belongs to " + mine
                    + " but the NameNode is " + clusterId + ". Refusing to join (its block report would"
                    + " get every block deleted as an orphan).");
        }
    }

    private void blockReport() throws IOException {
        nameNode.blockReport(new BlockReportRequest(id, storage.listBlocks()));
        lastBlockReportMs = System.currentTimeMillis();
    }

    private void execute(DatanodeCommand cmd) {
        try {
            switch (cmd.action()) {
                case REPLICATE -> {
                    byte[] data = storage.readBlock(cmd.blockId());
                    BlockTransferClient.writeBlock(cmd.blockId(), List.of(cmd.targetDn()), data);
                    log("replicated block " + cmd.blockId() + " to " + cmd.targetDn().datanodeId());
                }
                case DELETE -> {
                    storage.deleteBlock(cmd.blockId());
                    log("deleted block " + cmd.blockId());
                }
            }
        } catch (IOException e) {
            log(cmd.action() + " of block " + cmd.blockId() + " failed: " + e.getMessage());
        }
    }

    private void log(String msg) {
        System.out.println("[" + id + "] " + msg);
    }

    /** Stops heartbeating and closes the data port. Blocks on disk are kept. */
    @Override
    public void close() throws IOException {
        heartbeats.shutdownNow();
        commands.shutdownNow();
        scanner.shutdownNow();
        server.close();
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
