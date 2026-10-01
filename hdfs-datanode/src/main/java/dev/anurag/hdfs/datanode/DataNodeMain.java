package dev.anurag.hdfs.datanode;

import dev.anurag.hdfs.common.rpc.NameNodeRpcClient;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Starts a DataNode process:
 * {@code datanode --id dn-1 --port 9001 [--dir data/dn-1] [--namenode localhost:8020] [--host 127.0.0.1]}
 */
public class DataNodeMain {

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length)
                throw new IllegalArgumentException("Expected --name value pairs, got: " + args[i]);
            opt.put(args[i].substring(2), args[++i]);
        }
        String id = opt.get("id");
        if (id == null) throw new IllegalArgumentException("--id is required");
        int port = Integer.parseInt(opt.getOrDefault("port", "0"));
        Path dir = Path.of(opt.getOrDefault("dir", "data/" + id));
        String host = opt.getOrDefault("host", "127.0.0.1");
        NameNodeRpcClient nn = NameNodeRpcClient.connect(opt.getOrDefault("namenode", "localhost:8020"));
        long heartbeatMs = Long.parseLong(opt.getOrDefault("heartbeat-ms", "1000"));
        long blockReportMs = Long.parseLong(opt.getOrDefault("block-report-ms", "30000"));
        long scanMs = Long.parseLong(opt.getOrDefault("scan-ms", "60000"));

        DiskDataNodeStorage storage = new DiskDataNodeStorage(id, dir);
        DataNode dn = new DataNode(id, host, port, storage, nn, dir.resolve("CLUSTER_ID"), heartbeatMs, blockReportMs);
        dn.start();
        dn.startBlockScanner(scanMs);
        System.out.println("[" + id + "] serving blocks on " + host + ":" + dn.getPort()
                + " | storage " + dir.toAbsolutePath() + " (" + storage.listBlocks().size() + " blocks)"
                + " | NameNode " + nn.address());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { dn.close(); } catch (Exception ignored) {}
        }));
        Thread.currentThread().join();
    }
}
