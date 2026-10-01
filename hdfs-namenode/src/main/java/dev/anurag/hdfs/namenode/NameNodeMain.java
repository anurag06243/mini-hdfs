package dev.anurag.hdfs.namenode;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** Starts a NameNode process: {@code namenode [--port 8020] [--dir data/namenode] [--block-size 1048576] ...} */
public class NameNodeMain {

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = parse(args);
        int port = Integer.parseInt(opt.getOrDefault("port", "8020"));
        Path dir = Path.of(opt.getOrDefault("dir", "data/namenode"));
        int blockSize = Integer.parseInt(opt.getOrDefault("block-size", String.valueOf(1024 * 1024)));
        long staleMs = Long.parseLong(opt.getOrDefault("stale-ms", "6000"));
        long deadMs = Long.parseLong(opt.getOrDefault("dead-ms", "12000"));
        long checkpointEvery = Long.parseLong(opt.getOrDefault("checkpoint-edits", "1000"));

        NameNodeImpl nn = new NameNodeImpl(blockSize, staleMs, deadMs, dir);
        NameNodeServer server = new NameNodeServer(nn, port, 1000, checkpointEvery);
        server.start();

        System.out.println("[NameNode] listening on port " + server.getPort()
                + " | metadata " + dir.toAbsolutePath() + " | cluster " + nn.getClusterId());
        System.out.println("[NameNode] block size " + blockSize + " B, STALE after " + staleMs
                + " ms, DEAD after " + deadMs + " ms without heartbeat");
        if (nn.isInSafeMode()) System.out.println("[NameNode] in safe mode until DataNodes report their blocks");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.checkpoint();
                server.close();
                System.out.println("[NameNode] checkpoint written, shut down cleanly");
            } catch (Exception e) {
                System.err.println("[NameNode] shutdown error: " + e.getMessage());
            }
        }));
        Thread.currentThread().join();
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> opt = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length)
                throw new IllegalArgumentException("Expected --name value pairs, got: " + args[i]);
            opt.put(args[i].substring(2), args[++i]);
        }
        return opt;
    }
}
