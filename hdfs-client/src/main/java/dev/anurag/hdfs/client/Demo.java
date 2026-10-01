package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.HeartbeatResponse;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.datanode.IDataNodeStorage;
import dev.anurag.hdfs.datanode.InMemoryDataNode;
import dev.anurag.hdfs.datanode.TcpDataNodeServer;
import dev.anurag.hdfs.namenode.NameNodeImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Scripted end-to-end walkthrough: one NameNode + four DataNodes on real TCP ports. */
public class Demo {

    private static final int BLOCK_SIZE = 64 * 1024;
    private static final int NUM_DATANODES = 4;
    private static final Path DATA_DIR = Path.of("demo-data", "namenode");

    private final Map<String, InMemoryDataNode> storages = new LinkedHashMap<>();
    private final Map<String, TcpDataNodeServer> servers = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception {
        new Demo().run();
    }

    private void run() throws Exception {
        wipe(DATA_DIR.getParent());
        startDataNodes();

        scene(1, "Start cluster: 1 NameNode (persistent) + " + NUM_DATANODES + " DataNodes over TCP");
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE, DATA_DIR);
        registerAll(nn);
        servers.forEach((id, s) -> say(id + " listening on 127.0.0.1:" + s.getPort()));
        say("NameNode metadata dir: " + DATA_DIR.toAbsolutePath());
        HdfsClient client = new HdfsClient(nn, BLOCK_SIZE, (short) 3);

        scene(2, "Namespace ops + write pipeline (client -> DN -> DN -> DN)");
        nn.mkdir("/logs");
        byte[] small = "hello from mini-hdfs\n".getBytes();
        byte[] big = new byte[3 * BLOCK_SIZE + 5_000];
        new Random(42).nextBytes(big);
        client.put(small, "/logs/app.log");
        client.put(big, "/data.bin");
        say("put /logs/app.log (" + small.length + " B), /data.bin (" + big.length + " B, block size " + BLOCK_SIZE + ")");
        ls(nn, "/");
        ls(nn, "/logs");
        showBlocks(nn, "/data.bin");
        showDataNodeContents();

        scene(3, "Read path: locations from NN, bytes from DN");
        check("get /logs/app.log", small, client.get("/logs/app.log"));
        check("get /data.bin", big, client.get("/data.bin"));

        scene(4, "Read fallback: kill a DataNode, read still succeeds");
        String victim = nn.getBlockLocations("/logs/app.log").blocks().get(0).locations().get(0).datanodeId();
        servers.get(victim).close();
        say("killed " + victim + " (first replica in the location list)");
        check("get /logs/app.log", small, client.get("/logs/app.log"));
        restartDataNode(victim, nn);

        scene(5, "NameNode crash + restart: fsimage + edit log replay, BlockMap from block reports");
        nn.rename("/logs/app.log", "/logs/app-renamed.log");
        nn.delete("/data.bin");
        say("rename /logs/app.log -> /logs/app-renamed.log, rm /data.bin (edit log only, no checkpoint)");
        say("edits on disk:");
        Files.readAllLines(DATA_DIR.resolve("edits.log")).forEach(l -> say("    " + l));
        nn = null; // "crash": drop all in-memory state
        say("NameNode dropped. Starting a fresh one on the same dir...");
        NameNodeImpl nn2 = new NameNodeImpl(BLOCK_SIZE, DATA_DIR);
        registerAll(nn2);
        for (var e : storages.entrySet()) {
            nn2.blockReport(new BlockReportRequest(e.getKey(), e.getValue().listBlocks()));
        }
        say("DataNodes re-registered and sent full block reports");
        ls(nn2, "/logs");
        check("get /logs/app-renamed.log", small, new HdfsClient(nn2, BLOCK_SIZE, (short) 3).get("/logs/app-renamed.log"));
        say("/data.bin exists after restart? " + exists(nn2, "/data.bin"));
        nn2.saveNamespace();
        say("checkpoint: saveNamespace() wrote fsimage, edit log truncated to "
                + Files.size(DATA_DIR.resolve("edits.log")) + " bytes");

        scene(6, "Failure detection + self-healing re-replication (short thresholds: stale 300ms, dead 600ms)");
        failureDemo();

        for (TcpDataNodeServer s : servers.values()) s.close();
        System.out.println("\n=== Demo complete ===");
    }

    private void failureDemo() throws Exception {
        for (TcpDataNodeServer s : servers.values()) s.close();
        servers.clear();
        storages.clear();
        startDataNodes();
        say("fresh cluster: new in-memory NameNode + " + NUM_DATANODES + " empty DataNodes");
        NameNodeImpl nn = new NameNodeImpl(BLOCK_SIZE, 300, 600);
        registerAll(nn);
        HdfsClient client = new HdfsClient(nn, BLOCK_SIZE, (short) 3);
        byte[] data = "survive a DataNode death".getBytes();
        client.put(data, "/critical.txt");
        long blockId = nn.getBlockLocations("/critical.txt").blocks().get(0).blockId();
        say("wrote /critical.txt, block " + blockId + " on " + holders(blockId) + " (RF=3)");

        String victim = holders(blockId).get(0);
        servers.get(victim).close();
        say("killed " + victim + "; it stops heartbeating");

        Thread.sleep(700);
        List<String> live = storages.keySet().stream().filter(id -> !id.equals(victim)).toList();
        for (String id : live) nn.heartbeat(new HeartbeatRequest(id, 0, 100_000_000L));
        nn.checkAndReplicateDeadNodes();
        say("failure detector ran: " + victim + " marked DEAD, block " + blockId + " now under-replicated");

        Map<String, IDataNodeStorage> liveRegistry = new LinkedHashMap<>();
        live.forEach(id -> liveRegistry.put(id, storages.get(id)));
        for (String id : live) {
            HeartbeatResponse resp = nn.heartbeat(new HeartbeatRequest(id, 0, 100_000_000L));
            for (DatanodeCommand c : resp.commands()) {
                say("heartbeat reply to " + id + " carries command: " + c.action() + " block " + c.blockId()
                        + (c.targetDn() != null ? " -> " + c.targetDn().datanodeId() : ""));
            }
            storages.get(id).executeCommands(resp.commands(), liveRegistry);
        }
        List<String> after = holders(blockId).stream().filter(id -> !id.equals(victim)).toList();
        say("live replicas of block " + blockId + " now: " + after + " (" + after.size() + "/3)");
        check("get /critical.txt", data, client.get("/critical.txt"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void startDataNodes() throws IOException {
        for (int i = 1; i <= NUM_DATANODES; i++) {
            String id = "dn-" + i;
            InMemoryDataNode storage = new InMemoryDataNode(id);
            TcpDataNodeServer server = new TcpDataNodeServer(storage, 0);
            server.start();
            storages.put(id, storage);
            servers.put(id, server);
        }
    }

    private void restartDataNode(String id, NameNodeImpl nn) throws IOException {
        TcpDataNodeServer server = new TcpDataNodeServer(storages.get(id), 0);
        server.start();
        servers.put(id, server);
        nn.registerDatanode(new DatanodeInfo(id, "127.0.0.1", server.getPort(), 100_000_000L, 100_000_000L));
        say(id + " restarted on port " + server.getPort() + " (blocks kept on its disk)");
    }

    private void registerAll(NameNodeImpl nn) {
        servers.forEach((id, s) -> nn.registerDatanode(
                new DatanodeInfo(id, "127.0.0.1", s.getPort(), 100_000_000L, 100_000_000L)));
    }

    private List<String> holders(long blockId) {
        return storages.entrySet().stream().filter(e -> e.getValue().hasBlock(blockId)).map(Map.Entry::getKey).toList();
    }

    private static void ls(NameNodeImpl nn, String path) throws IOException {
        say("ls " + path);
        for (INodeInfo n : nn.listDir(path)) {
            say(String.format("    %-4s %-20s %8d B", n.isDirectory() ? "dir" : "file", n.name(), n.size()));
        }
    }

    private static void showBlocks(NameNodeImpl nn, String path) throws IOException {
        LocatedBlocks lb = nn.getBlockLocations(path);
        say("block map for " + path + " (" + lb.blocks().size() + " blocks):");
        for (LocatedBlock b : lb.blocks()) {
            say("    block " + b.blockId() + " -> "
                    + b.locations().stream().map(d -> d.datanodeId()).collect(Collectors.joining(", ")));
        }
    }

    private void showDataNodeContents() {
        say("what each DataNode physically holds:");
        storages.forEach((id, s) -> say("    " + id + ": blocks " + s.listBlocks()));
    }

    private static boolean exists(NameNodeImpl nn, String path) {
        try {
            nn.getBlockLocations(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static void check(String label, byte[] expected, byte[] actual) {
        boolean ok = Arrays.equals(expected, actual);
        say(label + ": " + actual.length + " B, content matches = " + ok + (ok ? "  [OK]" : "  [FAIL]"));
        if (!ok) throw new IllegalStateException(label + " returned wrong bytes");
    }

    private static void scene(int n, String title) {
        System.out.println("\n--- Scene " + n + ": " + title + " ---");
    }

    private static void say(String msg) {
        System.out.println("  " + msg);
    }

    private static void wipe(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> paths = Files.walk(dir)) {
            List<Path> all = new ArrayList<>(paths.toList());
            all.sort(Comparator.reverseOrder());
            for (Path p : all) Files.delete(p);
        }
    }
}
