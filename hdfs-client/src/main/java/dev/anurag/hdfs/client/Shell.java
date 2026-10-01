package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.HeartbeatResponse;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.common.model.DatanodeState;
import dev.anurag.hdfs.datanode.IDataNodeStorage;
import dev.anurag.hdfs.datanode.InMemoryDataNode;
import dev.anurag.hdfs.datanode.TcpDataNodeServer;
import dev.anurag.hdfs.namenode.NameNodeImpl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Interactive shell over an in-process mini-HDFS cluster (1 NameNode + N DataNodes on TCP).
 * Heartbeats and failure detection run on the shell thread between commands, because
 * NameNodeImpl is not thread-safe.
 */
public class Shell {

    private static final int BLOCK_SIZE = 64 * 1024;
    private static final short RF = 3;
    private static final int NUM_DATANODES = 4;
    private static final long STALE_MS = 3_000;
    private static final long DEAD_MS = 6_000;
    private static final Path NN_DIR = Path.of("shell-data", "namenode");

    private final Map<String, InMemoryDataNode> storages = new LinkedHashMap<>();
    private final Map<String, TcpDataNodeServer> servers = new LinkedHashMap<>();
    private final Map<String, DatanodeInfo> infos = new LinkedHashMap<>();
    private final Map<String, DatanodeState> lastSeenState = new LinkedHashMap<>();
    private NameNodeImpl nn;
    private HdfsClient client;
    private String cwd = "/";

    public static void main(String[] args) throws Exception {
        new Shell().run();
    }

    private void run() throws Exception {
        wipe(NN_DIR.getParent());
        nn = newNameNode();
        client = new HdfsClient(nn, BLOCK_SIZE, RF);
        for (int i = 1; i <= NUM_DATANODES; i++) startDataNode("dn-" + i, new InMemoryDataNode("dn-" + i));

        System.out.println("mini-HDFS shell  |  block size " + BLOCK_SIZE / 1024 + " KB, replication " + RF
                + ", " + NUM_DATANODES + " DataNodes, dead after " + DEAD_MS / 1000 + "s without heartbeat");
        System.out.println("Type 'help' for commands.\n");

        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.print("hdfs:" + cwd + "> ");
            if (!in.hasNextLine()) break;
            String line = in.nextLine().trim();
            if (line.isEmpty()) continue;
            tick();
            try {
                if (!dispatch(line)) break;
            } catch (IOException | RuntimeException e) {
                System.out.println("error: " + e.getMessage());
            }
            tick();
        }
        for (TcpDataNodeServer s : servers.values()) if (s != null) s.close();
    }

    private boolean dispatch(String line) throws Exception {
        String[] a = line.split("\\s+");
        switch (a[0]) {
            case "help" -> help();
            case "exit", "quit" -> { return false; }
            case "pwd" -> System.out.println(cwd);
            case "cd" -> {
                String target = resolve(a.length > 1 ? a[1] : "/");
                if (!isDir(target)) throw new IllegalArgumentException("not a directory: " + target);
                cwd = target;
            }
            case "ls" -> ls(resolve(a.length > 1 ? a[1] : "."));
            case "mkdir" -> { need(a, 2); nn.mkdir(resolve(a[1])); }
            case "put" -> {
                need(a, 3);
                Path local = Path.of(a[1]).toAbsolutePath().normalize();
                if (!Files.isRegularFile(local)) throw new IllegalArgumentException("local file not found: " + local);
                byte[] data = Files.readAllBytes(local);
                String dst = resolve(a[2]);
                if (isDir(dst)) dst = join(dst, Path.of(a[1]).getFileName().toString());
                client.put(data, dst);
                System.out.println("wrote " + dst + " (" + data.length + " B)");
                stat(dst);
            }
            case "write" -> {
                need(a, 3);
                byte[] data = line.substring(line.indexOf(a[1]) + a[1].length()).trim()
                        .concat("\n").getBytes(StandardCharsets.UTF_8);
                String dst = resolve(a[1]);
                client.put(data, dst);
                System.out.println("wrote " + dst + " (" + data.length + " B)");
            }
            case "cat" -> { need(a, 2); System.out.print(new String(client.get(resolve(a[1])), StandardCharsets.UTF_8)); }
            case "get" -> {
                need(a, 3);
                byte[] data = client.get(resolve(a[1]));
                Files.write(Path.of(a[2]), data);
                System.out.println("saved " + data.length + " B to " + Path.of(a[2]).toAbsolutePath());
            }
            case "mv" -> {
                need(a, 3);
                String src = resolve(a[1]);
                String dst = resolve(a[2]);
                if (isDir(dst)) dst = join(dst, src.substring(src.lastIndexOf('/') + 1));
                nn.rename(src, dst);
                System.out.println("moved " + src + " -> " + dst);
            }
            case "rm" -> {
                need(a, 2);
                String target = resolve(a[1]);
                if (target.equals("/")) throw new IllegalArgumentException("refusing to delete /");
                nn.delete(target);
                if (!isDir(cwd)) cwd = "/";
            }
            case "stat" -> { need(a, 2); stat(resolve(a[1])); }
            case "dns" -> dns();
            case "kill" -> { need(a, 2); kill(a[1]); }
            case "revive" -> { need(a, 2); revive(a[1]); }
            case "wait" -> waitSeconds(a.length > 1 ? Integer.parseInt(a[1]) : DEAD_MS / 1000 + 1);
            case "restart-nn" -> restartNameNode();
            case "checkpoint" -> {
                nn.saveNamespace();
                System.out.println("fsimage written, edit log truncated");
            }
            case "edits" -> {
                List<String> lines = Files.readAllLines(NN_DIR.resolve("edits.log"));
                if (lines.isEmpty()) System.out.println("(edit log empty - everything is in the fsimage)");
                lines.forEach(l -> System.out.println("  " + l.replace('\t', ' ')));
            }
            default -> System.out.println("unknown command '" + a[0] + "' - type 'help'");
        }
        return true;
    }

    private static void help() {
        System.out.println("""
                Files  (HDFS paths can be absolute or relative to the current directory; .. works)
                  cd [path]                 change directory (no arg = /)
                  pwd                       print current directory
                  ls [path]                 list a directory (default: current)
                  mkdir <path>              create a directory
                  put <local> <hdfs>        upload a file from your disk
                  write <hdfs> <text...>    create a file from text you type
                  cat <hdfs>                print a file
                  get <hdfs> <local>        download a file to your disk
                  mv <src> <dst>            rename / move (dst may be a directory)
                  rm <path>                 delete
                  stat <hdfs>               show blocks and which DataNodes hold them
                Cluster
                  dns                       DataNode status: state, port, blocks held
                  kill <dn-id>              crash a DataNode (it stops heartbeating)
                  revive <dn-id>            bring it back; it re-registers and sends a block report
                  wait [sec]                let time pass, printing heartbeat/failure events
                  restart-nn                crash + restart the NameNode (fsimage + edit log replay)
                  checkpoint                save fsimage, truncate the edit log
                  edits                     show the NameNode's edit log
                  exit""");
    }

    // ── cluster simulation ──────────────────────────────────────────────────

    private NameNodeImpl newNameNode() {
        return new NameNodeImpl(BLOCK_SIZE, STALE_MS, DEAD_MS, NN_DIR);
    }

    private void startDataNode(String id, InMemoryDataNode storage) throws IOException {
        TcpDataNodeServer server = new TcpDataNodeServer(storage, 0);
        server.start();
        storages.put(id, storage);
        servers.put(id, server);
        register(id);
    }

    private void register(String id) {
        DatanodeInfo info = new DatanodeInfo(id, "127.0.0.1", servers.get(id).getPort(), 100_000_000L, 100_000_000L);
        nn.registerDatanode(info);
        infos.put(id, info);
        lastSeenState.put(id, DatanodeState.NORMAL);
    }

    private boolean alive(String id) {
        return servers.get(id) != null;
    }

    /** One round of heartbeats from every live DataNode, the NN failure detector, and command execution. */
    private void tick() throws IOException {
        Map<String, IDataNodeStorage> live = new LinkedHashMap<>();
        storages.forEach((id, s) -> { if (alive(id)) live.put(id, s); });

        heartbeatAll(live);
        nn.checkAndReplicateDeadNodes();
        infos.forEach((id, info) -> {
            DatanodeState prev = lastSeenState.put(id, info.getState());
            if (prev != info.getState()) event(id + ": " + prev + " -> " + info.getState());
        });
        heartbeatAll(live);
    }

    private void heartbeatAll(Map<String, IDataNodeStorage> live) throws IOException {
        for (String id : live.keySet()) {
            HeartbeatResponse resp = nn.heartbeat(new HeartbeatRequest(id, 0, 100_000_000L));
            for (DatanodeCommand c : resp.commands()) {
                event("NN -> " + id + " (via heartbeat reply): " + c.action() + " block " + c.blockId()
                        + (c.targetDn() != null ? " to " + c.targetDn().datanodeId() : ""));
            }
            storages.get(id).executeCommands(resp.commands(), live);
        }
    }

    private void kill(String id) throws IOException {
        checkDn(id);
        if (!alive(id)) { System.out.println(id + " is already down"); return; }
        servers.get(id).close();
        servers.put(id, null);
        System.out.println(id + " crashed. NN marks it STALE after " + STALE_MS / 1000 + "s, DEAD after "
                + DEAD_MS / 1000 + "s. Try: wait");
    }

    private void revive(String id) throws IOException {
        checkDn(id);
        if (alive(id)) { System.out.println(id + " is already up"); return; }
        TcpDataNodeServer server = new TcpDataNodeServer(storages.get(id), 0);
        server.start();
        servers.put(id, server);
        register(id);
        nn.blockReport(new BlockReportRequest(id, storages.get(id).listBlocks()));
        System.out.println(id + " back on port " + server.getPort() + ", sent block report "
                + storages.get(id).listBlocks());
    }

    private void waitSeconds(long seconds) throws Exception {
        System.out.println("waiting " + seconds + "s ...");
        for (long t = 0; t < seconds * 2; t++) {
            Thread.sleep(500);
            tick();
        }
    }

    private void restartNameNode() throws IOException {
        System.out.println("NameNode crashed - all in-memory state gone.");
        nn = newNameNode();
        client = new HdfsClient(nn, BLOCK_SIZE, RF);
        System.out.println("New NameNode loaded fsimage + replayed edit log. BlockMap is empty until DataNodes report.");
        for (String id : storages.keySet()) {
            if (!alive(id)) continue;
            register(id);
            nn.blockReport(new BlockReportRequest(id, storages.get(id).listBlocks()));
            System.out.println("  " + id + " re-registered, block report " + storages.get(id).listBlocks());
        }
    }

    // ── output helpers ──────────────────────────────────────────────────────

    private void ls(String path) throws IOException {
        List<INodeInfo> entries = nn.listDir(path);
        if (entries.isEmpty()) System.out.println("(empty)");
        for (INodeInfo n : entries) {
            System.out.printf("  %-4s %10d B  %s%n", n.isDirectory() ? "dir" : "file", n.size(),
                    n.name() + (n.isDirectory() ? "/" : ""));
        }
    }

    private void stat(String path) throws IOException {
        LocatedBlocks lb = nn.getBlockLocations(path);
        System.out.println(path + ": " + lb.fileSize() + " B in " + lb.blocks().size() + " block(s)");
        for (LocatedBlock b : lb.blocks()) {
            String locs = b.locations().stream().map(d -> d.datanodeId()).collect(Collectors.joining(", "));
            System.out.printf("  block %-4d %7d B  on [%s]  (%d/%d replicas)%n",
                    b.blockId(), b.numBytes(), locs, b.locations().size(), RF);
        }
    }

    private void dns() {
        System.out.println("  id     process  NN view   port   blocks on disk");
        for (String id : storages.keySet()) {
            System.out.printf("  %-6s %-8s %-9s %-6s %s%n", id, alive(id) ? "up" : "DOWN",
                    infos.get(id).getState(), alive(id) ? servers.get(id).getPort() : "-",
                    storages.get(id).listBlocks());
        }
    }

    private static void event(String msg) {
        System.out.println("  [cluster] " + msg);
    }

    // ── misc ────────────────────────────────────────────────────────────────

    private boolean isDir(String path) {
        try {
            nn.listDir(path);
            return true;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    /** Turns a relative or absolute path (with . and ..) into a clean absolute path. */
    private String resolve(String p) {
        List<String> parts = new ArrayList<>();
        String full = p.startsWith("/") ? p : cwd + "/" + p;
        for (String s : full.split("/")) {
            if (s.isEmpty() || s.equals(".")) continue;
            if (s.equals("..")) { if (!parts.isEmpty()) parts.remove(parts.size() - 1); }
            else parts.add(s);
        }
        return "/" + String.join("/", parts);
    }

    private static String join(String dir, String name) {
        return dir.endsWith("/") ? dir + name : dir + "/" + name;
    }

    private void checkDn(String id) {
        if (!storages.containsKey(id)) throw new IllegalArgumentException("no such DataNode: " + id + " (see 'dns')");
    }

    private static void need(String[] a, int n) {
        if (a.length < n) throw new IllegalArgumentException("missing arguments - type 'help'");
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
