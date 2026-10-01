package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.DatanodeReport;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.rpc.NameNodeRpcClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Command-line client for a running cluster.
 *
 *   hdfs [--namenode host:port] [--replication N]                             interactive shell
 *   hdfs [--namenode host:port] <command> [args...]                           one command, then exit
 *
 * The NameNode address can also come from the HDFS_NAMENODE environment variable.
 */
public class HdfsCli {

    private final NameNodeRpcClient nn;
    private final HdfsClient client;
    private String cwd = "/";

    HdfsCli(NameNodeRpcClient nn, short replication) throws IOException {
        this.nn = nn;
        this.client = new HdfsClient(nn, nn.getBlockSize(), replication);
    }

    public static void main(String[] args) throws Exception {
        String address = System.getenv().getOrDefault("HDFS_NAMENODE", "localhost:8020");
        short replication = 3;
        int i = 0;
        for (; i < args.length && args[i].startsWith("--"); i += 2) {
            if (i + 1 >= args.length) usage("missing value for " + args[i]);
            switch (args[i]) {
                case "--namenode" -> address = args[i + 1];
                case "--replication" -> replication = Short.parseShort(args[i + 1]);
                default -> usage("unknown option " + args[i]);
            }
        }
        try (NameNodeRpcClient nn = NameNodeRpcClient.connect(address)) {
            HdfsCli cli;
            try {
                cli = new HdfsCli(nn, replication);
            } catch (IOException e) {
                System.err.println("error: " + e.getMessage());
                System.exit(1);
                return;
            }
            if (i < args.length) {
                try {
                    cli.run(Arrays.copyOfRange(args, i, args.length), String.join(" ", Arrays.copyOfRange(args, i, args.length)));
                } catch (IOException | RuntimeException e) {
                    System.err.println("error: " + e.getMessage());
                    System.exit(1);
                }
            } else {
                cli.repl(address);
            }
        }
    }

    private void repl(String address) {
        System.out.println("mini-HDFS client connected to NameNode " + address + ". Type 'help' for commands.");
        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.print("hdfs:" + cwd + "> ");
            if (!in.hasNextLine()) return;
            String line = in.nextLine().trim();
            if (line.isEmpty()) continue;
            if (line.equals("exit") || line.equals("quit")) return;
            try {
                run(line.split("\\s+"), line);
            } catch (IOException | RuntimeException e) {
                System.out.println("error: " + e.getMessage());
            }
        }
    }

    private void run(String[] a, String line) throws IOException {
        switch (a[0]) {
            case "help" -> help();
            case "pwd" -> System.out.println(cwd);
            case "cd" -> {
                String target = resolve(a.length > 1 ? a[1] : "/");
                nn.listDir(target); // throws if missing or not a directory
                cwd = target;
            }
            case "ls" -> ls(resolve(a.length > 1 ? a[1] : "."));
            case "mkdir" -> {
                need(a, 2);
                if (a[1].equals("-p")) { need(a, 3); mkdirs(resolve(a[2])); }
                else nn.mkdir(resolve(a[1]));
            }
            case "put" -> {
                need(a, 3);
                Path local = Path.of(a[1]).toAbsolutePath().normalize();
                if (!Files.isRegularFile(local)) throw new IllegalArgumentException("local file not found: " + local);
                String dst = resolve(a[2]);
                if (isDir(dst)) dst = join(dst, local.getFileName().toString());
                byte[] data = Files.readAllBytes(local);
                long start = System.nanoTime();
                client.put(data, dst);
                System.out.printf("wrote %s (%,d B) in %d ms%n", dst, data.length, (System.nanoTime() - start) / 1_000_000);
            }
            case "write" -> {
                need(a, 3);
                String text = line.substring(line.indexOf(a[1]) + a[1].length()).trim() + "\n";
                String dst = resolve(a[1]);
                client.put(text.getBytes(StandardCharsets.UTF_8), dst);
                System.out.println("wrote " + dst);
            }
            case "cat" -> { need(a, 2); System.out.print(new String(client.get(resolve(a[1])), StandardCharsets.UTF_8)); }
            case "get" -> {
                need(a, 3);
                byte[] data = client.get(resolve(a[1]));
                Path local = Path.of(a[2]).toAbsolutePath().normalize();
                Files.write(local, data);
                System.out.printf("saved %,d B to %s%n", data.length, local);
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
                nn.delete(target);
                if (!isDir(cwd)) cwd = "/";
            }
            case "stat" -> { need(a, 2); stat(resolve(a[1])); }
            case "report" -> report();
            default -> throw new IllegalArgumentException("unknown command '" + a[0] + "' - type 'help'");
        }
    }

    private static void help() {
        System.out.println("""
                Paths are absolute or relative to the current directory ('..' works).
                  ls [path]                 list a directory
                  cd [path] | pwd           change / print current directory
                  mkdir [-p] <path>         create a directory (-p: and any missing parents)
                  put <local> <hdfs>        upload a local file (hdfs may be a directory)
                  write <hdfs> <text...>    create a file from text
                  cat <hdfs>                print a file
                  get <hdfs> <local>        download a file
                  mv <src> <dst>            rename / move
                  rm <path>                 delete a file or a whole directory
                  stat <hdfs>               blocks and the DataNodes holding each one
                  report                    DataNode status as the NameNode sees it
                  exit""");
    }

    private void ls(String path) throws IOException {
        List<INodeInfo> entries = nn.listDir(path);
        if (entries.isEmpty()) System.out.println("(empty)");
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        for (INodeInfo n : entries) {
            System.out.printf("  %-4s %12s  %s  %s%n", n.isDirectory() ? "dir" : "file",
                    String.format("%,d", n.size()), fmt.format(new Date(n.modifiedAt())),
                    n.name() + (n.isDirectory() ? "/" : ""));
        }
    }

    private void stat(String path) throws IOException {
        LocatedBlocks lb = nn.getBlockLocations(path);
        System.out.printf("%s: %,d B in %d block(s)%n", path, lb.fileSize(), lb.blocks().size());
        for (LocatedBlock b : lb.blocks()) {
            String locs = b.locations().stream().map(d -> d.datanodeId()).sorted().collect(Collectors.joining(", "));
            System.out.printf("  block %-5d %,12d B  replicas %d  [%s]%n", b.blockId(), b.numBytes(), b.locations().size(), locs);
        }
    }

    private void report() throws IOException {
        List<DatanodeReport> dns = nn.getDatanodeReport();
        if (dns.isEmpty()) System.out.println("(no DataNodes registered)");
        System.out.printf("  %-8s %-21s %-7s %7s %12s %10s%n", "id", "address", "state", "blocks", "used", "last hb");
        for (DatanodeReport r : dns) {
            System.out.printf("  %-8s %-21s %-7s %7d %12s %8.1f s%n", r.datanodeId(), r.hostname() + ":" + r.dataPort(),
                    r.state(), r.numBlocks(), String.format("%,d B", r.usedBytes()), r.lastHeartbeatAgoMs() / 1000.0);
        }
    }

    private void mkdirs(String path) throws IOException {
        StringBuilder cur = new StringBuilder();
        for (String part : path.split("/")) {
            if (part.isEmpty()) continue;
            cur.append('/').append(part);
            if (!isDir(cur.toString())) nn.mkdir(cur.toString());
        }
    }

    private boolean isDir(String path) {
        try {
            nn.listDir(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private String resolve(String p) {
        List<String> parts = new ArrayList<>();
        for (String s : (p.startsWith("/") ? p : cwd + "/" + p).split("/")) {
            if (s.isEmpty() || s.equals(".")) continue;
            if (s.equals("..")) { if (!parts.isEmpty()) parts.remove(parts.size() - 1); }
            else parts.add(s);
        }
        return "/" + String.join("/", parts);
    }

    private static String join(String dir, String name) {
        return dir.endsWith("/") ? dir + name : dir + "/" + name;
    }

    private static void need(String[] a, int n) {
        if (a.length < n) throw new IllegalArgumentException("missing arguments - type 'help'");
    }

    private static void usage(String msg) {
        System.err.println(msg + "\nusage: hdfs [--namenode host:port] [--replication N] [command args...]");
        System.exit(2);
    }
}
