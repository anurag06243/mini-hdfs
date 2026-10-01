package dev.anurag.hdfs.namenode;

import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.common.protocol.UnregisteredDatanodeException;
import dev.anurag.hdfs.common.rpc.RpcOps;
import dev.anurag.hdfs.common.rpc.Wire;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Network front-end for {@link NameNodeImpl}: accepts control-plane RPCs from clients and
 * DataNodes (thread per connection) and runs the background monitor (liveness sweep,
 * re-replication scan, periodic checkpoint).
 *
 * Concurrency: one ReentrantReadWriteLock guards the whole namesystem. Lookups take the read
 * lock and run in parallel; every mutation (including heartbeats, which drain command queues)
 * takes the write lock. Edit-log fsync happens under the write lock, which caps mutation
 * throughput at roughly one fsync per op — real HDFS avoids that with group commit.
 */
public class NameNodeServer implements AutoCloseable {

    private final NameNodeImpl nn;
    private final ServerSocket serverSocket;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService handlers = Executors.newCachedThreadPool(daemon("nn-rpc"));
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(daemon("nn-monitor"));
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final long monitorIntervalMs;
    private final long checkpointEveryEdits;
    private volatile boolean running;

    public NameNodeServer(NameNodeImpl nn, int port, long monitorIntervalMs, long checkpointEveryEdits) throws IOException {
        this.nn = nn;
        this.monitorIntervalMs = monitorIntervalMs;
        this.checkpointEveryEdits = checkpointEveryEdits;
        this.serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void start() {
        running = true;
        Thread acceptor = new Thread(this::acceptLoop, "nn-accept");
        acceptor.setDaemon(true);
        acceptor.start();
        monitor.scheduleWithFixedDelay(this::runMonitor, monitorIntervalMs, monitorIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void runMonitor() {
        lock.writeLock().lock();
        try {
            nn.checkAndReplicateDeadNodes();
            if (nn.maybeCheckpoint(checkpointEveryEdits)) System.out.println("[NameNode] checkpoint written");
        } catch (Exception e) {
            System.err.println("[NameNode] monitor error: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = serverSocket.accept();
                s.setTcpNoDelay(true);
                connections.add(s);
                handlers.submit(() -> serve(s));
            } catch (SocketException e) {
                if (running) System.err.println("[NameNode] accept error: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("[NameNode] accept error: " + e.getMessage());
            }
        }
    }

    private void serve(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            while (true) {
                byte[] request;
                try {
                    request = Wire.readFrame(in);
                } catch (EOFException e) {
                    return; // client closed the connection
                }
                Wire.writeFrame(out, handle(request));
            }
        } catch (IOException e) {
            if (running) System.err.println("[NameNode] connection closed: " + e.getMessage());
        } finally {
            connections.remove(socket);
        }
    }

    private byte[] handle(byte[] request) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(request));
        byte op = in.readByte();
        boolean readOnly = op == RpcOps.GET_BLOCK_LOCATIONS || op == RpcOps.LIST_DIR
                || op == RpcOps.DATANODE_REPORT || op == RpcOps.VERSION || op == RpcOps.GET_BLOCK_SIZE;
        var l = readOnly ? lock.readLock() : lock.writeLock();
        l.lock();
        try {
            return Wire.build(out -> {
                out.writeByte(RpcOps.STATUS_OK);
                dispatch(op, in, out);
            });
        } catch (UnregisteredDatanodeException e) {
            return error(RpcOps.STATUS_UNREGISTERED, e.getMessage());
        } catch (IOException | RuntimeException e) {
            return error(RpcOps.STATUS_ERROR, e.getMessage() == null ? e.toString() : e.getMessage());
        } finally {
            l.unlock();
        }
    }

    private void dispatch(byte op, DataInputStream in, DataOutputStream out) throws IOException {
        switch (op) {
            case RpcOps.CREATE_FILE -> nn.createFile(in.readUTF(), in.readShort());
            case RpcOps.ADD_BLOCK -> {
                String path = in.readUTF();
                Set<String> excluded = new HashSet<>(Wire.readStrings(in));
                Wire.writeAddBlockResponse(out, nn.addBlock(path, excluded));
            }
            case RpcOps.ABANDON_BLOCK -> nn.abandonBlock(in.readUTF(), in.readLong());
            case RpcOps.CLOSE_FILE -> nn.closeFile(in.readUTF(), in.readLong());
            case RpcOps.GET_BLOCK_LOCATIONS -> Wire.writeLocatedBlocks(out, nn.getBlockLocations(in.readUTF()));
            case RpcOps.MKDIR -> nn.mkdir(in.readUTF());
            case RpcOps.LIST_DIR -> Wire.writeList(out, nn.listDir(in.readUTF()), Wire::writeINodeInfo);
            case RpcOps.DELETE -> nn.delete(in.readUTF());
            case RpcOps.RENAME -> nn.rename(in.readUTF(), in.readUTF());
            case RpcOps.DATANODE_REPORT -> Wire.writeList(out, nn.getDatanodeReport(), Wire::writeReport);
            case RpcOps.VERSION -> out.writeUTF(nn.getClusterId());
            case RpcOps.GET_BLOCK_SIZE -> out.writeInt(nn.getBlockSize());
            case RpcOps.REGISTER -> {
                DatanodeInfo info = new DatanodeInfo(in.readUTF(), in.readUTF(), in.readInt(), in.readLong(), in.readLong());
                nn.registerDatanode(info);
                System.out.println("[NameNode] registered " + info.getDatanodeId()
                        + " at " + info.getHostname() + ":" + info.getDataPort());
            }
            case RpcOps.HEARTBEAT -> {
                var resp = nn.heartbeat(new HeartbeatRequest(in.readUTF(), in.readLong(), in.readLong()));
                Wire.writeList(out, resp.commands(), Wire::writeCommand);
            }
            case RpcOps.BLOCK_REPORT -> nn.blockReport(new BlockReportRequest(in.readUTF(), Wire.readLongs(in)));
            default -> throw new IOException("Unknown RPC op: " + op);
        }
    }

    private static byte[] error(byte status, String message) throws IOException {
        return Wire.build(out -> { out.writeByte(status); out.writeUTF(message); });
    }

    /** Stops serving and closes the NameNode's edit log. */
    @Override
    public void close() throws IOException {
        running = false;
        serverSocket.close();
        monitor.shutdownNow();
        for (Socket s : connections) {
            try { s.close(); } catch (IOException ignored) {}
        }
        handlers.shutdownNow();
        lock.writeLock().lock();
        try {
            nn.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Runs a checkpoint under the namesystem lock (used on clean shutdown). */
    public void checkpoint() throws IOException {
        lock.writeLock().lock();
        try {
            nn.saveNamespace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
