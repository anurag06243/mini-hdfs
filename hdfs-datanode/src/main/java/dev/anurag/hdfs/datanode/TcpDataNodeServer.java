package dev.anurag.hdfs.datanode;

import dev.anurag.hdfs.common.dto.DatanodeDescriptor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TCP server for a DataNode's block I/O (see {@link BlockProtocol}). One thread per connection.
 * Writes are store-and-forward: verify checksum, store locally, forward to the next node, then
 * ACK upstream only after the downstream ACK — so the client's ACK means every replica is stored.
 */
public class TcpDataNodeServer implements AutoCloseable {

    private final IDataNodeStorage storage;
    private final ServerSocket serverSocket;
    private final ExecutorService threadPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "dn-xfer");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running;

    public TcpDataNodeServer(IDataNodeStorage storage, int port) throws IOException {
        this.storage = storage;
        this.serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
    }

    /** Returns the port actually bound (useful when port 0 was passed). */
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void start() {
        running = true;
        Thread acceptThread = new Thread(this::acceptLoop, "dn-accept-" + getPort());
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                client.setSoTimeout(BlockProtocol.IO_TIMEOUT_MS);
                threadPool.submit(() -> handleConnection(client));
            } catch (SocketException e) {
                if (running) System.err.println("[DN:" + getPort() + "] accept error: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("[DN:" + getPort() + "] accept error: " + e.getMessage());
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket;
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            byte opcode = in.readByte();
            switch (opcode) {
                case BlockProtocol.OP_WRITE_BLOCK -> handleWrite(in, out);
                case BlockProtocol.OP_READ_BLOCK  -> handleRead(in, out);
                default -> { out.writeByte(BlockProtocol.STATUS_ERROR); out.writeUTF("Unknown op " + opcode); }
            }
            out.flush();
        } catch (IOException e) {
            if (running) System.err.println("[DN:" + getPort() + "] connection error: " + e.getMessage());
        }
    }

    private void handleWrite(DataInputStream in, DataOutputStream out) throws IOException {
        long blockId = in.readLong();
        List<DatanodeDescriptor> downstream = readPipeline(in);
        long len = in.readLong();
        if (len < 0 || len > BlockProtocol.MAX_BLOCK_BYTES) throw new IOException("Bad block length " + len);
        byte[] data = in.readNBytes((int) len);
        long crc = in.readLong();

        if (data.length != len || BlockProtocol.crc32(data) != crc) {
            fail(out, "");
            return;
        }
        try {
            storage.writeBlock(blockId, data);
        } catch (IOException e) {
            System.err.println("[DN:" + getPort() + "] local write of block " + blockId + " failed: " + e.getMessage());
            fail(out, "");
            return;
        }
        if (!downstream.isEmpty()) {
            try {
                BlockTransferClient.writeBlock(blockId, downstream, data);
            } catch (PipelineException e) {
                fail(out, e.badNode());
                return;
            }
        }
        out.writeByte(BlockProtocol.STATUS_OK);
    }

    private void handleRead(DataInputStream in, DataOutputStream out) throws IOException {
        long blockId = in.readLong();
        byte[] data;
        try {
            data = storage.readBlock(blockId);
        } catch (IOException e) {
            out.writeByte(BlockProtocol.STATUS_ERROR);
            out.writeUTF(e.getMessage());
            return;
        }
        out.writeByte(BlockProtocol.STATUS_OK);
        out.writeLong(data.length);
        out.write(data);
        out.writeLong(BlockProtocol.crc32(data));
    }

    private static void fail(DataOutputStream out, String badNode) throws IOException {
        out.writeByte(BlockProtocol.STATUS_ERROR);
        out.writeUTF(badNode);
    }

    private static List<DatanodeDescriptor> readPipeline(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > 64) throw new IOException("Bad pipeline length " + size);
        List<DatanodeDescriptor> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) entries.add(new DatanodeDescriptor("", in.readUTF(), in.readInt()));
        return entries;
    }

    @Override
    public void close() throws IOException {
        running = false;
        serverSocket.close();
        threadPool.shutdownNow();
    }
}
