package dev.anurag.hdfs.datanode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TCP server that handles DataNode block I/O requests.
 * Accepts WRITE_BLOCK (with pipeline forwarding) and READ_BLOCK requests.
 * Each connection is handled on a separate thread.
 */
public class TcpDataNodeServer implements AutoCloseable {

    private final IDataNodeStorage storage;
    private final ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private volatile boolean running;

    public TcpDataNodeServer(IDataNodeStorage storage, int port) throws IOException {
        this.storage = storage;
        this.serverSocket = new ServerSocket(port);
        this.threadPool = Executors.newCachedThreadPool();
    }

    /** Returns the port actually bound (useful when port 0 was passed). */
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    /** Starts the accept loop in a background thread. */
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
                threadPool.submit(() -> handleConnection(client));
            } catch (SocketException e) {
                if (running) {
                    System.err.println("[DN:" + getPort() + "] Accept error: " + e.getMessage());
                }
                // else: normal shutdown via close()
            } catch (IOException e) {
                System.err.println("[DN:" + getPort() + "] Accept error: " + e.getMessage());
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket;
             DataInputStream  in  = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            byte opcode = in.readByte();
            switch (opcode) {
                case BlockProtocol.OP_WRITE_BLOCK -> handleWrite(in, out);
                case BlockProtocol.OP_READ_BLOCK  -> handleRead(in, out);
                default -> out.writeByte(BlockProtocol.STATUS_ERROR);
            }

        } catch (IOException e) {
            System.err.println("[DN:" + getPort() + "] Connection error: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // WRITE_BLOCK handler
    // -------------------------------------------------------------------------

    private void handleWrite(DataInputStream in, DataOutputStream out) throws IOException {
        long blockId     = in.readLong();
        List<PipelineEntry> pipeline = readPipeline(in);
        long dataLen     = in.readLong();
        byte[] data      = in.readNBytes((int) dataLen);

        // 1. Write locally first
        storage.writeBlock(blockId, data);

        // 2. Forward to the rest of the pipeline (pipeline shrinks at each hop)
        if (!pipeline.isEmpty()) {
            PipelineEntry next = pipeline.get(0);
            List<PipelineEntry> remaining = pipeline.subList(1, pipeline.size());
            forwardWrite(blockId, remaining, data, next.host, next.port);
        }

        // 3. ACK upstream
        out.writeByte(BlockProtocol.STATUS_OK);
        out.flush();
    }

    private void forwardWrite(long blockId, List<PipelineEntry> remainingPipeline,
                              byte[] data, String host, int port) throws IOException {
        try (Socket downstream = new Socket(host, port);
             DataOutputStream downOut = new DataOutputStream(downstream.getOutputStream());
             DataInputStream  downIn  = new DataInputStream(downstream.getInputStream())) {

            downOut.writeByte(BlockProtocol.OP_WRITE_BLOCK);
            downOut.writeLong(blockId);
            writePipeline(downOut, remainingPipeline);
            downOut.writeLong(data.length);
            downOut.write(data);
            downOut.flush();

            byte status = downIn.readByte();
            if (status != BlockProtocol.STATUS_OK) {
                throw new IOException("Downstream pipeline write failed for block " + blockId);
            }
        }
    }

    // -------------------------------------------------------------------------
    // READ_BLOCK handler
    // -------------------------------------------------------------------------

    private void handleRead(DataInputStream in, DataOutputStream out) throws IOException {
        long blockId = in.readLong();
        byte[] data  = storage.readBlock(blockId);

        out.writeByte(BlockProtocol.STATUS_OK);
        out.writeLong(data.length);
        out.write(data);
        out.flush();
    }

    // -------------------------------------------------------------------------
    // Pipeline serialisation helpers
    // -------------------------------------------------------------------------

    private static List<PipelineEntry> readPipeline(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<PipelineEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String host = in.readUTF();
            int    port = in.readInt();
            entries.add(new PipelineEntry(host, port));
        }
        return entries;
    }

    static void writePipeline(DataOutputStream out, List<PipelineEntry> pipeline) throws IOException {
        out.writeInt(pipeline.size());
        for (PipelineEntry e : pipeline) {
            out.writeUTF(e.host);
            out.writeInt(e.port);
        }
    }

    record PipelineEntry(String host, int port) {}

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        running = false;
        serverSocket.close();
        threadPool.shutdownNow();
    }
}
