package dev.anurag.hdfs.common.rpc;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.BlockReportRequest;
import dev.anurag.hdfs.common.dto.DatanodeReport;
import dev.anurag.hdfs.common.dto.HeartbeatRequest;
import dev.anurag.hdfs.common.dto.HeartbeatResponse;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.model.DatanodeInfo;
import dev.anurag.hdfs.common.protocol.IDatanodeProtocol;
import dev.anurag.hdfs.common.protocol.INameNodeService;
import dev.anurag.hdfs.common.protocol.UnregisteredDatanodeException;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Client-side proxy for the NameNode. One persistent TCP connection, calls serialized.
 * A broken connection is dropped and re-opened on the next call. Calls are NOT retried
 * automatically: a request may have been applied before the reply was lost, and ops such
 * as createFile are not idempotent.
 */
public class NameNodeRpcClient implements INameNodeService, IDatanodeProtocol, Closeable {

    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public NameNodeRpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /** Parses "host:port". */
    public static NameNodeRpcClient connect(String address) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) throw new IllegalArgumentException("Expected host:port, got " + address);
        return new NameNodeRpcClient(address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
    }

    public String address() {
        return host + ":" + port;
    }

    // ── INameNodeService ─────────────────────────────────────────────────────

    @Override
    public void createFile(String path, short rf) throws IOException {
        call(RpcOps.CREATE_FILE, o -> { o.writeUTF(path); o.writeShort(rf); }, i -> null);
    }

    @Override
    public AddBlockResponse addBlock(String path, Set<String> excluded) throws IOException {
        return call(RpcOps.ADD_BLOCK, o -> { o.writeUTF(path); Wire.writeStrings(o, new ArrayList<>(excluded)); },
                Wire::readAddBlockResponse);
    }

    @Override
    public void abandonBlock(String path, long blockId) throws IOException {
        call(RpcOps.ABANDON_BLOCK, o -> { o.writeUTF(path); o.writeLong(blockId); }, i -> null);
    }

    @Override
    public void closeFile(String path, long fileSize) throws IOException {
        call(RpcOps.CLOSE_FILE, o -> { o.writeUTF(path); o.writeLong(fileSize); }, i -> null);
    }

    @Override
    public LocatedBlocks getBlockLocations(String path) throws IOException {
        return call(RpcOps.GET_BLOCK_LOCATIONS, o -> o.writeUTF(path), Wire::readLocatedBlocks);
    }

    @Override
    public void mkdir(String path) throws IOException {
        call(RpcOps.MKDIR, o -> o.writeUTF(path), i -> null);
    }

    @Override
    public List<INodeInfo> listDir(String path) throws IOException {
        return call(RpcOps.LIST_DIR, o -> o.writeUTF(path), i -> Wire.readList(i, Wire::readINodeInfo));
    }

    @Override
    public void delete(String path) throws IOException {
        call(RpcOps.DELETE, o -> o.writeUTF(path), i -> null);
    }

    @Override
    public void rename(String src, String dst) throws IOException {
        call(RpcOps.RENAME, o -> { o.writeUTF(src); o.writeUTF(dst); }, i -> null);
    }

    @Override
    public List<DatanodeReport> getDatanodeReport() throws IOException {
        return call(RpcOps.DATANODE_REPORT, o -> {}, i -> Wire.readList(i, Wire::readReport));
    }

    @Override
    public int getBlockSize() throws IOException {
        return call(RpcOps.GET_BLOCK_SIZE, o -> {}, DataInputStream::readInt);
    }

    // ── IDatanodeProtocol ────────────────────────────────────────────────────

    @Override
    public String getClusterId() throws IOException {
        return call(RpcOps.VERSION, o -> {}, i -> i.readUTF());
    }

    @Override
    public void registerDatanode(DatanodeInfo info) throws IOException {
        call(RpcOps.REGISTER, o -> {
            o.writeUTF(info.getDatanodeId());
            o.writeUTF(info.getHostname());
            o.writeInt(info.getDataPort());
            o.writeLong(info.getTotalCapacityBytes());
            o.writeLong(info.getRemainingBytes());
        }, i -> null);
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest r) throws IOException {
        return call(RpcOps.HEARTBEAT, o -> {
            o.writeUTF(r.datanodeId());
            o.writeLong(r.usedBytes());
            o.writeLong(r.remainingBytes());
        }, i -> new HeartbeatResponse(Wire.readList(i, Wire::readCommand)));
    }

    @Override
    public void blockReport(BlockReportRequest r) throws IOException {
        call(RpcOps.BLOCK_REPORT, o -> { o.writeUTF(r.datanodeId()); Wire.writeLongs(o, r.blockIds()); }, i -> null);
    }

    // ── transport ────────────────────────────────────────────────────────────

    private synchronized <T> T call(byte op, Wire.Encoder args, Wire.Decoder<T> result) throws IOException {
        byte[] request = Wire.build(o -> { o.writeByte(op); args.encode(o); });
        byte[] response;
        try {
            ensureConnected();
            Wire.writeFrame(out, request);
            response = Wire.readFrame(in);
        } catch (IOException e) {
            disconnect();
            throw new IOException("NameNode " + address() + " unreachable: " + e.getMessage(), e);
        }
        DataInputStream r = new DataInputStream(new ByteArrayInputStream(response));
        byte status = r.readByte();
        switch (status) {
            case RpcOps.STATUS_OK -> { return result.decode(r); }
            case RpcOps.STATUS_UNREGISTERED -> throw new UnregisteredDatanodeException(r.readUTF());
            default -> throw new IOException(r.readUTF());
        }
    }

    private void ensureConnected() throws IOException {
        if (socket != null) return;
        Socket s = new Socket();
        s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
        s.setSoTimeout(READ_TIMEOUT_MS);
        s.setTcpNoDelay(true);
        socket = s;
        in = new DataInputStream(s.getInputStream());
        out = new DataOutputStream(s.getOutputStream());
    }

    private void disconnect() {
        if (socket != null) {
            try { socket.close(); } catch (IOException ignored) {}
        }
        socket = null;
        in = null;
        out = null;
    }

    @Override
    public synchronized void close() {
        disconnect();
    }
}
