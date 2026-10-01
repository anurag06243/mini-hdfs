package dev.anurag.hdfs.datanode;

import dev.anurag.hdfs.common.dto.DatanodeDescriptor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/** Client side of the data-plane protocol, used by HDFS clients and by DataNodes (forwarding, re-replication). */
public final class BlockTransferClient {

    private BlockTransferClient() {}

    public static String address(DatanodeDescriptor dn) {
        return dn.hostname() + ":" + dn.dataPort();
    }

    /**
     * Streams a block to {@code pipeline.get(0)}, which stores it and forwards it down the rest of
     * the pipeline. Returns only after every node has stored the block and ACKed.
     *
     * @throws PipelineException naming the node that failed, so the caller can exclude it and retry
     */
    public static void writeBlock(long blockId, List<DatanodeDescriptor> pipeline, byte[] data) throws PipelineException {
        if (pipeline.isEmpty()) throw new PipelineException("-", "Empty pipeline for block " + blockId, null);
        DatanodeDescriptor head = pipeline.get(0);
        List<DatanodeDescriptor> downstream = pipeline.subList(1, pipeline.size());
        byte status;
        String badNode;
        try (Socket socket = open(head);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            out.writeByte(BlockProtocol.OP_WRITE_BLOCK);
            out.writeLong(blockId);
            out.writeInt(downstream.size());
            for (DatanodeDescriptor dn : downstream) {
                out.writeUTF(dn.hostname());
                out.writeInt(dn.dataPort());
            }
            out.writeLong(data.length);
            out.write(data);
            out.writeLong(BlockProtocol.crc32(data));
            out.flush();

            status = in.readByte();
            if (status == BlockProtocol.STATUS_OK) return;
            badNode = in.readUTF();
        } catch (IOException e) {
            throw new PipelineException(address(head), "Write of block " + blockId + " failed", e);
        }
        throw new PipelineException(badNode.isEmpty() ? address(head) : badNode,
                "Pipeline rejected block " + blockId, null);
    }

    /** Reads a block from one DataNode and verifies its checksum. */
    public static byte[] readBlock(long blockId, DatanodeDescriptor dn) throws IOException {
        try (Socket socket = open(dn);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            out.writeByte(BlockProtocol.OP_READ_BLOCK);
            out.writeLong(blockId);
            out.flush();

            if (in.readByte() != BlockProtocol.STATUS_OK)
                throw new IOException(address(dn) + ": " + in.readUTF());
            long len = in.readLong();
            if (len < 0 || len > BlockProtocol.MAX_BLOCK_BYTES) throw new IOException("Bad block length " + len);
            byte[] data = in.readNBytes((int) len);
            if (data.length != len) throw new IOException("Truncated block " + blockId + " from " + address(dn));
            if (in.readLong() != BlockProtocol.crc32(data))
                throw new IOException("Checksum mismatch reading block " + blockId + " from " + address(dn));
            return data;
        }
    }

    private static Socket open(DatanodeDescriptor dn) throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress(dn.hostname(), dn.dataPort()), BlockProtocol.CONNECT_TIMEOUT_MS);
        s.setSoTimeout(BlockProtocol.IO_TIMEOUT_MS);
        return s;
    }
}
