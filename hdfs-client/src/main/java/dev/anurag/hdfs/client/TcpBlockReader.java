package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.datanode.BlockProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * Reads a block from a DataNode over TCP.
 * Tries each location in order; returns on the first success.
 */
class TcpBlockReader {

    static byte[] readWithFallback(long blockId, List<DatanodeDescriptor> locations)
            throws IOException {
        IOException lastError = null;
        for (DatanodeDescriptor dn : locations) {
            try {
                return read(blockId, dn);
            } catch (IOException e) {
                lastError = e;
            }
        }
        throw new IOException("All replicas failed for block " + blockId, lastError);
    }

    private static byte[] read(long blockId, DatanodeDescriptor dn) throws IOException {
        try (Socket socket = new Socket(dn.hostname(), dn.dataPort());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream  in  = new DataInputStream(socket.getInputStream())) {

            out.writeByte(BlockProtocol.OP_READ_BLOCK);
            out.writeLong(blockId);
            out.flush();

            byte status = in.readByte();
            if (status != BlockProtocol.STATUS_OK) {
                throw new IOException("Read error from " + dn.hostname() + ":" + dn.dataPort()
                        + " for block " + blockId);
            }

            long dataLen = in.readLong();
            return in.readNBytes((int) dataLen);
        }
    }
}
