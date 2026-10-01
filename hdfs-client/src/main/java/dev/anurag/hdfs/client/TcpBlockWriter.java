package dev.anurag.hdfs.client;

import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.datanode.BlockProtocol;
import dev.anurag.hdfs.datanode.TcpDataNodeServer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * Sends a WRITE_BLOCK request to the head of the pipeline.
 * The head DN forwards to the rest of the pipeline automatically.
 */
class TcpBlockWriter {

    /**
     * @param blockId  the block to write
     * @param pipeline ordered list of DataNodes — first is the connection target,
     *                 rest are forwarded as the downstream pipeline
     * @param data     block bytes
     */
    static void write(long blockId, List<DatanodeDescriptor> pipeline, byte[] data)
            throws IOException {
        if (pipeline.isEmpty()) throw new IOException("Pipeline is empty for block " + blockId);

        DatanodeDescriptor head = pipeline.get(0);
        List<DatanodeDescriptor> downstream = pipeline.subList(1, pipeline.size());

        try (Socket socket = new Socket(head.hostname(), head.dataPort());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream  in  = new DataInputStream(socket.getInputStream())) {

            out.writeByte(BlockProtocol.OP_WRITE_BLOCK);
            out.writeLong(blockId);

            // Write downstream pipeline entries
            out.writeInt(downstream.size());
            for (DatanodeDescriptor dn : downstream) {
                out.writeUTF(dn.hostname());
                out.writeInt(dn.dataPort());
            }

            out.writeLong(data.length);
            out.write(data);
            out.flush();

            byte status = in.readByte();
            if (status != BlockProtocol.STATUS_OK) {
                throw new IOException("Pipeline write failed for block " + blockId
                        + " at " + head.hostname() + ":" + head.dataPort());
            }
        }
    }
}
