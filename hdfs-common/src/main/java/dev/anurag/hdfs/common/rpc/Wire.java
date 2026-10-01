package dev.anurag.hdfs.common.rpc;

import dev.anurag.hdfs.common.dto.AddBlockResponse;
import dev.anurag.hdfs.common.dto.DatanodeCommand;
import dev.anurag.hdfs.common.dto.DatanodeDescriptor;
import dev.anurag.hdfs.common.dto.DatanodeReport;
import dev.anurag.hdfs.common.dto.INodeInfo;
import dev.anurag.hdfs.common.dto.LocatedBlock;
import dev.anurag.hdfs.common.dto.LocatedBlocks;
import dev.anurag.hdfs.common.model.DatanodeState;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Encoders/decoders shared by both ends of the control-plane RPC. */
public final class Wire {

    private Wire() {}

    // ── framing ──────────────────────────────────────────────────────────────

    public static void writeFrame(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    public static byte[] readFrame(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > RpcOps.MAX_FRAME_BYTES) throw new IOException("Bad frame length: " + len);
        return in.readNBytes(len);
    }

    /** Builds a payload with a DataOutputStream and returns its bytes. */
    public static byte[] build(Encoder e) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        e.encode(out);
        out.flush();
        return buf.toByteArray();
    }

    @FunctionalInterface
    public interface Encoder { void encode(DataOutputStream out) throws IOException; }

    @FunctionalInterface
    public interface Decoder<T> { T decode(DataInputStream in) throws IOException; }

    // ── lists ────────────────────────────────────────────────────────────────

    public interface ItemWriter<T> { void write(DataOutputStream out, T item) throws IOException; }

    public static <T> void writeList(DataOutputStream out, List<T> items, ItemWriter<T> w) throws IOException {
        out.writeInt(items.size());
        for (T item : items) w.write(out, item);
    }

    public static <T> List<T> readList(DataInputStream in, Decoder<T> r) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > RpcOps.MAX_FRAME_BYTES) throw new IOException("Bad list length: " + n);
        List<T> items = new ArrayList<>(n);
        for (int i = 0; i < n; i++) items.add(r.decode(in));
        return items;
    }

    public static void writeLongs(DataOutputStream out, List<Long> values) throws IOException {
        writeList(out, values, DataOutputStream::writeLong);
    }

    public static List<Long> readLongs(DataInputStream in) throws IOException {
        return readList(in, DataInputStream::readLong);
    }

    public static void writeStrings(DataOutputStream out, List<String> values) throws IOException {
        writeList(out, values, DataOutputStream::writeUTF);
    }

    public static List<String> readStrings(DataInputStream in) throws IOException {
        return readList(in, i -> i.readUTF());
    }

    // ── DTOs ─────────────────────────────────────────────────────────────────

    public static void writeDescriptor(DataOutputStream out, DatanodeDescriptor d) throws IOException {
        out.writeUTF(d.datanodeId());
        out.writeUTF(d.hostname());
        out.writeInt(d.dataPort());
    }

    public static DatanodeDescriptor readDescriptor(DataInputStream in) throws IOException {
        return new DatanodeDescriptor(in.readUTF(), in.readUTF(), in.readInt());
    }

    public static void writeAddBlockResponse(DataOutputStream out, AddBlockResponse r) throws IOException {
        out.writeLong(r.blockId());
        writeList(out, r.targets(), Wire::writeDescriptor);
    }

    public static AddBlockResponse readAddBlockResponse(DataInputStream in) throws IOException {
        return new AddBlockResponse(in.readLong(), readList(in, Wire::readDescriptor));
    }

    public static void writeLocatedBlocks(DataOutputStream out, LocatedBlocks lb) throws IOException {
        out.writeLong(lb.fileSize());
        writeList(out, lb.blocks(), (o, b) -> {
            o.writeLong(b.blockId());
            o.writeLong(b.offset());
            o.writeLong(b.numBytes());
            writeList(o, b.locations(), Wire::writeDescriptor);
        });
    }

    public static LocatedBlocks readLocatedBlocks(DataInputStream in) throws IOException {
        long size = in.readLong();
        List<LocatedBlock> blocks = readList(in, i -> new LocatedBlock(
                i.readLong(), i.readLong(), i.readLong(), readList(i, Wire::readDescriptor)));
        return new LocatedBlocks(size, blocks);
    }

    public static void writeINodeInfo(DataOutputStream out, INodeInfo n) throws IOException {
        out.writeUTF(n.name());
        out.writeBoolean(n.isDirectory());
        out.writeLong(n.size());
        out.writeLong(n.modifiedAt());
    }

    public static INodeInfo readINodeInfo(DataInputStream in) throws IOException {
        return new INodeInfo(in.readUTF(), in.readBoolean(), in.readLong(), in.readLong());
    }

    public static void writeCommand(DataOutputStream out, DatanodeCommand c) throws IOException {
        out.writeByte(c.action().ordinal());
        out.writeLong(c.blockId());
        out.writeBoolean(c.targetDn() != null);
        if (c.targetDn() != null) writeDescriptor(out, c.targetDn());
    }

    public static DatanodeCommand readCommand(DataInputStream in) throws IOException {
        DatanodeCommand.Action action = DatanodeCommand.Action.values()[in.readByte()];
        long blockId = in.readLong();
        DatanodeDescriptor target = in.readBoolean() ? readDescriptor(in) : null;
        return new DatanodeCommand(action, blockId, target);
    }

    public static void writeReport(DataOutputStream out, DatanodeReport r) throws IOException {
        out.writeUTF(r.datanodeId());
        out.writeUTF(r.hostname());
        out.writeInt(r.dataPort());
        out.writeByte(r.state().ordinal());
        out.writeInt(r.numBlocks());
        out.writeLong(r.usedBytes());
        out.writeLong(r.remainingBytes());
        out.writeLong(r.lastHeartbeatAgoMs());
    }

    public static DatanodeReport readReport(DataInputStream in) throws IOException {
        return new DatanodeReport(in.readUTF(), in.readUTF(), in.readInt(),
                DatanodeState.values()[in.readByte()], in.readInt(), in.readLong(), in.readLong(), in.readLong());
    }
}
