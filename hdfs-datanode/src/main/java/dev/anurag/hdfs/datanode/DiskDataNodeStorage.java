package dev.anurag.hdfs.datanode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Stores each block as two files under {@code <dir>/current}:
 *
 *   blk_<id>       raw block bytes
 *   blk_<id>.meta  [long CRC32][long length]
 *
 * Writes go to *.tmp files, are fsync'd, then renamed into place — meta first, data last —
 * so a block is visible only once both files are complete. Leftover *.tmp files from a crash
 * are deleted on startup. A block whose bytes no longer match its checksum (bit rot, disk
 * error, tampering) is moved to {@code <dir>/corrupt}, disappears from block reports, and the
 * NameNode re-replicates it from a healthy copy.
 */
public class DiskDataNodeStorage implements IDataNodeStorage {

    private static final String PREFIX = "blk_";
    private static final String META = ".meta";
    private static final String TMP = ".tmp";

    private final String datanodeId;
    private final Path current;
    private final Path corrupt;
    private final AtomicBoolean reportRequested = new AtomicBoolean();

    public DiskDataNodeStorage(String datanodeId, Path dir) throws IOException {
        this.datanodeId = datanodeId;
        this.current = dir.resolve("current");
        this.corrupt = dir.resolve("corrupt");
        Files.createDirectories(current);
        Files.createDirectories(corrupt);
        try (Stream<Path> files = Files.list(current)) {
            for (Path p : files.filter(p -> p.getFileName().toString().endsWith(TMP)).toList()) Files.delete(p);
        }
    }

    @Override
    public void writeBlock(long blockId, byte[] data) throws IOException {
        Path dataTmp = current.resolve(PREFIX + blockId + TMP);
        Path metaTmp = current.resolve(PREFIX + blockId + META + TMP);
        writeSynced(dataTmp, data);
        try (FileOutputStream fos = new FileOutputStream(metaTmp.toFile());
             DataOutputStream out = new DataOutputStream(fos)) {
            out.writeLong(BlockProtocol.crc32(data));
            out.writeLong(data.length);
            out.flush();
            fos.getChannel().force(true);
        }
        Files.move(metaTmp, metaFile(blockId), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Files.move(dataTmp, dataFile(blockId), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public byte[] readBlock(long blockId) throws IOException {
        byte[] data;
        long expectedCrc, expectedLen;
        try {
            data = Files.readAllBytes(dataFile(blockId));
            try (DataInputStream in = new DataInputStream(Files.newInputStream(metaFile(blockId)))) {
                expectedCrc = in.readLong();
                expectedLen = in.readLong();
            }
        } catch (NoSuchFileException e) {
            if (Files.exists(dataFile(blockId))) {
                quarantine(blockId);
                throw new CorruptBlockException("Block " + blockId + " has no checksum file");
            }
            throw new IOException("Block not found: " + blockId);
        }
        if (data.length != expectedLen || BlockProtocol.crc32(data) != expectedCrc) {
            quarantine(blockId);
            throw new CorruptBlockException("Checksum mismatch for block " + blockId + " on " + datanodeId);
        }
        return data;
    }

    @Override
    public boolean hasBlock(long blockId) {
        return Files.exists(dataFile(blockId)) && Files.exists(metaFile(blockId));
    }

    @Override
    public void deleteBlock(long blockId) throws IOException {
        Files.deleteIfExists(dataFile(blockId));
        Files.deleteIfExists(metaFile(blockId));
    }

    @Override
    public List<Long> listBlocks() {
        List<Long> ids = new ArrayList<>();
        try (Stream<Path> files = Files.list(current)) {
            files.map(p -> p.getFileName().toString())
                 .filter(n -> n.startsWith(PREFIX) && n.indexOf('.') < 0)
                 .forEach(n -> {
                     long id = Long.parseLong(n.substring(PREFIX.length()));
                     if (Files.exists(metaFile(id))) ids.add(id);
                 });
        } catch (IOException e) {
            throw new IllegalStateException("Cannot list " + current, e);
        }
        return ids;
    }

    @Override
    public long usedBytes() {
        long total = 0;
        for (long id : listBlocks()) {
            try { total += Files.size(dataFile(id)); } catch (IOException ignored) {}
        }
        return total;
    }

    @Override
    public long remainingBytes() {
        try {
            return Files.getFileStore(current).getUsableSpace();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public boolean consumeBlockReportRequest() {
        return reportRequested.getAndSet(false);
    }

    @Override
    public String getDatanodeId() {
        return datanodeId;
    }

    public Path dataFile(long blockId) {
        return current.resolve(PREFIX + blockId);
    }

    private Path metaFile(long blockId) {
        return current.resolve(PREFIX + blockId + META);
    }

    private void quarantine(long blockId) throws IOException {
        for (Path p : List.of(dataFile(blockId), metaFile(blockId))) {
            if (Files.exists(p)) Files.move(p, corrupt.resolve(p.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        }
        reportRequested.set(true);
        System.err.println("[" + datanodeId + "] block " + blockId + " failed checksum; moved to " + corrupt);
    }

    private static void writeSynced(Path file, byte[] data) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
            fos.write(data);
            fos.getChannel().force(true);
        }
    }
}
