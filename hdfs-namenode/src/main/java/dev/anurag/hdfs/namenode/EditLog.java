package dev.anurag.hdfs.namenode;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Write-ahead log for NameNode mutations.
 *
 * Each entry is one line: op TAB timestamp TAB field1 TAB field2 ...
 * Every entry is fsync'd before log() returns, so an acknowledged mutation survives
 * a power loss, not just a process crash. (Real HDFS batches fsyncs across concurrent
 * callers — "group commit" — to amortise the cost; we sync per entry.)
 *
 * Operations logged:
 *   CREATE_FILE    ts  path  rf  inodeId
 *   MKDIR          ts  path  inodeId
 *   ADD_BLOCK      ts  path  blockId
 *   ABANDON_BLOCK  ts  path  blockId
 *   CLOSE_FILE     ts  path  fileSize
 *   RENAME         ts  src   dst
 *   DELETE         ts  path
 */
public class EditLog implements Closeable {

    static final String CREATE_FILE   = "CREATE_FILE";
    static final String MKDIR         = "MKDIR";
    static final String ADD_BLOCK     = "ADD_BLOCK";
    static final String ABANDON_BLOCK = "ABANDON_BLOCK";
    static final String CLOSE_FILE    = "CLOSE_FILE";
    static final String RENAME        = "RENAME";
    static final String DELETE        = "DELETE";

    private final Path logFile;
    private FileChannel channel;
    private BufferedWriter writer;
    private long entriesSinceTruncate;

    public EditLog(Path logFile) throws IOException {
        this.logFile = logFile;
        open();
        this.entriesSinceTruncate = readAll().size();
    }

    /** Appends one entry and fsyncs it. Fields are tab-joined after the timestamp. */
    public synchronized void log(String op, Object... fields) throws IOException {
        StringBuilder sb = new StringBuilder(op).append('\t').append(System.currentTimeMillis());
        for (Object f : fields) sb.append('\t').append(f);
        writer.write(sb.toString());
        writer.newLine();
        writer.flush();
        channel.force(false);
        entriesSinceTruncate++;
    }

    /** Reads every entry in the log as a split-by-tab array. */
    public List<String[]> readAll() throws IOException {
        List<String[]> result = new ArrayList<>();
        if (!Files.exists(logFile)) return result;
        for (String line : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) result.add(line.split("\t"));
        }
        return result;
    }

    public synchronized long entriesSinceCheckpoint() {
        return entriesSinceTruncate;
    }

    /** Truncates the log after a successful checkpoint (saveNamespace). */
    public synchronized void truncate() throws IOException {
        writer.close();
        Files.deleteIfExists(logFile);
        open();
        entriesSinceTruncate = 0;
    }

    @Override
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    private void open() throws IOException {
        channel = FileChannel.open(logFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        writer = new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), StandardCharsets.UTF_8));
    }
}
