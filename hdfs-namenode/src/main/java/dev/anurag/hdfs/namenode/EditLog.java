package dev.anurag.hdfs.namenode;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Write-ahead log for NameNode mutations.
 *
 * Each entry is one line: op TAB timestamp TAB field1 TAB field2 ...
 * The log is opened in append mode; every entry is flushed to disk immediately.
 *
 * Operations logged:
 *   CREATE_FILE  ts  path  rf  inodeId
 *   MKDIR        ts  path  inodeId
 *   ADD_BLOCK    ts  path  blockId
 *   CLOSE_FILE   ts  path  fileSize
 *   RENAME       ts  src   dst
 *   DELETE       ts  path
 */
public class EditLog implements Closeable {

    // Op-code constants
    static final String CREATE_FILE = "CREATE_FILE";
    static final String MKDIR       = "MKDIR";
    static final String ADD_BLOCK   = "ADD_BLOCK";
    static final String CLOSE_FILE  = "CLOSE_FILE";
    static final String RENAME      = "RENAME";
    static final String DELETE      = "DELETE";

    private final Path logFile;
    private BufferedWriter writer;

    public EditLog(Path logFile) throws IOException {
        this.logFile = logFile;
        this.writer = openAppend(logFile);
    }

    /** Appends one log entry and flushes. Fields are tab-joined after the timestamp. */
    public synchronized void log(String op, Object... fields) throws IOException {
        StringBuilder sb = new StringBuilder(op)
                .append('\t').append(System.currentTimeMillis());
        for (Object f : fields) sb.append('\t').append(f);
        writer.write(sb.toString());
        writer.newLine();
        writer.flush();
    }

    /** Reads every entry in the log as a split-by-tab array. */
    public List<String[]> readAll() throws IOException {
        List<String[]> result = new ArrayList<>();
        if (!Files.exists(logFile)) return result;
        for (String line : Files.readAllLines(logFile)) {
            if (!line.isBlank()) result.add(line.split("\t"));
        }
        return result;
    }

    /** Truncates the log after a successful checkpoint (saveNamespace). */
    public synchronized void truncate() throws IOException {
        writer.close();
        Files.deleteIfExists(logFile);
        writer = openAppend(logFile);
    }

    @Override
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    private static BufferedWriter openAppend(Path file) throws IOException {
        return Files.newBufferedWriter(file,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
