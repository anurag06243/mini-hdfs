package dev.anurag.hdfs.datanode;

import java.io.IOException;

public class CorruptBlockException extends IOException {
    public CorruptBlockException(String message) {
        super(message);
    }
}
