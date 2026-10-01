package dev.anurag.hdfs.datanode;

import java.io.IOException;

/** A pipeline write failed; {@link #badNode()} is the "host:port" of the DataNode to blame. */
public class PipelineException extends IOException {

    private final String badNode;

    public PipelineException(String badNode, String message, Throwable cause) {
        super(message + " (failed node " + badNode + ")", cause);
        this.badNode = badNode;
    }

    public String badNode() {
        return badNode;
    }
}
