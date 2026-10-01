package dev.anurag.hdfs.common.model;

public enum DatanodeState {
    NORMAL,   // heartbeating on time
    STALE,    // missed a few heartbeats — avoid for new block placement
    DEAD      // missed too many heartbeats — trigger re-replication
}
