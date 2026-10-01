package dev.anurag.hdfs.common.model;

public abstract class INode {
    private final long id;
    private String name;
    private final INode parent;
    private long createdAt;
    private long modifiedAt;

    public INode(long id, String name, INode parent) {
        this.id = id;
        this.name = name;
        this.parent = parent;
        long now = System.currentTimeMillis();
        this.createdAt = now;
        this.modifiedAt = now;
    }

    public abstract boolean isDirectory();

    public abstract long getSize();

    public long getId() {
        return this.id;
    }

    public INode getParent(){ 
        return this.parent;
    }

    public String getName(){
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
        this.modifiedAt = System.currentTimeMillis();
    }

    public long getModifiedAt() {
        return this.modifiedAt;
    }
}
