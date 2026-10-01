package dev.anurag.hdfs.common.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class DirectoryNode extends INode {
    private Map<String, INode> children;

    public DirectoryNode(long id, String name, INode parent) {
        super(id, name, parent);
        this.children = new LinkedHashMap<>();
    }

    public void addChild(INode child) {
        this.children.put(child.getName(), child);
    }

    public INode getChild(String name) {
        return this.children.get(name);
    }

    public Map<String, INode> getChildren() {
        return this.children;
    }

    public void removeChild(String name) {
        this.children.remove(name);
    }

    public Collection<INode> listChildren() {
        return this.children.values();
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public long getSize() {
        long totalSize = 0;
        for (INode child : children.values()) {
            totalSize += child.getSize();
        }
        return totalSize;
    }

}
