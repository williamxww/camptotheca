package com.bow.camptotheca.hdfs.namenode.filesystem;


import java.util.ArrayList;
import java.util.List;

public class INodeDirectory extends INode {

    private List<INode> childNodes;

    public INodeDirectory() {
        super();
        this.childNodes = new ArrayList<>();
        this.fileType = INode.FILE_TYPE_DIR;
    }

    public INodeDirectory(String filename) {
        this();
        this.filename = filename;
    }

    public List<INode> getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(List<INode> childNodes) {
        this.childNodes = childNodes;
    }
}
