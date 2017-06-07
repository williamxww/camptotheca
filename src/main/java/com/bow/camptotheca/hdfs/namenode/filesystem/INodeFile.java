package com.bow.camptotheca.hdfs.namenode.filesystem;


import java.util.ArrayList;
import java.util.List;

public class INodeFile extends INode {
    private List<Block> blocks;

    public INodeFile() {
        super();
        this.fileType = INode.FILE_TYPE_FILE;
    }

    public INodeFile(String filename) {
        this();
        this.filename = filename;
        this.blocks = new ArrayList<>();
    }

    public List<Block> getBlocks() {
        return blocks;
    }

    public void setBlocks(List<Block> blocks) {
        this.blocks = blocks;
    }
}
