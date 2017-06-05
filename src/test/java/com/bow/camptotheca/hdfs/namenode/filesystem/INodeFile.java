package com.bow.camptotheca.hdfs.namenode.filesystem;

import com.bow.camptotheca.hdfs.config.InnerConfig;

import java.util.ArrayList;
import java.util.List;

public class INodeFile extends INode {
    private List<Block> blocks;

    public INodeFile() {
        super();
        this.fileType = InnerConfig.FILE_TYPE_FILE;
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
