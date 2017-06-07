package com.bow.camptotheca.hdfs.namenode.filesystem;

import java.util.Date;

public class INode {
    protected int fileType;

    protected String filename;

    protected Date modifyTime;

    protected INode parent;

    public INode() {
        this.parent = null;
        this.modifyTime = new Date();
    }

    public INode(String filename) {
        this();
        this.filename = filename;
    }

    public int getFileType() {
        return fileType;
    }

    public void setFileType(int fileType) {
        this.fileType = fileType;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public INode getParent() {
        return parent;
    }

    public void setParent(INode parent) {
        this.parent = parent;
    }
}
