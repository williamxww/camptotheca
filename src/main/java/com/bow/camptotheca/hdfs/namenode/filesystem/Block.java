package com.bow.camptotheca.hdfs.namenode.filesystem;


import java.util.List;

public class Block {
    private List<String> ips;
    private List<String> filenames;
    private List<Integer> ipIndexes;
    private int blockPos;


    public Block() {

    }

    public Block(List<String> ips, List<String> filenames, List<Integer> ipIndexes, int blockPos) {
        this.ips = ips;
        this.filenames = filenames;
        this.ipIndexes = ipIndexes;
        this.blockPos = blockPos;
    }

    public List<Integer> getIpIndexes() {
        return ipIndexes;
    }

    public void setIpIndexes(List<Integer> ipIndexes) {
        this.ipIndexes = ipIndexes;
    }

    public List<String> getIps() {
        return ips;
    }

    public void setIps(List<String> ips) {
        this.ips = ips;
    }

    public int getBlockPos() {
        return blockPos;
    }

    public void setBlockPos(int blockPos) {
        this.blockPos = blockPos;
    }

    public List<String> getFilenames() {
        return filenames;
    }

    public void setFilenames(List<String> filenames) {
        this.filenames = filenames;
    }
}
