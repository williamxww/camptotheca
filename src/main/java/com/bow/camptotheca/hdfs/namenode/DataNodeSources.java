package com.bow.camptotheca.hdfs.namenode;

import java.util.ArrayList;
import java.util.List;

public class DataNodeSources {

    private List<DataNode> dataNodes;

    public DataNodeSources() {
        this.dataNodes = new ArrayList<DataNode>();
    }

    public void addDataNode(List<String> ips, List<Integer> ports) {
        DataNode dataNode = new DataNode(ips, ports);
        this.dataNodes.add(dataNode);
    }

    public void addDataNode(List<String> ips, List<Integer> ports, List<String> filenames) {
        DataNode dataNode = new DataNode(ips, ports, filenames);
        this.dataNodes.add(dataNode);
    }

    public List<DataNode> getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(List<DataNode> dataNodes) {
        this.dataNodes = dataNodes;
    }

    @Override
    public String toString() {
        return this.dataNodes.toString();
    }

    public class DataNode {

        public List<String> ips;

        public List<Integer> ports;

        public List<String> filenames;

        public DataNode() {

        }

        public DataNode(List<String> ips, List<Integer> ports) {
            this.ips = ips;
            this.ports = ports;
        }

        public DataNode(List<String> ips, List<Integer> ports, List<String> filenames) {
            this(ips, ports);
            this.filenames = filenames;
        }

        @Override

        public String toString() {
            return ips.toString();
        }
    }
}
