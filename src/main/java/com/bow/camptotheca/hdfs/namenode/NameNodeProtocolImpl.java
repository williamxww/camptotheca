package com.bow.camptotheca.hdfs.namenode;

import com.bow.camptotheca.hdfs.config.CoreConfig;
import com.bow.camptotheca.hdfs.namenode.filesystem.Block;
import com.bow.camptotheca.hdfs.namenode.filesystem.FileSystem;
import com.bow.camptotheca.hdfs.namenode.filesystem.INodeFile;
import com.google.gson.Gson;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NameNodeProtocolImpl implements NameNodeProtocol {

    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return versionID;
    }

    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
            throws IOException {
        return new ProtocolSignature(versionID, null);
    }

    public int[] createFile() throws IOException {
        System.out.println("Running " + versionID + "uploadFile.....");

        return new int[] { 1, 2, 3 };
    }

    public String getWriteDataSources(String toFilename, long fileLength) {

        Random random = new Random();

        int blockSum = (int) ((fileLength + CoreConfig.getInstance().block_size - 1)
                / CoreConfig.getInstance().block_size);
        INodeFile nodeFile = (INodeFile) FileSystem.getInstance().createFile(toFilename);

        DataNodeSources dataNodeSources = new DataNodeSources();
        for (int i = 0; i < blockSum; i++) {
            List<String> ips = new ArrayList<>();
            List<Integer> ports = new ArrayList<>();
            List<String> filenames = new ArrayList<>();
            List<Integer> ipIndexes = new ArrayList<>();

            for (int j = 0; j < CoreConfig.getInstance().file_backup; j++) {
                int dataNodeIndex = random.nextInt(CoreConfig.getInstance().data_nodes.size());
                ips.add(CoreConfig.getInstance().data_nodes.get(dataNodeIndex).host);
                ports.add(CoreConfig.getInstance().data_nodes.get(dataNodeIndex).input_port);
                filenames.add(toFilename + "_" + i + "_" + j);
                ipIndexes.add(dataNodeIndex);
            }

            dataNodeSources.addDataNode(ips, ports);
            Block block = new Block(ips, filenames, ipIndexes, i);
            nodeFile.getBlocks().add(block);
        }

        Gson gson = new Gson();
        return gson.toJson(dataNodeSources);
    }

    public String getReadDataSources(String fromFilename) {
        INodeFile fromFile = (INodeFile) FileSystem.getInstance().findFile(fromFilename);
        DataNodeSources dataNodeSources = new DataNodeSources();
        for (Block block : fromFile.getBlocks()) {

            List<Integer> ports = new ArrayList<>();
            for (Integer index : block.getIpIndexes()) {
                ports.add(CoreConfig.getInstance().data_nodes.get(index).output_port);
            }
            ports.add(CoreConfig.getInstance().data_node.output_port);
            dataNodeSources.addDataNode(block.getIps(), ports, block.getFilenames());
        }
        Gson gson = new Gson();
        return gson.toJson(dataNodeSources);
    }

}