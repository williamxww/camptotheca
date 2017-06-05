package com.bow.camptotheca.hdfs.namenode;

import com.bow.camptotheca.hdfs.config.CoreConfig;
import com.bow.camptotheca.hdfs.namenode.filesystem.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class NameNodeMain {


    public static void main(String[] args) throws IOException {
        runMainRpc();
    }

    private static void runMainRpc() throws IOException {
        Configuration conf = new Configuration();

        RPC.Server server = new RPC.Builder(conf).setProtocol(NameNodeProtocol.class)
                .setInstance(new NameNodeProtocolImpl()).setBindAddress(CoreConfig.getInstance().name_node.host)
                .setNumHandlers(2)
                .setPort(CoreConfig.getInstance().name_node.port).build();
        server.start();

        FileSystem.getInstance();

        CoreConfig.getInstance();
    }
}