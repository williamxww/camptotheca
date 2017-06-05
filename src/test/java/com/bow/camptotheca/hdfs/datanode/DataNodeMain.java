package com.bow.camptotheca.hdfs.datanode;

import com.bow.camptotheca.hdfs.config.CoreConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class DataNodeMain {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        RPC.Server server = new RPC.Builder(conf).setProtocol(DataNodeProtocol.class)
                .setInstance(new DataNodeProtocolImpl()).setBindAddress(CoreConfig.getInstance().data_node.host)
                .setNumHandlers(2).setPort(CoreConfig.getInstance().data_node.rpc_port).build();

        server.start();

        new WriteFileManager().openSocketToClient();
        new ReadFileManager().openSocketToClient();

        CoreConfig.getInstance();
    }
}
