package com.bow.camptotheca.hdfs.datanode;

import org.apache.hadoop.ipc.ProtocolSignature;
import java.io.*;

public class DataNodeProtocolImpl implements DataNodeProtocol {


    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(versionID,null);
    }
}
