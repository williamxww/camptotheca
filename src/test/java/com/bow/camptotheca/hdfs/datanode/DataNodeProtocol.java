package com.bow.camptotheca.hdfs.datanode;


import org.apache.hadoop.ipc.VersionedProtocol;

public interface DataNodeProtocol extends VersionedProtocol {

    public static final long versionID = 1L;
}
