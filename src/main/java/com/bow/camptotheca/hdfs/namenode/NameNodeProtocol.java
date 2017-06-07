package com.bow.camptotheca.hdfs.namenode;

import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

public interface NameNodeProtocol extends VersionedProtocol {

    public static final long versionID = 1L;

    public int[] createFile() throws IOException;

    public String getWriteDataSources(String toFilename, long fileLength);
    
    public String getReadDataSources(String fromFilename);

}