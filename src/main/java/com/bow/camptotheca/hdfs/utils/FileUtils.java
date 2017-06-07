package com.bow.camptotheca.hdfs.utils;

import com.bow.camptotheca.hdfs.config.CoreConfig;

public class FileUtils {
    public static int getInputPortWithHost(String host) {
        for (CoreConfig.DataNodeJson dataNodeJson : CoreConfig.getInstance().data_nodes) {
            if (dataNodeJson.host.equals(host)) {
                return dataNodeJson.input_port;
            }
        }
        return -1;
    }
}
