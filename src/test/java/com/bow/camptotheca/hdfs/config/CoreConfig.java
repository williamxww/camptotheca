package com.bow.camptotheca.hdfs.config;

import com.google.gson.Gson;

import java.io.*;
import java.util.List;

public class CoreConfig {

    public List<DataNodeJson> data_nodes;
    public NameNodeJson name_node;
    public DataNodeJson data_node;
    public int block_size;
    public int file_backup;

    private static CoreConfig ourInstance = null;

    public static CoreConfig getInstance() {
        if (ourInstance == null) {
            try {
                String configFilePath = new File("").getAbsolutePath();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(new File(configFilePath+"/src/main/java/config/core-config.json"))));

                String tmpStr, result="";
                while ( (tmpStr= bufferedReader.readLine())!=null) {
                    result += tmpStr;
                }

                Gson gson = new Gson();
                ourInstance = gson.fromJson(result, CoreConfig.class);

            } catch (Exception e) {
                return null;
            }
        }

        return ourInstance;
    }

    private CoreConfig() {
    }

    public class DataNodeJson {
        public String host;
        public int rpc_port;
        public int input_port;
        public int output_port;
        public String root_dir;
    }

    public class NameNodeJson{
        public String host;
        public int port;
    }
}


