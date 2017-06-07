package com.bow.camptotheca.hdfs;

import com.bow.camptotheca.hdfs.config.CoreConfig;

import java.io.File;

public class test {

    public static void main(String[] args) {
        String path = new File("").getAbsolutePath();
        System.out.println(path);

        CoreConfig.getInstance();

    }
}
