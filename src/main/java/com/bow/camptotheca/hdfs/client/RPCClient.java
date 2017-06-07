package com.bow.camptotheca.hdfs.client;

import com.bow.camptotheca.hdfs.config.CoreConfig;

import java.io.IOException;
import java.util.Scanner;

public class RPCClient {

    public static void main(String[] args) throws IOException {
        DFSManager dfsManager = new DFSManager();

        dfsManager.uploadFile("E:/hello.txt", "/hello.txt");

        Scanner scanner = new Scanner(System.in);
        scanner.next();
        dfsManager.downloadFile("/hello.txt", "E:/download/hello.txt");

        CoreConfig.getInstance();
    }
}