package com.bow.camptotheca.hdfs.client;

import com.bow.camptotheca.hdfs.config.CoreConfig;
import com.bow.camptotheca.hdfs.namenode.DataNodeSources;
import com.bow.camptotheca.hdfs.namenode.NameNodeProtocol;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

public class DFSManager {

    public void uploadFile(String fromFilename, String toFilename) throws IOException {

        File file = new File(fromFilename);
        long fileLen = file.length();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        char[] sendBytes = new char[Math.min(1024, CoreConfig.getInstance().block_size)];

        // 连接namenode, 获取datanode ip : port
        Configuration conf = new Configuration();
        NameNodeProtocol proxy = RPC.getProxy(NameNodeProtocol.class, NameNodeProtocol.versionID,
                new InetSocketAddress(CoreConfig.getInstance().name_node.host, CoreConfig.getInstance().name_node.port),
                conf);
        Gson gson = new Gson();
        String writeDataNodesJson = proxy.getWriteDataSources(toFilename, fileLen);
        DataNodeSources dataNodeSources = gson.fromJson(writeDataNodesJson, DataNodeSources.class);

        System.out.println(writeDataNodesJson);

        // 依次开启datanode socket
        int curBlockPos = 0;
        for (DataNodeSources.DataNode dataNode : dataNodeSources.getDataNodes()) {

            System.out.println(dataNode.ips.get(0) + ":" + dataNode.ports.get(0) + "=======================");

            int length = 0;
            double sumL = 0;
            Socket socket = null;
            BufferedWriter bufferedWriter = null;
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(dataNode.ips.get(0), dataNode.ports.get(0)));
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

                bufferedWriter.write(toFilename + "_" + (curBlockPos++) + "_" + 0);
                bufferedWriter.newLine();
                bufferedWriter.write("" + (dataNode.ips.size() - 1));
                bufferedWriter.newLine();
                for (int pos = 1; pos < dataNode.ips.size(); pos++) {
                    bufferedWriter.write(dataNode.ips.get(pos));
                    bufferedWriter.newLine();
                    bufferedWriter.write(toFilename + "_" + (curBlockPos - 1) + "_" + pos);
                    bufferedWriter.newLine();
                }

                while (sumL < CoreConfig.getInstance().block_size
                        && (length = bufferedReader.read(sendBytes, 0, sendBytes.length)) > 0) {
                    sumL += length;
                    System.out.println("已传输：" + ((sumL / CoreConfig.getInstance().block_size) * 100) + "%");
                    bufferedWriter.write(sendBytes, 0, length);
                    bufferedWriter.flush();
                }

                socket.close();
                System.out.println("接受完成==================");
            } catch (Exception e) {
                System.out.println("客户端文件传输异常");
                e.printStackTrace();
            } finally {
                if (bufferedWriter != null)
                    bufferedWriter.close();
                if (socket != null)
                    socket.close();
            }
        }
        bufferedReader.close();
    }

    public void downloadFile(String fromFilename, String toFilename) throws IOException {

        // 连接namenode, 获取datanode ip : port
        Configuration conf = new Configuration();
        NameNodeProtocol proxy = RPC.getProxy(NameNodeProtocol.class, NameNodeProtocol.versionID,
                new InetSocketAddress(CoreConfig.getInstance().name_node.host, CoreConfig.getInstance().name_node.port),
                conf);
        Gson gson = new Gson();
        String writeDataNodesJson = proxy.getReadDataSources(fromFilename);
        DataNodeSources dataNodeSources = gson.fromJson(writeDataNodesJson, DataNodeSources.class);

        System.out.println(writeDataNodesJson);

        BufferedWriter fileBufferedWriter = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(new File(toFilename), true)));
        BufferedReader bufferedReader = null;

        for (DataNodeSources.DataNode dataNode : dataNodeSources.getDataNodes()) {

            boolean acceptFinish = false;
            for (int i = 0; !acceptFinish && i < dataNode.ips.size(); i++) {
                System.out.println(dataNode.ips.get(i) + ":" + dataNode.ports.get(i) + "___" + i + "___"
                        + "=======================");

                Socket socket = null;
                try {
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(dataNode.ips.get(i), dataNode.ports.get(i)));
                    BufferedWriter socketBufferedWriter = new BufferedWriter(
                            new OutputStreamWriter(socket.getOutputStream()));

                    socketBufferedWriter.write(dataNode.filenames.get(i));
                    socketBufferedWriter.newLine();
                    socketBufferedWriter.flush();

                    bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    char[] sendBytes = new char[Math.min(1024, CoreConfig.getInstance().block_size)];

                    int length;
                    int status = Integer.parseInt(bufferedReader.readLine());
                    if (status == 0) {
                        while ((length = bufferedReader.read(sendBytes, 0, sendBytes.length)) > 0) {
                            fileBufferedWriter.write(sendBytes, 0, length);
                            fileBufferedWriter.flush();
                        }
                        System.out.println("接受完成==================");
                        acceptFinish = true;
                    } else {
                        System.out.println("接受失败==================");
                        acceptFinish = false;
                    }

                    socket.close();
                    socketBufferedWriter.close();
                } catch (Exception e) {
                    System.out.println("客户端文件传输异常");
                    e.printStackTrace();
                } finally {
                    if (bufferedReader != null)
                        bufferedReader.close();
                    if (socket != null)
                        socket.close();
                }
            }

        }
        if (fileBufferedWriter != null)
            fileBufferedWriter.close();
    }
}