package com.bow.camptotheca.hdfs.datanode;

import com.bow.camptotheca.hdfs.config.CoreConfig;
import com.bow.camptotheca.hdfs.utils.FileUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class WriteFileManager {


    public boolean openSocketToClient() {
        final ServerSocket server;
        try {
            server = new ServerSocket(CoreConfig.getInstance().data_node.input_port);

            Thread thread = new Thread(() -> {
                while (true) {
                    System.out.println("Wait for client ....");
                    Socket socket;
                    try {
                        socket = server.accept();
                        new Thread(() -> {
                            receiveFile(socket);
                        }).start();
                        System.out.println("Client is coming in......");
                    } catch (Exception e) {
                        System.out.println("Server error.....");
                        e.printStackTrace();
                    }
                }
            });
            thread.start();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void receiveFile(Socket socket) {

        System.out.println("receive file ....");

        char[] inputByte;
        int length;
        int sum = 0;

        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;

        try {
            try {

                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                File file = new File(CoreConfig.getInstance().data_node.root_dir);
                if (!file.exists()) {
                    file.mkdir();
                }

                String toFilename = bufferedReader.readLine();
                int pipelineSum = Integer.parseInt(bufferedReader.readLine());
                List<String> ips = new ArrayList<>();
                List<String> filenames = new ArrayList<>();
                for (int i = 0; i < pipelineSum; i++) {
                    ips.add(bufferedReader.readLine());
                    filenames.add(bufferedReader.readLine());
                }
                System.out.println(ips);
                System.out.println(filenames);

                String filePath = CoreConfig.getInstance().data_node.root_dir + "/" + toFilename;
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(filePath))));
                inputByte = new char[Math.min(CoreConfig.getInstance().block_size, 1024)];
                System.out.println("Is receiving file...");

                while (sum < CoreConfig.getInstance().block_size
                        && (length = bufferedReader.read(inputByte, 0, inputByte.length)) > 0) {
                    sum += length;
                    bufferedWriter.write(inputByte, 0, length);
                    bufferedWriter.flush();
                }

                System.out.println("Finish receiving：" + filePath + "==============");

                bufferedReader.close();
                bufferedWriter.close();
                socket.close();

                // pipleline 数据备份传递到其他节点
                if (pipelineSum > 0) {
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(ips.get(0), FileUtils.getInputPortWithHost(ips.get(0))));
                    bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

                    bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bufferedWriter.write(filenames.get(0));
                    bufferedWriter.newLine();

                    bufferedWriter.write("" + (pipelineSum - 1));
                    bufferedWriter.newLine();

                    for (int pos = 1; pos < ips.size(); pos++) {
                        bufferedWriter.write(ips.get(pos));
                        bufferedWriter.newLine();
                        bufferedWriter.write(filenames.get(pos));
                        bufferedWriter.newLine();
                    }

                    bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
                    while ((length = bufferedReader.read(inputByte, 0, inputByte.length)) > 0) {
                        bufferedWriter.write(inputByte, 0, length);
                        bufferedWriter.flush();
                    }
                }
            } finally {
                if (bufferedWriter != null)
                    bufferedWriter.close();
                if (bufferedReader != null)
                    bufferedReader.close();
                if (socket != null)
                    socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}