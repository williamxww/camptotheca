package com.bow.camptotheca.hdfs.datanode;

import com.bow.camptotheca.hdfs.config.CoreConfig;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ReadFileManager {

    public boolean openSocketToClient() {
        final ServerSocket server;
        try {
            server = new ServerSocket(CoreConfig.getInstance().data_node.output_port);

            Thread thread = new Thread(() -> {
                while (true) {
                    System.out.println("Wait for client output ....");
                    Socket socket;
                    try {
                        socket = server.accept();
                        new Thread(() -> {
                            outputFile(socket);
                        }).start();
                        System.out.println("Client is coming in output......");
                    } catch (Exception e) {
                        System.out.println("Server error output .....");
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

    public void outputFile(Socket socket) {

        System.out.println("output file ....");

        char[] inputByte;
        int length;
        int sum = 0;

        BufferedReader bufferedReader = null;
        BufferedWriter bufferedWriter = null;

        try {
            try {

                bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String fromFilename = bufferedReader.readLine();
                fromFilename = CoreConfig.getInstance().data_node.root_dir + "/" + fromFilename;

                bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fromFilename))));
                inputByte = new char[Math.min(CoreConfig.getInstance().block_size, 1024)];
                System.out.println("Is outputing file" + fromFilename + "...");

                bufferedWriter.write("0");
                bufferedWriter.newLine();
                bufferedWriter.flush();
                while (sum < CoreConfig.getInstance().block_size
                        && (length = bufferedReader.read(inputByte, 0, inputByte.length)) > 0) {
                    sum += length;
                    bufferedWriter.write(inputByte, 0, length);
                    bufferedWriter.flush();
                }

                System.out.println("Finish outputing：" + fromFilename);
                bufferedReader.close();
            } catch (Exception exception) {
                bufferedWriter.write("1");
                bufferedWriter.newLine();
                bufferedWriter.flush();
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
