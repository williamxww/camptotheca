package com.bow.camptotheca.dfs.ElectionService;

import com.bow.camptotheca.dfs.FailureDetector.FailureDetector;
import com.bow.camptotheca.dfs.sdfsproxyMain;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Class for MasterTracker Module
 *
 */
public class MasterTracker {
    private String master = null;

    private ServerSocket welcomeSocket;

    private FailureDetector FD;

    public MasterTracker(int port) {
        FD = sdfsproxyMain.FD;
        try {
            welcomeSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("[ERROR]: Can't Create Server Socket");
            System.exit(-1);
        }
    }

    /**
     * SendMessage to clientSocket
     * 
     * @param clientSocket
     * @param msg
     * @return
     */
    private String sendMessage(Socket clientSocket, String msg) {
        String response = null;
        try {
            DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            outToServer.writeBytes(msg + '\n');
            System.out.println("[DEBUG][Election]: Sent Message " + msg);
            response = inFromServer.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("[ERROR][Election]: Sending Message");
        }
        return response;
    }

    /**
     * Handle reply from clientSocket
     * 
     * @param msg
     * @param clientSocket
     * @throws IOException
     */
    private void handleMessage(String msg, Socket clientSocket) throws IOException {
        System.out.println("[DEBUG][Election]: Recieved Message " + msg + " at time "
                + String.valueOf(System.currentTimeMillis()));
        Message m = Message.extractMessage(msg);
        if (m.type == MessageType.MASTER) {
            String masterID = "NOT_SET";
            if (master != null && FD.isAlive(master)) {
                masterID = master;
            }
            String msgreply = Message.MessageBuilder.buildMasterReplyMessage(masterID).toString();
            sendMessage(clientSocket, msgreply);
        } else if (m.type == MessageType.COORDINATOR) {
            String new_master = m.messageParams[0];
            master = new_master;
            System.out.println("[Election]:" + FD.getSelfID().pidStr);
            String msgreply = Message.MessageBuilder.buildOKMessage(FD.getSelfID().pidStr).toString();
            System.out.println(msgreply);
            sendMessage(clientSocket, msgreply);
        }
    }

    /**
     * Launch Master Tracker
     * 
     */
    public void startMT() {
        while (true) {
            try {
                System.out.println("[DEBUG][Election]: Waiting to accept connection");
                Socket connectionSocket = welcomeSocket.accept();
                BufferedReader inFromClient = new BufferedReader(
                        new InputStreamReader(connectionSocket.getInputStream()));
                String msg = inFromClient.readLine();
                if (msg != null) {
                    handleMessage(msg, connectionSocket);
                }
                connectionSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Getter method for master
     * 
     * @return
     */
    public String getMaster() {
        return master;
    }
}