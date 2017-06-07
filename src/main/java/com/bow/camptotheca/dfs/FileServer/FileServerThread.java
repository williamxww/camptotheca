package com.bow.camptotheca.dfs.FileServer;


import com.bow.camptotheca.dfs.Pid;
import com.bow.camptotheca.dfs.sdfsserverMain;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;
import java.util.Set;

/**Thread Class for handling file read and Write requests
 *
 */
class FileServerThread extends Thread {
    private static final int MasterPortDelta=3;
    Socket socket;
    Set<String> sdfsFiles;
    FileServerThread(Socket sock, Set<String> sdfsfiles) {
        socket=sock;
        sdfsFiles=sdfsfiles;
    }

    @Override
    public void run() {
        handleRequest();
    }

    private void closeSocket() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /** Replicate file from source
     * @param fileName
     * @param ipAddress
     * @param port
     * @return
     */
    private boolean replicateSDFSFile(String fileName, String ipAddress, int port) {
        Socket socket=null;
        Scanner soIn=null;
        try {
            socket=new Socket(ipAddress,port);
            socket.setSoTimeout(2000);
            soIn=new Scanner(new InputStreamReader(socket.getInputStream()));
            soIn.useDelimiter("\n");
            PrintWriter soOut=new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            soOut.println(Message.createGetMessage(fileName));
            soOut.flush();
            if (Message.retrieveMessage(soIn.next()).type.equals(MessageType.YES)) {
                createSDFSFile(socket,fileName);
            } else {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                socket.close();
                soIn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return true;
    }
    /**Send file to other process
     * @param filename
     */
    private void sendSDFSFile(String filename) {
        try {
            DataOutputStream out=new DataOutputStream(socket.getOutputStream());
            byte [] buffer=new byte[1024];
            FileInputStream fileIn=new FileInputStream(FileServer.baseDir+filename);
            int readlen;
            while((readlen=fileIn.read(buffer))!=-1) {
                out.write(buffer,0,readlen);
            }

            out.flush();
            fileIn.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**Handle fileOp requests from other processes
     * 
     */
    private void handleRequest() {
        try {
            Scanner in=new Scanner(new InputStreamReader(socket.getInputStream()));
            in.useDelimiter("\n");
            PrintWriter out=new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            Message messageRequest = Message.retrieveMessage(in.next());
            String fname=messageRequest.fileName;
            if (messageRequest.type.equals(MessageType.GET)) {
                if (sdfsFiles.contains(fname)) {
                    out.println(Message.createOkayMessage());
                    out.flush();
                    sendSDFSFile(fname);
                } else {
                    out.println(Message.createNayMessage());
                    out.flush();
                }

                closeSocket();
            } else if (messageRequest.type.equals(MessageType.PUT)) {
                if (sdfsFiles.contains(fname)) {
                    out.println(Message.createNayMessage());
                    out.flush();
                } else {
                    out.println(Message.createOkayMessage());
                    out.flush();
                    createSDFSFile(socket,fname);
                }

                closeSocket();
            } else if (messageRequest.type.equals(MessageType.REP)) {
                closeSocket();
                replicateSDFSFile(fname,messageRequest.ipAddress,messageRequest.port);
            } else if (messageRequest.type.equals(MessageType.DEL)) {
                new File(fname).delete();
                sdfsFiles.remove(fname);
                closeSocket();
            } else {
                System.out.println("Not Implemented");
                closeSocket();
                System.exit(1);
            }
            in.close();
        } catch (IOException e) {
            closeSocket();
            e.printStackTrace();
        }
    }
    /**Receive SDFS file
     * @param socket
     * @param fileName
     * @throws InterruptedIOException
     */
    private void createSDFSFile(Socket socket, String fileName) throws InterruptedIOException {
        try {
            FileOutputStream fs=new FileOutputStream(FileServer.baseDir+fileName);
            byte[] buffer=new byte[1024];
            DataInputStream in=new DataInputStream(socket.getInputStream());
            int readlen;
            while ((readlen=in.read(buffer))>0) {
                fs.write(buffer,0,readlen);
            }

            fs.close();
            System.out.println("[DEBUG][FILE_SERVER] new file in server : "+fileName);
            sdfsFiles.add(fileName);
            notifyFileAdd(fileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**Notify new file to Master
     * @param fileName
     */
    private void notifyFileAdd(String fileName) {
        Pid master= sdfsserverMain.ES.getMasterPid();
        try {
            Socket sock=new Socket(master.hostname,master.port+MasterPortDelta);
            BufferedReader in= new BufferedReader(new InputStreamReader(sock.getInputStream()));
            PrintWriter out=new PrintWriter(new OutputStreamWriter(sock.getOutputStream()));
            String [] filenames={fileName};
            out.println(com.bow.camptotheca.dfs.ElectionService.Message
                    .MessageBuilder
                    .buildNewfilesMessage(sdfsserverMain.FD.getSelfID().toString(),filenames)
                    .toString());
            out.flush();
            System.out.println("[DEBUG][FILE_SERVER]: notified master");
            in.readLine();
            System.out.println("[DEBUG][FILE_SERVER]: notify ack master");

            sock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
