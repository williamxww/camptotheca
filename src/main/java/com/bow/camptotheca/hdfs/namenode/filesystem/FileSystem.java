package com.bow.camptotheca.hdfs.namenode.filesystem;



public class FileSystem {
    private INode rootDir;
    private static FileSystem fileSystem;

    public FileSystem() {
        this.rootDir = new INodeDirectory("/");
    }

    public static FileSystem getInstance() {
        if (fileSystem == null) {
            fileSystem = new FileSystem();
        }
        return fileSystem;
    }

    public INode getINodeFromFilePath(String filepath) {
        if (filepath.equals("/")) {
            return this.rootDir;
        }

        INode curNode = this.rootDir;
        String[] filenames =  filepath.split("/");

        return getINodeFromFilePath(filenames,filenames.length);
    }

    public INode getINodeFromFilePath(String []filepaths, int length) {
        INode curNode = this.rootDir;

        boolean hasFindCurFile = true;
        for(int i=1;i<length;i++) {
            hasFindCurFile = false;
            for(INode node : ((INodeDirectory)curNode).getChildNodes()){
                if(node.getFilename().equals(filepaths[i]) &&
                        node.getFileType()== INode.FILE_TYPE_DIR){
                    curNode = node;
                    hasFindCurFile = true;
                    break;
                }
            }
            if (!hasFindCurFile) {
                return null;
            }
        }
        return curNode;
    }

    public INode createFile(String filename) {
        String[] filenames = filename.split("/");
        INodeDirectory parentDir = (INodeDirectory) getINodeFromFilePath(filenames, filenames.length - 1);
        if(parentDir==null)
            return null;

        INodeFile file = new INodeFile(filenames[filenames.length - 1]);
        file.setParent(parentDir);
        parentDir.getChildNodes().add(file);

        return file;
    }

    public INode findFile(String filename) {
        String[] filenames = filename.split("/");
        INodeDirectory parentDir = (INodeDirectory) getINodeFromFilePath(filenames, filenames.length - 1);
        for (INode node : parentDir.getChildNodes()) {
            if (node.getFilename().equals(filenames[filenames.length - 1]) &&
                    node.getFileType() == INode.FILE_TYPE_FILE) {
                return node;
            }
        }
        return null;
    }

    public INode getRootDir() {
        return rootDir;
    }

    public void setRootDir(INode rootDir) {
        this.rootDir = rootDir;
    }
}
