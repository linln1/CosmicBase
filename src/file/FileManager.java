package file;

import java.io.*;
import java.util.*;

/**
 * 记录打开的所有文件及对应文件指针，类似于FAT
 * 记录块大小
 */
public class FileManager{
    private File dbDirectory;
    private int blocksize;
    private boolean newdb;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    //一个db目录，块大小
    //用来构造FileManager,然后
    public FileManager(File dbDirectory, int blocksize){
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        newdb = !dbDirectory.exists();

        if(newdb){
            dbDirectory.mkdirs();
        }

        for(String filename: dbDirectory.list()){
            if(filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized int getBlocksize() {
        return blocksize;
    }

    public synchronized void read(Block blk, Page p){
        try{
            RandomAccessFile f = getFile(blk.getFilename());
            f.seek(blk.getBlknum() * blocksize);
            f.getChannel().read(p.contents());
        }
        catch (IOException e){
            throw new RuntimeException("cannot read block" + blk);
        }
    }

    public synchronized void write(Block blk, Page p){
        try{
            RandomAccessFile f = getFile(blk.getFilename());
            f.seek(blk.getBlknum() * blocksize);
            f.getChannel().write(p.contents());
        }
        catch (IOException e){
            throw new RuntimeException("can't write block" + blk);
        }
    }

    public int length(String filename){
        try{
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / blocksize);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("can't access " + filename);
        }
    }

    public synchronized Block append(String filename) {
        int newBlkNum = length(filename);
        Block blk = new Block(filename, newBlkNum);
        byte[] b = new byte[blocksize];
        try {
            RandomAccessFile f = getFile(blk.getFilename());
            f.seek(blk.getBlknum() * blocksize);
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    private RandomAccessFile getFile(String filename) throws IOException{
        RandomAccessFile f = openFiles.get(filename);
        if(f == null){
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }

    public boolean isNewdb() {
        return newdb;
    }



}
