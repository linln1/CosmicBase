package log;

import file.Block;
import file.FileManager;
import file.Page;

import java.util.Iterator;
import java.util.logging.LogManager;

/**
 * 日志管理工具，负责将日志记录写入日志文件。log的尾部是保存在bytebuffer里的
 * 当需要的时候就刷新到硬盘里面，数据持久化
 */
public class LogFileManager {
    private FileManager fm;
    private Page logpage;
    private Block currentblk;
    private String logName;
    private int latestLogSeqNum = 0 ;
    private int lastSavedLogSeqNum = 0;


    public LogFileManager(FileManager fm, String logName){
        this.fm = fm;
        this.logName = logName;
        byte[] b = new byte[fm.getBlocksize()];
        logpage = new Page(b);
        int logsize = fm.length(logName);
        if (logsize == 0)
            currentblk = appendNewBlock();
        else {
            currentblk = new Block(logName, logsize-1);
            fm.read(currentblk, logpage);
        }
    }

    public Iterator<byte[]> iterator() {
        flush();
        return (Iterator<byte[]>) new LogFileIterator(fm, currentblk);
    }

    private void flush(){
        fm.write(currentblk, logpage);
        lastSavedLogSeqNum = latestLogSeqNum;
    }

    /**
     * 附加一个记录到缓冲区中
     * 记录由任意字节的序列组成
     * 日志记录是被从右到左地写到缓冲区中，先写记录地字节大小，然后再写具体记录地内容
     * 缓冲区地开头包含最后写入记录的位置
     * 从右向左存储便于逆序读取记录
     * @param logrec
     * @return
     */
    public synchronized int append(byte[] logrec){
        int boundary = logpage.getInt(0);
        int recsize = logrec.length;
        int bytesneeded = recsize + Integer.BYTES;
        if (boundary - bytesneeded < Integer.BYTES) { // the simpledb.log simpledb.record doesn't fit,
            flush();        // so move to the next block.
            currentblk = appendNewBlock();
            boundary = logpage.getInt(0);
        }
        int recpos = boundary - bytesneeded;

        logpage.setBytes(recpos, logrec);
        logpage.setInt(0, recpos); // the new boundary
        latestLogSeqNum += 1;
        return latestLogSeqNum;
    }

    /**
     * 初始化bytebuffer然后把它附加到log的文件中去
     * @return
     */
    public Block appendNewBlock(){
        Block blk = fm.append(logName);
        logpage.setInt(0, fm.getBlocksize());
        fm.write(blk, logpage);
        return blk;
    }

    /**
     *将缓冲区中的记录写入到log文件中去
     * @param lsn
     */
    public void flush(int lsn){
        if(lsn >= lastSavedLogSeqNum){
            flush();
        }
    }
}
