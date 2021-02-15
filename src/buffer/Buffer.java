package buffer;

import file.*;
import log.*;

/**
 * 数据缓冲区，包装了Page，存储关于Page的信息，比如相关的硬盘始终，缓冲区被锁定的次数，内容是否被修改，如果是，就要记录id和修改事务的lsn
 * @ author linln
 */

public class Buffer {
    private Page page;
    private FileManager fm;
    private LogFileManager lfm;
    private Block blk;
    private int pins = 0;
    private int modified = -1; // negative means hasn't been modified.
    private int logSeqNum = -1; // negative means no corresponding log record.


    public Buffer() {}

    public Buffer(FileManager fm, LogFileManager lfm){
        this.fm = fm;
        this.lfm = lfm;
        page = new Page(fm.getBlocksize());
    }

    public synchronized Page getPage(){
        return page;
    }

    public synchronized Block getBlk() {
        return blk;
    }

    /**
     * Write the page to its block if the page is dirty.
     * The method ensures that the corresponding log record has been
     * written to disk prior to written the page to the disk.
     */
    void flush(){
        if(this.modified >= 0){
            lfm.flush(logSeqNum);
            fm.write(blk, page);
            modified = -1;
        }
    }

    /**
     * @param offset
     * @return the page in the style of page;
     */
    public int getInt(int offset){
        return page.getInt(offset);
    }

    /**
     * set the content of the page in the style of int
     * the checkout and the handler of overlimit is written in page.setInt()
     * @param offset
     * @param val
     * @param txnum
     * @param logSeqNum
     */
    public void setInt(int offset, int val, int txnum, int logSeqNum){
        modified = txnum;
        if(logSeqNum > 0)
            this.logSeqNum = logSeqNum;
        page.setInt(offset, val);
    }


    /**
     * @param offset
     * @return the page in the style of offset
     */
    public String getString(int offset){
        return page.getString(offset);
    }

    /**
     * set the content of string in the type of String
     * the checkout and handler of exceed boundary is written in page.setString()
     * @param offset
     * @param val
     * @param txnum
     * @param logSeqNum
     */
    public void setString(int offset, String val, int txnum, int logSeqNum){
        modified = txnum;
        if(logSeqNum > 0)
            this.logSeqNum = logSeqNum;
        page.setString(offset, val);
    }

    /**
     * @param offset
     * @return the byte in the style of offset
     */
    public byte[] getByte(int offset){
        return page.getBytes(offset);
    }

    public synchronized void setModified(int modified, int logSeqNum){
        this.modified = modified;
        if(logSeqNum >= 0){
            this.logSeqNum = logSeqNum;
        }
    }

    public boolean isPinned(){
        return pins > 0;
    }

    public void assignToBlock(Block b){
        flush();
        blk = b;
        fm.read(blk, page);
        pins = 0;
    }

    void pinsInc(){
        pins++;
    }

    void pinsDec(){
        pins--;
    }

    /**
     * Returns true if the buffer is dirty
     * dur to a modification by the specified transaction.
     * @param txnum the id of the transaction.
     * @return true if the transaction modified the buffer
     */
    boolean isModifiedBy(int txnum){
        return txnum == modified;
    }

    /**
     *
     */
//    public assignToNew(String filename, )
}
