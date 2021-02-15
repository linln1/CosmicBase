package log;

import file.Block;
import file.FileManager;
import file.Page;

import java.util.Iterator;

/**
 * 提供一种按照逆序来遍历日志文件的工具, 里面有一个logfilemgr，一些块，一个页结构
 * 一个file可能需要多个块，然后用多个块去构造一个页
 */
public class LogFileIterator implements Iterator<byte[]> {
    private FileManager fm;
    private Block blk;
    private Page p;
    private int currentpos; //目前所在的块的位置
    private int boundary; //目前块的边界

    public LogFileIterator(FileManager fm, Block currentblk) {
        this.fm = fm;
        this.blk = blk;
        byte[] b = new byte[fm.getBlocksize()];
        p = new Page(b);
        moveToBlock(blk);
    }

    /**
     * 确定现在的记录之前有没有旧的记录
     * @return
     */
    @Override
    public boolean hasNext() { return currentpos<fm.getBlocksize(); }


    /**
     * 如果没有记录，将记录移动到前一个块去
     * @return
     */
    @Override
    public byte[] next(){
        if(currentpos == fm.getBlocksize()){
            blk = new Block(blk.getFilename(), blk.getBlknum() - 1);
            moveToBlock(blk);
        }
        byte[] rec = p.getBytes(currentpos);
        currentpos += Integer.BYTES + rec.length;
        return rec;
    }

    /**
     * 将log移到某一个特定的block上去，并且将记录的位置放到那个块的最前面
     * @param blk
     */
    private void moveToBlock(Block blk){
        fm.read(blk, p);
        boundary = p.getInt(0);
        currentpos = boundary;
    }
}
