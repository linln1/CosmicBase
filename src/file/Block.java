package file;

import java.util.Objects;

/**
 * 记录对应的文件名，以及Block所连续分配的块数
 */
public class Block {
    private String filename;
    private int blknum;

    public Block(String filename, int blknum){
        this.filename = filename;
        this.blknum = blknum;
    }

    public synchronized String getFilename(){
        return filename;
    }

    public synchronized int getBlknum(){
        return blknum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Block block = (Block) o;
        return blknum == block.blknum &&
                Objects.equals(filename, block.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, blknum);
    }

    @Override
    public String toString(){
        return"[ file" + filename + ", block" + blknum +"]";
    }
}
