package mvcc.recover.record;


import file.Page;
import log.LogFileManager;
import mvcc.Transaction;

public class CheckpointRecord implements LogRecord {

    @Override
    public int getTxNum() {return -1;}

    @Override
    public int getType() {
        return CHECKPOINT;
    }

    @Override
    public void undo(Transaction tx) {

    }

    @Override
    public String toString(){
        return "<CHECKPOINT>";
    }

    /**
     * 静态方法，将checkpoint记录写入到log文件里面，这个包含CHECKPOINT运算符
     * @param lfm
     * @return
     */
    public static int writeToLog(LogFileManager lfm){
        byte[] rec = new byte[Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, CHECKPOINT);
        return lfm.append(rec);
    }
}
