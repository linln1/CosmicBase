package mvcc.recover.record;

import file.Page;
import log.LogFileManager;
import mvcc.Transaction;

public class CommitRecord implements LogRecord{
    private int txNum;

    public CommitRecord(Page p){
        int pos = Integer.BYTES;
        txNum = p.getInt(pos);
    }


    @Override
    public int getType() {
        return COMMIT;
    }

    @Override
    public int getTxNum() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {

    }

    @Override
    public String toString() {
        return "<COMMIT " + txNum + ">";
    }

    public static int writeToLog(LogFileManager lfm, int txNum){
        byte[] rec = new byte[ 2 * Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, COMMIT);
        p.setInt(Integer.BYTES, txNum);
        return lfm.append(rec);
    }
}
