package mvcc.recover.record;

import file.Page;
import jdk.nashorn.internal.runtime.regexp.joni.constants.StackPopLevel;
import log.LogFileManager;
import mvcc.Transaction;

public class StartRecord implements LogRecord{
    private int txNum;

    public StartRecord(Page p){
        int pos = Integer.BYTES;
        txNum = p.getInt(pos);
    }

    @Override
    public int getType() {
        return START;
    }

    @Override
    public int getTxNum() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {

    }

    @Override
    public String toString(){
        return "<START" + txNum + ">";
    }

    public static int writeToLog(LogFileManager lfm, int txNum){
        byte[] rec = new byte[2 * Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, START);
        p.setInt(Integer.BYTES, txNum);
        return lfm.append(rec);
    }
}
