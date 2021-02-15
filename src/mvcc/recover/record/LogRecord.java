package mvcc.recover.record;

import file.Page;
import mvcc.Transaction;

/**
 * 接口通过每一种类型的record来记录
 */
public interface LogRecord {
    static final int CHECKPOINT = 0;
    static final int START = 1;
    static final int COMMIT = 2;
    static final int ROLLBACK = 3;
    static final int SETINT = 4;
    static final int SETSTRING = 5;

    int getType();

    int getTxNum();

    void undo(Transaction tx);

    static LogRecord createLogRecord(byte[] bytes){
        Page p = new Page(bytes);
        switch (p.getInt(0)){
            case CHECKPOINT:
                return new CheckpointRecord();
            case START:
                return new StartRecord(p);
            case COMMIT:
                return new CommitRecord(p);
            case ROLLBACK:
                return new RollbackRecord(p);
            case SETINT:
                return new SetIntRecord(p);
            case SETSTRING:
                return new SetStringRecord(p);
            default:
                return null;
        }
    }

}
