package plan;

import mvcc.Transaction;
import parse.*;

public interface UpdatePlanner {
    public int executeInsert(InsertData data, Transaction tx);
    public int executeDelete(DeleteData data, Transaction tx);
    public int executeModify(ModifyData data, Transaction tx);
    public int executeCreateTable(CreateTable data, Transaction tx);
    public int executeCreateView(CreateView data, Transaction tx);
    public int executeCreateIndex(CreateIndex data, Transaction tx);
}
