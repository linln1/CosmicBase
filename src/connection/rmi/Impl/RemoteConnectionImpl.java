package connection.rmi.Impl;

import connection.rmi.RemoteConnection;
import connection.rmi.RemoteStatement;
import mvcc.Transaction;
import plan.Impl.Planner;
import server.CosmicDB;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
    private CosmicDB cosmicDB;
    private Transaction tx;
    private Planner p;

    RemoteConnectionImpl(CosmicDB db) throws RemoteException {
        this.cosmicDB = db;
        tx = db.newTx();
        p = db.getEm();
    }

    @Override
    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this, p);
    }

    @Override
    public void close() throws RemoteException {
        tx.commit();
    }

    Transaction getTx(){
        return tx;
    }

    void commit(){
        tx.commit();
        tx = cosmicDB.newTx();
    }

    void rollback() {
        tx.rollback();
        tx = cosmicDB.newTx();
    }
}
