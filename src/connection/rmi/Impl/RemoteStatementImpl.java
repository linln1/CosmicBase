package connection.rmi.Impl;

import connection.rmi.RemoteResultSet;
import connection.rmi.RemoteStatement;
import mvcc.Transaction;
import plan.Impl.Planner;
import plan.Plan;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
    private RemoteConnectionImpl rcImpl;
    private Planner planner;

    public RemoteStatementImpl(RemoteConnectionImpl rcImpl, Planner planner) throws RemoteException {
        this.rcImpl = rcImpl;
        this.planner = planner;
    }

    @Override
    public RemoteResultSet excuteQuery(String qry) throws RemoteException {
        try{
            Transaction tx = rcImpl.getTx();
            Plan pln = planner.createQueryPlan(qry, tx);
            return new RemoteResultSetImpl(pln, rcImpl);
        } catch (RuntimeException e) {
            rcImpl.rollback();
            throw e;
        }
    }

    @Override
    public int excuteUpdate(String upd) throws RemoteException {
        try{
            Transaction tx = rcImpl.getTx();
            int result = planner.executeUpdate(upd, tx);
            rcImpl.commit();
            return result;
        } catch (RuntimeException e) {
            rcImpl.rollback();
            throw e;
        }
    }

    @Override
    public void close() throws RemoteException {

    }
}
