package connection.rmi.Impl;

import connection.rmi.RemoteMetaData;
import connection.rmi.RemoteResultSet;
import excutor.Scan;
import plan.Plan;
import record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

public class RemoteResultSetImpl extends UnicastRemoteObject implements RemoteResultSet {
    private Scan s;
    private Schema sch;
    private RemoteConnectionImpl rcImpl;

    public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rcImpl) throws RemoteException {
        s = plan.open();
        sch = plan.schema();
        this.rcImpl = rcImpl;
    }


    @Override
    public boolean next() throws RemoteException {
        try {
            return s.nextPtr();
        }
        catch(RuntimeException e) {
            rcImpl.rollback();
            throw e;
        }
    }

    @Override
    public int getInt(String fldname) throws RemoteException {
        try {
            fldname = fldname.toLowerCase(); // to ensure case-insensitivity
            return s.getAsInt(fldname);
        }
        catch(RuntimeException e) {
            rcImpl.rollback();
            throw e;
        }
    }

    @Override
    public String getString(String fldname) throws RemoteException {
        try {
            fldname = fldname.toLowerCase(); // to ensure case-insensitivity
            return s.getAsString(fldname);
        }
        catch(RuntimeException e) {
            rcImpl.rollback();
            throw e;
        }
    }

    @Override
    public RemoteMetaData getMetaData() throws RemoteException {
        return new RemoteMetaDataImpl(sch);;
    }

    @Override
    public void close() throws RemoteException {
        s.close();
        rcImpl.commit();
    }
}
