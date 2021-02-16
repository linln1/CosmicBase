package connection.rmi.Impl;

import com.sun.org.apache.xerces.internal.impl.xs.models.XSCMLeaf;
import connection.rmi.RemoteMetaData;
import record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

public class RemoteMetaDataImpl extends UnicastRemoteObject implements RemoteMetaData {
    private Schema sch;
    private List<String> fields = new ArrayList<String>();


    protected RemoteMetaDataImpl(Schema sch) throws RemoteException {
        this.sch = sch;
        for (String fld : sch.fields()){
            fields.add(fld);
        }
    }

    @Override
    public int getColCnt() throws RemoteException {
        return 0;
    }

    @Override
    public String getColName(int colId) throws RemoteException {
        return null;
    }

    @Override
    public int getColType(int colId) throws RemoteException {
        return 0;
    }

    @Override
    public int getColDisplaySize(int colId) throws RemoteException {
        return 0;
    }
}
