package connection.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteMetaData extends Remote {
    public int getColCnt() throws RemoteException;
    public String getColName(int colId) throws RemoteException;
    public int getColType(int colId) throws RemoteException;
    public int getColDisplaySize(int colId) throws RemoteException;
}
