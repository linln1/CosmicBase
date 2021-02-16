package connection.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteStatement extends Remote {
    public RemoteResultSet excuteQuery(String qry) throws RemoteException;
    public int excuteUpdate(String upd) throws RemoteException;
    public void close() throws RemoteException;
}
