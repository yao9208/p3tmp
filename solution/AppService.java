import java.rmi.Remote;
import java.rmi.RemoteException;


public interface AppService  extends Remote{
	public void shutDownServer()  throws RemoteException;
}
