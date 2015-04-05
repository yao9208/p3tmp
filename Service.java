import java.io.IOException;
import java.rmi.*;

public interface Service extends Remote{
	public byte[] getReqFromFrontEnd() throws RemoteException, IOException;
	public int[] getRole() throws RemoteException;
	public int[] getMiddleServerList() throws RemoteException;
	public void addMiddleServer(int m) throws RemoteException;
}
