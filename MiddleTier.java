import java.rmi.Remote;
import java.rmi.RemoteException;


public interface MiddleTier extends Remote{
	public void receiveReq(byte[] req) throws RemoteException;
}
