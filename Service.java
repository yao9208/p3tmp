import java.io.IOException;
import java.rmi.*;

public interface Service extends Remote{
	public ReqAndTime getReqFromFrontEnd() throws RemoteException, IOException;
	public int[] getRole() throws RemoteException;
	public int[] getMiddleServerList() throws RemoteException;
	//public void pushRequst(byte[] req) throws RemoteException;
	public ReqAndTime getReqWithTime() throws RemoteException, IOException;
	public void pushRequst(ReqAndTime req) throws RemoteException;
}
