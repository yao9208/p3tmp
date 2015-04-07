import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;


public class AppServer extends UnicastRemoteObject implements AppService{
	/**
	 * 
	 */
	private static final long serialVersionUID = 693239945041979335L;
	ServerLib SL;
	Master mst;
	protected AppServer(ServerLib SL, Master mst) throws RemoteException {
		super();
		this.SL = SL;
		this.mst = mst;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void shutDownServer() throws RemoteException {
		SL.interruptGetNext();
		try {
			UnicastRemoteObject.unexportObject(mst, true);
			UnicastRemoteObject.unexportObject(this, true);
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		}
		SL.shutDown();
		
	}

}
