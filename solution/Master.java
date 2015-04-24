import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;


class Master extends UnicastRemoteObject implements Service{
	String ip;
	String port;
	ArrayList<int[]> roleList;
	//LinkedList<ServerInfo> serverInfoList;
	ServerLib SL;
	public static LinkedList<Integer> middleList;
	public static LinkedList<Integer> frontlist;
	public static LinkedList<ReqAndTime> reqQueue;
	static HashMap<Integer, Integer> lengthList;
	public static LinkedList<Integer> readyServers;
	static LinkedList<Long> waitTime;

	protected Master(String ip, String port, ServerLib SL) throws RemoteException {
		super();
		this.ip = ip;
		this.port = port;
		this.SL = SL;
		this.middleList = new LinkedList<Integer>();
		this.frontlist = new LinkedList<Integer>();
		this.reqQueue = new LinkedList<ReqAndTime>();
		roleList = new  ArrayList<int[]>();
		lengthList = new HashMap<Integer, Integer>();
		readyServers = new LinkedList<Integer>();
		waitTime = new LinkedList<Long>();

		//set = new HashSet<String>();
	}
	public boolean isMaster() throws AccessException, RemoteException, AlreadyBoundException{
		try {
			Naming.bind("//"+ip+":"+port+"/master", this);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	public ReqAndTime getReqWithTime(Integer id) throws RemoteException, IOException {
		ReqAndTime req=null;
		long starttime=System.currentTimeMillis();
		long time;
		readyServers.add(id);
		synchronized(id){
			try {
				id.wait();
			} catch (InterruptedException e) {
				
			}
		}  
		//System.out.println(System.currentTimeMillis()-starttime);
		waitTime.add(System.currentTimeMillis()-starttime);
		while(waitTime.size()>5){
			waitTime.remove();
		}
		synchronized(reqQueue) {
			while (!reqQueue.isEmpty()) {
				req = reqQueue.remove();
				time = System.currentTimeMillis();
				//System.out.println("time: "+(System.currentTimeMillis()-req.timestamp));
				if(req.r.isPurchase && (time-req.timestamp)>795){
					//System.out.println("get purchase drop");
					SL.drop(req.r);
				}else if ((time-req.timestamp)>795){
					//System.out.println("get browse drop");
					SL.drop(req.r);
				}
				else{
					break;
				}
			}
		}
		return req;
	}
	
	public int[] getRole() throws RemoteException {
		// TODO Auto-generated method stub 
		int role=0, middleNo=0;
		if(roleList.size()!=0){
			role = roleList.get(0)[0];
			middleNo = roleList.get(0)[1];
			int[] tmp = {role, middleNo};
			//System.out.println("master: "+role+" "+middleNo);
			roleList.remove(0);
			return tmp;
		}else{
			return null;
		}
	}
	public int[] getMiddleServerList() throws RemoteException {
		int size = middleList.size();
		int[] result = new int[size];
		for(int i=0; i<size; i++){
			result[i] = middleList.get(i);
		}
		return result;
	}
	@Override
	public void pushRequst(ReqAndTime req, int server, int length) throws RemoteException {
		reqQueue.add(req);	
		lengthList.put(server, length);
	}
	public static int queueLength(){
		return reqQueue.size();
	}
	@Override
	public ReqAndTime getReqFromFrontEnd() throws RemoteException, IOException {
		// TODO Auto-generated method stub
		return null;
	}
	public static int frontQueueLength(){
		int sum=0;
		Iterator<Entry<Integer, Integer>> it = lengthList.entrySet().iterator();
		while(it.hasNext()){
			Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>) it.next();
			sum += entry.getValue();
		}
		return sum;
	}
	public long sumTime(){
		long sum=0;
		for(int i=0; i<waitTime.size(); i++){
			sum += waitTime.get(i);
		}
		return sum;
	}

}
