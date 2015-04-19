import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;


public class Server extends UnicastRemoteObject implements AppService, Cloud.DatabaseOps{
	static int role=1;//1 front end, 2 app server, 3 cache
	static ServerLib SL;
	//static ArrayList<int[]> roleList;
	static LinkedList<Integer> middleServerList;
	static Service master=null;
	static int serverNo;//only for middle
	static int curserverIdx=0;
	static Master ms;
	public static Server aps;
	static boolean shutSignal=false;
	static HashMap<String, String> cache;
	static Cloud.DatabaseOps DB;
	protected Server() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}
	protected Server(int role) throws RemoteException {
		this.role = role;
	}
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <cloud_ip> <cloud_port>");
		final long startTime=System.currentTimeMillis();
		SL = new ServerLib( args[0], Integer.parseInt(args[1]) );
		
		final String ip = args[0];
		final String port = args[1];
		// register with load balancer so requests are sent to this server
		
		int hour = (int) SL.getTime();
		int instances = 2;
		ms = new Master(ip, port, SL);
		boolean ismaster = ms.isMaster();
		if(ismaster){
			SL.register_frontend();
			//startDBServer(ms);
			middleServerList = new LinkedList<Integer>();
			
			aps = new Server();
			Naming.bind("//"+ip+":"+port+"/DBserver", aps);
			DB = SL.getDB();
			cache = new HashMap<String, String>();
		}else{
			master = (Service) Naming.lookup("//"+ip+":"+port+"/master");
			int[] tmp = master.getRole();
			role = tmp[0];
			serverNo=tmp[1];
		}

		int i=0;
		if(ismaster){
			startFrontServer(ms);
			while(i<instances){
				startAppServer(ms);
				i++;
			}
			
			Runnable r2 = new Runnable(){
				public void run(){
					try {
						//System.out.println("thread2 sleep 5s...");
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					int miduptimes=0, middowntimes=0;
					int frontuptimes=0, frontdowntimes=0;
					long lasttime=0;
					while(true){
						long curTime;					
						curTime = System.currentTimeMillis();
						boolean urgent=true;
						int queueLength = ms.queueLength();
						int middlenum = ms.middleList.size();
						int frontnum = ms.frontlist.size();
						if(queueLength>=middlenum && middlenum<9){
							miduptimes++;
							if(queueLength<2*middlenum){
								urgent = false;
							}
							if(miduptimes>=3){
								System.out.println("ADD APP SERVER"+curserverIdx);
								startAppServer(ms);
								if(urgent){
									System.out.println("ADD APP SERVER "+curserverIdx);
									startAppServer(ms);
								}
								miduptimes=0;
							}
						}
						if(SL.getQueueLength()*(frontnum+1)>=1 && frontnum<3 && (curTime-lasttime)>2000 &&(curTime-startTime)>5000){
							//System.out.println(ms.frontlist.size());
							frontuptimes++;
							if(frontuptimes>=1){
								System.out.println("ADD FRONT SERVER "+curserverIdx);
								startFrontServer(ms);
								lasttime = curTime;
								frontuptimes=0;
							}
						}
						if(queueLength<(middlenum-1) && (curTime-startTime)>13000){
							middowntimes++;
							if(middowntimes>=20){
								int serverid = ms.middleList.remove();
								System.out.println("REMOVE APP SERVER"+serverid);
								try {
									shutdownServer(serverid, ip, port);
								} catch (Exception e) {
									e.printStackTrace();
								}
								middowntimes=0;
							}
						}
						if(SL.getQueueLength()*frontnum<1 && (curTime-startTime)>13000){
							frontdowntimes++;
							if(frontdowntimes>=15){
								int serverid = ms.frontlist.remove();
								System.out.println("REMOVE FRONT SERVER"+serverid);
								try {
									shutdownServer(serverid, ip, port);
								} catch (Exception e) {
									e.printStackTrace();
								}
								frontdowntimes=0;
							}
						}
						
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						while(ms.queueLength()>3*ms.middleList.size()){
							SL.dropHead();
//								byte[] arr = ms.reqQueue.remove();
//								dropReq(arr);							
						}
						frontuptimes=0;
					}
				}
			};
			Thread th2= new Thread(r2);
			th2.start();
						
		}
		if(role==1){
			ReqAndTime byteReq;
			if(!ismaster){
				aps = new Server();
				try {
					System.out.println("binding server"+serverNo);
					Naming.bind("//"+ip+":"+port+"/server"+serverNo, aps);
				} catch (Exception e) {
					e.printStackTrace();
				}
				SL.register_frontend();
				while (true) {
					byteReq = getReqWithTime();
					master.pushRequst(byteReq);
				}
			}else{
				int tmp=0;				
				while (true) {	
					long curTime;					
					curTime = System.currentTimeMillis();
					if(curTime-startTime>=5000){
						byteReq = getReqWithTime();
						ms.pushRequst(byteReq);
					}else{
						Cloud.FrontEndOps.Request r = SL.getNextRequest();
						SL.drop(r);
					}
					Thread.sleep(2);
				}
			}
		}
		if(role==2){
			aps = new Server();
			try {
				System.out.println("binding server"+serverNo);
				Naming.bind("//"+ip+":"+port+"/server"+serverNo, aps);
			} catch (Exception e) {
				e.printStackTrace();
			}
			ReqAndTime req=null;
			Cloud.DatabaseOps db= (Cloud.DatabaseOps) Naming.lookup("//"+ip+":"+port+"/DBserver");
			while (!shutSignal) {
				try {
					req = master.getReqWithTime();
				} catch (Exception e) {
					continue;
				}
				processReq(req, db);//already handle null
			}
		}
	}
	

	public static void processReq(ReqAndTime req, Cloud.DatabaseOps db) throws IOException, ClassNotFoundException{
		if(req==null){
			return;
		}
		long time = System.currentTimeMillis();
		if(req.r.isPurchase && (time-req.timestamp)>1810){
			SL.drop(req.r);
			return;
		}else if((time-req.timestamp)>850){
			SL.drop(req.r);
			return;
		}
        SL.processRequest(req.r, db);
	}
	

	public static void startAppServer(Master ms){
		int id = SL.startVM();
		int[] tmp = {2, curserverIdx};
		ms.roleList.add(tmp);
		ms.middleList.add(curserverIdx++);		
	}
	
	public static void startFrontServer(Master ms){
		SL.startVM();
		int[] tmp = {1, curserverIdx};
		ms.roleList.add(tmp);
		ms.frontlist.add(curserverIdx++);
	}
	
	public static void startDBServer(Master ms){
		SL.startVM();
		int[] tmp = {3, 0};
		ms.roleList.add(tmp);
	}
	
	public static ReqAndTime getReqWithTime(){
		Cloud.FrontEndOps.Request r = SL.getNextRequest();
		long curTime = System.currentTimeMillis();
		ReqAndTime result = new ReqAndTime(r, curTime);
		return result;
	}
	
	public static void shutdownServer(int id, String ip, String port) throws MalformedURLException, RemoteException, NotBoundException{		
		AppService service = (AppService) Naming.lookup("//"+ip+":"+port+"/server"+id);
		service.shutDownServer();
	}

	public void shutDownServer() throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("SHUT DOWN--------");
		shutSignal=true;
		SL.shutDown();
		try {
			UnicastRemoteObject.unexportObject(aps, true);
			UnicastRemoteObject.unexportObject(ms, true);
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		}
	}
	@Override
	public String get(String key) throws RemoteException {
		if(cache.containsKey(key)){
			return cache.get(key);
		}
		String val = DB.get(key);
		cache.put(key, val);
		return val;
	}
	@Override
	public boolean set(String arg0, String arg1, String arg2)
			throws RemoteException {
		boolean val = DB.set(arg0, arg1, arg2);
		return val;
	}
	@Override
	public boolean transaction(String arg0, float arg1, int arg2)
			throws RemoteException {
		boolean val = DB.transaction(arg0, arg1, arg2);
		return val;
	}
	
}
class Master extends UnicastRemoteObject implements Service{
	String ip;
	String port;
	ArrayList<int[]> roleList;
	//LinkedList<ServerInfo> serverInfoList;
	ServerLib SL;
	LinkedList<Integer> middleList;
	LinkedList<Integer> frontlist;
	LinkedList<ReqAndTime> reqQueue;

	protected Master(String ip, String port, ServerLib SL) throws RemoteException {
		super();
		this.ip = ip;
		this.port = port;
		this.SL = SL;
		this.middleList = new LinkedList<Integer>();
		this.frontlist = new LinkedList<Integer>();
		this.reqQueue = new LinkedList<ReqAndTime>();
		roleList = new  ArrayList<int[]>();
	}
	public boolean isMaster() throws AccessException, RemoteException, AlreadyBoundException{
		try {
			Naming.bind("//"+ip+":"+port+"/master", this);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	public ReqAndTime getReqWithTime() throws RemoteException, IOException {
		ReqAndTime req=null;
		long time;
		synchronized(reqQueue) {
			while (!reqQueue.isEmpty()) {
				req = reqQueue.remove();
				time = System.currentTimeMillis();
				if(req.r.isPurchase && (time-req.timestamp)>1780){
					SL.drop(req.r);
				}else if ((time-req.timestamp)>830){
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
	public void pushRequst(ReqAndTime req) throws RemoteException {
		reqQueue.add(req);		
	}
	public int queueLength(){
		return reqQueue.size();
	}
	@Override
	public ReqAndTime getReqFromFrontEnd() throws RemoteException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

}