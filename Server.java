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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;


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
	static long interval;
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
			if(SL.getTime()==20){
				instances=5;
			}
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
					long lasttime=0, lastmidtime=0, lastjudge=0;
					int prevLength=0,sumlen=0;
					double sum=0.0, alpha=1.0;
					int time=0;
					while(true){
						long curTime;					
						curTime = System.currentTimeMillis();						
						//boolean urgent=true;
						int queueLength = ms.queueLength();
						int frontqueueLength = ms.frontQueueLength();
						int middlenum = ms.middleList.size();
						int frontnum = ms.frontlist.size();
						boolean first=true;
						if(queueLength>=middlenum && middlenum<10 &&(curTime-lastmidtime)>6000){
							miduptimes++;
							sum += queueLength;
							if(miduptimes>=5){
								double bootnum=(sum/miduptimes/(middlenum*0.5));
								bootnum = (first)? bootnum-1:bootnum*0.7;
								System.out.println(sum+" scale "+bootnum);
								for (int j = 0; j < bootnum && ms.middleList.size()<10; j++) {
									System.out.print("ADD APP SERVER"
											+ curserverIdx);
									System.out.println(" time: "+(System.currentTimeMillis()-startTime));
									startAppServer(ms);
								}
								if(first){
									first=false;
								}
								lastmidtime=curTime;
								miduptimes=0;
								sum=0;
								alpha*=1;
							}
						}else if(queueLength<middlenum){
							miduptimes=0;
						}
						//System.out.println("frontnum: "+ms.frontlist.size()+" length: "+frontqueueLength);
						if(frontqueueLength>=(frontnum+1)*0.5 && frontnum<3 && (curTime-lasttime)>6000 &&(curTime-startTime)>3000){
							//System.out.println(ms.frontlist.size());
							frontuptimes++;
							if(frontuptimes>=1 && frontqueueLength<2*(frontnum+1)){
								System.out.println("length: "+frontqueueLength);
								System.out.print("ADD FRONT SERVER "+curserverIdx);
								System.out.println(" time: "+(System.currentTimeMillis()-startTime));
								startFrontServer(ms);
								lasttime = curTime;
								frontuptimes=0;
							}else if(frontuptimes>=1){
								

								for(int i=0; i<frontqueueLength/(frontnum+1)/5 && i<3; i++) {
									System.out.println("ADD FRONT SERVER "
											+ curserverIdx);
									startFrontServer(ms);
								}
								lasttime = curTime;
								frontuptimes=0;
							}
						}
						
						if(queueLength<(middlenum-1) && (curTime-startTime)>13000 &&(curTime-lastmidtime)>6800){
							middowntimes++;
							if(middowntimes>=35){
								int serverid = ms.middleList.remove();
								lastmidtime=curTime;
								System.out.print("REMOVE APP SERVER"+serverid);
								System.out.println(" time: "+(System.currentTimeMillis()-startTime));
								try {
									shutdownServer(serverid, ip, port);
								} catch (Exception e) {
									e.printStackTrace();
								}
								middowntimes=0;
							}
						}else if(queueLength>=(middlenum-1)){
							middowntimes=0;
						}
//						if(frontqueueLength<=frontnum && (curTime-lasttime)>3000 && (curTime-startTime)>16000){
//							frontdowntimes++;
//							if(frontdowntimes>=25){
//								int serverid = ms.frontlist.remove();
//								lasttime = curTime;
//								System.out.println("REMOVE FRONT SERVER"+serverid);
//								try {
//									shutdownServer(serverid, ip, port);
//								} catch (Exception e) {
//									e.printStackTrace();
//								}
//								frontdowntimes=0;
//							}
//						}else if(frontqueueLength>frontnum){
//							frontdowntimes=0;
//						}
						int curLength = SL.getQueueLength();
						int diff = curLength-prevLength;
						sumlen+=diff;
						time=time+1;
						prevLength=curLength;
						//System.out.println(time);
						if(time>=4 && (curTime-lastjudge)>3000 && ms.frontlist.size()<3){
							int num = sumlen;
							for(int i=0; i<num/2 && ms.frontlist.size()<3; i++){
								System.out.print("ADD FRONT SERVER "+curserverIdx);
								System.out.println(" time: "+(System.currentTimeMillis()-startTime));
								startFrontServer(ms);
							}
//							for(int i=0; i<num; i++){
//								System.out.println("ADD APP SERVER" + curserverIdx);
//								startAppServer(ms);
//							}
							time=0;
							sumlen=0;
							lastjudge=curTime;
							//lasttime=curTime; lastmidtime=curTime;
						}
						
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
//						while(ms.queueLength()>2.5*ms.middleList.size()){
//							//System.out.println("length drop");
//							SL.dropHead();
//							try {
//								Thread.sleep(2);
//							} catch (InterruptedException e) {
//								// TODO Auto-generated catch block
//								//e.printStackTrace();
//							}
////								byte[] arr = ms.reqQueue.remove();
////								dropReq(arr);							
//						}
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
					master.pushRequst(byteReq, serverNo, SL.getQueueLength());
				}
			}else{
				int tmp=0;		
				long prevTime=System.currentTimeMillis();
				while (true) {	
					long curTime;					
					curTime = System.currentTimeMillis();
					if(curTime-startTime>=5000){
						interval = curTime-prevTime;
						prevTime=curTime;
						byteReq = getReqWithTime();
						ms.pushRequst(byteReq, 0, SL.getQueueLength());						
					}else{
//						Cloud.FrontEndOps.Request r = SL.getNextRequest();
//						SL.drop(r);
						SL.dropHead();
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
		if(ms.frontlist.contains(id)){
			ms.frontlist.remove(id);
		}
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
		//ms.set.add(key);
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
	HashMap<Integer, Integer> lengthList;
	//HashSet<String> set;

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
	public ReqAndTime getReqWithTime() throws RemoteException, IOException {
		ReqAndTime req=null;
		long time;
		synchronized(reqQueue) {
			while (!reqQueue.isEmpty()) {
				req = reqQueue.remove();
				time = System.currentTimeMillis();
				//System.out.println("time: "+(System.currentTimeMillis()-req.timestamp));
				if(req.r.isPurchase && (time-req.timestamp)>795){
					System.out.println("get purchase drop");
					SL.drop(req.r);
				}else if ((time-req.timestamp)>795){
					System.out.println("get browse drop");
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
	public int queueLength(){
		return reqQueue.size();
	}
	@Override
	public ReqAndTime getReqFromFrontEnd() throws RemoteException, IOException {
		// TODO Auto-generated method stub
		return null;
	}
	public int frontQueueLength(){
		int sum=0;
		Iterator<Entry<Integer, Integer>> it = lengthList.entrySet().iterator();
		while(it.hasNext()){
			Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>) it.next();
			sum += entry.getValue();
		}
		return sum;
	}

}
