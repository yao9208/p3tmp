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
	static LinkedList<Integer> middleServerList;
	static Service master=null;
	static int serverNo;
	static int curserverIdx=0;
	static Master ms;
	public static Server aps;
	static boolean shutSignal=false;
	static HashMap<String, String> cache;
	static Cloud.DatabaseOps DB;
	static long interval;
	protected Server() throws RemoteException {
		super();
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
			Runnable schedule = new Runnable(){
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
					int prevLength=0;
					long sumlen=0;
					double sum=0.0;
					int time=0;
					while(true){
						long curTime;					
						curTime = System.currentTimeMillis();						
						int queueLength = ms.queueLength();
						int frontqueueLength = ms.frontQueueLength();
						int middlenum = ms.middleList.size();
						int frontnum = ms.frontlist.size();
						boolean first=true;
						if(queueLength>=middlenum && middlenum<10 &&(curTime-lastmidtime)>5500){
							miduptimes++;
							sum += queueLength;
							if(miduptimes>=5){
								double bootnum=(sum/miduptimes/(middlenum));
								bootnum = (first)? bootnum*2:bootnum*0.5;
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
							}
						}else if(queueLength<middlenum){
							miduptimes=0;
						}

						if(frontqueueLength>=(frontnum+1)*0.5 && frontnum<2 && (curTime-lasttime)>8000 &&(curTime-startTime)>3000){

							frontuptimes++;
							if(frontuptimes>=1 && frontqueueLength<2*(frontnum+1)){
								System.out.println("length: "+frontqueueLength);
								System.out.print("ADD FRONT SERVER "+curserverIdx);
								System.out.println(" time: "+(System.currentTimeMillis()-startTime));
								startFrontServer(ms);
								lasttime = curTime;
								frontuptimes=0;
							}
						}
											
						sumlen+=ms.sumTime();
						time=time+1;

						if(time>=4 && (curTime-lastjudge)>500 && ms.middleList.size()>2 && (curTime-startTime)>27000){
							if(sumlen/5>1500){
								System.out.println("scale in "+sumlen/5);
								int serverid = ms.middleList.remove();
								System.out.print("REMOVE APP SERVER"+serverid);
								System.out.println(" time: "+(System.currentTimeMillis()-startTime));
								try {
									shutdownServer(serverid, ip, port);
								} catch (Exception e) {
									e.printStackTrace();
								}
								
								if(ms.frontlist.size()>1){
									serverid = ms.frontlist.remove();
									System.out.print("REMOVE FRONT SERVER"+serverid);
									System.out.println(" time: "+(System.currentTimeMillis()-startTime));
									try {
										shutdownServer(serverid, ip, port);
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
								
								
								lastmidtime=curTime;
							}
							if(sumlen/5>2000 && ms.middleList.size()>2){
								int serverid = ms.middleList.remove();
								System.out.print("REMOVE APP SERVER"+serverid);
								System.out.println(" time: "+(System.currentTimeMillis()-startTime));		
								try {
									shutdownServer(serverid, ip, port);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							time=0;							
							sumlen=0;
						}else if(time>=4){
							sumlen=0;
							time=0;
						}
						
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}

						frontuptimes=0;
					}
				}
			};
			Thread masterSchedule= new Thread(schedule);
			masterSchedule.start();
			Thread notify = new notifyAppServer();
			notify.start();
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
					req = master.getReqWithTime(serverNo);
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

class notifyAppServer extends Thread{
	public void run(){
		while (true) {
			if (Master.reqQueue.size() > 0) {
				if (Master.readyServers.size() > 0) {
					Integer integer = Master.readyServers.remove();
					synchronized (integer) {
						integer.notify();
					}  
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
