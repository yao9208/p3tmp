
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;



public class Server extends UnicastRemoteObject implements AppService{
	static int role=1;//1 front end, 2 app server, 3 cache
	static ServerLib SL;
	//static ArrayList<int[]> roleList;
	static LinkedList<Integer> middleServerList;
	static Service master=null;
	static int middleNo;//only for middle
	static int curMidIdx=0;
	static Master ms;
	public static Server aps;
	static boolean shutSignal=false;
	protected Server() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}
	protected Server(int role) throws RemoteException {
		this.role = role;
	}
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <cloud_ip> <cloud_port>");
		SL = new ServerLib( args[0], Integer.parseInt(args[1]) );
		
		final String ip = args[0];
		final String port = args[1];
		// register with load balancer so requests are sent to this server
		
		int hour = (int) SL.getTime();
		int instances = 4;
		ms = new Master(ip, port, SL);
		boolean ismaster = ms.isMaster();
		if(ismaster){
			SL.register_frontend();
			middleServerList = new LinkedList<Integer>();
		}else{
			master = (Service) Naming.lookup("//"+ip+":"+port+"/master");
			int[] tmp = master.getRole();
			role = tmp[0];
			middleNo=tmp[1];
		}

		int i=0;
		if(ismaster){
			Runnable r1 = new Runnable(){
				public void run(){
					long startTime=System.currentTimeMillis();
					long curTime;
					while (true) {
						Cloud.FrontEndOps.Request r = SL.getNextRequest();
						SL.processRequest( r );
						curTime = System.currentTimeMillis();
						if(curTime-startTime>=9000){
							break;
						}
					}
				}
			};
			Thread thr = new Thread(r1);
			thr.start();
			Runnable r2 = new Runnable(){
				public void run(){
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					LinkedList<Integer> list = new LinkedList<Integer>();
					while(true){
						list.add(ms.queueLength());
						if(list.size()>=5){
							int sum=0;
							boolean flag=true;
							for(int m:list){
								System.out.print(m+":");
								if(m<1) flag=false;
							}
							System.out.println();
							for(int i=1; i<5; i++){
								sum+=list.get(i-1)-list.get(i);
							}
							if(flag && sum<=0){
								System.out.println("READY TO ADD SERVER ");
								startAppServer(ms);
							}
							if(!flag &&(ms.middleList.size()>2)){
								for(int n:ms.middleList){
									System.out.print(n+"-");
								}
								int serverid = ms.middleList.remove();
								System.out.println("READY TO REMOVE SERVER "+serverid);
								try {
									shutdownServer(serverid, ip, port);
								} catch (Exception e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							list.clear();
						}
						try {
							Thread.sleep(200);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			};
			Thread th2= new Thread(r2);
			th2.start();
			int middle=0;
			while(i<instances-2){
				startAppServer(ms);
				i++;
			}
			
		}
		if(role==1){
			byte[] byteReq;
			if(!ismaster){
				SL.register_frontend();
				while (true) {
					byteReq = getReqBytes();
					master.pushRequst(byteReq);
				}
			}else{
				int tmp=0;
				long startTime=System.currentTimeMillis();
				while (true) {					
					long curTime;					
					curTime = System.currentTimeMillis();
					if(curTime-startTime>=5000){
						byteReq = getReqBytes();
						ms.pushRequst(byteReq);
					}else{
						Cloud.FrontEndOps.Request r = SL.getNextRequest();
						SL.drop(r);
					}
				
				}
			}
		}
		if(role==2){
			aps = new Server();
			Registry registry = LocateRegistry.getRegistry();
			try {
				System.out.println("binding server"+middleNo);
				Naming.bind("//"+ip+":"+port+"/server"+middleNo, aps);
			} catch (Exception e) {
				e.printStackTrace();
			}
			byte[] req=null;
			while (!shutSignal) {
				try {
					req = master.getReqFromFrontEnd();
				} catch (Exception e) {
					continue;
				}
				processReq(req);//already handle null
			}
		}
	}
	
	
	public static void processReq(byte[] bytes) throws IOException, ClassNotFoundException{
		if(bytes==null){
			return;
		}
		ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        Cloud.FrontEndOps.Request r = (Cloud.FrontEndOps.Request)o.readObject();
        //System.out.println("processReq: "+r.toString());
        o.close();
        SL.processRequest(r);
	}
	public static void startAppServer(Master ms){
		int id = SL.startVM();
		int[] tmp = {2, curMidIdx};
		ms.roleList.add(tmp);
		ms.middleList.add(curMidIdx++);
		
	}
	public static void startFrontServer(Master ms){
		SL.startVM();
		int[] tmp = {1, 0};
		ms.roleList.add(tmp);
	}

	public static byte[] getReqBytes() throws IOException {
		// TODO Auto-generated method stub
		Cloud.FrontEndOps.Request r = SL.getNextRequest();
		if(r==null){
			return null;
		}
		//System.out.println("getReqBytes: "+r.toString());
		ByteArrayOutputStream b  = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(r);
		byte[] arr = b.toByteArray();
		o.close();
		b.close();
		return arr;
	}
	
	public static void shutdownServer(int id, String ip, String port) throws MalformedURLException, RemoteException, NotBoundException{
		
		AppService service = (AppService) Naming.lookup("//"+ip+":"+port+"/server"+id);
		service.shutDownServer();
	}
	@Override
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
	
}
class Master extends UnicastRemoteObject implements Service{
	String ip;
	String port;
	ArrayList<int[]> roleList;
	//LinkedList<ServerInfo> serverInfoList;
	ServerLib SL;
	LinkedList<Integer> middleList;
	LinkedList<byte[]> reqQueue;

	protected Master(String ip, String port, ServerLib SL) throws RemoteException {
		super();
		this.ip = ip;
		this.port = port;
		this.SL = SL;
		this.middleList = new LinkedList<Integer>();
		this.reqQueue = new LinkedList<byte[]>();
		//this.serverInfoList = new LinkedList<ServerInfo>();
		roleList = new  ArrayList<int[]>();
		// TODO Auto-generated constructor stub
	}
	public boolean isMaster() throws AccessException, RemoteException, AlreadyBoundException{
		//Master ms = (Master) UnicastRemoteObject.exportObject(this, 0);
		Registry registry = LocateRegistry.getRegistry();
		try {
			//registry.bind("//127.0.1.1:15640/master", (Remote) this);
			Naming.bind("//"+ip+":"+port+"/master", this);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	public byte[] getReqFromFrontEnd() throws IOException {
		byte[] arr=null;
		if (!reqQueue.isEmpty()) {
			arr = reqQueue.remove();
		}
		return arr;
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
	public void pushRequst(byte[] req) throws RemoteException {
		reqQueue.add(req);		
	}
	public int queueLength(){
		return reqQueue.size();
	}

}

