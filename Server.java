/* Sample code for basic Server */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;



public class Server extends UnicastRemoteObject implements MiddleTier{
	static int role=1;//1 front end, 2 app server, 3 cache
	static ServerLib SL;
	static ArrayList<int[]> roleList;
	static ArrayList<Integer> middleServerList;
	static Service master=null;
	static List<byte[]> waitlist;
	static int middleNo;//only for middle
	static int curMidIdx=0;
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
		
		String ip = args[0];
		String port = args[1];
		// register with load balancer so requests are sent to this server
		
		int hour = (int) SL.getTime();
		int instances = 2;
		switch(hour){
			case 6:
				instances=2;
				break;
			case 8:
				instances=3;
				break;
			case 19:
				instances=6;
				break;
			default:
				break;
		}
		Master ms = new Master(ip, port, SL);
		boolean ismaster = ms.isMaster();
		if(ismaster){
			SL.register_frontend();
			roleList = new ArrayList<int[]>();
			middleServerList = new ArrayList<Integer>();
			ms.roleList = roleList;
		}else{
			master = (Service) Naming.lookup("//"+ip+":"+port+"/master");
			int[] tmp = master.getRole();
			role = tmp[0];
			middleNo=tmp[1];
		}

		int i=0;
		if(ismaster){
			

				int middle=0;
				while(i<instances-2){
					startAppServer(ms,middle);
					middleServerList.add(middle++);
					ms.middleList = middleServerList;
					i++;
				}

//			startFrontServer(ms);
//			startFrontServer(ms);

		}
		if(role==1){
			int[] midList ;
			if(!ismaster){
				SL.register_frontend();
				midList = master.getMiddleServerList(); 
			}else{
				//Thread.sleep(4000);
				midList = ms.getMiddleServerList();
			}
			List<MiddleTier> mid = new ArrayList<MiddleTier>();
			for(int m:midList){
				mid.add((MiddleTier)Naming.lookup("//"+ip+":"+port+"/middle"+m));
			}
			while(true){
				for(int j=0; j<mid.size(); j++){
					System.out.println("mid size: "+mid.size());
					byte[] req = getReqBytes();
					mid.get(j).receiveReq(req);
				}

			}
		}
		if(role==2){
			waitlist = new ArrayList<byte[]>();
			Server svr = new Server();
			Registry registry = LocateRegistry.getRegistry();
			try {
				Naming.bind("//"+ip+":"+port+"/middle"+middleNo, svr);
			} catch (Exception e) {
				e.printStackTrace();
			}
			while (true) {
				System.out.print("");
				//System.out.println("middle "+middleNo+": waitlist size: "+waitlist.size());
				if (waitlist.size()!=0){
					System.out.println("waitlistsize: "+waitlist.size());
					for(int j=0; j<waitlist.size(); j++){
						processReq(waitlist.get(j));
						waitlist.remove(j);
					}
				}
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
        System.out.println("processReq: "+r.toString());
        o.close();
        SL.processRequest(r);
	}
	public static void startAppServer(Master ms, int no){
		int id = SL.startVM();
		int[] tmp = {2, curMidIdx++};
		ms.roleList.add(tmp);
		
	}
	public static void startFrontServer(Master ms){
		SL.startVM();
		int[] tmp = {1, 0};
		ms.roleList.add(tmp);
	}

	public void receiveReq(byte[] req) throws RemoteException {
		System.out.println("receiving..");
		//System.out.println(new String(req));
		waitlist.add(req);		
		System.out.println("waitlist size: "+waitlist.size());
	}
	public static byte[] getReqBytes() throws IOException {
		// TODO Auto-generated method stub
		Cloud.FrontEndOps.Request r = SL.getNextRequest();
		if(r==null){
			return null;
		}
		System.out.println("getReqBytes: "+r.toString());
		ByteArrayOutputStream b  = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(r);
		byte[] arr = b.toByteArray();
		o.close();
		b.close();
		return arr;
	}
	
}
class Master extends UnicastRemoteObject implements Service{
	String ip;
	String port;
	ArrayList<int[]> roleList;
	ServerLib SL;
	ArrayList<Integer> middleList;
	List<MiddleTier> mid = new ArrayList<MiddleTier>();
	protected Master(String ip, String port, ServerLib SL) throws RemoteException {
		super();
		this.ip = ip;
		this.port = port;
		this.SL = SL;
		this.middleList = new ArrayList<Integer>();
		
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
		// TODO Auto-generated method stub
		Cloud.FrontEndOps.Request r = SL.getNextRequest();
		if(r==null){
			return null;
		}
		System.out.println(r.toString());
		ByteArrayOutputStream b  = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(r);
		byte[] arr = b.toByteArray();
		o.close();
		b.close();
		return arr;
	}
	
	public int[] getRole() throws RemoteException {
		// TODO Auto-generated method stub
		int role=0, middleNo=0;
		if(roleList.size()!=0){
			role = roleList.get(0)[0];
			middleNo = roleList.get(0)[1];
			int[] tmp = {role, middleNo};
			System.out.println("master: "+role+" "+middleNo);
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
	public void addMiddleServer(int m) throws RemoteException {
		mid.add(m);
	}
}

