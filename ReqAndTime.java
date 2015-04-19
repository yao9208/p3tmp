import java.io.Serializable;


public class ReqAndTime implements Serializable{
	Cloud.FrontEndOps.Request r;
	long timestamp;
	public ReqAndTime(Cloud.FrontEndOps.Request r, long timestamp){
		this.r = r;
		this.timestamp = timestamp;
	}
}
