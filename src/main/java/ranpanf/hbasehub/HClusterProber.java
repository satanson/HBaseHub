package ranpanf.hbasehub;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.log4j.Logger;

/**
 * HClusterProber.
 * @author satanson
 *
 */
public class HClusterProber implements Runnable {
	static Logger log=Logger.getLogger(HClusterProber.class);
	
	private HClusterDescriptor cluster;
	private HBaseHubBackend hub;
	public HClusterProber(HClusterDescriptor cluster,HBaseHubBackend hub){
		this.cluster=cluster;
		this.hub = hub;
	}
	public void run() {
		//log.error("probe hbase clusters...");
		HttpClient cli=new DefaultHttpClient();
		cli.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,1000);
		cli.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT,1000);
		HttpGet get=new HttpGet("http://"+cluster);
		boolean ok=false;
		try {
			HttpResponse response=cli.execute(get);
			//log.error("get: "+get+":"+response.getStatusLine().getStatusCode());
			if (response.getStatusLine().getStatusCode()-200<99){
				ok=true;
			}else {
				ok=false;
			}
		} catch(Exception ex){
			//log.error(ex.getMessage());
			ok=false;
		} finally{
			hub.set(cluster, ok);
		}
	}

}
