package ranpanf.thrift;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;

import ranpanf.hbasehub.HBaseHubBackend;
import ranpanf.hbasehub.HClusterDescriptor;
import ranpanf.hbasehub.HClusterProber;
import ranpanf.thrift.HBaseHubService.Processor;
/**
 * HBaseHub.
 * @author satanson
 *
 */
public class HBaseHub {
	static Logger log=Logger.getLogger(HBaseHub.class);
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
LinkedList<String> arglist=new LinkedList<String>(Arrays.asList(args));
		
		int port=8811;
		if (!arglist.isEmpty()){
			port=Integer.parseInt(arglist.pop());
		}
		
		HBaseHubBackend hub=HBaseHubBackend.get();
		log.info("Start....");
		List<HClusterDescriptor> clusters=hub.getClusters();
		log.info(clusters.size() + " cluster(s)");
		for (HClusterDescriptor cluster : clusters) {
			log.info(cluster);
		}
		
		ScheduledExecutorService periodic = Executors
				.newScheduledThreadPool(2);
		for (HClusterDescriptor cluster : clusters) {
			periodic.scheduleAtFixedRate(new HClusterProber(cluster,hub), 0, 2, TimeUnit.SECONDS);
		}
		
		TServerSocket serverTransport = new TServerSocket(port);
		Processor<HBaseHubServiceImpl> processor = new HBaseHubService.Processor<HBaseHubServiceImpl>(new HBaseHubServiceImpl());
		Factory protFactory=new TBinaryProtocol.Factory(true, true);
		TServer server =new TThreadPoolServer(new Args(serverTransport).processor(processor).protocolFactory(protFactory));
		server.serve();
		System.out.println("can't reach here!!!");
	}
}
