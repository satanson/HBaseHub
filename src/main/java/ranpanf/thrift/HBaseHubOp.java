package ranpanf.thrift;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import ranpanf.avro.Avro;
import ranpanf.hbasehub.HClusterDescriptor;
import ranpanf.thrift.HBaseHubService.Processor;
/**
 * HBaseHubOp.
 * @author satanson
 * <p> 
 * HBaseHubOp provider API to connect HBaseHub and retrieve runtime info <br/>
 * on HBase clusters which are under supervisory control of HBaseHub, which is<br/>
 * daemon implemented via Apache thrift and persistently monitor the running <br/>
 * of the HBase clusters.
 *
 */
public class HBaseHubOp {
	static Avro avro=Avro.avroize(HClusterDescriptor.class);
	HBaseHubService.Client client;
	/** 
	* HBaseHubOp -- constructor.
	* <p>
	* 	the API wrapped code connecting and accessing HBaseHub listening at tcp://host:port<br/>
	* 	HBaseHub implemented via Apache thrift, and listening at <b>TCP port 8811</b>
	* </p>
	* @param 
	* 	String host: HBaseHub 's host name or IP address.
	* 	int port: HBaseHub's TCP port.
	* @return 
	* 	N/A
	*/ 
	public HBaseHubOp(String host,int port) throws TTransportException{
		TSocket transport = new TSocket(host,port);
		transport.open();
		TProtocol protocol = new  TBinaryProtocol(transport);
		client=new HBaseHubService.Client(protocol);
	}
	/** 
	* getClusters -- get the all cluster under supervisory control,both alive("active") and dead("inactive").
	* @param 
	* @return 
	* 	List<HClusterDescriptor>: return all clusters.
	*/ 
	public List<HClusterDescriptor> getClusters() throws Exception{
		List<ByteBuffer> bytesList=client.getClusters();
		List<HClusterDescriptor> clusterList=null;
		
		if (bytesList==null)return clusterList;
		clusterList=new LinkedList<HClusterDescriptor>();
		for(ByteBuffer bytes:bytesList){
			clusterList.add((HClusterDescriptor)avro.hydrate(bytes.array()));
		}
		return clusterList;
	}
	/** 
	* getMaster -- get the current master cluster.
	* @param 
	* 	void
	* @return 
	* 	HClusterDescriptor: get the current master cluster if it is present and alive,<br/> 
	*                       otherwise return HClusterDescriptor.INVALID_HCLUSTER_DESCRIPTOR()<br/>
	*                       which indicates that the current master is invalid or dead.
	*/ 
	public HClusterDescriptor getMaster() throws Exception{
		ByteBuffer bytes=client.getMasterCluster();
		if (bytes==null)return HClusterDescriptor.INVALID_HCLUSTER_DESCRIPTOR();
		return (HClusterDescriptor)avro.hydrate(bytes.array());
	}
	/** 
	* getSlaves -- get all alive slave clusters.
	* <p> 
	* 	get all alive clusters except master cluster.
	* </p>
	* @param 
	* 	void
	* @return 
	* 	List<HClusterDescriptor>: return all alive slave clusters.
	*/ 
	public List<HClusterDescriptor> getSlaves() throws Exception{
		List<ByteBuffer> bytesList=client.getSlaveClusters();
		List<HClusterDescriptor> clusterList=null;
		
		if (bytesList==null)return clusterList;
		clusterList=new LinkedList<HClusterDescriptor>();
		for(ByteBuffer bytes:bytesList){
			clusterList.add((HClusterDescriptor)avro.hydrate(bytes.array()));
		}
		return clusterList;
	}
	/** 
	* shift -- switch current cluster master to be another one.
	* <p> If the candidate master cluster is inactive(dead), then current master <br/>
	* cluster would keep unchanged. If it's  active(alive), then it would substitute </br>
	* the current master. 
	* </p>
	* @param 
	* 	HClusterDescriptor cluster: the candidate cluster chosen to be master. <br/>
	* @return 
	* 	true: on succeeding in switching cluster,<br/> 
	* 	false: otherwise. 
	*/ 
	public boolean shift(HClusterDescriptor cluster) throws Exception{
		return client.shift(ByteBuffer.wrap(avro.dehydrate(cluster)));
	}
}
