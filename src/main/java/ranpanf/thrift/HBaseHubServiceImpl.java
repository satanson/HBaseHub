package ranpanf.thrift;

import java.nio.ByteBuffer;
import ranpanf.hbasehub.*;
import ranpanf.avro.Avro;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ranpanf.thrift.HBaseHubService.Iface;
/**
 * HBaseHubServiceImpl.
 * @author satanson
 *
 */
public class HBaseHubServiceImpl implements Iface {
	static Logger log = Logger.getLogger(HBaseHubServiceImpl.class);
	static Avro avro=Avro.avroize(HClusterDescriptor.class);
	static HBaseHubBackend hub=HBaseHubBackend.get();
	public boolean shift(ByteBuffer cluster)
			throws HBaseClusterSwitchException, TException {
		try{
			return hub.setMaster((HClusterDescriptor)avro.hydrate(cluster.array()));
		} catch(Exception ex){
			throw new HBaseClusterSwitchException();
		}
	}

	public List<ByteBuffer> getClusters() throws TException {
		// TODO Auto-generated method stub
		log.error("invoke getClusters");
		List<ByteBuffer> clusterList=new LinkedList<ByteBuffer>();
		try{
			log.error("hub.getClusters()="+hub.getClusters().size());
			for (HClusterDescriptor desc:hub.getClusters()){
				if(hub.isAvailable(desc)){
					desc.setState("active");	
				} else{
					desc.setState("inactive");
				}
				clusterList.add(ByteBuffer.wrap(avro.dehydrate(desc)));		
			}
		} catch(Exception ex){
		}
		log.error("clusterList size="+clusterList.size());
		return clusterList;
	}

	public ByteBuffer getMasterCluster() throws TException {
		try {
			log.error(hub.getMaster());
			return ByteBuffer.wrap(avro.dehydrate(hub.getMaster()));
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	public List<ByteBuffer> getSlaveClusters() throws TException {
		List<ByteBuffer> clusterList=new LinkedList<ByteBuffer>();
		try{
			for (HClusterDescriptor desc:hub.getClusters()){
				if(hub.isAvailable(desc) && !desc.equals(hub.getMaster())){
					clusterList.add(ByteBuffer.wrap(avro.dehydrate(desc)));
				}
			}
		} catch(Exception ex){
		}
		return clusterList;
	}

}
