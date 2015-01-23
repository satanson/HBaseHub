package ranpanf.hbasehub;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.input.SAXBuilder;
import org.jdom2.Element;
/**
 * HBaseHubBackend.
 * @author satanson
 *
 */
public class HBaseHubBackend {
	static Logger log = Logger.getLogger(HBaseHubBackend.class);
	String conf=null;
	
	HClusterDescriptor[] clusters = null;
	HClusterDescriptor master=HClusterDescriptor.INVALID_HCLUSTER_DESCRIPTOR();
	Map<HClusterDescriptor, Boolean> activeMap = null;
	
	private HBaseHubBackend(String conf){
		log.error("create a new HBaseHubBackend"+this);
		this.conf=conf;
		try {
			log.info("set clusters");
			clusters = initClusters();
			activeMap = Collections
					.synchronizedMap(new TreeMap<HClusterDescriptor, Boolean>());
			for (int i = 0; i < clusters.length; ++i) {
				activeMap.put(clusters[i], false);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static HBaseHubBackend backend=new HBaseHubBackend("/hbasehub.xml");
	static public HBaseHubBackend get(){
		return backend;
	}
	public List<HClusterDescriptor> getClusters(){
		List<HClusterDescriptor> candidates=new LinkedList<HClusterDescriptor>();
		for(HClusterDescriptor cluster:clusters){
			candidates.add(cluster);
		}
		return candidates;
	}
	public  boolean setMaster(HClusterDescriptor desc){
		//log.error("setMaster=>"+desc);
		if (isAvailable(desc)){
			log.error(desc+" is available");
			master=desc;
			return true;
		}
		//log.error(desc+" is unavailable");
		return false;
	}
	public  HClusterDescriptor getMaster(){
		log.error("getMaster=>"+master);
		return master;
	}
	public  boolean isAvailable(HClusterDescriptor desc){
//		log.error("activeMap.containsKey(desc)?"+activeMap.containsKey(desc));
//		log.error("activeMap.get(desc)?"+activeMap.get(desc));
//		log.error("desc #"+desc+"#");
//		for(HClusterDescriptor key:activeMap.keySet()){
//			log.error("activeMap #"+key+"#:"+activeMap.get(key));
//		}
		if (activeMap.containsKey(desc) && activeMap.get(desc))
			return true;
		else
			return false;
	}
	public void set(HClusterDescriptor desc,boolean ok){
		activeMap.put(desc, ok);
	}
	public  HClusterDescriptor[] initClusters() throws Exception {
		InputStream in = HBaseHubBackend.class
				.getResourceAsStream(conf);
		SAXBuilder builder = new SAXBuilder();
		Document doc = builder.build(in);
		Element rootElm = doc.getRootElement();
		List<Element> elms = rootElm.getChildren("cluster");
		// log.info("elms.size="+elms.size());
		HClusterDescriptor[] hclusters = new HClusterDescriptor[elms.size()];

		Class<? extends Object> cls = HClusterDescriptor.class;
		for (int i = 0; i < elms.size(); ++i) {
			List<Attribute> attrs = elms.get(i).getAttributes();
			hclusters[i] = new HClusterDescriptor();
			for (Attribute attr : attrs) {
				String name = attr.getName();
				String value = attr.getValue();
				char[] cname = name.toCharArray();
				cname[0] = Character.toUpperCase(cname[0]);
				String setter_name = "set" + String.valueOf(cname);
				// log.info("setter: "+setter_name);
				Class<?>[] setter_params = new Class[] { String.class };
				Method setter = cls.getMethod(setter_name, setter_params);
				setter.invoke(hclusters[i], value);
			}
		}
		return hclusters;
	}

	public static class Test {
		public static void main(String[] args) throws Exception{
			HBaseHubBackend hub=HBaseHubBackend.get();
			log.info("Start....");
			List<HClusterDescriptor> clusters=hub.getClusters();
			log.info(clusters.size() + " cluster(s)");
			for (HClusterDescriptor cluster : clusters) {
				log.info(cluster);
			}
			
			ScheduledExecutorService periodic = Executors
					.newScheduledThreadPool(2);
			for (HClusterDescriptor cluster : clusters)  {
				periodic.scheduleAtFixedRate(new HClusterProber(cluster,hub), 0, 2, TimeUnit.SECONDS);
			}
			
			while (true) {
				for (HClusterDescriptor cluster : clusters) {
					log.error(cluster+"  is active ? " + hub.isAvailable(cluster));
				}
				Thread.sleep(2000);
			}
			//log.info("Stop...");
		}
	}
}
