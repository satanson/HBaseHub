
package ranpanf.hbasehub;
/**
 * HClusterDescriptor.
 * @author satanson <br/>
 * <p>
 * HClusterDescriptor used to wrap the running state of HBase clusters.<br/>
 * HClusterDescriptor object must be auto configured by <b>hbasehub.xml</b><br/>
 * HClusterDescriptor's (de)serialization is performed by ranpanf.avro.Avro<br/>
 * which implemented via Apache Avro. ranpanf.avro.Avro can create Schema of a<br/>
 * type on-fly.Avro provide two functions: "dehygrate" to serialize object and <br/>
 * "hygrate" to deserialize object. the function names originate from 3Body by Liu Cixin.<br/>
 * Please visit <a href="http://www.3body.com/forum.php">http://www.3body.com/forum.php</a> for details about 3Body. 
 */
public class HClusterDescriptor  implements Comparable<HClusterDescriptor>{
	private String hmaster;
	private String port;
	private String state="who care?";
	
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getHmaster() {
		return hmaster;
	}
	public void setHmaster(String hmaster) {
		this.hmaster = hmaster;
		
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String toString(){
		return hmaster+":"+port;
	}
	public boolean equals(Object other){
		if (other!=null && other instanceof HClusterDescriptor){
			return this.toString().equals(other.toString());
		}else{
			return false;
		}
	}
	public static HClusterDescriptor INVALID_HCLUSTER_DESCRIPTOR(){
		HClusterDescriptor desc=new HClusterDescriptor();
		desc.setHmaster("invalid");
		desc.setPort("invalid");
		return desc;
	}

	public int compareTo(HClusterDescriptor o) {
		return o.toString().compareTo(this.toString());
	}
}
