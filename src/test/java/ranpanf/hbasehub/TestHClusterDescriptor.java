package ranpanf.hbasehub;
import ranpanf.hbasehub.HClusterDescriptor;
import ranpanf.avro.Avro;
public class TestHClusterDescriptor {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		HClusterDescriptor a=new HClusterDescriptor();
		a.setHmaster("www.baidu.com");
		a.setPort("80");
		a.setState("active");
		
		HClusterDescriptor b=new HClusterDescriptor();
		b.setHmaster("www.baidu.com");
		b.setPort("80");
		b.setState("inactive");
		System.out.println("a==b?"+a.equals(b));
		Avro avro=Avro.avroize(HClusterDescriptor.class);
		HClusterDescriptor c=(HClusterDescriptor)avro.hydrate(avro.dehydrate(a));
		System.out.println("a==c?"+a.equals(c));
		System.out.println("b==c?"+b.equals(c));
		
		//Map<"www.baidu.com",> map =new HashMap();
	}

}
