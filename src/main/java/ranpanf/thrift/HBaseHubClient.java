package ranpanf.thrift;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import ranpanf.hbasehub.HClusterDescriptor;
/**
 * HBaseHubClient.
 * @author satanson
 *
 */
public class HBaseHubClient {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LinkedList<String> arglist = new LinkedList<String>(Arrays.asList(args));

		int port = 8811;
		String host = "localhost";
		if (!arglist.isEmpty()) {
			port = Integer.parseInt(arglist.pop());
		}
		if (!arglist.isEmpty()) {
			host = arglist.pop();
		}
		HBaseHubOp op = new HBaseHubOp(host, port);
		BufferedReader stdin = new BufferedReader(new InputStreamReader(
				System.in));
		String line="0";
		while(true) {
			List<HClusterDescriptor> clusters = op.getClusters();
			System.out.println("cluster size=" + clusters.size());
			int i=0;
			System.out.println("=====>Cluster List<=====");
			for (HClusterDescriptor desc : clusters) {
				System.out.println("("+i+")."+desc + " [" + desc.getState()+ "]");
				++i;
			}
			System.out.println("=====>>>>>>><<<<<<<=====");
			System.out.print("\nchoose master (0.."+(i-1)+") ?");
			if((line = stdin.readLine())== null)break;
			
			int n=Integer.parseInt(line);
			System.out.println("=====>Master Cluster<=====");
			op.shift(clusters.get(n));	
			System.out.println(op.getMaster());
			System.out.println("=====>>>>>>><<<<<<<<<=====");
			System.out.println("\n=====>Slave Clusters<=====");
			List<HClusterDescriptor> slaves = op.getSlaves();
			for (HClusterDescriptor desc : slaves) {
				System.out.println(desc + ":" + desc.getState());
			}
			System.out.println("=====>>>>>>><<<<<<<<<=====\n\n");
			
		}
	}

}
