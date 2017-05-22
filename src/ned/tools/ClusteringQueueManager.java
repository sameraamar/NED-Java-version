package ned.tools;
import java.util.LinkedList;
import java.util.Queue;

public class ClusteringQueueManager {
	
 	private Queue<String> queue; 

 	public ClusteringQueueManager( ) {
		queue=new LinkedList<String>();

	}
	synchronized public String  poll() {
		return this.queue.poll();	
	}
	
	synchronized public void  add(String str){
		queue.add(str);
	}

	public String peek() {
		return queue.peek();
	}
	public boolean isEmpty() {
		return queue.isEmpty();
	}
	public int size() {
		return queue.size();
	}
	
	


}
