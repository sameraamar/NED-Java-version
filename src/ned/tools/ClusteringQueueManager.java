package ned.tools;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import ned.types.Document;
import ned.types.LRUCache;

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
	public Object size() {
		return queue.size();
	}
	
	


}
