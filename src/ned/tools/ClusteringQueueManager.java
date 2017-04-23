package ned.tools;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import ned.types.Document;
import ned.types.LRUCache;

public class ClusteringQueueManager {
	
 	private Queue<String> queue; 
    private String next;
	public ClusteringQueueManager( ){
		
		queue=new LinkedList<String>();

	}
	synchronized public String  poll(){
		queue.poll();
		this.next=this.queue.poll();
		return next;
		
	}
	synchronized public void  add(String str){
		queue.add(str);
		if(next==null)
		next=queue.poll();
		
	}

	public String peek() {
		return next;
	}
	public boolean isEmpty() {
		return (next==null);
	}
	public Object size() {
		return queue.size();
	}
	
	


}
