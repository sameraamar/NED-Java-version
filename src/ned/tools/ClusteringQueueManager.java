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
	private Queue <String> queueBuffer;
	public ClusteringQueueManager( ){
		
		queueBuffer=new  LinkedList<String>();
		queue=new LinkedList<String>();

	}
	public String  poll(){
		String tmp=next;
		next=queue.poll();
		return tmp;
		
	}
	synchronized public void  add(String str){
		queue.add(str);
		if(next==null)
		next=queue.poll();
		
	}

	public String peek() {
		return queue.peek();
	}
	public boolean isEmpty() {
		return (next==null);
	}
	public Object size() {
		return queue.size();
	}
	
	


}
