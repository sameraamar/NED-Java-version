package ned.tools;
import java.util.LinkedList;
import java.util.Queue;

public class ClusteringQueueManager<T> {
	
 	private Queue<T> queue; 

 	public ClusteringQueueManager( ) {
		queue=new LinkedList<T>();

	}
	synchronized public T poll() {
		return this.queue.poll();	
	}
	
	synchronized public void add(T str){
		queue.add(str);
	}

	public T peek() {
		return queue.peek();
	}
	public boolean isEmpty() {
		return queue.isEmpty();
	}
	public int size() {
		return queue.size();
	}
	
	


}
