package ned.types;

import java.io.PrintStream;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import ned.tools.ClusteringQueueManager;
import ned.tools.RedisHelper;

public class DocumentClusteringThread extends Thread {
	private boolean stop = false;
	private GlobalData gd;
	private PrintStream out;
	
	public DocumentClusteringThread(PrintStream out)
	{
		this.out = out;
		gd = GlobalData.getInstance();
	}
	
	@Override
	public void run() 
	{
		while(!stop) 
		{
			mapToCluster();
			
    		Session.getInstance().message(Session.DEBUG, "Reader", "doing some cleanup...");
    		gd.flushClusters(out);
    		
			try 
			{
				sleep(4000);
			} 
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
			
		}
		
		//last time
		//wait for all other threads to finish
		while(!mapToCluster())
		{}

		gd.flushClustersAll(out);

	}
	
	private boolean mapToCluster()
	{
		Document doc = next();
		
		Document last = doc;
		while(doc!=null)
		{
	        DocumentClusteringHelper.mapToClusterHelper(doc);
	        last = doc;
			doc = next();
		}
		
		if(last != null)
			gd.markOldClusters(last);

		return gd.getQueue().isEmpty();
	}
	
	private Document next()
	{
		GlobalData gd = GlobalData.getInstance();
		ClusteringQueueManager queue = gd.getQueue();
		
		
		
		String id = queue.peek();
		if (id == null)
			return null;
		
		boolean breakme = false;
		if(id.equals("86417673814151168"))
			breakme = true;
		
		Document doc =RedisHelper.getDocumentFromRedis(id);
		if (doc==null)
			return null;
		

		if(doc.isNearestDetermined())
		{
			queue.poll();
			return doc;
		}
		
		return null;
	}

	public void shutdown() 
	{
		while (!gd.getQueue().isEmpty())
		{
		}
		stop = true;
	}
		
}
