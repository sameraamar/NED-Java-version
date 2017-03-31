package ned.types;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

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
    		
//			try 
//			{
//				sleep(10);
//			} 
//			catch (InterruptedException e) 
//			{
//				e.printStackTrace();
//			}
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
			
			if(doc != null && gd.queue.size()<5)
				gd.markOldClusters(doc.getId());
		}
		
		if(last != null)
			gd.markOldClusters(last.getId());

		return gd.queue.isEmpty();
	}
	
	private Document next()
	{
		//synchronized (gd.queue) 
		{
			GlobalData gd = GlobalData.getInstance();
			ConcurrentLinkedQueue<String> queue = gd.queue;
			
			if(queue.isEmpty())
				return null;
			
			String id = queue.peek();
			if (id == null)
				return null;
			
			Document doc = null;
			while(doc == null || !doc.isNearestDetermined())
				doc = gd.getDocumentFromRedis("id2document",id);
		
			id = queue.poll();
			
			return doc;
		}
	}

	public void shutdown() 
	{
		while (!gd.queue.isEmpty())
		{
		}
		stop = true;
	}
		
}
