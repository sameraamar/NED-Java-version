package ned.types;

import java.io.PrintStream;

import ned.tools.ClusteringQueueManager;
import ned.tools.RedisAccessHelper;
import redis.clients.jedis.JedisPool;

public class DocumentClusteringThread extends Thread {
	private boolean stop = false;
	private GlobalData gd;
	private PrintStream out;
	public int clusteredCounter;
	public JedisPool jedisPool;
	private String dil=" ||| ";
	public DocumentClusteringThread(PrintStream out)
	{
		this.out = out;
		gd = GlobalData.getInstance();
		clusteredCounter = 0;
	}
	
	@Override
	public void run() 
	{
        this.jedisPool = RedisAccessHelper.createRedisConnectionPool();
        try {
			doRun();
			
		} finally {
			JedisPool temp = this.jedisPool;
			this.jedisPool = null;
			temp.destroy();
		}
		
	}
	
	private void doRun() {
		
		String header = "leadId";
		if(! gd.getParams().is_prod_mode)
			header += dil+"id";

		header += dil+"entropy"+dil+"#users"+dil+"size"+dil+"text\n";
		out.print(header);
		
		while(!stop) 
		{
			mapToCluster();
			
    		Session.getInstance().message(Session.DEBUG, "Reader", "doing some cleanup...");
    		gd.flushClusters(out);
    		
			try 
			{
				sleep(4);
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
			if(!GlobalData.getInstance().getParams().scan_mode_only)
			{
				DocumentClusteringHelper.mapToClusterHelper(doc);
				clusteredCounter++;
			}
			
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
		
		Document doc = GlobalData.getInstance().id2doc.get(id); //RedisHelper.getDocumentFromRedis(GlobalData.ID2DOCUMENT,id);
		if (doc==null)
			return null;
		

		if(doc.isNearestDetermined() || GlobalData.getInstance().getParams().scan_mode_only)
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
