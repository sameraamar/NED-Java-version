package ned.types;

import java.io.PrintStream;

import ned.tools.ClusteringQueueManager;
import ned.tools.RedisAccessHelper;
import redis.clients.jedis.JedisPool;

public class DocumentClusteringThread extends Thread {
	private boolean stop = false;
	private PrintStream outFull;
	private PrintStream outShort;
	public int clusteredCounter;
	public JedisPool jedisPool;

	public DocumentClusteringThread(PrintStream outFull, PrintStream outShort)
	{
		this.outFull = outFull;
		this.outShort = outShort;
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
		GlobalData gd = GlobalData.getInstance();

		StringBuilder headerShort = new StringBuilder("leadId").append( gd.getParams().DELIMITER );			
		headerShort.append( "entropy" ).append( gd.getParams().DELIMITER );
		headerShort.append( "#users" ).append( gd.getParams().DELIMITER );
		headerShort.append( "size" ).append( gd.getParams().DELIMITER );
		headerShort.append( "text" ).append( gd.getParams().DELIMITER );
		headerShort.append("\n");
		
		//leadId\tid\tuser\t# users\tcreated\ttimestamp\tnearest\tdistance\tentropy\tsize\tage\ttext\n
		StringBuilder headerFull = new StringBuilder("leadId").append( gd.getParams().DELIMITER );
		headerFull.append( "id" ).append( gd.getParams().DELIMITER );
		headerFull.append( "created" ).append( gd.getParams().DELIMITER );
		headerFull.append( "timestamp" ).append( gd.getParams().DELIMITER );
		headerFull.append( "nearest" ).append( gd.getParams().DELIMITER );
		headerFull.append( "distance" ).append( gd.getParams().DELIMITER );
		headerFull.append( "entropy" ).append( gd.getParams().DELIMITER );
		headerFull.append( "#users" ).append( gd.getParams().DELIMITER );
		headerFull.append( "size" ).append( gd.getParams().DELIMITER );
		headerFull.append( "age" ).append( gd.getParams().DELIMITER );
		headerFull.append( "score" ).append( gd.getParams().DELIMITER );
		headerFull.append( "topic" ).append( gd.getParams().DELIMITER );
		headerFull.append( "text" ).append( gd.getParams().DELIMITER );
		headerFull.append("\n");
		
		outFull.print(headerFull);
		outShort.print(headerShort);
		
		while(!stop) 
		{
			mapToCluster();
			
    		Session.getInstance().message(Session.DEBUG, "Reader", "doing some cleanup...");
    		gd.flushClusters(outFull, outShort);
    		
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

		gd.flushClustersAll(outFull, outShort);
	}

	private boolean mapToCluster()
	{
		GlobalData gd = GlobalData.getInstance();

		Document doc = next();
		Document last = doc;
		while(doc!=null)
		{
			if(!gd.getParams().scan_mode_only)
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
		while (!GlobalData.getInstance().getQueue().isEmpty())
		{
		}
		stop = true;
	}
		
}
