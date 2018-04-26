package ned.types;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
		setOutput(outFull, outShort);
		clusteredCounter = 0;
	}
	
	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
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
		printHeader();
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
		{
			try {
				System.out.println("Clustering thread is going to sleep");
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		gd.flushClustersAll(outFull, outShort);
	}

	private void printHeader() {		
		String delimiter = GlobalData.getInstance().getParams().DELIMITER;
		
		//leadId\tentropy\tusers\tsize\ttfidf\ttext

		StringBuilder headerShort = new StringBuilder("leadId").append( delimiter );			
		headerShort.append( "entropy" ).append( delimiter );
		headerShort.append( "users" ).append( delimiter );
		headerShort.append( "size" ).append( delimiter );
		//headerShort.append( "tfidf" ).append( delimiter );
		headerShort.append( "text" );
		headerShort.append("\n");
		
		
		StringBuilder headerFull = new StringBuilder("leadId").append( delimiter );
		headerFull.append( "id" ).append( delimiter );
		headerFull.append( "created" ).append( delimiter );
		headerFull.append( "timestamp" ).append( delimiter );
		headerFull.append( "nearest" ).append( delimiter );
		headerFull.append( "distance" ).append( delimiter );
		//headerFull.append( "tfidf" ).append( delimiter );
		headerFull.append( "entropy" ).append( delimiter );
		headerFull.append( "users" ).append( delimiter );
		headerFull.append( "size" ).append( delimiter );
		headerFull.append( "age" ).append( delimiter );
		headerFull.append( "score" ).append( delimiter );
		headerFull.append( "text" );
		headerFull.append("\n");
		
		outFull.print(headerFull);
		outShort.print(headerShort);
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
	
	public void setOutput(PrintStream outFull, PrintStream outShort)
	{
		this.outFull = outFull;
		this.outShort = outShort;
	}
	
	private Document next()
	{
		GlobalData gd = GlobalData.getInstance();
		ClusteringQueueManager<String> queue = gd.getQueue();
		
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
