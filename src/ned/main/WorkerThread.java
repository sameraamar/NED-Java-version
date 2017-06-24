package ned.main;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import ned.hash.LSHForest;
import ned.tools.RedisAccessHelper;
import ned.types.Document;
import ned.types.DocumentClusteringHelper;
import ned.types.GlobalData;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class WorkerThread implements Runnable 
{
	private static Boolean locker = true;
	private static long time = 0;
	public static int counter = 0;
	public static int counter_all = 0;
	private int idx;
	
	private Document doc;
	private LSHForest forest;
	public JedisPool jedisPool;
	private int dimension;
	
    public WorkerThread(LSHForest forest, Document doc, int idx)
    {
        this.doc = doc;
        this.idx = idx;
        this.forest = forest;
        this.jedisPool = null;
    }

    private static void updateTime(long ms) 
    {
    	synchronized(locker)
    	{
    		counter++;
    		counter_all++;
    		time+=ms;
    	}
    }
    
    public static String glance()
    {
    	synchronized (locker) {
    		String msg = String.format("Total Processed: %d, AHT: %.2f, Group count: %d", counter_all, (1.0*time/counter), counter); 
			return msg; //(1.0*time/counter);
		}
    }
    

	public static void resetCounter() {
		synchronized(locker)
    	{
    		counter = 0;
    		time = 0;
    	}
	}
    
    @Override
    public void run() 
    {
    	long base = System.currentTimeMillis();
        //this.jedisPool = RedisAccessHelper.createRedisConnectionPool();

        processCommand();
        //this.jedisPool.destroy();
        this.jedisPool = null;
        updateTime(System.currentTimeMillis() - base);
    }

    private void processCommand() 
    {
		if (doc.getWords().size() == 0)
		{
	        this.doc.setNearestDetermined( true);
			return;
		}
    	Map<Integer, Double> word2idf = new Hashtable<Integer, Double>();
    	
		//JedisPool jedisPool = RedisAccessHelper.createRedisConnectionPool();
    	//GlobalData.getInstance().thread2redis.put(Thread.currentThread().getName(), jedisPool);
    	
    	List<String> set = forest.addDocument(this.doc, this.dimension, word2idf);

    	DocumentClusteringHelper.postLSHMapping(this.doc, set, word2idf);
    	this.doc.setNearestDetermined( true);

        //DocumentClusteringHelper.mapToClusterHelper(doc);
    	//GlobalData.getInstance().thread2redis.remove(Thread.currentThread().getName());
    	//jedisPool.destroy();
    }

    public Document getDocument()
	{
        return this.doc;
    }

	public void preRun() {
		dimension = GlobalData.getInstance().addDocument(doc, idx);
	}

}