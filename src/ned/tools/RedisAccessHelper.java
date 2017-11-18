package ned.tools;

import java.net.SocketTimeoutException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

import ned.main.WorkerThread;
import ned.types.ArrayFixedSize;
import ned.types.DirtyBit;
import ned.types.Document;
import ned.types.DocumentCluster;
import ned.types.DocumentClusteringThread;
import ned.types.DocumentWordCounts;
import ned.types.GlobalData;
import ned.types.RedisBasedMap;
import ned.types.Session;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisAccessHelper {

	private static final int TIME_OUT = 18000;
	private static final int PORT = 6379;
	public static final int REDIS_MAX_CONNECTIONS = 500;
	public static  boolean ready = false;

	private static JedisPool jedisPool = null;
	private static RedisSerializer<Object> docSerializer ;
	
	//private static ArrayFixedSize<JedisPool> pools;

	public static JedisPool createRedisConnectionPool() {
		JedisPoolConfig config = new JedisPoolConfig();
		//config.setMaxTotal(REDIS_MAX_CONNECTIONS);
		config.setMaxIdle(100);
		config.setMinIdle(50);
		//config.setMaxWaitMillis(10);
		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		config.setTestWhileIdle(false);
		config.setMaxTotal(REDIS_MAX_CONNECTIONS);
		//String redisHost = "ec2-54-245-53-209.us-west-2.compute.amazonaws.com";	
		String redisHost = "localhost";
		if(Session.getMachineName().indexOf("samer") >= 0)
			redisHost = "localhost";
		
		return new JedisPool(config, redisHost, PORT, TIME_OUT);
	}

	synchronized public static void initRedisConnectionPool() {
		initRedisConnectionPool(false);
	}
	
	synchronized public static void initRedisConnectionPool(boolean force) {
		if(force && jedisPool != null)
		{
			jedisPool.destroy();
			jedisPool = null;
		}
		
		if(jedisPool != null)
			return;
		
		System.out.println("Preparing jedisPool....  ");

		jedisPool = createRedisConnectionPool();

		System.out.println("jedisPool is Ready. ");

		ready=true;
	}

	public static Jedis getRedisClient() {
		
		Runnable thr = Thread.currentThread();
		JedisPool pool = null;
		
		if (thr.getClass().getName().equals( DocumentClusteringThread.class.getName() ) ) {
			DocumentClusteringThread new_name = (DocumentClusteringThread) thr;
			pool = new_name.jedisPool;
		} //else if (thr.getClass().getName().equals( MyMonitorThread.class.getName() ) ) {
		//	MyMonitorThread new_name = (MyMonitorThread) thr;
		//	pool = new_name.jedisPool;
		//} else {
		//	pool = GlobalData.getInstance().thread2redis.get(Thread.currentThread().getName());
		//}
		
		if(pool==null){
			initRedisConnectionPool();
			pool = jedisPool;
		}
		Date start=new Date();
		Jedis cn = null;
		try {
			if(pool.getNumActive()<=REDIS_MAX_CONNECTIONS){
				cn= pool.getResource();
				
			}else{
				System.out.println("redisConnections=="+pool.getNumActive());
				System.out.println("this.parameters.REDIS_MAX_CONNECTIONS="+REDIS_MAX_CONNECTIONS);

				Thread.sleep(5);
				cn=getRedisClient();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("e="+e.getMessage());
			System.out.println("redisConnections = "+pool.getNumActive());
			try {
				Thread.sleep(100);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if(cn!=null)
				cn.close();
			
			cn=getRedisClient();
		}
		finally {
		}
		Date finish=new Date();
		long rediscoontime=start.getTime()-finish.getTime();
		if(rediscoontime>10)
		{
			System.out.println("rediscoontime==="+rediscoontime);
		    System.out.println("redisConnections="+pool.getNumActive());
		 }
		return cn;
	}
	
	public static void retunRedisClient(Jedis jedis) {
		if(jedis!=null && jedis.isConnected())
		//	jedis.disconnect();
		jedis.close();
		
	}

	public static long redisSize(String hash) 
	{	
		Jedis jedis=null;		
		try{
			jedis=getRedisClient();
			long len = jedis.hlen(hash);
			return len;
		}
		catch(JedisException je){
			je.printStackTrace();
		}
		finally{
			retunRedisClient(jedis);
		}		
		return 0;		
	}

	public static void resetKey(String key)
	{
		Jedis jedis = null;
		int tries = 0;
		boolean try_again = true;
		
		while(try_again && tries < 5)
		{
			jedis=getRedisClient();
			
			try {
				jedis.del(key);
		 		try_again = false;
		 	}	 
		 	catch (JedisConnectionException se) 
		 	{
		 		System.out.println("Failed to reset key " + key + ". trying again (" + tries + ")");
		 		se.printStackTrace();
		 		tries ++;
		 	}
			
			if(try_again)
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			if(jedis != null)
	 			retunRedisClient(jedis);
	 	}
	 }
	
	 public static void saveStrSerializableMap(String jedisKey, Map<String, Map<Integer, Integer>> data)
		{
			Jedis jedis=getRedisClient();

			int count = 0;
			int update = 0;
			int skip = 0;
			
			Set<Entry<String, Map<Integer, Integer>>> entries = data.entrySet();
			
			
			for (Entry<String, Map<Integer, Integer>> entry : entries) {
				Map<Integer, Integer> value=entry.getValue();
				//if(value.isDirty)
				{
					String key = entry.getKey();
					byte[] bytes = getDocSerializer().serialize(value);

					if(jedis.hexists(jedisKey, key))
						update++;
					else
						count++;

					jedis.hset(jedisKey.getBytes(), key.getBytes(), bytes);
					//value.isDirty = false;
				}
				//else
				//	skip ++;
				
			}
			System.out.println(jedisKey + ": skipped " + skip + ", updated " + update + " added " + count);
			
			retunRedisClient(jedis);
		}
	 
		public static void saveStrMap(String jedisKey, Map<String, DocumentCluster> data)
		{
			Jedis jedis=getRedisClient();

			int count = 0;
			int update = 0;
			int skip = 0;
			
			Set<Entry<String, DocumentCluster>> entries = data.entrySet();
			
			
			for (Entry<String, DocumentCluster> entry : entries) {
				DocumentCluster value=entry.getValue();
				if(value.isDirty())
				{
					String key = entry.getKey();
					byte[] bytes = getDocSerializer().serialize(value);

					if(jedis.hexists(jedisKey, key))
						update++;
					else
						count++;

					jedis.hset(jedisKey.getBytes(), key.getBytes(), bytes);
					value.dirtyOff();;
				}
				else
					skip ++;
				
			}

			System.out.println(jedisKey + ": skipped " + skip + ", updated " + update + " added " + count);
			
			retunRedisClient(jedis);
		}
	
	public static void loadStrDocMap(String key, Hashtable<String, Document> data) {
		Jedis jedis=getRedisClient();
		
		//int size = Integer.valueOf( jedis.hget(key, "-100") );
		
		for (String sfield : jedis.hkeys(key))
		{
			//if(sfield.equals("-100"))
			//	continue;
			
			byte[] svalue = jedis.hget(key.getBytes(), sfield.getBytes());
			
			Document value=(Document) getDocSerializer().deserialize(svalue);

			data.put(sfield, value );
		}
		retunRedisClient(jedis);
	}

	public static RedisSerializer<Object> getDocSerializer() {
		if(docSerializer==null){
				if(docSerializer==null)
				docSerializer= new JdkSerializationRedisSerializer();
		}
		return docSerializer;
	}


	public static int getNumActive() {
		return jedisPool.getNumActive();
	}

	public static void saveStrDocWC(String jedisKey, Map<String, DocumentWordCounts> data) 
	{
		Jedis jedis=getRedisClient();

		int count = 0;
		int update = 0;
		int skip = 0;
		
		Set<Entry<String, DocumentWordCounts>> entries = data.entrySet();
		
		
		for (Entry<String, DocumentWordCounts> entry : entries) {
			DocumentWordCounts value=entry.getValue();
			if(value.isDirty())
			{
				String key = entry.getKey();
				byte[] bytes = getDocSerializer().serialize(value);

				if(jedis.hexists(jedisKey, key))
					update++;
				else
					count++;

				jedis.hset(jedisKey.getBytes(), key.getBytes(), bytes);
				value.dirtyOff();;
			}
			else
				skip ++;
			
		}

		System.out.println(jedisKey + ": skipped " + skip + ", updated " + update + " added " + count);
		
		retunRedisClient(jedis);
	}
	
}
