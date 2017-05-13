package ned.tools;

import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

import ned.main.WorkerThread;
import ned.types.ArrayFixedSize;
import ned.types.Document;
import ned.types.DocumentClusteringThread;
import ned.types.GlobalData;
import ned.types.RedisBasedMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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
		//jedisPool = new JedisPool(config,"redis-10253.c1.eu-west-1-3.ec2.cloud.redislabs.com", 10253, 10000);
		return new JedisPool(config,"localhost", PORT, TIME_OUT);
	}

	
	synchronized public static void initRedisConnectionPool() {
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
		if (thr instanceof DocumentClusteringThread) {
			DocumentClusteringThread new_name = (DocumentClusteringThread) thr;
			pool = new_name.jedisPool;
		} //else {
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
		jedis.close();
		
	}

	public static long redisSize(String hash) 
	{
		
		Date start=new Date();
		Jedis jdis=getRedisClient();
		long len = jdis.hlen(hash);
		Date finish=new Date();
		long rediscoontime=start.getTime()-finish.getTime();
		if(rediscoontime>10)
		{
			System.out.println("rediscoontime==="+rediscoontime);
		    System.out.println("redisConnections="+jedisPool.getNumActive());
		}
		jdis.close();
		return len;
	}

	 public static void resetKey(String key)
	 {
	 	Jedis jedis=getRedisClient();
		jedis.del(key);
		retunRedisClient(jedis);
	 }
	
	public static void saveStrDocMap(String jedisKey, Map<String, Document> data)
	{
		Jedis jedis=getRedisClient();

		int count = 0;
		int update = 0;
		int skip = 0;
		
		Set<Entry<String, Document>> entries = data.entrySet();
		
		
		for (Entry<String, Document> entry : entries) {
			Document value=entry.getValue();
			if(value.isDirty)
			{
				String key = entry.getKey();
				byte[] bytes = getDocSerializer().serialize(value);

				if(jedis.exists(key))
					update++;
				else
					count++;

				jedis.hset(jedisKey.getBytes(), key.getBytes(), bytes);
				value.isDirty = false;
			}
			else
				skip ++;
			
		}
		/*
		for (String field : data.keySet())
		{
			Document value = data.get(field);
			if(value.isDirty)
			{
				byte[] bytes = getDocSerializer().serialize(value);

				if(jedis.exists(key))
					update++;
				else
					count++;

				jedis.hset(key.getBytes(), field.getBytes(), bytes);
				value.isDirty = false;
			}
			else
				skip ++;
		}
		*/
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

	
}
