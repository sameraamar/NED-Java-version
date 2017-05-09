package ned.tools;

import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import ned.types.Document;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisAccessHelper {

	public static final int REDIS_MAX_CONNECTIONS = 500;
	public static  boolean ready = false;

	private static JedisPool jedisPool = null;
	private static RedisSerializer<Object> docSerializer ;

	public static JedisPool createRedisConnectionPool() {
		JedisPoolConfig config = new JedisPoolConfig();
		//config.setMaxTotal(REDIS_MAX_CONNECTIONS);
		config.setMaxIdle(100);
		config.setMinIdle(50);
		//config.setMaxWaitMillis(10);
		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		config.setTestWhileIdle(false);
		//jedisPool = new JedisPool(config,"redis-10253.c1.eu-west-1-3.ec2.cloud.redislabs.com", 10253, 10000);
		return new JedisPool(config,"localhost", 6379, 18000);
	}

	
	synchronized public static void initRedisConnectionPool() {
		System.out.println("Preparing jedisPool....  ");
		if(jedisPool==null){
		
				if(jedisPool==null)
				{	
					jedisPool = createRedisConnectionPool();
					System.out.println("jedisPool is Ready "+jedisPool.getNumActive());
				}
			}
		clearRedisKeys();
		ready=true;
	}

	public static Jedis getRedisClient() {
		
		if(jedisPool==null){
			initRedisConnectionPool();
		}
		Date start=new Date();
		Jedis cn = null;
		try {
			if(jedisPool.getNumActive()<=REDIS_MAX_CONNECTIONS){
				cn= jedisPool.getResource();
				
			}else{
				System.out.println("redisConnections=="+jedisPool.getNumActive());
				System.out.println("this.parameters.REDIS_MAX_CONNECTIONS="+REDIS_MAX_CONNECTIONS);

				Thread.sleep(5);
				cn=getRedisClient();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("e="+e.getMessage());
			System.out.println("redisConnections = "+jedisPool.getNumActive());
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
		    System.out.println("redisConnections="+jedisPool.getNumActive());
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
	 
	private static void clearRedisKeys()
	{		
	}
	
	/*public static void saveStrIntMap(String key, Map<String, Integer> data)
	{
		Jedis jedis=getRedisClient();
		
		int count = 0;
		int update = 0;
		
		for (String field : data.keySet())
		{
			Integer value = data.get(field);
			if(jedis.exists(key))
				update++;
			else
				count++;
			jedis.hset(key, field, value.toString());
		}
		
		System.out.println(key + ": updated " + update + " added " + count);
		
		retunRedisClient(jedis);
	}*/
	
	public static void saveStrDocMap(String key, Map<String, Document> data)
	{
		Jedis jedis=getRedisClient();

		int count = 0;
		int update = 0;
		int skip = 0;
		
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
		
		System.out.println(key + ": skipped " + skip + ", updated " + update + " added " + count);
		
		retunRedisClient(jedis);
	}
	
	/*public static void loadStrIntMap(String key, Map<String, Integer> data)
	{
		Jedis jedis=getRedisClient();
		
		//int size = Integer.valueOf( jedis.hget(key, "-100") );
		
		for (String field : jedis.hkeys(key))
		{
			//if(field.equals("-100"))
			//	continue;
			
			String value = jedis.hget(key, field);
			data.put(field, Integer.valueOf( value ) );
		}
		retunRedisClient(jedis);
	}

	
	public static void saveIntIntMap(String key, Map<Integer, Integer> data)
	{
		Jedis jedis=getRedisClient();
		
		int count = 0;
		int update = 0;		
		for (Integer field : data.keySet())
		{
			Integer value = data.get(field);
			
			if(jedis.exists(key))
				update++;
			else
				count++;
			
			jedis.hset(key, field.toString(), value.toString());
		}
		
		System.out.println(key + ": updated " + update + " added " + count);
		retunRedisClient(jedis);
	}
	
	public static void loadIntIntMap(String key, Map<Integer, Integer> data)
	{
		Jedis jedis=getRedisClient();
		
		//int size = Integer.valueOf( jedis.hget(key, "-100") );
		
		for (String sfield : jedis.hkeys(key))
		{
			//if(sfield.equals("-100"))
			//	continue;
		
			String svalue = jedis.hget(key, sfield);
			Integer value = Integer.valueOf(svalue);
			Integer field = Integer.valueOf(sfield);
			data.put(field, value );
		}
		retunRedisClient(jedis);
	}*/

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
	
}
