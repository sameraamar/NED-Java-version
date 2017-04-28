package ned.tools;

import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;

import ned.types.Document;
import ned.types.LRUCache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisHelper {

	public static final int REDIS_MAX_CONNECTIONS = 500;
	public static final String ID2DOCUMENT = "id2document";
	public static final String WORD2INDEX = "word2index";
	public static final String WORD2IDF = "word2idf";
	public static final int lru_cache_size = 50000;
	public static  boolean ready = false;


	
	private  static  LRUCache<String, Document> id2DocumentCache;//=new LRUCache<String, Document>(2000);
	private  static  LRUCache<String, String> word2IndexCache;//=new LRUCache<String, String>(2000);
	private  static  LRUCache<Integer, Double> word2idfCache=new LRUCache<Integer, Double>(2000);;
	private static JedisPool jedisPool = null;
	private static RedisSerializer<Object> docSerializer ;

	
	synchronized public static void initRedisConnectionPool() {
		System.out.println("Preparing jedisPool....  ");		
		if(jedisPool==null)
		{	
			JedisPoolConfig config = new JedisPoolConfig();
			//config.setMaxTotal(REDIS_MAX_CONNECTIONS);
			config.setMaxIdle(100);
			config.setMinIdle(50);
			//config.setMaxWaitMillis(10);
			config.setTestOnBorrow(false);
			config.setTestOnReturn(false);
			config.setTestWhileIdle(false);
			//jedisPool = new JedisPool(config,"redis-10253.c1.eu-west-1-3.ec2.cloud.redislabs.com", 10253, 10000);
			jedisPool = new JedisPool(config,"localhost", 6379, 18000);

			System.out.println("jedisPool is Ready "+jedisPool.getNumActive());
		}
			
		
		if(id2DocumentCache==null){
			id2DocumentCache=new LRUCache<String, Document>(lru_cache_size);
		}
		if(word2IndexCache==null){
			word2IndexCache=new LRUCache<String, String>(lru_cache_size);
		}
		if(word2idfCache==null){
			word2idfCache=new LRUCache<Integer, Double>(lru_cache_size);
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

	public static Document getDocumentFromRedis(String hash,String key) {
		Document doc=null;
		if(key == null)
			return null;
		
		if(id2DocumentCache != null)
		{
			doc= id2DocumentCache.get(key);
			if(doc!=null || id2DocumentCache.size()<lru_cache_size) {
				return doc;
			}
		}
	//	System.out.println("Cache Miss :("+key);
		Jedis jedis=getRedisClient();
		
		byte[] kbytes = key.getBytes();
		byte[] hbytes = hash.getBytes();
		byte[] retobject=jedis.hget(hbytes,kbytes);
		if(retobject!=null){
			doc=(Document) getDocSerializer().deserialize(retobject);
		}
		//jdis.close();
		retunRedisClient(jedis);
		return doc;
	}
	
	public Hashtable  <String,Document> getMultiDocumentFromRedis(String hash,String keys) {

		if(keys == null)
			return null;
		Hashtable  <String,Document> result=new Hashtable<String,Document>()  ;
		
		Jedis jedis=getRedisClient();
		
		String keysArray[] = keys.split(",");
		byte[][]  kyesBytes = new byte[keysArray.length][] ;
		int index=0;
		for (String string : keysArray) {
				if(string.isEmpty()) continue;
				kyesBytes[index]=string.getBytes();
				index++;
		}
		
		List<byte[]> hashValues;
		try {
			hashValues = jedis.hmget(hash.getBytes(),kyesBytes);
			if(hashValues!=null){
				hashValues.forEach(doc->{
					if(doc!=null){
						Document tDoc=(Document) getDocSerializer().deserialize(doc);
						result.put(tDoc.getId(),tDoc );
					}
				});
				
			}
			retunRedisClient(jedis);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public void delMultiDocumentFromRedis(String hash,String keys) {

		if(keys == null)
			return ;
		try {
			Runnable runnable = () -> {
				Jedis jedis=getRedisClient();
				
				String keysArray[] = keys.split(",");
				byte[][]  kyesBytes = new byte[keysArray.length][] ;
				int index=0;
				for (String string : keysArray) {
						if(string.isEmpty()) continue;
						kyesBytes[index]=string.getBytes();
						index++;
				}
				jedis.hdel(hash.getBytes(),kyesBytes);
			
				retunRedisClient(jedis);
			};
			ExecutionHelper.asyncRun (runnable);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ;
	}


	public static void setDocumentFromRedis(String hash,String key,Document doc){
		if(doc==null){
			return;
		}
		
		if(id2DocumentCache != null)
		{
			id2DocumentCache.put(key, doc);
			//System.out.println("Add to Cache "+key);
		}
		Runnable runnable = () -> {
			Date start=new Date();
			Jedis jdis=getRedisClient();
			
			
			byte[] sobject=getDocSerializer().serialize(doc);
			
			jdis.hset(hash.getBytes(),key.getBytes(),sobject);
			jdis.close();
			Date stop=new Date();
			long rediscoontime=start.getTime()-stop.getTime();
			if(rediscoontime>1)
			{
				System.out.println("setDocumentFromRedis Time ==="+rediscoontime);
			    System.out.println("redisConnections="+jedisPool.getNumActive());
			}
		};
		ExecutionHelper.asyncRun (runnable);	
	}
	 public static RedisSerializer<Object> getDocSerializer() {
		if(docSerializer==null){
				if(docSerializer==null)
				docSerializer= new JdkSerializationRedisSerializer();
		}
		return docSerializer;
	}
	private static void clearRedisKeys()
	{
		
		
		while(jedisPool==null){
			
		}
		Jedis jedis=getRedisClient();
		jedis.del(ID2DOCUMENT);
		jedis.del(WORD2INDEX);
		
		retunRedisClient(jedis);
	}
	
	public static double getIDF(int k)
	{
		
		double res=-1;
		if(word2idfCache!=null)
		//res=word2idfCache.get(k);
		if(res>-1){
			return res;
		}else{
			Jedis jedis=getRedisClient();
			String resStr=jedis.get(String.valueOf(k));
			if(resStr!=null)	{
				return Double.valueOf(resStr);

			}
		}
		return -1;
	}
	
	public static double getIDFOrDefault(int k)
	{
		return getIDF(k);
		//return word2idf.getOrDefault(k, -1.0);
	}
	public static void setIDF(int k,double idf)
	{
		word2idfCache.put(k, idf);
	Runnable runnable = () -> {
		Jedis jedis=getRedisClient();
		String key=String.valueOf(k);
		String value=String.valueOf(idf);
		jedis.hset(WORD2IDF,key,value);
		retunRedisClient(jedis);
	};
	
	ExecutionHelper.asyncRun (runnable);
		
		//return word2idf.getOrDefault(k, -1.0);
	}


}
