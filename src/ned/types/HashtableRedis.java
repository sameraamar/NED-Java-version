package ned.types;

import java.util.Hashtable;
import java.util.Set;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class HashtableRedis<T>
{
	Hashtable<String, T> cache;
	Class<T> clazz;
	JedisPool pool;
	GsonBuilder gson;
	Gson g;
	private String title;
		
	public HashtableRedis(String title, Class<T> clazz) 
	{
		this.cache = new Hashtable<String, T> ();

		this.title = title;
		pool = new JedisPool("localhost");
		this.clazz = clazz;
		gson = new GsonBuilder();
	    g = gson.setPrettyPrinting().create();
		
	   	//Jedis jedis = pool.getResource();
	    //jedis.expire(title, 432000) ;  //expire in 1 week :)
	    //pool.returnResource(jedis);
	}
	
	public void reset()
	{
	    Jedis jedis = pool.getResource();
	    jedis.expire(title, 0);
	    jedis.close();
	}
	
	public int cacheSize()
	{
		return cache.size();
	}

	public void updateAndFlush(String key, T value) 
	{
		remove(key);
		
		update(key, value);
		cache.remove(key);
		
	}
	
	public void newEntry(String key, T value)
	{
		cache.put(key, value);
		//update(key, value);
	}
	
	private void update(String key, T value)
	{
	    Jedis jedis = pool.getResource();

		if(value instanceof String)
			jedis.hset(title, key, (String)value);
		else
		{
			String v = g.toJson(value);
			jedis.hset(title, key, v.toString());
		}
		jedis.close();
	}
	
	public void remove(String key) 
	{
	    Jedis jedis = pool.getResource();
	    jedis.hdel(title, key);
	    jedis.close();
	}
	
	public T get(String key)
	{
		T value = cache.get(key);
		
		if (value != null)
			return value;
		
	    Jedis jedis = pool.getResource();
		String json = jedis.hget(title, key);
		//System.out.println("json :" + json.length());

		value = (T)g.fromJson(json, clazz);
		
		jedis.close();
		return value;
	}

	public long size()
	{
	    Jedis jedis = pool.getResource();
	    long res = jedis.hlen(title);
	    jedis.close();
		return res + cache.size();
	}
	
	public Set<String> keys()
	{
		Jedis jedis = pool.getResource();
	    Set<String> keys = jedis.hkeys(title);
	    jedis.close();
	    
	    return keys;
	}
	
	public static void main(String[] args) 
	{
		HashtableRedis<Document> hmap = new HashtableRedis<Document>("id2document", Document.class);
		
		Document doc = new Document("86383662215598080", "this is a test", 12121212);
		hmap.newEntry(doc.getId(), doc);
		hmap.updateAndFlush(doc.getId(), doc);
		
		System.out.println("keys");
		System.out.println( hmap.get("86383662215598080") );		
		System.out.println( hmap.get("86385650332483584") );
		
	}

	public T getOrDefault(String key, T def) 
	{
		T tmp = get(key);
		if (tmp == null)
			tmp = def;
		
		return tmp;
	}

	
	
}


