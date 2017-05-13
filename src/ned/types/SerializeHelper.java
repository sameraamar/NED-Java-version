package ned.types;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

abstract public class SerializeHelper<K, V> {
	public void set(Jedis jedis, String jedisKey, K key, V value)
	{
		jedis.hset(jedisKey, key.toString(), value.toString());
	}
	
	public V get(Jedis jedis, String jedisKey, K key)
	{
		String svalue = jedis.hget(jedisKey, key.toString());
		if(svalue == null)
			return null;
		
		return parse(svalue);
	}
	
	abstract V parse(String svalue);
	
	protected void saveMap(String jedisKey, Map<K, V> data)
	{
		Jedis jedis= RedisAccessHelper.getRedisClient();
		
		int count = 0;
		int update = 0;		
		Set<Entry<K, V>> es = data.entrySet();
		
		for (Entry<K, V> entry : es) {
			if(jedis.exists(entry.getKey().toString()))
				update++;
			else
				count++;
			
			jedis.hset(jedisKey, entry.getKey().toString(), entry.getValue().toString().toString());
		}
		for (K field : data.keySet())
		{
			V value = data.get(field);
			
			String key = field.toString();
			if(jedis.exists(key))
				update++;
			else
				count++;
			
			jedis.hset(jedisKey, key, value.toString());
		}
		
		System.out.println(jedisKey + ": updated " + update + " added " + count);
		
		RedisAccessHelper.retunRedisClient(jedis);
	}
	
	/*
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

}
