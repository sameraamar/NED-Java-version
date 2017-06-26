package ned.types;


import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;


public class SerializeHelperAdapterSimpleType<T> implements SerializeHelper<String, T>{
	private static final String INTEGER_NAME = Integer.class.getName();
	private static final String DOUBLE_NAME = Double.class.getName();
	private static final String STRING_NAME = String.class.getName();
	String clazz;
	
	public SerializeHelperAdapterSimpleType(Class<T> clazz) {
		this.clazz = clazz.getName();
	}
	
	@Override
	public void set(Jedis jedis, String jedisKey, String key, T value)
	{
		jedis.hset(jedisKey, key, value.toString());
	}
	
	@Override
	public T get(Jedis jedis, String jedisKey, String key) 
	{
		String svalue = jedis.hget(jedisKey, key);
		if(svalue == null)
			return null;
		
		return parse(svalue);
	}
	
	T parse(String svalue)
	{
		Object res = svalue;
		if(clazz.equals(INTEGER_NAME))
			res = Integer.parseInt(svalue);
		else if(clazz.equals(DOUBLE_NAME))
			res = Double.parseDouble(svalue);
		//else if(clazz.equals(STRING_NAME))
		//	res = svalue;
		
		return (T)res;
	}
	
	@Override
	public void saveMap(String jedisKey, Map<String, T> data) {
		Jedis jedis= RedisAccessHelper.getRedisClient();
		
		int count = 0;
		int update = 0;		
		Set<Entry<String, T>> es = data.entrySet();
		
		for (Entry<String, T> entry : es) {
			String skey = entry.getKey();
			if(jedis.hexists(jedisKey, skey))
				update++;
			else
				count++;
			
			jedis.hset(jedisKey, skey, entry.getValue().toString());
		}
		
		System.out.println(jedisKey + ": updated " + update + " added " + count);
		
		RedisAccessHelper.retunRedisClient(jedis);
	}
	
}
