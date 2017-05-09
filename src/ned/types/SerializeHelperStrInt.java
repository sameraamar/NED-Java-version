package ned.types;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrInt implements SerializeHelper<String, Integer>{

	@Override
	public void set(Jedis jedis, String jedisKey, String key, Integer value) 
	{
		jedis.hset(jedisKey, key, value.toString());
	}

	@Override
	public Integer get(Jedis jedis, String jedisKey, String key) 
	{
		String svalue = jedis.hget(jedisKey, key);
		if(svalue == null)
			return null;
		
		return Integer.valueOf(svalue);
	}

	@Override
	public void saveMap(String jedisKey, RedisBasedMap<String, Integer> redisBasedMap) {
		RedisAccessHelper.saveStrIntMap(jedisKey, redisBasedMap);
	}
}
