package ned.types;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperIntInt implements SerializeHelper<Integer, Integer>{

	@Override
	public void set(Jedis jedis, String jedisKey, Integer key, Integer value) 
	{
		jedis.hset(jedisKey, key.toString(), value.toString());
	}

	@Override
	public Integer get(Jedis jedis, String jedisKey, Integer key) 
	{
		String svalue = jedis.hget(jedisKey, key.toString());
		if(svalue == null)
			return null;
		
		return Integer.valueOf(svalue);
	}

	@Override
	public void saveMap(String jedisKey, RedisBasedMap<Integer, Integer> redisBasedMap) {
		RedisAccessHelper.saveIntIntMap(jedisKey, redisBasedMap);
	}
}
