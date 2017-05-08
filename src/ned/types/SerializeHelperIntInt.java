package ned.types;

import redis.clients.jedis.Jedis;

public class SerializeHelperIntInt implements SerializeHelper<Integer>{

	@Override
	public void set(Jedis jedis, String jedisKey, Object key, Integer value) 
	{
		jedis.set(jedisKey, key.toString(), value.toString());
	}

	@Override
	public Integer get(Jedis jedis, String jedisKey, Object key) 
	{
		String svalue = jedis.hget(jedisKey, key.toString());
		return Integer.valueOf(svalue);
	}

}
