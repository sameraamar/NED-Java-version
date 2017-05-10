package ned.types;

import redis.clients.jedis.Jedis;

public class SerializeHelperIntDble implements SerializeHelper<Integer, Double>{

	@Override
	public void set(Jedis jedis, String jedisKey, Integer key, Double value) 
	{
		jedis.hset(jedisKey, key.toString(), value.toString());
	}

	@Override
	public Double get(Jedis jedis, String jedisKey, Integer key) 
	{
		String svalue = jedis.hget(jedisKey, key.toString());
		if(svalue == null)
			return null;
		
		return Double.valueOf(svalue);
	}

	@Override
	public void saveMap(String jedisKey, RedisBasedMap<Integer, Double> redisBasedMap) {
		//RedisAccessHelper.saveStrIntMap(jedisKey, redisBasedMap);
	}
}
