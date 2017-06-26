package ned.types;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperIntInt implements SerializeHelper<Integer, Integer>{

	@Override
	public Integer get(Jedis jedis, String jedisKey, Integer key)
	{
		String svalue = jedis.hget(jedisKey, key.toString());
		if(svalue == null)
			return null;
		
		return Integer.parseInt( svalue );
	}

	@Override
	public void set(Jedis jedis, String jedisKey, Integer key, Integer value)
	{
		jedis.hset(jedisKey, key.toString(), value.toString());
	}
	
	@Override
	public void saveMap(String jedisKey, Map<Integer, Integer> data) {
		Jedis jedis= RedisAccessHelper.getRedisClient();
		
		int count = 0;
		int update = 0;		
		Set<Entry<Integer, Integer>> es = data.entrySet();
		
		for (Entry<Integer, Integer> entry : es) {
			Integer skey = entry.getKey();
			if(jedis.hexists(jedisKey, skey.toString()))
				update++;
			else
				count++;
			
			jedis.hset(jedisKey, skey.toString(), entry.getValue().toString());
		}
		
		System.out.println(jedisKey + ": updated " + update + " added " + count);
		
		RedisAccessHelper.retunRedisClient(jedis);
	}
	
}
