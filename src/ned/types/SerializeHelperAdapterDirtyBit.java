package ned.types;


import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;


public class SerializeHelperAdapterDirtyBit<T> implements SerializeHelper<String, T>{

	@Override
	public void set(Jedis jedis, String jedisKey, String key, T value) 
	{
		byte[] svalue = RedisAccessHelper.getDocSerializer().serialize(value);
		jedis.hset(jedisKey.getBytes(), key.getBytes(), svalue);
		
		DirtyBit d = (DirtyBit)value;
		d.dirtyOff();
	}

	@Override
	public T get(Jedis jedis, String jedisKey, String key) 
	{
		byte[] svalue = jedis.hget(jedisKey.getBytes(), key.getBytes());
		if(svalue == null || svalue.length==0)
			return null;
		
		T doc = (T) RedisAccessHelper.getDocSerializer().deserialize(svalue);
		DirtyBit d = (DirtyBit)doc;
		d.dirtyOff();;
		return doc;
	}

	@Override
	public void saveMap(String jedisKey, Map<String, T> data) {
		Jedis jedis= RedisAccessHelper.getRedisClient();

		int count = 0;
		int update = 0;
		int skip = 0;
		
		Set<Entry<String, T>> entries = data.entrySet();
		
		
		for (Entry<String, T> entry : entries) {
			T value=entry.getValue();
			DirtyBit d = (DirtyBit)value;
			if(d.isDirty())
			{
				String key = entry.getKey();
				byte[] bytes = RedisAccessHelper.getDocSerializer().serialize(value);

				if(jedis.hexists(jedisKey, key))
					update++;
				else
					count++;

				jedis.hset(jedisKey.getBytes(), key.getBytes(), bytes);
				d.dirtyOff();;
			}
			else
				skip ++;
			
		}

		System.out.println(jedisKey + ": skipped " + skip + ", updated " + update + " added " + count);
		
		RedisAccessHelper.retunRedisClient(jedis);
	}
	
	
}
