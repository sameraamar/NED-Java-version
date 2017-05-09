package ned.types;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrDoc implements SerializeHelper<String, Document>{

	@Override
	public void set(Jedis jedis, String jedisKey, String key, Document value) 
	{
		byte[] svalue = RedisAccessHelper.getDocSerializer().serialize(value);
		jedis.hset(jedisKey.getBytes(), key.getBytes(), svalue);
		value.isDirty = false;
	}

	@Override
	public Document get(Jedis jedis, String jedisKey, String key) 
	{
		byte[] svalue = jedis.hget(jedisKey.getBytes(), key.getBytes());
		if(svalue == null || svalue.length==0)
			return null;
		
		Document doc = (Document) RedisAccessHelper.getDocSerializer().deserialize(svalue);
		doc.isDirty = false;
		return doc;
	}

	@Override
	public void saveMap(String jedisKey, RedisBasedMap<String, Document> redisBasedMap) {
		RedisAccessHelper.saveStrDocMap(jedisKey, redisBasedMap);
	}

}
