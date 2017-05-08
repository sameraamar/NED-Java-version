package ned.types;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrDoc implements SerializeHelper<Document>{

	@Override
	public void set(Jedis jedis, String jedisKey, Object key, Document value) 
	{
		byte[] svalue = RedisAccessHelper.getDocSerializer().serialize(value);
		jedis.set(jedisKey.getBytes(), key.toString().getBytes(), svalue);
	}

	@Override
	public Document get(Jedis jedis, String jedisKey, Object key) 
	{
		byte[] svalue = jedis.hget(jedisKey.getBytes(), key.toString().getBytes());
		Document doc = (Document) RedisAccessHelper.getDocSerializer().deserialize(svalue);
		return doc;
	}

}
