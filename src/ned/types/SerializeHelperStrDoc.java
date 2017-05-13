package ned.types;

import java.util.Map;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrDoc extends SerializeHelper<String, Document>{

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
	protected void saveMap(String jedisKey, Map<String, Document> data) 
	{
		RedisAccessHelper.saveStrDocMap(jedisKey, data);
	}

	@Override
	Document parse(String svalue) {
		return null;
	}

}
