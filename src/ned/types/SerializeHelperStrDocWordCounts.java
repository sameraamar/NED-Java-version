package ned.types;

import java.util.Map;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrDocWordCounts extends SerializeHelper<String, DocumentWordCounts>{

	@Override
	public void set(Jedis jedis, String jedisKey, String key, DocumentWordCounts value) 
	{
		byte[] svalue = RedisAccessHelper.getDocSerializer().serialize(value);
		jedis.hset(jedisKey.getBytes(), key.getBytes(), svalue);
		value.dirtyOff();
	}

	@Override
	public DocumentWordCounts get(Jedis jedis, String jedisKey, String key) 
	{
		byte[] svalue = jedis.hget(jedisKey.getBytes(), key.getBytes());
		if(svalue == null || svalue.length==0)
			return null;
		
		DocumentWordCounts doc = (DocumentWordCounts) RedisAccessHelper.getDocSerializer().deserialize(svalue);
		doc.dirtyOff();;
		return doc;
	}

	@Override
	protected void saveMap(String jedisKey, Map<String, DocumentWordCounts> data) 
	{
		RedisAccessHelper.saveStrDocWC(jedisKey, data);
	}

	@Override
	DocumentWordCounts parse(String svalue) {
		return null;
	}

}
