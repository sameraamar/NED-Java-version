package ned.types;

import java.io.Serializable;
import java.util.Map;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrMap extends SerializeHelper<String, Map<Integer, Integer>>{

	@Override
	public void set(Jedis jedis, String jedisKey, String key, Map<Integer, Integer> value) 
	{
		byte[] svalue = RedisAccessHelper.getDocSerializer().serialize(value);
		jedis.hset(jedisKey.getBytes(), key.getBytes(), svalue);
		//value.isDirty = false;
	}

	@Override
	public Map<Integer, Integer> get(Jedis jedis, String jedisKey, String key) 
	{
		byte[] svalue = jedis.hget(jedisKey.getBytes(), key.getBytes());
		if(svalue == null || svalue.length==0)
			return null;
		
		Map<Integer, Integer> doc = (Map<Integer, Integer>) RedisAccessHelper.getDocSerializer().deserialize(svalue);
		//doc.isDirty = false;
		return doc;
	}

	@Override
	protected void saveMap(String jedisKey, Map<String, Map<Integer, Integer>> data) 
	{
		RedisAccessHelper.saveStrSerializableMap(jedisKey, data);
	}

	@Override
	Map<Integer, Integer> parse(String svalue) {
		return null;
	}

}
