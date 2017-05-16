package ned.types;

import java.io.Serializable;
import java.util.Map;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrSerializable extends SerializeHelper<String, Serializable>{

	@Override
	public void set(Jedis jedis, String jedisKey, String key, Serializable value) 
	{
		byte[] svalue = RedisAccessHelper.getDocSerializer().serialize(value);
		jedis.hset(jedisKey.getBytes(), key.getBytes(), svalue);
		//value.isDirty = false;
	}

	@Override
	public Serializable get(Jedis jedis, String jedisKey, String key) 
	{
		byte[] svalue = jedis.hget(jedisKey.getBytes(), key.getBytes());
		if(svalue == null || svalue.length==0)
			return null;
		
		Serializable doc = (Serializable) RedisAccessHelper.getDocSerializer().deserialize(svalue);
		//doc.isDirty = false;
		return doc;
	}

	@Override
	protected void saveMap(String jedisKey, Map<String, Serializable> data) 
	{
		//RedisAccessHelper.saveStrSerializableMap(jedisKey, data);
	}

	@Override
	Serializable parse(String svalue) {
		return null;
	}

}
