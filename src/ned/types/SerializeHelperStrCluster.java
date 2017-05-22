package ned.types;

import java.util.Map;

import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class SerializeHelperStrCluster extends SerializeHelper<String, DocumentCluster>{

	@Override
	public void set(Jedis jedis, String jedisKey, String key, DocumentCluster value) 
	{
		byte[] svalue = RedisAccessHelper.getDocSerializer().serialize(value);
		jedis.hset(jedisKey.getBytes(), key.getBytes(), svalue);
		value.dirtyOff();
	}

	@Override
	public DocumentCluster get(Jedis jedis, String jedisKey, String key) 
	{
		byte[] svalue = jedis.hget(jedisKey.getBytes(), key.getBytes());
		if(svalue == null || svalue.length==0)
			return null;
		
		DocumentCluster doc = (DocumentCluster) RedisAccessHelper.getDocSerializer().deserialize(svalue);
		doc.dirtyOff();;
		return doc;
	}

	@Override
	protected void saveMap(String jedisKey, Map<String, DocumentCluster> data) 
	{
		RedisAccessHelper.saveStrMap(jedisKey, (Map<String, DocumentCluster>)data);
	}

	@Override
	DocumentCluster parse(String svalue) {
		return null;
	}

}
