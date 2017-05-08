package ned.types;


import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import ned.tools.RedisAccessHelper;
import ned.tools.RedisHelper;
import redis.clients.jedis.Jedis;

public class RedisBasedMap<K, V> extends LinkedHashMap<K, V> {
  /**
	 * 
	 */
	private static final long serialVersionUID = -389590226404496306L;
	
	private int cacheSize;
	private String jedisKey;
	private SerializeHelper<V> s;

	public RedisBasedMap(String redisKey, int cacheSize, boolean resumeMode, SerializeHelper<V> s) {
		super(16, (float) 0.75, true);
		this.cacheSize = cacheSize;
		this.jedisKey = redisKey;
		this.s= s;
		
		if(!resumeMode)
		{
			RedisAccessHelper.resetKey(redisKey);
		}
	}
	
	@Override
	public V get(Object key) {
		V value = super.get(key);
		
		if (value == null)
		{
			Jedis jedis = RedisAccessHelper.getRedisClient();
			value = s.get(jedis, jedisKey, key);
			RedisAccessHelper.retunRedisClient(jedis);
		}
		
		return value;
	}
	
	@Override
	public V put(K key, V value) {
		V v = super.put(key, value);
		
		Jedis jedis = RedisAccessHelper.getRedisClient();
		s.set(jedis, jedisKey, key, value);
		RedisAccessHelper.retunRedisClient(jedis);
		
		return v;
	}

	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return size() >= cacheSize;
	}
}