package ned.types;

import java.util.concurrent.ConcurrentHashMap;
import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class RedisBasedMap<K, V> extends ConcurrentHashMap<K, V> {
  /**
	 * 
	 */
	private static final long serialVersionUID = -389590226404496306L;
	
	private String jedisKey;
	private SerializeHelper<K, V> s;

	public RedisBasedMap(String redisKey, boolean resumeMode, SerializeHelper<K, V> s) {
		//super(16, (float) 0.75, true);
		super(16, (float) 0.75);
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
			value = s.get(jedis, jedisKey, (K)key);
			RedisAccessHelper.retunRedisClient(jedis);
			
			if (value != null)
				super.put((K)key, value);
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

	/*protected boolean removeEldestEntry(Map.Entry<K, V> eldest) 
	{
		System.out.println(">>>>>>>>>>> removeEldestEntry");
		return size() >= cacheSize;
	}*/
	
	public void save()
	{
		s.saveMap(jedisKey, this);
		clear();
	}
	
	public void load()
	{
		
	}
}