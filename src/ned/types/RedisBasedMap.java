package ned.types;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import ned.tools.RedisAccessHelper;
import redis.clients.jedis.Jedis;

public class RedisBasedMap<K, V> implements Map<K, V> {
	private ConcurrentHashMap<K, V> map;
	private String jedisKey;
	
	private SerializeHelper<K, V> s;
	boolean saveOnUpdate;
	boolean readFromRedisIfMissing;
	
	
	public RedisBasedMap(String redisKey, boolean reset, SerializeHelper<K, V> s) {
		init(redisKey, reset, false, true, s);
	}
	
	public RedisBasedMap(String redisKey, boolean reset, boolean saveOnUpdate, boolean readFromRedisOnMissing, SerializeHelper<K, V> s) {
		init(redisKey, reset, saveOnUpdate, readFromRedisOnMissing, s);
	}
	
	private void init(String redisKey, boolean reset, boolean saveOnUpdate, boolean readFromRedisOnMissing, SerializeHelper<K, V> s) {
		//super(16, (float) 0.75, true);
		map = new ConcurrentHashMap<K, V>(16, (float) 0.75);
		this.jedisKey = redisKey;
		this.s= s;
		this.saveOnUpdate =saveOnUpdate;
		this.readFromRedisIfMissing = readFromRedisOnMissing;
		
		if(reset)
		{
			RedisAccessHelper.resetKey(redisKey);
		}
	}
	
	@Override
	public V get(Object key) {
		V value = map.get(key);
		
		if (value == null && readFromRedisIfMissing)
		{
			Jedis jedis = RedisAccessHelper.getRedisClient();
			value = s.get(jedis, jedisKey, (K)key);
			RedisAccessHelper.retunRedisClient(jedis);
			
			if (value != null)
				map.put((K)key, value);
		}
		
		return value;
	}
	
	@Override
	public V put(K key, V value) {
		V v = map.put(key, value);
		
		if(saveOnUpdate)
		{
			Jedis jedis = RedisAccessHelper.getRedisClient();
			s.set(jedis, jedisKey, key, value);
			RedisAccessHelper.retunRedisClient(jedis);
		}
		
		return v;
	}
//		Runnable runnable = () -> {
//			Jedis jedis=getRedisClient();
//			
//			String keysArray[] = keys.split(",");
//			byte[][]  kyesBytes = new byte[keysArray.length][] ;
//			int index=0;
//			for (String string : keysArray) {
//					if(string.isEmpty()) continue;
//					kyesBytes[index]=string.getBytes();
//					index++;
//			}
//			jedis.hdel(hash.getBytes(),kyesBytes);
//		
//			retunRedisClient(jedis);
//		};
//		ExecutionHelper.asyncRun (runnable);

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
	
	public long redisSize()
	{
		return RedisAccessHelper.redisSize(jedisKey);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}
}