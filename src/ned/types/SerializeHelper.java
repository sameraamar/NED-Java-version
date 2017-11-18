package ned.types;

import java.util.Map;

import redis.clients.jedis.Jedis;

public interface SerializeHelper<K, V> {
	public void set(Jedis jedis, String jedisKey, K key, V value);
	
	public V get(Jedis jedis, String jedisKey, K key);
		
	public void saveMap(String jedisKey, Map<K, V> data);
}
