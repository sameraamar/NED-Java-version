package ned.types;

import redis.clients.jedis.Jedis;

abstract public interface SerializeHelper<K, V> {
	public void set(Jedis jedis, String jedisKey, K key, V value);
	public V get(Jedis jedis, String jedisKey, K key);
	public void saveMap(String jedisKey, RedisBasedMap<K, V> redisBasedMap);
}
