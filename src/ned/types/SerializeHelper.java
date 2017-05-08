package ned.types;

import redis.clients.jedis.Jedis;

abstract public interface SerializeHelper<V> {
	public void set(Jedis jedis, String jedisKey, Object key, V value);
	public V get(Jedis jedis, String jedisKey, Object key);
}
