package ned.types;


import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.redis.serializer.SerializationException;

import ned.tools.RedisHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
	private int cacheSize;
	private long actualSize;
    private Jedis jedis;
    private final String hashName;

  

  public LRUCache(int cacheSize,String hashName) {
    super(16, (float) 0.75, true);
    this.cacheSize = cacheSize;
    this.jedis=RedisHelper.getRedisClient();
    this.hashName=hashName;
    this.actualSize=0;
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }
  synchronized public V put(K key, V value) {
	  V r = super.put(key, value);
	  byte[] serObject;
	  actualSize++;	
	  try{
		 // System.out.println(value.getClass().getName());
		  serObject=mySerialize(value);
		 //= this.jedis.hset(hashName.getBytes(StandardCharsets.UTF_8), key.toString().getBytes(StandardCharsets.UTF_8), serObject);
		  this.jedis.hset(hashName.getBytes(), String.valueOf(key).getBytes(), serObject);

	  }catch(JedisException e){		
		e.printStackTrace();
		jedis=RedisHelper.getRedisClient();
		//this.put(key,value);
	  }
	 return r;
  }
@SuppressWarnings("unchecked")
synchronized public V get(Object key) {
	  V r = super.get(key);
	  if(r==null){			
			 try {
				// Object o=this.jedis.hget(hashName, String.valueOf(key));
				 Object o=this.jedis.hget(hashName.getBytes(), String.valueOf(key).getBytes());
				 if(o!=null) r =(V) o;
			} catch (Exception e) {
				// System.out.println("Str="+str+" r="+r);
				e.printStackTrace();
				 jedis=RedisHelper.getRedisClient();
				// return  this.get(key);
			}		
	 }else{
		 return r;
	 }
	return null;
  }
  
  protected void finalize() {
	    if( jedis != null ) jedis.close() ;
  }
  
  private byte [] mySerialize(Object o){
	  String clazz=o.getClass().getName();
	  if(clazz.equals("java.lang.Long")){
		  Long l=(Long)o;
		  return  String.valueOf(l.longValue()).getBytes();
		   
	  }
	  if(clazz.equals("java.lang.Intger")){
		  Long l=(Long)o;
		  return  String.valueOf(l.intValue()).getBytes();
	  }
	  if(clazz.equals("java.lang.Double")){
		  Double l=(Double)o;
		  return  String.valueOf(l.doubleValue()).getBytes();
	  }
	  if(clazz.equals("ned.types.Document")){
		  Document l=(Document)o;
		  return  RedisHelper.getDocSerializer().serialize(l);
	  }
	  if(clazz.equals("java.lang.String")){
		  String l=(String)o;
		  return l.getBytes();
	  }
	  System.out.println(clazz);
	return null;
	  
  }
  
  
}