package ned.types;


import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.redis.serializer.SerializationException;

import ned.tools.ExecutionHelper;
import ned.tools.RedisHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
	private int cacheSize;
	private long actualSize;
    private Jedis jedis;
    private final String hashName;

  

  public LRUCache(int cacheSize,String hashName,boolean flush) {
    super(16, (float) 0.75, true);
    this.cacheSize = cacheSize;
    this.jedis=RedisHelper.getRedisClient();
    this.hashName=hashName;
    this.actualSize=0;
    if(flush){
    	jedis.del(hashName);
    }
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }
   public V put(K key, V value) {
	  V r = super.put(key, value);
	 
	  actualSize++;	
	  Runnable task= () ->{
	  synchronized(jedis){
		  try{
				 // System.out.println(value.getClass().getName());
			  byte[]  serObject=mySerialize(value);
				 //= this.jedis.hset(hashName.getBytes(StandardCharsets.UTF_8), key.toString().getBytes(StandardCharsets.UTF_8), serObject);
			  if(verifySerializer(value,serObject)){
				  this.jedis.hset(hashName.getBytes(), String.valueOf(key).getBytes(), serObject);
			  }else{
				  throw new Exception("verifySerializer "+value); 
			  }

			  }catch(JedisException e){		
				e.printStackTrace();
				jedis=RedisHelper.getRedisClient();
				//this.put(key,value);
			  }
		  		catch(Exception e){		
				e.printStackTrace();
				//jedis=RedisHelper.getRedisClient();
				//this.put(key,value);
			  }
	  	}
	  };
	  task.run();
	 // ExecutionHelper.asyncRun(task);
	 return r;
  }
@SuppressWarnings("unchecked")
 public V get(Object key) {
	  V r = super.get(key);
	  if(r==null){		
		  synchronized(jedis){
			  try {
					// Object o=this.jedis.hget(hashName, String.valueOf(key));
					 Object o=this.jedis.hget(hashName.getBytes(), String.valueOf(key).getBytes());
					 if(o!=null ){
						 r =myDeSerialize((byte[]) o);
					 }else{
						 if(actualSize>cacheSize){
							 System.out.println("NOT In redis "+key);
							// r=get(key);
						 }
						
					 }
				} catch (Exception e) {
					//
					e.printStackTrace();
					 jedis=RedisHelper.getRedisClient();
					// return  this.get(key);
				}		
		  }
			
	 }
	  return r;
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
	  if(clazz.equals("java.lang.Integer")){
		  Integer l=(Integer)o;
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
	//  System.out.println(clazz);
	return String.valueOf(o).getBytes();
	  
  }
  private V myDeSerialize(byte[] o){
	  String clazz=o.getClass().getName();
	 if(this.hashName.equals("numberOfDocsIncludeWord")){
		 Integer i=Integer.valueOf(new String(o));
		 return (V)  i;
	 }
	 if(this.hashName.equals("word2index")){	
		 String s=new String(o);
		 System.out.println("s= "+s);
		 return (V)  s;
	 }
	 if(this.hashName.equals("word2idf")){
		 Double d = Double.valueOf(new String(o));
		
			 System.out.println("d= "+d);
		 
		 return (V)  d;
	 }
	 if(this.hashName.equals("id2document")){			
		 return (V) RedisHelper.getDocSerializer().deserialize(o);
	 }
	
	return (V) RedisHelper.getDocSerializer().deserialize(o);
	  
  }
  private boolean verifySerializer(V value,  byte[] serObject){
	return value.getClass().getName().equals(myDeSerialize(serObject).getClass().getName());
	  
  }
  
}