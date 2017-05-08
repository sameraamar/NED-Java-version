package ned.types;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import ned.tools.RedisHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6418459494767522439L;
	private int cacheSize;
    private Jedis wjedis;
    private Jedis rjedis;
    private AtomicInteger nextJedisIndex;
    private Jedis[] jedisArr;
    private int jedisArrSize=6;
    private Integer readLock = 0;
    private Integer writeLock = 1;
    
    private JedisPool myJedisPool;
    private boolean storeInRedis;
    private final String hashName;
    byte[] hashNameBytes;
    
  public LRUCache(int cacheSize, String hashName, boolean resetRedis, boolean storeInRedis) {
    super(16, (float) 0.75, true);
    this.cacheSize = cacheSize;
    this.storeInRedis = storeInRedis;
    myJedisPool=RedisHelper.getRedisConnectionPool();
    this.wjedis=myJedisPool.getResource();
    this.rjedis=myJedisPool.getResource();
    this.hashName=hashName;
	hashNameBytes = hashName.getBytes();
    prepareJedis();


    if(resetRedis){
    	wjedis.del(hashName);
    } 
  }
  
  private void prepareJedis(){
	  jedisArr=new Jedis[jedisArrSize];
	  for(int i=0;i<jedisArrSize;++i){
		  jedisArr[i]=myJedisPool.getResource();
		// System.out.println(this.hashName+i);
	  }
	  this.nextJedisIndex=new AtomicInteger(0);
	  System.out.println(this.hashName+" Done ");
  }
  
  private Jedis getJedis(){
		Jedis jedis=jedisArr[this.nextJedisIndex.get()%jedisArrSize];
		nextJedisIndex.set((nextJedisIndex.incrementAndGet()%jedisArrSize));
		return jedis;	
	
	
	  
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return super.size() >= cacheSize;
  }
   public V put(K key, V value) {
	  V r = super.put(key, value);
	  
	  if(storeInRedis) {
		  
			  try{
				  byte[]  serObject=mySerialize(value);
				  Jedis jedis=getJedis();
				  	synchronized(jedis){
					  jedis.hset(hashNameBytes, String.valueOf(key).getBytes(), serObject);
					}
				  }catch(JedisException e){		
					e.printStackTrace();
					wjedis=myJedisPool.getResource();
					this.put(key,value);
				  }
			  		catch(Exception e){		
					e.printStackTrace();
					//jedis=RedisHelper.getRedisClient();
					//this.put(key,value);
				  }
		
		  // ExecutionHelper.asyncRun(task);
	  }
	  
	  return r;
  }
@SuppressWarnings("unchecked")
 public V get(Object key) {
	  V r = super.get(key);

	  if(storeInRedis && r==null){		
			  try {
				  Object o;
				  Jedis jedis=getJedis();
				  synchronized(jedis){
					   o=jedis.hget(hashNameBytes, String.valueOf(key).getBytes());
				  }
				 if(o!=null ){
					 r =myDeSerialize((byte[]) o);
				 }
				} catch (Exception e) {
					//
					e.printStackTrace();
					 rjedis=myJedisPool.getResource();
					 return  this.get(key);
				}		
		  
			
	 }
	  if(r!=null){
		  super.put((K) key, r);
	  }
		  
		  
	  return r;
  }
  
	@Override
	public V getOrDefault(Object key, V defaultValue) {
		V val = get(key);
		if (val == null)
			return defaultValue;
		return val;
	}

  protected void finalize() {
	    if( rjedis != null ) rjedis.close() ;
	    if( wjedis != null ) wjedis.close() ;

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
	 if(this.hashName.equals(GlobalData.DOCSICLUDEDWORD)){
		 Integer i=Integer.valueOf(new String(o));
		 return (V)  i;
	 }
	 if(this.hashName.equals(RedisHelper.WORD2INDEX)){	
		 Integer i=Integer.valueOf(new String(o));
		 return (V)  i;
	 }
	 if(this.hashName.equals(RedisHelper.WORD2IDF)){
		 Double d = Double.valueOf(new String(o));
		 return (V)  d;
	 }
	 if(this.hashName.equals(RedisHelper.ID2DOCUMENT)){			
		 return (V) RedisHelper.getDocSerializer().deserialize(o);
	 }
	
	return (V) RedisHelper.getDocSerializer().deserialize(o);
	  
  }
  private boolean verifySerializer(V value,  byte[] serObject){
	return value.getClass().getName().equals(myDeSerialize(serObject).getClass().getName());
	  
  }
  
  public int size(){
	  if(storeInRedis)
	  {
		  synchronized(rjedis) 
		  {
			  return rjedis.hlen(hashNameBytes).intValue();
		  }
	  }
	  return super.size();
  }
  
}