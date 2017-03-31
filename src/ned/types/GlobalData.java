package ned.types;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import ned.modules.Twokenize;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

//amy winehouse 94801731006894080  Sat Jul 23 16:11:08 +0000 2011

public class GlobalData {
	public static final String ID2DOCUMENT = "id2document";

	public class Parameters 
	{
		public int DOUBLE_SCALE = 5; //precision scale for double
		public int monitor_timer_seconds = 5; //seconds
		public int number_of_threads = 50000;
		public int print_limit = 5000;
		public int number_of_tables = 70;
		public int hyperplanes = 13;
		public int max_bucket_size = 2000;
		public int max_documents = 10000;
		public int max_thread_delta_time = 1800; //seconds
		public int offset = 0; //8800000-17*500000;
		public int skip_files = 0;
		public int search_recents = 2000;
		public double threshold = 0.6;
		public double min_cluster_entropy = 0.0;
		public double min_cluster_size = 1;
		public int inital_dimension = 50000;
		public int dimension_jumps = 50000;
	}
	
	private static GlobalData globalData = null;
	private static ForkJoinPool forkPool;
	public static GlobalData getInstance() 
	{
		if (globalData == null)
			globalData = new GlobalData();
		
		return globalData;
	}
	
	public ConcurrentLinkedQueue<String> queue; 
	public Hashtable<String, Integer>    word2index;
	public Hashtable<String, Document>   id2document = null;
	
	//for calculating IDF
	public int numberOfDocuments;
	private Hashtable<Integer, Double> word2idf;
	public Hashtable<Integer, Integer>   numberOfDocsIncludeWord;
	public Hashtable<String, DocumentCluster>  clusters;
	public Hashtable<String, String> id2cluster;
	public ArrayList<String> recent;
	public Parameters parameters = new Parameters();
	public List<String> cleanClusterQueue = null;
	private JedisPool jedisPool = null;
	private RedisSerializer<Object> docSerializer ;
	
	
	public RedisSerializer<Object> getDocSerializer() {
		if(docSerializer==null){
			docSerializer= new JdkSerializationRedisSerializer();

		}
		return docSerializer;
	}


	public Jedis getRedisClient() {
		
		if(jedisPool==null){
			synchronized(this) 
			{
				if(jedisPool==null)
				{	
					JedisPoolConfig config = new JedisPoolConfig();
					config.setMaxTotal(10000);
					config.setMaxIdle(0);
					config.setMinIdle(0);
					config.setMaxWaitMillis(10);
					config.setTestOnBorrow(false);
					config.setTestOnReturn(false);
					config.setTestWhileIdle(false);
					jedisPool = new JedisPool(config,"localhost", 6379, 30000);
				}
			}
		}

		Jedis cn = null;
		try {
			cn = jedisPool.getResource();
		} catch (Exception e) {
			if(cn!=null)
				cn.close();
			
			cn=this.getRedisClient();
		}
		finally {
		}
		  
		return cn;
	}


	public long redisSize(String hash) 
	{
		if(id2document != null)
			return id2document.size();
		
		Jedis jdis=getRedisClient();
		long len = jdis.hlen(hash);
		jdis.close();
		return len;
	}

	private GlobalData()
	{	
		word2index  = new Hashtable<String , Integer>();
		id2document = new Hashtable<String , Document>();
		numberOfDocsIncludeWord = new Hashtable<Integer, Integer>();
		cleanClusterQueue = (List<String>) Collections.synchronizedList(new LinkedList<String>()); //new LinkedList<Document>();
		clusters = new Hashtable<String, DocumentCluster>();
		numberOfDocuments = 0;
		queue = new ConcurrentLinkedQueue<String>();
		word2idf = new Hashtable<Integer, Double>();
		id2cluster = new Hashtable<String, String>();
		clearRedisKeys();
	}

	private void clearRedisKeys()
	{
		if(id2document != null)
			return;
		
		Jedis jdis=getRedisClient();
		jdis.del(ID2DOCUMENT);
		jdis.close();
	}
	
	
	public DocumentCluster clusterByDoc(String id)
	{
		String leadId = id2cluster.get(id);
		if (leadId == null)
			return null;
		
		DocumentCluster c = clusters.get(leadId);
		return c;
	}
	
	private double calcIDF(int word) 
	{		
		double numerator = (numberOfDocuments + 0.5) / numberOfDocsIncludeWord.get(word);
		numerator = Math.log10(numerator);
		
		double denominator = Math.log10(numberOfDocuments + 1.0);	
		
		double idf = numerator / denominator;
		return idf;
	}
	
	public double getIDF(int k)
	{
		return word2idf.get(k);
	}
	
	
	public double getIDFOrDefault(int k)
	{
		return word2idf.getOrDefault(k, -1.0);
	}
	
	public void calcWeights(Document doc, Hashtable<Integer, Double> weights)
	{
		Hashtable<Integer, Integer> wordCount = doc.getWordCount();
		Enumeration<Integer> tmp = wordCount.keys();
		
		while(tmp.hasMoreElements())
		{
			int k = tmp.nextElement();
			double a = wordCount.get(k) * this.word2idf.get(k);
			weights.put(k, a);
		}
	}
	
	public int wordCounts(List<String> list, Hashtable<Integer, Integer> d)
	{
		int max_idx = addWords(list);
		
		for (String w : list) 
		{
			int idx = word2index.get(w);
			int val = d.getOrDefault(idx,  0);
			val += 1;
			d.put(idx, val);
		}
		
		return max_idx;
	}
	
	private int addWords(List<String> list)
	{
		int max = 0;
		
		for (String w : list) 
		{
			int tmp = addWord(w);
			if (tmp > max)
				max = tmp;
		}
		
		return max;
	}	
	private int addWord(String word)
	{
		int idx = word2index.getOrDefault(word, -1);
		
		if (idx == -1) 
		{
			idx = word2index.size();
			//index2word.put(idx, word);
			word2index.put(word, idx);
		}
		
		return idx;
	}
	
	public void addDocument(Document doc) 
	{
		int d = wordCounts( doc.getWords(), doc.getWordCount());
		doc.setDimension ( d );
		
		numberOfDocuments++;
		if (d > 0)
		{
			//update number of documents holding each word (for TFIDF)
			for (int i : doc.getWordCount().keySet()) 
			{
				int val = numberOfDocsIncludeWord.getOrDefault(i, 0);
				numberOfDocsIncludeWord.put(i, val+1);					
			}
	
			for (int i : doc.getWordCount().keySet()) 
			{
				double idf = calcIDF(i);
				word2idf.put(i, idf);
			}
			
			addToRecent(doc);
		}
		
		//id2document.put(doc.getId(), doc);
		this.setDocumentFromRedis(ID2DOCUMENT, doc.getId(), doc);
	}
	
	public Document getDocumentFromRedis(String hash,String key) {

		if(key == null)
			return null;
		
		if(id2document != null)
		{
			return id2document.get(key);
		}
		
		Document doc=null;
		Jedis jdis=getRedisClient();
		
		byte[] kbytes = key.getBytes();
		byte[] hbytes = hash.getBytes();
		byte[] retobject=jdis.hget(hbytes,kbytes);
		if(retobject!=null){
			doc=(Document) this.getDocSerializer().deserialize(retobject);
		}
		jdis.close();
		return doc;
	}

	public void setDocumentFromRedis(String hash,String key,Document doc){
		if(doc==null){
			return;
		}
		
		if(id2document != null)
		{
			id2document.put(key, doc);
			return;
		}
		
		Jedis jdis=getRedisClient();
		
		
		byte[] sobject=this.getDocSerializer().serialize(doc);
		jdis.hset(hash.getBytes(),key.getBytes(),sobject);
		jdis.close();
		
	}
	
	public void addToRecent(Document doc) {
		if (this.recent == null)
		{
			synchronized (this) {
				if(this.recent == null)
					this.recent = new ArrayList<String>(); //(List<String>) Collections.synchronizedList(new ArrayList<String>()); //new LinkedList<Document>();
			}
		}
		Integer lock = 0;

		synchronized (lock) {
			this.recent.add(doc.getId());
			if (this.recent.size() > this.parameters.search_recents)
				this.recent.remove(0);
		}
	}

	public String tweetWithoutURL(String text)
	{
		//Create Regex pattern to find urls
		Pattern urlPattern = Pattern.compile("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?");

		//Create a matcher with our 'urlPattern'
		Matcher matcher = urlPattern.matcher(text);

		String tweetWithoutHashtagAndUrl = null;
		//Check if matcher finds url
		if(matcher.find()) {
		    //Matcher found urls
		    //Removing them now..
		    tweetWithoutHashtagAndUrl = matcher.replaceAll("");
		    //Use new tweet here    
		} else {
		    //Matcher did not find any urls, which means the 'tweetWithoutHashtag' already is ready for further usage
		    tweetWithoutHashtagAndUrl = text;
		}
		return tweetWithoutHashtagAndUrl;
	}
	
	public void flushClustersAll(PrintStream out)
	{
		Enumeration<DocumentCluster> elements = clusters.elements();
		while(elements.hasMoreElements())
		{
			DocumentCluster c = elements.nextElement();
			
			boolean print = true;
			if (c.size() < this.getParams().min_cluster_size)
				print = false;
			
			else if(c.entropy() < this.getParams().min_cluster_entropy )
				print = false;
			
			if (print)
				out.println(c.toString());
			
			clusters.remove(c.leadId);
		} 
	}
	
	public void flushClusters(PrintStream out)
	{
		//System.out.println("flushClusters");
		flushClusters(out, null);
	}
	
	public void flushClusters(PrintStream out, Set<String> todelete)
	{
		int counter = 0;
		if (todelete == null)
			todelete = prepareListBeforeRelease();
		
		
		ArrayList<String> marktoremove = new ArrayList<String>();
		int countDocs = 0;
		for (String leadId : todelete) 
		{
			DocumentCluster cluster = clusterByDoc(leadId);
			if (cluster == null) 
				continue;
			
			boolean print = true;
			if (cluster.size() < this.getParams().min_cluster_size)
				print = false;
			
			else if(cluster.entropy() < this.getParams().min_cluster_entropy )
				print = false;
			
			if (print)
				out.println(cluster.toString());

			counter+=1;
			
			this.clusters.remove(leadId);
			for (String id : cluster.getIdList()) 
			{
				marktoremove.add(id);
			}
		}
		
		for (String id : marktoremove) {
			this.id2cluster.remove(id);
			
		
			if(id2document!=null)
				this.id2document.remove(id);
			else {
				Jedis jdis=getRedisClient();
				
				jdis.hdel(ID2DOCUMENT, id);
				jdis.close();
			}
			
			countDocs++;
		}
		
		
		if (counter>0)
			Session.getInstance().message(Session.DEBUG, "cleanClusters", "released "+counter+" clusters (" + countDocs + " docs)" );
	}

	public Set<String> prepareListBeforeRelease() {
		Set<String> todelete = new HashSet<String>();
		
		while (!cleanClusterQueue.isEmpty()) {
			String docId = cleanClusterQueue.remove(0); 
			String leadId = clusterByDoc(docId).leadId;
			todelete.add(leadId);
		}
		return todelete;
	}
	
	public List<String> identifyWords(String text) {
		
		List<String> words = Twokenize.tokenizeRawTweetText(text.toLowerCase());
		
		//StringTokenizer st = new StringTokenizer(text, delim);
		//String text2 = tweetWithoutURL(text);
		//String[] words = text2.toLowerCase().split("\\W+"); //("\\P{L}+");
		return words;
	}

	public Hashtable<String, String> getId2Cluster() {
		return id2cluster;
	}

	/*private HashMap<Integer, DocumentCluster> getClusters()
	{
		return clusters;
	}*/
	
	public void createCluster(Document doc)
	{
		DocumentCluster cluster = new DocumentCluster(doc);
		
		this.clusters.put(doc.getId(), cluster);
		this.id2cluster.put(doc.getId(), doc.getId());
		
		//this.clusters.put(next_index, cluster);
		//this.id2cluster.put(doc.getId(), next_index);

		//next_index++;
	}	
	
	public void mapToCluster(String leadId, Document doc)
	{
		//int idx = clusterIndexByDoc(leadId);
		//this.id2cluster.put(doc.getId(), idx);
		this.id2cluster.put(doc.getId(), leadId);
	}

	public Parameters getParams() {
		return parameters;
	}

	public void markOldClusters(String docId) 
	{
		int young = 0;
		int old = 0;
		
		Document doc = GlobalData.getInstance().getDocumentFromRedis(ID2DOCUMENT, docId);
		Enumeration<DocumentCluster> enumerator = clusters.elements();
		
		while(enumerator.hasMoreElements())
		//for (String leadId : this.clusters.keySet()) 
		{
			DocumentCluster c = enumerator.nextElement(); //this.clusterByDoc(leadId);
			if (c.isOpen(doc))
				young++;
			else
			{
				this.cleanClusterQueue.add(c.leadId);
				old++;
			}
		}
		
		if (old>0)
			Session.getInstance().message(Session.DEBUG, "markOldClusters", "marked " + old + " old clusters for cleanup");
	}

	public String memoryGlance() 
	{
		long len = redisSize(ID2DOCUMENT);
		return String.format("\t[monitor] Words: %d, Documents %d, Clusters %d, Recent: %d",
				this.word2index.size(),
				//this.id2document.size(),
				len,
				this.clusters.size(),
				this.recent==null ? 0 : this.recent.size()
			);
	}



	public void markForCleanup(String leadId) 
	{
		cleanClusterQueue.add(leadId);
	}


	synchronized public static ForkJoinPool getForkPool() {
		/*if(forkPool == null)
			forkPool = new ForkJoinPool(
							30000, 
							ForkJoinPool.defaultForkJoinWorkerThreadFactory, 
							new UncaughtExceptionHandler() {
								@Override
								public void uncaughtException(Thread t, Throwable e) {					
									e.printStackTrace();
								}
							}, 
							true);*/
			
		return ForkJoinPool.commonPool();
	}
	
}
