package ned.types;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import ned.modules.Twokenize;
import ned.tools.ClusteringQueueManager;
import ned.tools.ExecutionHelper;

public class GlobalData {
	public static final String LAST_DIMENSION = "dimension";
	public static final String LAST_NUM_DOCS = "doc_count";
	public static final String LAST_SEEN_IDX = "last_idx";
	public static final String K_ID2DOCUMENT = "id2doc";
	public static final String K_WORD2INDEX = "w2i";
	public static final String K_WORD2COUNTS = "w2c";
	public static final String K_RESUME_INFO = "resume";

	public class Parameters 
	{
		public int monitor_timer_seconds = 5; //seconds
		public int number_of_threads =100;
		public int print_limit = 5000;
		public int number_of_tables = 70;
		public int hyperplanes = 13;
		public int max_bucket_size = 2000;
		public int max_documents = 50_000_000; //10_000_000;
		public int max_thread_delta_time = 3600; //seconds
		public int offset = 0; //230000;
		public int search_recents = 2000;
		public double threshold = 0.6;
		public double min_cluster_entropy = 0.0;
		public double min_cluster_size = 1;
		public int inital_dimension = 100000;
		public int dimension_jumps = 50000;
		public boolean resume_mode = false;
	}
	
	private static GlobalData globalData = null;
	public static GlobalData getInstance() 
	{
		if (globalData == null)
		{
			globalData = new GlobalData();
		}
		return globalData;
	}	
	
	public static void release()
	{
		globalData = null;
	}
	
	//public ConcurrentHashMap<String, JedisPool>  thread2redis = new  ConcurrentHashMap<String, JedisPool>();

	//public Queue<String> queue; 
	public RedisBasedMap<String, Integer>    word2index;
	public RedisBasedMap<String, Document>   id2doc;
	
	//for calculating IDF
	//int numberOfDocuments;
	//public RedisBasedMap<Integer, Double> word2idf;

	public RedisBasedMap<Integer, Integer>   numberOfDocsIncludeWord;
	public RedisBasedMap<String, Integer>   resumeInfo;
	
	public ConcurrentHashMap<String, DocumentCluster>  clusters;
	public ConcurrentHashMap<String, String> id2cluster;
	//public LRUCache<String,Document> recent;
	
	private ClusteringQueueManager queue;
	private RoundRobinArray<String> recentManager = null;

	private Parameters parameters = new Parameters();
	public List<String> cleanClusterQueue = null;
	
	private GlobalData()
	{	
		//word2index  = new Hashtable<String , Integer>();
		//index2word  = new Hashtable<Integer, String>();
		//numberOfDocsIncludeWord = new ConcurrentHashMap<Integer, Integer>();
		cleanClusterQueue = new LinkedList<String>();
		//cleanClusterQueue = (List<String>) Collections.synchronizedList(new LinkedList<String>()); //new LinkedList<Document>();
		clusters = new ConcurrentHashMap<String, DocumentCluster>();
		//numberOfDocuments = 0;
		queue = new ClusteringQueueManager();
		id2cluster = new ConcurrentHashMap<String, String>();
	}
	
	synchronized public void init() throws Exception
	{
		if(recentManager != null)
			return;
		
		recentManager = new RoundRobinArray<String>(parameters.search_recents);
		//id2doc = new Hashtable<String, Document>();
		id2doc = new RedisBasedMap<String, Document>(GlobalData.K_ID2DOCUMENT, !getParams().resume_mode, new SerializeHelperStrDoc() );
		word2index = new RedisBasedMap<String, Integer>(GlobalData.K_WORD2INDEX, !getParams().resume_mode, new SerializeHelperStrInt() );
		numberOfDocsIncludeWord = new RedisBasedMap<Integer, Integer>(GlobalData.K_WORD2COUNTS, !getParams().resume_mode, new SerializeHelperIntInt() );
		resumeInfo = new RedisBasedMap<String, Integer>(GlobalData.K_RESUME_INFO, !getParams().resume_mode, new SerializeHelperStrInt() );
		if(!getParams().resume_mode)
		{
			resumeInfo.put(LAST_SEEN_IDX, getParams().offset-1);
			resumeInfo.put(LAST_NUM_DOCS, 0);
			resumeInfo.put(LAST_DIMENSION, 0);
		}
		else
		{
			if(word2index.redisSize() != numberOfDocsIncludeWord.redisSize())
			{
				throw new Exception(String.format("Mistmach in the size of word2index vs. numberOfDocsIncludeWord (%d vs. %d)", word2index.redisSize(), numberOfDocsIncludeWord.redisSize()));
			}
		}
	}

	public RoundRobinArray<String> getRecentManager() {
		return recentManager;
	}

	public ClusteringQueueManager getQueue() {
		return queue;
	}

	public void setQueue(ClusteringQueueManager queue) {
		this.queue = queue;
	}

	public DocumentCluster clusterByDoc(String id)
	{
		String leadId = id2cluster.get(id);
		if (leadId == null)
			return null;
		
		DocumentCluster c = clusters.get(leadId);
		return c;
	}
	
	/*public int clusterIndexByDoc(String id)
	{
		Integer idx = id2cluster.get(id);
		if (idx == null)
			return -1;
		return idx.intValue();
	}*/
	
	/*private DocumentCluster getClusterByIndex(int index) 
	{
		return this.clusters.get(index);
	}*/
	
	/*public int clustersSize()
	{
		return this.clusters.size();
	}*/
	

	
	public double calcIDF(int word) 
	{		
		int numberOfDocuments = resumeInfo.get(LAST_NUM_DOCS);
		
		Integer appeared = numberOfDocsIncludeWord.get(word);
		
		if (appeared ==null)
			return -1;
		
		double numerator = (numberOfDocuments + 0.5) / appeared;
		numerator = Math.log10(numerator);
		
		double denominator = Math.log10(numberOfDocuments + 1.0);	
		
		return numerator / denominator;
	}
	
	/*public double getIDF(int k)
	{
		return word2idf.get(k);
	}
	
	
	public double getOrDefault(int k)
	{
		return word2idf.getOrDefault(k, -1.0);
	}*/
	
	public void calcWeights(Document doc, Map<Integer, Double> weights, Map<Integer, Double> word2idf) 
	{
		 HashMap<Integer, Integer> wordCount = doc.getWordCount();

		Set<Entry<Integer, Integer>> tmp = wordCount.entrySet();
		
		for (Entry<Integer, Integer> entry : tmp) {
			int k =entry.getKey();
			Integer a = wordCount.get(k);
			Double b = word2idf.get(k);
			if (b==null)
			{
				b = calcIDF(k);
				word2idf.put(k, b);
			}
			double r=a*b;
			weights.put(k, r);
		}
		
		
	}
	
	/*public void calcWeights1(Document doc, Hashtable<Integer, Double> weights) 
	{		
		
		ConcurrentHashMap<Integer, Integer> wordCount = doc.getWordCount();
		
		int numberOfDocuments = resumeInfo.get(LAST_NUM_DOCS);
		int Dt_size = numberOfDocuments; //id2document.size();
		
		for (Integer k : wordCount.keySet()) {
			double numerator = (Dt_size + 0.5) / numberOfDocsIncludeWord.get(k);
			numerator = Math.log10(numerator);
			
			double denominator = Math.log10(Dt_size + 1.0);
			//double p1 = Math.log10(Dt_size + 0.5) / word2appearance.get(k);
		    //double p2 = Math.log10(Dt_size + 1.0);
		    
			double a = wordCount.get(k) * (numerator / denominator);
		    weights.put(k, a);
		}
		
	}*/
	
	private int wordCounts(List<String> list, HashMap<Integer, Integer> hashMap)
	{
		int max_idx = addWords(list);
		
		for (String w : list) 
		{
			int idx = word2index.get(w);
			int val = hashMap.getOrDefault(idx,  0);
			val += 1;
			hashMap.put(idx, val);
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
			synchronized (word2index)
			{
				if (word2index.getOrDefault(word, -1) == -1)
				{
					idx = resumeInfo.get(LAST_DIMENSION);
					resumeInfo.put(LAST_DIMENSION, idx+1);
					
					//index2word.put(idx, word);
					word2index.put(word, idx);
				}
			}
		}
		
		return idx;
	}
	
	public void addDocument(Document doc, int idx) 
	{
		int d = wordCounts( doc.getWords(), doc.getWordCount());
		doc.setDimension ( d );

		int lastDocIndex = resumeInfo.get(LAST_SEEN_IDX); 
		if(lastDocIndex < idx)
		{
			Set<Entry<Integer, Integer>> keySet = doc.getWordCount().entrySet();
			//update number of documents holding each word (for TFIDF)
			for (Entry<Integer, Integer> k : keySet) 
			{
				int val = numberOfDocsIncludeWord.getOrDefault(k.getKey(), 0);
				numberOfDocsIncludeWord.put(k.getKey(), val+1);					
			}
			resumeInfo.put(LAST_SEEN_IDX, idx);
			resumeInfo.put(LAST_NUM_DOCS, resumeInfo.get(LAST_NUM_DOCS)+1);
		}
		
		id2doc.put(doc.getId(), doc);
		//RedisHelper.setDocumentFromRedis(ID2DOCUMENT, doc.getId(), doc);

		//numberOfDocuments++;
		addToRecent(doc.getId());
	}
	
	private void addToRecent(String docId) {
		
		this.recentManager.add(docId.intern());
	}
	
	//public List<String> getRecent() {		
	//	return this.recentManager.getRecentCopy();
	//}

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
		for (String leadId : this.clusters.keySet()) {
			DocumentCluster c = clusterByDoc(leadId);
			
			boolean print = true;
			if (c.size() < this.getParams().min_cluster_size)
				print = false;
			
			else if(c.entropy() < this.getParams().min_cluster_entropy )
				print = false;
			
			if (print)
				out.println(c.toString());
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
			//this.id2document.remove(id);
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

	public ConcurrentHashMap<String, String> getId2Cluster() {
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

	public void markOldClusters(Document doc) 
	{
		int young = 0;
		int old = 0;
		
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
		
		int wait;
		if (true)
			wait = 0;
		
		if (old>0)
			Session.getInstance().message(Session.DEBUG, "markOldClusters", "marked " + old + " old clusters for cleanup");
	}

	public String memoryGlance() {
		
		StringBuffer msg = new StringBuffer();
		
		msg.append("\t[monitor] Sumitted TaskCount ").append(ExecutionHelper.getQueuedSubmissionCount());
		msg.append(" Total Active threads=").append( Thread.activeCount());
		msg.append(" ActiveTasks= ").append(ExecutionHelper.activeCount());
		msg.append(" QueuedTaskCount ").append(ExecutionHelper.getQueuedTaskCount());
		msg.append("\n");
		
		msg.append( String.format("\t[monitor] Documents: %d/%d, Words: %d/%d, W2Count: %d/%d, Clusters %d, Recent: %d",
				this.id2doc.size(), 
				-1, //this.id2doc.redisSize(), 
				this.word2index.size(),
				-1, //this.word2index.redisSize(),
				this.numberOfDocsIncludeWord.size(),
				-1, //this.numberOfDocsIncludeWord.redisSize(),
				this.clusters.size(),
				this.recentManager.size()
			) ).append("\n");
		
		
		return msg.toString();
	}

	public void markForCleanup(String leadId) 
	{
		cleanClusterQueue.add(leadId);
	}
	
}
