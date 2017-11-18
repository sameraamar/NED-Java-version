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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import ned.modules.Twokenize;
import ned.tools.ClusteringQueueManager;
import ned.tools.ExecutionHelper;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class GlobalData {
	private static final String V = "";
	public static final String LAST_DIMENSION = "dimension";
	public static final String LAST_NUM_DOCS = "doc_count";
	public static final String LAST_SEEN_IDX = "last_idx";
	
	public static final String K_ID2DOCUMENT = "id2doc" + V;
	public static final String K_ID2WORD_COUNT = "id2word_counts" + V;
	public static final String K_WORD2INDEX = "w2i" + V;
	public static final String K_WORD2COUNTS = "w2c" + V;
	public static final String K_RESUME_INFO = "resume" + V;
	public static final String K_ID2CLUSTR_INFO = "id2cluster" + V;
	public static final String K_CLUSTR2REPLCMENT = "replacement" + V;

	public class Parameters 
	{
		public int roll_file = 50_000_000;
		public String DELIMITER = "\t";
		public int monitor_timer_seconds = 1; //seconds
		public int number_of_threads =100;
		public int print_limit = 5000;
		public int number_of_tables = 70;
		public int hyperplanes = 13; // k  -->  2^k * 2000 --> 
		public int max_bucket_size = 2000;
		public int max_documents = 50_000_001;
		public int max_thread_delta_time = 1*3600; //seconds
		public int offset =  0;
		public int provider_buffer_size = 25000; //read documents ahead
		public int search_recents = 2000;
		public double threshold = 0.6;
		public double min_cluster_entropy = 0.0;
		public double min_cluster_size = 1;
		public int dimension_jumps = 100000;
		public int inital_dimension = 9 * dimension_jumps;
		public boolean resume_mode = false;
		public boolean scan_mode_only = false; //keep this false unless you only wants to be in scan mode
		public int dimension_max = 15 * dimension_jumps;
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
	
	public RedisBasedMap<String, Integer>    word2index;
	public RedisBasedMap<String, Document>   id2doc;
	public RedisBasedMap<String, DocumentWordCounts> id2wc;

	public RedisBasedMap<Integer, Integer>   numberOfDocsIncludeWord;
	public RedisBasedMap<String, Integer>   resumeInfo;
	
	public ConcurrentHashMap<String, DocumentCluster>  clusters;
	public RedisBasedMap<String, String> id2cluster;
	public RedisBasedMap<String, String> cluster2replacement;
	
	private ClusteringQueueManager<String> queue;
	private RoundRobinArray<String> recentManager = null;

	private Parameters parameters = new Parameters();
	public List<String> cleanClusterQueue = null;
	public static ConcurrentHashMap<String, Double> id2nearestDist;
	public static ConcurrentHashMap<String, Boolean> id2nearestOk;
	public static ConcurrentHashMap<String, String> id2nearestId;
	
	private GlobalData()
	{	
		cleanClusterQueue = new LinkedList<String>();
		clusters = new ConcurrentHashMap<String, DocumentCluster>();
		queue = new ClusteringQueueManager<String>();
	}
	
	synchronized public void init() throws Exception
	{
		if(recentManager != null)
			return;
		
		recentManager = new RoundRobinArray<String>(parameters.search_recents);
		id2doc = new RedisBasedMap<String, Document>(GlobalData.K_ID2DOCUMENT, !getParams().resume_mode, new SerializeHelperAdapterDirtyBit<Document>() );
		id2wc = new RedisBasedMap<String, DocumentWordCounts>(GlobalData.K_ID2WORD_COUNT, !getParams().resume_mode, new SerializeHelperAdapterDirtyBit<DocumentWordCounts>() );
		word2index = new RedisBasedMap<String, Integer>(GlobalData.K_WORD2INDEX, !getParams().resume_mode, new SerializeHelperAdapterSimpleType<Integer>(Integer.class) );
		numberOfDocsIncludeWord = new RedisBasedMap<Integer, Integer>(GlobalData.K_WORD2COUNTS, !getParams().resume_mode, new SerializeHelperIntInt() );
		resumeInfo = new RedisBasedMap<String, Integer>(GlobalData.K_RESUME_INFO, !getParams().resume_mode, new SerializeHelperAdapterSimpleType<Integer>(Integer.class) );
		id2cluster = new RedisBasedMap<String, String>(GlobalData.K_ID2CLUSTR_INFO, true, new SerializeHelperAdapterSimpleType<String>(String.class) );
		cluster2replacement = new RedisBasedMap<String, String>(GlobalData.K_CLUSTR2REPLCMENT, true, new SerializeHelperAdapterSimpleType<String>(String.class) );
		id2nearestDist = new ConcurrentHashMap<String, Double>();
		id2nearestOk = new ConcurrentHashMap<String, Boolean>();
		id2nearestId = new ConcurrentHashMap<String, String>();
		
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

	public void save(boolean force)
	{
		try{
			System.out.println("Save to Redis...");
			id2doc.save();
	    	id2cluster.save();
	    	cluster2replacement.save();
	    	
	    	if(force || !getParams().resume_mode)
			{
				id2wc.save();
				word2index.save();
		    	numberOfDocsIncludeWord.save();
		    	resumeInfo.save();
			}
		}
		catch(JedisConnectionException re){
			System.out.println(re.getMessage());
			try {
				Thread.sleep(3000);
				save(force);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	public RoundRobinArray<String> getRecentManager() {
		return recentManager;
	}

	public ClusteringQueueManager<String> getQueue() {
		return queue;
	}

	public void setQueue(ClusteringQueueManager<String> queue) {
		this.queue = queue;
	}

	public DocumentCluster clusterByDoc(String id)
	{
		if(id == null)
			return null;
		
		String leadId = id2cluster.get(id);
		if (leadId == null)
			return null;
		
		DocumentCluster c = clusters.get(leadId);
		return c;
	}
	
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
	
	public void calcWeights(DocumentWordCounts doc, Map<Integer, Double> weights, Map<Integer, Double> word2idf) 
	{
		Map<Integer, Integer> wordCount = doc.getWordCount();
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
	
	private int wordCounts(List<String> list, Map<Integer, Integer> map)
	{
		int max_idx = addWords(list);
		
		for (String w : list) 
		{
			int idx = word2index.get(w);
			int val = map.getOrDefault(idx,  0);
			val += 1;
			map.put(idx, val);
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

					word2index.put(word, idx);
				}
			}
		}
		
		return idx;
	}
	
	public int addDocument(Document doc, int idx) 
	{
		
		DocumentWordCounts dwc = doc.bringWordCount();
		if(dwc == null)
		{
			String id = doc.getId().intern();
			synchronized (id) {
				dwc = id2wc.getOrDefault( doc.getId(), new DocumentWordCounts(id, new HashMap<Integer, Integer>()) );
				id2wc.put(id,  dwc);
			}
		}
		
		int d = wordCounts( doc.getWords(), dwc.getWordCount() );
		//doc.setDimension ( d );

		
		int lastDocIndex = resumeInfo.get(LAST_SEEN_IDX); 
		if(lastDocIndex < idx)
		{
			Set<Entry<Integer, Integer>> es = dwc.getWordCount().entrySet();
			//update number of documents holding each word (for TFIDF)
			for (Entry<Integer, Integer> k : es) 
			{
				int val = numberOfDocsIncludeWord.getOrDefault(k.getKey(), 0);
				numberOfDocsIncludeWord.put(k.getKey(), val+1);					
			}
			resumeInfo.put(LAST_SEEN_IDX, idx);
			resumeInfo.put(LAST_NUM_DOCS, resumeInfo.get(LAST_NUM_DOCS)+1);
		}
		
		id2doc.put(doc.getId(), doc);

		addToRecent(doc.getId());
		
		return d;
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
	
	public void flushClustersAll(PrintStream outFull, PrintStream outShort)
	{
		KeySetView<String, DocumentCluster> keys = this.clusters.keySet();
		System.out.println("Printing left-over clusters: " + keys.size());
		int c = keys.size();
		int c10 = c < 10 ? 10 : c / 10; //don't fail on zero division
		for (String leadId : keys) {
			flushOneCluster(leadId, outShort, outFull);
			c--;
			if(c % c10 == 0)
				System.out.println(c + " left");
		} 
	}
	
	public void flushClusters(PrintStream outFull, PrintStream outShort)
	{
		flushClusters(outFull, outShort, null);
	}
	
	public void flushClusters(PrintStream outFull, PrintStream outShort, Set<String> todelete)
	{
		int counter = 0;
		if (todelete == null)
			todelete = prepareListBeforeRelease();
		
		
		ArrayList<String> marktoremove = new ArrayList<String>();
		int countDocs = 0;
		for (String leadId : todelete) 
		{
			DocumentCluster cluster = flushOneCluster(leadId, outShort, outFull);
			if(cluster == null)	
				continue;
			
			counter+=1;
			
			this.clusters.remove(leadId);
			for (String id : cluster.getIdList()) 
			{
				marktoremove.add(id);
			}
		}
		
		for (String id : marktoremove) 
		{
			GlobalData.id2nearestId.remove(id);
			GlobalData.id2nearestDist.remove(id);
			GlobalData.id2nearestOk.remove(id);
			
			//this.id2cluster.remove(id);
			//countDocs++;
		}
		
		if (counter>0)
			Session.getInstance().message(Session.DEBUG, "cleanClusters", "released "+counter+" clusters (" + countDocs + " docs)" );
	}

	private DocumentCluster flushOneCluster(String leadId, PrintStream outShort, PrintStream outFull) {
		DocumentCluster cluster = clusterByDoc(leadId);
		if (cluster == null) 
			return null;
		
		boolean print = true;
		if (cluster.size() < this.getParams().min_cluster_size)
			print = false;
		
		else if(cluster.entropy() < this.getParams().min_cluster_entropy )
			print = false;
		
		if (print)
		{
			outFull.print(cluster.toStringFull());
			outShort.print(cluster.toStringShort());
		}
		
		return cluster;
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
		int old = 0;
		
		Enumeration<DocumentCluster> enumerator = clusters.elements();
		
		while(enumerator.hasMoreElements())
		{
			DocumentCluster c = enumerator.nextElement(); //this.clusterByDoc(leadId);
			if (!c.isOpen(doc))
			{
				this.cleanClusterQueue.add(c.leadId);
				old++;
			}
		}

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
		
		msg.append( String.format("\t[monitor] Documents: %d/%d, Document-WordCounts: %d/%d, Words: %d/%d, "
				+ "\n\tW2Count: %d/%d, Clusters %d, clust2replcmnt: %d, id2cluster: %d, nearest [Id: %d, D: %d, B: %d]",
				this.id2doc.size(), 
				-1, //this.id2doc.redisSize(), 
				this.id2wc.size(), 
				-1, //this.id2doc.redisSize(), 
				this.word2index.size(),
				-1, //this.word2index.redisSize(),
				this.numberOfDocsIncludeWord.size(),
				-1, //this.numberOfDocsIncludeWord.redisSize(),
				this.clusters.size(),
				this.cluster2replacement.size(),
				this.id2cluster.size(),
				id2nearestId.size(),
				id2nearestDist.size(),
				id2nearestOk.size()
			) ).append("\n");
		
		
		return msg.toString();
	}

	public void markForCleanup(String leadId) 
	{
		cleanClusterQueue.add(leadId);
	}
	
	public static Double  getId2nearestDist(String key) {
		Double res = id2nearestDist.get(key);
		if(res==null)
			res=1.0;
		return res;
	}

	public static void setId2nearestDist(String key, Double value) {
		id2nearestDist.put(key, value);
	}

	public static Boolean getId2nearestOk(String key) {
		Boolean res = id2nearestOk.get(key);
		if(res==null)
			res=false;
		return res;
		
	}

	public static  void setId2nearestOk(String key, boolean value) {
		id2nearestOk.put(key, value);
	}

	public static String getId2nearestId(String key) {
		return id2nearestId.get(key);
	}

	public static void setId2nearestId(String key, String value) {
		id2nearestId.put(key, value);
	}
	
}
