package ned.types;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ned.modules.Twokenize;
import ned.tools.ClusteringQueueManager;
import ned.tools.ExecutionHelper;
import ned.tools.RecentManager;
import ned.tools.RedisHelper;

public class GlobalData {
	public static final String ID2DOCUMENT = "id2document";
	public static final String WORD2INDEX = "word2index";
	public static final String WORD2IDF = "word2idf";
	
	public class Parameters 
	{
		public int monitor_timer_seconds = 5; //seconds
		public int number_of_threads =300;
		public int print_limit = 5000;
		public int number_of_tables = 70;
		public int hyperplanes = 13;
		public int max_bucket_size = 2000;
		public int max_documents = 50000000;
		public int max_thread_delta_time = 3600; //seconds
		public int skip_files = 0;//17;
		public int offset = 0;//8800000-skip_files*500000;
		public int search_recents = 2000;
		public double threshold = 0.6;
		public double min_cluster_entropy = 0.95;
		public double min_cluster_size = 3;
		public int inital_dimension = 50000;
		public int dimension_jumps = 5000;
	}
	
	
	private static GlobalData globalData = null;
	public static GlobalData getInstance() 
	{
		if (globalData == null)
			globalData = new GlobalData();
		
		return globalData;
	}
	
	//public Queue<String> queue; 
	public Hashtable<String, Integer>    word2index;
	//public Hashtable<String, Document>   id2document;
	
	//for calculating IDF
	int numberOfDocuments;
	private ConcurrentHashMap<Integer, Double> word2idf;

	public ConcurrentHashMap<Integer, Integer>   numberOfDocsIncludeWord;
	public ConcurrentHashMap<String, DocumentCluster>  clusters;
	public ConcurrentHashMap<String, String> id2cluster;
	//public LRUCache<String,Document> recent;
	
	private ClusteringQueueManager queue;
	private RecentManager recentManager;

	public RecentManager getRecentManager() {
		return recentManager;
	}

	public Parameters parameters = new Parameters();
	public List<String> cleanClusterQueue = null;
	
	
	private GlobalData()
	{	
		word2index  = new Hashtable<String , Integer>();
		//index2word  = new Hashtable<Integer, String>();
		//id2document = new Hashtable<String , Document>();
		numberOfDocsIncludeWord = new ConcurrentHashMap<Integer, Integer>();
		cleanClusterQueue = new LinkedList<String>();
		//cleanClusterQueue = (List<String>) Collections.synchronizedList(new LinkedList<String>()); //new LinkedList<Document>();
		clusters = new ConcurrentHashMap<String, DocumentCluster>();
		numberOfDocuments = 0;
		queue = new ClusteringQueueManager();
		word2idf = new ConcurrentHashMap<Integer, Double>();
		id2cluster = new ConcurrentHashMap<String, String>();
		recentManager = new RecentManager(2000);
		
		
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
		double numerator = (numberOfDocuments + 0.5) / numberOfDocsIncludeWord.get(word);
		numerator = Math.log10(numerator);
		
		double denominator = Math.log10(numberOfDocuments + 1.0);	
		
		return numerator / denominator;
	}
	
	public double getIDF(int k)
	{
		return word2idf.get(k);
	}
	
	
	public double getOrDefault(int k)
	{
		return word2idf.getOrDefault(k, -1.0);
	}
	
	public void calcWeights(Document doc, ConcurrentHashMap<Integer, Double> tmp2) 
	{
		
			 ConcurrentHashMap<Integer, Integer> wordCount = doc.getWordCount();
				Enumeration<Integer> tmp = wordCount.keys();
				
				while(tmp.hasMoreElements())
				{
					int k = tmp.nextElement();
					Integer a = wordCount.get(k);
					if (word2idf.get(k)==null)
						{
							double val = getIDF(k);
							word2idf.put(k, val);
						}
					Double b = word2idf.get(k);
					
					tmp2.put(k, a*b);
				}
				
		
		
	}
	
	public void calcWeights1(Document doc, Hashtable<Integer, Double> weights) 
	{		
		
		ConcurrentHashMap<Integer, Integer> wordCount = doc.getWordCount();
		
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
		
	}
	
	private int wordCounts(List<String> list, ConcurrentHashMap<Integer, Integer> concurrentHashMap)
	{
		int max_idx = addWords(list);
		
		for (String w : list) 
		{
			int idx = word2index.get(w);
			int val = concurrentHashMap.getOrDefault(idx,  0);
			val += 1;
			concurrentHashMap.put(idx, val);
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

		//update number of documents holding each word (for TFIDF)
		for (int i : doc.getWordCount().keySet()) 
		{
			int val = numberOfDocsIncludeWord.getOrDefault(i, 0);
			numberOfDocsIncludeWord.put(i, val+1);					
		}
		
		doc.setDimension ( d );
		//id2document.put(doc.getId(), doc);
		RedisHelper.setDocumentFromRedis(ID2DOCUMENT, doc.getId(), doc);

		numberOfDocuments++;
		
		addToRecent(doc.getId());
		
		for (int i : doc.getWordCount().keySet()) 
			word2idf.put(i, calcIDF(i));
	}
	
	private void addToRecent(String docId) {
		
		this.recentManager.AddToRecent(docId);
	}
	public List<String> getRecent() {
		
		return this.recentManager.getRecent();
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
		System.out.println("Total Active threads="+Thread.activeCount()+" StealCount= "+ExecutionHelper.getCommonForkPool().getStealCount());
		return String.format("\t[monitor] Words: %d, Documents: %d, Clusters %d, Recent: %d",
				this.word2index.size(),
				RedisHelper.redisSize(ID2DOCUMENT),//this.id2document.size(),
				this.clusters.size(),
				this.recentManager.getRecentsize()
			);
	}

	public void markForCleanup(String leadId) 
	{
		cleanClusterQueue.add(leadId);
	}
	
}
