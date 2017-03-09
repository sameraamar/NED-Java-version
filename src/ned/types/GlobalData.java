package ned.types;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ned.modules.Twokenize;



public class GlobalData {
	public class Parameters 
	{
		public int inital_dimension = 50000;
		public int print_limit = 1000;
		public int number_of_tables = 60;
		public int hyperplanes = 12;
		public int max_bucket_size = 100;
		public int max_documents = 100000;
		public int max_thread_delta_time = 3600; //seconds
		public int offset = 0; //8800000;
		public int search_recents = 100;
		public double threshold = 0.6;
	}
	
	private static GlobalData globalData = null;
	public static GlobalData getInstance() 
	{
		if (globalData == null)
			globalData = new GlobalData();
		
		return globalData;
	}
	
	private GlobalData()
	{	
		word2index  = new HashMap<String , Integer>();
		index2word  = new HashMap<Integer, String>();
		id2document = new HashMap<String , Document>();
		numberOfDocsIncludeWord = new HashMap<Integer, Integer>();
		clusters = new ArrayList<DocumentCluster>();
		id2cluster = new HashMap<String, DocumentCluster>();
	}
	
	
	
	public void calcWeights(Document doc, Dict weights) 
	{		
		
		Dict wordCount = doc.getWordCount();
		
		int Dt_size = id2document.size();
		
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
	
	private int wordCounts(String[] strings, Dict d)
	{
		int max_idx = addWords(strings);
		
		for (String w : strings) 
		{
			int idx = word2index.get(w);
			Double val = d.getOrDefault(idx,  0.0);
			val += 1.0;
			d.put(idx, val);
		}
		
		return max_idx;
	}
	
	private int addWords(String[] strings)
	{
		int max = 0;
		
		for (String w : strings) 
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
			index2word.put(idx, word);
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
		id2document.put(doc.getId(), doc);
		
		addToRecent(doc);
	}
	
	private void addToRecent(Document doc) {
		if (this.recent == null)
			this.recent = new LinkedList<Document>();
		
		this.recent.addLast(doc);
		if (this.recent.size() > this.parameters.search_recents)
			this.recent.removeFirst();
	}

	public HashMap<String, Integer>    word2index;
	public HashMap<Integer, String>    index2word;
	public HashMap<String, Document>   id2document;
	public HashMap<Integer, Integer>   numberOfDocsIncludeWord;
	public ArrayList<DocumentCluster>  clusters;
	public HashMap<String, DocumentCluster> id2cluster;
	public LinkedList<Document> recent;
	public Parameters parameters = new Parameters();
	public LinkedList<String> cleanClusterQueue = new LinkedList<String>();;
	
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
	
	public void flushClustersAll(PrintStream out, int minsize)
	{
		for (DocumentCluster c : clusters) {
			if (c.size() >= minsize)
				out.println(c.toString());
		} 
		//flushClusters(out, false);
	}
	
	public void flushClusters(PrintStream out)
	{
		int counter = 0;
		ArrayList<String> toRemove = new ArrayList<String>();
		while (!cleanClusterQueue.isEmpty()) {
			String docId = cleanClusterQueue.removeFirst(); //important since we add to the last
			toRemove.add(docId);
			
			DocumentCluster cluster = id2cluster.get(docId);
			if (cluster == null) 
				continue;
			
			out.println(cluster.toString());
			
			/*for (String itemId : cluster.getIdList()) 
			{
				toRemove.add(itemId);
			}*/
			counter+=1;
			id2cluster.remove(docId);
			
		}
		
		/*
		for (String id : toRemove) 
		{
			id2document.remove(id);
		}
		*/
		if (counter>0)
			Session.getInstance().message(Session.INFO, "cleanClusters", "released "+counter+" clusters" );
	}
	
	public String[] identifyWords(String text) {
		
		//List<String> words = Twokenize.tokenize(text);
		//StringTokenizer st = new StringTokenizer(text, delim);
		String text2 = tweetWithoutURL(text);
		String[] words = text2.toLowerCase().split("\\W+"); //("\\P{L}+");
		return words;
	}

	public HashMap<String, DocumentCluster> getId2Cluster() {
		return id2cluster;
	}

	public ArrayList<DocumentCluster> getClusters()
	{
		return clusters;
	}

	public Parameters getParams() {
		return parameters;
	}
	
}
