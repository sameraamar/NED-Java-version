package ned.types;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ned.tools.ExecutionHelper;

public class Document {
    private String id;
    private String text ;
    private List<String> words;
    //private java.util.Hashtable<Integer, Double> weights ;
    private ConcurrentHashMap<Integer, Integer> wordCount ;
    private int dimension;
    
    public int max_idx;
	private String cleanText;
	//document attributes
	private long timestamp;
	private String created_at;
	private String retweeted_id;
	private int retweet_count;
	private String reply_to;
	
	private String nearest;
	private double nearestDist;
	private boolean nearestDetermined;
	private int favouritesCount;
	private String user_id;

    public Document(String id, String text, long timestamp)
    {
    	this.id = id;
        this.text = text;
        //this.weights = null;
        this.nearest = null;
    	this.nearestDist = 1.0;
    	this.nearestDetermined = false;
        this.timestamp = timestamp;
        this.created_at = null;
    	this.retweeted_id = null;
    	this.retweet_count = 0;
    	this.favouritesCount = -1;
    	this.reply_to = null;
        this.wordCount = null;
        this.words = GlobalData.getInstance().identifyWords(text);
        this.cleanText = String.join(" ", words);
    }
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Document) 
		{
			String other = ((Document)obj).id;
			return this.id.equals(other);
		}
		
		return false;
	}
	
	@Override
	public int hashCode() 
	{
		return id.hashCode();
	}
	
	@Override
	protected void finalize() throws Throwable {
		//System.out.println("Document - finalize");
		super.finalize();
	}
	
    private static double Norm(ConcurrentHashMap<Integer, Double> weights) {
    	double res = 0;
        Enumeration<Double> values = weights.elements();
        
        while(values.hasMoreElements())
        {
			double v = values.nextElement();
            res += v * v;
        }

        res = Math.sqrt(res);
        return res;
	}
    
    public double Norm()
    {        
    	double res = Norm(getWeights());
        return res;
    }

    public static double Distance(Document left, Document right)
    {
    	Set<Integer> commonWords = DocumentClusteringHelper.intersection(left.getWordCount(), right.getWordCount());
     	if(commonWords.isEmpty()){
     		return 1;
     	}

        if (left.getWords().size() > right.getWords().size())
        {
            Document tmp = right;
            right = left;
            left = tmp;
        }
        
        ConcurrentHashMap<Integer, Double> rWeights = right.getWeights();
        ConcurrentHashMap<Integer, Double> lWeights = left.getWeights();
        
        double res = 0;
        double norms = Norm(rWeights) * Norm(lWeights);
        double dot = 0.0;

        for (Integer k : commonWords) {
            dot += rWeights.get(k) * lWeights.get(k);
		}
        
        res = dot / norms; 
        return 1.0 - res;
     	 
    }

	public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("{").append(id).append(": ").append(text);
    	//sb.append(weights)
    	sb.append("}");
    	return sb.toString();
    }

	public String getText() {
		return text;
	}

	public ConcurrentHashMap<Integer, Double> getWeights() {
		//if (weights == null) 
		//{
			//synchronized(this) {
				//if (weights!=null) //some other process already handlde
				//	return weights;
				
		ConcurrentHashMap<Integer, Double> tmp = new ConcurrentHashMap<Integer, Double>();
				
				GlobalData gd = GlobalData.getInstance();
				//gd.addDocument(this);
				gd.calcWeights(this, tmp);
			//}
		//}
		return tmp;
	}

	public List<String> getWords() {
		return words;
	}

	public String getId() {
		return id;
	}

	public int getDimension() {
		return dimension;
	}

	ConcurrentHashMap<Integer, Integer> getWordCount() {
		if (wordCount == null)
			wordCount = new ConcurrentHashMap<Integer, Integer>();
		
		return wordCount;
	}

	void setWordCount(ConcurrentHashMap<Integer, Integer> wordCount) {
		this.wordCount = wordCount;
	}

	void setDimension(int dimension) {
		this.dimension = dimension;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public String getCleanText() {
		return cleanText;
	}

	public void setCreatedAt(String created_at) {
		this.created_at = created_at;
	}

	public void updateNearest(Document right) {
		if(right == null)
			return;
		
		if(right.getId().compareTo(getId()) >= 0)
			return;
		
		double tmp = Document.Distance(this, right);
		if (nearest==null || tmp < nearestDist)
		{
			nearestDist = tmp;
			nearest = right.getId();
		}
	}

	public boolean isNearestDetermined() {
		return nearestDetermined;
	}

	public void setNearestDetermined(boolean nearestDetermined) {
		this.nearestDetermined = nearestDetermined;
	}

	public double getNearestDist() {
		return nearestDist;
	}

	public String getNearest() {
		return nearest;
	} 
	
	//**************************************************************
	public static Document parse(String json, boolean isBasicOnly)
	{
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();
		
		if(jsonObj.get("text") == null || jsonObj.get("id_str") == null)
			return null;
		
		String text = jsonObj.get("text").getAsString();
		String id = jsonObj.get("id_str").getAsString();
		long timestamp = jsonObj.get("timestamp").getAsLong();
		
        Document doc = new Document(id, text, timestamp); //id == "94816822100099073" is for Amy Winhouse event

        doc.created_at = jsonObj.get("created_at").getAsString();

        if(!isBasicOnly)
		{
        	JsonObject userObj = jsonObj.get("user").getAsJsonObject();
        	doc.user_id = userObj.get("id_str").getAsString();			
			
			//String retweeted_status = jsonObj.get("retweeted_status").getAsString();
			
        	JsonElement element = jsonObj.get("in_reply_to_status_id_str");
			if(!element.isJsonNull())
				doc.reply_to = element.getAsString();
				        
			doc.retweet_count = jsonObj.get("retweet_count").getAsInt();
			doc.favouritesCount = jsonObj.get("favorite_count").getAsInt();
	        
			element = jsonObj.get("retweeted_status");
			if(element!=null && !element.isJsonNull())
			{
				JsonObject retweetObj = element.getAsJsonObject();
				doc.retweeted_id = retweetObj.get("id_str").getAsString();
			}
		}        
        return doc;
	}

	public String getCreatedAt() {
		return created_at;
	}

	public String getRetweetedId() {
		return retweeted_id;
	}

	public int getRetweetCount() {
		return retweet_count;
	}

	public String getReplyTo() {
		return reply_to;
	}

	public String getUserId() {
		return user_id;
	}

	public int getFavouritesCount() {
		return favouritesCount;
	}

}
