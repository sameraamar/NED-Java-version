package ned.types;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import ned.tools.RedisHelper;

public class Document  implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 4575371423244913253L;
	
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
	private String retweeted_user_id;
	private String quoted_status_id;
	private String reply_to_user_id;
	private String quoted_user_id;
	
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
	
    private static double Norm(HashMap<Integer, Double> hashMap) {
    	double res = 0;
        Set<Integer> keys = hashMap.keySet();
        
		for( Integer key:keys)
        {
			double v = hashMap.get(key);
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
        
        HashMap<Integer, Double> rWeights = right.getWeights();
        HashMap<Integer, Double> lWeights = left.getWeights();
        
        double res = 0;
        double norms = Norm(rWeights) * Norm(lWeights);
        double dot = 0.0;

        for (Integer k : commonWords) {
            dot += rWeights.get(k) * lWeights.get(k);
		}
        
        res = dot / norms; 
        res = 1.0 - res;
        //if(res < 0)
        //	res = 0;
        
//		if(left.getId().equals("86498628092440576"))
//		{
//			System.out.println("dist\t" + left.getId() + "\t" + right.getId() + "\t" + res);	
//		}
        
        return res;
     	 
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

	public HashMap<Integer, Double> getWeights() {
		//if (weights == null) 
		//{
			//synchronized(this) {
				//if (weights!=null) //some other process already handlde
				//	return weights;
				
		HashMap<Integer, Double> tmp = new HashMap<Integer, Double>();
				
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
	public void updateNearest(String rightId) 
	{
		if(rightId == null)
			return;
		
		Document right =RedisHelper.getDocumentFromRedis(rightId);
		updateNearest(right);
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
		
		String created_at = jsonObj.get("created_at").getAsString();
		JsonElement element = jsonObj.get("timestamp");
		long timestamp;
		if(element != null)
			timestamp = element.getAsLong();
		else {
			//convert from created_at to timestamp
			timestamp = 0;
		}
			

		Document doc = new Document(id, text, timestamp); //id == "94816822100099073" is for Amy Winhouse event

		
        doc.created_at = created_at;

        if(!isBasicOnly)
		{
        	JsonObject userObj = jsonObj.get("user").getAsJsonObject();
        	doc.user_id = userObj.get("id_str").getAsString();			
			
			//String retweeted_status = jsonObj.get("retweeted_status").getAsString();
			
        	element = jsonObj.get("in_reply_to_status_id_str");
			if(!element.isJsonNull())
				doc.reply_to = element.getAsString();
			
        	element = jsonObj.get("in_reply_to_user_id");
			if(!element.isJsonNull())
				doc.reply_to_user_id = element.getAsString();
			
        	element = jsonObj.get("quoted_status_id");
			if(element != null && !element.isJsonNull())
				doc.quoted_status_id = element.getAsString();
	        
			element = jsonObj.get("quoted_status");
			if(element!=null && !element.isJsonNull())
			{				
				JsonObject obj = element.getAsJsonObject();
				userObj = obj.get("user").getAsJsonObject();
	        	doc.quoted_user_id = userObj.get("id_str").getAsString();
			}
			
			doc.retweet_count = jsonObj.get("retweet_count").getAsInt();
			doc.favouritesCount = jsonObj.get("favorite_count").getAsInt();
	        
			element = jsonObj.get("retweeted_status");
			if(element!=null && !element.isJsonNull())
			{
				JsonObject retweetObj = element.getAsJsonObject();
				doc.retweeted_id = retweetObj.get("id_str").getAsString();
				
				userObj = retweetObj.get("user").getAsJsonObject();
	        	doc.retweeted_user_id = userObj.get("id_str").getAsString();
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

	public String getRetweetedUserId() {
		return retweeted_user_id;
	}

	public String getQuotedStatusId() {
		return quoted_status_id;
	}

	public String getQuotedUserId() {
		return quoted_user_id;
	}
	
	public String getReplyToUserId() {
		return reply_to_user_id;
	}
}
