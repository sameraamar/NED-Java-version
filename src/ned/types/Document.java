package ned.types;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Document  implements Serializable, DirtyBit {
    /**
	 * 
	 */
	private static final long serialVersionUID = 4575371423244913253L;
	
	public boolean isDirtyBit;
	
	private String id;
    private String text ;
    private List<String> words;
    private int dimension;
    
    public int max_idx;
	private String cleanText;
	//document attributes
	private long timestamp;
	private String created_at;
	private String retweeted_id;
	private int retweet_count;
	private String reply_to;
	
	//private String nearest;
	//private double nearestDist;
	//private boolean nearestDetermined;
	private int favouritesCount;
	private String user_id;
	private String retweeted_user_id;
	private String quoted_status_id;
	private String reply_to_user_id;
	private String quoted_user_id;
	
	public static Document createOrGet(String id)
	{
		Document doc = null;
		
		id = id.intern();
		synchronized (id) {
			doc = GlobalData.getInstance().id2doc.get(id);
			if(doc == null)
			{
				doc = new Document(id);
				GlobalData.getInstance().id2doc.put(id, doc);
			}
			
		}
		
		return doc;
	}
	
	private Document(String id)
	{
    	this.id = id.intern();
	}
	
    public void init(String text, long timestamp)
    {
    	dirtyOn();
        this.text = text;
        //this.weights = null;
        //this.nearest = null;
    	//this.nearestDist = 1.0;
    	//this.nearestDetermined = false;
        this.timestamp = timestamp;
        this.created_at = null;
    	this.retweeted_id = null;
    	this.retweet_count = 0;
    	this.favouritesCount = -1;
    	this.reply_to = null;
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
	
    private static double Norm(Map<Integer, Double> rWeights) {
    	double res = 0;
        for(Double v : rWeights.values())
        {
            res += v * v;
        }

        res = Math.sqrt(res);
        return res;
	}
    
    public static double Distance(DocumentWordCounts left, DocumentWordCounts right, Map<Integer, Double> word2idf)
    {
    	Set<Integer> commonWords = DocumentClusteringHelper.intersection(left.getWordCount(), right.getWordCount());
     	if(commonWords.isEmpty()){
     		return 1;
     	}

        if (left.getWordCount().size() > right.getWordCount().size())
        {
            DocumentWordCounts tmp = right;
            right = left;
            left = tmp;
        }
        
        Map<Integer, Double> rWeights = right.getWeights(word2idf);
        Map<Integer, Double> lWeights = left.getWeights(word2idf);
        
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

	public List<String> getWords() {
		return words;
	}

	public String getId() {
		return id;
	}

	public int getDimension() 
	{
		return dimension;
	}
	
	DocumentWordCounts bringWordCount()
	{
		DocumentWordCounts wordCount = GlobalData.getInstance().id2wc.get(id);
		return wordCount;
	}

	void setDimension(int dimension) {
		this.dimension = dimension;
		dirtyOn();
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public String getCleanText() {
		return cleanText;
	}

	public void setCreatedAt(String created_at) {
		this.created_at = created_at;
		dirtyOn();
	}

	public void updateNearest(DocumentWordCounts rWordCount, Map<Integer, Double> word2idf) {
		if(rWordCount == null)
			return;
		
		if(rWordCount.getId().compareTo(getId()) >= 0)
			return;
		
		DocumentWordCounts myWC = GlobalData.getInstance().id2wc.get( getId() );
				
		double tmp = Document.Distance(myWC, rWordCount, word2idf);
		if (getNearestId()==null || tmp < getNearestDist())
		{
			//try {
				setNearestDist(tmp);
			//} catch (NullPointerException ne)
			//{
			//	System.out.println("Failed on NullPointerException... don't know why!?");
			//	setNearestDist(tmp);
			//}
			setNearestId( rWordCount.getId() );
			dirtyOn();
		}
	}
	public void updateNearest(String rightId, Map<Integer, Double> word2idf) 
	{
		if(rightId == null)
			return;
		
		//Document right = RedisHelper.getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);
		DocumentWordCounts right = GlobalData.getInstance().id2wc.get(rightId);
		updateNearest(right, word2idf);
	}

	public boolean isNearestDetermined() {
		return GlobalData.getId2nearestOk(id);
	}

	public void setNearestDetermined(boolean nearestDetermined) {
		GlobalData.setId2nearestOk(id,  nearestDetermined);
	}

	public double getNearestDist() {
		return GlobalData.getId2nearestDist(id);
	}

	private void setNearestDist(double d) {
		GlobalData.setId2nearestDist(id, d);
	}

	public String getNearestId() {
		return GlobalData.getId2nearestId(id);
	} 
	
	public void setNearestId(String n) {
		GlobalData.setId2nearestId(id, n);
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
			
		//id == "94816822100099073" is for Amy Winhouse event
		Document doc = Document.createOrGet(id);
		doc.init(text, timestamp); 

		
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

	@Override
	public boolean isDirty() {
		return isDirtyBit;
	}

	@Override
	public void dirtyOff() {
		isDirtyBit = false;
	}

	@Override
	public void dirtyOn() {
		isDirtyBit = true;
	}
}
