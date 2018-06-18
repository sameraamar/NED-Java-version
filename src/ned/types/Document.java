package ned.types;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Document  implements Serializable, DirtyBit {
    /**
	 * 
	 */
	private static final long serialVersionUID = 4575371423244913253L;

	private static boolean isBasicOnly;

	private int hashtags;
	private int multipleWordsTag = 0;
	private int symbols;
	private int urls;
	private int user_mentions;
	
	public boolean isDirtyBit;
	
	private String id;
    private String text ;
    private List<String> words;
    //private int dimension;
    
    public int max_idx;
	private String cleanText;
	//document attributes
	private long timestamp;
	private String created_at;
	private Double score;
	private String retweeted_id;
	private int retweet_count;
	private String reply_to;
	
	private int favouritesCount;
	private String user_id;
	private String retweeted_user_id;
	private String quoted_status_id;
	private String reply_to_user_id;
	private String quoted_user_id;

	private int retweetedFavouritesCount;

	private transient String sourceFile = "";

	private transient String json;


	//public Map<Integer, Double> tfidf;
	
	private Document(String id)
	{
    	this.id = id.intern();
	}
	
    private void init(String text, long timestamp)
    {
        this.text = text;
        this.timestamp = timestamp;
        this.created_at = null;
        this.score = 0.0;
    	this.retweeted_id = null;
    	this.retweet_count = 0;
    	this.favouritesCount = -1;
    	this.reply_to = null;
        this.words = GlobalData.getInstance().identifyWords(text);
        if(this.words.size()>0 && this.words.get(0).equals("rt"))
        	this.words.remove(0);
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
        res = 1.0 - res;
        if (res<0.0) 
        	res = 0.0;
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

	public List<String> getWords() {
		return words;
	}

	public String getId() {
		return id;
	}

	public DocumentWordCounts bringWordCount()
	{
		DocumentWordCounts wordCount = GlobalData.getInstance().id2wc.get(id);
		return wordCount;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public String getCleanText() {
		return cleanText;
	}

	public int getMultipleWordsTag() {
		return multipleWordsTag;
	}

	public int getHashtags() {
		return hashtags;
	}
	
	public int getSymbols() {
		return symbols;
	}
	
	public int getURLs() {
		return urls;
	}
	public int getUserMentions() {
		return user_mentions;
	}
	
	public void setCreatedAt(String created_at) {
		this.created_at = created_at;
		dirtyOn();
	}
	
	public void setScore(double score) {
		this.score = score;
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
			setNearestDist(tmp);
			setNearestId( rWordCount.getId() );	
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
	public static Document parse(String json, boolean isBasicOnly, String sourceFile)
	{
		json = json.replaceAll("u'", "'").replaceAll("u\"", "\"");
		
		JsonParser jsonParser = new JsonParser();
		
		//System.out.println(json);
		JsonObject jsonObj = null;
		
		try {
			jsonObj = jsonParser.parse(json).getAsJsonObject();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("String: " + json);
			return null;
		}
		
		if(jsonObj.get("text") == null || jsonObj.get("id_str") == null)
			return null;
		
		String text = jsonObj.get("text").getAsString();
		String id = jsonObj.get("id_str").getAsString();
		
		String created_at = jsonObj.get("created_at").getAsString();
		Double score = (jsonObj.get("score") != null) ? jsonObj.get("score").getAsDouble() : 0.0;
		JsonElement element = jsonObj.get("timestamp");
		long timestamp;
		if(element != null)
			timestamp = element.getAsLong();
		else {
			//convert from created_at to timestamp
			//example: Tue Mar 07 23:58:53 +0000 2017
			DateFormat osLocalizedDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
			//DateFormat osLocalizedDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
			timestamp = 0;
			try {
				Date dateTime = osLocalizedDateFormat.parse(created_at);
				timestamp = dateTime.getTime() / 1000;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
			
		//id == "94816822100099073" is for Amy Winhouse event
		Document doc = new Document(id);
		doc.init(text, timestamp);
		doc.json = json;
		doc.sourceFile = sourceFile;
		doc.dirtyOn();
		
		doc.score = score;
        doc.created_at = created_at;
        
        JsonElement userObj = jsonObj.get("user");
        if (userObj.isJsonObject())
        {
	    	doc.user_id = userObj.getAsJsonObject().get("id_str").getAsString();			
        } else if (userObj.isJsonPrimitive())
        {
        	doc.user_id = userObj.getAsString();
        }
        
        if(!isBasicOnly)
		{
        	element = jsonObj.get("entities");
			if(element!=null && !element.isJsonNull())
			{				
				JsonObject obj = element.getAsJsonObject();
				JsonArray hashtagsList = obj.get("hashtags").getAsJsonArray();
				ArrayList<String> tags = new ArrayList<String>();
				for (int h=0; h<hashtagsList.size(); h++)
				{
					String tag = hashtagsList.get(h).getAsJsonObject().get("text").getAsString();
					
					tags.add(tag);
					String[] camelCaseWords = tag.split("(?=[A-Z])");
					doc.multipleWordsTag  += camelCaseWords.length>1 ? 1 : 0;
				}
				
				doc.hashtags = tags.size();
				
				doc.symbols = obj.get("symbols").getAsJsonArray().size();
				doc.urls = obj.get("urls").getAsJsonArray().size();
				doc.user_mentions = obj.get("user_mentions").getAsJsonArray().size();
			}
        	
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
				JsonObject userObj2 = obj.get("user").getAsJsonObject();
	        	doc.quoted_user_id = userObj2.get("id_str").getAsString();
	        	
	        	doc.text += " [" + obj.get("text").getAsString() + "]";
			}
			
			doc.retweet_count = jsonObj.get("retweet_count").getAsInt();
			doc.favouritesCount = jsonObj.get("favorite_count").getAsInt();
	        
			element = jsonObj.get("retweeted_status");
			if(element!=null && !element.isJsonNull())
			{
				JsonObject retweetObj = element.getAsJsonObject();
				doc.retweeted_id = retweetObj.get("id_str").getAsString();
				
				JsonObject userObj2 = retweetObj.get("user").getAsJsonObject();
	        	doc.retweeted_user_id = userObj2.get("id_str").getAsString();
	        	
	        	doc.retweetedFavouritesCount = retweetObj.get("favorite_count").getAsInt();
			}
		}        
        return doc;
	}
	
	public static Document parse01(String json, boolean isBasicOnly)
	{
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();
		
		JsonObject object = (JsonObject) jsonObj.get("object");
		
		if(object.get("summary") == null || object.get("id") == null)
			return null;
		
		String text = object.get("summary").getAsString();
		String id = object.get("id").getAsString();
		
		String created_at = jsonObj.get("postedTime").getAsString();
		JsonElement element = jsonObj.get("timestamp");
		long timestamp;
		if(element != null)
			timestamp = element.getAsLong();
		else {
			//convert from created_at to timestamp
			timestamp = 0;
		}
			
		//id == "94816822100099073" is for Amy Winhouse event
		Document doc = new Document(id);
		doc.init(text, timestamp);
		doc.dirtyOn();
		
        doc.created_at = created_at;
        JsonObject userObj = jsonObj.get("actor").getAsJsonObject();
    	doc.user_id = userObj.get("id").getAsString();	
    	 return doc;
		
	}
	
	public String getCreatedAt() {
		return created_at;
	}
	
	public Double getScore() {
		return score;
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

	public void setUserId(String asString) {
		user_id = asString;
		dirtyOn();
	}

	@Deprecated
	public static Document createOrGetDocument(String json, String sourceFile)
	{
		Document doc = null;
		
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();
		
		if(jsonObj.get("text") == null || jsonObj.get("id_str") == null)
			return null;
		
		String id = jsonObj.get("id_str").getAsString();
		
		id = id.intern();
		synchronized (id) {
			doc = GlobalData.getInstance().id2doc.get(id);
			if(doc == null)
			{
				doc = Document.parse(json, isBasicOnly, sourceFile);
				GlobalData.getInstance().id2doc.put(id, doc);
			}
			
		}
		
		return doc;
	}

	public int getRetweetedFavouritesCount() {
		return retweetedFavouritesCount;
	}

	public void setRetweetedFavouritesCount(int retweetedFavouritesCount) {
		this.retweetedFavouritesCount = retweetedFavouritesCount;
	}
	
	public String getSource() {
		return this.sourceFile;
	}	
	
	public String getJson() {
		return this.json;
	}
}
