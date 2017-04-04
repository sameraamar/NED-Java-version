package ned.types;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Document implements Serializable{
    //private double cacheNorm;
    private String id;
    private String text ;
    private List<String> words;
    //private Hashtable<Integer, Double> weights ;
    private Hashtable<Integer, Integer> wordCount ;
    private int maxWordIndex;
    
    public int max_idx;
	private String cleanText;
	//document attributes
	private long timestamp;
	private String created_at;
	private String retweeted_id;
	private int retweet_count;
	private String reply_to;
	private String user_id;
	
	private String nearest;
	private double nearestDist;
	private boolean nearestDetermined;
	private int favouritesCount;
	private boolean cacheOn;
	private Hashtable<Integer, Double> cacheWeights;
	private double cacheNorm;

    public Document(String id, String text, long timestamp)
    {
    	this.id = id;
        this.text = text;
        this.cacheOn = false;
        this.cacheNorm = -1;
        this.cacheWeights = null;
        this.nearest = null;
    	this.nearestDist = 1.0;
    	this.nearestDetermined = false;
        this.timestamp = timestamp;
        this.created_at = null;
    	this.retweeted_id = null;
    	this.retweet_count = 0;
    	this.favouritesCount = -1;
    	this.reply_to = null;
        this.words = GlobalData.getInstance().identifyWords(text);
		this.wordCount = new Hashtable<Integer, Integer>();
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
	
    public double Norm()
    {
        if (cacheOn && cacheNorm >= 0.0)
            return cacheNorm;

        double res = 0;
        res=getWeights().entrySet().parallelStream()
        .mapToDouble(entry->
        	
        	entry.getValue().doubleValue()*entry.getValue().doubleValue()
           
        ).sum();
        
        
        /*
        Enumeration<Double> values = getWeights().elements();
        
        while(values.hasMoreElements())
        {
			double v = values.nextElement();
            res += v * v;
        }
*/
        res = Math.sqrt(res);
        
        if (cacheOn)
        	cacheNorm = res;
        
        return res;
    }

    public static double Distance(Document left, Document right)
    {
    	double res = 0.0;
    	 
    	 List<String> commonWords = DocumentClusteringHelper.intersection(left.getWords(), right.getWords());
    	if(commonWords.isEmpty()){
    		//System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
    		return 1;
    	}
        Hashtable<Integer, Double> leftWeights = left.getWeights();
		Hashtable<Integer, Double> rightWeights = right.getWeights();
		if (rightWeights.size() > leftWeights.size())
        {
			Hashtable<Integer, Double> tmp = leftWeights;
            leftWeights = rightWeights;
            rightWeights = tmp;
        }
		
    	
    	
        double dot = 0.0;
        double norm1 = right.Norm();
        double norm2 = left.Norm();
        double norms = norm1 * norm2;
        
        //right.getWeights().keySet().retainAll(left.getWeights().keySet())
        
        Enumeration<Integer> keys = rightWeights.keys();
        while( keys.hasMoreElements() )
        {
        	Integer key = keys.nextElement();
			if (leftWeights.containsKey(key))
            {
                dot += rightWeights.get(key) * leftWeights.get(key);
            }

        }

        res = dot / norms; 
        
        if(norms < 0)
        	norms = norms;
        
        res = 1.0 - res;
       
		int doubleScale = GlobalData.getInstance().getParams().DOUBLE_SCALE;

		try {
		if(doubleScale>0)
			res = BigDecimal.valueOf(res).setScale(doubleScale, RoundingMode.HALF_UP).doubleValue();
		} catch (java.lang.NumberFormatException e)
		{
			e.printStackTrace();
		}
		if(res < 0.0)
			res = res * 1;
		
		return res;
    }
    
    public static double Distance(Document left2, double norm, Hashtable<Integer, Double> weights, Document right)
    {
        double res = 0;

        double norms = right.Norm() * norm;

        double dot = 0.0;
        
        //right.getWeights().keySet().retainAll(left.getWeights().keySet())
        
        Hashtable<Integer, Double> a = weights;
        Hashtable<Integer, Double> b = right.getWeights();
        
        if(a.size() > b.size())
        {
        	Hashtable<Integer, Double> c = a;
        	a = b;
        	b = c;
        }
        
        Enumeration<Integer> keys = a.keys();
        
        while( keys.hasMoreElements() )
        {
        	Integer key = keys.nextElement();
			if (b.containsKey(key))
            {
                dot += b.get(key) * a.get(key);
            }
        }

        res = dot / norms; 
        res = 1.0 - res;
        
		int doubleScale = GlobalData.getInstance().getParams().DOUBLE_SCALE;
		if(doubleScale>0)
			res = BigDecimal.valueOf(res).setScale(doubleScale, RoundingMode.HALF_UP).doubleValue();
		
		return res;
    }
    
    public void calcWeights(Hashtable<Integer, Double> weights)
    {
		if (weights == null || weights.isEmpty()) 
		{
			GlobalData gd = GlobalData.getInstance();
			gd.calcWeights(this, weights);
		}
    }
    
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("{\"").append(id).append("\":\"").append(text);
    	//sb.append(weights)
    	sb.append("\"}");
    	return sb.toString();
    }

	public String getText() {
		return text;
	}

	public Hashtable<Integer, Double> getWeights() 
	{
		if(!cacheOn)
		{
			Hashtable<Integer, Double> tmp = new Hashtable<Integer, Double>();
			calcWeights(tmp);
			return tmp;
		}
		
		if(cacheWeights == null)
		{
			synchronized (this) {
				if(cacheWeights == null)
				{
					Hashtable<Integer, Double> tmp = new Hashtable<Integer, Double>();
					calcWeights(tmp);
					cacheWeights = tmp;
				}
			}	
		}
		
		return cacheWeights;
		
	}

	public void setCacheFlag(boolean flag)
	{
		if(!flag)
		{
			cacheNorm = -1;
			cacheWeights = null;
		}
		
		cacheOn = flag;
	}
	
	public List<String> getWords() {
		return words;
	}

	public String getId() {
		return id;
	}

	public int getMaxWordIndex() {
		return maxWordIndex;
	}

	Hashtable<Integer, Integer> getWordCount() {		
		return wordCount;
	}

	void setWordCount(Hashtable<Integer, Integer> wordCount) {
		this.wordCount = wordCount;
	}

	void setMaxWordIndex(int maxIndex) {
		this.maxWordIndex = maxIndex;
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

	public void updateNearest(String rightId) 
	{
		if(rightId == null)
			return;
		
		Document right = GlobalData.getInstance().getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);
		updateNearest(right);
	}

	public void updateNearest(Document right) {
		if(right == null)
			return;
		
		double tmp = Document.Distance(this, right);
		synchronized (this) 
		{
			if (nearest==null || tmp < nearestDist)
			{
				nearestDist = tmp;
				nearest = right.getId();
			}
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
