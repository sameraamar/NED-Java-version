package ned.types;

import java.util.Enumeration;
import java.util.List;

public class Document {
    private double cacheNorm;
    private String id;
    private String text ;
    private List<String> words;
    private java.util.Hashtable<Integer, Double> weights ;
    private java.util.Hashtable<Integer, Integer> wordCount ;
    private int dimension;
    
    public int max_idx;
	private long timestamp;
	private String cleanText;
	private String created_at;

    public Document(String id, String text, long timestamp)
    {
    	this.id = id;
        this.text = text;
        this.weights = null;
        this.timestamp = timestamp;
        this.cacheNorm = -1;
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
		super.finalize();
	}
	
    synchronized public double Norm()
    {
        if (cacheNorm >= 0)
            return cacheNorm;

        double res = 0;
        Enumeration<Double> values = getWeights().elements();
        
        while(values.hasMoreElements())
        {
			double v = values.nextElement();
            res += v * v;
        }

        res = cacheNorm = Math.sqrt(res);
        return res;
    }

    public static double Distance(Document left, Document right)
    {
        double res = 0;

        double norms = right.Norm() * left.Norm();

        if (right.getWeights().size() > left.getWeights().size())
        {
            Document tmp = left;
            left = right;
            right = tmp;
        }

        double dot = 0.0;
        
        //right.getWeights().keySet().retainAll(left.getWeights().keySet())
        
        Enumeration<Integer> keys = right.getWeights().keys();
        while( keys.hasMoreElements() )
        {
        	Integer key = keys.nextElement();
			if (left.getWeights().containsKey(key))
            {
                dot += right.getWeights().get(key) * left.getWeights().get(key);
            }

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

	public java.util.Hashtable<Integer, Double> getWeights() {
		if (weights == null) 
		{
			synchronized(this) {
				if (weights!=null) //some other process already handlde
					return weights;
				
				weights = new java.util.Hashtable<Integer, Double>();
				
				GlobalData gd = GlobalData.getInstance();
				gd.addDocument(this);
				gd.calcWeights(this, weights);
			}
		}
		return weights;
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

	java.util.Hashtable<Integer, Integer> getWordCount() {
		if (wordCount == null)
			wordCount = new java.util.Hashtable<Integer, Integer>();
		
		return wordCount;
	}

	void setWordCount(java.util.Hashtable<Integer, Integer> wordCount) {
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


 
}
