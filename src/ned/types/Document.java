package ned.types;

import java.util.List;

public class Document {
    private double cacheNorm;
    private String id;
    private String text ;
    private String[] words;
    private Dict weights ;
    private Dict wordCount ;
    private int dimension;
    
    public int max_idx;
	private long timestamp;
	private String cleanText;

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

    public double Norm()
    {
        if (cacheNorm >= 0)
            return cacheNorm;

        double res = 0;
        for (double v : getWeights().values()) {
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
        for (Integer key : right.getWeights().keySet()) {
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

	public Dict getWeights() {
		if (weights == null) 
		{
			weights = new Dict();
			
			GlobalData gd = GlobalData.getInstance();
			gd.addDocument(this);
			gd.calcWeights(this, weights);
		}
		return weights;
	}

	public String[] getWords() {
		return words;
	}

	public String getId() {
		return id;
	}

	public int getDimension() {
		return dimension;
	}

	Dict getWordCount() {
		if (wordCount == null)
			wordCount = new Dict();
		
		return wordCount;
	}

	void setWordCount(Dict wordCount) {
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


 
}
