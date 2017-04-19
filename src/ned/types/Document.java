package ned.types;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import ned.tools.ExecutionHelper;

public class Document {
    private String id;
    private String text ;
    private List<String> words;
    //private java.util.Hashtable<Integer, Double> weights ;
    private java.util.Hashtable<Integer, Integer> wordCount ;
    private int dimension;
    
    public int max_idx;
	private long timestamp;
	private String cleanText;
	private String created_at;
	public String nearest;
	public double nearestDist;
	public boolean nearestDetermined;

    public Document(String id, String text, long timestamp)
    {
    	this.id = id;
        this.text = text;
        //this.weights = null;
        this.nearest = null;
    	this.nearestDist = 1.0;
    	this.nearestDetermined = false;
        this.timestamp = timestamp;
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
	
    private static double Norm(Hashtable<Integer, Double> weights) {
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
    
    synchronized public double Norm()
    {        
    	double res = Norm(getWeights());
        return res;
    }

    public static double Distance(Document left, Document right)
    {
    	 List<String> commonWords = DocumentClusteringHelper.intersection(left.getWords(), right.getWords());
     	if(commonWords.isEmpty()){
     		//System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD");
     		return 1;
     	}
     	;
        if (left.getWords().size() > right.getWords().size())
        {
            Document tmp = right;
            right = left;
            left = tmp;
        }
    	
        
        HashSet<Integer> intersection = new HashSet<Integer>();
        Enumeration<Integer> lkeys = left.getWordCount().keys();
        Hashtable<Integer, Integer> rWords = right.getWordCount();
        while( lkeys.hasMoreElements() )
        {
        	Integer key = lkeys.nextElement();
			if (rWords.containsKey(key))
            {
				intersection.add(key);
            }
        }
        Hashtable<Integer, Double> rWeights = right.getWeights();
        Hashtable<Integer, Double> lWeights = left.getWeights();
        
        //Callable<Double> callable = ()->{
            double res = 0;

       
        double norms = Norm(rWeights) * Norm(lWeights);


        double dot = 0.0;
        
        //right.getWeights().keySet().retainAll(left.getWeights().keySet())
        
        for (Integer k : intersection) {
            dot += rWeights.get(k) * lWeights.get(k);
		}
        
        res = dot / norms; 
        return 1.0 - res;
        /*
     	 };
 		
 		 Future<?> f=ExecutionHelper.asyncAwaitRun(callable);
 		 try {
			double res= (Double) f.get();
			//System.out.println("res="+res);
			return res;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 		 return 0;
 		 */
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
		//if (weights == null) 
		//{
			//synchronized(this) {
				//if (weights!=null) //some other process already handlde
				//	return weights;
				
				Hashtable<Integer, Double> tmp = new java.util.Hashtable<Integer, Double>();
				
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


 
}
