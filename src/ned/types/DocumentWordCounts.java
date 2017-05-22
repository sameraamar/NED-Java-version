package ned.types;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

public class DocumentWordCounts implements DirtyBit, Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 971781535924421298L;
	
	String id;
    private Map<Integer, Integer> wordCount ;
    private boolean dirtyBit;

    public DocumentWordCounts(String id, Map<Integer, Integer> wordCount )
    {
    	this.id = id.intern();
    	this.wordCount = wordCount;
    	dirtyOn();
    }
    
    public Map<Integer, Integer> getWordCount()
    {
    	return this.wordCount;
    }
    
    public String getId()
    {
    	return this.id;
    }
    

	public Map<Integer, Double> getWeights(Map<Integer, Double> word2idf) {
		//if (weights == null) 
		//{
			//synchronized(this) {
				//if (weights!=null) //some other process already handlde
				//	return weights;
				
		Hashtable<Integer, Double> tmp = new Hashtable<Integer, Double>();
				
				GlobalData gd = GlobalData.getInstance();
				//gd.addDocument(this);
				gd.calcWeights(this, tmp, word2idf);
			//}
		//}
		return tmp;
	}
    
	@Override
	public boolean isDirty() {
		return dirtyBit;
	}

	@Override
	public void dirtyOff() {
		dirtyBit = false;
	}

	@Override
	public void dirtyOn() {
		dirtyBit = true;
	}
}
