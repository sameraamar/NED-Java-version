package ned.hash;


import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import ned.tools.ArrayFixedSize;
import ned.tools.ExecutionHelper;
import ned.tools.HyperPlansManager;
import ned.tools.RecentManager;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.LRUCache;
import ned.types.Session;

public class LSHTable
{
    private int maxBucketSize ;
    private int hyperPlanesNumber;
    //private int dimension;
    private int tableId;
    public  Boolean fixingDim=false;
    
    private HyperPlansManager hyperPlanes;
    private HashMap<Long, ArrayFixedSize> buckets = null;
    
    public LSHTable(int tableId,int hyperPlanesNumber, int dimension, int maxBucketSize)
    {
    	this.tableId=tableId;
    	this.hyperPlanesNumber = hyperPlanesNumber;
        buckets = new HashMap<Long, ArrayFixedSize>(maxBucketSize);
        this.maxBucketSize = maxBucketSize;
		hyperPlanes = new HyperPlansManager(hyperPlanesNumber, dimension, GlobalData.getInstance().getParams().dimension_jumps);
    }

    public void init()
    {
    	hyperPlanes.init();
    }
    
     private long GenerateHashCode(Document doc)
     {
     	
     	boolean[] st = new boolean [hyperPlanesNumber];
     	Session session = Session.getInstance();
     	
     	session.message(Session.DEBUG, "GenerateHashCode", doc.getText());
     	ConcurrentHashMap<Integer, Double> weights = doc.getWeights();
     	hyperPlanes.fixDim(doc.getDimension());
 		
 		for (int i = 0 ; i<hyperPlanesNumber; i++)
    	{
    		double tmp = 0;
    		//Samer: remove syncronized
    		//synchronized (weights) {
				for (Integer j : weights.keySet()) 
	    		{
					try {
	    			tmp += weights.get(j) * hyperPlanes.get(i, j);
					} catch(ArrayIndexOutOfBoundsException e)
					{
						System.out.println( "doc dimension: " + doc.getDimension() + " this.fixingDim = ");
						throw e;
					}
	    		}
    		//}
			session.message(Session.DEBUG, "GenerateHashCode", ""+ tmp);

			st[i]=( tmp>=0 ? true : false );
    		session.message(Session.DEBUG, "GenerateHashCode", "\nLOG");
    	}
 		long res=convertBooleanArrayToLong(st);
         return res;
     }
     
     private long convertBooleanArrayToLong(boolean[] st){
     	long res=0;
     	for (int i = 0 ; i<hyperPlanesNumber; i++){
     		if(st[i]){
     			res+=Math.pow(2,i);
     		}
     		
     	}
 		return res;
     	
     }

	private String GenerateHashCode_orig(Document doc)
    {
    	StringBuffer st = new StringBuffer();
    	Session session = Session.getInstance();
    	
    	session.message(Session.DEBUG, "GenerateHashCode", doc.getText());
    	ConcurrentHashMap<Integer, Double> weights = doc.getWeights();
		if (!this.fixingDim && doc.getDimension() >= hyperPlanes.getDimension()-3*GlobalData.getInstance().getParams().dimension_jumps) 
		{
			hyperPlanes.fixDim(doc.getDimension());
		}
		
    	for (int i = 0 ; i<hyperPlanesNumber; i++)
    	{
    		double tmp = 0;
    		//Samer: remove syncronized
    		//synchronized (weights) {
				for (Integer j : weights.keySet()) 
	    		{
	    			tmp += weights.get(j) * hyperPlanes.get(i, j);
	    		}
    		//}
			session.message(Session.DEBUG, "GenerateHashCode", ""+ tmp);

    		st.append( tmp>=0 ? "1" : "0" );
    		session.message(Session.DEBUG, "GenerateHashCode", "\nLOG");
    	}
    	
        return st.toString();
    }

    public ArrayFixedSize AddDocument(Document doc)
    {
        long code = GenerateHashCode(doc);
        ArrayFixedSize bucket = buckets.get(code);
        if (bucket == null)
        {
        	//Rami -- watch out this sync. How can we avoid it?
        	//although i am not sure it will cause any issue since we create the bucket once per code
        	synchronized (buckets) {
        		bucket = buckets.get(code);
        		if (bucket == null) //still null
        		{
        			buckets.put(code, new ArrayFixedSize(maxBucketSize));
        			bucket = buckets.get(code);
        		}
        	}
        }

        List<String> list;
        String excludeId = doc.getId();
        
        if(bucket == null)
        	System.out.println("************************************************************** Somethign strange (bucket is null)!");
        
        if(doc == null)
        	System.out.println("************************************************************** Somethign strange (doc is null)!");
        
        bucket.add(doc.getId());
        	
        return bucket;
        	/*list = new LinkedList<String>();
        	
     		for(int i=0; i<bucket.size(); i++)
     		{
     			String id = bucket.get(i);
     			if (excludeId.compareTo(id) <= 0)
     				continue;    		
     			list.add((String)id);
     		} 
     		return list;*/
	}
    
    public String toString() 
    {
    	StringBuffer sb = new StringBuffer();
    	/*
    	for (ArrayList<Double> arrayList : hyperPlanes) {
    		sb.append("[");
			for (Double d : arrayList) {
				sb.append(d + ",");
			}
			sb.append("]\n");
    	}
    	*/
    	sb.append(buckets.toString().replaceAll("}], ", "}],\n"));
    	sb.append("\n");
    	return sb.toString();
    }

	public int getMaxBucketSize() {
		return maxBucketSize;
	}

	public int getHyperPlanesNumber() {
		return hyperPlanesNumber;
	}

	public int getDimension() {
		return hyperPlanes.getDimension();
	}

}
