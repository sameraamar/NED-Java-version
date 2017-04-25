package ned.hash;


import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import ned.tools.ExecutionHelper;
import ned.tools.HyperPlansManager;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.LRUCache;
import ned.types.Session;

public class LSHTable
{
    private int maxBucketSize ;
    private int hyperPlanesNumber;
    private int dimension;
    private int tableId;
    public  Boolean fixingDim=false;
    
    private HyperPlansManager hyperPlanes;
    private HashMap<Long, LRUCache<String,Document>> buckets = null;
    
    public LSHTable(int tableId,int hyperPlanesNumber, int dimension, int maxBucketSize)
    {
    	this.tableId=tableId;
    	this.hyperPlanesNumber = hyperPlanesNumber;
        buckets = new HashMap<Long, LRUCache<String,Document>>(maxBucketSize);
        this.maxBucketSize = maxBucketSize;
        this.dimension = dimension;
		hyperPlanes = new HyperPlansManager(hyperPlanesNumber, dimension, GlobalData.getInstance().getParams().dimension_jumps);
    }

    public void init()
    {
    	hyperPlanes.init();
    }

     private void FixDimension(int newDimension) 
    {
    	 Runnable task = () -> {
    	    	int nDimension = dimension + GlobalData.getInstance().getParams().dimension_jumps;
    	    
    			Session.getInstance().message(Session.DEBUG, "FixDimension", "Fixing to a new dimension: " + newDimension);
    	    	
    	    	hyperPlanes.fixDim(nDimension);
    	    	
    	    	dimension = hyperPlanes.getDimension();
    	    	synchronized(fixingDim){
    	    		fixingDim=false;
    	   	 	}
    	};
    	    	
    	synchronized(this){	 
    	if(fixingDim) {
    		System.out.println("fixingDim..........");
    		return ;
    	}else{
    		fixingDim=true;
    		ExecutionHelper.asyncAwaitRun(task);
    		//new Thread(task).start();
    	}
    	
   	 }
    	
    	
    	
    }
    
     private long GenerateHashCode(Document doc)
     {
     	
     	boolean[] st = new boolean [hyperPlanesNumber];
     	Session session = Session.getInstance();
     	
     	session.message(Session.DEBUG, "GenerateHashCode", doc.getText());
     	ConcurrentHashMap<Integer, Double> weights = doc.getWeights();
 		if (!this.fixingDim && doc.getDimension() >= this.dimension-3*GlobalData.getInstance().getParams().dimension_jumps) 
        {
            this.FixDimension(doc.getDimension());
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
		if (!this.fixingDim && doc.getDimension() >= this.dimension-3*GlobalData.getInstance().getParams().dimension_jumps) 
		{
			this.FixDimension(doc.getDimension());
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

    public List<String> AddDocument(Document doc)
    {
        long code = GenerateHashCode(doc);
        if (buckets.get(code) == null)
            buckets.put(code, new LRUCache<String,Document>(maxBucketSize));
        LRUCache<String, Document> bucket = buckets.get(code);
        Set<String> ids ;
        List<String> list;
        String excludeId = doc.getId();
        synchronized(bucket){
        	bucket.put(doc.getId(),doc);
        }
        list = new LinkedList<String>();
		ids = buckets.get(code).keySet();
        for (String id : ids) {
			if (excludeId.compareTo(id) <= 0)
				continue;    		
			list.add((String)id);
		} 
		return list;
      

        

		
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
		return this.dimension;
	}

}
