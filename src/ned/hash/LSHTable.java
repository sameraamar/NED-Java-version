package ned.hash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import ned.tools.ExecutionHelper;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class LSHTable
{
    private int maxBucketSize ;
    private int hyperPlanesNumber;
    private int dimension;
    public  Boolean fixingDim=false;
    
    private ArrayList<Double>[] hyperPlanes = null;
    private java.util.Dictionary<String, Bucket> buckets = null;
    
    public LSHTable(int hyperPlanesNumber, int dimension, int maxBucketSize)
    {
    	this.hyperPlanesNumber = hyperPlanesNumber;
        buckets = new Hashtable<String, Bucket>();
        this.maxBucketSize = maxBucketSize;
        this.dimension = dimension;
    }

    public void init()
    {
        GenerateHyperPlanes();
    }
    
    private void GenerateHyperPlanes() 
    {
    	if (getHyperPlanes() == null) {
			ArrayList[] arrayLists = new ArrayList[hyperPlanesNumber];
			hyperPlanes = arrayLists;
		}
    	
    	for (int i = 0 ; i<hyperPlanesNumber; i++)
    	{
    		getHyperPlanes()[i] = new ArrayList<Double>();
    		for (int j = 0 ; j<dimension; j++)
    		{
        		getHyperPlanes()[i].add( Utility.randomFill() );
    		}
    	}
    }

     private void FixDimension(int newDimension) 
    {
    	 Runnable task = () -> {
     		
    	    	int nDimension = dimension + GlobalData.getInstance().getParams().dimension_jumps;
    	    //	System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"+nDimension);
    	    //	System.out.println("Start FixDimension new ="+nDimension);
    			Session.getInstance().message(Session.DEBUG, "FixDimension", "Fixing to a new dimension: " + newDimension);
    	    	
    	    	int delta = nDimension - dimension;
    	    	for (int i = 0 ; i<hyperPlanesNumber; i++)
    	    	{
    	    		for (int j = 0 ; j<delta; j++)
    	    		{
    	        		getHyperPlane(i).add( Utility.randomFill() );
    	    		}
    	    	}
    	    	
    	    	dimension = nDimension;
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
    
   

	private String GenerateHashCode(Document doc)
    {
    	StringBuffer st = new StringBuffer();
    	Session session = Session.getInstance();
    	
    	session.message(Session.DEBUG, "GenerateHashCode", doc.getText());
    	Hashtable<Integer, Double> weights = doc.getWeights();
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
	    			tmp += weights.get(j) * getHyperPlane(i).get(j);
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
        String code = GenerateHashCode(doc);
        if (buckets.get(code) == null)
            buckets.put(code, new Bucket(maxBucketSize));

        buckets.get(code).Append(doc);

        return buckets.get(code).getDocIDsList(doc.getId());
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

	private void setMaxBucketSize(int maxBucketSize) {
		this.maxBucketSize = maxBucketSize;
	}

	public ArrayList<Double> getHyperPlane(int i)
	{
		return this.hyperPlanes[i];
	}
	
	public int getHyperPlanesNumber() {
		return hyperPlanesNumber;
	}

	private ArrayList<Double>[] getHyperPlanes() {
		return hyperPlanes;
	}

	public int getDimension() {
		return this.dimension;
	}

}
