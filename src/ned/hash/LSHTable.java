package ned.hash;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class LSHTable
{
    private int maxBucketSize ;
    private int hyperPlanesNumber;
    private int dimension;
    
    private ArrayList<Double>[] hyperPlanes = null;
    private java.util.Dictionary<Long, Bucket> buckets = null;
    
    public LSHTable(int hyperPlanesNumber, int dimension, int maxBucketSize)
    {
    	this.hyperPlanesNumber = hyperPlanesNumber;
        buckets = new Hashtable<Long, Bucket>();
        this.maxBucketSize = maxBucketSize;
        this.dimension = dimension;
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

    synchronized private void FixDimension(int newDimension) 
    {
    	if (dimension > newDimension)
    		return;
    	
    	newDimension = dimension + GlobalData.getInstance().getParams().dimension_jumps;
    	
		Session.getInstance().message(Session.DEBUG, "FixDimension", "Fixing to a new dimension: " + newDimension);
    	
    	int delta = newDimension - dimension;
    	for (int i = 0 ; i<hyperPlanesNumber; i++)
    	{
    		for (int j = 0 ; j<delta; j++)
    		{
        		getHyperPlane(i).add( Utility.randomFill() );
    		}
    	}
    	
    	dimension = newDimension;
    }
    
    private long GenerateHashCode(Document doc)
    {
    	
    	boolean[] st = new boolean[hyperPlanesNumber];
    	Session session = Session.getInstance();
    	
    	session.message(Session.DEBUG, "GenerateHashCode", doc.getText());
    	Hashtable<Integer, Double> weights = doc.getWeights();
		if (doc.getMaxWordIndex() >= this.dimension) 
		{
			this.FixDimension(doc.getMaxWordIndex());
		}
		
		int doubleScale = GlobalData.getInstance().getParams().DOUBLE_SCALE;
    	for (int i = 0 ; i<hyperPlanesNumber; i++)
    	{
    		double tmp = 0;
    		int index=i;
    		// ForkJoinPool forkJoinPool = new ForkJoinPool();
    		//Future<Double> future=forkJoinPool.submit(() ->{
    		tmp+= weights.keySet().parallelStream().mapToDouble(j->weights.get(j) * getHyperPlane(index).get(j)).sum();
    		//});
    		//forkJoinPool.shutdown();
    		/*
			for (Integer j : weights.keySet()) 
    		{
    			tmp += weights.get(j) * getHyperPlane(i).get(j);
    		}

    		try {
				tmp+=(Double)future.get();
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
			if(doubleScale>0)
				tmp = BigDecimal.valueOf(tmp).setScale(doubleScale, RoundingMode.HALF_UP).doubleValue();
			
    		st[i]=( tmp>=0 ? true : false );
    	}
    	long res=convertBooleanArrayToLong(st);
        return res;
    }
    
    private long convertBooleanArrayToLong(boolean[] st){
    	long res=0;
    	for (int i = 0 ; i<hyperPlanesNumber; i++){
    		if(st[i]){
    			res+=Math.pow(10,i);
    		}
    		
    	}
		return res;
    	
    }
    
    
    private String GenerateHashCode0(Document doc)
    {
    	StringBuffer st = new StringBuffer();
    	Session session = Session.getInstance();
    	
    	session.message(Session.DEBUG, "GenerateHashCode", doc.getText());
    	Hashtable<Integer, Double> weights = doc.getWeights();
		if (doc.getMaxWordIndex() >= this.dimension) 
		{
			this.FixDimension(doc.getMaxWordIndex());
		}
		
		int doubleScale = GlobalData.getInstance().getParams().DOUBLE_SCALE;
    	for (int i = 0 ; i<hyperPlanesNumber; i++)
    	{
    		double tmp = 0;

			for (Integer j : weights.keySet()) 
    		{
    			tmp += weights.get(j) * getHyperPlane(i).get(j);
    		}

			if(doubleScale>0)
				tmp = BigDecimal.valueOf(tmp).setScale(doubleScale, RoundingMode.HALF_UP).doubleValue();
			
    		st.append( tmp>=0 ? "1" : "0" );
    	}
    	
        return st.toString();
    }


    public List<String> AddDocument(Document doc)
    {
        long code = GenerateHashCode(doc);
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
