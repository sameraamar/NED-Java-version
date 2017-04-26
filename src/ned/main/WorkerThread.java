package ned.main;

import java.util.Hashtable;
import java.util.List;

import ned.hash.DocumentHandler;
import ned.hash.LSHForest;
import ned.types.Document;
import ned.types.DocumentClusteringHelper;
import ned.types.GlobalData;

public class WorkerThread implements Runnable 
{
	private static Boolean locker = true;
	private static long time = 0;
	private static int counter = 0;
	
	private Document doc;
	private LSHForest forest;
	
    public WorkerThread(LSHForest forest, Document doc)
    {
        this.doc = doc;
        this.forest = forest;
    }

    private static void updateTime(long ms) 
    {
    	synchronized(locker)
    	{
    		counter++;
    		time+=ms;
    	}
    }
    
    public static double avegTime()
    {
    	synchronized (locker) {
			return (1.0*time/counter);
		}
    }
    

	public static void resetCounter() {
		synchronized(locker)
    	{
    		counter = 0;
    		time = 0;
    	}
	}
    
    @Override
    public void run() 
    {
    	long base = System.currentTimeMillis();
        processCommand();
        updateTime(System.currentTimeMillis() - base);
    }

    private void processCommand() 
    {
		if (doc.getWords().size() == 0)
		{
	        this.doc.setNearestDetermined( true);
			return;
		}
    	//Hashtable<Integer, Double> weights = doc.getWeights();
		
		boolean breakme = false;
		if(doc.getId().equals("86417673814151168"))
			breakme = false;
		
    	List<String> set = forest.addDocument(this.doc);
		
    	DocumentClusteringHelper.postLSHMapping(this.doc, set);
    	this.doc.setNearestDetermined( true);

        //DocumentClusteringHelper.mapToClusterHelper(doc);
    }

    public Document getDocument()
	{
        return this.doc;
    }

	public void preRun() {
		GlobalData.getInstance().addDocument(doc);
	}

}