package ned.main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import ned.hash.LSHForestAbstract;
import ned.types.Document;
import ned.types.DocumentClusteringHelper;
import ned.types.GlobalData;

public class WorkerThread implements Runnable 
{
	private Document doc;
	private LSHForestAbstract forest;
	ArrayList<Future<List<String>>> neighbors;
	
    public WorkerThread(LSHForestAbstract forest, Document doc)
    {
        this.doc = doc;
        this.forest = forest;
    }

    @Override
    public void run() 
    {
    	GlobalData gd = GlobalData.getInstance();
		if (doc.getWords().size() > 0)
		{
	        //Step 2: Can be parallelized per table
			long base = System.currentTimeMillis();
			List<String> set  = forest.addDocument(this.doc);
			//List<String> set = forest.processResults(neighbors, doc);
			long time1 = System.currentTimeMillis() - base;
			
			base = System.currentTimeMillis();
			//List<String> set  = forest.addDocument4(this.doc);
			long time2 = System.currentTimeMillis() - base;

			//System.out.println("parallel - serial: " + (time2-time1));
			
			//Step 2.5: (nice to have) synchronized milestone

			//Step 3: post LSH mapping
			

			//gd.executer.postLSH(doc, set);
	        DocumentClusteringHelper.postLSHMapping(doc, set);
		}
		else{
			this.doc.setNearestDetermined(true);
	        //update the document in redis with the update doc //setNearestDetermined
	        gd.setDocumentFromRedis(GlobalData.ID2DOCUMENT, doc.getId(), doc);
		}
	}
    
    public void preRun()
    {
    	GlobalData gd = GlobalData.getInstance();

    	//Step 1: (after parse) should be in main thread (chronological order)
    	gd.addDocument(doc);
    	
        //Step 2: Can be parallelized per table
		//neighbors  = forest.addDocumentFuture(this.doc);
    }
    
    public Document getDocument()
	{
        return this.doc;
    }
}