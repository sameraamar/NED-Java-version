package ned.main;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

import ned.hash.LSHForestAbstract;
import ned.types.Document;
import ned.types.DocumentClusteringHelper;
import ned.types.GlobalData;

public class WorkerThread implements Runnable 
{
	private Document doc;
	private LSHForestAbstract forest;
	
    public WorkerThread(LSHForestAbstract forest, Document doc)
    {
        this.doc = doc;
        this.forest = forest;
    }

    @Override
    public void run() 
    {
        processCommand();
    }
    
    private void processCommand() 
    {
    	GlobalData gd = GlobalData.getInstance();

    	//Step 1: (after parse) should be in main thread (chronological order)
    	gd.addDocument(doc);
		
		if (doc.getWords().size() > 0)
		{
	        //Step 2: Can be parallelized per table
			long base = System.currentTimeMillis();
			//List<String> set  = forest.addDocument(this.doc);
			long time1 = System.currentTimeMillis() ;
			
			long serialD=time1-base;
			
			base = System.currentTimeMillis();
			List<String> set1  = forest.addDocument5(this.doc);
			long time2 = System.currentTimeMillis();
			
			long parallelD=time2-time1;
			/*if(parallelD / serialD>1){
				System.out.println("serial -parallel : " + (parallelD / serialD));
			}*/

			
			
			//Step 2.5: (nice to have) synchronized milestone

			//Step 3: post LSH mapping
			
			gd.executer.postLSH(doc, Collections.synchronizedList(set1));
	        //DocumentClusteringHelper.postLSHMapping(doc, set);
		}
		else{
			this.doc.setNearestDetermined(true);
	        //update the document in redis with the update doc //setNearestDetermined
	        gd.setDocumentFromRedis("id2document", doc.getId(), doc);
		}
		
    }

    public Document getDocument()
	{
        return this.doc;
    }
}