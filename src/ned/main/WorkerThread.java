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
	private Document doc;
	private LSHForest forest;
	
    public WorkerThread(LSHForest forest, Document doc)
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
		if (doc.getWords().size() == 0)
		{
	        this.doc.nearestDetermined = true;
			return;
		}
    	//Hashtable<Integer, Double> weights = doc.getWeights();
		
    	List<String> set = forest.AddDocument(this.doc);

        DocumentClusteringHelper.postLSHMapping(this.doc, set);
        this.doc.nearestDetermined = true;
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