package ned.main;

import java.util.Hashtable;
import java.util.List;

import ned.hash.DocumentHandler;
import ned.hash.LSHForest;
import ned.types.Document;
import ned.types.DocumentClusteringHelper;

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
			return;
		
    	Hashtable<Integer, Double> weights = doc.getWeights();
		
    	List<String> set = forest.AddDocument(this.doc);

        Object[] candidate = DocumentClusteringHelper.postLSHMapping(this.doc, set);
        
        Document nearest = (Document)candidate[0];
        Double dist = (Double)candidate[1];
        
        DocumentClusteringHelper.mapToClusterHelper(doc, nearest, dist);
    }

    public Document getDocument()
	{
        return this.doc;
    }
}