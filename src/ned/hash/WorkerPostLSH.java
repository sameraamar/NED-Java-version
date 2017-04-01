package ned.hash;

import java.util.List;

import ned.types.Document;
import ned.types.DocumentClusteringHelper;

public class WorkerPostLSH implements Runnable {

	private Document doc;
	private List<String> neighbors;

	public WorkerPostLSH(Document doc, List<String> neighbors) {
		this.doc = doc;
		this.neighbors = neighbors;
	}

	@Override
	public void run() 
	{
		DocumentClusteringHelper.postLSHMapping(doc, neighbors);	
	}
	
}