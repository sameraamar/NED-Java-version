package ned.hash;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import ned.types.Document;

public class LSHTableThread extends Thread 
{
	ConcurrentLinkedQueue<Document> queueIn ;
	ConcurrentLinkedQueue<List<String>> queueOut ;
	boolean stop;
	
	LSHTable lsh;
	
	public LSHTableThread(int id,int hyperPlanesNumber, int dimension, int maxBucketSize)
	{
		this.lsh = new LSHTable(id,hyperPlanesNumber, dimension, maxBucketSize);
		this.stop = false;
		this.queueIn = new ConcurrentLinkedQueue<Document>(); 
		this.queueOut= new ConcurrentLinkedQueue<List<String>>();
	}
	
	public void addDocumentRequest(Document doc)
	{
		queueIn.add(doc);
	}
	
	public List<String> addDocumentResponse()
	{
		while(queueOut.isEmpty())
			;
		return queueOut.poll();
	}
	
	public void finish()
	{
		this.stop = true;
	}
	
	@Override
	public void run()
	{
		while (true)
		{
			if (this.stop)
				break;
			Document doc = queueIn.poll();
			
			if (doc == null)
				continue;
			
			List<String> res = this.lsh.AddDocument(doc);
			queueOut.add(res);			
		}
	}

}
