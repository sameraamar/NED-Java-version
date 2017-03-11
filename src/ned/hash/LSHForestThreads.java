package ned.hash;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import ned.types.Document;

public class LSHForestThreads extends Thread {
	LSHForest lshForest;
	ConcurrentLinkedQueue<Document> queueIn ;
	ConcurrentLinkedQueue<List<String>> queueOut ;
	
	public LSHForestThreads(int tablesNumer, int hyperPlanesNumber, int dimension, int maxBucketSize) 
	{
		this.lshForest = new LSHForest(tablesNumer, hyperPlanesNumber, dimension, maxBucketSize);
		
		this.queueIn = new ConcurrentLinkedQueue<Document>(); 
		this.queueOut= new ConcurrentLinkedQueue<List<String>>();
	}


	public void addDocumentRequest(Document doc)
	{
		synchronized (queueIn) 
		{
			queueIn.add(doc);
			queueIn.notify();
		}
	}
	
	public List<String> addDocumentResponse()
	{
		synchronized (queueOut) 
		{
			if (queueOut.isEmpty())
			{
				try {
					queueOut.wait();
				} 
				catch (InterruptedException e) 
				{
				}
			}
		}
		return queueOut.poll();
	}
	
	@Override 
	public void run()
	{
		try {
			doRun();
		} catch(Throwable thr) {
			thr.printStackTrace();
		}
		System.out.println("Thread is going down");
	}
	public void doRun()
	{
		
		System.out.println("Thread started!");
		while (true)
		{
			synchronized (queueIn) 
			{
				try {
					queueIn.wait();
				} catch (InterruptedException e) 
				{}
			}
			
			Document doc = queueIn.poll();
			if (doc != null)
			{			
				List<String> list = this.lshForest.AddDocument(doc);
					
				synchronized (queueOut) 
				{
					queueOut.add(list);
					queueOut.notify();
				} 
			}			
		}
		
		
	}
	
	
}
