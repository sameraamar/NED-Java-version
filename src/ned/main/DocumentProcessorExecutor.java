package ned.main;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import ned.hash.LSHForestAbstract;
import ned.hash.LSHTable;
import ned.hash.WorkerLSHTable;
import ned.hash.WorkerPostLSH;
import ned.types.Document;

public class DocumentProcessorExecutor {
	private ExecutorService executor;
	LSHForestAbstract forest;
	
	public DocumentProcessorExecutor(LSHForestAbstract forest, int number_of_threads)
	{
		this.forest = forest;
		executor = Executors.newFixedThreadPool(number_of_threads);
	}
	
	public void submit(Document doc)
	{
		Runnable worker = new WorkerThread(forest, doc);
		worker.run();
		//executor.execute(worker);
	}
	
	public Future<List<String>> addToLSH(LSHTable lshTable, Document doc) 
	{
		WorkerLSHTable worker = new WorkerLSHTable(lshTable, doc);
		Future<List<String>> neighbors = executor.submit(worker);
		return neighbors;
	}
	
	public void postLSH(Document doc, List<String> neighbors)
	{
		Runnable worker = new WorkerPostLSH(doc, neighbors);
		executor.execute(worker);
	}
	
	public void await()
	{
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void shutdown()
	{
        System.out.println("Executer.shutdown(): stopping processing");

		executor.shutdown();
        while (!executor.isTerminated()) 
        {
        }
        System.out.println("Executer.shutdown(): Finished all threads");
	}

	public ExecutorService getExecutor() 
	{
		return executor;
	}

}