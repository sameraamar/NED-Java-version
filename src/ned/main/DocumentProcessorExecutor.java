package ned.main;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import ned.hash.LSHForest;
import ned.types.Document;

public class DocumentProcessorExecutor {
	private ExecutorService executor;
	private int number_of_threads;
	LSHForest forest;
	
	public DocumentProcessorExecutor(LSHForest forest, int number_of_threads)
	{
		this.forest = forest;
		this.number_of_threads = number_of_threads;
		executor = Executors.newFixedThreadPool(number_of_threads);		
	}
	
	public void submit(Document doc, int idx)
	{
		WorkerThread worker = new WorkerThread(forest, doc, idx);
		worker.preRun();
		//ExecutionHelper.asyncRun(worker);
		//worker.run();
		executor.execute(worker);
	}
	
	public boolean await()
	{
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public void shutdown()
	{
		shutdown(executor);
	}
	
	private void shutdown(ExecutorService executor)
	{
		executor.shutdown();
        while (!executor.isTerminated()) 
        {
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
        System.out.println("Finished all threads");
	}

	public void refresh()
	{
		System.out.println("Refresh the thread-executor manager!");
		ExecutorService temp = executor;
		executor = Executors.newFixedThreadPool(number_of_threads);
		temp.shutdown();
	}
	
	public ExecutorService getExecutor() 
	{
		return executor;
	}
}
