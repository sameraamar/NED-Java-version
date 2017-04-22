package ned.main;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import ned.hash.LSHForest;
import ned.tools.ExecutionHelper;
import ned.types.Document;

public class DocumentProcessorExecutor {
	private ExecutorService executor;
	LSHForest forest;
	
	public DocumentProcessorExecutor(LSHForest forest, int number_of_threads)
	{
		this.forest = forest;
		executor = Executors.newFixedThreadPool(number_of_threads);		
	}
	
	public void submit(Document doc)
	{
		WorkerThread worker = new WorkerThread(forest, doc);
		worker.preRun();

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

	public ExecutorService getExecutor() 
	{
		return executor;
	}

	
	/*GlobalData gd = GlobalData.getInstance();
	gd.getParams().number_of_threads
	public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 10; i++) {
            Runnable worker = new WorkerThread("" + i);
            executor.execute(worker);
          }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
        
    }*/

}