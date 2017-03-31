package ned.tools;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

abstract public class ProcessorExecutor {

	protected ExecutorService executor;

	protected ProcessorExecutor(int number_of_threads) 
	{
		executor = Executors.newFixedThreadPool(number_of_threads);
	}

	public ExecutorService getExecutor()
	{
		return executor;
	}
	
	abstract protected Runnable createWorker(String line);

	public void submit(String line) {
		Runnable worker = createWorker(line);
		executor.execute(worker);
	}

	public void await() {
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void shutdown() {
		executor.shutdown();
	    while (!executor.isTerminated()) 
	    {
	    }
	    System.out.println("Finished all threads");
	}

}