package ned.tools;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class MapToTopicExecuter {
	private ExecutorService executor;
	private Map<String, JudgmentEntry> map;
	
	public MapToTopicExecuter(SortedMap<String, JudgmentEntry> map, int number_of_threads)
	{
		executor = Executors.newFixedThreadPool(number_of_threads);		
		this.map = map;
	}
	
	public void submit(String line)
	{
		Runnable worker = new MapToTopicWorker(map, line);
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
        }
        System.out.println("Finished all threads");
	}

}