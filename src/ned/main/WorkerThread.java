package ned.main;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

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

    
    private void testParallelStream()
    {
		/*
		 * final Set<String> thNames = Collections.synchronizedSet(new
		 * HashSet<String>());
		 * 
		 * 
		 * for (int i = 0; i < 1_000_000; ++i) { forkJoinPool.submit(() -> { try
		 * { Thread.sleep(1); thNames.add(Thread.currentThread().getName());
		 * System.out.println("Size: " + thNames.size() + " activeCount: " +
		 * forkJoinPool.getActiveThreadCount() + " parallelism: " +
		 * forkJoinPool.getParallelism()); } catch (Exception e) { throw new
		 * RuntimeException(e); } }); }
		 */
		Date start = new Date();
		long sum = 0;
		IntStream.range(1, 1_000_000).parallel().forEach(x -> {
				System.out.println(x);
				});
		Date start1 = new Date();

		ForkJoinPool forkJoinPool = GlobalData.getForkPool();
		
		try {
			forkJoinPool.submit(() ->
			// parallel task here, for example
			IntStream.range(1, 1_000_000).parallel().forEach(x -> System.out.println("with=" + x))).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Date start2 = new Date();

		long without = start1.getTime() - start.getTime();
		long with = start2.getTime() - start1.getTime();

		System.out.println(without + "," + with);
		
		int h = 8/0;
    }
    
    private void processCommand() 
    {
    	GlobalData gd = GlobalData.getInstance();

    	
    	
    	//Step 1: (after parse) should be in main thread (chronological order)
    	gd.addDocument(doc);
		
    	//testParallelStream();
    	
		if (doc.getWords().size() > 0)
		{
	        //Step 2: Can be parallelized per table
			long base = System.currentTimeMillis();
			List<String> set  = forest.addDocument(this.doc);
			long time1 = System.currentTimeMillis() - base;
			
			base = System.currentTimeMillis();
			set  = forest.addDocument4(this.doc);
			long time2 = System.currentTimeMillis() - base;

			//System.out.println("parallel - serial: " + (time2-time1));
			
			//Step 2.5: (nice to have) synchronized milestone

			//Step 3: post LSH mapping 
	        DocumentClusteringHelper.postLSHMapping(doc, set);
		}
		else
			this.doc.setNearestDetermined(true);
        //update the document in redis with the update doc //setNearestDetermined
        gd.setDocumentFromRedis("id2document", doc.getId(), doc);
    }

    public Document getDocument()
	{
        return this.doc;
    }
}