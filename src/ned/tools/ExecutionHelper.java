package ned.tools;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;

public class ExecutionHelper {
	private  static ExecutorService executor = Executors.newFixedThreadPool(2000);
	private  static ForkJoinPool myForkJoinPool = new ForkJoinPool(100);

	public static void asyncRun(Runnable task) {
			executor.execute(task);
	}
	
	public static void shutdown()
	{
		executor.shutdown();
	}
	
	public static Future<?> asyncAwaitRun(Callable<?> task) {
		ForkJoinPool fj = getNewForkPool ();
		ForkJoinTask<?> f=fj.submit(task);
		//fj.shutdown();
		return f;
		
	}
public static Future<?> asyncAwaitRun(Runnable task) {
	ForkJoinPool fj = getNewForkPool ();
		ForkJoinTask<?> f=fj.submit(task);
		try {
			f.get();
			//fj.shutdownNow();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return f;
		
	}
	
	
	public static boolean canCreateNewThread(){
		if(Thread.activeCount()<20000){
			try {
				return true;
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
		return false;
		
	}
	public synchronized static ForkJoinPool getCommonForkPool() {
	 		//
	 		return ForkJoinPool.commonPool();
		}
	public static ForkJoinPool getNewForkPool() {
	 	return myForkJoinPool;
	 	/*
		long start=System.currentTimeMillis();
		ForkJoinPool fj = new ForkJoinPool(1);
		long stop=System.currentTimeMillis();
		long duration=stop-start;
		if(duration>10){
			System.gc();
			System.out.println("getNewForkPool duration is "+duration);
		}
	 		return fj;
	 		*/
	}
public static long activeCount(){
	return myForkJoinPool.getActiveThreadCount();
}

public static long getQueuedTaskCount(){
	return myForkJoinPool.getQueuedTaskCount();
}

public static long getQueuedSubmissionCount(){
	return myForkJoinPool.getQueuedSubmissionCount();
}
	
public static void setCommonPoolSize(){
	System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "1000");
}

}