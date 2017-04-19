package ned.tools;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;

import javax.annotation.Generated;

import ned.types.GlobalData;

public class ExecutionHelper {
	private  static Executor executor = Executors.newFixedThreadPool(10);
	
	
	public static void asyncRun(Runnable task) {
			executor.execute(task);
	}
	public static Future<?> asyncAwaitRun(Callable<?> task) {
		ForkJoinPool fj = getCommonForkPool();
		ForkJoinTask<?> f=fj.submit(task);
		//fj.shutdown();
		return f;
		
	}
public static Future<?> asyncAwaitRun(Runnable task) {
	ForkJoinPool fj = getNewForkPool ();
		ForkJoinTask<?> f=fj.submit(task);
		fj.shutdown();
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
	synchronized private static ForkJoinPool getNewForkPool() {
		long start=System.currentTimeMillis();
		ForkJoinPool fj = new ForkJoinPool(1);
		long stop=System.currentTimeMillis();
		long duration=stop-start;
		if(duration>1){
			System.out.println("getNewForkPool duration is "+duration);
		}
	 		return fj;
		 }
	
	

}