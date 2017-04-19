package ned.tools;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;

import ned.types.GlobalData;

public class ExecutionHelper {
	private  static Executor executor = Executors.newFixedThreadPool(10);
	
	
	public static void asyncRun(Runnable task) {
			executor.execute(task);
	}
	public static Future asyncAwaitRun(Callable task) {
		
		ForkJoinTask<?> f=getCommonForkPool().submit(task);
		
		
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
	 		System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "2000000");
	 		return ForkJoinPool.commonPool();
		}
	synchronized private static ForkJoinPool getNewForkPool() {
	 		return new ForkJoinPool(1);
		 }
	
	

}