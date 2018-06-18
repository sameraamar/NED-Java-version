package ned.main;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import ned.provider.DocumentParserThread;
import ned.tools.RedisAccessHelper;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class MyMonitorThread extends Thread
{
    private DocumentProcessorExecutor executor;
    private int seconds;
    private long starttime;

    private boolean stop=false;
	private DocumentParserThread parserThread;

    public MyMonitorThread(DocumentProcessorExecutor executorService, int delay)
    {
        this.executor = executorService;
        this.seconds=delay;
    }
    
    public MyMonitorThread(int delay)
    {
        this.executor = null;
        this.seconds=delay;
        this.parserThread = null;
    }

    public MyMonitorThread(DocumentProcessorExecutor executer, DocumentParserThread parserThread, int delay) {
    	this.executor = null;
        this.seconds=delay;
        this.parserThread = parserThread;
	}

	public void shutdown() {
    	Session.getInstance().message(Session.INFO, "[monitor]", "request to shutdown");
        this.stop=true;
    }

    @Override
    public void run()
    {
    starttime = System.nanoTime();
        while(!stop){
            /*System.out.println(
                    String.format("[monitor] [%d/%d] Active: %d, Completed: %d, Task: %d, isShutdown: %s, isTerminated: %s",
                        this.executor.getPoolSize(),
                        this.executor.getCorePoolSize(),
                        this.executor.getActiveCount(),
                        this.executor.getCompletedTaskCount(),
                        this.executor.getTaskCount(),
                        this.executor.isShutdown(),
                        this.executor.isTerminated()));
                */
        
        GlobalData gd = GlobalData.getInstance();

    		Session.getInstance().message(Session.INFO, "[monitor]", gd.memoryGlance());
    				
        	long delta =  System.nanoTime()-starttime;
        	delta = TimeUnit.NANOSECONDS.toSeconds(delta);
        	String waht = WorkerThread.glance();
        	//if(waht>500){
        	//	System.out.println("GC Run");
        	//	System.gc();
        	//}

            StringBuffer msg = new StringBuffer();
            msg.append("\tActive Redis Connections: ").append(RedisAccessHelper.getNumActive()).append("\n");
            msg.append("\tElapsed time: ").append(Utility.humanTime(delta));
            msg.append(", Worker AHT: ").append(waht).append("\n\t");
            WorkerThread.resetCounter();
            if (executor != null)
	        {
	        String str = this.executor.getExecutor().toString();
	        str = str.substring(str.indexOf('[')+1, str.indexOf(']'));
	        msg.append( "[monitor]").append( "\t" + str).append("\n") ;  
	        }
	        
	        try {            
	            msg.append("\tidf('winehouse')=").append(gd.calcIDF(gd.word2index.getOrDefault("winehouse",-1)));
	            msg.append(", idf('the')=").append(gd.calcIDF(gd.word2index.getOrDefault("the",-1)));
	            msg.append(", idf('rt')=").append(gd.calcIDF(gd.word2index.getOrDefault("rt",-1)));
	            msg.append(", idf('ramadan')=").append(gd.calcIDF(gd.word2index.getOrDefault("ramadan",-1)));
	        } catch (NullPointerException e) {
	        }
	
            msg.append("\n");
            msg.append("\tClustering Queue: ").append(gd.getQueue().size()).append(", ID=").append(gd.getQueue().peek());
            if (parserThread != null)
            	msg.append("\n\tparser queue: " + parserThread.queue.size());
	        Session.getInstance().message(Session.INFO, "[monitor]", msg.toString());
	                
	        try {
	        	int _sec = seconds;
	        	while(!stop && _sec>0)
	        	{
	                Thread.sleep(1000);
	                _sec--;
	        	}
	        } catch (Exception e) {
	            Session.getInstance().message(Session.INFO, "[monitor]", "Exception");
	            e.printStackTrace();
	        }
        }

    }

}
