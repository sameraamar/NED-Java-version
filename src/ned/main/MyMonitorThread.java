package ned.main;

import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;

public class MyMonitorThread extends Thread
{
    private ExecutorService executor;
    private int seconds;
    private boolean flush=false;

    private boolean run=true;
	private PrintStream out;

    public MyMonitorThread(ExecutorService executorService, int delay)
    {
        this.executor = executorService;
        this.seconds=delay;
    }
    

    public MyMonitorThread(int delay)
    {
        this.executor = null;
        this.seconds=delay;
    }

    public void shutdown(){
        this.run=false;
    }

    @Override
    public void run()
    {
        while(run){
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
        				
        		if (executor != null)
        		{
        			String msg = this.executor.toString();
        			msg = msg.substring(msg.indexOf('[')+1, msg.indexOf(']'));
        			Session.getInstance().message(Session.INFO, "[monitor]", "\t" + msg);  		
        		}
        		
                try {            
	                StringBuffer msg = new StringBuffer();
	            	msg.append("\tidf('i')=").append(gd.getOrDefault(gd.word2index.getOrDefault("i",-1)));
	            	msg.append(", idf('the')=").append(gd.getOrDefault(gd.word2index.getOrDefault("the",-1)));
	            	msg.append(", idf('rt')=").append(gd.getOrDefault(gd.word2index.getOrDefault("rt",-1)));
	            	msg.append(", idf('ramadan')=").append(gd.getOrDefault(gd.word2index.getOrDefault("ramadan",-1)));
	            	msg.append("Queue: ").append(gd.queue.size());
	            	
	            	Session.getInstance().message(Session.INFO, "[monitor]", msg.toString());
                } catch (NullPointerException e) {
                }
                
                
                if(flush)
                {
            		Session.getInstance().message(Session.INFO, "[monitor]", "doing some cleanup...");
            		
            		gd.markOldClusters(gd.recent.get(0));
            		gd.flushClusters(out);
            		System.gc();
                	flush = false;
                }
                
	            try {
                    Thread.sleep(seconds*1000);
	            } catch (Exception e) {
                	Session.getInstance().message(Session.INFO, "[monitor]", "Exception");
                	e.printStackTrace();
                }
        }

    }


	public void flush(PrintStream out) 
	{
		this.out = out;
		flush = true;
	}
}