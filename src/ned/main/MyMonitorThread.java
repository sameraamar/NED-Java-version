package ned.main;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class MyMonitorThread extends Thread
{
    private ExecutorService executor;
    private int seconds;
    private long starttime;

    private boolean run=true;

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

    public void shutdown() {
Session.getInstance().message(Session.INFO, "[monitor]", "request to shutdown");

        this.run=false;
    }

    @Override
    public void run()
    {
    starttime = System.nanoTime();
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
    				
        	long delta =  System.nanoTime()-starttime;
        	delta = TimeUnit.NANOSECONDS.toSeconds(delta);
        	double waht= WorkerThread.avegTime();
        	if(waht>200){
        		System.gc();
        	}

            StringBuffer msg = new StringBuffer();
            msg.append("Elapsed time: ").append(Utility.humanTime(delta));
            msg.append(" Worker AHT: ").append(String.format("\t%.2f",waht)).append(".\n\t");
            WorkerThread.resetCounter();
            if (executor != null)
        {
        String str = this.executor.toString();
        str = str.substring(str.indexOf('[')+1, str.indexOf(']'));
        msg.append( "[monitor]").append( "\t" + str).append("\n") ;  
        }
        
                try {            

            msg.append("\tidf('i')=").append(gd.getOrDefault(gd.word2index.getOrDefault("i",-1)));
            msg.append(", idf('the')=").append(gd.getOrDefault(gd.word2index.getOrDefault("the",-1)));
            msg.append(", idf('rt')=").append(gd.getOrDefault(gd.word2index.getOrDefault("rt",-1)));
            msg.append(", idf('ramadan')=").append(gd.getOrDefault(gd.word2index.getOrDefault("ramadan",-1)));
            msg.append(". Queue: ").append(gd.queue.size()).append(", ID=").append(gd.queue.peek());
            

            
            Session.getInstance().message(Session.INFO, "[monitor]", msg.toString());
                } catch (NullPointerException e) {
                }
                
            try {
                    Thread.sleep(seconds*1000);
            } catch (Exception e) {
                Session.getInstance().message(Session.INFO, "[monitor]", "Exception");
                e.printStackTrace();
                }
        }

    }

}
