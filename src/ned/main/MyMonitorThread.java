package ned.main;

import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;

import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;

public class MyMonitorThread extends ExecutorMonitorThread
{
    private boolean flush=false;
    private PrintStream out;

    public MyMonitorThread(ExecutorService executorService, int delay)
    {
        super(executorService, delay);
    }
    
    @Override
    protected void printHook() {
		GlobalData gd = GlobalData.getInstance();
		
		Session.getInstance().message(Session.INFO, "[monitor]", gd.memoryGlance());
		
		StringBuffer msg = new StringBuffer();
		msg.append("Cluster Queue: ").append(gd.queue.size()).append(". ID: ").append(gd.queue.peek());
		Session.getInstance().message(Session.INFO, "[monitor]", msg.toString());
		
/*
		StringBuffer msg = new StringBuffer();
		msg.append("\tn('i')=").append(gd.numberOfDocsIncludeWord.get(gd.word2index.getOrDefault("i",-1)));
		msg.append(String.format("  idf('i')==%.5f", gd.getIDFOrDefault(gd.word2index.getOrDefault("i",-1))));
		msg.append(String.format(", idf('rt')==%.5f", gd.getIDFOrDefault(gd.word2index.getOrDefault("rt",-1))));
		msg.append(String.format(", idf('ramadan')=%.5f", gd.getIDFOrDefault(gd.word2index.getOrDefault("ramadan",-1))));
		msg.append(String.format(", idf('amy')==%.5f", gd.getIDFOrDefault(gd.word2index.getOrDefault("amy",-1))));
		msg.append(String.format(", idf('winehouse')==%.5f", gd.getIDFOrDefault(gd.word2index.getOrDefault("winehouse",-1))));
		msg.append(", Queue: ").append(gd.queue.size()).append(". ID: ").append(gd.queue.peek());
		msg.append("\n\t");
		msg.append("Common Paral.=").append(ForkJoinPool.getCommonPoolParallelism());
		msg.append(", ForkPool.StealCount=").append(GlobalData.getForkPool().getStealCount());
		msg.append(", ForkPool.Actives=").append(GlobalData.getForkPool().getActiveThreadCount());
		msg.append(", ForkPool.Running=").append(GlobalData.getForkPool().getRunningThreadCount());
		msg.append(", ForkPool.Submitted=").append(GlobalData.getForkPool().getQueuedSubmissionCount());
		msg.append(", ForkPool.Queued=").append(GlobalData.getForkPool().getQueuedTaskCount());
		
		Session.getInstance().message(Session.INFO, "[monitor]", msg.toString());
		
		msg = new StringBuffer();
		if(gd.recent!=null && gd.recent.size()>1)
		{
			Document doc = gd.getDocumentFromRedis(GlobalData.ID2DOCUMENT, gd.recent.get(gd.recent.size()-1));
			if(doc != null)
				msg.append("\tlast document time: " ).append(doc.getCreatedAt()).append("\n");
		}
		Session.getInstance().message(Session.INFO, "[monitor]", msg.toString());
        
        */
        if(flush)
        {
    		Session.getInstance().message(Session.INFO, "[monitor]", "doing some cleanup...");
    		
    		gd.markOldClusters(gd.recent.get(0));
    		gd.flushClusters(out);
    		System.gc();
        	flush = false;
        }
     
	}


	public void flush(PrintStream out) 
	{
		this.out = out;
		flush = true;
	}
}