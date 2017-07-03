package ned.provider;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import ned.hash.LSHForest;
import ned.main.DocumentProcessorExecutor;
import ned.main.MyMonitorThread;
import ned.tools.ExecutionHelper;
import ned.tools.RedisAccessHelper;
import ned.types.Document;
import ned.types.DocumentClusteringThread;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class AppMain2 {

	private static MyMonitorThread threadMonitor;
	private static DocumentProcessorExecutor executer;
	private static DocumentClusteringThread clustering;
	private static PrintStream outFull;
	private static PrintStream outShort;
	private static String outfolder;
	private static LSHForest forest;

	private static void release()
	{
		forest = null;
		executer = null;
		threadMonitor = null;
		clustering = null;
	}
	
	public static void main(String[] args) throws Exception 
	{
		long base = 0;
		try {
			GlobalData gd = GlobalData.getInstance();
			RedisAccessHelper.initRedisConnectionPool();
			while(!RedisAccessHelper.ready){
				System.out.print('.');
				Thread.sleep(100);
			}

			base = System.nanoTime();

			gd.init();

			ExecutionHelper.setCommonPoolSize();
			
			
			createOutFolder();
			openOutput(0);

	    	clustering = new DocumentClusteringThread(outFull, outShort);
			int delay = gd.getParams().monitor_timer_seconds; //seconds

			
			forest = new LSHForest(gd.getParams().number_of_tables, 
					 gd.getParams().hyperplanes, 
					 gd.getParams().inital_dimension, 
					 gd.getParams().max_bucket_size);	
			
			executer = new DocumentProcessorExecutor(forest, gd.getParams().number_of_threads);
			threadMonitor  = new MyMonitorThread(executer, delay);
			
	    	Session.getInstance().message(Session.ERROR, "Reader", "Starting Monitor...");


			threadMonitor.start();
	    	clustering.start();
	    	
			doMain();
			

			
			outFull.close();
			outShort.close();
			
		} catch(Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			//wait till all processes finish
			executer.shutdown();
			ExecutionHelper.shutdown();
			
			Session.getInstance().message(Session.INFO, "Summary", "wait till all processes finish");

			long current = System.nanoTime();
			long seconds = TimeUnit.NANOSECONDS.toSeconds(current-base);
			Session.getInstance().message(Session.INFO, "Summary", "Done in " + Utility.humanTime(seconds) );
			
			if ( clustering != null )
			{
		    	Session.getInstance().message(Session.INFO, "Main", "Waiting for clustering thread to finish...");
				clustering.shutdown();
				while(clustering.isAlive())
					;
			}
			if (threadMonitor != null) {
				threadMonitor.shutdown();
				while(threadMonitor.isAlive())
				{
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			release();
		}
	}

	public static void createOutFolder() {
		GlobalData gd = GlobalData.getInstance();
		String folder = "../temp";
		if(Session.getMachineName().indexOf("samer") >= 0)
			folder  = "c:/temp";
		
		folder = folder + "/threads_"+gd.getParams().max_documents+"_"+gd.getParams().offset;

		File theDir = new File(folder);
	
		// if the directory does not exist, create it
		if (!theDir.exists()) {
		    System.out.println("creating directory: " + theDir.getAbsolutePath());

		    theDir.mkdir();
		}
		
		outfolder = folder;
	}

	public static void openOutput(int index) throws FileNotFoundException {
		
		String roll = String.format("%03d", index);
		String threadsFileNameFull = outfolder + "/full_"+roll+".txt";
		String threadsFileNameShort = outfolder +"/short_"+roll+".txt";
		outFull = new PrintStream(new FileOutputStream(threadsFileNameFull));
		outShort = new PrintStream(new FileOutputStream(threadsFileNameShort));
	}
	
	private static void doMain() throws Exception {
		GlobalData gd = GlobalData.getInstance();
		DocumentProvider p = new DocProviderGZip(gd.getParams().max_documents, gd.getParams().offset);
		p.start();
		
		long base = System.nanoTime(); 
		long middletime  = base;
		int middle_processed = p.getCursor();

		while(p.hasNext())
		{
			Document d = p.next();
			int idx = p.getCursor();

			gd.addDocument(d, idx);
			
			GlobalData.getInstance().getQueue().add(d.getId());
			executer.submit(d, idx);
			
			int processed = p.getProcessed();
			
			
			if (processed % (gd.getParams().print_limit) == 0)
            {   	        		
            	long currenttime = System.nanoTime();
        		long tmp = currenttime - middletime;
        		middle_processed = p.getCursor() - middle_processed;
            	double average2 = 1.0 * TimeUnit.NANOSECONDS.toMillis(tmp) / middle_processed;
            	average2 = Math.round(100.0 * average2) / 100.0;
            	
            	StringBuffer msg = new StringBuffer();
            	msg.append( "Processed " ).append ( processed ).append(" docs. ");
            	long seconds = TimeUnit.NANOSECONDS.toSeconds( currenttime - base );
            	long milliseconds = TimeUnit.NANOSECONDS.toMillis( currenttime - base );
            	msg.append(" elapsed time: ").append(Utility.humanTime(seconds));
            	msg.append("(AHT: ").append(average2).append(" ms). ");
//            	//msg.append("Cursor: ").append(doc.getId());
            	msg.append("Dim: ").append( forest.getDimension() );
            	if(clustering.clusteredCounter > 0)
            	{
            		String ahtStr = String.format(". Finalized %d docs, ms/doc: %.3f", clustering.clusteredCounter, 1.0 * milliseconds / clustering.clusteredCounter);
            		msg.append( ahtStr ).append(" ms");
            	}
            	
            	Session.getInstance().message(Session.INFO, "Reader", msg.toString());

        		middletime = System.nanoTime();
        		middle_processed = p.getCursor();
        	}
			
			if (processed % (gd.getParams().print_limit * 10) == 0)
            {
            	int lastIndex = gd.resumeInfo.get(GlobalData.LAST_SEEN_IDX);
            	waitForClusteringQueue();
            	gd.save(lastIndex <= idx);
            }
			
			boolean debug = false;
		}
		p.close();
	}

	public static void waitForClusteringQueue() {
		System.out.println("Wait for queue to get empty!");
		while(!GlobalData.getInstance().getQueue().isEmpty())
		{
			try {
				if(clustering.isInterrupted()|| clustering.isStop()){
					clustering.setStop(false);
					clustering.resume();
				}
				
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
