package ned.main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ned.hash.DocumentHandler;
import ned.hash.LSHForest;
import ned.tools.ExecutionHelper;
import ned.tools.RedisAccessHelper;
import ned.types.Document;
import ned.types.DocumentClusteringThread;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class AppMain {

		
	private static LSHForest forest;
	private static DocumentProcessorExecutor executer;
	private static MyMonitorThread threadMonitor;
	private static DocumentClusteringThread clustering;

	public static void release()
	{
		forest = null;
		executer = null;
		threadMonitor = null;
		clustering = null;
	}

	public static void main(String[] args) throws Exception 
	{
		try {
			GlobalData gd = GlobalData.getInstance();
			RedisAccessHelper.initRedisConnectionPool();
			while(!RedisAccessHelper.ready){
				System.out.print('.');
				Thread.sleep(100);
			}

			if(args.length > 0)
			{
			
				int o = Integer.parseInt(args[0]);
				int max = Integer.parseInt(args[1]);
				boolean resume = Boolean.parseBoolean(args[2]);
				int dimension = Integer.parseInt(args[3]);
				
				gd.getParams().offset = o ;
				gd.getParams().max_documents = max;
				gd.getParams().resume_mode = resume;
				gd.getParams().inital_dimension = dimension;
				System.out.println(String.format("Run: offset %d,  max-doc %d, resume? = %b, dim? = %d", o, max, resume, dimension));
			}			
			
			gd.init();
			printParameters(System.out);

			ExecutionHelper.setCommonPoolSize();
			String folder = "../";;
			if(getMachineName().indexOf("saaama") >= 0)
				folder  = "c:/";
			String threadsFileName = folder + "temp/threads_"+gd.getParams().max_documents+"_"+gd.getParams().offset+".txt";
			PrintStream out = new PrintStream(new FileOutputStream(threadsFileName));
			
			forest = new LSHForest(gd.getParams().number_of_tables, 
					 gd.getParams().hyperplanes, 
					 gd.getParams().inital_dimension, 
					 gd.getParams().max_bucket_size);		
			
			executer = new DocumentProcessorExecutor(forest, gd.getParams().number_of_threads);
	    	clustering = new DocumentClusteringThread(out);

	    	Session.getInstance().message(Session.ERROR, "Reader", "Starting Monitor...");
			int delay = gd.getParams().monitor_timer_seconds; //seconds
			threadMonitor  = new MyMonitorThread(executer, delay);

			doMain(out);
			
			out.close();
			
		} catch(Exception e) {
			e.printStackTrace();
			throw e;
		} finally {

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

	public static void doMain(PrintStream out) throws IOException {
		GlobalData gd = GlobalData.getInstance();
		
		String folder = "../data";
		if(getMachineName().indexOf("saaama") >= 0)
			folder  = "c:/data/events_db/petrovic";

		String[] files = {"petrovic_00000000.gz",
	                    "petrovic_00500000.gz",
	                    "petrovic_01000000.gz",
	                    "petrovic_01500000.gz",
	                    "petrovic_02000000.gz",
	                    "petrovic_02500000.gz",
	                    "petrovic_03000000.gz",
	                    "petrovic_03500000.gz",
	                    "petrovic_04000000.gz",
	                    "petrovic_04500000.gz",
	                    "petrovic_05000000.gz",
	                    "petrovic_05500000.gz",
	                    "petrovic_06000000.gz",
	                    "petrovic_06500000.gz",
	                    "petrovic_07000000.gz",
	                    "petrovic_07500000.gz",
	                    "petrovic_08000000.gz",
	                    "petrovic_08500000.gz",
	                    "petrovic_09000000.gz",
	                    "petrovic_09500000.gz",
	                    "petrovic_10000000.gz",
	                    "petrovic_10500000.gz",
	                    "petrovic_11000000.gz",
	                    "petrovic_11500000.gz",
	                    "petrovic_12000000.gz",
	                    "petrovic_12500000.gz",
	                    "petrovic_13000000.gz",
	                    "petrovic_13500000.gz",
	                    "petrovic_14000000.gz",
	                    "petrovic_14500000.gz",
	                    "petrovic_15000000.gz",
	                    "petrovic_15500000.gz",
	                    "petrovic_16000000.gz",
	                    "petrovic_16500000.gz",
	                    "petrovic_17000000.gz",
	                    "petrovic_17500000.gz",
	                    "petrovic_18000000.gz",
	                    "petrovic_18500000.gz",
	                    "petrovic_19000000.gz",
	                    "petrovic_19500000.gz",
	                    "petrovic_20000000.gz",
	                    "petrovic_20500000.gz",
	                    "petrovic_21000000.gz",
	                    "petrovic_21500000.gz",
	                    "petrovic_22000000.gz",
	                    "petrovic_22500000.gz",
	                    "petrovic_23000000.gz",
	                    "petrovic_23500000.gz",
	                    "petrovic_24000000.gz",
	                    "petrovic_24500000.gz",
	                    "petrovic_25000000.gz",
	                    "petrovic_25500000.gz",
	                    "petrovic_26000000.gz",
	                    "petrovic_26500000.gz",
	                    "petrovic_27000000.gz",
	                    "petrovic_27500000.gz",
	                    "petrovic_28000000.gz",
	                    "petrovic_28500000.gz",
	                    "petrovic_29000000.gz",
	                    "petrovic_29500000.gz"  
	                   };
		//files = new String[] {"test.json.gz"};


		int processed = 0;
		int middle_processed = 0;
		int cursor = 0;
		boolean stop = false;
		long base = System.nanoTime();
		long middletime = base; 
		long firstdoc = 0;
		long lastdoc;

		printParameters(out);

		//CleanupThread thread = new CleanupThread(out);
		//threadsFileName.isDaemon(true);
		//thread.start();
		threadMonitor.start();
    	Session.getInstance().message(Session.ERROR, "Reader", "Loading data...");

    	clustering.start();

    	int offset = gd.getParams().offset;
		int skip_files = (offset / 500_000);
		offset = offset % 500_000;
		int fileidx = -1;
		
		int offset_p = (int)(offset * 0.05);
		boolean flushData = false;
		for (String filename : files) {
			fileidx++;
			if (stop)
				break;
			
			if(fileidx < skip_files)
			{
            	Session.getInstance().message(Session.INFO, "Reader", "Skipping file " + fileidx + ": " + filename);
				continue;
			}
	    	Session.getInstance().message(Session.INFO, "Reader", "reading from file: " + filename);

			GZIPInputStream stream = new GZIPInputStream(new FileInputStream(folder + "/" + filename));
			Reader decoder = new InputStreamReader(stream, "UTF-8");
			BufferedReader buffered = new BufferedReader(decoder);
			
			String line=buffered.readLine();
			while(!stop && line != null)
	        {
				cursor += 1;
				
				if (cursor <= offset)
				{
					if (cursor % offset_p == 0)
		            	Session.getInstance().message(Session.INFO, "Reader", "Skipped " + cursor + " documents.");
					
					line=buffered.readLine();
					continue;
				}
				
				Document doc = DocumentHandler.preprocessor(line);
				GlobalData.getInstance().getQueue().add(doc.getId());
				
	            int idx = gd.getParams().offset+processed;
				executer.submit(doc, idx);
				
	            processed ++;
	            middle_processed++;
	            idx++;
	            
	            
	            if (processed % (gd.getParams().print_limit) == 0)
	            {
	            	if(GlobalData.getInstance().getQueue().size() > 15000)
		            {
		            	executer.refresh();
		            }
		            	        		
	            	long currenttime = System.nanoTime();
	        		long tmp = currenttime - middletime;
	            	double average2 = 1.0 * TimeUnit.NANOSECONDS.toMillis(tmp) / middle_processed;
	            	average2 = Math.round(100.0 * average2) / 100.0;
	            	
	            	StringBuffer msg = new StringBuffer();
	            	msg.append( "Processed " ).append ( processed ).append(" docs. ");
	            	long seconds = TimeUnit.NANOSECONDS.toSeconds( currenttime - base );
	            	long milliseconds = TimeUnit.NANOSECONDS.toMillis( currenttime - base );
	            	msg.append(" elapsed time: ").append(Utility.humanTime(seconds));
	            	msg.append("(AHT: ").append(average2).append(" ms). ");
//	            	//msg.append("Cursor: ").append(doc.getId());
	            	msg.append("Dim: ").append( forest.getDimension() );
	            	if(clustering.clusteredCounter > 0)
	            	{
	            		String ahtStr = String.format(". Finalized %d docs, ms/doc: %.3f", clustering.clusteredCounter, 1.0 * milliseconds / clustering.clusteredCounter);
	            		msg.append( ahtStr ).append(" ms");
	            	}
	            	
	            	Session.getInstance().message(Session.INFO, "Reader", msg.toString());
	            	
	            	flushData = processed % (2*gd.getParams().print_limit) == 0;
	            	if(flushData)
	            	{
	            		//executer.await();
	        			//DocumentHandler.waitForSuProcesses();
	            		//Session.getInstance().message(Session.DEBUG, "Reader", "doing some cleanup...");
	            		//gd.flushClusters(out);
	            		flushData = false;
	            	}

            		middletime = System.nanoTime();
            		middle_processed = 0;
	            }
	            
	            //if (processed % (gd.getParams().print_limit* 40) == 0){
	            //	System.out.println("GC submited");
	            //	System.gc();
	            //}
	            
	            if (processed == gd.getParams().max_documents)
	            	stop = true;
	            
	            
	            if (stop || processed % (gd.getParams().print_limit* 10) == 0)
	            {
	            	
	            	int lastIndex = gd.resumeInfo.get(GlobalData.LAST_SEEN_IDX);

	        		if(lastIndex <= idx)
	        		{
		            	System.out.println("Wait for queue to get empty!");
		            	while(!gd.getQueue().isEmpty())
		            	{
		            		try {
								Thread.sleep(50);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
		            	}
		            	
		            	gd.save();
		            	
		            	//if (stop || processed % (gd.getParams().print_limit* 20) == 0)
		            	//	RedisAccessHelper.initRedisConnectionPool(true);
		            	
		            	System.gc();
	        		}
	            }
	            
	            line=buffered.readLine();
	        }
			buffered.close();
	        
		}
		
		//wait till all processes finish
		executer.shutdown();
		ExecutionHelper.shutdown();
		
		Session.getInstance().message(Session.INFO, "Summary", "wait till all processes finish");

		long current = System.nanoTime();
		long seconds = TimeUnit.NANOSECONDS.toSeconds(current-base);
		Session.getInstance().message(Session.INFO, "Summary", "Done in " + Utility.humanTime(seconds) );
	}
	
	private static String getMachineName()
	{
		String hostname = "Unknown";

		try
		{
		    InetAddress addr;
		    addr = InetAddress.getLocalHost();
		    return addr.getHostName().toLowerCase();
		}
		catch (UnknownHostException ex)
		{
		    System.out.println("Hostname can not be resolved");
		}		
		return "";
	}
		
	private static void printParameters(PrintStream out) 
	{
		GsonBuilder gson = new GsonBuilder();
		Gson g = gson.setPrettyPrinting().create();
		String params = g.toJson(GlobalData.getInstance().getParams());
	                
	    out.println(params);
	}

}
