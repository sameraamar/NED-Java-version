package ned.main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ned.hash.LSHForest;
import ned.hash.LSHForestAbstract;
import ned.hash.LSHForestParallel;
import ned.types.Document;
import ned.types.DocumentClusteringThread;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class AppMain {

		
	private static LSHForestAbstract forest;
	private static ExecutorMonitorThread threadMonitor;
	private static DocumentClusteringThread clustering;
	

	public static void main(String[] args) throws IOException
	{
		GlobalData gd = GlobalData.getInstance();
		gd.initRedisConnectionPool();
		try {			
			Hashtable<String, String> arguments = new Hashtable<String, String>();
			
			if(args.length > 0 && args[0] == "-help")
			{
				System.out.println("-ifolder <input-folder> -ofolder <output-folder> -threads <threads-file-name>");
				return;
			}
			
			for (int i=0; i<args.length; i++)
			{
				if(args[i].startsWith("-"))
				{
					arguments.put(args[i], args[++i]);
				}
			}
			
			String inputFolder = "/Users/ramidabbah/private/mandoma/samer_a/data";
			if (System.getProperty("os.name").startsWith("Windows"))
				inputFolder = "C:\\data\\events_db\\petrovic";
			
			
			inputFolder = arguments.getOrDefault("-ifolder", inputFolder);
			int documentNumber = Integer.parseInt( arguments.getOrDefault("-max_doc", gd.getParams().max_documents+"") );
			gd.getParams().max_documents = documentNumber;
			
			
			String outputFolder = arguments.getOrDefault("-ofolder", inputFolder + "/out");

			String threadsFileName = outputFolder + "/" + arguments.getOrDefault("-threads", "threads.txt" );

			System.out.println("Max documents:" + gd.getParams().max_documents);
			System.out.println("input folder:" + inputFolder);
			System.out.println("thread file:" + threadsFileName);
			System.out.println("REDIS_MAX_CONNECTIONS:" + gd.parameters.REDIS_MAX_CONNECTIONS);

			PrintStream out = new PrintStream(new FileOutputStream(threadsFileName));
			
			/*forest = new LSHForestParallel(gd.getParams().number_of_tables, 
					 gd.getParams().hyperplanes, 
					 gd.getParams().inital_dimension, 
					 gd.getParams().max_bucket_size);
			*/
			forest = new LSHForest(gd.getParams().number_of_tables, 
					 gd.getParams().hyperplanes, 
					 gd.getParams().inital_dimension, 
					 gd.getParams().max_bucket_size);
				 
			gd.executer = new DocumentProcessorExecutor(forest, gd.getParams().number_of_threads);
	    	clustering = new DocumentClusteringThread(out);

	    	Session.getInstance().message(Session.ERROR, "Reader", "Starting Monitor...");
			int delay = gd.getParams().monitor_timer_seconds; //seconds
			threadMonitor  = new MyMonitorThread(gd.executer.getExecutor(), delay);

			doMain(out, inputFolder);
			
			out.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {

			
			if ( gd.executer != null )
				gd.executer.shutdown();
			
			if (threadMonitor != null)
				threadMonitor.shutdown();
						
			if ( clustering != null )
				clustering.shutdown();

		}
	}

	public static void doMain(PrintStream out, String inputFolder) throws IOException {
		GlobalData gd = GlobalData.getInstance();
		gd.initRedisConnectionPool();
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
    	Session.getInstance().message(Session.ERROR, "Reader", "monitoring thread started");

    	clustering.start();
    	Session.getInstance().message(Session.INFO, "Reader", "clustering thread started");

 		System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "20000");
    	
		int offset = gd.getParams().offset;
		int offset_p = (int)(offset * 0.05);
		boolean flushData = false;
		int fileidx = -1;
		for (String filename : files) {
			fileidx ++;
			
			if (stop)
				break;
			
	    	Session.getInstance().message(Session.INFO, "Reader", "reading from file: " + filename);
			if(fileidx < gd.getParams().skip_files)
			{
            	Session.getInstance().message(Session.INFO, "Reader", "Skipping file " + fileidx );
				continue;
			}
			
			GZIPInputStream stream = new GZIPInputStream(new FileInputStream(inputFolder + "/" + filename));
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
				
				Document doc = Document.parse(line, true);
				if(doc == null)
					continue;
				
				synchronized (gd.queue) 
				{
					gd.queue.add(doc.getId());
				}
								
				gd.executer.submit(doc);
				
	            processed ++;
	            middle_processed++;

	            if (processed % (gd.getParams().print_limit) == 0)
	            {
	            	//gd.flushClusters(out);
	            	System.gc();
	        		long tmp = System.nanoTime() - middletime;
	            	double average2 = 1.0 * TimeUnit.NANOSECONDS.toMillis(tmp) / middle_processed;
	            	average2 = Math.round(100.0 * average2) / 100.0;
	            	
	            	StringBuffer msg = new StringBuffer();
	            	msg.append( "Processed " ).append ( processed ).append(" docs. ");
	            	long seconds = TimeUnit.NANOSECONDS.toSeconds( System.nanoTime() - base);
	            	msg.append(" elapsed time: ").append(Utility.humanTime(seconds));
	            	msg.append("(AHT: ").append(average2).append(" ms). ");
	            	msg.append("Dim: ").append( forest.getDimension() );
	            	
	            	Session.getInstance().message(Session.INFO, "Reader", msg.toString());
	            	
            		middletime = System.nanoTime();
            		middle_processed = 0;
	            }
	            
	            if (processed == gd.getParams().max_documents)
	            	stop = true;
	            
	            line=buffered.readLine();
	        }
			buffered.close();
	        
		}
		

		long current = System.nanoTime();
		//wait till all processes finish
		gd.executer.shutdown();
    	Session.getInstance().message(Session.INFO, "Main", "Waiting for clustering thread to finish...");

    	if(clustering!=null)
    		clustering.shutdown();
    	
    	if(gd.clusters.size() > 0)
    		System.out.println("!!!! not all clusters were saved. Still in memory: " + gd.clusters.size());
    	
    	//gd.flushClustersAll(out);

		long seconds = TimeUnit.NANOSECONDS.toSeconds(current-base);
		Session.getInstance().message(Session.INFO, "Summary", "Done in " + Utility.humanTime(seconds) );
	}
	

	
	private static void printParameters(PrintStream out) 
	{
		GsonBuilder gson = new GsonBuilder();
		Gson g = gson.setPrettyPrinting().create();
		String params = g.toJson(GlobalData.getInstance().getParams());
	                
	    out.println(params);
	}

}
