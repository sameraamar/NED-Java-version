package ned.main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ned.hash.DocumentHandler;
import ned.hash.LSHForest;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class AppMain {

	public static int checkThreads(Thread[] list)
	{
		int count =0 ;
		
		for (int i = 0 ; i <list.length; i++)
		{
			if (list[i].isAlive())
				count++;
		}
		
		return count;
	}
	public static void main(String[] args) throws IOException {
		GlobalData gd = GlobalData.getInstance();
		
		LSHForest forest = null;
		forest = new LSHForest(gd.getParams().number_of_tables, 
				 gd.getParams().hyperplanes, 
				 gd.getParams().inital_dimension, 
				 gd.getParams().max_bucket_size);		
		
		String folder = "C:\\data\\events_db\\petrovic";
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

		String threadsFileName = "c:/temp/threads.txt";
		PrintStream out = new PrintStream(new FileOutputStream(threadsFileName));
		
		printParameters(out);


		//CleanupThread thread = new CleanupThread(out);
		//threadsFileName.isDaemon(true);
		//thread.start();
		
		int offset = gd.getParams().offset;
		int offset_p = (int)(offset * 0.05);
		for (String filename : files) {
			if (stop)
				break;
			
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
				if (firstdoc == 0)
					firstdoc = doc.getTimestamp();
				
				lastdoc = doc.getTimestamp();
				
				DocumentHandler.process(forest, doc);
				//GlobalData.releaseOldDocuments();

	            processed ++;
	            middle_processed++;
	            
	            if (processed % gd.getParams().print_limit == 0)
	            {
		            long passed = System.nanoTime() - base;
	            	double average1 = 1.0 * TimeUnit.NANOSECONDS.toMillis(passed) / processed;
	            	average1 = Math.round(100.0 * average1) / 100.0;

	            	long tmp = System.nanoTime() - middletime;
	            	double average2 = 1.0 * TimeUnit.NANOSECONDS.toMillis(tmp) / middle_processed;
	            	average2 = Math.round(100.0 * average2) / 100.0;
	            	
	            	long reportedtime = lastdoc - firstdoc;
	            	
	            	StringBuffer msg = new StringBuffer();
	            	msg.append( "Processed " ).append ( processed ).append(" docs. ");
	            	msg.append(gd.clustersSize()).append(" clusters.");
	            	msg.append("[spent ").append(  Utility.humanTime( TimeUnit.NANOSECONDS.toSeconds(passed) ) ).append("]. ");
	            	msg.append("[reported ").append( Utility.humanTime( reportedtime ) ).append("]. ");
	            	msg.append("(overall AHT: ").append(average1).append(" ms). ");
	            	msg.append("(AHT: ").append(average2).append(" ms). ");
	            	msg.append("Cursor: ").append(doc.getId());
	            	msg.append("Dim: ").append( forest.getDimension() );
	            	
	            	Session.getInstance().message(Session.INFO, "Reader", msg.toString());
	            	
            		/*Session.getInstance().message(Session.INFO, "Reader", "doing some cleanup...");
	            	gd.markOldClusters(doc);
	            	synchronized(thread) {
	            		thread.notify();
	            	}
            		middletime = System.nanoTime();
            		middle_processed = 0;
            		*/
	            	
            		middletime = System.nanoTime();
            		middle_processed = 0;
	            	
	            	if (processed % (15*gd.getParams().print_limit) == 0)
	            	{
	            		Session.getInstance().message(Session.INFO, "Reader", "doing some cleanup...");
	            		gd.markOldClusters(doc);
	            		gd.flushClusters(out);
	            		System.gc();
	            	}
	            }
	            
	            if (processed == gd.getParams().max_documents)
	            	stop = true;
	            
	            line=buffered.readLine();
	        }
			buffered.close();
	        
		}
		
		long current = System.nanoTime();

		//ThreadManagerHelper.pprint(out);
		
		gd.flushClustersAll(out);
		
		out.close();

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
