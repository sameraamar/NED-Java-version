package ned.tools;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import ned.hash.LSHForest;
import ned.types.Document;
import ned.types.DocumentClusteringThread;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.Utility;

public class ExpandPositiveDS {

	
	private static SortedMap<String, JudgmentEntry> map;
	private static MapToTopicExecuter mapper;
	public static int counter = 0;

	public static void main(String[] args) throws IOException
	{
		try {
			
			long base = System.nanoTime();
			
			String judgment_extended = "/tmp/threads.txt";
			String filename = "/Users/ramidabbah/private/mandoma/samer_a/data/relevance_judgments_00000000";
			if (System.getProperty("os.name").startsWith("Windows")){
				judgment_extended = "c:/temp/threads.txt";

				 filename = "C:\\data\\events_db\\petrovic\\relevance_judgments_00000000";
			}
			PrintStream out = new PrintStream(new FileOutputStream(judgment_extended));

			
			loadLabeledTweets(filename);
			
			int size = map.size();
			mapper = new MapToTopicExecuter(map, GlobalData.getInstance().getParams().number_of_threads);
			
			doMain(out);
			
			mapper.shutdown();
			
			dumpLabeledTweets(filename+".txt");
			
			System.out.println("added " + counter + " entries");
			System.out.println("the map was " + size + ". But now " + map.size() + " (delta of " + (map.size()-size) + ")");
			
			out.close();
			
			System.out.println("Finished in " + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-base)/1000.0) + " seconds.");
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			
		}


	}

	public static void doMain(PrintStream out) throws IOException {
		GlobalData gd = GlobalData.getInstance();
		
		
		String folder = "/Users/ramidabbah/private/mandoma/samer_a/data/";
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

		int processed = 0;
		int middle_processed = 0;
		int cursor = 0;
		boolean stop = false;
		long base = System.nanoTime();
		long middletime = base;
		
		
    	Session.getInstance().message(Session.ERROR, "Reader", "Loading data...");

    	int offset = gd.getParams().offset;
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
				if(cursor < offset)
				{
					if(cursor % 50000 == 0)
						System.out.println("Cursor " + cursor);
					
					line=buffered.readLine();
					continue;					
				}
				
				Document doc = Document.parse(line, false);
				if(doc == null)
				{
					line=buffered.readLine();
					continue;
				}
				
				mapper.submit(line);
				
	            processed ++;

	            if (processed % (gd.getParams().print_limit) == 0)
	            {
	        		long tmp = System.nanoTime() - middletime;
	            	double average2 = 1.0 * TimeUnit.NANOSECONDS.toMillis(tmp) / middle_processed;
	            	average2 = Math.round(100.0 * average2) / 100.0;
	            	
	            	StringBuffer msg = new StringBuffer();
	            	msg.append( "Processed " ).append ( processed ).append(" docs. ");
	            	long seconds = TimeUnit.NANOSECONDS.toSeconds( System.nanoTime() - base);
	            	msg.append(" elapsed time: ").append(Utility.humanTime(seconds));
	            	msg.append("(AHT: ").append(average2).append(" ms). ");
	            	
	            	Session.getInstance().message(Session.INFO, "Reader", msg.toString());
	            	
            		middletime = System.nanoTime();
            		middle_processed = 0;
	            }
	            
	            if (processed >= gd.getParams().max_documents)
	            	stop = true;
	            
	            line=buffered.readLine();
	        }
			buffered.close();
	        
		}
		
		long current = System.nanoTime();

		long seconds = TimeUnit.NANOSECONDS.toSeconds(current-base);
		Session.getInstance().message(Session.INFO, "Summary", "Done in " + Utility.humanTime(seconds) );
	}
	
	public static void loadLabeledTweets(String filename) throws IOException
	{		
		FileInputStream stream = new FileInputStream(filename);
		Reader decoder = new InputStreamReader(stream, "UTF-8");
		BufferedReader buffered = new BufferedReader(decoder);
		
		System.out.println("Loading file: " + filename + " to memory");
		
		map = new TreeMap<String, JudgmentEntry>(new Comparator<String> () 
										        {  
										            public int compare(String left, String right){  
										                 long diff = Long.parseUnsignedLong(right) - Long.parseUnsignedLong(left);  //Descending
										                 if (diff == 0)
										                	 return 0;
										                 
										                 if(diff < 0)
										                	 return 1;
										
										                 return -1;
										            }  
										            
										        });
		
		String line=buffered.readLine();
		int count = 0;
		int failed = 0;
		int skip = 0;
		while(line != null)
        {
			JsonParser jsonParser = new JsonParser();
			JsonObject jsonObj = jsonParser.parse(line).getAsJsonObject();
			int topic = jsonObj.get("topic_id").getAsInt();
			
			JsonElement tweet = jsonObj.get("json");
			if(tweet==null || tweet.isJsonNull())
			{
				skip++;
				line=buffered.readLine();
				continue;
			}
			
			String json = jsonObj.get("json").toString();
			Document doc = Document.parse(json, false);
			if(doc == null)
			{	
				failed ++;
				line=buffered.readLine();
				continue;
			}
			count ++;
			
			JudgmentEntry entry = new JudgmentEntry();
			entry.doc = doc;
			entry.docJson = json;
			entry.topic = topic;
			
			map.put(doc.getId(), entry);
			
			if(count % 1000 == 0)
				System.out.println("Loaded " + count);
			
			line=buffered.readLine();
			
        }
		
		buffered.close();
		System.out.println("File loaded: "+count+" successful, "+ failed +" failed, "+ skip +" skipped.");
		
	}
	
	private static void printParameters(PrintStream out) 
	{
		GsonBuilder gson = new GsonBuilder();
		Gson g = gson.setPrettyPrinting().create();
		String params = g.toJson(GlobalData.getInstance().getParams());
	                
	    out.println(params);
	}

	private static void dumpLabeledTweets(String filename) throws UnsupportedEncodingException, FileNotFoundException
	{
		FileOutputStream stream = new FileOutputStream(filename);
		//Writer decoder = new OutputStreamWriter(stream, "UTF-8");
		//BufferedWriter buffered = new BufferedWriter(decoder);
		PrintStream out = new PrintStream(stream);
		
		System.out.println("Saving to file: " + filename );
		
		
		//String[] elements = new String[map.keySet().size()];
		//map.keySet().toArray(elements);
		
		//Arrays.sort(elements, 
		//		);
		for(String id : map.keySet())
		{
		//for (int i=0; i<elements.length; i++)
		//{
		//	String id = (String)elements[i];
			JudgmentEntry element = map.get(id);
			
			StringBuffer entry = new StringBuffer();
			entry.append("{\"_id\": ");
			entry.append(element.doc.getId());
			entry.append(", \"json\": ");
			entry.append(element.docJson);
			entry.append(", \"status\": \"Loaded\", \"topic_id\": ");
			entry.append(element.topic);
			entry.append("}");


			
			out.println(entry.toString());
		}
		
		out.close();
		System.out.println("Done saving: "+map.keySet().size()+" items.");
	}
}
