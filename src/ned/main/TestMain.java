package ned.main;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.regex.Pattern;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.RedisBasedMap;
import ned.types.SerializeHelperAdapterDirtyBit;

public class TestMain {

	private static String DELIMITER = " ||| ";


	public static void main(String[] args) throws Exception {
		GlobalData.getInstance().getParams().resume_mode = true;
		GlobalData.getInstance().init();
		System.out.println("------------------------------------------");

	
		String[] ids = {"86419754188935168"};
		
		for (String id : Arrays.asList( ids )) {
			Document doc = GlobalData.getInstance().id2doc.get(id);
			System.out.println(doc==null ? "NULL" : doc.toString());
		}
		
		//updateText();
	}

	static public void updateText()
	{
		String filename = "C:\\temp\\threads_petrovic_all\\hit_results-MechanichalTurk.csv.txt";
		String filenameOut = "C:\\temp\\threads_petrovic_all\\hit_results-MechanichalTurk_text.csv.txt";

		RedisBasedMap<String, Document> id2doc = new RedisBasedMap<String, Document>(GlobalData.K_ID2DOCUMENT, false, new SerializeHelperAdapterDirtyBit<Document>() );

		BufferedReader br = null;
		try {
			PrintStream out = new PrintStream(new FileOutputStream(filenameOut));

			br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
		    
			int count = 0;
		    int printed = 0;
		    
		    if(line.startsWith("idHitResult"))
		    {
		    	out.print(line + "\ttext\n");
		    	line = br.readLine();
		    }
		    
	        //System.out.println(line);
		    while (line != null) {
		        if(line.trim().equals(""))
		        	continue;
		        
		    	String[] tokens = line.split( Pattern.quote("\t") ) ;
		    	String id = tokens[4]; 
		    	
		    	Document doc = id2doc.get(id);
		    	
		    	String text = doc.getText().replace("\t", " ").replace("\n", " ").replace("\r", " ");
		    	out.print(line + "\t" + text + "\n");
		        
		        count++;
		        line = br.readLine();
		    }
		    
		    
		    br.close();
		    out.close();
		    System.out.println("count: " + count );

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		    
		}
		
	}
	
	static public void filterShortResults()
	{
		String filename = "C:\\temp\\threads_petrovic_all\\run_June11\\full_002.txt";
		String filenameOut = "C:\\temp\\threads_petrovic_all\\run_June11\\full_002-filtered.txt";
		
		
		
		BufferedReader br = null;
		try {
			PrintStream out = new PrintStream(new FileOutputStream(filenameOut));

			br = new BufferedReader(new FileReader(filename));
		    String line = br.readLine();
		    
		    out.print(line + "\n");

		    int count = 0;
		    int printed = 0;
		    String leadId = null, entropy = null;
		    String size="0";
		    if(line.startsWith("leadId"))
		    	line = br.readLine();
		    
	        System.out.println(line);
		    while (line != null) {
		        if(line.trim().equals(""))
		        	continue;
		        
		    	String[] tokens = line.split( Pattern.quote(DELIMITER) ) ;
		    	leadId = tokens[0]; // tk.nextToken();
		    	entropy = tokens[6]; //tk.nextToken();
		    	size = tokens[7]; //tk.nextToken();
		    	//size = tk.nextToken();
		    	
		    	Double entr = null;
		    	Integer s = null;
		    	
		        boolean skip = false;
		        try {
		        	entr = Double.parseDouble(entropy);
		        } catch (Exception e)
		        {
		        	e.printStackTrace();
		        	System.out.println(e.getMessage());
		        }

		        try {
		        	s = Integer.parseInt(size);
		        } catch (Exception e)
		        {
		        	e.printStackTrace();
		        	System.out.println(e.getMessage());
		        }

		        if(entr != null && entr < 1.0 )
		        	skip = true;
		        
		        if(s != null && s < 10 )
		        	skip = true;
		        
		        if(!skip)
		        {
		        	out.print(line + "\n");
		        	printed ++;
		        }
		        count++;
		        line = br.readLine();
		    }
		    
		    
		    br.close();
		    out.close();
		    System.out.println("count: " + count + " , printed: " + printed );
		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		    
		}
		
	}
}

