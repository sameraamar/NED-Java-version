package ned.main;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.StringTokenizer;

import ned.types.Document;
import ned.types.GlobalData;

public class TestMain {

	public static void main(String[] args) throws Exception {
		
		GlobalData.getInstance().getParams().resume_mode = true;
		GlobalData.getInstance().init();
		System.out.println("------------------------------------------");

		String[] ids = {"97829502591320067", "98454114152878081", "97986298270330880", "98199872209035265", "98454114152878081", "98079151780675586",
				"90224059228499969", "90239997587886080", "91813843075993600"};
		
		for (String id : Arrays.asList( ids )) {
			Document doc = GlobalData.getInstance().id2doc.get(id);
			System.out.println(doc==null ? "NULL" : doc.toString());
		}
		
		//updateText();
	}

	
	static public void updateText()
	{
		String DELIMITER = " ||| ";
		String filename = "C:\\temp\\threads_50000000_4000000\\short_000.txt";
		String filenameOut = "C:\\temp\\threads_50000000_4000000\\short_000_out.txt";
		
		
		
		BufferedReader br = null;
		try {
			PrintStream out = new PrintStream(new FileOutputStream(filenameOut));

			br = new BufferedReader(new FileReader(filename));
		    String line = br.readLine();
		    
		    out.print(line + "\n");

		    int count = 0;
		    int printed = 0;
		    String leadId = null, entropy = null;
		    String size;
	        line = br.readLine();
		    while (line != null) {
		        if(line.trim().equals(""))
		        	continue;
		        
		    	StringTokenizer tk = new StringTokenizer(line, DELIMITER);
		    	leadId = tk.nextToken();
		    	entropy = tk.nextToken();
		    	size = tk.nextToken();
		    	size = tk.nextToken();
		    	
		    	Double entr = null;
		    	Integer s = null;
		    	
		        boolean skip = false;
		        try {
		        	entr = Double.parseDouble(entropy);
		        } catch (Exception e)
		        {
		        }

		        try {
		        	s = Integer.parseInt(size);
		        } catch (Exception e)
		        {
		        }

		        if(entr != null && entr < 1.1 )
		        	skip = true;
		        
		        if(entr != null && s < 10 )
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
		    System.out.println("count: " + count + " , printed: " + printed );
		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		    
		}
		
	}
}
