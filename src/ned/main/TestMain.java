
package ned.main;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.StringTokenizer;

public class TestMain {

	public static void main(String[] args) throws Exception {
		
//		GlobalData.getInstance().getParams().resume_mode = true;
//		GlobalData.getInstance().init();
//		System.out.println("------------------------------------------");
//
//		Document doc = GlobalData.getInstance().id2doc.get("93431326086152192");
//		System.out.println(doc==null ? "NULL" : doc.toString());
		
		updateText();
	}

	
	static public void updateText()
	{
		String DELIMITER = " ||| ";
		String filename = "../temp/threads_50000000_13000000/res_short.txt";
		String filenameOut = "../temp/threads_50000000_13000000/res_001.txt";
		
		
		
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
	        System.out.println(line);
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

		        if(entr != null && entr < 1.2 )
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
		    System.out.println("count: " + count + " , printed: " + printed );
		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		    
		}
		
	}
}

