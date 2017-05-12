package ned.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;

public class GlobalMain {

	public static void main(String[] args) throws Exception {
		
		int o = 0;
		int max = 50_001;
		boolean resume = false;

		for(int i=0; i<20; i++) 
		{
			System.out.println("------------------------------------------");
			System.out.println(String.format("Iteration %d: offset %d,  max-doc %d, resume? = %b", i+1, o, max, resume));
			
			runPocess(i+1, o, max, resume);
			
			o += max - 2000;
			resume = true;
		}
	}

	private static void runPocess(int i, int o, int max, boolean resume) throws Exception {
		try {
		      String cmd = "java";
		      
		      ClassLoader cl = ClassLoader.getSystemClassLoader();

		        URL[] urls = ((URLClassLoader)cl).getURLs();

		        String cp = ".";
		        for(URL url: urls){
		        	cp += ";";
		        	cp += url.getFile().substring(1);
		        }
		      
		       System.out.println(cp);
			ProcessBuilder pb = new ProcessBuilder(cmd, "-cp", cp, 
					AppMain.class.getName(), 
					String.valueOf(o), 
					String.valueOf(max), 
					String.valueOf(resume));
			
			System.out.println( pb.command() );
			//ProcessBuilder pb = new ProcessBuilder("ls");
					
		      //pb.directory(Thread.currentThread().pro)
		      //System.out.println( pb.directory().toString() );
		      System.out.println("Redirect output and error to file");
		      File outputFile = new File("c:/temp/Log.txt");
		      File errorFile = new File("c:/temp/ErrLog.txt");
		      pb.redirectOutput(outputFile);
		      pb.redirectError(errorFile);

		      
		      if(errorFile.length() > 0)
		      {
		    	  System.out.println("There are errors in " + errorFile.getName());
		    	  throw new Exception("There are errors in " + errorFile.getName());
		      }
		      
		      Process childProcess = pb.start();
		      
		      
		      Thread closeChildThread = new Thread() {
		    	    public void run() {
		    	        childProcess.destroy();
		    	    }
		    	};

		    	Runtime.getRuntime().addShutdownHook(closeChildThread); 
		      
//		      while (myProcess.isAlive())
//		      {
//		  		try {
//		    	  Thread.sleep(10000);
//		  		}
//		  		catch (Exception err) {
//					err.printStackTrace();
//				}
//		      }
		      
		      
		      int errCode = childProcess.waitFor();
		      System.out.println("Command executed, any errors? " + (errCode == 0 ? "No" : "Yes"));
		      System.out.println("Echo Output:\n" + output(childProcess.getInputStream()));   
		}
		catch (Exception err) {
			err.printStackTrace();
			throw new Exception(err);
		}
	}

	private static String output(InputStream inputStream) throws IOException {
		StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line + System.getProperty("line.separator"));
            }
        } finally {
            br.close();
        }
        return sb.toString();
	}

}
