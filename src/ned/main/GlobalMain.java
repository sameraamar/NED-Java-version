package ned.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;

import ned.types.GlobalData;
import ned.types.Utility;

public class GlobalMain {

	public static void main(String[] args) throws Exception {
		
		Integer o;
		int max = 1_050_000;
		int start_idx = 8_000_000;
		Integer dimension;
		
		GlobalData.getInstance().getParams().resume_mode = false;
		GlobalData.getInstance().init();
		int jumps = GlobalData.getInstance().getParams().dimension_jumps;

		for(int i=0; i<20; i++) 
		{
			System.out.println("------------------------------------------");
			
			if(i == 0)
			{
				dimension = jumps;
				o = start_idx;
			}
			else{
				GlobalData.release();
				
				GlobalData.getInstance().getParams().resume_mode = true;
				GlobalData.getInstance().init();
				o = 1 + GlobalData.getInstance().resumeInfo.get(GlobalData.LAST_SEEN_IDX);
				o -= 50000;

				dimension = GlobalData.getInstance().resumeInfo.get(GlobalData.LAST_DIMENSION);
				dimension = (dimension / jumps) * jumps;
			}
			
			System.out.println(Utility.humanTime( System.currentTimeMillis()/1000 ));
			boolean resume = (i>0);
			System.out.println(String.format("Iteration %d: offset %d,  max-doc %d, resume? = %b", i+1, o, max, resume));
			runPocess(i+1, o, max, resume, dimension);
			
		}
	}

	private static void runPocess(int i, int o, int max, boolean resume, int dimension) throws Exception {
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
			ProcessBuilder pb = new ProcessBuilder(cmd, "-cp", cp, "-Xms30000m",
					AppMain.class.getName(), 
					String.valueOf(o), 
					String.valueOf(max), 
					String.valueOf(resume),
					String.valueOf(dimension));
			
			System.out.println( pb.command() );
			//ProcessBuilder pb = new ProcessBuilder("ls");
					
		      //pb.directory(Thread.currentThread().pro)
		      //System.out.println( pb.directory().toString() );
		      System.out.println("Redirect output and error to file");
		      File outputFile = new File("c:/temp/Log_" + i + ".txt");
		      File errorFile = new File("c:/temp/ErrLog_" + i + ".txt");
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
