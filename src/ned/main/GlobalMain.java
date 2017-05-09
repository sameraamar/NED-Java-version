package ned.main;

import java.io.IOException;

import ned.types.GlobalData;

public class GlobalMain {

	public static void main(String[] args) throws IOException {
		
		int o = 0;
		int max =1_100_000;
		boolean resume = false;

		for(int i=0; i<5; i++) 
		{
			GlobalData.getInstance().getParams().offset = o;
			GlobalData.getInstance().getParams().max_documents = max;
			GlobalData.getInstance().getParams().resume_mode = resume;

			AppMain.main(args);
			
			GlobalData.release();
			
			System.out.println("released global data");
			o += 1_000_000;
			resume = true;
		}
	}

}
