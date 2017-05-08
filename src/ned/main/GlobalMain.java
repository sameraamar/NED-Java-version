package ned.main;

import java.io.IOException;

import ned.types.GlobalData;

public class GlobalMain {

	public static void main(String[] args) {
		GlobalData.getInstance().getParams().offset = 0;
		GlobalData.getInstance().getParams().max_documents = 1500000;
		GlobalData.getInstance().getParams().resume_mode = true;

		try {
			
			AppMain.main(args);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
