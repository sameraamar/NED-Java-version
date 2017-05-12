package ned.main;

import java.io.IOException;

import ned.types.Document;
import ned.types.GlobalData;

public class TestMain {

	public static void main(String[] args) throws Exception {
		
		GlobalData.getInstance().getParams().resume_mode = true;
		GlobalData.getInstance().init();
		System.out.println("------------------------------------------");

		Document doc = GlobalData.getInstance().id2doc.get("86424586052308994");
		System.out.println(doc==null ? "NULL" : doc.toString());
	}

}
