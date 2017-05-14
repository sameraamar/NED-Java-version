package ned.main;

import java.io.IOException;

import ned.types.Document;
import ned.types.GlobalData;

public class TestMain {

	public static void main(String[] args) throws Exception {
		
		GlobalData.getInstance().getParams().resume_mode = true;
		GlobalData.getInstance().init();
		System.out.println("------------------------------------------");

		Document doc = GlobalData.getInstance().id2doc.get("93431326086152192");
		System.out.println(doc==null ? "NULL" : doc.toString());
	}

}
