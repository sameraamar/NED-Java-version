package ned.main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

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

	
	public void updateText()
	{
		String filename = "c:/temp/threads_7m_not_filtered.txt";
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();

		    while (line != null) {
		        sb.append(line);
		        sb.append(System.lineSeparator());
		        line = br.readLine();
		    }
		    String everything = sb.toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		    br.close();
		}
	}
}
