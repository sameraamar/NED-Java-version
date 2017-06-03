package ned.main;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Hashtable;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class LabeledDocuments {

	public Hashtable<String, String> positive;

	public static LabeledDocuments loadLabeledData(String inputfile) throws IOException
	{		
		LabeledDocuments labeled = new LabeledDocuments();
		
		
		FileInputStream stream = new FileInputStream(inputfile);
		Reader decoder = new InputStreamReader(stream, "UTF-8");
		BufferedReader buffered = new BufferedReader(decoder);
		System.out.println("Loading file: " + inputfile + " to memory");
		
		labeled.positive = new Hashtable<String, String> ();		
		
		String line=buffered.readLine();
		int count = 0;
		int failed = 0;
		int skip = 0;
		while(line != null)
        {
			JsonParser jsonParser = new JsonParser();
			JsonObject jsonObj = jsonParser.parse(line).getAsJsonObject();
			String topic = jsonObj.get("topic_id").getAsString();
			String status = jsonObj.get("status").getAsString();
			
			/*JsonElement tweet = jsonObj.get("json");
			if(tweet==null || tweet.isJsonNull())
			{
				skip++;
				line=buffered.readLine();
				continue;
			}*/
			
			String id = jsonObj.get("_id").toString();
			//String json = jsonObj.get("json").toString();
			//Document doc = Document.parse(json, false);
			
			count ++;
			
			StringBuffer sb = new StringBuffer();
			sb.append(id).append(",");
			sb.append(topic).append(",");
			sb.append(status);
			
			labeled.positive.put(id, topic);
						
			if(count % 1000 == 0)
				System.out.println("Loaded " + count);
			
			line=buffered.readLine();
			
        }
		
		buffered.close();
		System.out.println("File loaded: "+count+" successful, "+ failed +" failed, "+ skip +" skipped.");
		return labeled;
	}
}
