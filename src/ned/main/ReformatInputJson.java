package ned.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ReformatInputJson {

	public static String reformat(String text)
	{
		Gson gson = new GsonBuilder().setLenient().create();

		JsonArray results = gson.fromJson(text, JsonArray.class);	        

		Iterator<JsonElement> resultsItr = results.iterator();

		StringBuilder sb = new StringBuilder();
		
		while(resultsItr.hasNext()) 
		{
		    JsonElement elem = resultsItr.next();
		    JsonObject obj = elem.getAsJsonObject();

		    JsonElement user = obj.get("user");

		    JsonObject userObject = new JsonObject();
		    userObject.add("id_str", user);
		    
		    obj.add("user", userObject);
		    
		    sb.append(elem.toString());
		    sb.append('\n');
		}
		
		System.out.println("handled " + results.size() + " json objects");
		return sb.toString();
	}
	
	
	public static void main(String[] args) throws IOException 
	{
		String filename; // = "C:/temp/liacom_tweets_partial.txt";
		String ofilename; // = "C:/temp/liacom_tweets_partial.new.txt";
		
		if (args.length<2)
		{
			System.out.println("Give two paraneters: <orig-file-name> <out-file-name>");
			return;
		}
		
		filename = args[0];
		ofilename = args[1];
		
		System.out.println("Read from: " + filename);
		System.out.println("Write to : " + ofilename);
		
		FileInputStream fr = new FileInputStream(filename);
		InputStreamReader fi = new InputStreamReader(fr, StandardCharsets.UTF_8);
		BufferedReader r = new BufferedReader(fi);
		
		String line = r.readLine();
		StringBuilder content = new StringBuilder();
		
		while (line != null)
		{
			line = line.trim();
			
			if (line.length() > 0)
				content.append(line);
			
			line = r.readLine();
		}
		
		
		String text = reformat(content.toString());

		FileOutputStream fos = new FileOutputStream(ofilename);
		OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
		BufferedWriter w = new BufferedWriter(osw);
		
		w.write(text);
		
		w.close();
		r.close();
	}

}
