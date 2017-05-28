package ned.hash;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;
import ned.types.DocumentClusteringHelper;

public class DocumentHandler {
//	private static LinkedList<DocumentHandler>  list;
//
//	LSHForest forest;
//	Document doc;
	
//	Document nearest;
//	Double dist;
	
	public DocumentHandler(LSHForest forest, Document doc) 
	{
//		this.doc = doc;
//		this.forest = forest;
	}
	
	
//	@Override
//	public void run() 
//	{
//		List<String> set = forest.AddDocument(this.doc);
//
//        Object[] candidate = DocumentClusteringHelper.postLSHMapping(this.doc, set);
//        
//        nearest = (Document)candidate[0];
//        dist = (Double)candidate[1];
//		DocumentClusteringHelper.mapToClusterHelper(doc, nearest, dist);
//
//	}
	
	//**************************************************************
	public static Document preprocessor(String json)
	{
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();
		String text = jsonObj.get("text").getAsString();
		String id = jsonObj.get("id_str").getAsString();
		String created_at = jsonObj.get("created_at").getAsString();
		long timestamp = jsonObj.get("timestamp").getAsLong();
				
        Document doc = Document.createOrGet(id);
        doc.init(text, timestamp);
        
        doc.setCreatedAt(created_at);
        //Hashtable<Integer, Double> weights = doc.getWeights();

        if(doc.getUserId() == null)
        {
        	JsonObject userObj = jsonObj.get("user").getAsJsonObject();
            doc.setUserId( userObj.get("id_str").getAsString() );			
        }
		
    	return doc;
	}
	
//	public static boolean process(LSHForest forest, Document doc)
//	{
//		if(list == null)
//		{
//			list = new LinkedList<DocumentHandler>();
//		}
// 		
//		DocumentHandler h = new DocumentHandler(forest, doc);
//		h.start();
//		list.add(h);
//		
//		GlobalData gd = GlobalData.getInstance();
//		if (list.size() == gd.getParams().number_of_threads)
//		{
//			waitForSuProcesses();
//			return true;
//		}
//		return false;
//	}
	
//	public static void waitForSuProcesses()
//	{
//
//		while(!list.isEmpty())
//		{
//			try 
//			{
//				DocumentHandler h = list.poll();
//				h.join();
//				//ThreadManagerHelper.mapToClusterHelper(h.doc, h.nearest, h.dist);
//			} 
//			catch (InterruptedException e) 
//			{
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	
//		}
//
//	}	
}
