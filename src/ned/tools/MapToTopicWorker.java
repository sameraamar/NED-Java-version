package ned.tools;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import ned.hash.LSHForest;
import ned.types.Document;
import ned.types.DocumentClusteringHelper;
import ned.types.GlobalData;
import ned.types.Session;

public class MapToTopicWorker implements Runnable 
{
	private String docJson;
	Map<String, JudgmentEntry> map;
	
    public MapToTopicWorker(Map<String, JudgmentEntry> map, String docJson)
    {
        this.docJson = docJson;
        this.map = map;
    }

    public void run() 
    {
        processCommand();
    }

    private void processCommand() 
    {
    	Document doc = Document.parse(docJson, false);
		if(doc == null)
			return;
    	
		
		for (JudgmentEntry item : map.values()) {
			if( compareToReplyTweet(item.doc, doc ) )
			{
				addToLabeledDS(doc.getId(), doc, docJson, item.topic);
				addToLabeledDS(doc.getReplyTo(), null, null, item.topic);
				addToLabeledDS(doc.getRetweetedId(), null, null, item.topic);
			}
		}
				
    }

	private void addToLabeledDS(String id, Document doc, String docJson, int topic) 
	{
		if (id == null)
			return;
		
		JudgmentEntry current = map.getOrDefault(id, null);

		
		if (current == null)
		{
			JudgmentEntry judgmentEntry = new JudgmentEntry();
			judgmentEntry.doc = doc;
			judgmentEntry.docJson = docJson;
			judgmentEntry.topic = topic;
			
			map.put(id, judgmentEntry);
			ExpandPositiveDS.counter++;
		}
		
		else { 
			if (current.doc == null)
			{
				current.doc = doc;
				current.topic = topic;
				current.docJson = docJson;
			}
			assert(current.topic == topic);
		}

	}

	private boolean compareToReplyTweet(Document doc, Document neighbor) 
	{
		boolean is_reply = isReply(doc, neighbor);
		
		is_reply = is_reply || isReply(neighbor, doc);
		
		return is_reply;
	}

	private boolean isReply(Document doc, Document neighbor) {
		String reply = doc.getReplyTo();
		if(reply != null && neighbor.equals(reply) )
			return true;
		
		String retweet = doc.getRetweetedId();
		if(reply != null && neighbor.equals(retweet) )
			return true;
		
		return false;
	}
}