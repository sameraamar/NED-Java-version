package ned.tools;

import java.io.PrintStream;
import ned.types.Document;

public class FlattenToCSVWorker extends ProcessorWorker
{
	PrintStream out;
	
    public FlattenToCSVWorker(PrintStream out, String docJson)
    {
        super( docJson );
        this.out = out;
    }

    @Override
    protected void processCommand() 
    {

    	Document doc = Document.parse(docJson, false);
		if(doc == null)
			return;
		
		
		writeToCSV(doc);
		
    }
    protected void writeToCSV(Document doc) 
    {
		String jRply = handleNull(doc.getReplyTo());
		
		String jRtwt = handleNull(doc.getRetweetedId());
		int retweets = doc.getRetweetCount();
		String created_at = doc.getCreatedAt();
		String id = doc.getId();
		long timestamp = doc.getTimestamp();
		String userId = doc.getUserId();
		int likes = doc.getFavouritesCount();
		
		StringBuffer sb = new StringBuffer();
		sb.append(id).append(",");
		sb.append(userId).append(",");
		sb.append(created_at).append(",");
		sb.append(timestamp).append(",");
		sb.append(retweets).append(",");
		sb.append(likes).append(",");
		sb.append(jRtwt).append(",");
		sb.append(jRply);
		
		out.println(sb.toString());
    }

	private String handleNull(String value) 
	{
		return value==null ? "" : value; 
	}

}