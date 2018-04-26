package ned.main;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import ned.modules.Twokenize;
import ned.provider.DocProviderGZip;
import ned.provider.DocumentParserThread;
import ned.provider.DocumentProvider;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.RedisBasedMap;
import ned.types.SerializeHelperAdapterDirtyBit;

public class FlattenJSONTweets {
	private static DocumentParserThread parserThread;

	private static String DELIMITER = "\t";

	public static void main(String[] args) throws Exception 
	{
		String fileName = args[0];
		
		System.out.println("Writing to " + fileName);
		PrintStream out = new PrintStream(new FileOutputStream(fileName));		
		
		PrintStream outJson = null;
		if (args.length>=2) //json option
		{
			String fileNameJson = args[2];
			System.out.println("Writing to " + fileNameJson);
			outJson = new PrintStream(new FileOutputStream(fileNameJson));
		}
		
		GlobalData gd = GlobalData.getInstance();
		DocumentProvider documentProvider = new DocProviderGZip(gd.getParams().max_documents, gd.getParams().offset, true);
    	parserThread = new DocumentParserThread(documentProvider);
    	parserThread.start();
    	
    	int cursor = 0;
    	Thread.sleep(500);
    	StringBuilder sb = new StringBuilder();
    	sb.append("source").append(DELIMITER);
    	sb.append("tweet_id").append(DELIMITER);
    	sb.append("timestamp").append(DELIMITER);
    	sb.append("created_at").append(DELIMITER);
    	sb.append("like").append(DELIMITER);
    	sb.append("retweet").append(DELIMITER);
    	sb.append("reply").append(DELIMITER);
    	sb.append("quote").append(DELIMITER);
    	sb.append("hashtags").append(DELIMITER);
    	sb.append("MultipleWordsTag").append(DELIMITER);
    	sb.append("symbols").append(DELIMITER);
    	sb.append("urls").append(DELIMITER);
    	sb.append("mentions").append(DELIMITER);
    	sb.append("is_retweet").append(DELIMITER);
    	sb.append("is_quote").append(DELIMITER);
    	sb.append("is_reply").append(DELIMITER);
    	sb.append("clean_text").append("\n");
    	
		out.print(sb.toString());
		
		String current = null;
    	while(parserThread.isready())
		{
			Document d = parserThread.queue.poll();
			while(d == null)
			{
				Thread.sleep(5);
				d = parserThread.queue.poll();
			}
			
			StringBuilder line = new StringBuilder();
			line.append(d.getSource());
			line.append(DELIMITER);
			line.append(d.getId());
			line.append(DELIMITER);
			line.append(d.getTimestamp());
			line.append(DELIMITER);
			line.append(d.getCreatedAt());
			line.append(DELIMITER);
			line.append(d.getFavouritesCount());
			line.append(DELIMITER);
			line.append(d.getReplyTo());
			line.append(DELIMITER);
			line.append(d.getRetweetedId());
			line.append(DELIMITER);
			line.append(d.getQuotedStatusId());
			line.append(DELIMITER);
			line.append(d.getHashtags());
			line.append(DELIMITER);
			line.append(d.getMultipleWordsTag());
			line.append(DELIMITER);
			line.append(d.getSymbols());
			line.append(DELIMITER);
			line.append(d.getURLs());
			line.append(DELIMITER);
			line.append(d.getUserMentions());
			line.append(DELIMITER);
			line.append(isNullOrEmpty(d.getRetweetedId()) ? 0 : 1);
			line.append(DELIMITER);
			line.append(isNullOrEmpty(d.getQuotedStatusId()) ? 0 : 1);
			line.append(DELIMITER);
			line.append(isNullOrEmpty(d.getReplyTo()) ? 0 : 1);
			line.append(DELIMITER);
			line.append(d.getCleanText());
			//line.append(DELIMITER);
			//line.append(d.getText());
			
			line.append("\n");

			out.print(line);
			outJson.print(d.getJson());
			outJson.print("\n");
			
	    	
			cursor++;
			if(current == null)
				current = d.getSource();
			
			if(!current.equals(d.getSource()))
			{
				System.out.println(current + " processed: " + cursor);
				current = d.getSource();
			}
			
			/*********** deal (WA) with some strange bug... sometimes the processing stops before the new file is ready *********/
			int X = 500000;
			int temp = (cursor+3) % X;
			if (temp <= 6)
			{
				//System.out.println("Sleeping 0.5 sec..." + parserThread.isready());
				Thread.sleep(500);  //wait 0.5 sec to give the provider to read more. Bug!!!
			}

		}
    	
    	out.close();
		System.out.println("Finished. Processed: " + cursor);
    	parserThread.shutdown();
	}

	private static boolean isNullOrEmpty(String str) {
		return str==null || str.equals("");
	}

}

