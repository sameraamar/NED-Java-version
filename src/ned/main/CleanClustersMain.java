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
import ned.provider.DocumentParserThread;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.RedisBasedMap;
import ned.types.SerializeHelperAdapterDirtyBit;

public class CleanClustersMain {
	private static DocumentParserThread parserThread;

	private static String DELIMITER = "\t";

	public static void main(String[] args) throws Exception 
	{
		String fileName = args[0];
		
		System.out.println("Writing to " + fileName);
		PrintStream out = new PrintStream(new FileOutputStream(fileName));

    	parserThread = new DocumentParserThread(false);
    	parserThread.start();
    	
    	int cursor = 0;
    	Thread.sleep(500);
    	StringBuilder sb = new StringBuilder();
    	sb.append("tweet_id").append(DELIMITER);
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
		
    	while(parserThread.isready())
		{
			Document d = parserThread.queue.poll();
			while(d == null)
			{
				Thread.sleep(5);
				d = parserThread.queue.poll();
			}
			
			StringBuilder line = new StringBuilder();
			line.append(d.getId());
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
	    	
			cursor++;
			if(cursor % 10000 == 0)
				System.out.println("processed: " + cursor);
		}
    	
    	out.close();
		System.out.println("Finished. Processed: " + cursor);
    	parserThread.shutdown();
	}

	private static boolean isNullOrEmpty(String str) {
		return str==null || str.equals("");
	}

}

