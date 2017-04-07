package ned.tools;

import java.io.PrintStream;
import java.util.concurrent.Executors;

import ned.types.HashtableRedis;

public class FlattenToCSVExecutor extends ProcessorExecutor {
	PrintStream out;
	private HashtableRedis<String> id2group;
	
	public FlattenToCSVExecutor(PrintStream out, int number_of_threads)
	{
		super(number_of_threads);
		this.out = out;
		out.println( "id,userId,created_at,timestamp,retweets,likes,jRtwt,jRply,topic_id,group,level" );
	}
	
	protected Runnable createWorker(String line) {
		return new FlattenToCSVWorker(out, line);
	}

}
