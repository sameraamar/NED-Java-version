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
	}
	
	protected Runnable createWorker(String line) {
		return new FlattenToCSVWorker(out, line);
	}

}
