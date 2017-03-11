package ned.main;

import java.io.PrintStream;
import java.util.Set;

import ned.types.GlobalData;

public class CleanupThread extends Thread {
	PrintStream out;
	
	public CleanupThread(PrintStream out)
	{
		this.out = out;
	}
	
	@Override
	public void run() {
		GlobalData gd = GlobalData.getInstance();
		while (true)
		{
			Set<String> list = null;
			synchronized (this)
			{
				try {
					this.wait();
					
					list = null;//gd.prepareListBeforeRelease();
				} catch (InterruptedException e) {
				}
			}

			gd.flushClusters(out, list);
		}
	}
	
}
