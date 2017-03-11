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
			Set<Integer> list = null;
			synchronized (this)
			{
				try {
					this.wait();
					
					list = gd.prepareListBeforeRelease();
				} catch (InterruptedException e) {
				}
			}

			if (list != null)
				gd.flushClusters(out, list);
		}
	}
	
}
