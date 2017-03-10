package ned.main;

import ned.types.GlobalData;

public class CleanupThread extends Thread {

	@Override
	public void run() {
		GlobalData gd = GlobalData.getInstance();
		while (true)
		{
			synchronized (gd.cleanClusterQueue)
			{
				
			}
		}
	}
	
}
