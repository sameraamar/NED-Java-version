package ned.provider;

import ned.tools.ClusteringQueueManager;

public class DocumentParserThread extends Thread {
	private boolean stop = false;
	ClusteringQueueManager queue;

	public DocumentParserThread()
	{
		queue = new ClusteringQueueManager();
	}
	
	public void stop(boolean stop) {
		this.stop = stop;
	}
	
	@Override
	public void run() 
	{
        try {
			doRun();
		} finally {
			
		}
		
	}
	
	private void doRun() {

		while(!stop) 
		{
			
			
		}
		
	}
	
	public void shutdown() 
	{
		stop = true;
	}
		
}
