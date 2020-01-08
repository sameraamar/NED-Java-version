package ned.provider;

import ned.tools.ClusteringQueueManager;
import ned.types.Document;
import ned.types.GlobalData;

public class DocumentParserThread extends Thread {
	private boolean stop = false;
	public ClusteringQueueManager<Document> queue;
	public DocumentProvider p;
	private int buffer_size;

	public DocumentParserThread()
	{
		queue = new ClusteringQueueManager<Document>();
		
		GlobalData gd = GlobalData.getInstance();
		buffer_size = GlobalData.getInstance().getParams().provider_buffer_size;

		p = new DocProviderGZip(gd.getParams().max_documents, gd.getParams().offset);
	}
	
	
	public void shutdown(boolean stop) {
		this.stop = stop;
	}
	
	@Override
	public void run() 
	{
        try {
    		p.start();
    		doRun();
		} 
        catch (Exception e)
        {
        	e.printStackTrace();
        }
        finally 
        {
        	if(p != null)
        		p.close();
		}
		
	}
	
	private void doRun() throws Exception {

		int countHelper = queue.size();

		
		while(!stop) 
		{
			
			if(countHelper < buffer_size)
			{
				for (; countHelper < 2*buffer_size; countHelper++)
				{
					if(!p.hasNext())
					{
						stop = true;
						break;
					}
					queue.add(p.next());
				}
			}
			else
			{
				try {
					Thread.sleep(10);
					countHelper = queue.size();
				} 
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void shutdown() 
	{
		stop = true;
	}


	public boolean isready() {
		try {
			while (queue.isEmpty())
			{
				if(!stop && p.hasNext())
				{
					//System.out.println("go to sleep!");
					Thread.sleep(100);
				}
				else
					return false;
			}
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}
		
}
