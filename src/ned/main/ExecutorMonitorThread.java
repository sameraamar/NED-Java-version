package ned.main;

import java.util.concurrent.ExecutorService;

import ned.types.Session;

public class ExecutorMonitorThread extends Thread {

	protected ExecutorService executor;
	protected int seconds;
	private boolean run = true;

	public ExecutorMonitorThread(ExecutorService executorService, int delay)
    {
        this.executor = executorService;
        this.seconds=delay;
    }
	
	public void shutdown() 
	{
	    this.run=false;
	}

	@Override
	public void run() 
	{
	    while(run)
	    {
	    				
				String msg = this.executor.toString();
				msg = msg.substring(msg.indexOf('[')+1, msg.indexOf(']'));
				Session.getInstance().message(Session.INFO, "[monitor]", "\t" + msg);  		
	
	    		
	    		printHook();
	            
	
	            try {
	                Thread.sleep(seconds*1000);
	                //System.out.println("HELLO");
	            	//Session.getInstance().message(Session.INFO, "[monitor] queue size: " , "" + gd.queue.size());
	                //System.out.println("BYE");
	            } catch (Exception e) {
	            	Session.getInstance().message(Session.INFO, "[monitor]", "Exception");
	            	e.printStackTrace();
	            }
	    }
	
	}

	protected void printHook()
	{
	}

}