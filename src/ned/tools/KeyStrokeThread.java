package ned.tools;
import java.io.*;

public class KeyStrokeThread extends Thread 
{
	protected boolean stop;

	public KeyStrokeThread()
	{
		setDaemon(true);
	}
	
	@Override
	public void run() 
	{
        try {
        	java.io.InputStreamReader reader = new java.io.InputStreamReader(System.in);
        	stop = false;
        	
            while(!stop)
            {
            	try 
                {
                    int key = System.in.read();
                    // read a character and process it 
                    System.out.println("key pressed");
                    if (key == 'a')
                    {
                    	System.out.println("A clicked");
                    }
                    if (key == 'q' || key == 'Q')
                    {
                    	System.exit(2);
                    }
                } 
                catch (java.io.IOException ioex) 
                {
                    System.out.println("IO Exception");
                }
	            // edit, lets not hog any cpu time
	            try 
	            {
	            	Thread.sleep(50);
	                System.out.println("nop yet");
	            } 
	            catch (InterruptedException ex) {
	                // can't do much about it can we? Ignoring 
	                System.out.println("Interrupted Exception");
	            }
            }
		} 
        catch (Exception e)
        {
        	
        }
        finally 
        {

		}
		
	}
	
	
    public static void main(String[] args) 
    {
    	KeyStrokeThread th = new KeyStrokeThread();
    	
    	th.start();
    	

	}


	public void shutdown() {
		System.out.println("KeyStroke: request to shutdown...");
		stop = true;
	}

}
