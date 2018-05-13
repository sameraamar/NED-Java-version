package ned.types;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Session {
	public static final int ERROR=0;
	public static final int INFO=1;
	public static final int DEBUG=2;
	public static final int FINE=3;
	
	private int logLevel = INFO;
	
	//singleton
	private static Session instance;
	
	public static Session getInstance() { 
		if (instance == null)
		{
			instance = new Session();
		}
		
		return instance;
	}
	//singleton
	
	private Session() 
	{
	}
	
	public void message(int logLevel, String prefix, String msg) 
	{
		if (this.logLevel < logLevel)
			return;
		
		System.out.println(msg);
	}
	
	public static String getMachineName()
	{
		String hostname = "Unknown";

		try
		{
		    InetAddress addr;
		    addr = InetAddress.getLocalHost();
		    return addr.getHostName().toLowerCase();
		}
		catch (UnknownHostException ex)
		{
		    System.out.println("Hostname can not be resolved");
		}		
		return "";
	}
	
	public static boolean checkMachine()
	{	
		String m = Session.getMachineName();

		String[] myMachines = {"samer", "saaama", "my7pro", "-processor"};
		for (String my : myMachines) {
			if(m.toLowerCase().indexOf(my.toLowerCase())>=0)
				return true;
		}
		
		return false;
	}
}
