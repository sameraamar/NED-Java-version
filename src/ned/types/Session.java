package ned.types;

public class Session {
	public static final int ERROR=0;
	public static final int INFO=1;
	public static final int DEBUG=2;
	public static final int FINE=3;
	
	private int logLevel = INFO;
	
	//singleton
	private static Session instance;
	
	public boolean isDebugMode()
	{
		return this.logLevel >= DEBUG;
	}
	
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
	
}
