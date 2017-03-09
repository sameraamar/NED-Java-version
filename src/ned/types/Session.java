package ned.types;

public class Session {
	public static final int ERROR=0;
	public static final int INFO=1;
	public static final int DEBUG=2;
	public static final int FINE=3;
	
	private int logLevel = INFO;
	
	//singlton
	private static Session instance;
	
	public static Session getInstance() { 
		if (instance == null)
		{
			instance = new Session();
		}
		
		return instance;
	}
	//singlton
	
	private Session() 
	{
	}
	
	public void message(int logLevel, String prefix, String msg) 
	{
		if (this.logLevel < logLevel)
			return;
		
		System.out.println("MSG: " + msg);
	}
	
}
