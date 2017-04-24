package ned.types;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

final public class Utility {
	private static Random rand = new Random();

	private Utility()
	{
	}
    
	public static double randomFill(){
        double randomNum = 2.0 * rand.nextDouble() - 1.0;
        return randomNum;
    }
    
	public static String humanTime(long seconds)
	{
		long diff[] = new long[] { 0, 0, 0, 0 };
	    /* sec */diff[3] =   (seconds >= 60 ? seconds % 60 : seconds);
	    /* min */diff[2] =   (seconds = (seconds / 60)) >= 60 ? seconds % 60 : seconds;
	    /* hours */diff[1] = (seconds = (seconds / 60)) >= 24 ? seconds % 24 : seconds;
	    /* days */diff[0] =  (seconds = (seconds / 24));

	    /*
	    String text = "";
	    if (diff[0] > 1)
	    	text += "%d d%s";
	    if (diff[1] > 1)
	    	text += "%d h%s";
	    if (diff[1] > 1)
	    	text += "%d m%s";
	    if (diff[1] > 1)
	    	text += "%d s%s";
	    */
	    
	    String text = String.format(
		        //"%d hour%s, %d minute%s, %d second%s",
		        "%d days %02d:%02d:%02d",
	        diff[0],
	        //diff[0] > 1 ? "s" : "",
	        diff[1],
	        //diff[1] > 1 ? "s" : "",
	        diff[2],
	        //diff[2] > 1 ? "s" : "",
	        diff[3]
	        //diff[3] > 1 ? "s" : ""
	        );
	    
	    return text;
	}
	
	public static void main(String[] args) throws IOException 
	{
		Set<Integer> s = new HashSet<Integer>();
		
		s.add(6);
		s.add(8);
		s.add(6);
		s.add(8);
		s.add(8);
		s.add(6);
		s.add(9);
		
		System.out.println("numbers");
		for(int i=0;i<10;i++)
			System.out.println( randomFill() );
		
		System.out.println(s);
	}
	
}
