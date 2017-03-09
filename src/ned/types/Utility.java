package ned.types;

import java.util.Random;

final public class Utility {

	public static double randomFill(){
        Random rand = new Random();
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
	    	text += "%d day%s";
	    if (diff[1] > 1)
	    	text += "%d hour%s";
	    if (diff[1] > 1)
	    	text += "%d minute%s";
	    if (diff[1] > 1)
	    	text += "%d second%s";
	    */
	    
	    String text = String.format(
		        "%d hour%s, %d minute%s, %d second%s",
		        //"%d day%s, %d hour%s, %d minute%s, %d second%s",
	        //diff[0],
	        //diff[0] > 1 ? "s" : "",
	        diff[1],
	        diff[1] > 1 ? "s" : "",
	        diff[2],
	        diff[2] > 1 ? "s" : "",
	        diff[3],
	        diff[3] > 1 ? "s" : "");
	    
	    return text;
	}
}
