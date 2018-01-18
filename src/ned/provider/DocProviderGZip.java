package ned.provider;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import ned.types.ArrayFixedSize;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;

public class DocProviderGZip extends DocumentProvider {
	private String FOLDER = "";
	private int fileidx;
	private boolean isBasicOnly;
	private String loadNewFile = "symaphone";
	
	public DocProviderGZip(int maxDocument, int skip, boolean isBasicOnly) {
		super(maxDocument, skip);
		this.isBasicOnly = isBasicOnly;
	}
	
	@Override
	protected Document parseNextHook() throws Exception {
	
		String line = null;
		String source = null;
		synchronized (loadNewFile) {
			line = buffered.readLine();
			if (line != null)
				source = gzfiles[fileidx];
		}
		Document doc = Document.parse(line, isBasicOnly, source);
		
		return doc;
	}

	@Override
	protected int hasNextHook() throws Exception {
		if( buffered == null )
			return -1;
		
		boolean isReady = buffered.ready();
		if( !isReady )
		{
			synchronized (loadNewFile) {
				fileidx++;
				if(!openFile())
					return 0;
			}
		}
		
		return 1;
	}

	@Override
	protected void closeHook()
	{
		if(buffered != null)
		{
			try {
				buffered.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			buffered = null;
		}
	}
	
	@Override
	protected void startHook(int skip) throws Exception
	{
		FOLDER = "../data";
		if(Session.checkMachine())
			FOLDER= "c:/data/Thesis/events_db"; //"c:/data/Thesis/events_db";

		FOLDER += "/petrovic"; // "/tweets";
		
		int offset = skip;
		int skip_files = (offset / 500_000);
		offset = offset % 500_000;
		int offset_p = (int)(offset * 0.05);
		
		synchronized (loadNewFile) {
			fileidx = skip_files;
			Session.getInstance().message(Session.INFO, "Reader", "Jumping to file " + fileidx + ": " + gzfiles[fileidx]);
			
		    if(!openFile())
		    	return;
		}
			
	    while(offset>0)
	    {
	    	if(offset % offset_p == 0)
	    		Session.getInstance().message(Session.INFO, "Reader", "Skipping " + offset + " documents.");
				
	    	String line=buffered.readLine();
	    	offset--;	
	    }

	}
	
	private boolean openFile() throws IOException 
	{
		GZIPInputStream stream;

		if(fileidx >= gzfiles.length)
			return false;
		
		System.out.println("opening file (" + fileidx + "): " + gzfiles[fileidx]);
		stream = new GZIPInputStream(new FileInputStream(FOLDER + "/" + gzfiles[fileidx]));

		Reader decoder = new InputStreamReader(stream, "UTF-8");
		BufferedReader temp = buffered;
		buffered = new BufferedReader(decoder);
		if (temp != null)
			temp.close();
		
		return true;
	}
	String[] gzfiles2 = {"petrovic_00000000.gz",
    "petrovic_00500000.gz"};

	String[] gzfiles3 = {"corset1.json.gz"};

	String[] gzfiles_datatset2 = { /*"2017_03_07.gz", "2017_03_08.gz", "2017_03_09.gz", */
			"2017_03_10.gz",    "2017_03_11.gz",
			"2017_03_12.gz",    "2017_03_13.gz",    "2017_03_14.gz",    "2017_03_15.gz",    "2017_03_16.gz",    "2017_03_17.gz",    "2017_03_18.gz", 
			"2017_03_19.gz",    "2017_03_20.gz",    "2017_03_21.gz",    "2017_03_22.gz",    "2017_03_23.gz",    "2017_03_24.gz",    "2017_03_25.gz", 
			"2017_03_26.gz",    "2017_03_27.gz",    "2017_03_28.gz",    "2017_03_29.gz",    "2017_03_30.gz",    "2017_03_31.gz",    "2017_04_01.gz", 
			"2017_04_02.gz",    "2017_04_03.gz",    "2017_04_04.gz",    "2017_04_05.gz",    "2017_04_06.gz",    "2017_04_07.gz",    "2017_04_08.gz", 
			"2017_04_09.gz",    "2017_04_10.gz",    "2017_04_11.gz",    "2017_04_12.gz",    "2017_04_13.gz",    "2017_04_14.gz",    "2017_04_15.gz", 
			"2017_04_16.gz",    "2017_04_17.gz",    "2017_04_18.gz",    "2017_04_19.gz",    "2017_04_20.gz",    "2017_04_21.gz",    "2017_04_22.gz", 
			"2017_04_23.gz",    "2017_04_24.gz",    "2017_04_25.gz",    "2017_04_26.gz",    "2017_04_27.gz",    "2017_04_28.gz",    "2017_04_29.gz", 
			"2017_04_30.gz",    "2017_05_01.gz",    "2017_05_02.gz",    "2017_05_03.gz",    "2017_05_04.gz",    "2017_05_05.gz",    "2017_05_06.gz", 
			"2017_05_07.gz",    "2017_05_08.gz",    "2017_05_09.gz",    "2017_05_10.gz",    "2017_05_11.gz",    "2017_05_12.gz",    "2017_05_13.gz", 
			"2017_05_14.gz",    "2017_05_15.gz",    "2017_05_16.gz",    "2017_05_17.gz",    "2017_05_18.gz",    "2017_05_19.gz",    "2017_05_20.gz", 
			"2017_05_21.gz",    "2017_05_22.gz",    "2017_05_23.gz",    "2017_05_24.gz",    "2017_05_25.gz",    "2017_05_26.gz",    "2017_05_27.gz", 
			"2017_05_31.gz",    "2017_06_01.gz",    "2017_06_02.gz",    "2017_06_03.gz",    "2017_06_04.gz",    "2017_06_05.gz",    "2017_06_06.gz", 
			"2017_06_07.gz",    "2017_06_08.gz",    "2017_06_09.gz",    "2017_06_10.gz",    "2017_06_11.gz",    "2017_06_12.gz",    "2017_06_13.gz", 
			"2017_06_14.gz",    "2017_06_15.gz",    "2017_06_16.gz",    "2017_06_17.gz",    "2017_06_18.gz",    "2017_06_20.gz",    "2017_06_21.gz", 
			"2017_06_22.gz",    "2017_06_23.gz",    "2017_06_25.gz",    "2017_06_26.gz",    "2017_06_28.gz",    "2017_06_29.gz",    "2017_06_30.gz", 
			"2017_07_02.gz",    "2017_07_04.gz",    "2017_07_05.gz",    "2017_07_07.gz",    "2017_07_08.gz",    "2017_07_09.gz",    "2017_07_10.gz", 
			"2017_07_11.gz",    "2017_07_12.gz",    "2017_07_13.gz",    "2017_07_14.gz",    "2017_07_15.gz",    "2017_07_16.gz",    "2017_07_17.gz", 
			"2017_07_18.gz",    "2017_07_19.gz",    "2017_07_20.gz",    "2017_07_21.gz",    "2017_07_22.gz",    "2017_07_23.gz",    "2017_07_24.gz", 
			"2017_07_25.gz",    "2017_07_26.gz",    "2017_07_27.gz",    "2017_07_28.gz",    "2017_07_29.gz",    "2017_07_30.gz",    "2017_07_31.gz", 
			"2017_08_01.gz",    "2017_08_02.gz",    "2017_08_03.gz",    "2017_08_04.gz",    "2017_08_05.gz",    "2017_08_06.gz",    "2017_08_07.gz", 
			"2017_08_08.gz",    "2017_08_09.gz",    "2017_08_10.gz",    "2017_08_11.gz",    "2017_08_12.gz",    "2017_08_13.gz",    "2017_08_14.gz", 
			"2017_08_15.gz",    "2017_08_16.gz",    "2017_08_17.gz",    "2017_08_18.gz",    "2017_08_19.gz",    "2017_08_20.gz",    "2017_08_21.gz", 
			"2017_08_22.gz",    "2017_08_23.gz",    "2017_08_24.gz",    "2017_08_25.gz",    "2017_08_26.gz",    "2017_08_27.gz",    "2017_08_28.gz", 
			"2017_08_29.gz",    "2017_08_30.gz",    "2017_08_31.gz",    "2017_09_01.gz",    "2017_09_02.gz",    "2017_09_03.gz",    "2017_09_04.gz", 
			"2017_09_05.gz",    "2017_09_06.gz",    "2017_09_10.gz",    "2017_09_11.gz",    "2017_09_12.gz",    "2017_09_13.gz",    "2017_09_14.gz", 
			"2017_09_15.gz",    "2017_09_16.gz",    "2017_09_17.gz",    "2017_09_18.gz",    "2017_09_19.gz",    "2017_09_20.gz",    "2017_09_21.gz", 
			"2017_09_22.gz",    "2017_09_23.gz",    "2017_09_24.gz",    "2017_09_25.gz",    "2017_09_26.gz",    "2017_09_27.gz",    "2017_09_28.gz", 
			"2017_09_29.gz",    "2017_09_30.gz",    "2017_10_01.gz",    "2017_10_02.gz",    "2017_10_03.gz",    "2017_10_04.gz",    "2017_10_05.gz", 
			"2017_10_06.gz",    "2017_10_07.gz",    "2017_10_08.gz",    "2017_10_09.gz",    "2017_10_10.gz",    "2017_10_11.gz",    "2017_10_12.gz", 
			"2017_10_13.gz",    "2017_10_14.gz",    "2017_10_15.gz",    "2017_10_16.gz",    "2017_10_17.gz",    "2017_10_18.gz",    "2017_10_19.gz", 
			"2017_10_20.gz",    "2017_10_21.gz",    "2017_10_22.gz",    "2017_10_23.gz",    "2017_10_24.gz",    "2017_10_25.gz",    "2017_10_26.gz", 
			"2017_10_27.gz",    "2017_10_28.gz",    "2017_10_29.gz",    "2017_10_30.gz",    "2017_10_31.gz",    "2017_11_01.gz",    "2017_11_02.gz", 
			"2017_11_03.gz",    "2017_11_04.gz",    "2017_11_05.gz",    "2017_11_06.gz",    "2017_11_07.gz",    "2017_11_08.gz",    "2017_11_09.gz", 
			"2017_11_10.gz",    "2017_11_11.gz",    "2017_11_12.gz",    "2017_11_13.gz",    "2017_11_14.gz",    "2017_11_15.gz",    "2017_11_16.gz", 
			"2017_11_17.gz",    "2017_11_18.gz",    "2017_11_19.gz",    "2017_11_20.gz",    "2017_11_21.gz",    "2017_11_22.gz",    "2017_11_23.gz", 
			"2017_11_24.gz",    "2017_11_25.gz",    "2017_11_26.gz",    "2017_11_27.gz",    "2017_11_28.gz",    "2017_11_29.gz",    "2017_11_30.gz", 
			"2017_12_01.gz",    "2017_12_02.gz",    "2017_12_03.gz",    "2017_12_04.gz",    "2017_12_05.gz",    "2017_12_06.gz",    "2017_12_07.gz", 
			"2017_12_08.gz",    "2017_12_09.gz",    "2017_12_10.gz",    "2017_12_11.gz",    "2017_12_12.gz",    "2017_12_13.gz",    "2017_12_14.gz", 
			"2017_12_15.gz",    "2017_12_16.gz", "2017_12_16.gz", "2017_12_17.gz"} ; //,    "2017_12_25.gz"};
	
	String[] gzfiles = {"petrovic_00000000.gz",
                    "petrovic_00500000.gz",
                    "petrovic_01000000.gz",
                    "petrovic_01500000.gz",
                    "petrovic_02000000.gz",
                    "petrovic_02500000.gz",
                    "petrovic_03000000.gz",
                    "petrovic_03500000.gz",
                    "petrovic_04000000.gz",
                    "petrovic_04500000.gz",
                    "petrovic_05000000.gz",
                    "petrovic_05500000.gz",
                    "petrovic_06000000.gz",
                    "petrovic_06500000.gz",
                    "petrovic_07000000.gz",
                    "petrovic_07500000.gz",
                    "petrovic_08000000.gz",
                    "petrovic_08500000.gz",
                    "petrovic_09000000.gz",
                    "petrovic_09500000.gz",
                    "petrovic_10000000.gz",
                    "petrovic_10500000.gz",
                    "petrovic_11000000.gz",
                    "petrovic_11500000.gz",
                    "petrovic_12000000.gz",
                    "petrovic_12500000.gz",
                    "petrovic_13000000.gz",
                    "petrovic_13500000.gz",
                    "petrovic_14000000.gz",
                    "petrovic_14500000.gz",
                    "petrovic_15000000.gz",
                    "petrovic_15500000.gz",
                    "petrovic_16000000.gz",
                    "petrovic_16500000.gz",
                    "petrovic_17000000.gz",
                    "petrovic_17500000.gz",
                    "petrovic_18000000.gz",
                    "petrovic_18500000.gz",
                    "petrovic_19000000.gz",
                    "petrovic_19500000.gz",
                    "petrovic_20000000.gz",
                    "petrovic_20500000.gz",
                    "petrovic_21000000.gz",
                    "petrovic_21500000.gz",
                    "petrovic_22000000.gz",
                    "petrovic_22500000.gz",
                    "petrovic_23000000.gz",
                    "petrovic_23500000.gz",
                    "petrovic_24000000.gz",
                    "petrovic_24500000.gz",
                    "petrovic_25000000.gz",
                    "petrovic_25500000.gz",
                    "petrovic_26000000.gz",
                    "petrovic_26500000.gz",
                    "petrovic_27000000.gz",
                    "petrovic_27500000.gz",
                    "petrovic_28000000.gz",
                    "petrovic_28500000.gz",
                    "petrovic_29000000.gz",
                    "petrovic_29500000.gz"  
                   };
	private BufferedReader buffered;

}
