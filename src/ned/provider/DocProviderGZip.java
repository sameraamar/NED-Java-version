package ned.provider;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import ned.types.ArrayFixedSize;
import ned.types.Document;
import ned.types.GlobalData;
import ned.types.Session;

public class DocProviderGZip extends DocumentProvider {
	private String FOLDER = "";
	private int fileidx;
	private boolean isBasicOnly;
	
	public DocProviderGZip(int maxDocument, int skip, boolean isBasicOnly) {
		super(maxDocument, skip);
		this.isBasicOnly = isBasicOnly;
	}
	
	@Override
	protected Document parseNextHook() throws Exception {
	
		String line = buffered.readLine();
		Document doc = Document.parse(line, isBasicOnly);
		
		return doc;
	}

	@Override
	protected boolean hasNextHook() throws Exception {
		if( buffered == null )
			return false;
		
		boolean isReady = buffered.ready();
		if( !isReady )
		{
			buffered.close();
			buffered = null;
			fileidx++;
			if(!openFile())
				return false;
		}
		
		return true;
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
			FOLDER = "D:\\Thesis\\Backup_Laptop\\events_db\\petrovic";
			//FOLDER= "c:/data/Thesis/events_db/petrovic";

		int offset = skip;
		int skip_files = (offset / 500_000);
		offset = offset % 500_000;
		int offset_p = (int)(offset * 0.05);
		
		fileidx = skip_files;
		Session.getInstance().message(Session.INFO, "Reader", "Jumping to file " + fileidx + ": " + gzfiles[fileidx]);
		
	    if(!openFile())
	    	return;
			
	    while(offset>0)
	    {
	    	if(offset % offset_p == 0)
	    		Session.getInstance().message(Session.INFO, "Reader", "Skipping " + offset + " documents.");
				
	    	String line=buffered.readLine();
	    	offset--;	
	    }

	}
	
	private boolean openFile() {
		GZIPInputStream stream;
		try {
			if(fileidx >= gzfiles.length)
				return false;
			
			System.out.println("opening file (" + fileidx + "): " + gzfiles[fileidx]);
			stream = new GZIPInputStream(new FileInputStream(FOLDER + "/" + gzfiles[fileidx]));

			Reader decoder = new InputStreamReader(stream, "UTF-8");
			buffered = new BufferedReader(decoder);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	String[] gzfiles2 = {"petrovic_00000000.gz",
            "petrovic_00500000.gz"};

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
