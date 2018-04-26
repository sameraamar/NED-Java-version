package ned.provider;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;

import ned.types.Session;

public class DocProviderForDemo extends DocProviderGZip {
	public DocProviderForDemo(int maxDocument, int skip, boolean isBasicOnly) 
	{
		super(maxDocument, skip, isBasicOnly);
		
		//String[] gzfilesTemp = {"demo.fgfg.txt" };
		//String[] gzfilesTemp = {"petrovic_11500000.gz" }; 

		//gzfiles = gzfilesTemp;
	}

	@Override
	protected String getBaseFolder()
	{
		if (!Session.checkMachine())
			return "../data/demo";
		
		return "C:\\data\\Thesis\\events_db\\petrovic\\tweets";
	}

	@Override
	protected boolean checkFileNamesFormat(File dir, String name)
	{
		return name.contains(".");
	}
	

}
