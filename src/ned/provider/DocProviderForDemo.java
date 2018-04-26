package ned.provider;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;

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
		return "C:\\data\\Thesis\\events_db\\petrovic\\tweets";
	}

	@Override
	protected boolean checkFileNamesFormat(File dir, String name)
	{
		return name.contains(".");
	}
	

}
