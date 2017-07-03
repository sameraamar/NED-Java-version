package ned.provider;

import ned.types.Document;

public abstract class DocumentProvider {
	private int maxDocument;
	private int processed;
	private int cursor;
	private int skip;

	public DocumentProvider(int maxDocument, int skip)
	{
		this.maxDocument = maxDocument;
		this.skip = skip;
		this.cursor = -1;
		this.processed = 0;
	}
	
	public Document next() throws Exception
	{	
		Document doc = findNextHook();
		cursor++;
		processed++;
		return doc;
	}

	public boolean hasNext() throws Exception
	{
		if(processed >= maxDocument)
			return false;
		
		return hasNextHook();
	}
	
	public void start() throws Exception
	{
		cursor = skip;
		startHook(skip);
	}

	public void close() throws Exception {
		closeHook();
	}
	
	public int getCursor()
	{
		return cursor;
	}
	
	public int getProcessed()
	{
		return processed;
	}
	
	abstract protected void closeHook() throws Exception;
	abstract protected Document findNextHook() throws Exception ;
	abstract protected void startHook(int skip) throws Exception;
	abstract protected boolean hasNextHook() throws Exception;
	
}
