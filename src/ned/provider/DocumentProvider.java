package ned.provider;

import ned.types.Document;
import ned.types.GlobalData;

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
		cursor++;
		processed++;
		
		Document d = parseNextHook();
		d = handleResumeMode(d);

		return d;
	}

	abstract protected Document parseNextHook() throws Exception;

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

	public static Document handleResumeMode(Document newDoc)
	{
		if (!GlobalData.getInstance().getParams().resume_mode)
			return newDoc;
		
		String id = newDoc.getId().intern();
		synchronized (id) {
			Document doc = GlobalData.getInstance().id2doc.get(id);
			if(doc == null)
			{
				GlobalData.getInstance().id2doc.put(id, newDoc);
				return newDoc;
			}
				
			return doc;			
		}
	}
	
	public void close() {
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
	
	abstract protected void closeHook();
	abstract protected void startHook(int skip) throws Exception;
	abstract protected boolean hasNextHook() throws Exception;
	
}
