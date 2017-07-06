package ned.provider;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ned.tools.ExecutionHelper;
import ned.types.ArrayFixedSize;
import ned.types.Document;
import ned.types.GlobalData;

public abstract class DocumentProvider {
	private int maxDocument;
	private int processed;
	private int cursor;
	private int skip;
	private ArrayFixedSize<Document> buffer;
	private int index;
	private int buffer_size;
	private ArrayFixedSize<Document> buffer_bk;
	private AtomicBoolean bk_running;
	
	public DocumentProvider(int maxDocument, int skip)
	{
		this.buffer_size = GlobalData.getInstance().getParams().provider_buffer_size;
		this.buffer = new ArrayFixedSize<Document>(buffer_size); 
		this.index = 0;
		this.maxDocument = maxDocument;
		this.skip = skip;
		this.cursor = -1;
		this.processed = 0;
	}
	
	public Document next() throws Exception
	{	
		buffer = threadPrepareBuffer_thread();
		
		cursor++;
		processed++;
		
		Document d = buffer.get(index);
		d = handleResumeMode(d);
		//buffer.set(index, null);

		index++;
		return d;
	}

	public boolean hasNext() throws Exception
	{
		if(processed >= maxDocument)
			return false;
		
		if(index < buffer.size())
			return true;
		
		return hasNextHook();
	}
	
	public void start() throws Exception
	{
		cursor = skip;
		startHook(skip);
		bk_running = new AtomicBoolean(false);
	}

	private ArrayFixedSize<Document> threadPrepareBuffer_thread() throws Exception {
		if(buffer_bk == null && bk_running.compareAndSet(false, true))
		{
			System.out.println("Putting a task to prepre new buffer: " + Thread.currentThread().toString());
			Runnable task = () -> {
				try {
					System.out.println("Starting preparing a buffer: " + Thread.currentThread().toString());
					int left = GlobalData.getInstance().getParams().max_documents - this.processed + 1;
					int bs = buffer_size < left ? buffer_size : left; 
					ArrayFixedSize<Document> tmp = new ArrayFixedSize<Document>(bs);
					prepareBuffer(tmp);
					buffer_bk = tmp;
					
					String msg = "A new buffer of size: " + buffer_bk.size() + " ready.";
					System.out.println(msg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			};		
		
			try {
				ExecutionHelper.asyncRun(task);				
			} catch (Exception e) {				
				e.printStackTrace();
			}
			
			
		}
		
		if(index < buffer.size())
			return buffer;

		while(buffer_bk == null)
			Thread.sleep(10);
		
		buffer = buffer_bk;
		bk_running.set(false);
		buffer_bk = null;
		

		index = 0;
		System.out.println("switch buffer...");
		
		return buffer;
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
	
	private ArrayFixedSize<Document> threadPrepareBuffer_singleThread() throws Exception {
		if(index < buffer.size())
			return buffer;
			
		prepareBuffer(buffer);
		
		String msg = "Prepare buffer of size: " + buffer.capacity() + ".... real size is " + buffer.size();
		System.out.println(msg);
		index = 0;
		
		return buffer;
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
	abstract protected void prepareBuffer(ArrayFixedSize<Document> buffer2) throws Exception ;
	abstract protected void startHook(int skip) throws Exception;
	abstract protected boolean hasNextHook() throws Exception;
	
}
