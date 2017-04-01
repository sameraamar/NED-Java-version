package ned.hash;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import ned.types.Document;

public class WorkerLSHTable implements Callable<List<String>> 
{
	private LSHTable table;
	private Document doc;
	
	public WorkerLSHTable(LSHTable table, Document doc)
	{
		this.doc = doc;
		this.table = table;		
	}

	@Override
	public List<String> call() throws Exception {
		List<String> neighbors = Collections.synchronizedList( table.AddDocument(doc) );
		return neighbors;
	}

	

}
