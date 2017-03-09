package ned.hash;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import ned.types.Document;

public class LSHForest {

	private int numberOfTables ;
	private LSHTable[] tables = null;
	
	public LSHForest(int tablesNumer, int hyperPlanesNumber, int dimension, int maxBucketSize) 
	{
		this.numberOfTables = tablesNumer;
		tables = new LSHTable[tablesNumer];
		for (int i = 0; i<tablesNumer; i++)
		{
			tables[i] = new LSHTable(hyperPlanesNumber, dimension, maxBucketSize);
			
		}
	}
	
	public LinkedList<Document> AddDocument(Document doc)
    {
		LinkedList<Document> res = new LinkedList<Document>();
		
		for (int i = 0; i<numberOfTables; i++)
		{
			LinkedList<Document> tmpList = tables[i].AddDocument(doc);
			
			//add to the set
			res.addAll(tmpList);
			
		}
		
		return res;
    }
	
	public HashSet<String> AddDocument2(Document doc)
    {
		HashSet<String> res = new HashSet<String>();
		
		for (int i = 0; i<numberOfTables; i++)
		{
			LinkedList<Document> tmpList = tables[i].AddDocument(doc);
			
			//add to the set
			for (Document entry : tmpList) {
				res.add(entry.getId());
			}
			
		}
		
		return res;
    }
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		
		for (int i = 0; i<numberOfTables; i++) 
		{
			sb.append( tables[i].toString() );
		
			sb.append("\n");
		}
		return sb.toString();
	}
	
	public int getTablesNumber() {
		return numberOfTables;
	}

}
