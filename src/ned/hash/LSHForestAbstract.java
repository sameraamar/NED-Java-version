package ned.hash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import ned.types.Document;

abstract public class LSHForestAbstract {

	protected int numberOfTables;
	protected LSHTable[] tables = null;

	public LSHForestAbstract(int tablesNumer, int hyperPlanesNumber, int dimension, int maxBucketSize) 
	{
		this.numberOfTables = tablesNumer;
		tables = new LSHTable[tablesNumer];
		for (int i = 0; i<tablesNumer; i++)
		{
			tables[i] = new LSHTable(hyperPlanesNumber, dimension, maxBucketSize);
		}
	}

	public ArrayList<Future<List<String>>>  addDocumentFuture(Document doc)
	{
		return null;
	}
	public List<String> processResults(List< Future<List<String>> > neighbors, Document doc)
	{
		return null;
	}

	abstract public List<String> addDocument(Document doc);
	abstract public List<String> addDocument5(Document doc);


	public int getTablesNumber() {
		return numberOfTables;
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
	
	public int getDimension() {
		return this.tables[0].getDimension();
	}

}