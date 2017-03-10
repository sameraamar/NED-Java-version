package ned.hash;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
	
	public List<String> AddDocument(Document doc)
    {
		HashMap<String, Integer> hitCounts = new HashMap<String, Integer>();
		
		for (int i = 0; i<numberOfTables; i++)
		{
			List<String> tmpList = tables[i].AddDocument(doc);
			
			for (String tmp : tmpList) {
				if (tmp == doc.getId())
					continue;
				
				Integer c = hitCounts.getOrDefault(tmp, 0);
				hitCounts.put(tmp, c+1);
			}
			
		}

        ArrayList<String> output = new ArrayList<String>();
        output.addAll(hitCounts.keySet());
        
        output.sort( new Comparator<String> () 
					        {  
					            @Override  
					            public int compare(String left, String right){  
					                 return hitCounts.get(right) - hitCounts.get(left) ;  //Descending  
					            }  
					            
					        }
        ); 
        
        int compare_with = 3*numberOfTables;
        int toIndex = Math.min(compare_with, output.size());
        List<String> res = output.subList(0, toIndex);
        
        return res;
    }
	
	/*
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
    */
	
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
