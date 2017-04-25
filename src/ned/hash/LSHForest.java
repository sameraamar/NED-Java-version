package ned.hash;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import ned.tools.ExecutionHelper;
import ned.tools.GeneralHelper;
import ned.types.Document;

public class LSHForest {
	
	private int numberOfTables ;
	private LSHTable[] tables = null;
	
	public LSHForest(int tablesNumer, int hyperPlanesNumber, int dimension, int maxBucketSize) 
	{
		long base = System.currentTimeMillis();
		this.numberOfTables = tablesNumer;
		tables = new  LSHTable[tablesNumer];
		
		for (int i = 0; i<tablesNumer; i++)
		{
			LSHTable table = new LSHTable(i,hyperPlanesNumber, dimension, maxBucketSize);
			tables[i] = table;
		}
		/*
		Arrays.stream(tables).parallel()
			.forEach(table -> {
				
				table.init();
			
		});
		
		*/
		for (int i = 0; i<tablesNumer; i++)
		{
			tables[i].init();
		}

		System.out.println("LSHForest.init: " + (System.currentTimeMillis()-base));
	}
	
	public List<String> addDocument00(Document doc)
    {
		ConcurrentHashMap<String, Integer> hitCounts = new ConcurrentHashMap<String, Integer>();

		for (int i = 0; i<numberOfTables; i++)
		{
			List<String> tmpList = tables[i].AddDocument(doc);
			
			for (String tmp : tmpList) {
				if ( tmp.compareTo(doc.getId()) >=0 )
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
	public List<String> addDocument32(Document doc)
    {
		
		HashMap<String, Integer> hitCounts = new HashMap<String, Integer>();


        for (int i = 0; i<numberOfTables; i++)
		{
			List<String> tmpList = tables[i].AddDocument(doc);
			
			for (String tmp : tmpList) {
				if ( tmp.compareTo(doc.getId()) >=0 )
					continue;
				
				Integer c = hitCounts.getOrDefault(tmp, 0);
				hitCounts.put(tmp, c+1);
			}
			
		}
		
        return findTopX(hitCounts);
    }

	protected List<String> findTopX(HashMap<String, Integer> hitCounts) {
		PriorityQueue<String> pqueue = new PriorityQueue<String>(new Comparator<String> () 
        {  
            public int compare(String left, String right){  
            	return hitCounts.get(right)-hitCounts.get(left); //descending order
            }  
            
        });
		
		for (String k : hitCounts.keySet()) {
			pqueue.add(k);
		}
		
		ArrayList<String> output = new ArrayList<String>();

		int compare_with = 3*numberOfTables;
        int toIndex = Math.min(compare_with, pqueue.size());
		for(int i=0; i<toIndex; i++)
			output.add( pqueue.poll() );
		
		return output;
 	}
	
	protected List<String> findTopX1(HashMap<String, Integer> hitCounts) {
		TreeMap<String, Integer> sorted = GeneralHelper.sortMapByValue(hitCounts);
		 ArrayList<String> output = new ArrayList<String>();
	       output.addAll(sorted.keySet());
	     
	        int compare_with = 3*numberOfTables;
	        int toIndex = Math.min(compare_with, output.size());
	        List<String> res =output.subList(0, toIndex);
	        return res;
	}
	
	
	public List<String> addDocument(Document doc)
    {
		
			return this.addDocument32(doc);
		
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

	public int getDimension() {
		return this.tables[0].getDimension();
	}

}
