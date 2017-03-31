package ned.hash;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ned.types.Document;
import ned.types.GlobalData;

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

	public List<String> addDocument3(Document doc)
    {
		HashMap<String, Integer> hitCounts = new HashMap<String, Integer>();
		
		Stream<LSHTable> tablesStream = Arrays.stream(tables);
		List<String> tmpList = tablesStream.parallel()
				.map(table->{
					List<String> neighbors = table.AddDocument(doc);
					return neighbors;
					})
				.reduce((a, b) -> {
						ArrayList<String> output = new ArrayList<String>();
				        output.addAll(a);
				        output.addAll(b);
				        return output;
					})
				.get();
		
		for (String id : tmpList) {
			if(doc.getId().compareTo(id) > 0)
			{
				int c = hitCounts.getOrDefault(id,  0);
				hitCounts.put(id,  c+1);
			}
		}
		
		tmpList.sort( new Comparator<String> () 
        {  
            @Override  
            public int compare(String left, String right){  
                 return hitCounts.get(right) - hitCounts.get(left) ;  //Descending  
            }  
            
        });
		
		List<String> res = tmpList.subList(0, Math.min( tmpList.size(), 3*numberOfTables) );
        return res;
    }
	
	public List<String> addDocument4(Document doc)
    {
		final HashMap<String, Integer> hitCounts = new HashMap<String, Integer>();
		
		Stream<LSHTable> tablesStream = Arrays.stream(tables);

		tablesStream.parallel()
			.map(table -> {
				 return table.AddDocument(doc);
			})
			.forEach(tmpList -> {
				
				for (String tmp : tmpList) {
					if (tmp.compareTo( doc.getId() ) >= 0)
						continue;
					
					//synchronized(hitCounts) {
						Integer c = hitCounts.getOrDefault(tmp, 0);
						hitCounts.put(tmp, c+1);
					//}
				}
			});
		
		/*boolean breakme = false;
		for (String h : hitCounts.keySet())
		{
			if (hitCounts.get(h) > 5)
				breakme = true;
		}*/
				
		ArrayList<String> output = new ArrayList<String>(3*numberOfTables);
		hitCounts.entrySet()
        .stream()
        .parallel()
        .sorted( new Comparator<Entry<String, Integer>> () 
					        {  
					            @Override
								public int compare(Entry<String, Integer> left, Entry<String, Integer> right) {
									return right.getValue() - left.getValue();  //Descending  
								}  
					            
					        })
        .limit(3*numberOfTables)
        .forEach(entry -> {
        	output.add(entry.getKey());
        });
		
        return output;
    }
	
	
	public List<String> addDocument(Document doc)
    {
		final HashMap<String, Integer> hitCounts = new HashMap<String, Integer>();
		
		for (int i = 0; i<numberOfTables; i++)
		{
			List<String> tmpList = tables[i].AddDocument(doc);
			
			for (String tmp : tmpList) {
				if (tmp.compareTo( doc.getId() ) >= 0)
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

	public int getDimension() {
		return this.tables[0].getDimension();
	}

}
