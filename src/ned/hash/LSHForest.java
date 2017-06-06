package ned.hash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Stream;

import ned.tools.GeneralHelper;
import ned.types.ArrayFixedSize;
import ned.types.Document;
import ned.types.RoundRobinArray;

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
	/*
	public List<String> addDocument00(Document doc, int dim, Map<Integer, Double> word2idf)
    {
		//AtomicIntegerArray hitCount1 = new AtomicIntegerArray(length)
		HashMap<String, AtomicInteger> hitCounts = new HashMap<String, AtomicInteger>();

		for (int t = 0; t<numberOfTables; t++)
		{
			RoundRobinArray<String> tmpList = tables[t].AddDocument(doc, dim, word2idf);
			int s = tmpList.size();
			for (int k=0; k<s; k++) {
				String tmp = tmpList.get(k);
				if(tmp == null)
				{
					System.out.println("Something strange.. value "+ k +" is null. tmpList.len = " + s);
					continue;
				}
				String id = doc.getId();
				if ( tmp.compareTo(id) >=0 )
					continue;
				
				AtomicInteger ai = null;
				if(!hitCounts.containsKey(tmp))
				{
					ai = new AtomicInteger(0);
					synchronized (hitCounts) {
						if(!hitCounts.containsKey(tmp))
							hitCounts.put(tmp, ai);
					}
					
				}
				if(ai == null)
					ai = hitCounts.get(tmp);
				ai.incrementAndGet();
			}
		}

        ArrayList<String> output = new ArrayList<String>();
        output.addAll(hitCounts.keySet());
        
        output.sort( new Comparator<String> () 
					        {  
					            @Override  
					            public int compare(String left, String right){  
					                 return hitCounts.get(right).get() - hitCounts.get(left).get() ;  //Descending  
					            }  
					            
					        }
        ); 
        
        int compare_with = 3*numberOfTables;
        int toIndex = Math.min(compare_with, output.size());
        List<String> res = output.subList(0, toIndex);
        return res;
    }
    */
	public List<String> addDocument01(Document doc, int dim, Map<Integer, Double> word2idf)
    {
		HashMap<String, AtomicInteger> hitCounts = new HashMap<String, AtomicInteger>();
		
		Stream<LSHTable> stream = Arrays.stream(tables);
		
		 stream.map(table->{
			return table.AddDocument(doc, dim, word2idf);
		}).forEach(tmpList->{
			int s = tmpList.size();
			for (int k=0; k<s; k++) {
				String tmp = tmpList.get(k);
				if(tmp == null)
				{
					System.out.println("Something strange.. value "+ k +" is null. tmpList.len = " + s);
					continue;
				}
				String id = doc.getId();
				if ( tmp.compareTo(id) >=0 )
					continue;
				
				AtomicInteger ai = null;
				if(!hitCounts.containsKey(tmp))
				{
					ai = new AtomicInteger(0);
					synchronized (hitCounts) {
						if(!hitCounts.containsKey(tmp))
							hitCounts.put(tmp, ai);
					}
					
				}
				if(ai == null)
					ai = hitCounts.get(tmp);
				ai.incrementAndGet();
			}
			
		});;



		
		

        ArrayList<String> output = new ArrayList<String>();
        Set<Entry<String, AtomicInteger>> es = hitCounts.entrySet();
        int compare_with = 3*numberOfTables;
        int i=0;
        int max=0;
        String maxkey="";
        for (Entry<String, AtomicInteger> entry : es) {
        	if(i<compare_with){
        		output.add(entry.getKey());
        		max=entry.getValue().get();
    			maxkey=entry.getKey();
        		
        	}else{
        		if(max<entry.getValue().get()){
        			output.remove(maxkey);
        			output.add(entry.getKey());
        			max=entry.getValue().get();
        			maxkey=entry.getKey();
        		}
        		
        	}
			i++;
		}
       /* 
        output.sort( new Comparator<String> () 
					        {  
					            @Override  
					            public int compare(String left, String right){  
					                 return hitCounts.get(right).get() - hitCounts.get(left).get() ;  //Descending  
					            }  
					            
					        }
        ); 
        
       
        int toIndex = Math.min(compare_with, output.size());
        List<String> res = output.subList(0, toIndex);
        return res;
		*/
		return output;
    }

	public List<String> addDocument(Document doc, int dim, Map<Integer, Double> word2idf)
    {
		return this.addDocument01(doc, dim, word2idf);
		/*
		Callable <List<String>> task = () -> {
			return this.addDocument00(doc);
			
		};
	
		try {
			Future f=ExecutionHelper.asyncAwaitRun(task);
			return (List<String>) f.get();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return null;
		*/
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
