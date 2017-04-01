package ned.hash;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import ned.types.Document;
import ned.types.GlobalData;

public class LSHForestParallel extends LSHForestAbstract {
	
	public LSHForestParallel(int tablesNumer, int hyperPlanesNumber, int dimension, int maxBucketSize) 
	{
		super(tablesNumer, hyperPlanesNumber, dimension, maxBucketSize);		
	}
	
	public List<String> addDocument(Document doc)
    {
		final GlobalData gd = GlobalData.getInstance();
		
		ArrayList< Future<List<String>> > neighbors = new ArrayList< Future<List<String>> >(numberOfTables);
		
		for (int i = 0; i<numberOfTables; i++)
		{
			neighbors.add( gd.executer.addToLSH(tables[i], doc) );
		}
		
		List<String> res = processResults(neighbors, doc);
		return res;
    }

	public List<String> addDocument5(Document doc)
	{
		int d = 8/0;
		return null;
	}

	@Override
	public ArrayList<Future<List<String>>> addDocumentFuture(Document doc) 
	{
		final GlobalData gd = GlobalData.getInstance();
		ArrayList< Future<List<String>> > neighbors = new ArrayList< Future<List<String>> >(numberOfTables);
		
		for (int i = 0; i<numberOfTables; i++)
		{
			neighbors.add( gd.executer.addToLSH(tables[i], doc) );
		}
		return neighbors;
	}
	
	public List<String> processResults(ArrayList< Future<List<String>> > neighbors, Document doc)
	{
		final HashMap<String, Integer> hitCounts = new HashMap<String, Integer>();

		for (int i = 0; i<numberOfTables; i++)
		{
			List<String> tmpList = null;
			try {
				tmpList = neighbors.get(i).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			
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

}
