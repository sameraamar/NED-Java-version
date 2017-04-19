package ned.types;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import ned.tools.ExecutionHelper;

public class DocumentClusteringHelper {
	
	
	private static void determineClosest(Document doc, List<String> list)
	{
		String id = doc.getId();
		
		/*Iterator<String> iter = list.iterator();
		while(iter.hasNext()){
			String rightId = iter.next();
		    if(rightId.compareTo(id) < 0)
		    {
		    	Document right = GlobalData.getInstance().id2document.get(rightId);
				doc.updateNearest(right);
		    }
		}*/
		
		Object[] tmp = list.toArray();
		for (int i=0; i<tmp.length; i++)
		{
			String rightId = (String)tmp[i];
			if(rightId.compareTo(id) < 0)
		    {
		    	Document right = GlobalData.getInstance().id2document.get(rightId);
				doc.updateNearest(right);
		    }
		}
		
	}
	
	public static void postLSHMapping(Document doc, List<String> set)
	{
		DocumentClusteringHelper.determineClosest(doc, set);
		DocumentClusteringHelper.determineClosest(doc, GlobalData.getInstance().recent);
		
		//handle recent documents
		//searchInRecentDocuments(doc);
	}
	
	public static void searchInRecentDocuments(Document doc) 
	{
		GlobalData gd = GlobalData.getInstance();
		
		//java.util.concurrent.ConcurrentLinkedDeque<Document> list = new java.util.concurrent.ConcurrentLinkedDeque<Document>();
		Object[] tmp = gd.recent.toArray();  //TODO : This is not effecient
		for (int i=0; i<tmp.length; i++)
		//for (Document r : gd.recent)
		{
			Document r = (Document)tmp[i];
			doc.updateNearest(r);
		}
	}
	
	public static void mapToClusterHelper(Document doc)
	{
		GlobalData data = GlobalData.getInstance();
		
		Document nearest = null;
		Double distance = null;
		if (doc.nearest != null)
		{
			nearest =  data.id2document.get(doc.nearest);
			distance = doc.nearestDist;
		}
		
		boolean createNewThread = false;
		
		if (nearest == null)
			createNewThread = true;
	
		else if (distance > data.getParams().threshold)
				createNewThread = true;

			
		if (!createNewThread) 
		{
			DocumentCluster cluster = data.clusterByDoc(nearest.getId());
			if (cluster != null && cluster.isOpen(doc))
			{
				cluster.addDocument(doc, nearest, distance);
				data.mapToCluster(cluster.leadId, doc);
				
				//int idx = data.clusterIndexByDoc(nearest.getId());
				//data.getId2Cluster().put(doc.getId(), idx);
			}
			else 
			{
				createNewThread = true;
			}
		}
		
		if (createNewThread)
		{
			data.createCluster(doc);
		}
	}
	
	public static void pprint(PrintStream out)
	{
		Runnable task=()->{
		GlobalData gd = GlobalData.getInstance();
		
		for (String leadId : gd.getId2Cluster().keySet())
		{
			DocumentCluster c = gd.clusterByDoc(leadId);
			if (c.size() > 1)
			{
				out.println(c.toString());
			}
		}
		};
		ExecutionHelper.asyncRun(task);
	}
	
//	 public static <T> List<T> intersection(List<T> list1, List<T> list2) {
//	        List<T> list = new ArrayList<T>();
//	        for (T t : list1) {
//	            if(list2.contains(t)) {
//	                list.add(t);
//	            }
//	        }
//
//	        return list;
//	}
	
	 public static <K, V> Set<K> intersection(Hashtable<K, V> left, Hashtable<K, V> right) {
		 if (left.size() > right.size())
	        {
				Hashtable<K, V> tmp = right;
				right = left;
				left = tmp;
	        }
	        
	        HashSet<K> intersection = new HashSet<K>();
	        Enumeration<K> lkeys = left.keys();
	        while( lkeys.hasMoreElements() )
	        {
	        	K key = lkeys.nextElement();
				if (right.containsKey(key))
	            {
					intersection.add(key);
	            }
	        }

	        return intersection;
	    }
	
}
