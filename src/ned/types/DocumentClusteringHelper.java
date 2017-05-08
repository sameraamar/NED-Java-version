package ned.types;

import java.io.PrintStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import ned.tools.ArrayFixedSize;
import ned.tools.ExecutionHelper;

public class DocumentClusteringHelper {
	
	
	private static void determineClosest(Document doc, List<String> list)
	{
		String id = doc.getId();
		
		Iterator<String> iter = list.iterator();
		while(iter.hasNext()){
			String rightId = iter.next();
		    if(rightId.compareTo(id) < 0)
		    {
		    	Document right = GlobalData.getInstance().id2doc.get(rightId);//RedisHelper.getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);
				doc.updateNearest(right);
		    }
		}
		
		
	/*
		Object[] tmp = list.toArray();
		for (int i=0; i<tmp.length; i++)
		{
			String rightId = (String)tmp[i];
			if(rightId.compareTo(id) < 0)
		    {
		    	Document right = 		RedisHelper.getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);
				doc.updateNearest(right);
		    }
		}
		*/
	}
	
	public static void postLSHMapping(Document doc, List<String> set)
	{
		ArrayFixedSize<String> recent = GlobalData.getInstance().getRecentManager();
		if(recent!=null)
		{
			for(int i=0; i<recent.size(); i++)
				set.add(recent.get(i));
		}
		DocumentClusteringHelper.determineClosest(doc, set);
		//DocumentClusteringHelper.determineClosest(doc, GlobalData.getInstance().getRecent());
		//handle recent documents
		//searchInRecentDocuments(doc);
	}
	
	/*public static void searchInRecentDocuments(Document doc) 
	{
		GlobalData gd = GlobalData.getInstance();
		
		//java.util.concurrent.ConcurrentLinkedDeque<Document> list = new java.util.concurrent.ConcurrentLinkedDeque<Document>();
		  List<String> tmp = gd.getRecent();
		 
		 
		  for(String str:tmp){
			  doc.updateNearest(str);
		  }
		  
		//for (int i=0; i<tmp.length; i++)
		//{
		//	Document r = (Document)tmp[i];
		//}
		
	}*/
	
	public static void mapToClusterHelper(Document doc)
	{
		GlobalData data = GlobalData.getInstance();
		
		Document nearest = null;
		Double distance = null;
		if (doc.getNearest() != null)
		{
			nearest = GlobalData.getInstance().id2doc.get(doc.getNearest());//RedisHelper.getDocumentFromRedis(GlobalData.ID2DOCUMENT, doc.getNearest()); // data.id2document.get(doc.getNearest());
			distance = doc.getNearestDist();
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
	
	 public static <K, V> Set<K> intersection(ConcurrentHashMap<K, V> left, ConcurrentHashMap<K, V> right) {
		 if (left.size() > right.size())
	        {
			 ConcurrentHashMap<K, V> tmp = right;
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
