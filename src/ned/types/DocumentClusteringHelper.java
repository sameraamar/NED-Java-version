package ned.types;

import java.io.PrintStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import ned.tools.ExecutionHelper;

public class DocumentClusteringHelper {
	
	
	private static void determineClosest11(Document doc, List<String> list, Map<Integer, Double> word2idf)
	{
		String id = doc.getId();
		
		Iterator<String> iter = list.iterator();
		Iterable<String> iterable = () -> iter;

		Stream<String> stream = StreamSupport.stream(iterable.spliterator(), true);

		stream.forEach(rightId->{
		    if(rightId.compareTo(id) < 0)

		    {
			    DocumentWordCounts right = GlobalData.getInstance().id2wc.get(rightId);
			    doc.updateNearest(right, word2idf);
		    }
		});
	}
	
	private static void determineClosest(Document doc, List<String> list, Map<Integer, Double> word2idf)
	{
		String id = doc.getId();
		
		Iterator<String> iter = list.iterator();
		while(iter.hasNext()){
			String rightId = iter.next();
		    if(rightId.compareTo(id) < 0)
		    {
		    	DocumentWordCounts right = GlobalData.getInstance().id2wc.get(rightId);
				doc.updateNearest(right, word2idf);
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
	
	public static void postLSHMapping(Document doc, List<String> set, Map<Integer, Double> word2idf)
	{
		RoundRobinArray<String> recent = GlobalData.getInstance().getRecentManager();
		if(recent!=null)
		{
			int s = recent.size();
			for(int i=0; i< s;i++)
				set.add(recent.get(i));
		}
		DocumentClusteringHelper.determineClosest(doc, set, word2idf);
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

		String nearest = null;
		Double distance = null;
		if (doc.getNearestId() != null)
		{
			nearest = doc.getNearestId();
			distance = doc.getNearestDist();
		}
		
		boolean createNewThread = false;
		
		if (nearest == null)
			createNewThread = true;
	
		else if (distance > data.getParams().threshold)
				createNewThread = true;
					
		String targetCluster = null;
		if (!createNewThread) 
		{
			targetCluster = lookForCluster(doc, nearest);
			
			if (targetCluster == null)
				createNewThread = true;
		}

		if(createNewThread)
		{
			data.createCluster(doc);
		}
		else
		{
			assert(targetCluster != null);
			DocumentCluster cluster = data.clusterByDoc(targetCluster); 
			cluster.addDocument(doc);
			data.mapToCluster(cluster.leadId, doc);
		}
	}

	private static String lookForCluster(Document doc, String nearest) {
		String original = nearest;
		while(true) 
		{
			if(nearest == null)
			{
				return null;
			}
			
			GlobalData gd = GlobalData.getInstance();
			String joinClusterId = gd.id2cluster.get(nearest);
			
			if(joinClusterId==null)
			{
				/*if(GlobalData.debug( nearest ))
					System.out.println(" ***************************** found null "  + nearest + " while handling doc " + doc.getId());

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
				}*/

				joinClusterId = gd.id2cluster.get(nearest);
				if(GlobalData.debug( nearest ))
					System.out.println(" ***************************** after sleep 1 sec "  + nearest + " cluster is " + joinClusterId + " while handling doc " + doc.getId());

				if(joinClusterId==null)
				{
					System.out.println("joinClusterId==null : " + nearest + " while handling doc " + doc.getId());
					return null;
				}
					
			}
			
			DocumentCluster cluster = gd.clusterByDoc(joinClusterId);
			if (cluster != null && cluster.isOpen(doc))
			{
				return cluster.leadId;
			}
			String replacement = gd.cluster2replacement.get(joinClusterId);
			if(replacement == null || replacement.equals(joinClusterId)) 
			{
				gd.cluster2replacement.put(joinClusterId, doc.getId());
				return null;
			}
	
			nearest = replacement;
		}		
	}
		
	public static void pprint(PrintStream out)
	{
		Runnable task=()->{
		GlobalData gd = GlobalData.getInstance();
		
		for (String leadId : gd.id2cluster.keySet())
		{
			DocumentCluster c = gd.clusterByDoc(leadId);
			if (c.size() > 1)
			{
				out.print(c.toString());
				out.print("\n");
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
	
	 public static <K, V> Set<K> intersection(Map<K, V> left, Map<K, V> right) 
	 {
		 if (left.size() > right.size())
		 {
			 Map<K, V> tmp = right;
			 right = left;
			 left = tmp;
		 }
        
		 HashSet<K> intersection = new HashSet<K>();
		 Set<K> lkeys = left.keySet();
		 for (K key : lkeys) 
		 {
			 if (right.containsKey(key))
			 {
				 intersection.add(key);
			 }
		 }

		 return intersection;
	 }
	 
	 public static <K, V> Set<K> intersection(HashMap<K, V> left,HashMap<K, V> right) {
		 if (left.size() > right.size())
	        {
			 	HashMap<K, V> tmp = right;
				right = left;
				left = tmp;
	        }
	        
	        HashSet<K> intersection = new HashSet<K>();
	        Set<Entry<K, V>> lkeys = left.entrySet();
	        
	        for (Entry<K, V> entry : lkeys) {
	        	K k = entry.getKey();
	        	if (right.containsKey(k))
	            {
					intersection.add(k);
	            }
			}
	       

	        return intersection;
	    }
	
}
