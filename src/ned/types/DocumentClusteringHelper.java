package ned.types;

import java.io.PrintStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DocumentClusteringHelper {
	
	private static void determineClosest3(Document doc, List<String> list)

	{
		ForkJoinPool forkJoinPool = GlobalData.getForkPool();
		
		forkJoinPool.execute(() ->
		{
			try {Stream<String> stream = list.parallelStream();
				Optional<Object[]> nearest = stream.filter( rightId-> {
							return ( doc.getId().compareTo(rightId) > 0 ) ;
							} )
					.map( rightId -> {
						Document right = GlobalData.getInstance().getDocumentFromRedis("id2document", rightId);
						int i=0;
						while(right==null) 
						{
							i++;
							
							right = GlobalData.getInstance().getDocumentFromRedis("id2document", rightId);
						}
						Object[] result = { doc, new Double( Document.Distance(doc, right) ) };
						return result;
						} )
					.reduce((a, b) -> {
						if((Double)a[1] < (Double)b[1])
							return a;
						return b;
					});
				
				
				nearest.ifPresent(x -> {
					doc.updateNearest((Document)x[0]);
				});
				
				/*Object[] x = null;
				try{
					x = nearest.get();
	
					doc.updateNearest((Document)x[0]);
				} 
				catch (NoSuchElementException e)
				{
					System.out.println("!!!!!!!!!!!!!!!!!!!!! Could not find a good neighbor for: " + doc.getId());
				}*/
				
//				DocumentClusteringHelper.mapToClusterHelper(doc);
//				
//				GlobalData.getInstance().markOldClusters(doc.getId());
//				
//				GlobalData.getInstance().queue.poll();
				
				doc.setNearestDetermined(true);
		        //update the document in redis with the update doc //setNearestDetermined
				GlobalData.getInstance().setDocumentFromRedis("id2document", doc.getId(), doc);
		    } catch (Throwable thr) {
				thr.printStackTrace();
			}
		});	
	}
	
	private static void determineClosest2(Document doc, List<String> list)

	{
		//long base = System.currentTimeMillis();
		GlobalData gd = GlobalData.getInstance();
		list.parallelStream().filter( rightId-> {
			return ( gd.getDocumentFromRedis("id2document", rightId)!=null && doc.getId().compareTo(rightId) > 0 ) ;
			} ).forEach(rightId-> {
				
				doc.updateNearest(gd.getDocumentFromRedis("id2document", rightId));
			});
		doc.setNearestDetermined(true);
        gd.setDocumentFromRedis("id2document", doc.getId(), doc);

		//long ms = System.currentTimeMillis() - base;
		//System.out.println("determineClosest: " + list.size() + " - " + ms + " ms.");
	}
	
	
	private static void determineClosest1(Document doc, List<String> list)
	{
		GlobalData gd = GlobalData.getInstance();
		//double minDist = 1.0;
		//Document nearest = null;
		for (String rightId : list) {
			Document right = gd.getDocumentFromRedis("id2document", rightId);

			//Document right = GlobalData.getInstance().id2document.get(rightId);
					
			if ( right == null )
				continue;
			
			if ( doc.getId().compareTo(rightId) <= 0 ) 
				continue;
			
			doc.updateNearest(right);
			
		}
		
		doc.setNearestDetermined(true);
        gd.setDocumentFromRedis("id2document", doc.getId(), doc);
	}
	
	public static void postLSHMapping(Document doc, List<String> set)
	{
		long base = System.currentTimeMillis();
		StringBuffer msg = new StringBuffer("time: to compare ");
		//handle recent documents
        @SuppressWarnings("unchecked")
		List<String> compare = (List<String>) GlobalData.getInstance().recent.clone();
        compare.addAll(set);
		
		msg.append(compare.size()).append(" items. ");

        //Samer: DocumentClusteringHelper.searchInRecentDocuments(doc);
        msg.append(" concatenation: ").append(System.currentTimeMillis()-base).append("\t");
         
        doc.setCacheFlag(true);
		
        long milestone = System.currentTimeMillis();
		DocumentClusteringHelper.determineClosest1(doc, compare);
        msg.append(" Serial: ").append(System.currentTimeMillis()-milestone).append("\t");

        milestone = System.currentTimeMillis();
        DocumentClusteringHelper.determineClosest2(doc, compare);
        msg.append(" Stream: ").append(System.currentTimeMillis()-milestone).append("\t");

		milestone = System.currentTimeMillis();
		DocumentClusteringHelper.determineClosest3(doc, compare);
        msg.append(" Fork+Stream: ").append(System.currentTimeMillis()-milestone).append("\t");
        
        doc.setCacheFlag(false);
        
        long time = System.currentTimeMillis() - base;
        msg.append(" total: ").append(time).append("\t");
        
        //msg.append("java.util.concurrent.ForkJoinPool.common‌​.parallelism=").append(System.getProperty("java.util.concurrent.ForkJoinPool.common‌​.parallelism"));
        
		System.out.println(msg.toString());
	}

	/*@SuppressWarnings("unchecked")
	public static void searchInRecentDocuments(Document doc) 
	{
		GlobalData gd = GlobalData.getInstance();
		DocumentClusteringHelper.determineClosest(doc, (List<String>)gd.recent.clone());
	}
	
	public static void searchInRecentDocuments1(Document doc) 
	{
		GlobalData gd = GlobalData.getInstance();
		
		//java.util.concurrent.ConcurrentLinkedDeque<Document> list = new java.util.concurrent.ConcurrentLinkedDeque<Document>();
		Object[] tmp = gd.recent.toArray();  //TODO : This is not effecient
		for (int i=0; i<tmp.length; i++)
		//for (Document r : gd.recent)
		{
			Document r = (Document)tmp[i];
			if ( doc.getId().compareTo(r.getId() ) <= 0 ) 
				continue;

			doc.updateNearest(r);
		}
	}*/
	
	public static void mapToClusterHelper(Document doc)
	{
		GlobalData data = GlobalData.getInstance();
		
		Document nearest = null;
		Double distance = null;
		if (doc.getNearest() != null)
		{
			 nearest = GlobalData.getInstance().getDocumentFromRedis("id2document",doc.getNearest());


			//nearest =  data.id2document.get(doc.getNearest());
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
		GlobalData gd = GlobalData.getInstance();
		
		for (String leadId : gd.getId2Cluster().keySet())
		{
			DocumentCluster c = gd.clusterByDoc(leadId);
			if (c.size() > 1)
			{
				out.println(c.toString());
			}
		}
	}
	
}
