package ned.types;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
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
						Document right = GlobalData.getInstance().getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);
						int i=0;
						while(right==null) 
						{
							i++;
							
							right = GlobalData.getInstance().getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);
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
				
				doc.setNearestDetermined(true);
		        //update the document in redis with the update doc //setNearestDetermined
				GlobalData.getInstance().setDocumentFromRedis(GlobalData.ID2DOCUMENT, doc.getId(), doc);
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
			
			if(rightId==null)
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>> how can rightId be null");
			
			return ( doc.getId().compareTo(rightId) > 0 ) ;
			} ).forEach(rightId-> {
				
				doc.updateNearest(gd.getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId));
			});
		doc.setNearestDetermined(true);
        gd.setDocumentFromRedis(GlobalData.ID2DOCUMENT, doc.getId(), doc);

		//long ms = System.currentTimeMillis() - base;
		//System.out.println("determineClosest: " + list.size() + " - " + ms + " ms.");
	}
	
	
	private static void determineClosest1(Document doc, List<String> list)
	{
		GlobalData gd = GlobalData.getInstance();
		//double minDist = 1.0;
		//Document nearest = null;
		for (String rightId : list) {
			
			if(rightId == null)
			{
				System.out.println("WE HAVE A BUG>>>>>>>>> the rightId is null (determineClosets1)");
				continue;
			}
			
			if ( doc.getId().compareTo(rightId) <= 0 ) 
				continue;
			
			Document right = gd.getDocumentFromRedis(GlobalData.ID2DOCUMENT, rightId);

			int i = 0;
			if ( right == null )
				continue;
			
			doc.updateNearest(right);
			
		}
		
		
		doc.setNearestDetermined(true);
        gd.setDocumentFromRedis(GlobalData.ID2DOCUMENT, doc.getId(), doc);
	}
	
	public static void postLSHMapping(Document doc, List<String> set)
	{
		long base = System.currentTimeMillis();
		StringBuffer msg = new StringBuffer("time: to compare ");
		//handle recent documents
		

        for(String s : set)
        {
        	if(s == null)
        		System.out.println("DocumentClusterHelp.post(set): >>>>>>>>>>>>>>>>> something strange");
        }
		
        
        List<String> compare = new ArrayList<String>();
        GlobalData.getInstance().recent.forEach(id -> compare.add(id));
        
        compare.addAll(set);
        
        
		msg.append(compare.size()).append(" items. ");

        msg.append(" concatenation: ").append(System.currentTimeMillis()-base).append("\t");
         
        doc.setCacheFlag(true);
		
        long milestone = System.currentTimeMillis();
        if(compare.size() < 500)
        	DocumentClusteringHelper.determineClosest1(doc, compare);
        else
        	DocumentClusteringHelper.determineClosest2(doc, compare);

        msg.append(" Stream: ").append(System.currentTimeMillis()-milestone).append("\t");
        
		milestone = System.currentTimeMillis();
		//DocumentClusteringHelper.determineClosest3(doc, compare);
       // msg.append(" Fork+Stream: ").append(System.currentTimeMillis()-milestone).append("\t");
        
        doc.setCacheFlag(false);
        
        long time = System.currentTimeMillis() - base;
        msg.append(" total: ").append(time).append("\t");
        
        //msg.append("java.util.concurrent.ForkJoinPool.common‌​.parallelism=").append(System.getProperty("java.util.concurrent.ForkJoinPool.common‌​.parallelism"));
        
		//System.out.println(msg.toString());
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
			 nearest = GlobalData.getInstance().getDocumentFromRedis(GlobalData.ID2DOCUMENT,doc.getNearest());


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
