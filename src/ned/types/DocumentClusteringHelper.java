package ned.types;

import java.io.PrintStream;
import java.util.List;

public class DocumentClusteringHelper {
	
	
	private static Document determineClosest(Document doc, List<String> list)
	{
		double minDist = 1.0;
		Document nearest = null;
		for (String rightId : list) {
			
			Document right = GlobalData.getInstance().id2document.get(rightId);
					
			if ( right == null || rightId.equals(doc.getId()) )
				continue;
			
			//Document right = GlobalData.getInstance().id2document.get(rightId);
			double tmp = Document.Distance(doc, right);
			if (nearest==null || tmp < minDist)
			{
				minDist = tmp;
				nearest = right;
			}
			
		}
		
		return nearest;
	}
	
	public static Object[] postLSHMapping(Document doc, List<String> set)
	{
		Document nearest = DocumentClusteringHelper.determineClosest(doc, set);
		
		Double dist = null;
		if (nearest != null)
			dist = Document.Distance(doc, nearest);
		
		//handle recent documents
		Object[] candidate = searchInRecentDocuments(doc);
		Double recentDist = null;
		Document recentDoc = null;
		if (candidate[0] != null) //there is something
		{
			recentDist = (Double)candidate[1];
			recentDoc = (Document)candidate[0];
		}
		if (nearest == null || (recentDist != null && dist>recentDist) )
		{
			nearest = recentDoc;
			dist = recentDist;	
		}
		
		candidate[0] = nearest;
		candidate[1] = dist;
		return candidate;
	}
	
	public static Object[] searchInRecentDocuments(Document doc) 
	{
		Document nearest = null;
		double min_dist = 1.0;
		GlobalData gd = GlobalData.getInstance();
		
		//java.util.concurrent.ConcurrentLinkedDeque<Document> list = new java.util.concurrent.ConcurrentLinkedDeque<Document>();
		Object[] tmp = gd.recent.toArray();  //TODO : This is not effecient
		for (int i=0; i<tmp.length; i++)
		//for (Document r : gd.recent)
		{
			Document r = (Document)tmp[i];
			if (r.getId().equals(doc.getId()))
				continue;

			double dist = Document.Distance(doc,  r);
			if (nearest == null || dist<min_dist)
			{
				nearest = r;
				min_dist = dist;
			}
		}
		
		Object[] res = new Object[2];
		res[0] = nearest;
		res[1] = min_dist;
		return res;
	}
	
	public static void mapToClusterHelper(Document doc, Document nearest, Double distance)
	{
		boolean createNewThread = false;
		GlobalData data = GlobalData.getInstance(); 
		
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
				if (cluster != null)
					data.markForCleanup(cluster.leadId);
			}
		}
		
		if (createNewThread)
		{
			data.createCluster(doc);
			
			//DocumentCluster cluster = new DocumentCluster(doc);
			//int idx = data.getClusters().add(cluster);
			//data.getId2Cluster().put(doc.getId(), idx);
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
