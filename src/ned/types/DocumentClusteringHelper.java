package ned.types;

import java.io.PrintStream;
import java.util.List;

public class DocumentClusteringHelper {
	
	
	private static void determineClosest(Document doc, List<String> list)
	{
		//double minDist = 1.0;
		//Document nearest = null;
		for (String rightId : list) {
			
			Document right = GlobalData.getInstance().id2document.get(rightId);
					
			if ( right == null || rightId.equals(doc.getId()) )
				continue;
			
			doc.updateNearest(right);
			
		}
	}
	
	public static void postLSHMapping(Document doc, List<String> set)
	{
		DocumentClusteringHelper.determineClosest(doc, set);
		
		//handle recent documents
		searchInRecentDocuments(doc);
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
			if (r.getId().equals(doc.getId()))
				continue;

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
