package ned.types;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.LinkedList;

public class ThreadManagerHelper {
	private static Document determineClosest(Document doc, LinkedList<Document> list)
	{
		double minDist = 1.0;
		Document nearest = null;
		for (Document right : list) {
			
			String rightId = right.getId();
					
			if ( rightId.equals(doc.getId()) )
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
	
	public static void mapToCluster(Document doc, LinkedList<Document> set)
	{
		Document nearest = ThreadManagerHelper.determineClosest(doc, set);
		
		Double dist = null;
		if (nearest != null)
			dist = Document.Distance(doc, nearest);
		
		
		//handle recent documents
		Object[] candidate = searchinRecentDocuments(doc);
		Double recentDist = null;
		Document recentDoc = null;
		if (candidate[0] != null) //there is something
		{
			recentDist = (Double)candidate[1];
			recentDoc = (Document)candidate[0];
		}
		if (nearest == null || dist>recentDist) 
		{
			nearest = (Document)candidate[0];
			dist = (Double)candidate[1];	
		}
		
		
		ThreadManagerHelper.mapToClusterHelper(doc, nearest, dist);
    }
	
	public static Object[] searchinRecentDocuments(Document doc) 
	{
		Document nearest = null;
		double min_dist = 1.0;
		GlobalData gd = GlobalData.getInstance();
		for (Document r : gd.recent)
		{
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
	
	private static void mapToClusterHelper(Document doc, Document nearest, Double distance)
	{
		boolean createNewThread = false;
		GlobalData data = GlobalData.getInstance(); 
		
		if (nearest == null)
			createNewThread = true;
	
		else if (distance > data.getParams().threshold)
				createNewThread = true;

			
		if (!createNewThread) 
		{
			DocumentCluster cluster = data.getId2Cluster().get(nearest.getId());
			if (cluster != null && cluster.canAdd(doc))
			{
				cluster.addDocument(doc, nearest, distance);
				data.getId2Cluster().put(doc.getId(), cluster);
			}
			else 
			{
				createNewThread = true;
				data.cleanClusterQueue.addLast(cluster.leadId);
			}
		}
		
		if (createNewThread)
		{
			DocumentCluster cluster = new DocumentCluster(doc);
			data.getClusters().add(cluster);
			data.getId2Cluster().put(doc.getId(), cluster);
		}
		
	}
	
	public static void pprint(PrintStream out)
	{
		GlobalData gd = GlobalData.getInstance();
		
		for (DocumentCluster cluster : gd.clusters) 
		{
			if (cluster.size() > 1)
			{
				out.println(cluster.toString());
			}
		}
	}
	
}
