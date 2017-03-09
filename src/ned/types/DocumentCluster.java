package ned.types;

import java.util.ArrayList;
import java.util.LinkedList;

public class DocumentCluster {
	private ArrayList<String> idList;
	private ArrayList<String> neighbor;
	private ArrayList<Double> distance;
	String leadId;
	private long starttime;
	
	public DocumentCluster(Document leadDocument)
	{
		this.idList = new ArrayList<String>();
		this.neighbor = new ArrayList<String>();
		this.distance = new ArrayList<Double>();
		this.leadId = leadDocument.getId();
		this.starttime = leadDocument.getTimestamp();
		
		addDocument(leadDocument, null, null); //Double.MAX_VALUE);
	}
	
	public void addDocument(Document doc, Document myNeighbor, Double distance) 
	{
		this.idList.add(doc.getId());
		String id = null;
		if (myNeighbor != null)
		{
			id = myNeighbor.getId();
		}
		else
			distance = null;
		
		this.neighbor.add(id);
		this.distance.add(distance);
	}

	public boolean canAdd(Document doc) {
		long timestamp = doc.getTimestamp();
		
		long delta = timestamp - this.starttime ;
		
		if (delta/1000 > GlobalData.getInstance().getParams().max_thread_delta_time)
			return false;
				
		return true;
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		GlobalData gd = GlobalData.getInstance();
		
		sb.append("LEAD: ").append(leadId).append(" SIZE: ").append(this.idList.size());
		sb.append(" Age: ").append(this.age()).append(" (s)\n");;
		sb.append("id\tnearest\tdistance\ttext1\ttext2\n");
		for (int i =0; i<this.idList.size(); i++)
		{
			String leadId = idList.get(i);
			String nId = this.neighbor.get(i);
			
			sb.append(leadId).append("\t").append(nId).append("\t").append(this.distance.get(i));
			
			Document leadDoc = gd.id2document.get( leadId );
			sb.append("\t").append( leadDoc.getCleanText() );

			Document nDoc = gd.id2document.get(nId);
			String text = nDoc == null ? "NA" : nDoc.getCleanText();
			sb.append("\t").append(text);
			sb.append("\n");
		}
		return sb.toString();
	}
	
	//return in seconds
	public long age()
	{
		int s = size();
		if (s < 2)
			return 0;
		
		String id = this.idList.get(s-1);
		Document doc = GlobalData.getInstance().id2document.get(id);
		long lasttime = doc.getTimestamp();
		
		return (lasttime-starttime)/1000;
	}

	public int size() {
		return this.idList.size();
	}
	
	public ArrayList<String> getIdList()
	{
		return idList;
	}
}
