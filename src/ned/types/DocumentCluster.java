package ned.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public class DocumentCluster {
	private List<String> idList;
	//private List<String> neighbor;
	//private List<Double> distance;
	String leadId;
	private long starttime;
	private long lasttime;
	private double entropy;
	
	public DocumentCluster(Document leadDocument)
	{
		this.idList = (List<String>) Collections.synchronizedList(new ArrayList<String>()); //new ArrayList<String>();
		//this.neighbor = (List<String>) Collections.synchronizedList(new ArrayList<String>()); //new ArrayList<String>();
		//this.distance = (List<Double>) Collections.synchronizedList(new ArrayList<Double>()); //new ArrayList<Double>();
		this.leadId = leadDocument.getId();
		this.starttime = leadDocument.getTimestamp();
		this.lasttime  = leadDocument.getTimestamp();
		entropy = -1;
		addDocument(leadDocument, null, null); //Double.MAX_VALUE);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DocumentCluster) 
		{
			String other = ((DocumentCluster)obj).leadId;
			return this.leadId.equals(other);
		}
		
		return false;
	}
	
	@Override
	public int hashCode() 
	{
		return leadId.hashCode();
	}
	
	@Override
	protected void finalize() throws Throwable {
		//System.out.println("DocumentCluster - finalize");
		super.finalize();
	}
	
	public void addDocument(Document doc, Document myNeighbor, Double distance) 
	{
		this.idList.add(doc.getId());
		this.lasttime = doc.getTimestamp();
		
//		String id = null;
//		if (myNeighbor != null)
//		{
//			id = myNeighbor.getId();
//		}
//		else
//			distance = null;
//		
		//this.neighbor.add(id);
		//this.distance.add(distance);
		
		entropy = -1;
	}
	
	public boolean isOpen(Document doc)
	{
		long delta = doc.getTimestamp() - this.starttime ;
		if (delta > GlobalData.getInstance().getParams().max_thread_delta_time)
			return false;
				
		return true;
	}

	/*public boolean canAdd(Document doc) {
		long timestamp = doc.getTimestamp();
		
		long delta = timestamp - this.starttime ;
		
		if (delta > GlobalData.getInstance().getParams().max_thread_delta_time)
			return false;
				
		return true;
	}*/
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		GlobalData gd = GlobalData.getInstance();
		
		double ent = entropy();
		if(ent > 3.5)
			sb.append("**3**");
		else if (ent > 2.5)
			sb.append("**2**");

		sb.append("LEAD: ").append(leadId).append(" SIZE: ").append(this.idList.size());
		sb.append(" Entropy: ").append(ent);
		sb.append(" Age: ").append(this.age()).append(" (s)\n");;
		
		sb.append("id\tnearest\tdistance\ttext\tnearest_text\n");
		for (int i =0; i<this.idList.size(); i++)
		{
			String docId = idList.get(i);
			Document doc = gd.id2document.get(docId);
			
			Document nDoc = null;
			if(i>0) { //this is placeholder for the lead - skip
				nDoc = gd.id2document.get(doc.getNearest());
			}
			
			sb.append(docId).append("\t").append(doc.getNearest()).append(String.format("\t%.7f", doc.getNearestDist()));
			sb.append("\t").append( doc.getCleanText() );

			String text = nDoc == null ? "NA" : nDoc.getCleanText();
			sb.append("\t").append(text);
			sb.append("\n");
		}
		return sb.toString();
	}
	
	public double entropy() 
	{
		if (entropy > -1)
			//don't calculate again
			return entropy;
		
		GlobalData gd = GlobalData.getInstance();
		
		HashMap<Integer, Integer> wordcount = new HashMap<Integer, Integer>();
		int N = 0;
		List<String> tmpList = (List<String>) Collections.synchronizedList(idList);
		
		for(String id : tmpList)
		{
			Document doc = gd.id2document.get(id);

			Hashtable<Integer, Integer> tmp = doc.getWordCount();
			for (Integer i : tmp.keySet())
			{
				int count = wordcount.getOrDefault(i, 0); 
				count += tmp.get(i).intValue();
				wordcount.put(i, count);
				
				N += tmp.get(i).intValue();
			}
		}
		
		double sum = 0.0;
		for (Integer i : wordcount.keySet())
		{
			int Ni = (int)wordcount.get(i);
			double d = (double)Ni / N;
			
			sum -= d * Math.log10(d);
		}
		
		return sum;
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
		
		return (lasttime-starttime); //check if we need to divide by 1000?
	}

	public int size() {
		return this.idList.size();
	}
	
	public List<String> getIdList()
	{
		return idList;
	}
}
