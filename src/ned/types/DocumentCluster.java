package ned.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class DocumentCluster {
	private ArrayList<String> idList;
	private ArrayList<String> neighbor;
	private ArrayList<Double> distance;
	String leadId;
	private long starttime;
	private double entropy;
	
	public DocumentCluster(Document leadDocument)
	{
		this.idList = new ArrayList<String>();
		this.neighbor = new ArrayList<String>();
		this.distance = new ArrayList<Double>();
		this.leadId = leadDocument.getId();
		this.starttime = leadDocument.getTimestamp();
		entropy = -1;
		addDocument(leadDocument, null, null); //Double.MAX_VALUE);
	}
	
	@Override
	protected void finalize() throws Throwable {
		int f = 9/0;
		super.finalize();
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
		
		entropy = -1;
	}

	public boolean canAdd(Document doc) {
		long timestamp = doc.getTimestamp();
		
		long delta = timestamp - this.starttime ;
		
		if (delta > GlobalData.getInstance().getParams().max_thread_delta_time)
			return false;
				
		return true;
	}
	
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
	
	public double entropy() 
	{
		if (entropy > -1)
			//don't calculate again
			return entropy;
		
		GlobalData gd = GlobalData.getInstance();
		
		HashMap<Integer, Integer> wordcount = new HashMap<Integer, Integer>();
		int N = 0;
		for(String id : idList)
		{
			Document doc = gd.id2document.get(id);

			Dict tmp = doc.getWordCount();
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
	
	public ArrayList<String> getIdList()
	{
		return idList;
	}
}
