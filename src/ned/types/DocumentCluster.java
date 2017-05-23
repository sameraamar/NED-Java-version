package ned.types;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DocumentCluster implements Serializable, DirtyBit {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3760238718822776447L;
	
	private List<String> idList;
	//private List<String> neighbor;
	//private List<Double> distance;
	String leadId;
	private long starttime;
	private long lasttime;
	private double entropy;
	private boolean isDirtyBit;
	
	public DocumentCluster(Document leadDocument)
	{
		this.idList = (List<String>) Collections.synchronizedList(new ArrayList<String>()); //new ArrayList<String>();
		//this.neighbor = (List<String>) Collections.synchronizedList(new ArrayList<String>()); //new ArrayList<String>();
		//this.distance = (List<Double>) Collections.synchronizedList(new ArrayList<Double>()); //new ArrayList<Double>();
		this.leadId = leadDocument.getId();
		this.starttime = leadDocument.getTimestamp();
		this.lasttime  = leadDocument.getTimestamp();
		entropy = -1;
		addDocument(leadDocument);
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
	
	public void addDocument(Document doc) 
	{
		this.idList.add(doc.getId());
		this.lasttime = doc.getTimestamp();
		
		dirtyOn();
		entropy = -1;
	}
	
	public boolean isOpen(Document doc)
	{
		long delta = doc.getTimestamp() - this.starttime ;
		if (delta > GlobalData.getInstance().getParams().max_thread_delta_time)
			return false;
				
		return true;
	}

	@Override
	/*public String toString() {
		StringBuffer text = new StringBuffer();
		
		text.append("LeadId\tDocId\tSize\n");
		int size = size();
		for (String id : idList) {
			text.append(leadId).append("\t");
			text.append(id).append("\t");
			text.append(size);
			text.append("\n");
		}
		
		return text.toString();
	}*/

public String toString()
	{
		StringBuilder sb = new StringBuilder();
		GlobalData gd = GlobalData.getInstance();

		double ent = entropy();
		
		/*sb.append("LEAD: ").append(leadId).append(" SIZE: ").append(this.idList.size());
		sb.append(" Entropy: ").append(ent);
		sb.append(" Age: ").append(a).append(" (s)\n");;
		*/
		long a = this.age2();
		
		//sb.append("id\tcreated\ttimestamp\tnearest\tdistance\tentropy\tsize\tage\ttext\tnearest_text\n");
		int s = size();
		for (int i =0; i<s; i++)
		{
			String docId = idList.get(i);
			Document doc = gd.id2doc.get(docId);
			
			Document nDoc = null;
			String nearestId = doc.getNearestId();
			if(nearestId != null)
				nDoc = gd.id2doc.get(nearestId);
			
			sb.append(docId).append("\t");
			Date time=Date.from( Instant.ofEpochSecond( doc.getTimestamp() ) );
			sb.append(time.toString()).append("\t");
			
			sb.append(doc.getTimestamp()).append("\t");
			sb.append(nearestId).append("\t");
			sb.append(String.format("%.7f\t", doc.getNearestDist()));
			
			sb.append( ent ).append("\t");
			sb.append( s ).append("\t");
			sb.append( a ).append("\t");
			
			sb.append( doc.getCleanText() );

			//String text = nDoc == null ? "NA" : nDoc.getCleanText();
			//sb.append("\t").append(text);
			
			if(nearestId!=null && nearestId.compareTo(doc.getId()) >= 0)
			{
				sb.append("\t!!!!! bad nearest choice...");
			}
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
		List<String> tmpList = idList;
		
		for(String id : tmpList)
		{
			DocumentWordCounts doc = GlobalData.getInstance().id2wc.get(id);
			if(doc!=null){
				Map<Integer, Integer> tmp = doc.getWordCount();
				for (Integer i : tmp.keySet())
				{
					int count = wordcount.getOrDefault(i, 0); 
					count += tmp.get(i).intValue();
					wordcount.put(i, count);
					
					N += tmp.get(i).intValue();
				}
				
			}
			
		}
		
		double sum = 0.0;
		Set<Entry<Integer, Integer>> es = wordcount.entrySet();
		
		for (Entry<Integer, Integer> entry : es) {
			int Ni = entry.getValue();
			double d = (double)Ni / N;
			
			sum -= d * Math.log10(d);
		}
		
		return sum;
	}
	
	//return in seconds
	public long age2()
	{
		int s = size();
		if (s < 2)
			return 0;
		
		String id = this.idList.get(s-1);
		Document doc = GlobalData.getInstance().id2doc.get(id);
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
	

	@Override
	public boolean isDirty() {
		return isDirtyBit;
	}

	@Override
	public void dirtyOff() {
		isDirtyBit = false;
	}

	@Override
	public void dirtyOn() {
		isDirtyBit = true;
	}
}
