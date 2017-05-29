package ned.types;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocumentCluster implements Serializable, DirtyBit {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3760238718822776447L;
	
	private List<String> idList;
	//private List<String> neighbor;
	//private List<Double> distance;
	private HashSet<String> users;
	String leadId;
	private long starttime;
	private long lasttime;
	private double entropy;
	private boolean isDirtyBit;
	private double score;
	
	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public DocumentCluster(Document leadDocument)
	{
		this.idList = (List<String>) Collections.synchronizedList(new ArrayList<String>()); //new ArrayList<String>();
		//this.neighbor = (List<String>) Collections.synchronizedList(new ArrayList<String>()); //new ArrayList<String>();
		//this.distance = (List<Double>) Collections.synchronizedList(new ArrayList<Double>()); //new ArrayList<Double>();
		this.leadId = leadDocument.getId();
		this.starttime = leadDocument.getTimestamp();
		this.lasttime  = leadDocument.getTimestamp();
		entropy = -1;
		users = new HashSet<String>() ;
		addDocument(leadDocument);
		score=0;
		
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
		if(this.lasttime-this.starttime>0){
			score = Double.valueOf(2.0)/(doc.getTimestamp()-this.lasttime);
		}
		
		this.lasttime = doc.getTimestamp();
		users.add(doc.getUserId());
		
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
	public String toString() {
		if(GlobalData.getInstance().getParams().is_prod_mode)
			return toStringProd();
		
		return toStringNonProd();
	}
			
	/*
	public String toString() {
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
	}
	*/
	

public String toStringNonProd()
	{
		GlobalData gd = GlobalData.getInstance();
		String dil=GlobalData.dilimitter;
		String ent =String.format("%.7f",  entropy());
		String scoreAsStr = String.format("%.7f", score);
		
		/*sb.append("LEAD: ").append(leadId).append(" SIZE: ").append(this.idList.size());
		sb.append(" Entropy: ").append(ent);
		sb.append(" Age: ").append(a).append(" (s)\n");;
		*/
		long a = this.age2();
		
		//sb.append("leadId\tid\tuser\t# users\tcreated\ttimestamp\tnearest\tdistance\tentropy\tsize\tage\ttext\n");
		Pattern whitespace = Pattern.compile("\\s");
		Pattern whitespace2 = Pattern.compile("\\s\\s");
		int s = size();
		int numOfUsers = users.size();
		
		StringBuilder sb = new StringBuilder();
		sb.append( ent ).append(dil);
		sb.append(numOfUsers).append(dil);
		sb.append( s ).append(dil);
		sb.append( a ).append(dil);
		sb.append( scoreAsStr ).append(dil);
		String block = sb.toString();
		
		sb = new StringBuilder();
		
		
			String docId = idList.get(0);
			Document doc = gd.id2doc.get(docId);
			
			Document nDoc = null;
			String nearestId = doc.getNearestId();
			if(nearestId != null){
				nDoc = gd.id2doc.get(nearestId);
			}
				
			
			sb.append(leadId).append(dil);
			
			//sb.append(doc.getUserId()).append("\t");
			Date time=Date.from( Instant.ofEpochSecond( doc.getTimestamp() ) );
			sb.append(time.toString()).append(dil);
			
			//sb.append(doc.getTimestamp()).append(dil);
			sb.append(nearestId).append(dil);
			sb.append(String.format("%.7f", doc.getNearestDist()));
			sb.append(dil);
			sb.append(block);
			
			String lbl = gd.labeled.positive.get(docId);
			lbl = lbl == null ? "" : "t_" + lbl;
			sb.append( lbl ).append(dil);
			
			Matcher matcher = whitespace.matcher(doc.getText());
			String result = matcher.replaceAll(" ");
			
			matcher = whitespace2.matcher(result);
			result = matcher.replaceAll(" ");
			sb.append( result ).append(dil);;
			
			
			sb.append( score ).append(dil);;
			//String text = nDoc == null ? "NA" : nDoc.getCleanText();
			//sb.append("\t").append(text);
			
			
			sb.append("\n");
		
		/*
		for (int i =0; i<1; i++)

		{
			String docId = idList.get(i);
			Document doc = gd.id2doc.get(docId);
			
			Document nDoc = null;
			String nearestId = doc.getNearestId();
			if(nearestId != null)
				nDoc = gd.id2doc.get(nearestId);
			
			sb.append(leadId).append("\t");
			sb.append(docId).append("\t");
			//sb.append(doc.getUserId()).append("\t");
			Date time=Date.from( Instant.ofEpochSecond( doc.getTimestamp() ) );
			sb.append(time.toString()).append("\t");
			
			sb.append(doc.getTimestamp()).append("\t");
			sb.append(nearestId).append("\t");
			sb.append(String.format("%.7f\t", doc.getNearestDist()));
			
			sb.append(block);
			
			String lbl = gd.labeled.positive.get(docId);
			lbl = lbl == null ? "" : "t_" + lbl;
			sb.append( lbl ).append("\t");
			
			Matcher matcher = whitespace.matcher(doc.getText());
			String result = matcher.replaceAll(" ");
			
			matcher = whitespace2.matcher(result);
			result = matcher.replaceAll(" ");
			sb.append( result );

			//String text = nDoc == null ? "NA" : nDoc.getCleanText();
			//sb.append("\t").append(text);
			
			
			sb.append("\n");
		}
		
		*/
		return sb.toString();
	}



public String toStringProd()
	{
		GlobalData gd = GlobalData.getInstance();
		String dil=GlobalData.dilimitter;
		String ent =String.format("%.7f",  entropy());
		String scoreAsStr = String.format("%.7f", score);
		
		/*sb.append("LEAD: ").append(leadId).append(" SIZE: ").append(this.idList.size());
		sb.append(" Entropy: ").append(ent);
		sb.append(" Age: ").append(a).append(" (s)\n");;
		*/
		long a = this.age2();
		
		//sb.append("leadId\tid\tuser\t# users\tcreated\ttimestamp\tnearest\tdistance\tentropy\tsize\tage\ttext\n");
		Pattern whitespace = Pattern.compile("\\s");
		Pattern whitespace2 = Pattern.compile("\\s\\s");
		int s = size();
		int numOfUsers = users.size();
		
		StringBuilder sb = new StringBuilder();
		sb.append( ent ).append(dil);
		sb.append(numOfUsers).append(dil);
		sb.append( s ).append(dil);
		sb.append( a ).append(dil);
		sb.append( scoreAsStr ).append(dil);
		String block = sb.toString();
		
		sb = new StringBuilder();
		
		for (int i =0; i<s; i++)
		{
			String docId = idList.get(i);
			Document doc = gd.id2doc.get(docId);
			if(i == 0)
			{
				Document nDoc = null;
				String nearestId = doc.getNearestId();
				if(nearestId != null)
					nDoc = gd.id2doc.get(nearestId);
				
				sb.append(docId).append(dil);

				Date time=Date.from( Instant.ofEpochSecond( doc.getTimestamp() ) );
				sb.append(time.toString()).append(dil);
				
				sb.append(doc.getTimestamp()).append(dil);
				sb.append(nearestId).append(dil);
				sb.append(String.format("%.7f\t", doc.getNearestDist())).append(dil);				
				sb.append(block);
				
				String lbl = gd.labeled.positive.get(docId);
				lbl = lbl == null ? "" : "t_" + lbl;
				sb.append( lbl ).append(dil);
				sb.append( score ).append(dil);;
				
				
				//String text = doc.getCleanText();
				//sb.append("\t").append(text);
			}
			
			Matcher matcher = whitespace.matcher(doc.getCleanText());
			String result = matcher.replaceAll(" ");
			
			matcher = whitespace2.matcher(result);
			result = matcher.replaceAll(" ");
			sb.append("<").append(docId).append("> ").append( result ).append(" ");
			
			if (i == s-1)
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
