package ned.tools;

import java.util.ArrayList;

import ned.types.Document;

public class JudgmentEntry implements Comparable<JudgmentEntry>
{
	int topic;
	String docJson;
	Document doc;
	
	public JudgmentEntry()
	{
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof JudgmentEntry)
		{
			return doc.equals( ((JudgmentEntry) obj).doc );
		}
		return false;
	}
	
	@Override
	public int hashCode() 
	{
		return doc.hashCode();
	}

	public int compareTo(JudgmentEntry arg0) 
	{
		return this.doc.getId().compareTo(arg0.doc.getId());
	}
}

