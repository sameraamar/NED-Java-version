package ned.hash;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;


import ned.types.Document;

public class Bucket
{
	private HashMap<String, Document> documents;
	private PriorityQueue<String> queue;
    private int maxBucketSize ;

    public Bucket(int maxBucketSize)
    {
        this.maxBucketSize = maxBucketSize;
        documents = new HashMap<String, Document>();
        queue = new PriorityQueue<String>();
    }

    public void Append(Document doc)
    {
    	String id = doc.getId();
        documents.put(id, doc);
        queue.add(id);
        
        if (queue.size() > maxBucketSize)
        {
			id = queue.poll();
			documents.remove(id);
        }
    }

    public String toString() 
    {
    	return documents.toString();
    }
	
	public List<String> getDocIDsList(String excludeId) {
		Object[] ids = queue.toArray();
		
		List<String> list = new LinkedList<String>();
		for (Object id : ids) {
			if (excludeId.compareTo((String)id) <= 0)
				continue;
		
			list.add((String)id);
		} 

		return list;
	}

}