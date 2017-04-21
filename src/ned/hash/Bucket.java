package ned.hash;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import ned.types.Document;

public class Bucket
{
	private ConcurrentHashMap<String, Document> documents;
	private ConcurrentLinkedQueue<String> queue;
    private int maxBucketSize ;

    public Bucket(int maxBucketSize)
    {
        this.maxBucketSize = maxBucketSize;
        documents = new ConcurrentHashMap<String, Document>();
        queue = new ConcurrentLinkedQueue<String>();
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