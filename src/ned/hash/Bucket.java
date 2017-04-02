package ned.hash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ned.types.Document;

public class Bucket
{
	private ArrayList<String> documents;
    private int maxBucketSize ;

    public Bucket(int maxBucketSize)
    {
        this.maxBucketSize = maxBucketSize;
        documents = new ArrayList<String>() ; //Collections.synchronizedList(new ArrayList<String>());
    }

    synchronized public void Append(Document doc)
    {
    	String id = doc.getId();
        documents.add(id);
        
        if (documents.size() > maxBucketSize)
        {
            documents.remove(id);
        }
    }

    public String toString() 
    {
    	return documents.toString();
    }
    
    public List<String> getDocIDsList(String excludeId) 
    {
    	@SuppressWarnings("unchecked")
		ArrayList<String> bucketList = (ArrayList<String>)documents.clone();
    	
    	List<String> list = bucketList.parallelStream()
		.filter( id -> {
					//Document right = gb.id2document.get(rightId);
					//if(id == null)
					//	return false;
					
					return ( id.compareTo(excludeId)<0 ) ;
					} )
		.collect(Collectors.toList());
    	
    	return list;
    }
	
	public List<String> getDocIDsList1(String excludeId) {
		Object[] ids = documents.toArray();
		
		List<String> list = new LinkedList<String>();
		for (Object id : ids) {
			if (excludeId.equals(id))
				continue;
		
			list.add((String)id);
		} 

		return list;
	}

}