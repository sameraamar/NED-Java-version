package ned.hash;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import ned.types.Document;

public class Bucket
{
    private LinkedList<Document> docList = new LinkedList<Document>();

    public int maxBucketSize ;


    public Bucket(int maxBucketSize)
    {
        this.maxBucketSize = maxBucketSize;
    }

    public void Append(Document doc)
    {
        getDocList().addLast(doc);
        if (getCount() > maxBucketSize)
        {
            getDocList().removeFirst();
        }
    }

    public String toString() 
    {
    	return docList.toString();
    }
    
	public int getCount() {
		return (getDocList().size());
	}

	public LinkedList<Document> getDocList() {
		return docList;
	}
	
	public List<String> getDocIDsList(String excludeId) {
		List<String> list = new ArrayList<String>();
		for (Document doc : docList) {
			if (excludeId.equals(doc.getId()))
					continue;
			
			list.add(doc.getId());
		}
		return list;
	}

}