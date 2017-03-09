package ned.hash;

import java.util.LinkedList;

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

}