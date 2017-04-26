package ned.tools;


public class ArrayFixedSize
{
	String[] data;
	int currentSize;
	Integer index;

	public ArrayFixedSize(int size)
	{
		index = -1;
		currentSize = 0;
		data = new String[size];
	}
	
	public void add(String s) 
	{
		synchronized(index)
		{
			index = (index + 1) % data.length; //update based on FIFO
			
			data[index] = s;
			if(currentSize < data.length)
				currentSize++;
		}
	}

	public void add(int i, String element) {
		if(i >= currentSize)
			throw new ArrayIndexOutOfBoundsException(i);

		data[i] = element;
	}

	public String get(int i) {
		if(i >= currentSize)
			throw new ArrayIndexOutOfBoundsException(i);

		return data[i];
	}

	public boolean isEmpty() {
		return currentSize==0;
	}

	public int size() {
		return currentSize;
	}

}
