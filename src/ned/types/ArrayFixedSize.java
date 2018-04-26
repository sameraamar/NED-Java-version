package ned.types;

public class ArrayFixedSize<T>
{
	Object[] data;
	int lastIndex;

	public ArrayFixedSize(int size)
	{
		data = new Object[size];
		lastIndex = -1;
	}

	public void set(int i, T element) 
	{
		if(i > lastIndex)
			lastIndex = i;
		
		data[i] = element;
	}

	public void set(int from, int to, T element) 
	{
		if(to < 0)
			to = data.length;
		
		for(int i=from; i<to; i++)
			data[i] = null;
	}
	
	public T get(int i) {
		return (T)data[i];
	}

	public int size() {
		return lastIndex+1;
	}

	public int capacity() {
		return data.length;
	}


}
