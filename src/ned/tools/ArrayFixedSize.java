package ned.tools;

public class ArrayFixedSize<T>
{
	Object[] data;
	int currentSize;
	Integer index;

	public ArrayFixedSize(int size)
	{
		index = -1;
		currentSize = 0;
		data = new Object[size];
	}
	
	public void add(T s) 
	{
		synchronized(index)
		{
			index = (index + 1) % data.length; //update based on FIFO
			
			data[index] = s;
			if(currentSize < data.length)
				currentSize++;
		}
	}

	public void add(int i, T element) {
		if(i >= currentSize)
			throw new ArrayIndexOutOfBoundsException(i);

		data[i] = element;
	}

	public T get(int i) {
		if(i >= currentSize)
			throw new ArrayIndexOutOfBoundsException(i);

		return (T)data[i];
	}

	public boolean isEmpty() {
		return currentSize==0;
	}

	public int size() {
		return currentSize;
	}

	@Override
	public String toString() {
		return data.toString();
	}
}
