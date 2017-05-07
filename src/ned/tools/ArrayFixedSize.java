package ned.tools;

import java.util.concurrent.atomic.AtomicInteger;

public class ArrayFixedSize
{
	String[] data;
	int currentSize;
	AtomicInteger index;

	public ArrayFixedSize(int size)
	{
		index=new AtomicInteger(-1);
		currentSize = 0;
		data = new String[size];
	}
	
	public void add(String s) 
	{
		
			index.set( (index.incrementAndGet()) % data.length); //update based on FIFO
		
		data[index.get()] = s;
		if(currentSize < data.length)
			currentSize++;		
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

	@Override
	public String toString() {
		return data.toString();
	}
}
