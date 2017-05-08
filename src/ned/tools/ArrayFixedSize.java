package ned.tools;

import java.util.concurrent.atomic.AtomicInteger;

public class ArrayFixedSize<T>
{
	Object[] data;
	AtomicInteger currentSize;
	AtomicInteger index;
	Object indexLocker = new Integer(0);

	public ArrayFixedSize(int size)
	{
		index = new AtomicInteger(-1);
		currentSize = new AtomicInteger( 0 );
		data = new Object[size];
	}
	
	public void add(T s) 
	{
		if(data.length == 0)
			return;
		
		int idx = index.updateAndGet(n -> (n >= data.length-1) ? 0 : n + 1);
		
		//synchronized(indexLocker)
		//{
		//	index = idx = (index + 1) % data.length; //update based on FIFO
		//}
		
		data[idx] = s;

		if(currentSize.get() < data.length)
			currentSize.incrementAndGet();
	}

	public void add(int i, String element) {

		if(data.length == 0)
			return;
		
		if(i >= data.length || i >= currentSize.get())
			throw new ArrayIndexOutOfBoundsException(i);

		data[i] = element;
	}

	public String get(int i) {
		if(i >= data.length || i >= currentSize.get())
			throw new ArrayIndexOutOfBoundsException(i);

		return (String)data[i];
	}

	public boolean isEmpty() {
		return currentSize.get()==0;
	}

	public int size() {
		return currentSize.get();
	}

}
