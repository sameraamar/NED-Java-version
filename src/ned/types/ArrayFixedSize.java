package ned.types;

public class ArrayFixedSize<T>
{
	Object[] data;

	public ArrayFixedSize(int size)
	{
		data = new Object[size];
	}
	
	/*
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

		if(notNullCount.get() < data.length)
			notNullCount.incrementAndGet();
	}
	*/

	public void set(int i, T element) 
	{
		if(element instanceof String)
			element = (T) ((String)element).intern();
		
		data[i] = element;
	}

	public T get(int i) {
		return (T)data[i];
	}

	public int length() {
		return data.length;
	}

}
