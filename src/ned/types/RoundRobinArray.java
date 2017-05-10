package ned.types;

import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinArray<T>
{
	ArrayFixedSize<T> data;
	boolean isFull;
	AtomicInteger index;

	public RoundRobinArray(int size)
	{
		data = new ArrayFixedSize<T>(size);
		index = new AtomicInteger(-1);
		isFull = false;
	}
	
	public void add(T s) 
	{
		if(data.length() == 0)
			return;
		
		int idx = index.updateAndGet(n -> (n >= data.length()-1) ? 0 : n + 1);
		
		data.set(idx, s);

		if(idx == data.length()-1)
			isFull = true;
	}

	public T get(int i) {
		return data.get(i);
	}

	public int size()
	{
		return index.get();
	}
}
