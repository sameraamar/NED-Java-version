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
		if(data.capacity() == 0)
			return;
		
		//synchronized (index) {
				int newIdx = index.updateAndGet(n -> {
					int idx = (n >= data.capacity()-1) ? 0 : n + 1;
					data.set(idx, s);
					return idx;
				});
				
				//data.set(newIdx, s);
				if(newIdx == data.capacity()-1)
					isFull = true;
		//}
		
	}

	public T get(int i) {
		return data.get(i);
	}

	public int size()
	{
		return index.get();
	}
}
