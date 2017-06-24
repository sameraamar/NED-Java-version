package ned.types;

public class ArrayFixedSize<T>
{
	Object[] data;

	public ArrayFixedSize(int size)
	{
		data = new Object[size];
	}

	public void set(int i, T element) 
	{
		data[i] = element;
	}

	public T get(int i) {
		return (T)data[i];
	}

	public int length() {
		return data.length;
	}

}
