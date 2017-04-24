package ned.tools;

import java.util.Arrays;

import ned.types.Utility;

public class HyperPlansManager {
	private int hyperplanes;
	private int dimension;
	private int dimension_jumps;
	float[] data ;

	public HyperPlansManager(int hyperplanes, int dimension, int dimension_jumps)
	{
		this.dimension = dimension;
		this.dimension_jumps = dimension_jumps;
		this.hyperplanes = hyperplanes;
		data = null;
	}
	
	public void init()
	{
		int n = hyperplanes * dimension;
		data = new float[n];
		for(int i=0; i<n; i++)
			data[i] = Utility.randomFill();
	}
	
	synchronized public void fixDim(int newdim)
	{
		if(newdim <= this.dimension)
			return;

		int n = newdim / dimension_jumps;
		newdim = dimension_jumps * (n+1);
		
		
		int newsize = hyperplanes * newdim;
		data = Arrays.copyOf(data, newsize);
		
		int size = hyperplanes * this.dimension;
		for(int i=size; i<newsize; i++)
			data[i] = Utility.randomFill();

		this.dimension = newdim;

	}
	
	public float get(int i, int j)
	{
		if (i>=this.hyperplanes || i<0)
			throw new ArrayIndexOutOfBoundsException("Bad i in :(" + i + ", " + j + ")");
		if (j>=this.dimension || j<0)
			throw new ArrayIndexOutOfBoundsException("Bad j in :(" + i + ", " + j + ")");
		
		return data[j*hyperplanes + i];
	}

	public int getDimension()
	{
		return this.dimension;
	}
}
