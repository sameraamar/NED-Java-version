package ned.tools;

import java.util.Arrays;

import ned.types.Utility;

public class HyperPlansManager {
	private int hyperplanes;
	private int dimension;
	private int dimension_jumps;
	double[] data ;

	public HyperPlansManager(int hyperplanes, int dimension, int dimension_jumps)
	{
		this.dimension = dimension;
		this.dimension_jumps = dimension_jumps;
		this.hyperplanes = hyperplanes;
		int n = hyperplanes * dimension;
		data = new double[n];
	}
	
	public void init()
	{
		int n = hyperplanes * dimension;
		for(int i=0; i<n; i++)
			data[i] = Utility.randomFill();
	}
	
	public void fixDim(int newdim)
	{
		if(newdim <= this.dimension-dimension_jumps)
			return;
		
		synchronized (this)
		{
			if(newdim <= this.dimension-dimension_jumps)
				return;
			
			int n = newdim / dimension_jumps;
			newdim = dimension_jumps * (n+1);
	
			//System.out.println("working on fixing dimension to " + newdim);
			
			
			int newsize = hyperplanes * newdim;
			
			data = Arrays.copyOf(data, newsize);
			
			int size = hyperplanes * this.dimension;
			for(int i=size; i<newsize; i++)
				data[i] = Utility.randomFill();
	
			this.dimension = newdim;
		}
	}
	
	public double get(int i, int j)
	{
		if (i>=this.hyperplanes || i<0)
			throw new ArrayIndexOutOfBoundsException("Bad i in :(" + i + ", " + j + "). Dim: " + this.dimension);
		if (j>=this.dimension)
		{
			System.out.println("Fixing dimension...");
			fixDim(j); 
		}
		else if(j<0)
			throw new ArrayIndexOutOfBoundsException("Bad j in :(" + i + ", " + j + "). Dim: " + this.dimension);
		
		return data[j*hyperplanes + i];
	}

	public int getDimension()
	{
		return this.dimension;
	}
}
