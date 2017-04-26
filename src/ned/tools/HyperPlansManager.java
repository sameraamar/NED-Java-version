package ned.tools;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import ned.types.Utility;

public class HyperPlansManager {
	private int hyperplanes;
	//private int dimension;
	private int dimension_jumps;
	double[] data ;
	private Callable<double[]>  futureTask;
	private Future<?> newdata;

	public HyperPlansManager(int hyperplanes, int dimension, int dimension_jumps)
	{
		//this.dimension = dimension;
		this.dimension_jumps = dimension_jumps;
		this.hyperplanes = hyperplanes;
		int n = hyperplanes * dimension;
		data = new double[n];
	}
	
	public void init()
	{
		int n = data.length;
		for(int i=0; i<n; i++)
			data[i] = Utility.randomFill();
	}


	private void fixDimWait(int j) {
		int dimension = data.length/hyperplanes;
		while(j>=dimension)
			//wait
		{
			try {
				Thread.sleep(1);
				fixDim(j);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			dimension = data.length/hyperplanes;
		}
	}

	public void fixDim(int newdim)
	{
		prepare();
		
		int dimension = data.length/hyperplanes;
		if(newdim < dimension)
			return;
		
		//int n = newdim / dimension_jumps;
		//newdim = dimension_jumps * (n+1);
		
		try {
			while(newdata == null) //wait
				Thread.sleep(1);
			
			data = (double[])newdata.get();
			
			futureTask = null;
			prepare();
		} catch (InterruptedException | ExecutionException | NullPointerException e ) {
			e.printStackTrace();
			System.out.println("newdim " + newdim + " dimension " + dimension + "dimension_jumps " + dimension_jumps);
		}
	}
	
	private void prepare()
	{
		if (futureTask != null)
			return;
		
		futureTask = () -> {
			int newsize = data.length + hyperplanes * dimension_jumps;
			
			double[] temp = Arrays.copyOf(data, newsize);
			
			for(int i=newsize; i<newsize; i++)
				temp[i] = Utility.randomFill();
			
			return temp;
		};
		
		newdata = ExecutionHelper.asyncAwaitRun(futureTask);
	}
	
	public double get(int i, int j)
	{
		int dimension = data.length/hyperplanes;
		if (i>=this.hyperplanes || i<0)
			throw new ArrayIndexOutOfBoundsException("Bad i in :(" + i + ", " + j + "). Dim: " + dimension);
		if (j>=dimension) //dimension
		{
			System.out.println("Fixing dimension...");
			fixDimWait(j);
		}
		else if(j<0)
			throw new ArrayIndexOutOfBoundsException("Bad j in :(" + i + ", " + j + "). Dim: " + dimension);
		
		return data[j*hyperplanes + i];
	}

	public int getDimension()
	{
		return data.length/hyperplanes;
	}
}
