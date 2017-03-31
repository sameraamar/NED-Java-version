package ned.types;

public class TestGC {

	@Override
	protected void finalize() throws Throwable {
		System.out.println("From finalize function");
	}
	
	public static void main(String[] args) {
		
		for (int i=0; i<1000000; i++) 
		{
			Document doc = new Document("11", "This is a test", 12121212);
			//TestGC doc = new TestGC();
			if(i%10000 == 0)
			{
				System.out.println(i);
			}
			System.gc();
			
		}
		
	}

}
