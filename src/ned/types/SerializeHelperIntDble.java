package ned.types;

public class SerializeHelperIntDble extends SerializeHelper<Integer, Double>{

	@Override
	Double parse(String svalue) 
	{
		return Double.parseDouble(svalue);
	}
}
