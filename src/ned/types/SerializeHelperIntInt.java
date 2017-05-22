package ned.types;

public class SerializeHelperIntInt extends SerializeHelper<Integer, Integer>{

	@Override
	Integer parse(String svalue) 
	{
		return Integer.parseInt(svalue);
	}


}
