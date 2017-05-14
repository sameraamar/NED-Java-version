package ned.types;

public class SerializeHelperStrInt extends SerializeHelper<String, Integer>{

	@Override
	Integer parse(String svalue) 
	{
		return Integer.parseInt(svalue);
	}
}
