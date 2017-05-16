package ned.types;

public class SerializeHelperStrStr extends SerializeHelper<String, String>{

	@Override
	String parse(String svalue) 
	{
		return svalue;
	}
}
