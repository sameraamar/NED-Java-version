package ned.tools;

import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

//a comparator that compares Strings
class ValueComparator implements Comparator<String>{

	HashMap<String, Integer> map = new HashMap<String, Integer>();

	public ValueComparator(HashMap<String, Integer> map){
		this.map.putAll(map);
	}

	@Override
	public int compare(String s1, String s2) {
		if(map.get(s1) >= map.get(s2)){
			return -1;
		}else{
			return 1;
		}	
	}
}
public class GeneralHelper {
	
	public static TreeMap<String, Integer> sortMapByValue(HashMap<String, Integer> map){
		Comparator<String> comparator = new ValueComparator(map);
		//TreeMap is a map sorted by its keys. 
		//The comparator is used to sort the TreeMap by keys. 
		TreeMap<String, Integer> result = new TreeMap<String, Integer>(comparator);
		result.putAll(map);
		return result;
	}
}
