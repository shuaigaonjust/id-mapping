import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;


public class UDFGenMapStringInt extends UDF {

	public Map<String, Integer> evaluate(String str) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		if(str != null && str.length() != 0 && !str.toUpperCase().equals("EMPTY")) {
			map.put(str, 0);
		}
		return map;
	}
	
	public Map<String, Integer> evaluate(String str, int activity) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		if(str != null && str.length() != 0 && !str.toUpperCase().equals("EMPTY")) {
			map.put(str, activity);
		}
		return map;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		UDFGenMapStringInt uDFGenMapStringInt = new UDFGenMapStringInt();
//		System.out.print(uDFGenMapStringInt.evaluate("123").toString());
		System.out.print(uDFGenMapStringInt.evaluate("empty").toString());
//		System.out.print(uDFGenMapStringInt.evaluate("adf").toString());
//		System.out.print(uDFGenMapStringInt.evaluate("").toString());

//		Matcher m = mac.matcher("waln0:aaaaaaaaaaA");
//		if (m.find()) {
//			System.out.println(m.group(2));
//		}
	}

}
