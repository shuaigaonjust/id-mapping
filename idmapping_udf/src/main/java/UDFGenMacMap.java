import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by admin on 2016/6/3.
 */
public class UDFGenMacMap extends UDF {
    static Pattern mac = Pattern.compile("(eth0:|wlan0:)?([0-9a-f]{2})[/\\s:-]?([0-9a-f]{2})[/\\s:-]?([0-9a-f]{2})[/\\s:-]?([0-9a-f]{2})[/\\s:-]?([0-9a-f]{2})[/\\s:-]?([0-9a-f]{2})?$", Pattern.CASE_INSENSITIVE);
    static String strMac = new String();

    public Map<String, Integer> evaluate(String str, String type) {
        return evaluate(str, 0, type);
    }

    public Map<String, Integer> evaluate(String str, int activity, String type) {
        Map<String, Integer> map = new HashMap<String, Integer>();

        if (type.equals("mac")) {
            Matcher m = mac.matcher(str);
            if (m.find()) {
                strMac = m.group(2) + ":"
                        + m.group(3) + ":"
                        + m.group(4) + ":"
                        + m.group(5) + ":"
                        + m.group(6);
                if (m.group(7) != null ) {
                    strMac += ":" + m.group(7);
                }
                map.put(strMac, activity);
            }
        }
        return map;
    }

    public static void main(String[] args) {
        String s = "00 06 38 51:48:34";
        UDFGenMacMap udfGenMacMap = new UDFGenMacMap();
        System.out.println(udfGenMacMap.evaluate(s,"mac").keySet().toString());
    }
}
