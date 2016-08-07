import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2016/7/26.
 */
public class UDFIncreaseActivity extends UDF {
    public Map<String, Integer> evaluate(Map<String, Integer> map) {
        Map<String, Integer> mapResult = new HashMap<String, Integer>();
        for(Map.Entry<String, Integer> tmp : map.entrySet()) {
            if ((tmp.getValue() & 0xff) >= 254) {
                mapResult.put(tmp.getKey(), tmp.getValue());
            } else {
                mapResult.put(tmp.getKey(), tmp.getValue() + 1);
            }
        }
        return mapResult;
    }

    public static void main(String[] args) {
        UDFIncreaseActivity udfIncreaseActivity = new UDFIncreaseActivity();
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("1", 1); map.put("2", 2); map.put("3", 3); map.put("4", 4); map.put("254", 254);
        System.out.println(udfIncreaseActivity.evaluate(map).toString());
    }
}
