import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Map;

/**
 * Created by admin on 2016/6/23.
 */
public class UDFMapStringInt2String extends UDF {
    public String evaluate(Map<String, Integer> arg) {
        return  arg.toString();
    }
}
