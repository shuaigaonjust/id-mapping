import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

public class UDAFMaxMap extends UDAF {
    public static class MaximumInt implements UDAFEvaluator {
        public void init() {}
        public boolean iterator(IntWritable a) { return true;}
        public IntWritable terminatePartial() {return new IntWritable();}
        public boolean merge(IntWritable a) {return true;}
        public IntWritable terminate() {return  new IntWritable();}

    }
}
