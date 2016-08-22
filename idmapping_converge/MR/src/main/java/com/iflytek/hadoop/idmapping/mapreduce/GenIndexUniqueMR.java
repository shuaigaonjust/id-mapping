package com.iflytek.hadoop.idmapping.mapreduce;

import ids.IDs;
import ids.Index;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

/**
 * Created by admin on 2016/8/11.
 */
public class GenIndexUniqueMR {

    public static class GenIndexUniqueMapper extends Mapper<AvroKey<ids.IDs>,NullWritable,Text,Index>  {
        Index index = new Index();

        private void write(String gid, Set<String> set, String product, Context context) throws IOException, InterruptedException {
            for (String value : set) {
                index.setGlobalId(gid);
                index.setProduct(product);
                index.setId(value);
                context.write(new Text(value), index);
            }
        }

        protected void map(AvroKey<IDs> key, NullWritable value, Context context) throws IOException, InterruptedException {
            Set<String> set;
            String gid = key.datum().getGlobalId();
            write(gid, key.datum().getDid().keySet(),"did", context);
            write(gid, key.datum().getImei().keySet(), "imei", context);
            write(gid, key.datum().getUid().keySet(), "uid", context);
            write(gid, key.datum().getPhoneNumber().keySet(),"phone_number",context);
            write(gid, key.datum().getOpenudid().keySet(),"openudid",context);
            write(gid, key.datum().getImsi().keySet(),"imsi",context);
            write(gid, key.datum().getIdfa().keySet(),"idfa",context);
            write(gid, key.datum().getMac().keySet(),"mac",context);
            write(gid, key.datum().getAndroidId().keySet(),"android_id",context);
        }
    }

    public static class GenIndexUniqueReducer extends Reducer<Text,Index,NullWritable,ids.Index> {
        @Override
        protected void reduce(Text key, Iterable<Index> values, Context context) throws IOException, InterruptedException {
            ids.Index index = new ids.Index();
            for (Index t : values) {
                context.write(null, t);
                break;
            }
        }
    }
}
