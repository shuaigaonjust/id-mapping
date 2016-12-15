package com.iflytek.hadoop.idmapping.mapreduce;

import ids.IDs;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by admin on 2016/8/11.
 */
public class ExtractIDsToTextMR {

    public static class ExtractIDsToTextMapper extends Mapper<AvroKey<IDs>, NullWritable, Text, Text> {

        protected void map(AvroKey<IDs> key, NullWritable value, Context context) throws IOException, InterruptedException {
            String outKey = key.datum().getGlobalId();
            String outValue = key.datum().toString();
            context.write(new Text(outKey), new Text("ids_" + outValue));
        }
    }

}
