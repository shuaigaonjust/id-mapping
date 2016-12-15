package com.iflytek.hadoop.idmapping.mapreduce;

import ids.Index;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by admin on 2016/8/11.
 */
public class ExtractIndexToTextMR {

    public static class ExtractIndexToTextMapper extends Mapper<AvroKey<Index>, NullWritable, Text, Text> {

        protected void map(AvroKey<Index> key, NullWritable value, Context context) throws IOException, InterruptedException {
            String outKey = key.datum().getId();
            String outValue = key.datum().getGlobalId();
            context.write(new Text(outKey), new Text("index_" + outValue));
        }
    }

}
