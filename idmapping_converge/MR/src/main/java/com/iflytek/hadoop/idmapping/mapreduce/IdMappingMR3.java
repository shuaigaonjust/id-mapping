package com.iflytek.hadoop.idmapping.mapreduce;
import ids.IDs;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.iflytek.hadoop.idmapping.util.IdMappingUtil;

public class IdMappingMR3 {
    public static class IdMappingM3 extends Mapper<AvroKey<IDs>, NullWritable, Text, IDs>{
		/* step III map class
		*  将ids中的计算id的String作为key输出
		* */
		@Override
		public void map(AvroKey<IDs> key,NullWritable value,Context context) throws IOException,InterruptedException {
			context.write(new Text(key.datum().toString()), key.datum());
		}
    }

    public static class IdMappingR3 extends Reducer<Text,IDs,NullWritable,IDs>{
        /* step III reduce
        *  聚合并产生global_id
        * */
		@Override
		public void reduce(Text key,Iterable<IDs> values,Context context) throws IOException,InterruptedException {
			IDs ids = values.iterator().next();
			String tempGlobal_id = IdMappingUtil.getOneGlobal_Id(ids);
			if(!tempGlobal_id.equals("")){
			  ids.setGlobalId(tempGlobal_id);
			  context.write(null, ids);
			}
		}
    }
}