package com.iflytek.hadoop.idmapping.mapreduce;

import com.iflytek.hadoop.idmapping.util.IdMappingUtil;
import ids.IDs;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IdMappingMR2 {
    public static class IdMappingM2 extends Mapper<AvroKey<IDs>,NullWritable,Text,IDs>{
		/* idmapping step II mapper class
		*  对global_id非空的，将global_id作为key输出，否则以随机值处理，使不参与计算
		* */
		@Override
		public void map(AvroKey<IDs> key,NullWritable value,Context context) throws IOException,InterruptedException {
			 String secondKey = key.datum().getGlobalId();
			 // current ID's map is null or don't merge in step I
			 if (secondKey.length() == 0) {
			 	secondKey = IdMappingUtil.Random + "_" +IdMappingUtil.getRandomString(context.getTaskAttemptID().toString(), context.getNumReduceTasks());
			 }
			 context.write(new Text(secondKey), key.datum());
         }
    }

    public static class IdMappingR2 extends Reducer<Text,IDs,NullWritable,IDs>{
        /* idmmaping step II reduce class
        *  将第一步聚合后的结果还原到原来的条数
        * */
		@Override
		public void reduce(Text key,Iterable<IDs> values,Context context) throws IOException,InterruptedException{
	    	// 不处理数据直接输出
			if(key.toString().startsWith(IdMappingUtil.Random + "_")){
	    		for(IDs ids: values){
	    	        context.write(null, ids);
	    		}
				return;
	    	}
			// 合并为一条
			IDs tempIDs = new IDs();
			IdMappingUtil.initIDs(tempIDs);
			for(IDs ids : values) {
				IdMappingUtil.convergeID(ids, tempIDs);
			}
			context.write(null, tempIDs);
		}
    }
}